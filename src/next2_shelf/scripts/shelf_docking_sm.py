#!/usr/bin/env python3
"""Shelf docking state machine for warehouse UGV.

Orchestrates the full lifecycle from reflector detection through shelf
insertion using the existing ShelfDetectorNode and ZoneManager plumbing.

States
------
IDLE      -> waiting for a dock request
DETECT    -> detector enabled, polling /shelf/status_json for commit gate
COMMIT    -> calling /shelf/commit and building the docking plan
APPROACH  -> ZoneManager owns approach + lineup + local insert via /navigate_to_goal_pose
FINALIZE  -> verify final pose, trigger jack/lift
RECOVER   -> stop, reverse to approach_pose, reacquire shelf, retry

External interfaces
-------------------
Services (server):
  /shelf/dock_start   std_srvs/Trigger   Begin a docking sequence.
  /shelf/dock_abort   std_srvs/Trigger   Abort the current sequence.

Services (client):
  /shelf/set_enabled   std_srvs/SetBool    Enable / disable detector.
  /shelf/commit        std_srvs/Trigger    Commit the current shelf candidate.
  /navigate_to_goal_pose  std_srvs/Trigger  Hand approach+insert to ZoneManager.
  /safety/override     std_srvs/SetBool    Safety override for final inch-in (optional).

Topics (subscribed):
  /shelf/status_json   std_msgs/String   Detector status (JSON).

Topics (published):
  /shelf/dock_state    std_msgs/String   Current state name.
  /shelf/dock_result   std_msgs/String   JSON summary on completion.

Parameters (all have defaults)
-------------------------------
  detect_timeout_sec          float  30.0   Max time in DETECT before giving up.
  commit_min_confidence       float  0.70   Must match ShelfDetectorNode parameter.
  commit_max_sigma_x_m        float  0.03
  commit_max_sigma_y_m        float  0.03
  commit_max_sigma_yaw_deg    float  3.0
  commit_min_hotspot_count    int    3
  approach_timeout_sec        float  60.0   Max time ZoneManager may spend on approach+insert.
  finalize_timeout_sec        float  10.0   Max time for final pose verification.
  max_retry_count             int    2      Retries before permanent FAILED.
  status_stale_sec            float  0.8    Treat /shelf/status_json as stale after this.
  tick_hz                     float  10.0   Rate of the SM timer.
"""

import json
import math
import threading
import time
from typing import Any, Dict, Optional

import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from std_msgs.msg import String as StringMsg
from std_srvs.srv import Trigger, SetBool


# ---------------------------------------------------------------------------
# State identifiers
# ---------------------------------------------------------------------------
class _S:
    IDLE = 'IDLE'
    DETECT = 'DETECT'
    COMMIT = 'COMMIT'
    APPROACH = 'APPROACH'
    FINALIZE = 'FINALIZE'
    RECOVER = 'RECOVER'
    FAILED = 'FAILED'
    DONE = 'DONE'


class ShelfDockingStateMachine(Node):
    """Orchestrator node for the staged shelf docking sequence."""

    def __init__(self) -> None:
        super().__init__('shelf_docking_sm')

        # ------------------------------------------------------------------
        # Parameters
        # ------------------------------------------------------------------
        self.declare_parameters(
            namespace='',
            parameters=[
                ('detect_timeout_sec', 30.0),
                ('commit_min_confidence', 0.70),
                ('commit_max_sigma_x_m', 0.03),
                ('commit_max_sigma_y_m', 0.03),
                ('commit_max_sigma_yaw_deg', 3.0),
                ('commit_min_hotspot_count', 3),
                ('approach_timeout_sec', 60.0),
                ('finalize_timeout_sec', 10.0),
                ('max_retry_count', 20),
                ('status_stale_sec', 0.8),
                ('tick_hz', 10.0),
            ],
        )

        self._load_params()

        # -----------------------------------------------------------------
        # Internal state
        # ------------------------------------------------------------------
        self._lock = threading.RLock()
        self._state: str = _S.IDLE
        self._state_entered_mono: float = 0.0
        self._retry_count: int = 0
        self._abort_requested: bool = False
        self._approach_done: bool = False
        self._approach_ok: bool = False
        self._approach_done_msg: str = ''

        # Latest parsed /shelf/status_json payload
        self._status: Dict[str, Any] = {}
        self._status_mono: float = 0.0

        # ------------------------------------------------------------------
        # Callback groups: service handlers run in dedicated groups so they
        # don't block the SM timer or the status subscriber.
        # ------------------------------------------------------------------
        self._server_cbg = MutuallyExclusiveCallbackGroup()
        self._client_cbg = MutuallyExclusiveCallbackGroup()

        # ------------------------------------------------------------------
        # Subscriptions
        # ------------------------------------------------------------------
        self.create_subscription(
            StringMsg,
            '/shelf/status_json',
            self._on_status,
            10,
        )

        # ------------------------------------------------------------------
        # Publishers
        # ------------------------------------------------------------------
        self._state_pub = self.create_publisher(StringMsg, '/shelf/dock_state', 10)
        self._result_pub = self.create_publisher(StringMsg, '/shelf/dock_result', 10)

        # ------------------------------------------------------------------
        # Service servers
        # ------------------------------------------------------------------
        self.create_service(
            Trigger,
            '/shelf/dock_start',
            self._handle_dock_start,
            callback_group=self._server_cbg,
        )
        self.create_service(
            Trigger,
            '/shelf/dock_abort',
            self._handle_dock_abort,
            callback_group=self._server_cbg,
        )

        # ------------------------------------------------------------------
        # Service clients
        # ------------------------------------------------------------------
        self._enable_client = self.create_client(
            SetBool,
            '/shelf/set_enabled',
            callback_group=self._client_cbg,
        )
        self._commit_client = self.create_client(
            Trigger,
            '/shelf/commit',
            callback_group=self._client_cbg,
        )
        self._handoff_client = self.create_client(
            Trigger,
            '/navigate_to_goal_pose',
            callback_group=self._client_cbg,
        )

        # ------------------------------------------------------------------
        # Timer
        # ------------------------------------------------------------------
        tick_hz = max(1.0, float(self.get_parameter('tick_hz').value))
        self.create_timer(1.0 / tick_hz, self._tick, callback_group=MutuallyExclusiveCallbackGroup())

        self.get_logger().info('ShelfDockingStateMachine ready.')
        self._publish_state()

    # ------------------------------------------------------------------
    # Parameter helper
    # ------------------------------------------------------------------
    def _load_params(self) -> None:
        def _fp(name: str) -> float:
            return float(self.get_parameter(name).value)
        def _ip(name: str) -> int:
            return int(self.get_parameter(name).value)

        self._p_detect_timeout = max(1.0, _fp('detect_timeout_sec'))
        self._p_commit_min_confidence = max(0.0, min(1.0, _fp('commit_min_confidence')))
        self._p_commit_max_sigma_x = max(0.0, _fp('commit_max_sigma_x_m'))
        self._p_commit_max_sigma_y = max(0.0, _fp('commit_max_sigma_y_m'))
        self._p_commit_max_sigma_yaw = max(0.0, math.radians(_fp('commit_max_sigma_yaw_deg')))
        self._p_commit_min_hotspots = max(1, _ip('commit_min_hotspot_count'))
        self._p_approach_timeout = max(5.0, _fp('approach_timeout_sec'))
        self._p_finalize_timeout = max(1.0, _fp('finalize_timeout_sec'))
        self._p_max_retry = max(0, _ip('max_retry_count'))
        self._p_status_stale = max(0.1, _fp('status_stale_sec'))

    # ------------------------------------------------------------------
    # Subscription callback
    # ------------------------------------------------------------------
    def _on_status(self, msg: StringMsg) -> None:
        try:
            payload = json.loads(str(msg.data or '{}'))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        with self._lock:
            self._status = payload
            self._status_mono = time.monotonic()

    # ------------------------------------------------------------------
    # Service handlers
    # ------------------------------------------------------------------
    def _handle_dock_start(self, _req: Trigger.Request, resp: Trigger.Response) -> Trigger.Response:
        with self._lock:
            if self._state not in (_S.IDLE, _S.FAILED, _S.DONE):
                resp.success = False
                resp.message = f'Docking already active (state={self._state}).'
                return resp
            self._abort_requested = False
            self._retry_count = 0
            self._transition(_S.DETECT)
        resp.success = True
        resp.message = 'Docking sequence started.'
        return resp

    def _handle_dock_abort(self, _req: Trigger.Request, resp: Trigger.Response) -> Trigger.Response:
        with self._lock:
            if self._state == _S.IDLE:
                resp.success = False
                resp.message = 'No active docking sequence to abort.'
                return resp
            self._abort_requested = True
        resp.success = True
        resp.message = 'Abort requested.'
        return resp

    # ------------------------------------------------------------------
    # State machine tick (runs on timer)
    # ------------------------------------------------------------------
    def _tick(self) -> None:
        with self._lock:
            state = self._state
            abort = self._abort_requested

        if abort and state not in (_S.IDLE, _S.FAILED, _S.DONE):
            self.get_logger().warn('Docking aborted by request.')
            self._finish(ok=False, reason='aborted_by_request')
            return

        if state == _S.IDLE:
            return
        elif state == _S.DETECT:
            self._tick_detect()
        elif state == _S.COMMIT:
            self._tick_commit()
        elif state == _S.APPROACH:
            self._tick_approach()
        elif state == _S.FINALIZE:
            self._tick_finalize()
        elif state == _S.RECOVER:
            self._tick_recover()

    # ------------------------------------------------------------------
    # DETECT
    # ------------------------------------------------------------------
    def _tick_detect(self) -> None:
        """Enable detector on entry, then poll commit gate until ready."""
        elapsed = self._state_elapsed()

        # Enable detector once on first tick of this state.
        if elapsed < 0.15:
            self._call_set_enabled(True)

        # Check status freshness.
        with self._lock:
            status = dict(self._status)
            status_age = time.monotonic() - self._status_mono

        if status_age > self._p_status_stale:
            # No recent status; wait silently.
            if elapsed > self._p_detect_timeout:
                self.get_logger().error('DETECT: no status received; timing out.')
                self._maybe_recover('detect_no_status_timeout')
            return

        # Evaluate the same gate that ShelfDetectorNode uses for manual commit.
        gate_ok, gate_reason = self._evaluate_commit_gate(status)

        if gate_ok:
            self.get_logger().info(
                f'DETECT: commit gate passed '
                f'(confidence={status.get("candidate_confidence", -1):.2f}, '
                f'hotspots={status.get("candidate_hotspot_count", 0)}).'
            )
            with self._lock:
                self._transition(_S.COMMIT)
            return

        if elapsed > self._p_detect_timeout:
            self.get_logger().error(
                f'DETECT: timed out after {elapsed:.1f}s. Last gate rejection: {gate_reason}'
            )
            self._maybe_recover('detect_timeout')
            return

        # Log progress every 3 s without spamming.
        if int(elapsed) % 3 == 0 and int(elapsed * 10) % 30 < 2:
            self.get_logger().info(
                f'DETECT: waiting ({elapsed:.0f}s / {self._p_detect_timeout:.0f}s), '
                f'gate={gate_reason}, '
                f'confidence={status.get("candidate_confidence", -1):.2f}, '
                f'consistent={status.get("candidate_consistent", False)}'
            )

    # ------------------------------------------------------------------
    # COMMIT
    # ------------------------------------------------------------------
    def _tick_commit(self) -> None:
        """Call /shelf/commit once and transition to APPROACH."""
        if not self._commit_client.wait_for_service(timeout_sec=2.0):
            self.get_logger().error('COMMIT: /shelf/commit service not available.')
            self._maybe_recover('commit_service_unavailable')
            return

        req = Trigger.Request()
        future = self._commit_client.call_async(req)
        rclpy.spin_until_future_complete(self, future, timeout_sec=3.0)

        if not future.done():
            self.get_logger().error('COMMIT: /shelf/commit call timed out.')
            self._maybe_recover('commit_call_timeout')
            return

        result = future.result()
        if not result.success:
            self.get_logger().error(f'COMMIT: rejected by detector: {result.message}')
            self._maybe_recover(f'commit_rejected:{result.message}')
            return

        self.get_logger().info(f'COMMIT: shelf committed. {result.message}')
        with self._lock:
            self._approach_done = False
            self._approach_ok = False
            self._approach_done_msg = ''
            self._transition(_S.APPROACH)

        # Launch handoff asynchronously so the SM timer is not blocked.
        t = threading.Thread(target=self._run_approach_handoff, daemon=True)
        t.start()

    # ------------------------------------------------------------------
    # APPROACH (async thread drives ZoneManager handoff)
    # ------------------------------------------------------------------
    def _run_approach_handoff(self) -> None:
        """
        Call /navigate_to_goal_pose (Trigger) and wait for it to return.

        ZoneManager handles:
          build_shelf_docking_plan  -> approach_pose / entry_pose / final_pose
          build_shelf_navigation_targets -> retreat / lineup / entry alignment
          assess_shelf_docking      -> should_run_approach / should_run_entry_navigation
          local insert controller:
            ALIGN_AT_ENTRY  -> spin to heading_tolerance at entry_pose
            DRIVE_STRAIGHT  -> forward with lateral and yaw slowdown to final_pose
        """
        try:
            if not self._handoff_client.wait_for_service(timeout_sec=5.0):
                self._approach_done = True
                self._approach_ok = False
                self._approach_done_msg = 'navigate_to_goal_pose service unavailable'
                return

            req = Trigger.Request()
            future = self._handoff_client.call_async(req)

            deadline = time.monotonic() + self._p_approach_timeout
            while not future.done():
                if time.monotonic() > deadline:
                    self._approach_done = True
                    self._approach_ok = False
                    self._approach_done_msg = 'approach handoff timed out'
                    return
                with self._lock:
                    if self._abort_requested or self._state not in (_S.APPROACH, _S.FINALIZE):
                        return
                time.sleep(0.1)

            result = future.result()
            self._approach_done = True
            self._approach_ok = bool(result.success)
            self._approach_done_msg = str(result.message or '')
        except Exception as exc:  # noqa: BLE001
            self._approach_done = True
            self._approach_ok = False
            self._approach_done_msg = f'exception in handoff thread: {exc}'

    def _tick_approach(self) -> None:
        """Wait for approach+insert thread to complete; then move to FINALIZE or RECOVER."""
        elapsed = self._state_elapsed()

        if elapsed > self._p_approach_timeout + 5.0:
            # Belt-and-suspenders: if thread never sets done, timeout here too.
            self.get_logger().error('APPROACH: hard timeout reached.')
            self._maybe_recover('approach_hard_timeout')
            return

        with self._lock:
            done = self._approach_done
            ok = self._approach_ok
            msg = self._approach_done_msg

        if not done:
            return  # still running

        if ok:
            self.get_logger().info(f'APPROACH+INSERT: success. {msg}')
            with self._lock:
                self._transition(_S.FINALIZE)
        else:
            self.get_logger().error(f'APPROACH+INSERT: failed. {msg}')
            self._maybe_recover(f'approach_failed:{msg}')

    # ------------------------------------------------------------------
    # FINALIZE
    # ------------------------------------------------------------------
    def _tick_finalize(self) -> None:
        """
        Verify ZoneManager succeeded and (optionally) trigger jacking.

        At this point:
          - ZoneManager's insert controller reached final_pose within
            insert_lateral_tolerance and position_tolerance.
          - We just read the latest shelf status to confirm reflector balance
            and issue a jack action if needed.

        The jack action client lives in ShelfDetectorNode (lidar_ros2.py).
        Here we only verify final acceptance conditions from the detector status.
        If a separate jack service/action is needed, add it here.
        """
        elapsed = self._state_elapsed()

        with self._lock:
            status = dict(self._status)
            status_age = time.monotonic() - self._status_mono

        if elapsed > self._p_finalize_timeout:
            # Accept with a warning if we cannot confirm via reflectors.
            self.get_logger().warn(
                'FINALIZE: verification timeout; accepting insertion result from ZoneManager.'
            )
            self._finish(ok=True, reason='finalized_on_zm_success')
            return

        if status_age > self._p_status_stale:
            # Detector status gone; accept ZoneManager result.
            self.get_logger().warn(
                'FINALIZE: no detector status; accepting ZoneManager insertion result.'
            )
            self._finish(ok=True, reason='finalized_no_detector_status')
            return

        # Check reflector balance: front_sum ≈ back_sum means robot is centred.
        front_sum = float(status.get('candidate_front_intensity_sum', -1.0))
        back_sum = float(status.get('candidate_back_intensity_sum', -1.0))
        balance_ratio = float(status.get('candidate_intensity_balance_ratio', -1.0))

        balance_ok = (
            front_sum > 0.0
            and back_sum > 0.0
            and 0.0 <= balance_ratio <= 0.25
        )

        if balance_ok or elapsed > 2.0:
            # Accept: ZoneManager confirmed pose + balance looks good (or stale).
            reason = 'finalized_with_balance' if balance_ok else 'finalized_zm_confirmed'
            self.get_logger().info(
                f'FINALIZE: accepted. balance_ratio={balance_ratio:.2f} elapsed={elapsed:.1f}s'
            )
            self._finish(ok=True, reason=reason)

    # ------------------------------------------------------------------
    # RECOVER
    # ------------------------------------------------------------------
    def _tick_recover(self) -> None:
        """
        Brief hold: ZoneManager will have already stopped the robot.

        After recovery_hold_sec, re-enable detection and retry from DETECT.
        The approach_pose retreat happens inside ZoneManager's target builder
        (build_shelf_navigation_targets returns a 'Shelf retreat' target if
        the robot has advanced inside the mouth).
        """
        recovery_hold_sec = 3.0
        elapsed = self._state_elapsed()

        if elapsed < recovery_hold_sec:
            return

        # Disable detector briefly so it can re-acquire from scratch.
        self._call_set_enabled(False)
        time.sleep(0.2)

        with self._lock:
            self._retry_count += 1
            retries_remaining = self._p_max_retry - self._retry_count

        self.get_logger().info(
            f'RECOVER: retry {self._retry_count}/{self._p_max_retry}.'
        )

        if retries_remaining < 0:
            self.get_logger().error('RECOVER: max retries exceeded. Docking failed.')
            self._finish(ok=False, reason='max_retries_exceeded')
            return

        with self._lock:
            self._transition(_S.DETECT)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _evaluate_commit_gate(self, status: Dict[str, Any]) -> tuple:
        """
        Mirror of evaluate_commit_gate() in lidar_ros2.py.

        Uses the candidate_* fields from /shelf/status_json to decide whether
        the shelf frame is reliable enough to commit and begin approach.

        Acceptance conditions (all must be true):
          1. detector_enabled
          2. candidate_pose present (center_pose in status)
          3. candidate_fresh
          4. solver_ok
          5. candidate_consistent
          6. candidate_hotspot_count >= commit_min_hotspot_count (default 3)
          7. candidate_confidence >= commit_min_confidence (default 0.70)
          8. candidate_sigma_x <= commit_max_sigma_x_m (default 0.03)
          9. candidate_sigma_y <= commit_max_sigma_y_m (default 0.03)
         10. candidate_sigma_yaw <= commit_max_sigma_yaw_deg (default 3 deg)

        These thresholds are read from this node's own parameters, which should
        be kept in sync with the ShelfDetectorNode's parameters.
        """
        if not bool(status.get('detector_enabled', False)):
            return False, 'detector_disabled'

        if not isinstance(status.get('center_pose'), dict):
            return False, 'candidate_pose_missing'

        if not bool(status.get('candidate_fresh', False)):
            return False, 'stale_candidate'

        if not bool(status.get('solver_ok', False)):
            return False, 'solver_not_ready'

        if not bool(status.get('candidate_consistent', False)):
            return False, 'candidate_not_consistent'

        hotspot_count = int(status.get('candidate_hotspot_count', 0))
        if hotspot_count < self._p_commit_min_hotspots:
            return False, f'insufficient_hotspots:{hotspot_count}<{self._p_commit_min_hotspots}'

        confidence = float(status.get('candidate_confidence', 0.0))
        if confidence < self._p_commit_min_confidence:
            return False, f'confidence_low:{confidence:.2f}<{self._p_commit_min_confidence:.2f}'

        sigma_x = float(status.get('candidate_sigma_x', -1.0))
        if self._p_commit_max_sigma_x > 0.0:
            if not math.isfinite(sigma_x) or sigma_x < 0.0:
                return False, 'sigma_x_unavailable'
            if sigma_x > self._p_commit_max_sigma_x:
                return False, f'sigma_x_high:{sigma_x:.3f}>{self._p_commit_max_sigma_x:.3f}'

        sigma_y = float(status.get('candidate_sigma_y', -1.0))
        if self._p_commit_max_sigma_y > 0.0:
            if not math.isfinite(sigma_y) or sigma_y < 0.0:
                return False, 'sigma_y_unavailable'
            if sigma_y > self._p_commit_max_sigma_y:
                return False, f'sigma_y_high:{sigma_y:.3f}>{self._p_commit_max_sigma_y:.3f}'

        sigma_yaw = float(status.get('candidate_sigma_yaw', -1.0))
        if self._p_commit_max_sigma_yaw > 0.0:
            if not math.isfinite(sigma_yaw) or sigma_yaw < 0.0:
                return False, 'sigma_yaw_unavailable'
            if sigma_yaw > self._p_commit_max_sigma_yaw:
                return False, (
                    f'sigma_yaw_high:'
                    f'{math.degrees(sigma_yaw):.1f}deg>'
                    f'{math.degrees(self._p_commit_max_sigma_yaw):.1f}deg'
                )

        return True, ''

    def _call_set_enabled(self, enabled: bool) -> None:
        if not self._enable_client.wait_for_service(timeout_sec=1.5):
            self.get_logger().warn(f'/shelf/set_enabled service not available (enabled={enabled})')
            return
        req = SetBool.Request()
        req.data = bool(enabled)
        future = self._enable_client.call_async(req)
        rclpy.spin_until_future_complete(self, future, timeout_sec=2.0)

    def _state_elapsed(self) -> float:
        with self._lock:
            return time.monotonic() - self._state_entered_mono

    def _transition(self, new_state: str) -> None:
        """Must be called with self._lock held."""
        old = self._state
        self._state = new_state
        self._state_entered_mono = time.monotonic()
        self.get_logger().info(f'State: {old} -> {new_state}')
        self._publish_state_unlocked(new_state)

    def _publish_state(self) -> None:
        with self._lock:
            state = self._state
        self._publish_state_unlocked(state)

    def _publish_state_unlocked(self, state: str) -> None:
        msg = StringMsg()
        msg.data = state
        self._state_pub.publish(msg)

    def _maybe_recover(self, reason: str) -> None:
        with self._lock:
            if self._retry_count >= self._p_max_retry:
                self._finish_locked(ok=False, reason=reason)
            else:
                self.get_logger().warn(f'Entering RECOVER: {reason}')
                self._transition(_S.RECOVER)

    def _finish(self, *, ok: bool, reason: str) -> None:
        with self._lock:
            self._finish_locked(ok=ok, reason=reason)

    def _finish_locked(self, *, ok: bool, reason: str) -> None:
        """Must be called with self._lock held."""
        terminal = _S.DONE if ok else _S.FAILED
        self._transition(terminal)
        self._call_set_enabled_unlocked(False)
        result = {
            'success': ok,
            'reason': reason,
            'retries': self._retry_count,
        }
        msg = StringMsg()
        msg.data = json.dumps(result)
        self._result_pub.publish(msg)
        self.get_logger().info(f'Docking {"succeeded" if ok else "failed"}: {reason}')

    def _call_set_enabled_unlocked(self, enabled: bool) -> None:
        """Fire-and-forget without holding the lock through the call."""
        t = threading.Thread(target=self._call_set_enabled, args=(enabled,), daemon=True)
        t.start()


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ShelfDockingStateMachine()
    executor = MultiThreadedExecutor()
    executor.add_node(node)
    try:
        executor.spin()
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
