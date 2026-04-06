#!/usr/bin/env python3

import json
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import rclpy
import yaml
from action_msgs.msg import GoalStatus
from rclpy.action import ActionClient
from rclpy.node import Node
from std_msgs.msg import Bool, Int32

from next_ros2ws_interfaces.action import GoToZone
from next_ros2ws_interfaces.srv import (
    ClearMissionState,
    GetMissionStatus,
    GetSequenceStatus,
    ResumeMission,
    StartSequence,
    StopSequence,
)
from .action_registry import ACTION_MAPPING_PREFIX, merge_action_mappings, normalize_action_id
from .db_manager import DatabaseManager


class MissionManager(Node):
    """Owns mission sequencing state and resume/clear persistence."""

    def __init__(self):
        super().__init__('mission_manager')

        # Initialize database manager
        db_path = os.path.expanduser(
            str(self.declare_parameter('db_path', '~/DB/robot_data.db').value)
        )
        self.db_manager = DatabaseManager(db_path=db_path)

        # Legacy file path for migration fallback
        self.declare_parameter('mission_state_file', os.path.expanduser('~/mission_state.yaml'))
        state_path = str(self.get_parameter('mission_state_file').value or '').strip()
        self.state_file = os.path.expanduser(state_path or '~/mission_state.yaml')

        self._lock = threading.RLock()
        self._mission_token = 0
        self._goal_handle = None
        self._estop_active = False
        self._action_publishers: Dict[Tuple[str, str], Any] = {}
        self._action_publish_requests: List[Dict[str, Any]] = []
        self._action_publish_queue_lock = threading.Lock()
        self._lift_status_raw = 0
        self._lift_status_stamp = 0.0

        self.zone_stop_timer_default_s = max(
            0.0,
            float(self.declare_parameter('zone_stop_timer_default_s', 0.0).value),
        )
        self.zone_action_timeout_s = max(
            1.0,
            float(self.declare_parameter('zone_action_timeout_s', 45.0).value),
        )
        self.zone_action_poll_hz = max(
            1.0,
            float(self.declare_parameter('zone_action_poll_hz', 10.0).value),
        )
        self.zone_action_lift_status_topic = str(
            self.declare_parameter('zone_action_lift_status_topic', '/lift/status').value
            or '/lift/status'
        ).strip() or '/lift/status'

        self.go_to_zone_client = ActionClient(self, GoToZone, '/go_to_zone')
        self.estop_sub = self.create_subscription(Bool, '/set_estop', self._estop_callback, 10)
        self.lift_status_sub = self.create_subscription(
            Int32,
            self.zone_action_lift_status_topic,
            self._lift_status_callback,
            10,
        )
        self._action_publish_timer = self.create_timer(0.02, self._drain_action_publish_queue)

        self.start_sequence_srv = self.create_service(StartSequence, '/mission/start_sequence', self.start_sequence_callback)
        self.stop_sequence_srv = self.create_service(StopSequence, '/mission/stop_sequence', self.stop_sequence_callback)
        self.sequence_status_srv = self.create_service(
            GetSequenceStatus,
            '/mission/sequence_status',
            self.sequence_status_callback,
        )
        self.mission_status_srv = self.create_service(GetMissionStatus, '/mission/status', self.mission_status_callback)
        self.resume_srv = self.create_service(ResumeMission, '/mission/resume', self.resume_callback)
        self.clear_srv = self.create_service(ClearMissionState, '/mission/clear', self.clear_callback)

        self._reset_state_locked(message='Idle')
        self._load_state_from_database()

        self.get_logger().info('MissionManager started')
        self.get_logger().info(f'Database: {db_path}')
        self.get_logger().info(f'Legacy state file: {self.state_file}')
        self.get_logger().info(
            'Zone post-arrival: '
            f'stop_default={self.zone_stop_timer_default_s:.2f}s '
            f'action_timeout={self.zone_action_timeout_s:.2f}s '
            f'poll_hz={self.zone_action_poll_hz:.2f} '
            f'lift_status={self.zone_action_lift_status_topic}'
        )

    def _reset_state_locked(self, message: str = 'Idle'):
        self.running = False
        self.pending_resume = False
        self.mission_type = ''
        self.mission_data: List[str] = []
        self.progress = 0
        self.current_zone = ''
        self.interrupted_by = ''
        self.message = str(message)

    def _serialize_locked(self) -> Dict:
        return {
            'running': bool(self.running),
            'pending_resume': bool(self.pending_resume),
            'type': str(self.mission_type),
            'data': list(self.mission_data),
            'progress': int(self.progress),
            'current_zone': str(self.current_zone),
            'interrupted_by': str(self.interrupted_by),
            'message': str(self.message),
            'updated_at': time.time(),
        }

    def _mission_state_locked(self) -> str:
        if self.running:
            return 'RUNNING'
        if self.pending_resume:
            return 'PAUSED'
        if self.mission_type and self.progress >= len(self.mission_data) and not self.pending_resume:
            return 'COMPLETED'
        if self.interrupted_by:
            return 'INTERRUPTED'
        return 'IDLE'

    def _persist_locked(self):
        """Save mission state to database."""
        try:
            self.db_manager.save_mission_state(self._serialize_locked())
        except Exception as exc:
            self.get_logger().warn(f'Failed to persist mission state to database: {exc}')

    def _load_state_from_database(self):
        """Load mission state from database with legacy file fallback."""
        try:
            data = self.db_manager.get_mission_state()

            # If database has no meaningful state, try legacy file
            if not data or (not data.get('running') and not data.get('pending_resume') and not data.get('data')):
                if os.path.exists(self.state_file):
                    self.get_logger().info('Database empty, attempting legacy file migration...')
                    try:
                        with open(self.state_file, 'r', encoding='utf-8') as f:
                            data = yaml.safe_load(f) or {}
                        if data:
                            self.db_manager.save_mission_state(data)
                            self.get_logger().info('Migrated mission state from legacy file to database')
                    except Exception as exc:
                        self.get_logger().warn(f'Failed to load legacy mission state file: {exc}')
                        return

            if not isinstance(data, dict):
                return

            with self._lock:
                self.running = bool(data.get('running', False))
                self.pending_resume = bool(data.get('pending_resume', False))
                self.mission_type = str(data.get('type', '') or '')
                raw_data = data.get('data', [])
                self.mission_data = [str(item) for item in raw_data if str(item).strip()] if isinstance(raw_data, list) else []
                self.progress = max(0, int(data.get('progress', 0) or 0))
                self.current_zone = str(data.get('current_zone', '') or '')
                self.interrupted_by = str(data.get('interrupted_by', '') or '')
                self.message = str(data.get('message', '') or 'Idle')

                # Never auto-run on restart; convert in-flight into resumable.
                if self.running:
                    self.running = False
                    self.pending_resume = bool(self.mission_data)
                    if not self.interrupted_by:
                        self.interrupted_by = 'restart'
                    if not self.message:
                        self.message = 'Mission interrupted by node restart'
                    try:
                        self._persist_locked()
                    except Exception as exc:
                        self.get_logger().warn(f'Failed to persist recovered mission state: {exc}')
        except Exception as exc:
            self.get_logger().error(f'Failed to load mission state from database: {exc}')

    @staticmethod
    def _clean_zone_list(raw_list: List[str]) -> List[str]:
        zones = []
        prev_name = ''
        for item in raw_list:
            name = str(item or '').strip()
            if not name:
                continue
            # Preserve sequence order and allow revisits; only collapse direct duplicates.
            if name == prev_name:
                continue
            zones.append(name)
            prev_name = name
        return zones

    def _estop_callback(self, msg: Bool):
        new_state = bool(msg.data)
        if new_state == self._estop_active:
            return

        self._estop_active = new_state
        if not new_state:
            return

        cancel_handle = None
        with self._lock:
            if self.running and self.mission_type == 'sequence':
                self.running = False
                self.pending_resume = True
                self.interrupted_by = 'estop'
                self.message = 'Mission interrupted by E-STOP'
                cancel_handle = self._goal_handle
                try:
                    self._persist_locked()
                except Exception as exc:
                    self.get_logger().warn(f'Failed to persist E-STOP mission state: {exc}')

        if cancel_handle is not None:
            try:
                cancel_handle.cancel_goal_async()
            except Exception:
                pass

    def _mark_interrupted_locked(self, reason: str, message: str):
        self.running = False
        self.pending_resume = bool(self.mission_data)
        self.interrupted_by = str(reason)
        self.message = str(message)

    def _mark_completed_locked(self):
        self.running = False
        self.pending_resume = False
        self.interrupted_by = ''
        self.current_zone = ''
        self.progress = len(self.mission_data)
        self.message = 'Sequence complete'

    def _lift_status_callback(self, msg: Int32):
        self._lift_status_raw = int(msg.data)
        self._lift_status_stamp = time.monotonic()

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _parse_scalar(raw: str) -> Any:
        text = str(raw or '').strip()
        if not text:
            return 0
        lowered = text.lower()
        if lowered == 'true':
            return True
        if lowered == 'false':
            return False

        try:
            if any(ch in text for ch in ('.', 'e', 'E')):
                return float(text)
            return int(text)
        except ValueError:
            return text

    @classmethod
    def _parse_action_payload(cls, field_raw: str, msg_type: str) -> Dict[str, Any]:
        raw = str(field_raw or '').strip()
        msg_type_lc = str(msg_type or '').strip().lower()
        if not raw:
            if msg_type_lc.endswith('/bool') or msg_type_lc.endswith('bool'):
                return {'data': False}
            return {'data': 0}

        if raw.startswith('{') or raw.startswith('['):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                return {'data': parsed}
            except Exception:
                pass

        if ':' in raw:
            key, value_raw = raw.split(':', 1)
            field = str(key or '').strip() or 'data'
            return {field: cls._parse_scalar(value_raw)}

        return {'data': cls._parse_scalar(raw)}

    @staticmethod
    def _decode_lift_status(packed: int) -> Tuple[bool, bool, int]:
        value = int(packed)
        at_top = bool(value & 0x01)
        at_bottom = bool(value & 0x02)
        direction = (value >> 8) & 0xFF
        if direction >= 0x80:
            direction -= 0x100
        return at_top, at_bottom, direction

    def _wait_interruptible(self, seconds: float, token: int) -> Tuple[bool, str]:
        duration = max(0.0, float(seconds))
        if duration <= 0.0:
            return True, ''

        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            with self._lock:
                if token != self._mission_token:
                    return False, 'mission token changed'
                if not self.running:
                    return False, 'mission stopped'
            remaining = deadline - time.monotonic()
            if remaining > 0.0:
                time.sleep(min(0.1, remaining))
        return True, ''

    def _get_zone_metadata(self, zone_name: str) -> Dict[str, Any]:
        try:
            zones = self.db_manager.get_all_zones()
        except Exception as exc:
            self.get_logger().warn(f'Failed loading zones metadata: {exc}')
            return {}

        if not isinstance(zones, dict):
            return {}
        zone_data = zones.get(zone_name, {})
        return zone_data if isinstance(zone_data, dict) else {}

    def _resolve_zone_stop_timer(self, zone_meta: Dict[str, Any]) -> float:
        zone_specific = self._safe_float(zone_meta.get('charge_duration', 0.0), 0.0)
        if zone_specific > 0.0:
            return zone_specific
        return self.zone_stop_timer_default_s

    def _load_action_mappings(self) -> Dict[str, Dict[str, str]]:
        try:
            payload = self.db_manager.get_ui_mappings()
        except Exception as exc:
            self.get_logger().warn(f'Failed loading UI mappings: {exc}')
            payload = {}

        raw_mappings = payload.get('mappings', {}) if isinstance(payload, dict) else {}
        if not isinstance(raw_mappings, dict):
            raw_mappings = {}
        return merge_action_mappings(raw_mappings)

    def _resolve_action_mapping(self, action_id: str) -> Tuple[Optional[Dict[str, str]], str]:
        normalized = normalize_action_id(action_id)
        if not normalized:
            return None, 'Action id is empty'

        mapping_key = f'{ACTION_MAPPING_PREFIX}{normalized}'
        mappings = self._load_action_mappings()
        mapping = mappings.get(mapping_key)
        if not isinstance(mapping, dict):
            return None, f'Action mapping "{mapping_key}" not found'
        return mapping, ''

    def _resolve_action_publisher(self, topic: str, msg_type: str, *, create_if_missing: bool):
        normalized_topic = str(topic or '').strip()
        normalized_type = str(msg_type or '').strip() or 'std_msgs/msg/Int32'
        if not normalized_topic:
            return None, None, 'Action topic is empty'

        msg_cls = None
        msg_type_lc = normalized_type.lower()
        if msg_type_lc in ('std_msgs/msg/int32', 'std_msgs/int32'):
            msg_cls = Int32
            normalized_type = 'std_msgs/msg/Int32'
        elif msg_type_lc in ('std_msgs/msg/bool', 'std_msgs/bool'):
            msg_cls = Bool
            normalized_type = 'std_msgs/msg/Bool'
        else:
            return None, None, f'Unsupported action message type "{normalized_type}"'

        key = (normalized_topic, normalized_type)
        publisher = self._action_publishers.get(key)
        if publisher is None and create_if_missing:
            publisher = self.create_publisher(msg_cls, normalized_topic, 10)
            self._action_publishers[key] = publisher
        if publisher is None:
            return None, None, f'Action publisher not initialized for "{normalized_topic}"'
        return publisher, msg_cls, ''

    def _drain_action_publish_queue(self):
        with self._action_publish_queue_lock:
            pending = list(self._action_publish_requests)
            self._action_publish_requests.clear()

        for request in pending:
            try:
                publisher, msg_cls, err = self._resolve_action_publisher(
                    request.get('topic', ''),
                    request.get('msg_type', ''),
                    create_if_missing=True,
                )
                if publisher is None or msg_cls is None:
                    request['ok'] = False
                    request['message'] = err
                else:
                    payload = dict(request.get('payload', {}) or {})
                    if msg_cls is Int32:
                        value = payload.get('data', None)
                        if value is None and payload:
                            value = next(iter(payload.values()))
                        if value is None:
                            value = 0
                        msg = Int32()
                        msg.data = int(float(value))
                        publisher.publish(msg)
                    elif msg_cls is Bool:
                        value = payload.get('data', None)
                        if value is None and payload:
                            value = next(iter(payload.values()))
                        if value is None:
                            value = False
                        msg = Bool()
                        msg.data = bool(value)
                        publisher.publish(msg)
                    else:
                        request['ok'] = False
                        request['message'] = 'Unsupported action message class'
                        request['event'].set()
                        continue

                    request['ok'] = True
                    request['message'] = str(request.get('success_message', '') or 'Action published')
            except Exception as exc:
                request['ok'] = False
                request['message'] = f'Failed publishing action "{request.get("action_id", "")}": {exc}'
            finally:
                request['event'].set()

    def _publish_zone_action(self, action_id: str) -> Tuple[bool, str]:
        normalized = normalize_action_id(action_id)
        mapping, err = self._resolve_action_mapping(normalized)
        if mapping is None:
            return False, err

        topic = str(mapping.get('topic', '')).strip()
        msg_type = str(mapping.get('type', 'std_msgs/msg/Int32')).strip()
        field = str(mapping.get('field', '')).strip()

        payload = self._parse_action_payload(field, msg_type)
        done = threading.Event()
        request = {
            'action_id': normalized,
            'topic': topic,
            'msg_type': msg_type,
            'payload': payload,
            'success_message': f'Action "{normalized}" published to {topic}',
            'event': done,
            'ok': False,
            'message': '',
        }
        with self._action_publish_queue_lock:
            self._action_publish_requests.append(request)

        if not done.wait(timeout=min(self.zone_action_timeout_s, 5.0)):
            return False, f'Timed out waiting to publish action "{normalized}"'
        return bool(request.get('ok')), str(request.get('message') or '')

    def _wait_for_lift_completion(self, target: str, token: int) -> Tuple[bool, str]:
        target_name = 'top' if str(target).strip().lower() == 'top' else 'bottom'
        deadline = time.monotonic() + self.zone_action_timeout_s
        poll_period = max(0.02, 1.0 / self.zone_action_poll_hz)
        stable_hits = 0

        while time.monotonic() < deadline:
            with self._lock:
                if token != self._mission_token:
                    return False, 'mission token changed'
                if not self.running:
                    return False, 'mission stopped'

            packed = self._lift_status_raw
            at_top, at_bottom, direction = self._decode_lift_status(packed)
            at_target = at_top if target_name == 'top' else at_bottom

            # Require two consecutive "at target + stopped" observations.
            if at_target and direction == 0:
                stable_hits += 1
                if stable_hits >= 2:
                    return True, f'Lift reached {target_name}'
            else:
                stable_hits = 0

            time.sleep(poll_period)

        return (
            False,
            f'Timeout waiting for lift {target_name} ({self.zone_action_timeout_s:.1f}s)',
        )

    def _execute_zone_action_and_wait(self, action_id: str, token: int) -> Tuple[bool, str]:
        normalized = normalize_action_id(action_id)
        if not normalized:
            return False, 'Action id is empty'

        ok, message = self._publish_zone_action(normalized)
        if not ok:
            return False, message

        if normalized == 'lift_up':
            return self._wait_for_lift_completion('top', token)
        if normalized == 'lift_down':
            return self._wait_for_lift_completion('bottom', token)

        self.get_logger().info(
            f'No completion condition configured for action "{normalized}", continuing'
        )
        return True, message

    def _mark_interrupted_if_active(self, token: int, reason: str, message: str):
        with self._lock:
            if token != self._mission_token:
                return
            if not self.running:
                return
            self._mark_interrupted_locked(reason, message)
            try:
                self._persist_locked()
            except Exception:
                pass

    def _run_zone_post_arrival(self, zone_name: str, token: int):
        zone_meta = self._get_zone_metadata(zone_name)
        stop_timer_s = self._resolve_zone_stop_timer(zone_meta)
        zone_type = str(zone_meta.get('type', 'normal') or 'normal').strip().lower()
        zone_action = normalize_action_id(zone_meta.get('action', ''))

        if stop_timer_s > 0.0:
            with self._lock:
                if token != self._mission_token or not self.running:
                    return
                self.message = (
                    f'Zone "{zone_name}": waiting {stop_timer_s:.1f}s before next step'
                )
                try:
                    self._persist_locked()
                except Exception:
                    pass

            ok, _reason = self._wait_interruptible(stop_timer_s, token)
            if not ok:
                return

        should_run_action = bool(zone_action) or zone_type == 'action'
        if should_run_action:
            if not zone_action:
                self._mark_interrupted_if_active(
                    token,
                    'action_failed',
                    f'Action zone "{zone_name}" has no action configured',
                )
                return

            with self._lock:
                if token != self._mission_token or not self.running:
                    return
                self.message = f'Zone "{zone_name}": executing action "{zone_action}"'
                try:
                    self._persist_locked()
                except Exception:
                    pass

            ok, action_message = self._execute_zone_action_and_wait(zone_action, token)
            if not ok:
                self._mark_interrupted_if_active(token, 'action_failed', action_message)
                return

        dispatch = False
        with self._lock:
            if token != self._mission_token:
                return
            if not self.running:
                return

            self.progress += 1
            self.current_zone = ''
            if self.progress >= len(self.mission_data):
                self._mark_completed_locked()
            else:
                dispatch = True
                self.message = f'Reached zone {self.progress}/{len(self.mission_data)}'

            try:
                self._persist_locked()
            except Exception:
                pass

        if dispatch:
            self._dispatch_next()

    def _prepare_dispatch_locked(self) -> Tuple[bool, str, Optional[int], Optional[str]]:
        if not self.running or self.mission_type != 'sequence':
            return False, 'No active sequence', None, None

        if self.progress >= len(self.mission_data):
            self._mark_completed_locked()
            self._persist_locked()
            return True, 'Sequence complete', None, None

        if self._estop_active:
            self._mark_interrupted_locked('estop', 'Cannot run sequence while E-STOP is active')
            self._persist_locked()
            return False, self.message, None, None

        zone_name = self.mission_data[self.progress]
        return True, '', int(self._mission_token), str(zone_name)

    def _dispatch_next(self) -> Tuple[bool, str]:
        with self._lock:
            ok, message, token, zone_name = self._prepare_dispatch_locked()
            if token is None or zone_name is None:
                return ok, message

        if not self.go_to_zone_client.wait_for_server(timeout_sec=1.0):
            with self._lock:
                if token == self._mission_token and self.running and self.mission_type == 'sequence':
                    self._mark_interrupted_locked('unavailable', 'GoToZone action server not available')
                    self._persist_locked()
                    return False, self.message
            return False, 'GoToZone action server not available'

        with self._lock:
            if token != self._mission_token:
                return False, 'Mission token changed'
            if not self.running or self.mission_type != 'sequence':
                return False, 'No active sequence'
            if self.progress >= len(self.mission_data):
                self._mark_completed_locked()
                self._persist_locked()
                return True, 'Sequence complete'
            if str(self.mission_data[self.progress]) != str(zone_name):
                return False, 'Mission progress changed'
            if self._estop_active:
                self._mark_interrupted_locked('estop', 'Cannot run sequence while E-STOP is active')
                self._persist_locked()
                return False, self.message

            self.current_zone = zone_name
            self.message = f'Navigating to zone {self.progress + 1}/{len(self.mission_data)}: {zone_name}'
            self._persist_locked()

        goal = GoToZone.Goal()
        goal.name = zone_name
        send_future = self.go_to_zone_client.send_goal_async(goal)
        send_future.add_done_callback(lambda fut, _token=token: self._on_goal_response(fut, _token))
        return True, f'Navigating to zone {self.progress + 1}/{len(self.mission_data)}: {zone_name}'

    def _on_goal_response(self, future, token: int):
        try:
            goal_handle = future.result()
        except Exception as exc:
            with self._lock:
                if token != self._mission_token:
                    return
                self._mark_interrupted_locked('action_error', f'Failed to send GoToZone goal: {exc}')
                try:
                    self._persist_locked()
                except Exception:
                    pass
            return

        with self._lock:
            if token != self._mission_token:
                return
            if not self.running:
                return
            if goal_handle is None or not goal_handle.accepted:
                self._mark_interrupted_locked('rejected', f'GoToZone rejected zone "{self.current_zone}"')
                try:
                    self._persist_locked()
                except Exception:
                    pass
                return

            self._goal_handle = goal_handle
            result_future = goal_handle.get_result_async()
            result_future.add_done_callback(lambda fut, _token=token: self._on_goal_result(fut, _token))

    def _on_goal_result(self, future, token: int):
        try:
            wrapped = future.result()
        except Exception as exc:
            with self._lock:
                if token != self._mission_token:
                    return
                self._goal_handle = None
                if not self.running:
                    return
                self._mark_interrupted_locked('result_error', f'GoToZone result error: {exc}')
                try:
                    self._persist_locked()
                except Exception:
                    pass
            return

        run_post_arrival = False
        dispatch_next = False
        zone_name = ''
        with self._lock:
            if token != self._mission_token:
                return

            self._goal_handle = None

            if not self.running:
                return

            status = int(wrapped.status)
            result = wrapped.result

            if status == GoalStatus.STATUS_SUCCEEDED and bool(getattr(result, 'success', True)):
                zone_name = str(self.current_zone or '').strip()
                if not zone_name and 0 <= self.progress < len(self.mission_data):
                    zone_name = str(self.mission_data[self.progress])
                if zone_name:
                    idx = min(self.progress + 1, len(self.mission_data))
                    self.message = f'Arrived at zone {idx}/{len(self.mission_data)}: {zone_name}'
                    run_post_arrival = True
                else:
                    # Defensive fallback: if zone name is unavailable, do not deadlock sequence.
                    self.progress += 1
                    self.current_zone = ''
                    if self.progress >= len(self.mission_data):
                        self._mark_completed_locked()
                    else:
                        dispatch_next = True
            elif status == GoalStatus.STATUS_CANCELED:
                if not self.interrupted_by:
                    self._mark_interrupted_locked('manual', 'Sequence canceled')
            else:
                status_text = str(getattr(result, 'message', '') or '').strip()
                if not status_text:
                    status_text = f'GoToZone failed (status={status})'
                self._mark_interrupted_locked('nav_failed', status_text)

            try:
                self._persist_locked()
            except Exception:
                pass

        if run_post_arrival:
            worker = threading.Thread(
                target=self._run_zone_post_arrival,
                args=(zone_name, token),
                daemon=True,
            )
            worker.start()
        elif dispatch_next:
            self._dispatch_next()

    def start_sequence_callback(self, request, response):
        zones = self._clean_zone_list(list(request.zones))
        if len(zones) == 0:
            response.ok = False
            response.message = 'No zones provided'
            return response

        with self._lock:
            if self.running:
                response.ok = False
                response.message = 'Sequence already running'
                return response

            if self._estop_active:
                response.ok = False
                response.message = 'E-STOP active. Reset before starting sequence.'
                return response

            self._mission_token += 1
            self.running = True
            self.pending_resume = False
            self.mission_type = 'sequence'
            self.mission_data = zones
            self.progress = 0
            self.current_zone = ''
            self.interrupted_by = ''
            self.message = 'Sequence starting'
        ok, msg = self._dispatch_next()
        response.ok = bool(ok)
        response.message = str(msg)

        return response

    def stop_sequence_callback(self, _request, response):
        cancel_handle = None
        with self._lock:
            if self.running:
                self.running = False
                self.pending_resume = bool(self.mission_data)
                self.interrupted_by = 'manual'
                self.message = 'Sequence stopped by operator'
                cancel_handle = self._goal_handle
                try:
                    self._persist_locked()
                except Exception:
                    pass
                response.ok = True
                response.message = 'Sequence stopped'
            else:
                response.ok = True
                response.message = 'No active sequence'

        if cancel_handle is not None:
            try:
                cancel_handle.cancel_goal_async()
            except Exception:
                pass

        return response

    def sequence_status_callback(self, _request, response):
        with self._lock:
            total = len(self.mission_data) if self.mission_type == 'sequence' else 0
            current_zone = ''
            if self.running and 0 <= self.progress < len(self.mission_data):
                current_zone = self.mission_data[self.progress]

            response.ok = True
            response.active = bool(self.running and self.mission_type == 'sequence')
            response.current_index = int(self.progress)
            response.total_zones = int(total)
            response.current_zone = str(current_zone)
            response.message = str(self.message)
        return response

    def mission_status_callback(self, _request, response):
        with self._lock:
            response.ok = True
            response.active = bool(self.running)
            response.paused = bool(self.pending_resume)
            response.state = self._mission_state_locked()
            response.type = str(self.mission_type)
            response.data = list(self.mission_data)
            response.progress = int(self.progress)
            response.interrupted_by = str(self.interrupted_by)
            response.message = str(self.message)
        return response

    def resume_callback(self, _request, response):
        with self._lock:
            if self.running:
                response.ok = False
                response.type = str(self.mission_type)
                response.message = 'Mission already running'
                return response

            if not self.pending_resume or self.mission_type != 'sequence' or not self.mission_data:
                response.ok = False
                response.type = str(self.mission_type)
                response.message = 'No resumable mission'
                return response

            if self._estop_active:
                response.ok = False
                response.type = str(self.mission_type)
                response.message = 'E-STOP active. Reset before resuming mission.'
                return response

            self._mission_token += 1
            self.running = True
            self.pending_resume = False
            self.interrupted_by = ''
            if self.progress < 0 or self.progress >= len(self.mission_data):
                self.progress = 0
        ok, msg = self._dispatch_next()
        response.ok = bool(ok)
        response.type = str(self.mission_type)
        response.message = str(msg)
        return response

    def clear_callback(self, _request, response):
        cancel_handle = None
        with self._lock:
            if self.running:
                cancel_handle = self._goal_handle
            self._mission_token += 1
            self._goal_handle = None
            self._reset_state_locked(message='Mission state cleared')
            try:
                self._persist_locked()
            except Exception:
                pass

            response.ok = True
            response.message = 'Mission state cleared'

        if cancel_handle is not None:
            try:
                cancel_handle.cancel_goal_async()
            except Exception:
                pass

        return response


def main(args=None):
    rclpy.init(args=args)
    node = MissionManager()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.destroy_node()
        except Exception:
            pass
        try:
            rclpy.shutdown()
        except Exception:
            pass


if __name__ == '__main__':
    main()
