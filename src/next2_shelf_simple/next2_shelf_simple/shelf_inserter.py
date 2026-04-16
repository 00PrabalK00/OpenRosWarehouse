"""
Simple shelf insertion node.

Subscribes to the existing shelf detector (/shelf/status_json),
drives the robot into the shelf with direct cmd_vel — no Nav2.

Philosophy: the shelf does NOT move. Once we get a good reading,
freeze the yaw and drive straight in. Use live LiDAR only for
forward-distance and lateral-offset — never trust live yaw after
the robot starts moving (front/back pair can flip inside the shelf).

Three phases:
  ALIGN    → rotate in place to face the frozen shelf yaw
  APPROACH → drive toward entry point (standoff_m from shelf opening)
  INSERT   → drive into the shelf center with lateral + heading correction

Live yaw readings that differ >45° from frozen yaw are discarded
(the 3-hotspot solver sometimes produces ~131° instead of ~180°).
"""

import json
import math
import time

import rclpy
from rclpy.node import Node
from geometry_msgs.msg import Twist
from std_msgs.msg import String
from std_srvs.srv import Trigger, SetBool


def _normalize(a: float) -> float:
    """Wrap angle to [-pi, pi]. Safe for NaN (returns 0.0)."""
    if not math.isfinite(a):
        return 0.0
    a = a % (2.0 * math.pi)
    if a > math.pi:
        a -= 2.0 * math.pi
    return a


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


class ShelfInserter(Node):
    # States
    IDLE = 'IDLE'
    ALIGN = 'ALIGN'
    APPROACH = 'APPROACH'
    INSERT = 'INSERT'
    DONE = 'DONE'
    ABORTED = 'ABORTED'

    def __init__(self):
        super().__init__('shelf_inserter')

        # --- Parameters (all overridable) ---
        self.declare_parameter('cmd_vel_topic', '/cmd_vel_joy')
        self.declare_parameter('status_topic', '/shelf/status_json')
        # Gains
        self.declare_parameter('align_gain', 0.8)
        self.declare_parameter('align_max_w', 0.3)
        self.declare_parameter('align_tolerance', 0.05)  # rad (~3°)
        self.declare_parameter('approach_speed', 0.08)
        self.declare_parameter('approach_heading_gain', 0.6)
        self.declare_parameter('approach_max_w', 0.2)
        self.declare_parameter('approach_arrival', 0.08)  # m from entry point
        self.declare_parameter('insert_speed', 0.05)
        self.declare_parameter('insert_lateral_gain', 1.2)
        self.declare_parameter('insert_heading_gain', 0.4)
        self.declare_parameter('insert_max_w', 0.15)
        self.declare_parameter('insert_arrival', 0.05)  # m from shelf center
        # Intensity balance stop
        self.declare_parameter('intensity_balance_enabled', True)
        self.declare_parameter('intensity_balance_tolerance', 0.22)
        # Timeouts
        self.declare_parameter('total_timeout_sec', 30.0)
        self.declare_parameter('no_progress_timeout_sec', 8.0)
        # Entry standoff (distance from shelf center to entry pose)
        self.declare_parameter('entry_standoff_m', 0.50)
        # Yaw outlier rejection threshold (rad)
        self.declare_parameter('yaw_reject_threshold', 0.80)  # ~45°
        # Control rate
        self.declare_parameter('rate_hz', 20.0)

        # --- Read params ---
        self._p = {}
        for p in self._parameters:
            self._p[p] = self.get_parameter(p).value

        # --- ROS interfaces ---
        self.cmd_pub = self.create_publisher(
            Twist, self._p['cmd_vel_topic'], 10)
        self.state_pub = self.create_publisher(
            String, '/shelf_simple/state', 10)

        self.status_sub = self.create_subscription(
            String, self._p['status_topic'], self._status_cb, 10)

        self.start_srv = self.create_service(
            Trigger, '/shelf_simple/start', self._start_cb)
        self.abort_srv = self.create_service(
            Trigger, '/shelf_simple/abort', self._abort_cb)

        # --- State ---
        self.state = self.IDLE
        self.frozen_yaw = None        # frozen shelf yaw at commit time (rad)
        self.latest_status = None     # latest raw status dict
        self.start_time = 0.0
        self.last_progress_time = 0.0
        self.last_fwd_dist = float('inf')

        # Control timer
        dt = 1.0 / self._p['rate_hz']
        self.timer = self.create_timer(dt, self._control_loop)

        self.get_logger().info('shelf_inserter ready — call /shelf_simple/start')

    # ---- Status callback (always running) ----
    def _status_cb(self, msg: String):
        try:
            self.latest_status = json.loads(msg.data)
        except (json.JSONDecodeError, AttributeError):
            pass

    # ---- Service callbacks ----
    def _start_cb(self, req, resp):
        if self.state not in (self.IDLE, self.DONE, self.ABORTED):
            resp.success = False
            resp.message = f'Already running (state={self.state})'
            return resp

        # Get shelf pose from latest status
        status = self.latest_status
        if not status:
            resp.success = False
            resp.message = 'No shelf status received yet'
            return resp

        # Use committed pose, or center_pose, or candidate — whatever is available
        pose_data = (
            status.get('committed_target_pose')
            or status.get('center_pose')
        )

        if not pose_data:
            resp.success = False
            resp.message = 'No shelf pose available in status'
            return resp

        # Freeze yaw — this is the ONE thing we trust from detection
        self.frozen_yaw = _normalize(float(pose_data['yaw']))

        # Housekeeping
        self.start_time = time.monotonic()
        self.last_progress_time = self.start_time
        self.last_fwd_dist = float('inf')

        # Begin
        self.state = self.ALIGN
        self._publish_state()
        self.get_logger().info(
            f'START → ALIGN  frozen_yaw={math.degrees(self.frozen_yaw):.1f}°')

        resp.success = True
        resp.message = 'Insertion started'
        return resp

    def _abort_cb(self, req, resp):
        self._stop()
        self.state = self.ABORTED
        self._publish_state()
        self.get_logger().info('ABORTED by service call')
        resp.success = True
        resp.message = 'Aborted'
        return resp

    # ---- Main control loop (20 Hz) ----
    def _control_loop(self):
        if self.state not in (self.ALIGN, self.APPROACH, self.INSERT):
            return

        now = time.monotonic()

        # Timeout checks
        elapsed = now - self.start_time
        if elapsed > self._p['total_timeout_sec']:
            self.get_logger().warn(f'Total timeout ({self._p["total_timeout_sec"]}s)')
            self._stop()
            self.state = self.ABORTED
            self._publish_state()
            return

        if (now - self.last_progress_time) > self._p['no_progress_timeout_sec']:
            self.get_logger().warn('No progress timeout')
            self._stop()
            self.state = self.ABORTED
            self._publish_state()
            return

        # Get live detection — robot-relative pose in laser frame
        status = self.latest_status
        if not status:
            return

        live = status.get('center_pose')
        if not live:
            return

        # Live measurements in robot/laser frame
        rel_x = float(live['x'])     # forward distance to shelf center
        rel_y = float(live['y'])     # lateral offset (+ = left)
        live_yaw = _normalize(float(live['yaw']))

        # ---- YAW GUARD ----
        # Reject live yaw readings that differ too much from frozen yaw.
        # The 3-hotspot solver sometimes produces ~131° instead of ~180°,
        # and inside the shelf front/back flips causing 180° yaw jump.
        yaw_diff = abs(_normalize(live_yaw - self.frozen_yaw))
        if yaw_diff > self._p['yaw_reject_threshold']:
            # Bad reading — use frozen yaw for heading, but still use
            # rel_x and rel_y for distance (they're geometry-independent)
            heading_error = 0.0  # assume aligned since we can't trust this reading
            self.get_logger().debug(
                f'Yaw rejected: live={math.degrees(live_yaw):.1f}° '
                f'frozen={math.degrees(self.frozen_yaw):.1f}° '
                f'diff={math.degrees(yaw_diff):.1f}°')
        else:
            # Good reading — heading error is how much the shelf is rotated
            # relative to robot forward. For insertion: we want rel_yaw ≈ π
            # (shelf facing us) or ≈ 0 depending on convention.
            #
            # In the laser frame, the shelf yaw points "into" the shelf.
            # When we're facing the shelf perfectly: shelf yaw ≈ ±π (pointing away from us).
            # The heading error = how much we need to rotate to face the shelf.
            # This is: atan2(rel_y, rel_x) compared to our forward (0).
            # Simpler: the angle TO the shelf center from robot forward.
            heading_error = _normalize(live_yaw - self.frozen_yaw)

        # Forward distance (always positive when shelf is ahead)
        fwd_dist = abs(rel_x)

        # Standoff
        standoff = self._p['entry_standoff_m']
        dist_to_entry = max(0.0, fwd_dist - standoff)

        # Track progress on forward distance
        if fwd_dist < self.last_fwd_dist - 0.005:
            self.last_fwd_dist = fwd_dist
            self.last_progress_time = now

        # Phase logic
        linear_x = 0.0
        angular_z = 0.0

        # Compute heading to shelf center (angle from robot forward axis)
        bearing = math.atan2(rel_y, rel_x)

        if self.state == self.ALIGN:
            # Rotate to face the shelf center
            angular_z = _clip(
                bearing * self._p['align_gain'],
                -self._p['align_max_w'],
                self._p['align_max_w'])
            if abs(bearing) < self._p['align_tolerance']:
                self.state = self.APPROACH
                self._publish_state()
                self.get_logger().info(
                    f'ALIGN → APPROACH  fwd={fwd_dist:.2f}m lat={rel_y:.3f}m')

        elif self.state == self.APPROACH:
            # Drive toward entry point with heading correction
            linear_x = self._p['approach_speed']
            angular_z = _clip(
                bearing * self._p['approach_heading_gain'],
                -self._p['approach_max_w'],
                self._p['approach_max_w'])

            if dist_to_entry < self._p['approach_arrival']:
                self.state = self.INSERT
                self._publish_state()
                self.get_logger().info(
                    f'APPROACH → INSERT  fwd={fwd_dist:.2f}m lat={rel_y:.3f}m')

            # Re-align if heading drifts too far
            if abs(bearing) > 0.40:
                self.state = self.ALIGN
                self._publish_state()
                self.get_logger().info('APPROACH → re-ALIGN (heading drift)')

        elif self.state == self.INSERT:
            # Constant slow speed forward
            linear_x = self._p['insert_speed']

            # Lateral + heading correction to stay on center line
            angular_z = _clip(
                -rel_y * self._p['insert_lateral_gain']
                + heading_error * self._p['insert_heading_gain'],
                -self._p['insert_max_w'],
                self._p['insert_max_w'])

            # Slow down when very close
            if fwd_dist < 0.12:
                linear_x *= 0.5

            # Check arrival — position based
            arrived = fwd_dist < self._p['insert_arrival']

            # Check intensity balance (front vs back reflector intensity)
            balance_done = False
            if self._p['intensity_balance_enabled']:
                ratio = status.get('candidate_intensity_balance_ratio',
                                   status.get('intensity_balance_ratio', 1.0))
                if isinstance(ratio, (int, float)) and ratio < self._p['intensity_balance_tolerance']:
                    balance_done = True

            if arrived or balance_done:
                self._stop()
                self.state = self.DONE
                self._publish_state()
                reason = 'balance' if balance_done else 'position'
                self.get_logger().info(
                    f'INSERT → DONE ({reason})  fwd={fwd_dist:.2f}m lat={rel_y:.3f}m')
                return

        # Publish velocity
        twist = Twist()
        twist.linear.x = linear_x
        twist.angular.z = angular_z
        self.cmd_pub.publish(twist)

    def _stop(self):
        """Send zero velocity."""
        self.cmd_pub.publish(Twist())

    def _publish_state(self):
        msg = String()
        msg.data = self.state
        self.state_pub.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = ShelfInserter()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node._stop()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == '__main__':
    main()
