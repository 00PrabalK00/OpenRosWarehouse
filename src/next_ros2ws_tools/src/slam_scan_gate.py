#!/usr/bin/env python3
"""
Motion-aware keyframe gate for SLAM scans.

This node sits between the deskewed merged scan and slam_toolbox. It reduces
wall thickening by:
  1. Publishing scans only when translation/rotation since the last accepted
     scan is meaningful.
  2. Rejecting scans during aggressive simultaneous translation + turning,
     which are the most distortion-prone frames even after deskew.
"""

import math
from typing import Optional, Tuple

import rclpy
from nav_msgs.msg import Odometry
from rclpy.node import Node
from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
from sensor_msgs.msg import Imu, LaserScan

_SENSOR_QOS = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)


class SlamScanGate(Node):
    def __init__(self):
        super().__init__('slam_scan_gate')

        self.input_scan_topic = str(
            self.declare_parameter('input_scan_topic', '/scan_slam').value or '/scan_slam'
        ).strip() or '/scan_slam'
        self.output_scan_topic = str(
            self.declare_parameter('output_scan_topic', '/scan_slam_keyframed').value or '/scan_slam_keyframed'
        ).strip() or '/scan_slam_keyframed'
        self.odom_topic = str(
            self.declare_parameter('odom_topic', '/odometry/filtered').value or '/odometry/filtered'
        ).strip() or '/odometry/filtered'
        self.imu_topic = str(
            self.declare_parameter('imu_topic', '/imu/data').value or '/imu/data'
        ).strip() or '/imu/data'

        self.keyframe_translation_m = max(
            0.01,
            float(self.declare_parameter('keyframe_translation_m', 0.10).value),
        )
        self.keyframe_rotation_rad = max(
            0.01,
            float(self.declare_parameter('keyframe_rotation_rad', 0.09).value),
        )
        self.max_keyframe_interval_sec = max(
            0.05,
            float(self.declare_parameter('max_keyframe_interval_sec', 0.75).value),
        )
        self.min_publish_interval_sec = max(
            0.0,
            float(self.declare_parameter('min_publish_interval_sec', 0.08).value),
        )
        self.reject_turn_rate_rad_s = max(
            0.0,
            float(self.declare_parameter('reject_turn_rate_rad_s', 0.65).value),
        )
        self.reject_turn_linear_speed_mps = max(
            0.0,
            float(self.declare_parameter('reject_turn_linear_speed_mps', 0.08).value),
        )
        self.stationary_linear_speed_mps = max(
            0.0,
            float(self.declare_parameter('stationary_linear_speed_mps', 0.02).value),
        )
        self.stationary_angular_speed_rad_s = max(
            0.0,
            float(self.declare_parameter('stationary_angular_speed_rad_s', 0.05).value),
        )
        self.odom_timeout_sec = max(
            0.02,
            float(self.declare_parameter('odom_timeout_sec', 0.75).value),
        )
        self.imu_timeout_sec = max(
            0.02,
            float(self.declare_parameter('imu_timeout_sec', 0.75).value),
        )
        self.summary_period_sec = max(
            1.0,
            float(self.declare_parameter('summary_period_sec', 5.0).value),
        )

        self.scan_pub = self.create_publisher(LaserScan, self.output_scan_topic, 10)
        self.scan_sub = self.create_subscription(
            LaserScan,
            self.input_scan_topic,
            self.scan_callback,
            _SENSOR_QOS,
        )
        self.odom_sub = self.create_subscription(
            Odometry,
            self.odom_topic,
            self.odom_callback,
            20,
        )
        self.imu_sub = self.create_subscription(
            Imu,
            self.imu_topic,
            self.imu_callback,
            _SENSOR_QOS,
        )

        self.latest_pose: Optional[Tuple[float, float, float]] = None
        self.latest_linear_speed = 0.0
        self.latest_angular_speed_odom = 0.0
        self.latest_angular_speed_imu = 0.0
        self.latest_odom_stamp_ns = 0
        self.latest_imu_stamp_ns = 0

        self.last_published_pose: Optional[Tuple[float, float, float]] = None
        self.last_publish_stamp_ns = 0

        self.accepted_scans = 0
        self.dropped_scans = 0
        self.drop_small_motion = 0
        self.drop_aggressive_turn = 0
        self.drop_missing_prior = 0
        self.last_summary_ns = int(self.get_clock().now().nanoseconds)

        self.get_logger().info(
            f'SlamScanGate started: {self.input_scan_topic} -> {self.output_scan_topic} '
            f'(odom={self.odom_topic}, imu={self.imu_topic}, '
            f'keyframe={self.keyframe_translation_m:.2f}m/{self.keyframe_rotation_rad:.2f}rad)'
        )

    @staticmethod
    def _stamp_to_ns(stamp_msg) -> int:
        return int(int(stamp_msg.sec) * 1000000000 + int(stamp_msg.nanosec))

    @staticmethod
    def _yaw_from_quaternion(x: float, y: float, z: float, w: float) -> float:
        siny_cosp = 2.0 * ((w * z) + (x * y))
        cosy_cosp = 1.0 - (2.0 * ((y * y) + (z * z)))
        return math.atan2(siny_cosp, cosy_cosp)

    @staticmethod
    def _angle_diff(a: float, b: float) -> float:
        d = float(a) - float(b)
        while d > math.pi:
            d -= 2.0 * math.pi
        while d < -math.pi:
            d += 2.0 * math.pi
        return d

    def odom_callback(self, msg: Odometry):
        pose = msg.pose.pose
        twist = msg.twist.twist
        yaw = self._yaw_from_quaternion(
            float(pose.orientation.x),
            float(pose.orientation.y),
            float(pose.orientation.z),
            float(pose.orientation.w),
        )
        self.latest_pose = (
            float(pose.position.x),
            float(pose.position.y),
            float(yaw),
        )
        self.latest_linear_speed = math.hypot(
            float(twist.linear.x),
            float(twist.linear.y),
        )
        self.latest_angular_speed_odom = float(twist.angular.z)
        self.latest_odom_stamp_ns = self._stamp_to_ns(msg.header.stamp)

    def imu_callback(self, msg: Imu):
        self.latest_angular_speed_imu = float(msg.angular_velocity.z)
        self.latest_imu_stamp_ns = self._stamp_to_ns(msg.header.stamp)

    def _current_angular_speed(self, scan_stamp_ns: int) -> float:
        if self.latest_imu_stamp_ns > 0:
            imu_age = abs(float(scan_stamp_ns - self.latest_imu_stamp_ns)) / 1e9
            if imu_age <= self.imu_timeout_sec:
                return float(self.latest_angular_speed_imu)
        return float(self.latest_angular_speed_odom)

    def _odom_fresh(self, scan_stamp_ns: int) -> bool:
        if self.latest_pose is None or self.latest_odom_stamp_ns <= 0:
            return False
        odom_age = abs(float(scan_stamp_ns - self.latest_odom_stamp_ns)) / 1e9
        return odom_age <= self.odom_timeout_sec

    def _maybe_log_summary(self):
        now_ns = int(self.get_clock().now().nanoseconds)
        if (now_ns - self.last_summary_ns) < int(self.summary_period_sec * 1e9):
            return
        total = max(1, self.accepted_scans + self.dropped_scans)
        accept_ratio = 100.0 * float(self.accepted_scans) / float(total)
        self.get_logger().info(
            f'SlamScanGate: accepted={self.accepted_scans} dropped={self.dropped_scans} '
            f'({accept_ratio:.0f}% pass) small_motion={self.drop_small_motion} '
            f'aggressive_turn={self.drop_aggressive_turn} missing_prior={self.drop_missing_prior}'
        )
        self.last_summary_ns = now_ns

    def scan_callback(self, msg: LaserScan):
        scan_stamp_ns = self._stamp_to_ns(msg.header.stamp)
        if scan_stamp_ns <= 0:
            scan_stamp_ns = int(self.get_clock().now().nanoseconds)

        if not self._odom_fresh(scan_stamp_ns):
            self.dropped_scans += 1
            self.drop_missing_prior += 1
            self._maybe_log_summary()
            return

        current_pose = self.latest_pose
        if current_pose is None:
            self.dropped_scans += 1
            self.drop_missing_prior += 1
            self._maybe_log_summary()
            return

        if self.last_published_pose is None:
            self.scan_pub.publish(msg)
            self.last_published_pose = current_pose
            self.last_publish_stamp_ns = scan_stamp_ns
            self.accepted_scans += 1
            self._maybe_log_summary()
            return

        time_since_pub = max(0.0, float(scan_stamp_ns - self.last_publish_stamp_ns) / 1e9)
        if time_since_pub < self.min_publish_interval_sec:
            self.dropped_scans += 1
            self.drop_small_motion += 1
            self._maybe_log_summary()
            return

        px, py, pyaw = self.last_published_pose
        cx, cy, cyaw = current_pose
        delta_translation = math.hypot(cx - px, cy - py)
        delta_rotation = abs(self._angle_diff(cyaw, pyaw))

        linear_speed = float(self.latest_linear_speed)
        angular_speed = abs(self._current_angular_speed(scan_stamp_ns))
        moving = (
            linear_speed > self.stationary_linear_speed_mps
            or angular_speed > self.stationary_angular_speed_rad_s
        )

        force_due_time = moving and time_since_pub >= self.max_keyframe_interval_sec
        motion_keyframe = (
            delta_translation >= self.keyframe_translation_m
            or delta_rotation >= self.keyframe_rotation_rad
        )

        aggressive_turn = (
            linear_speed >= self.reject_turn_linear_speed_mps
            and angular_speed >= self.reject_turn_rate_rad_s
            and (not force_due_time)
            and delta_translation < (self.keyframe_translation_m * 1.5)
            and delta_rotation < (self.keyframe_rotation_rad * 1.5)
        )
        if aggressive_turn:
            self.dropped_scans += 1
            self.drop_aggressive_turn += 1
            self._maybe_log_summary()
            return

        if not motion_keyframe and not force_due_time:
            self.dropped_scans += 1
            self.drop_small_motion += 1
            self._maybe_log_summary()
            return

        self.scan_pub.publish(msg)
        self.last_published_pose = current_pose
        self.last_publish_stamp_ns = scan_stamp_ns
        self.accepted_scans += 1
        self._maybe_log_summary()


def main(args=None):
    rclpy.init(args=args)
    node = SlamScanGate()
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
