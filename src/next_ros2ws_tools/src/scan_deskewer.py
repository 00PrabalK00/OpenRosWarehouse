#!/usr/bin/env python3
"""
LaserScan deskewer for mapping.

Uses buffered fused odometry linear velocity plus IMU yaw rate to compensate
motion during each raw scan, then republishes an instantaneous scan in the
sensor frame for downstream scan merging / SLAM.
"""

from bisect import bisect_left
from collections import deque
import math
from typing import Optional, Tuple

import rclpy
import tf2_ros
from nav_msgs.msg import Odometry
from rclpy.duration import Duration
from rclpy.node import Node
from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
from rclpy.time import Time
from sensor_msgs.msg import Imu, LaserScan
from tf2_ros import TransformException

_SENSOR_QOS = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)


class ScanDeskewer(Node):
    def __init__(self):
        super().__init__('scan_deskewer')

        self.input_scan_topic = str(
            self.declare_parameter('input_scan_topic', '/scan').value or '/scan'
        ).strip() or '/scan'
        self.output_scan_topic = str(
            self.declare_parameter('output_scan_topic', 'scan_deskewed').value or 'scan_deskewed'
        ).strip() or 'scan_deskewed'
        self.odom_topic = str(
            self.declare_parameter('odom_topic', '/odometry/filtered').value or '/odometry/filtered'
        ).strip() or '/odometry/filtered'
        self.imu_topic = str(
            self.declare_parameter('imu_topic', '/imu/data').value or '/imu/data'
        ).strip() or '/imu/data'
        self.base_frame = str(
            self.declare_parameter('base_frame', 'base_link').value or 'base_link'
        ).strip() or 'base_link'
        self.reference_time_fraction = min(
            1.0,
            max(
                0.0,
                float(self.declare_parameter('reference_time_fraction', 0.5).value),
            ),
        )
        self.input_stamp_origin_fraction = min(
            1.0,
            max(
                0.0,
                float(self.declare_parameter('input_stamp_origin_fraction', 0.0).value),
            ),
        )
        self.input_stamp_offset_sec = float(
            self.declare_parameter('input_stamp_offset_sec', 0.0).value
        )
        self.odom_timeout_sec = max(
            0.02,
            float(self.declare_parameter('odom_timeout_sec', 0.25).value),
        )
        self.imu_timeout_sec = max(
            0.02,
            float(self.declare_parameter('imu_timeout_sec', 0.25).value),
        )
        self.motion_linear_epsilon = max(
            0.0,
            float(self.declare_parameter('motion_linear_epsilon', 0.01).value),
        )
        self.motion_angular_epsilon = max(
            0.0,
            float(self.declare_parameter('motion_angular_epsilon', 0.02).value),
        )
        self.max_scan_duration_sec = max(
            0.0,
            float(self.declare_parameter('max_scan_duration_sec', 0.25).value),
        )
        self.history_duration_sec = max(
            0.25,
            float(self.declare_parameter('history_duration_sec', 2.0).value),
        )
        self.lock_output_geometry = bool(
            self.declare_parameter('lock_output_geometry', False).value
        )

        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self)

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

        self.latest_odom_vx = 0.0
        self.latest_odom_vy = 0.0
        self.latest_odom_wz = 0.0
        self.latest_odom_stamp_ns = 0
        self.latest_imu_wz = 0.0
        self.latest_imu_stamp_ns = 0
        self.odom_history = deque()
        self.imu_history = deque()
        self.locked_geometry: Optional[Tuple[float, float, float, int]] = None

        self.get_logger().info(
            f'ScanDeskewer started: {self.input_scan_topic} -> {self.output_scan_topic} '
            f'(odom={self.odom_topic}, imu={self.imu_topic}, base_frame={self.base_frame}, '
            f'ref_frac={self.reference_time_fraction:.2f}, '
            f'input_origin_frac={self.input_stamp_origin_fraction:.2f}, '
            f'input_offset_sec={self.input_stamp_offset_sec:.3f}, '
            f'lock_output_geometry={self.lock_output_geometry})'
        )

    def odom_callback(self, msg: Odometry):
        twist = msg.twist.twist
        self.latest_odom_vx = float(twist.linear.x)
        self.latest_odom_vy = float(twist.linear.y)
        self.latest_odom_wz = float(twist.angular.z)
        self.latest_odom_stamp_ns = self._stamp_to_ns(msg.header.stamp)
        self._append_history_sample(
            self.odom_history,
            (
                self.latest_odom_stamp_ns,
                self.latest_odom_vx,
                self.latest_odom_vy,
                self.latest_odom_wz,
            ),
        )

    def imu_callback(self, msg: Imu):
        self.latest_imu_wz = float(msg.angular_velocity.z)
        self.latest_imu_stamp_ns = self._stamp_to_ns(msg.header.stamp)
        self._append_history_sample(
            self.imu_history,
            (
                self.latest_imu_stamp_ns,
                self.latest_imu_wz,
            ),
        )

    @staticmethod
    def _stamp_to_ns(stamp_msg) -> int:
        return int(int(stamp_msg.sec) * 1000000000 + int(stamp_msg.nanosec))

    @staticmethod
    def _normalize_angle(angle: float) -> float:
        return math.atan2(math.sin(angle), math.cos(angle))

    def _append_history_sample(self, history: deque, sample: Tuple[float, ...]):
        stamp_ns = int(sample[0])
        if stamp_ns <= 0:
            return

        if history and stamp_ns < int(history[-1][0]):
            history.append(sample)
            ordered = sorted(history, key=lambda item: int(item[0]))
            history.clear()
            history.extend(ordered)
        elif history and stamp_ns == int(history[-1][0]):
            history[-1] = sample
        else:
            history.append(sample)

        cutoff_ns = stamp_ns - int(self.history_duration_sec * 1e9)
        while history and int(history[0][0]) < cutoff_ns:
            history.popleft()

    @staticmethod
    def _integrate_body_twist(vx: float, vy: float, wz: float, dt: float) -> Tuple[float, float, float]:
        theta = float(wz) * float(dt)
        if abs(wz) <= 1e-6:
            return float(vx) * float(dt), float(vy) * float(dt), theta

        sin_theta = math.sin(theta)
        cos_theta = math.cos(theta)
        dx = ((float(vx) * sin_theta) + (float(vy) * (cos_theta - 1.0))) / float(wz)
        dy = ((float(vx) * (1.0 - cos_theta)) + (float(vy) * sin_theta)) / float(wz)
        return dx, dy, theta

    @staticmethod
    def _rotate_xy(x: float, y: float, yaw: float) -> Tuple[float, float]:
        cy = math.cos(yaw)
        sy = math.sin(yaw)
        return (cy * x) - (sy * y), (sy * x) + (cy * y)

    @classmethod
    def _compose_pose(
        cls,
        pose: Tuple[float, float, float],
        delta: Tuple[float, float, float],
    ) -> Tuple[float, float, float]:
        px, py, pyaw = pose
        dx, dy, dyaw = delta
        world_dx, world_dy = cls._rotate_xy(dx, dy, pyaw)
        return (
            float(px + world_dx),
            float(py + world_dy),
            cls._normalize_angle(pyaw + dyaw),
        )

    @classmethod
    def _relative_pose(
        cls,
        ref_pose: Tuple[float, float, float],
        target_pose: Tuple[float, float, float],
    ) -> Tuple[float, float, float]:
        ref_x, ref_y, ref_yaw = ref_pose
        target_x, target_y, target_yaw = target_pose
        dx = float(target_x - ref_x)
        dy = float(target_y - ref_y)
        rel_x, rel_y = cls._rotate_xy(dx, dy, -ref_yaw)
        rel_yaw = cls._normalize_angle(target_yaw - ref_yaw)
        return rel_x, rel_y, rel_yaw

    @classmethod
    def _transform_point(
        cls,
        x: float,
        y: float,
        pose: Tuple[float, float, float],
    ) -> Tuple[float, float]:
        px, py, pyaw = pose
        rx, ry = cls._rotate_xy(x, y, pyaw)
        return float(px + rx), float(py + ry)

    @staticmethod
    def _interpolate_sample(
        sample_times,
        samples,
        query_ns: int,
        timeout_sec: float,
    ) -> Optional[Tuple[float, ...]]:
        if not samples:
            return None

        idx = bisect_left(sample_times, int(query_ns))
        if idx <= 0:
            age_sec = abs(float(int(sample_times[0]) - int(query_ns))) / 1e9
            return tuple(float(value) for value in samples[0][1:]) if age_sec <= timeout_sec else None
        if idx >= len(samples):
            age_sec = abs(float(int(query_ns) - int(sample_times[-1]))) / 1e9
            return tuple(float(value) for value in samples[-1][1:]) if age_sec <= timeout_sec else None

        left = samples[idx - 1]
        right = samples[idx]
        left_ns = int(left[0])
        right_ns = int(right[0])
        left_age_sec = abs(float(int(query_ns) - left_ns)) / 1e9
        right_age_sec = abs(float(right_ns - int(query_ns))) / 1e9
        if left_age_sec > timeout_sec and right_age_sec > timeout_sec:
            return None

        if right_ns <= left_ns:
            return tuple(float(value) for value in right[1:])

        ratio = float(int(query_ns) - left_ns) / float(right_ns - left_ns)
        ratio = min(1.0, max(0.0, ratio))
        return tuple(
            float(left[i]) + (float(right[i]) - float(left[i])) * ratio
            for i in range(1, len(left))
        )

    @classmethod
    def _interpolate_pose(
        cls,
        pose_times,
        poses,
        query_ns: int,
    ) -> Tuple[float, float, float]:
        if not poses:
            return 0.0, 0.0, 0.0

        idx = bisect_left(pose_times, int(query_ns))
        if idx <= 0:
            return poses[0]
        if idx >= len(poses):
            return poses[-1]

        left_ns = int(pose_times[idx - 1])
        right_ns = int(pose_times[idx])
        left_pose = poses[idx - 1]
        right_pose = poses[idx]
        if right_ns <= left_ns:
            return right_pose

        ratio = float(int(query_ns) - left_ns) / float(right_ns - left_ns)
        ratio = min(1.0, max(0.0, ratio))
        left_yaw = float(left_pose[2])
        right_yaw = float(right_pose[2])
        yaw_delta = cls._normalize_angle(right_yaw - left_yaw)
        return (
            float(left_pose[0] + (float(right_pose[0]) - float(left_pose[0])) * ratio),
            float(left_pose[1] + (float(right_pose[1]) - float(left_pose[1])) * ratio),
            cls._normalize_angle(left_yaw + yaw_delta * ratio),
        )

    def _lookup_mount(self, frame_id: str, stamp_msg) -> Optional[Tuple[float, float, float]]:
        if not frame_id or frame_id == self.base_frame:
            return 0.0, 0.0, 0.0

        scan_time = Time.from_msg(stamp_msg)
        try:
            transform = self.tf_buffer.lookup_transform(
                self.base_frame,
                frame_id,
                scan_time,
                timeout=Duration(seconds=0.05),
            )
        except TransformException:
            try:
                transform = self.tf_buffer.lookup_transform(
                    self.base_frame,
                    frame_id,
                    Time(),
                    timeout=Duration(seconds=0.02),
                )
            except TransformException as exc:
                self.get_logger().warn(
                    f'Deskew skipped for frame={frame_id}: missing TF {self.base_frame}<-{frame_id}: {exc}',
                    throttle_duration_sec=2.0,
                )
                return None

        t = transform.transform.translation
        q = transform.transform.rotation
        siny_cosp = 2.0 * ((q.w * q.z) + (q.x * q.y))
        cosy_cosp = 1.0 - (2.0 * ((q.y * q.y) + (q.z * q.z)))
        yaw = math.atan2(siny_cosp, cosy_cosp)
        return float(t.x), float(t.y), float(yaw)

    def _motion_for_time(
        self,
        query_ns: int,
        odom_times,
        odom_samples,
        imu_times,
        imu_samples,
    ) -> Optional[Tuple[float, float, float]]:
        if not odom_samples:
            return None

        odom_motion = self._interpolate_sample(
            odom_times,
            odom_samples,
            int(query_ns),
            self.odom_timeout_sec,
        )
        if odom_motion is None:
            return None

        vx, vy, wz = odom_motion
        imu_motion = self._interpolate_sample(
            imu_times,
            imu_samples,
            int(query_ns),
            self.imu_timeout_sec,
        ) if imu_samples else None
        if imu_motion is not None:
            wz = float(imu_motion[0])

        return float(vx), float(vy), float(wz)

    def _get_output_geometry(self, msg: LaserScan) -> Tuple[float, float, float, int]:
        beam_count = len(msg.ranges)
        angle_min = float(msg.angle_min)
        angle_increment = float(msg.angle_increment)
        angle_max = angle_min + (angle_increment * max(0, beam_count - 1))

        if not self.lock_output_geometry:
            return angle_min, angle_max, angle_increment, beam_count

        if self.locked_geometry is None:
            self.locked_geometry = (angle_min, angle_max, angle_increment, beam_count)
            self.get_logger().info(
                'Locked deskew output geometry: '
                f'count={beam_count}, angle_min={angle_min:.4f}, '
                f'angle_max={angle_max:.4f}, angle_increment={angle_increment:.8f}'
            )

        return self.locked_geometry

    @staticmethod
    def _new_intensities(msg: LaserScan, beam_count: int):
        return [0.0] * beam_count if msg.intensities else []

    def scan_callback(self, msg: LaserScan):
        beam_count = len(msg.ranges)
        if beam_count == 0:
            return

        angle_increment = float(msg.angle_increment)
        if abs(angle_increment) <= 1e-9:
            self.get_logger().warn('Deskew skipped: invalid angle_increment', throttle_duration_sec=2.0)
            return

        out_angle_min, out_angle_max, out_angle_increment, out_beam_count = self._get_output_geometry(msg)
        corrected_ranges = [float('inf')] * out_beam_count
        corrected_intensities = self._new_intensities(msg, out_beam_count)

        def insert_beam(projected_angle: float, projected_range: float, source_idx: int):
            corrected_idx = int(round((projected_angle - out_angle_min) / out_angle_increment))
            if 0 <= corrected_idx < out_beam_count and projected_range < corrected_ranges[corrected_idx]:
                corrected_ranges[corrected_idx] = projected_range
                if corrected_intensities and source_idx < len(msg.intensities):
                    corrected_intensities[corrected_idx] = float(msg.intensities[source_idx])

        def passthrough_scan(output_stamp):
            angle = float(msg.angle_min)
            for idx, raw_range in enumerate(msg.ranges):
                rng = float(raw_range)
                if math.isfinite(rng):
                    insert_beam(angle, rng, idx)
                angle += angle_increment

            output = LaserScan()
            output.header.frame_id = msg.header.frame_id
            output.header.stamp = output_stamp
            output.angle_min = out_angle_min
            output.angle_max = out_angle_max
            output.angle_increment = out_angle_increment
            output.time_increment = 0.0
            output.scan_time = float(msg.scan_time)
            output.range_min = float(msg.range_min)
            output.range_max = float(msg.range_max)
            output.ranges = corrected_ranges
            output.intensities = corrected_intensities
            self.scan_pub.publish(output)

        mount = self._lookup_mount(msg.header.frame_id, msg.header.stamp)
        if mount is None:
            passthrough_scan(msg.header.stamp)
            return

        time_increment = max(0.0, float(msg.time_increment))
        scan_duration = max(0.0, time_increment * max(0, beam_count - 1))
        scan_duration = max(scan_duration, max(0.0, float(msg.scan_time)))
        if self.max_scan_duration_sec > 0.0:
            scan_duration = min(scan_duration, self.max_scan_duration_sec)
        if time_increment <= 1e-9 and beam_count > 1 and scan_duration > 1e-9:
            time_increment = scan_duration / float(beam_count - 1)
        if scan_duration <= 1e-6:
            passthrough_scan(msg.header.stamp)
            return

        input_stamp_time = Time.from_msg(msg.header.stamp)
        scan_start_time = input_stamp_time - Duration(
            seconds=scan_duration * self.input_stamp_origin_fraction
        )
        if abs(self.input_stamp_offset_sec) > 1e-9:
            scan_start_time = scan_start_time + Duration(seconds=self.input_stamp_offset_sec)

        ref_offset = scan_duration * self.reference_time_fraction
        ref_time = scan_start_time + Duration(seconds=ref_offset)
        ref_time_ns = int(ref_time.nanoseconds)
        scan_start_ns = int(scan_start_time.nanoseconds)

        odom_samples = tuple(self.odom_history)
        imu_samples = tuple(self.imu_history)
        odom_times = [int(sample[0]) for sample in odom_samples]
        imu_times = [int(sample[0]) for sample in imu_samples]

        ref_motion = self._motion_for_time(
            ref_time_ns,
            odom_times,
            odom_samples,
            imu_times,
            imu_samples,
        )
        if ref_motion is None:
            passthrough_scan(msg.header.stamp)
            return

        vx, vy, wz = ref_motion
        if (
            math.hypot(vx, vy) <= self.motion_linear_epsilon
            and abs(wz) <= self.motion_angular_epsilon
        ):
            passthrough_scan(ref_time.to_msg())
            return

        tx, ty, mount_yaw = mount
        beam_times_ns = []
        beam_motions = []
        for idx in range(beam_count):
            beam_offset = min(scan_duration, time_increment * idx)
            beam_time_ns = scan_start_ns + int(round(beam_offset * 1e9))
            motion = self._motion_for_time(
                beam_time_ns,
                odom_times,
                odom_samples,
                imu_times,
                imu_samples,
            )
            if motion is None:
                passthrough_scan(msg.header.stamp)
                return
            beam_times_ns.append(beam_time_ns)
            beam_motions.append(motion)

        beam_poses = [(0.0, 0.0, 0.0)] * beam_count
        for idx in range(1, beam_count):
            prev_time_ns = int(beam_times_ns[idx - 1])
            curr_time_ns = int(beam_times_ns[idx])
            dt_sec = max(0.0, float(curr_time_ns - prev_time_ns) / 1e9)
            prev_vx, prev_vy, prev_wz = beam_motions[idx - 1]
            curr_vx, curr_vy, curr_wz = beam_motions[idx]
            delta = self._integrate_body_twist(
                0.5 * (prev_vx + curr_vx),
                0.5 * (prev_vy + curr_vy),
                0.5 * (prev_wz + curr_wz),
                dt_sec,
            )
            beam_poses[idx] = self._compose_pose(beam_poses[idx - 1], delta)

        ref_pose = self._interpolate_pose(beam_times_ns, beam_poses, ref_time_ns)

        angle = float(msg.angle_min)
        for idx, raw_range in enumerate(msg.ranges):
            rng = float(raw_range)
            if (not math.isfinite(rng)) or (rng <= float(msg.range_min)) or (rng >= float(msg.range_max)):
                angle += angle_increment
                continue

            sensor_x = rng * math.cos(angle)
            sensor_y = rng * math.sin(angle)

            base_x, base_y = self._rotate_xy(sensor_x, sensor_y, mount_yaw)
            base_x += tx
            base_y += ty

            beam_to_ref_pose = self._relative_pose(ref_pose, beam_poses[idx])
            ref_base_x, ref_base_y = self._transform_point(base_x, base_y, beam_to_ref_pose)

            sensor_ref_x = ref_base_x - tx
            sensor_ref_y = ref_base_y - ty
            sensor_ref_x, sensor_ref_y = self._rotate_xy(sensor_ref_x, sensor_ref_y, -mount_yaw)

            corrected_range = math.hypot(sensor_ref_x, sensor_ref_y)
            if (corrected_range <= float(msg.range_min)) or (corrected_range >= float(msg.range_max)):
                angle += angle_increment
                continue

            corrected_angle = math.atan2(sensor_ref_y, sensor_ref_x)
            insert_beam(corrected_angle, corrected_range, idx)

            angle += angle_increment

        output = LaserScan()
        output.header.frame_id = msg.header.frame_id
        output.header.stamp = ref_time.to_msg()
        output.angle_min = out_angle_min
        output.angle_max = out_angle_max
        output.angle_increment = out_angle_increment
        output.time_increment = 0.0
        output.scan_time = 0.0
        output.range_min = float(msg.range_min)
        output.range_max = float(msg.range_max)
        output.ranges = corrected_ranges
        output.intensities = corrected_intensities
        self.scan_pub.publish(output)


def main(args=None):
    rclpy.init(args=args)
    node = ScanDeskewer()
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
