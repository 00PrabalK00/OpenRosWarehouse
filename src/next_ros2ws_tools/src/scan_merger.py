#!/usr/bin/env python3
"""
Scan Merger Node - Combines two LaserScan topics into one configurable output.
"""

from collections import deque
import math

import rclpy
import tf2_ros
from rclpy.duration import Duration
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from rclpy.time import Time
from sensor_msgs.msg import LaserScan, PointCloud2, PointField
from sensor_msgs_py import point_cloud2
from tf2_ros import TransformException

# LiDAR drivers typically publish with BEST_EFFORT reliability.
# Subscribing with RELIABLE (the rclpy default) causes a QoS incompatibility
# and results in zero messages received — the scan_merger goes blind.
_SENSOR_QOS = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    history=HistoryPolicy.KEEP_LAST,
    depth=5,
)


class ScanMerger(Node):
    def __init__(self):
        super().__init__('scan_merger')

        # NOTE: `use_sim_time` is a built-in ROS parameter on rclpy nodes.
        # Redeclaring it raises ParameterAlreadyDeclaredException and crashes
        # the node at startup.

        self.front_scan_topic = str(
            self.declare_parameter('front_scan_topic', '/scan').value or '/scan'
        ).strip() or '/scan'
        self.rear_scan_topic = str(
            self.declare_parameter('rear_scan_topic', '/scan2').value or '/scan2'
        ).strip() or '/scan2'
        self.output_scan_topic = str(
            self.declare_parameter('output_scan_topic', 'scan_combined').value or 'scan_combined'
        ).strip() or 'scan_combined'
        self.output_cloud_topic = str(
            self.declare_parameter('output_cloud_topic', 'scan_combined_cloud').value or 'scan_combined_cloud'
        ).strip() or 'scan_combined_cloud'
        self.output_frame = str(
            self.declare_parameter('output_frame', 'base_link').value or 'base_link'
        ).strip() or 'base_link'
        self.output_stamp_mode = str(
            self.declare_parameter('output_stamp_mode', 'now').value or 'now'
        ).strip().lower() or 'now'
        if self.output_stamp_mode not in {'source', 'reception', 'now'}:
            self.get_logger().warn(
                f'Unknown output_stamp_mode="{self.output_stamp_mode}", falling back to "now"'
            )
            self.output_stamp_mode = 'now'
        self.side_overlap_angle_rad = min(
            math.pi / 2.0,
            max(
                0.0,
                float(self.declare_parameter('side_overlap_angle_rad', 0.35).value),
            ),
        )
        self.ownership_split_enabled = bool(
            self.declare_parameter('ownership_split_enabled', False).value
        )
        self.angular_downsample_factor = max(
            1,
            int(self.declare_parameter('angular_downsample_factor', 1).value),
        )
        self.angular_gap_fill_enabled = bool(
            self.declare_parameter('angular_gap_fill_enabled', True).value
        )
        self.angular_gap_fill_max_bins = max(
            0,
            int(self.declare_parameter('angular_gap_fill_max_bins', 3).value),
        )
        self.angular_gap_fill_max_range_delta_m = max(
            0.01,
            float(self.declare_parameter('angular_gap_fill_max_range_delta_m', 0.22).value),
        )
        self.beam_footprint_fill_enabled = bool(
            self.declare_parameter('beam_footprint_fill_enabled', True).value
        )
        self.beam_footprint_fill_max_bins = max(
            0,
            int(self.declare_parameter('beam_footprint_fill_max_bins', 2).value),
        )
        self.isolated_point_filter_enabled = bool(
            self.declare_parameter('isolated_point_filter_enabled', False).value
        )
        self.isolated_point_filter_window_bins = max(
            1,
            int(self.declare_parameter('isolated_point_filter_window_bins', 2).value),
        )
        self.isolated_point_filter_range_delta_m = max(
            0.01,
            float(self.declare_parameter('isolated_point_filter_range_delta_m', 0.18).value),
        )
        self.isolated_point_filter_min_support = max(
            1,
            int(self.declare_parameter('isolated_point_filter_min_support', 1).value),
        )
        self.publish_on_input = bool(self.declare_parameter('publish_on_input', True).value)
        self.publish_min_interval_sec = max(
            0.0,
            float(self.declare_parameter('publish_min_interval_sec', 0.0).value),
        )
        self.watchdog_period_sec = max(
            0.05,
            float(self.declare_parameter('watchdog_period_sec', 0.25).value),
        )
        self.approximate_sync_enabled = bool(
            self.declare_parameter('approximate_sync_enabled', True).value
        )
        self.max_pair_stamp_diff_sec = max(
            0.0,
            float(self.declare_parameter('max_pair_stamp_diff_sec', 0.04).value),
        )
        self.unmatched_scan_timeout_sec = max(
            float(self.max_pair_stamp_diff_sec),
            float(self.declare_parameter('unmatched_scan_timeout_sec', 0.14).value),
        )
        self.allow_partial_fallback = bool(
            self.declare_parameter('allow_partial_fallback', True).value
        )
        self.sync_queue_size = max(
            2,
            int(self.declare_parameter('sync_queue_size', 8).value),
        )

        # Publisher for merged scan
        self.merged_pub = self.create_publisher(LaserScan, self.output_scan_topic, 10)
        self.merged_cloud_pub = self.create_publisher(PointCloud2, self.output_cloud_topic, 10)

        # Latest scans and receive times
        self.front_scan = None
        self.rear_scan = None
        self.front_rx_ns = 0
        self.rear_rx_ns = 0
        self.front_generation = 0
        self.rear_generation = 0
        self.last_published_front_generation = 0
        self.last_published_rear_generation = 0
        self.last_publish_ns = 0
        self.front_queue = deque(maxlen=self.sync_queue_size)
        self.rear_queue = deque(maxlen=self.sync_queue_size)

        # TF buffer/listener to project each lidar scan into the output frame robustly.
        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self)

        # Fallback mounting poses (used when TF for lidar frames is missing)
        self.declare_parameter('front_lidar_x', 0.35)
        self.declare_parameter('front_lidar_y', 0.0)
        self.declare_parameter('front_lidar_yaw', math.pi)
        self.declare_parameter('rear_lidar_x', -0.35)
        self.declare_parameter('rear_lidar_y', 0.0)
        self.declare_parameter('rear_lidar_yaw', 0.0)

        # Keep the parameter for launch/config compatibility, but do not suppress
        # a lidar from the merged output just because the opposite side is late.
        self.scan_stale_sec = max(0.0, float(self.declare_parameter('scan_stale_sec', 0.5).value))

        # Subscribe to both lidars with BEST_EFFORT QoS to match sensor driver publishers.
        self.front_sub = self.create_subscription(
            LaserScan,
            self.front_scan_topic,
            self.front_callback,
            _SENSOR_QOS,
        )

        self.rear_sub = self.create_subscription(
            LaserScan,
            self.rear_scan_topic,
            self.rear_callback,
            _SENSOR_QOS,
        )

        # Use immediate publish on fresh input to reduce motion lag.
        # Keep a slow watchdog only to flush any unpublished new input.
        self.timer = self.create_timer(self.watchdog_period_sec, self._watchdog_publish)

        self.get_logger().info('Scan Merger Node started')
        self.get_logger().info(
            f'Merging {self.front_scan_topic} (front) + {self.rear_scan_topic} (rear) '
            f'-> {self.output_scan_topic} + {self.output_cloud_topic} in {self.output_frame}'
        )
        if self.ownership_split_enabled:
            merge_mode = (
                f'ownership split: front/front-half + rear/rear-half '
                f'with {math.degrees(self.side_overlap_angle_rad):.1f} deg side overlap'
            )
        else:
            merge_mode = 'full union: preserve all valid beams from front + rear'
        self.get_logger().info(
            f'Scan merger mode={merge_mode}, '
            f'stamp_mode={self.output_stamp_mode}, publish_on_input={self.publish_on_input}, '
            f'angular_downsample_factor={self.angular_downsample_factor}, '
            f'angular_gap_fill={self.angular_gap_fill_enabled}'
            f'({self.angular_gap_fill_max_bins} bins, '
            f'{self.angular_gap_fill_max_range_delta_m:.2f}m), '
            f'beam_footprint_fill={self.beam_footprint_fill_enabled}'
            f'({self.beam_footprint_fill_max_bins} bins), '
            f'isolated_point_filter={self.isolated_point_filter_enabled}'
        )
        self.get_logger().info(
            f'Approx sync={self.approximate_sync_enabled} '
            f'(pair_window={self.max_pair_stamp_diff_sec:.3f}s, '
            f'unmatched_timeout={self.unmatched_scan_timeout_sec:.3f}s, '
            f'partial_fallback={self.allow_partial_fallback})'
        )
        if self.approximate_sync_enabled:
            self.get_logger().info(
                'Unmatched lidar updates now reuse the latest scan from the other sensor '
                'instead of dropping front/rear coverage from the merged scan'
            )

    def front_callback(self, msg):
        """Store front lidar scan."""
        self.front_scan = msg
        self.front_rx_ns = int(self.get_clock().now().nanoseconds)
        self.front_generation += 1
        if self.publish_on_input:
            if self.approximate_sync_enabled:
                self._enqueue_scan(self.front_queue, msg, self.front_rx_ns, self.front_generation)
                self._publish_from_queues(trigger='front')
            else:
                self.publish_merged(require_new_data=True)
        elif self.approximate_sync_enabled:
            self._enqueue_scan(self.front_queue, msg, self.front_rx_ns, self.front_generation)

    def rear_callback(self, msg):
        """Store rear lidar scan."""
        self.rear_scan = msg
        self.rear_rx_ns = int(self.get_clock().now().nanoseconds)
        self.rear_generation += 1
        if self.publish_on_input:
            if self.approximate_sync_enabled:
                self._enqueue_scan(self.rear_queue, msg, self.rear_rx_ns, self.rear_generation)
                self._publish_from_queues(trigger='rear')
            else:
                self.publish_merged(require_new_data=True)
        elif self.approximate_sync_enabled:
            self._enqueue_scan(self.rear_queue, msg, self.rear_rx_ns, self.rear_generation)

    def _has_unpublished_input(self) -> bool:
        return (
            self.front_generation != self.last_published_front_generation
            or self.rear_generation != self.last_published_rear_generation
        )

    def _watchdog_publish(self):
        if self.approximate_sync_enabled:
            self._publish_from_queues(trigger=None)
            return
        self.publish_merged(require_new_data=True)

    def _prune_expired_queue_entries(self, now_ns: int):
        if self.unmatched_scan_timeout_sec <= 0.0:
            return

        max_age_ns = int(float(self.unmatched_scan_timeout_sec) * 1e9)
        min_keep_ns = int(now_ns) - max_age_ns

        while self.front_queue and int(self.front_queue[0].get('stamp_ns', 0)) < min_keep_ns:
            self.front_queue.popleft()
        while self.rear_queue and int(self.rear_queue[0].get('stamp_ns', 0)) < min_keep_ns:
            self.rear_queue.popleft()

    def _enqueue_scan(self, queue, msg: LaserScan, rx_ns: int, generation: int):
        stamp_ns = self._stamp_to_ns(msg.header.stamp)
        if stamp_ns <= 0:
            stamp_ns = int(rx_ns)
        queue.append({
            'msg': msg,
            'rx_ns': int(rx_ns),
            'stamp_ns': int(stamp_ns),
            'generation': int(generation),
        })

    def _find_best_match(self, entry, other_queue):
        max_dt_ns = int(float(self.max_pair_stamp_diff_sec) * 1e9)
        best = None
        best_dt_ns = None
        for candidate in other_queue:
            dt_ns = abs(int(entry['stamp_ns']) - int(candidate['stamp_ns']))
            if dt_ns > max_dt_ns:
                continue
            if best is None or dt_ns < best_dt_ns:
                best = candidate
                best_dt_ns = dt_ns
        return best, best_dt_ns

    def _find_best_pair_across_queues(self):
        best_front = None
        best_rear = None
        best_dt_ns = None
        for front_entry in self.front_queue:
            rear_entry, dt_ns = self._find_best_match(front_entry, self.rear_queue)
            if rear_entry is None:
                continue
            if best_dt_ns is None or dt_ns < best_dt_ns:
                best_front = front_entry
                best_rear = rear_entry
                best_dt_ns = dt_ns
        return best_front, best_rear, best_dt_ns

    @staticmethod
    def _remove_queue_entry(queue, generation: int):
        for idx, entry in enumerate(queue):
            if int(entry.get('generation', -1)) == int(generation):
                del queue[idx]
                return

    def _publish_interval_allows(self, now_ns: int) -> bool:
        if self.publish_min_interval_sec <= 0.0 or self.last_publish_ns <= 0:
            return True
        dt_sec = float(int(now_ns) - int(self.last_publish_ns)) / 1e9
        return dt_sec >= self.publish_min_interval_sec

    def _publish_latest_merged(
        self,
        *,
        require_new_data: bool = False,
        now_ns: int = None,
        clear_sync_queues: bool = False,
    ) -> bool:
        if now_ns is None:
            now_ns = int(self.get_clock().now().nanoseconds)

        if require_new_data and not self._has_unpublished_input():
            return False
        if not self._publish_interval_allows(now_ns):
            return False

        front_scan = self.front_scan
        rear_scan = self.rear_scan

        if front_scan is None and rear_scan is None:
            return False

        if self._publish_merged_scan(
            front_scan,
            rear_scan,
            front_rx_ns=self.front_rx_ns if front_scan is not None else None,
            rear_rx_ns=self.rear_rx_ns if rear_scan is not None else None,
            now_ns=now_ns,
        ):
            if front_scan is not None:
                self.last_published_front_generation = self.front_generation
            if rear_scan is not None:
                self.last_published_rear_generation = self.rear_generation
            if clear_sync_queues:
                self.front_queue.clear()
                self.rear_queue.clear()
            return True

        return False

    def _publish_from_queues(self, trigger: str = None):
        now_ns = int(self.get_clock().now().nanoseconds)
        self._prune_expired_queue_entries(now_ns)
        if not self._publish_interval_allows(now_ns):
            return

        front_entry = None
        rear_entry = None

        if trigger == 'front' and self.front_queue:
            front_entry = self.front_queue[-1]
            rear_entry, _ = self._find_best_match(front_entry, self.rear_queue)
        elif trigger == 'rear' and self.rear_queue:
            rear_entry = self.rear_queue[-1]
            front_entry, _ = self._find_best_match(rear_entry, self.front_queue)

        if front_entry is None or rear_entry is None:
            front_entry, rear_entry, _ = self._find_best_pair_across_queues()

        if front_entry is not None and rear_entry is not None:
            if self._publish_merged_scan(
                front_entry['msg'],
                rear_entry['msg'],
                front_rx_ns=int(front_entry['rx_ns']),
                rear_rx_ns=int(rear_entry['rx_ns']),
                now_ns=now_ns,
            ):
                self.last_published_front_generation = int(front_entry['generation'])
                self.last_published_rear_generation = int(rear_entry['generation'])
                self._remove_queue_entry(self.front_queue, int(front_entry['generation']))
                self._remove_queue_entry(self.rear_queue, int(rear_entry['generation']))
                return

        if not self.allow_partial_fallback:
            return

        # No close timestamp match was available, so keep publishing the latest
        # front+rear data we do have rather than dropping one side from output.
        self._publish_latest_merged(
            require_new_data=True,
            now_ns=now_ns,
            clear_sync_queues=True,
        )

    def publish_merged(self, require_new_data: bool = False):
        """Merge and publish combined scan."""
        if self.approximate_sync_enabled:
            self._publish_from_queues(trigger=None)
            return
        self._publish_latest_merged(require_new_data=require_new_data)

    def _publish_merged_scan(
        self,
        front_scan: LaserScan,
        rear_scan: LaserScan,
        *,
        front_rx_ns=None,
        rear_rx_ns=None,
        now_ns: int = None,
    ) -> bool:
        if now_ns is None:
            now_ns = int(self.get_clock().now().nanoseconds)

        ref_scan = front_scan if front_scan is not None else rear_scan
        if ref_scan is None:
            return False

        angle_increments = []
        for scan in (front_scan, rear_scan):
            if scan is None:
                continue
            inc = abs(float(scan.angle_increment))
            if math.isfinite(inc) and inc > 1e-9:
                angle_increments.append(inc)

        if not angle_increments:
            self.get_logger().warn('Invalid angle_increment in reference scan; skipping publish', throttle_duration_sec=2.0)
            return False
        angle_inc = min(angle_increments)
        output_angle_inc = angle_inc * float(self.angular_downsample_factor)

        merged = LaserScan()
        merged.header.stamp = self._select_output_stamp(
            front_scan,
            rear_scan,
            front_rx_ns=front_rx_ns,
            rear_rx_ns=rear_rx_ns,
        )
        merged.header.frame_id = self.output_frame

        # Full 360-degree output in the configured output frame.
        merged.angle_min = -math.pi
        merged.angle_max = math.pi
        merged.angle_increment = output_angle_inc
        if self.output_stamp_mode == 'source':
            merged.time_increment = (
                0.0 if abs(float(ref_scan.time_increment)) <= 1e-9 else float(ref_scan.time_increment)
            )
            merged.scan_time = 0.0 if abs(merged.time_increment) <= 1e-9 else float(ref_scan.scan_time)
        else:
            # This is a synthetic merged scan, not a single lidar revolution.
            merged.time_increment = 0.0
            merged.scan_time = 0.0

        if front_scan is not None and rear_scan is not None:
            merged.range_min = min(float(front_scan.range_min), float(rear_scan.range_min))
            merged.range_max = max(float(front_scan.range_max), float(rear_scan.range_max))
        else:
            merged.range_min = float(ref_scan.range_min)
            merged.range_max = float(ref_scan.range_max)

        # Number of bins across [-pi, pi].
        num_points = int(round((merged.angle_max - merged.angle_min) / merged.angle_increment)) + 1
        if num_points < 10:
            self.get_logger().warn('Merged scan has too few bins; skipping publish', throttle_duration_sec=2.0)
            return False

        merged.ranges = [merged.range_max] * num_points
        merged.intensities = [0.0] * num_points
        cloud_points = []

        front_count = 0
        rear_count = 0
        if front_scan is not None:
            front_count = self._merge_scan_into(
                merged,
                num_points,
                front_scan,
                sensor='front',
                cloud_points=cloud_points,
            )
        if rear_scan is not None:
            rear_count = self._merge_scan_into(
                merged,
                num_points,
                rear_scan,
                sensor='rear',
                cloud_points=cloud_points,
            )

        if front_scan is not None and rear_scan is not None and front_count == 0 and rear_count == 0:
            self.get_logger().warn('Both lidar contributions are empty after merge; check TF/range validity', throttle_duration_sec=2.0)

        if front_scan is None or rear_scan is None:
            missing = 'rear(/scan2)' if rear_scan is None else 'front(/scan)'
            self.get_logger().warn(
                f'Publishing partial merged scan: waiting for first {missing} message',
                throttle_duration_sec=2.0,
            )

        removed_points = self._apply_isolated_point_filter(merged)
        if removed_points > 0:
            self.get_logger().info(
                f'Scan merger removed {removed_points} isolated bins from {self.output_scan_topic}',
                throttle_duration_sec=2.0,
            )

        self.merged_pub.publish(merged)
        self.merged_cloud_pub.publish(self._build_union_cloud(merged.header, cloud_points))
        self.last_publish_ns = now_ns
        return True

    @staticmethod
    def _build_union_cloud(header, cloud_points) -> PointCloud2:
        fields = [
            PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1),
            PointField(name='y', offset=4, datatype=PointField.FLOAT32, count=1),
            PointField(name='z', offset=8, datatype=PointField.FLOAT32, count=1),
            PointField(name='intensity', offset=12, datatype=PointField.FLOAT32, count=1),
        ]
        return point_cloud2.create_cloud(header, fields, cloud_points)

    def _fallback_mount(self, sensor: str):
        if sensor == 'front':
            tx = float(self.get_parameter('front_lidar_x').value)
            ty = float(self.get_parameter('front_lidar_y').value)
            yaw = float(self.get_parameter('front_lidar_yaw').value)
        elif sensor == 'rear':
            tx = float(self.get_parameter('rear_lidar_x').value)
            ty = float(self.get_parameter('rear_lidar_y').value)
            yaw = float(self.get_parameter('rear_lidar_yaw').value)
        else:
            tx = 0.0
            ty = 0.0
            yaw = 0.0
        return tx, ty, yaw

    @staticmethod
    def _stamp_to_ns(stamp_msg) -> int:
        return int(int(stamp_msg.sec) * 1000000000 + int(stamp_msg.nanosec))

    def _select_output_stamp(self, front_scan: LaserScan, rear_scan: LaserScan, *, front_rx_ns=None, rear_rx_ns=None):
        if self.output_stamp_mode == 'now':
            return self.get_clock().now().to_msg()

        if self.output_stamp_mode == 'reception':
            rx_candidates = []
            if front_scan is not None and front_rx_ns is not None and int(front_rx_ns) > 0:
                rx_candidates.append(int(front_rx_ns))
            if rear_scan is not None and rear_rx_ns is not None and int(rear_rx_ns) > 0:
                rx_candidates.append(int(rear_rx_ns))
            if rx_candidates:
                return Time(nanoseconds=max(rx_candidates)).to_msg()

        stamps = []
        if front_scan is not None:
            stamps.append((self._stamp_to_ns(front_scan.header.stamp), front_scan.header.stamp))
        if rear_scan is not None:
            stamps.append((self._stamp_to_ns(rear_scan.header.stamp), rear_scan.header.stamp))
        if not stamps:
            return self.get_clock().now().to_msg()
        return max(stamps, key=lambda item: item[0])[1]

    def _sensor_owns_angle(self, sensor: str, angle_rad: float) -> bool:
        if not self.ownership_split_enabled:
            return True

        side_threshold = (math.pi / 2.0) - (self.side_overlap_angle_rad / 2.0)
        angle_abs = abs(float(angle_rad))

        if sensor == 'front':
            return angle_abs <= (side_threshold + self.side_overlap_angle_rad)
        if sensor == 'rear':
            return angle_abs >= side_threshold
        return True

    @staticmethod
    def _range_is_valid(scan: LaserScan, rng: float) -> bool:
        return (
            math.isfinite(float(rng))
            and float(rng) > float(scan.range_min)
            and float(rng) < float(scan.range_max)
        )

    @staticmethod
    def _write_bin_if_closer(merged: LaserScan, index: int, rng: float, intensity: float) -> bool:
        if index < 0 or index >= len(merged.ranges):
            return False
        if not ScanMerger._range_is_valid(merged, rng):
            return False
        if float(rng) >= float(merged.ranges[index]):
            return False
        merged.ranges[index] = float(rng)
        if index < len(merged.intensities):
            merged.intensities[index] = float(intensity)
        return True

    @staticmethod
    def _normalize_angle(angle_rad: float) -> float:
        return math.atan2(math.sin(float(angle_rad)), math.cos(float(angle_rad)))

    def _fill_beam_footprint(
        self,
        merged: LaserScan,
        *,
        merged_idx: int,
        base_angle: float,
        base_range: float,
        intensity: float,
        lidar_yaw: float,
        tx: float,
        ty: float,
        raw_angle: float,
        raw_range: float,
        raw_angle_inc: float,
    ) -> int:
        if not self.beam_footprint_fill_enabled or self.beam_footprint_fill_max_bins <= 0:
            return 0

        half_raw_inc = 0.5 * abs(float(raw_angle_inc))
        if half_raw_inc <= 1e-9:
            return 0

        cy = math.cos(lidar_yaw)
        sy = math.sin(lidar_yaw)
        half_width = 0.0

        for edge_angle in (float(raw_angle) - half_raw_inc, float(raw_angle) + half_raw_inc):
            lx = float(raw_range) * math.cos(edge_angle)
            ly = float(raw_range) * math.sin(edge_angle)
            bx = cy * lx - sy * ly + float(tx)
            by = sy * lx + cy * ly + float(ty)
            edge_base_angle = math.atan2(by, bx)
            half_width = max(
                half_width,
                abs(self._normalize_angle(edge_base_angle - float(base_angle))),
            )

        if half_width <= 1e-9:
            return 0

        extra_bins = int(math.ceil(float(half_width) / float(merged.angle_increment)))
        extra_bins = min(int(self.beam_footprint_fill_max_bins), extra_bins)
        if extra_bins <= 0:
            return 0

        filled = 0
        angle_margin = 0.55 * float(merged.angle_increment)
        for offset in range(-extra_bins, extra_bins + 1):
            if offset == 0:
                continue
            idx = int(merged_idx) + int(offset)
            if idx < 0 or idx >= len(merged.ranges):
                continue
            idx_angle = float(merged.angle_min) + (float(idx) * float(merged.angle_increment))
            angle_error = abs(self._normalize_angle(idx_angle - float(base_angle)))
            if angle_error > (float(half_width) + angle_margin):
                continue
            if self._write_bin_if_closer(merged, idx, base_range, intensity):
                filled += 1

        return filled

    def _apply_isolated_point_filter(self, merged: LaserScan) -> int:
        if not self.isolated_point_filter_enabled:
            return 0

        ranges = list(merged.ranges)
        filtered_ranges = list(ranges)
        filtered_intensities = list(merged.intensities)
        removed = 0
        num_points = len(ranges)

        for idx, center in enumerate(ranges):
            if not self._range_is_valid(merged, center):
                continue

            support = 0
            start = max(0, idx - self.isolated_point_filter_window_bins)
            stop = min(num_points, idx + self.isolated_point_filter_window_bins + 1)
            for neighbor_idx in range(start, stop):
                if neighbor_idx == idx:
                    continue
                neighbor = ranges[neighbor_idx]
                if not self._range_is_valid(merged, neighbor):
                    continue
                if abs(float(neighbor) - float(center)) <= self.isolated_point_filter_range_delta_m:
                    support += 1
                    if support >= self.isolated_point_filter_min_support:
                        break

            if support >= self.isolated_point_filter_min_support:
                continue

            filtered_ranges[idx] = float(merged.range_max)
            if idx < len(filtered_intensities):
                filtered_intensities[idx] = 0.0
            removed += 1

        if removed <= 0:
            return 0

        merged.ranges = filtered_ranges
        if len(filtered_intensities) == len(merged.ranges):
            merged.intensities = filtered_intensities
        return removed

    def _merge_scan_into(
        self,
        merged: LaserScan,
        num_points: int,
        scan: LaserScan,
        sensor: str,
        *,
        cloud_points=None,
    ) -> int:
        try:
            if scan.header.frame_id == self.output_frame:
                tx = 0.0
                ty = 0.0
                yaw = 0.0
            else:
                # Prefer transform at scan capture time to minimise motion distortion.
                # Falls back to latest available transform when the scan timestamp is not
                # yet in the TF buffer (e.g. during the first few frames after startup).
                scan_time = Time.from_msg(scan.header.stamp)
                try:
                    tf = self.tf_buffer.lookup_transform(
                        self.output_frame,
                        scan.header.frame_id,
                        scan_time,
                        timeout=Duration(seconds=0.1),
                    )
                except TransformException:
                    tf = self.tf_buffer.lookup_transform(
                        self.output_frame,
                        scan.header.frame_id,
                        Time(),
                        timeout=Duration(seconds=0.05),
                    )
                t = tf.transform.translation
                tx = float(t.x)
                ty = float(t.y)
                q = tf.transform.rotation
                siny_cosp = 2.0 * (q.w * q.z + q.x * q.y)
                cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
                yaw = math.atan2(siny_cosp, cosy_cosp)
        except TransformException as ex:
            # Use topic-role fallback so we don't drop an entire lidar due to frame-name mismatch.
            tx, ty, yaw = self._fallback_mount(sensor)
            self.get_logger().warn(
                f'Using fallback pose for {sensor} lidar frame={scan.header.frame_id}: {ex}',
                throttle_duration_sec=2.0,
            )

        cy = math.cos(yaw)
        sy = math.sin(yaw)

        count = 0
        angle = float(scan.angle_min)
        angle_inc = float(scan.angle_increment)
        prev_projected = None
        for i, r in enumerate(scan.ranges):
            rng = float(r)
            if (not math.isfinite(rng)) or (rng <= float(scan.range_min)) or (rng >= float(scan.range_max)):
                angle += angle_inc
                continue

            # Point in lidar frame
            lx = rng * math.cos(angle)
            ly = rng * math.sin(angle)

            # Transform to base_link (rotation + translation)
            bx = cy * lx - sy * ly + tx
            by = sy * lx + cy * ly + ty

            br = math.hypot(bx, by)
            if (br <= float(merged.range_min)) or (br >= float(merged.range_max)):
                angle += angle_inc
                continue

            ba = math.atan2(by, bx)
            intensity = float(scan.intensities[i]) if i < len(scan.intensities) else 0.0
            if cloud_points is not None:
                cloud_points.append((float(bx), float(by), 0.0, float(intensity)))
            if not self._sensor_owns_angle(sensor, ba):
                angle += angle_inc
                continue
            merged_idx = int(round((ba - float(merged.angle_min)) / float(merged.angle_increment)))

            if self._write_bin_if_closer(merged, merged_idx, br, intensity):
                count += 1

            count += self._fill_beam_footprint(
                merged,
                merged_idx=merged_idx,
                base_angle=ba,
                base_range=br,
                intensity=intensity,
                lidar_yaw=yaw,
                tx=tx,
                ty=ty,
                raw_angle=angle,
                raw_range=rng,
                raw_angle_inc=angle_inc,
            )

            if (
                self.angular_gap_fill_enabled
                and self.angular_gap_fill_max_bins > 0
                and prev_projected is not None
            ):
                prev_idx = int(prev_projected['idx'])
                idx_delta = int(merged_idx) - prev_idx
                gap_bins = abs(idx_delta) - 1
                range_delta = abs(float(br) - float(prev_projected['range']))
                if (
                    gap_bins > 0
                    and gap_bins <= int(self.angular_gap_fill_max_bins)
                    and range_delta <= float(self.angular_gap_fill_max_range_delta_m)
                ):
                    step = 1 if idx_delta > 0 else -1
                    total_steps = gap_bins + 1
                    for offset in range(1, gap_bins + 1):
                        blend = float(offset) / float(total_steps)
                        interp_range = ((1.0 - blend) * float(prev_projected['range'])) + (blend * float(br))
                        interp_intensity = ((1.0 - blend) * float(prev_projected['intensity'])) + (blend * intensity)
                        interp_idx = prev_idx + (step * offset)
                        if self._write_bin_if_closer(merged, interp_idx, interp_range, interp_intensity):
                            count += 1

            if 0 <= merged_idx < num_points:
                prev_projected = {
                    'idx': int(merged_idx),
                    'range': float(br),
                    'intensity': float(intensity),
                }

            angle += angle_inc

        return count


def main(args=None):
    rclpy.init(args=args)
    node = ScanMerger()
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
