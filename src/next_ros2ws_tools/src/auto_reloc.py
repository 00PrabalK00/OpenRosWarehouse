#!/usr/bin/env python3
"""
Improved Correlative Scan-Matching Relocalization (Keepout-aware)

Key improvements:
1. Adaptive multi-resolution sampling
2. Robust M-estimator scoring
3. Particle filter refinement
4. Better ambiguity detection
5. Adaptive thresholding
6. Keepout-zone avoidance (never accept / sample candidates in keepout mask)
"""
import math
import threading
import time
from collections import deque

import cv2
import numpy as np
import rclpy
import tf2_ros
from geometry_msgs.msg import PoseWithCovarianceStamped
from nav_msgs.msg import OccupancyGrid
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from sensor_msgs.msg import LaserScan
from std_srvs.srv import Trigger
from tf2_ros import TransformException


# LiDAR drivers typically publish BEST_EFFORT; match to avoid silent subscription mismatch.
_SENSOR_QOS = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)


def yaw_to_quat(yaw: float):
    half = yaw * 0.5
    return (0.0, 0.0, math.sin(half), math.cos(half))


def tf_to_xy_yaw(tf):
    tx = tf.transform.translation.x
    ty = tf.transform.translation.y
    q = tf.transform.rotation
    siny_cosp = 2.0 * (q.w * q.z + q.x * q.y)
    cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
    yaw = math.atan2(siny_cosp, cosy_cosp)
    return tx, ty, yaw


def pose_msg_to_xy_yaw(msg: PoseWithCovarianceStamped):
    pose = msg.pose.pose
    x = float(pose.position.x)
    y = float(pose.position.y)
    q = pose.orientation
    siny_cosp = 2.0 * (q.w * q.z + q.x * q.y)
    cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
    yaw = math.atan2(siny_cosp, cosy_cosp)
    return x, y, yaw


def stamp_to_sec(stamp):
    return float(stamp.sec) + float(stamp.nanosec) * 1e-9


def norm_angle(a: float) -> float:
    return math.atan2(math.sin(a), math.cos(a))


def shortest_angle(from_a: float, to_a: float) -> float:
    return norm_angle(to_a - from_a)


def se2_compose(a, b):
    """Compose 2D transforms: a(target<-mid) o b(mid<-src) => target<-src."""
    ax, ay, atheta = a
    bx, by, btheta = b
    c, s = math.cos(atheta), math.sin(atheta)
    x = ax + c * bx - s * by
    y = ay + s * bx + c * by
    return x, y, norm_angle(atheta + btheta)


def se2_inverse(t):
    """Invert 2D transform (target<-source) => (source<-target)."""
    tx, ty, theta = t
    c, s = math.cos(theta), math.sin(theta)
    ix = -c * tx - s * ty
    iy = s * tx - c * ty
    return ix, iy, norm_angle(-theta)


class ImprovedCorrelativeRelocalizer(Node):
    def __init__(self):
        super().__init__('auto_reloc')

        # --- Core Parameters ---
        self.declare_parameter('map_topic', '/map')
        self.declare_parameter('scan_topic', '/scan')
        self.declare_parameter('scan2_topic', '/scan2')
        self.declare_parameter('base_frame', 'base_link')

        # --- Keepout mask (Nav2 costmap filter mask) ---
        self.declare_parameter('keepout_mask_topic', '/keepout_filter_mask')
        self.declare_parameter('enforce_keepout_mask', True)
        self.declare_parameter('require_keepout_mask', False)  # if True and no mask yet -> reject relocalize
        self.declare_parameter('keepout_value_threshold', 50)  # values >= threshold treated as keepout
        self.declare_parameter('keepout_unknown_is_keepout', False)  # -1 cells
        self.declare_parameter('keepout_outside_is_keepout', False)  # outside mask bounds
        self.declare_parameter('skip_keepout_in_sampling', True)  # avoid sampling candidates in keepout
        self.declare_parameter('robot_radius_m', 0.35)  # conservative footprint model (circle)
        self.declare_parameter('keepout_clearance_m', 0.05)  # extra margin beyond radius
        self.declare_parameter('keepout_footprint_samples', 16)  # circle samples around robot
        # Safety: when validation fails, reject by default instead of forcing acceptance.
        # Set to true only for explicit recovery scenarios confirmed by a human operator.
        self.declare_parameter('force_accept_on_validation_failure', False)

        # Lidar transforms
        self.declare_parameter('front_lidar_x', 0.35)
        self.declare_parameter('front_lidar_y', 0.0)
        self.declare_parameter('front_lidar_yaw', math.pi)
        self.declare_parameter('rear_lidar_x', -0.35)
        self.declare_parameter('rear_lidar_y', 0.0)
        self.declare_parameter('rear_lidar_yaw', 0.0)
        self.declare_parameter('front_scan_frame', 'laser_front_frame')
        self.declare_parameter('rear_scan_frame', 'laser_rear_frame')
        self.declare_parameter('allow_manual_lidar_fallback', False)

        # Scan processing
        self.declare_parameter('scan_downsample', 3)  # Reduced from 4 for more points
        self.declare_parameter('max_points', 120)  # Increased from 90
        self.declare_parameter('min_scan_points', 30)  # Reduced from 40
        self.declare_parameter('max_scan_time_diff_sec', 0.20)
        self.declare_parameter('use_scan_bundle', True)
        self.declare_parameter('bundle_scan_count', 6)
        self.declare_parameter('bundle_window_sec', 1.2)
        self.declare_parameter('bundle_sync_slop_sec', 0.10)
        self.declare_parameter('bundle_max_points', 360)
        self.declare_parameter('bundle_motion_compensate_odom', True)
        self.declare_parameter('odom_frame', 'odom')

        # Map processing
        self.declare_parameter('occupied_threshold', 65)

        # Multi-resolution sampling
        self.declare_parameter('use_multi_resolution', True)
        self.declare_parameter('coarse_samples', 800)  # Coarse grid
        self.declare_parameter('medium_samples', 400)  # Medium grid around top candidates
        self.declare_parameter('fine_samples', 200)    # Fine grid around best
        self.declare_parameter('medium_radius_m', 3.0)
        self.declare_parameter('fine_radius_m', 1.0)

        # Scoring
        self.declare_parameter('use_robust_scoring', True)
        self.declare_parameter('sigma', 0.18)  # Slightly tighter
        self.declare_parameter('huber_delta', 0.10)  # Robust M-estimator threshold
        self.declare_parameter('unknown_penalty_exp', 1.5)  # Reduced from 2.0
        self.declare_parameter('distance_clip_m', 1.5)
        self.declare_parameter('sample_near_obstacles_dist_m', 1.5)

        # Yaw search pruning
        self.declare_parameter('use_pca_yaw_pruning', False)
        self.declare_parameter('pca_yaw_window_deg', 90.0)

        # Yaw search
        self.declare_parameter('coarse_yaw_steps', 48)  # Increased from 36
        self.declare_parameter('medium_yaw_steps', 24)
        self.declare_parameter('fine_yaw_steps', 13)
        self.declare_parameter('fine_yaw_span_deg', 10.0)

        # Particle refinement
        self.declare_parameter('use_particle_refinement', True)
        self.declare_parameter('num_particles', 50)
        self.declare_parameter('particle_xy_std', 0.15)
        self.declare_parameter('particle_yaw_std_deg', 8.0)
        self.declare_parameter('particle_iterations', 3)

        # Validation (adaptive)
        self.declare_parameter('min_accept_score', 0.0)  # Accept any score
        self.declare_parameter('min_known_ratio', 0.0)  # Accept any known ratio
        self.declare_parameter('min_score_margin', 0.0)  # Accept any margin
        self.declare_parameter('max_mean_dist_m', 1e6)  # Effectively unlimited
        self.declare_parameter('min_hit_ratio', 0.0)  # Accept any hit ratio
        self.declare_parameter('use_adaptive_thresholds', True)
        # Optional margin leniency when scan geometry is arc/curve-dominant.
        # Straight-line dominant scans keep original strict thresholds.
        self.declare_parameter('curve_edge_margin_relax_enabled', True)
        self.declare_parameter('curve_edge_ratio_start', 0.30)
        self.declare_parameter('curve_edge_ratio_full', 0.65)
        self.declare_parameter('curve_edge_margin_relax_max', 0.70)
        # If the only failing gate is score margin but clustering says unambiguous,
        # allow acceptance (optional score floor via margin_bypass_min_score).
        self.declare_parameter('allow_unambiguous_margin_bypass', True)
        self.declare_parameter('margin_bypass_min_score', 0.0)

        # Ambiguity detection (improved)
        self.declare_parameter('ambiguity_distance_m', 1.5)  # Increased
        self.declare_parameter('min_far_score_margin', 0.050)
        self.declare_parameter('use_spatial_clustering', True)
        self.declare_parameter('cluster_eps', 0.8)

        # Local prior search
        self.declare_parameter('use_local_prior_search', True)
        self.declare_parameter('local_search_radius_m', 3.0)  # Increased
        self.declare_parameter('local_yaw_span_deg', 70.0)  # Increased
        self.declare_parameter('local_min_accept_score', 0.0)
        self.declare_parameter('local_min_known_ratio', 0.0)
        self.declare_parameter('require_good_prior_for_local_search', True)
        self.declare_parameter('local_prior_max_age_sec', 8.0)
        self.declare_parameter('local_prior_max_cov_sum', 1.5)
        self.declare_parameter('amcl_pose_topic', '/amcl_pose')
        self.declare_parameter('initial_pose_topic', '/initialpose')

        # Startup behavior
        self.declare_parameter('auto_relocate_on_startup', False)
        self.declare_parameter('startup_delay_sec', 4.0)
        self.declare_parameter('startup_spin_before_match', False)
        self.declare_parameter('startup_spin_speed', 0.25)
        self.declare_parameter('startup_spin_duration_sec', 2.0)
        self.declare_parameter('startup_spin_settle_sec', 0.25)

        # Subscriptions and publishers
        self.map_sub = self.create_subscription(
            OccupancyGrid,
            self.get_parameter('map_topic').value,
            self.map_cb,
            1,
        )
        self.keepout_sub = self.create_subscription(
            OccupancyGrid,
            self.get_parameter('keepout_mask_topic').value,
            self.keepout_cb,
            1,
        )
        self.scan_sub = self.create_subscription(
            LaserScan,
            self.get_parameter('scan_topic').value,
            self.scan_cb,
            _SENSOR_QOS,
        )
        self.scan2_sub = self.create_subscription(
            LaserScan,
            self.get_parameter('scan2_topic').value,
            self.scan2_cb,
            _SENSOR_QOS,
        )
        self.amcl_pose_sub = self.create_subscription(
            PoseWithCovarianceStamped,
            str(self.get_parameter('amcl_pose_topic').value),
            self.amcl_pose_cb,
            10,
        )
        self.initial_pose_sub = self.create_subscription(
            PoseWithCovarianceStamped,
            str(self.get_parameter('initial_pose_topic').value),
            self.initial_pose_cb,
            10,
        )

        self.initpose_pub = self.create_publisher(PoseWithCovarianceStamped, '/initialpose', 10)
        self.srv = self.create_service(Trigger, '/auto_relocate', self.handle_trigger)

        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self)

        # State
        self.map_msg = None
        self.dist_m = None
        self.gradient_x = None
        self.gradient_y = None
        self.free_mask = None
        self.known_mask = None

        self.keepout_msg = None
        self.keepout_grid = None  # np.int16 [h,w]
        self.keepout_info = None
        self._warned_keepout_missing = False

        self.scan = None
        self.scan2 = None
        self.scan_history = deque(maxlen=60)
        self.scan2_history = deque(maxlen=60)
        self.last_amcl_cov_sum = None
        self.last_amcl_pose_sec = None
        self.last_local_prior = None
        self._last_scan_curve_relax = 0.0
        self._last_scan_minor_major_ratio = 0.0

        self._startup_timer = None
        self.rng = np.random.default_rng()

        self.get_logger().info('Improved correlative relocalizer ready: call /auto_relocate')

        if bool(self.get_parameter('auto_relocate_on_startup').value):
            startup_delay = max(0.1, float(self.get_parameter('startup_delay_sec').value))
            self._startup_timer = self.create_timer(startup_delay, self._startup_relocalize_once)
            self.get_logger().info(
                f'Auto relocalization on startup enabled (delay={startup_delay:.2f}s)'
            )

    # ---------------------------
    # Keepout utilities
    # ---------------------------
    def keepout_cb(self, msg: OccupancyGrid):
        self.keepout_msg = msg
        w, h = msg.info.width, msg.info.height
        try:
            grid = np.array(msg.data, dtype=np.int16).reshape((h, w))
        except Exception as exc:
            self.get_logger().warn(f'Keepout mask reshape failed: {exc}')
            return
        self.keepout_grid = grid
        self.keepout_info = msg.info

    def _keepout_ready(self) -> bool:
        return self.keepout_grid is not None and self.keepout_info is not None

    def _world_to_keepout_ij(self, wx: float, wy: float):
        """
        Map world -> keepout grid (ix, iy, in_bounds).
        """
        if not self._keepout_ready():
            return 0, 0, False
        res = float(self.keepout_info.resolution)
        ox = float(self.keepout_info.origin.position.x)
        oy = float(self.keepout_info.origin.position.y)
        w = int(self.keepout_info.width)
        h = int(self.keepout_info.height)

        ix = int(math.floor((wx - ox) / res))
        iy = int(math.floor((wy - oy) / res))
        inb = (0 <= ix < w) and (0 <= iy < h)
        return ix, iy, inb

    def _is_keepout_cell_value(self, v: int) -> bool:
        """
        Interpret keepout mask cell value.
        Typical masks are 0 for free, 100 for keepout, and -1 for unknown.
        """
        thr = int(self.get_parameter('keepout_value_threshold').value)
        unknown_is_keepout = bool(self.get_parameter('keepout_unknown_is_keepout').value)
        if v < 0:
            return unknown_is_keepout
        return v >= thr

    def _is_keepout_world(self, wx: float, wy: float) -> bool:
        """
        True if point is in keepout.
        """
        if not bool(self.get_parameter('enforce_keepout_mask').value):
            return False
        if not self._keepout_ready():
            # If required, treat as keepout to force failure upstream; otherwise safe.
            return bool(self.get_parameter('keepout_outside_is_keepout').value) and bool(
                self.get_parameter('require_keepout_mask').value
            )

        ix, iy, inb = self._world_to_keepout_ij(wx, wy)
        if not inb:
            return bool(self.get_parameter('keepout_outside_is_keepout').value)
        v = int(self.keepout_grid[iy, ix])
        return self._is_keepout_cell_value(v)

    def _pose_hits_keepout(self, x: float, y: float, yaw: float = 0.0) -> bool:
        """
        Conservative footprint check: circle model (robot_radius + clearance).
        yaw is unused for circle, kept for API symmetry.
        """
        if not bool(self.get_parameter('enforce_keepout_mask').value):
            return False

        if not self._keepout_ready():
            # if require_keepout_mask, we'll fail earlier in handle_trigger
            return False

        r = float(self.get_parameter('robot_radius_m').value)
        clearance = float(self.get_parameter('keepout_clearance_m').value)
        rad = max(0.01, r + clearance)

        # Center
        if self._is_keepout_world(x, y):
            return True

        n = int(self.get_parameter('keepout_footprint_samples').value)
        n = max(4, min(64, n))
        for k in range(n):
            a = (2.0 * math.pi * k) / float(n)
            px = x + rad * math.cos(a)
            py = y + rad * math.sin(a)
            if self._is_keepout_world(px, py):
                return True
        return False

    def _filter_candidates_keepout(self, candidates: list, max_check: int = 5000):
        """
        Return candidates that do NOT intersect keepout, preserving score order.
        To limit CPU, only checks up to max_check entries (after that, returns what it has).
        """
        if not bool(self.get_parameter('enforce_keepout_mask').value):
            return candidates
        if not self._keepout_ready():
            return candidates

        out = []
        checked = 0
        for c in candidates:
            checked += 1
            if checked > max_check:
                break
            if not self._pose_hits_keepout(float(c['x']), float(c['y']), float(c.get('yaw', 0.0))):
                out.append(c)
        return out

    # ---------------------------
    # Map + scan callbacks
    # ---------------------------
    def map_cb(self, msg: OccupancyGrid):
        self.map_msg = msg
        w, h = msg.info.width, msg.info.height
        data = np.array(msg.data, dtype=np.int16).reshape((h, w))

        occ_thresh = int(self.get_parameter('occupied_threshold').value)
        known = (data >= 0)
        free = (data == 0).astype(np.uint8)
        occupied = (data >= occ_thresh)

        if not np.any(occupied):
            self.get_logger().warn('Map has no occupied cells; cannot relocalize yet.')
            self.dist_m = None
            self.free_mask = free
            self.known_mask = known
            return

        # Distance field
        inv_occ = (~occupied).astype(np.uint8)
        dist_px = cv2.distanceTransform(inv_occ, cv2.DIST_L2, 5)
        if dist_px is None:
            self.get_logger().warn('distanceTransform failed')
            return

        self.dist_m = dist_px * float(msg.info.resolution)
        self.free_mask = free
        self.known_mask = known

        # Compute gradients for alignment scoring
        grad_y, grad_x = np.gradient(self.dist_m)
        self.gradient_x = grad_x.astype(np.float32)
        self.gradient_y = grad_y.astype(np.float32)

    def scan_cb(self, msg: LaserScan):
        self.scan = msg
        self.scan_history.append(msg)

    def scan2_cb(self, msg: LaserScan):
        self.scan2 = msg
        self.scan2_history.append(msg)

    def amcl_pose_cb(self, msg: PoseWithCovarianceStamped):
        try:
            cov = list(msg.pose.covariance)
            cov_sum = float(cov[0]) + float(cov[7]) + float(cov[35])
            x, y, yaw = pose_msg_to_xy_yaw(msg)
            received_sec = time.time()
            self.last_amcl_cov_sum = cov_sum
            self.last_amcl_pose_sec = received_sec
            self._store_local_prior('amcl', x, y, yaw, cov_sum, received_sec=received_sec)
        except Exception:
            pass

    def initial_pose_cb(self, msg: PoseWithCovarianceStamped):
        try:
            x, y, yaw = pose_msg_to_xy_yaw(msg)
            cov = list(msg.pose.covariance)
            cov_sum = float(cov[0]) + float(cov[7]) + float(cov[35])
            self._store_local_prior('initialpose', x, y, yaw, cov_sum)
        except Exception:
            pass

    # ---------------------------
    # Startup helpers
    # ---------------------------
    def _startup_relocalize_once(self):
        if self._startup_timer is not None:
            self._startup_timer.cancel()
            self._startup_timer = None
        threading.Thread(target=self._startup_relocalize_worker, daemon=True).start()

    def _startup_relocalize_worker(self):
        try:
            if bool(self.get_parameter('startup_spin_before_match').value):
                self._spin_robot_for_bundle()
            req = Trigger.Request()
            resp = Trigger.Response()
            result = self.handle_trigger(req, resp)
            if bool(result.success):
                self.get_logger().info(f'Startup relocalization succeeded: {result.message}')
            else:
                self.get_logger().warn(f'Startup relocalization failed: {result.message}')
        except Exception as exc:
            self.get_logger().error(f'Startup relocalization crashed: {exc}')

    def _spin_robot_for_bundle(self):
        speed = float(self.get_parameter('startup_spin_speed').value)
        duration = float(self.get_parameter('startup_spin_duration_sec').value)
        settle = max(0.0, float(self.get_parameter('startup_spin_settle_sec').value))
        if duration <= 0.0 or abs(speed) <= 1e-4:
            return

        self.get_logger().warn(
            'Startup relocalization spin is disabled: auto_reloc no longer '
            'publishes cmd_vel. Proceeding without pre-spin scan collection.'
        )
        if settle > 0.0:
            time.sleep(settle)

    # ---------------------------
    # TF helpers + scan processing
    # ---------------------------
    @staticmethod
    def _msg_time_sec(scan: LaserScan) -> float:
        return stamp_to_sec(scan.header.stamp)

    @staticmethod
    def _transform_points_xy_yaw(pts: np.ndarray, tx: float, ty: float, yaw: float) -> np.ndarray:
        if pts.shape[0] == 0:
            return pts
        c, s = math.cos(yaw), math.sin(yaw)
        rot = np.array([[c, -s], [s, c]], dtype=np.float32)
        out = (pts @ rot.T) + np.array([tx, ty], dtype=np.float32)
        return out.astype(np.float32)

    @staticmethod
    def _cap_points(pts: np.ndarray, max_points: int) -> np.ndarray:
        if pts.shape[0] == 0:
            return pts
        if max_points <= 0 or pts.shape[0] <= max_points:
            return pts.astype(np.float32)
        idx = np.linspace(0, pts.shape[0] - 1, max_points, dtype=int)
        return pts[idx].astype(np.float32)

    def _recent_scans(self, history: deque, max_count: int, window_sec: float):
        items = list(history)
        if not items:
            return []
        max_count = max(1, int(max_count))
        window_sec = max(0.01, float(window_sec))
        latest_t = self._msg_time_sec(items[-1])
        out = []
        for scan in reversed(items):
            dt = latest_t - self._msg_time_sec(scan)
            if dt < 0.0:
                continue
            if dt > window_sec:
                break
            out.append(scan)
            if len(out) >= max_count:
                break
        out.reverse()
        return out

    def _nearest_scan_by_time(self, history: deque, target_t: float, slop_sec: float):
        items = list(history)
        if not items:
            return None
        slop_sec = max(0.0, float(slop_sec))
        best = None
        best_dt = float('inf')
        for scan in reversed(items):
            dt = abs(self._msg_time_sec(scan) - target_t)
            if dt < best_dt:
                best = scan
                best_dt = dt
            if self._msg_time_sec(scan) < (target_t - slop_sec):
                break
        if best is None or best_dt > slop_sec:
            return None
        return best

    @staticmethod
    def _stamp_to_time(msg_stamp):
        try:
            return rclpy.time.Time.from_msg(msg_stamp)
        except Exception:
            return rclpy.time.Time()

    def _lookup_se2(self, target_frame: str, source_frame: str, msg_stamp):
        tf = self.tf_buffer.lookup_transform(
            target_frame,
            source_frame,
            self._stamp_to_time(msg_stamp),
        )
        return tf_to_xy_yaw(tf)

    def _store_local_prior(
        self,
        source: str,
        x: float,
        y: float,
        yaw: float,
        cov_sum: float = None,
        *,
        received_sec: float = None,
    ):
        self.last_local_prior = {
            'source': str(source or 'prior'),
            'x': float(x),
            'y': float(y),
            'yaw': float(yaw),
            'cov_sum': None if cov_sum is None else float(cov_sum),
            'received_sec': float(received_sec if received_sec is not None else time.time()),
        }

    def _relative_base_delta_from_odom(self, src_stamp, ref_stamp):
        """Return base_ref <- base_src using odom as common frame."""
        odom_frame = str(self.get_parameter('odom_frame').value)
        base_frame = str(self.get_parameter('base_frame').value)
        t_odom_base_src = self._lookup_se2(odom_frame, base_frame, src_stamp)
        t_odom_base_ref = self._lookup_se2(odom_frame, base_frame, ref_stamp)
        return se2_compose(se2_inverse(t_odom_base_ref), t_odom_base_src)

    def _local_prior_status(self):
        if not bool(self.get_parameter('require_good_prior_for_local_search').value):
            return True, self.last_local_prior, 'quality gate disabled'

        prior = self.last_local_prior
        if not isinstance(prior, dict):
            return False, None, 'no recent AMCL or /initialpose prior received'

        max_age = max(0.1, float(self.get_parameter('local_prior_max_age_sec').value))
        max_cov = max(0.0, float(self.get_parameter('local_prior_max_cov_sum').value))
        age = max(0.0, time.time() - float(prior.get('received_sec', 0.0)))
        source = str(prior.get('source', 'prior') or 'prior')
        cov_sum = prior.get('cov_sum')

        if age > max_age:
            return False, prior, f'{source} prior too old ({age:.2f}s > {max_age:.2f}s)'
        if cov_sum is not None and float(cov_sum) > max_cov:
            return False, prior, f'{source} prior covariance too high ({float(cov_sum):.3f} > {max_cov:.3f})'

        cov_text = 'n/a' if cov_sum is None else f'{float(cov_sum):.3f}'
        return True, prior, f'{source} prior accepted (age={age:.2f}s, cov={cov_text})'

    def _is_local_prior_good(self):
        ok, _, _ = self._local_prior_status()
        return ok

    def _current_prior_pose(self, prior=None):
        base_frame = str(self.get_parameter('base_frame').value)
        try:
            tf = self.tf_buffer.lookup_transform('map', base_frame, rclpy.time.Time())
            x, y, yaw = tf_to_xy_yaw(tf)
            return x, y, yaw, f'tf(map->{base_frame})'
        except TransformException:
            if isinstance(prior, dict):
                return (
                    float(prior.get('x', 0.0)),
                    float(prior.get('y', 0.0)),
                    float(prior.get('yaw', 0.0)),
                    str(prior.get('source', 'stored_prior') or 'stored_prior'),
                )
            raise

    def _estimate_scan_axis_yaw(self, pts_b: np.ndarray):
        if pts_b is None or pts_b.shape[0] < 3:
            return None
        centered = pts_b - np.mean(pts_b, axis=0, keepdims=True)
        cov = np.cov(centered.T)
        if cov.shape != (2, 2):
            return None
        vals, vecs = np.linalg.eigh(cov)
        if not np.isfinite(vals).all():
            return None
        axis = vecs[:, int(np.argmax(vals))]
        return math.atan2(float(axis[1]), float(axis[0]))

    def _prune_yaw_candidates(self, yaw_list: np.ndarray, axis_yaw: float):
        if axis_yaw is None:
            return yaw_list
        if not bool(self.get_parameter('use_pca_yaw_pruning').value):
            return yaw_list
        half = math.radians(float(self.get_parameter('pca_yaw_window_deg').value))
        if half >= math.pi:
            return yaw_list
        alt = norm_angle(axis_yaw + math.pi)
        kept = []
        for yaw in yaw_list:
            d0 = abs(shortest_angle(axis_yaw, float(yaw)))
            d1 = abs(shortest_angle(alt, float(yaw)))
            if min(d0, d1) <= half:
                kept.append(float(yaw))
        if len(kept) < 8:
            return yaw_list
        return np.array(kept, dtype=np.float32)

    def scan_to_points_in_frame(
        self,
        scan: LaserScan,
        target_frame: str,
        fallback_laser_frame: str = '',
    ):
        """Convert scan points to any target frame with timestamped TF lookup."""
        if scan is None:
            return np.zeros((0, 2), dtype=np.float32)

        laser_frame = str(scan.header.frame_id or fallback_laser_frame or '').strip()
        if not laser_frame:
            return np.zeros((0, 2), dtype=np.float32)

        lookup_time = rclpy.time.Time()
        try:
            lookup_time = rclpy.time.Time.from_msg(scan.header.stamp)
        except Exception:
            lookup_time = rclpy.time.Time()

        try:
            tf = self.tf_buffer.lookup_transform(target_frame, laser_frame, lookup_time)
            tx, ty, tyaw = tf_to_xy_yaw(tf)
        except TransformException as exc:
            base_frame = str(self.get_parameter('base_frame').value)
            allow_fallback = bool(self.get_parameter('allow_manual_lidar_fallback').value)
            if allow_fallback and target_frame == base_frame:
                front_frame = str(self.get_parameter('front_scan_frame').value)
                rear_frame = str(self.get_parameter('rear_scan_frame').value)
                if laser_frame == front_frame:
                    tx = float(self.get_parameter('front_lidar_x').value)
                    ty = float(self.get_parameter('front_lidar_y').value)
                    tyaw = float(self.get_parameter('front_lidar_yaw').value)
                elif laser_frame == rear_frame:
                    tx = float(self.get_parameter('rear_lidar_x').value)
                    ty = float(self.get_parameter('rear_lidar_y').value)
                    tyaw = float(self.get_parameter('rear_lidar_yaw').value)
                else:
                    self.get_logger().warn(
                        f'TF {target_frame}<-{laser_frame} unavailable; fallback not configured for this frame: {exc}'
                    )
                    return np.zeros((0, 2), dtype=np.float32)
            else:
                self.get_logger().warn(f'TF {target_frame}<-{laser_frame} not available: {exc}')
                return np.zeros((0, 2), dtype=np.float32)

        ds = int(self.get_parameter('scan_downsample').value)
        max_pts = int(self.get_parameter('max_points').value)

        angles = scan.angle_min + np.arange(len(scan.ranges)) * scan.angle_increment
        ranges = np.array(scan.ranges, dtype=np.float32)

        valid = np.isfinite(ranges) & (ranges > scan.range_min) & (ranges < scan.range_max)
        max_r = float(scan.range_max)
        if math.isfinite(max_r) and max_r > 0.0:
            # Drop near-max returns that are typically weak/noisy for map matching.
            valid = valid & (ranges < (0.98 * max_r))
        idx = np.nonzero(valid)[0][::max(1, ds)]
        if idx.size == 0:
            return np.zeros((0, 2), dtype=np.float32)
        if idx.size > max_pts:
            idx = idx[np.linspace(0, idx.size - 1, max_pts, dtype=int)]

        a = angles[idx]
        r = ranges[idx]
        pts_src = np.stack([r * np.cos(a), r * np.sin(a)], axis=1)
        return self._transform_points_xy_yaw(pts_src.astype(np.float32), tx, ty, tyaw)

    def scan_to_points_base(self, scan: LaserScan, laser_frame: str):
        """Convert scan to points in base_link frame."""
        base_frame = str(self.get_parameter('base_frame').value)
        return self.scan_to_points_in_frame(scan, base_frame, fallback_laser_frame=laser_frame)

    def build_scan_points(self):
        """Build scan points for matching (single-scan or short temporal bundle)."""
        use_bundle = bool(self.get_parameter('use_scan_bundle').value)
        bundle_max_points = max(30, int(self.get_parameter('bundle_max_points').value))

        # Baseline mode: latest scan(s) only.
        if not use_bundle:
            pts1 = self.scan_to_points_base(
                self.scan,
                self.scan.header.frame_id if self.scan is not None else '',
            )
            pts2 = self.scan_to_points_base(
                self.scan2,
                self.scan2.header.frame_id if self.scan2 is not None else '',
            )
            if pts2.shape[0] > 0:
                pts = np.vstack([pts1, pts2]) if pts1.shape[0] > 0 else pts2
            else:
                pts = pts1
            return self._cap_points(pts, bundle_max_points), {
                'front_scans': 1,
                'rear_scans': 1 if pts2.shape[0] > 0 else 0
            }

        bundle_scan_count = max(1, int(self.get_parameter('bundle_scan_count').value))
        bundle_window_sec = max(0.05, float(self.get_parameter('bundle_window_sec').value))
        bundle_sync_slop_sec = max(0.0, float(self.get_parameter('bundle_sync_slop_sec').value))
        odom_comp = bool(self.get_parameter('bundle_motion_compensate_odom').value)
        base_frame = str(self.get_parameter('base_frame').value)
        odom_frame = str(self.get_parameter('odom_frame').value)

        front_scans = self._recent_scans(self.scan_history, bundle_scan_count, bundle_window_sec)
        if not front_scans and self.scan is not None:
            front_scans = [self.scan]
        if not front_scans:
            return np.zeros((0, 2), dtype=np.float32), {'front_scans': 0, 'rear_scans': 0}

        ref_stamp_msg = front_scans[-1].header.stamp
        if odom_comp:
            try:
                # Probe TF availability once at ref time.
                self._lookup_se2(odom_frame, base_frame, ref_stamp_msg)
            except TransformException as exc:
                self.get_logger().warn(
                    f'Bundle odom compensation disabled (missing TF {odom_frame}<-{base_frame}): {exc}'
                )
                odom_comp = False

        all_pts = []
        front_used = 0
        rear_used = 0

        for front_scan in front_scans:
            if odom_comp:
                pts_front = self.scan_to_points_base(front_scan, front_scan.header.frame_id)
                try:
                    dtx, dty, dyaw = self._relative_base_delta_from_odom(
                        front_scan.header.stamp,
                        ref_stamp_msg,
                    )
                    pts_front = self._transform_points_xy_yaw(pts_front, dtx, dty, dyaw)
                except TransformException as exc:
                    self.get_logger().warn(
                        f'Bundle odom compensation disabled (front scan TF failure): {exc}'
                    )
                    odom_comp = False
            else:
                pts_front = self.scan_to_points_base(front_scan, front_scan.header.frame_id)
            if pts_front.shape[0] > 0:
                all_pts.append(pts_front)
                front_used += 1

            rear_scan = self._nearest_scan_by_time(
                self.scan2_history,
                self._msg_time_sec(front_scan),
                bundle_sync_slop_sec,
            )
            if rear_scan is None:
                continue

            if odom_comp:
                pts_rear = self.scan_to_points_base(rear_scan, rear_scan.header.frame_id)
                try:
                    dtx, dty, dyaw = self._relative_base_delta_from_odom(
                        rear_scan.header.stamp,
                        ref_stamp_msg,
                    )
                    pts_rear = self._transform_points_xy_yaw(pts_rear, dtx, dty, dyaw)
                except TransformException as exc:
                    self.get_logger().warn(
                        f'Bundle odom compensation disabled (rear scan TF failure): {exc}'
                    )
                    odom_comp = False
            else:
                pts_rear = self.scan_to_points_base(rear_scan, rear_scan.header.frame_id)
            if pts_rear.shape[0] > 0:
                all_pts.append(pts_rear)
                rear_used += 1

        if not all_pts:
            return np.zeros((0, 2), dtype=np.float32), {'front_scans': 0, 'rear_scans': 0}

        merged = np.vstack(all_pts).astype(np.float32)
        return self._cap_points(merged, bundle_max_points), {'front_scans': front_used, 'rear_scans': rear_used}

    # ---------------------------
    # Scoring
    # ---------------------------
    def score_pose_robust(self, pts_b: np.ndarray, x: float, y: float, yaw: float):
        """
        Robust M-estimator scoring for a single pose.
        Returns dict with score and diagnostic metrics.
        """
        if pts_b.shape[0] == 0:
            return None

        msg = self.map_msg
        dist = self.dist_m
        known_mask = self.known_mask
        h, w = dist.shape

        res = float(msg.info.resolution)
        ox = float(msg.info.origin.position.x)
        oy = float(msg.info.origin.position.y)

        # Rotate and translate points
        c, s = math.cos(yaw), math.sin(yaw)
        px = pts_b[:, 0]
        py = pts_b[:, 1]
        gx = x + c * px - s * py
        gy = y + s * px + c * py

        # Map to grid
        ix = np.floor((gx - ox) / res).astype(np.int32)
        iy = np.floor((gy - oy) / res).astype(np.int32)

        oob = (ix < 0) | (ix >= w) | (iy < 0) | (iy >= h)
        ix = np.clip(ix, 0, w - 1)
        iy = np.clip(iy, 0, h - 1)

        # Get distances
        d = dist[iy, ix]
        known = (~oob) & known_mask[iy, ix]

        # Robust scoring
        d_clip_m = max(0.05, float(self.get_parameter('distance_clip_m').value))
        d_eval = np.minimum(d, d_clip_m)
        if bool(self.get_parameter('use_robust_scoring').value):
            sigma = max(0.05, float(self.get_parameter('sigma').value))
            delta_m = max(res, float(self.get_parameter('huber_delta').value))

            # Huber loss in meters: quadratic near obstacles, linear for outliers.
            residuals = d_eval
            huber = np.where(
                residuals <= delta_m,
                0.5 * residuals ** 2,
                delta_m * (residuals - 0.5 * delta_m)
            )

            # Convert to likelihood (0 to 1)
            likelihood = np.exp(-huber / max(1e-6, sigma * sigma))
        else:
            # Original Gaussian
            sigma = max(0.05, float(self.get_parameter('sigma').value))
            likelihood = np.exp(-(d_eval * d_eval) / (2.0 * sigma * sigma))

        # Mask unknown
        likelihood = np.where(known, likelihood, 0.0)
        d_known = np.where(known, d, 0.0)

        known_count = known.sum()
        total_count = pts_b.shape[0]
        known_ratio = known_count / max(1.0, total_count)

        if known_count == 0:
            return {
                'score': 0.0,
                'known_ratio': 0.0,
                'mean_likelihood': 0.0,
                'mean_dist': 999.0,
                'hit_ratio': 0.0,
            }

        mean_likelihood = likelihood.sum() / known_count
        mean_dist = d_known.sum() / known_count

        hit_threshold = max(res, float(self.get_parameter('sigma').value) * 0.8)
        hit_count = ((d <= hit_threshold) & known).sum()
        hit_ratio = hit_count / known_count

        base_score = likelihood.sum() / max(1.0, float(known_count))
        unknown_exp = max(0.0, float(self.get_parameter('unknown_penalty_exp').value))
        score = base_score * (known_ratio ** unknown_exp)

        return {
            'score': float(score),
            'known_ratio': float(known_ratio),
            'mean_likelihood': float(mean_likelihood),
            'mean_dist': float(mean_dist),
            'hit_ratio': float(hit_ratio),
        }

    def score_candidates_batch(self, pts_b: np.ndarray, cand_xy: np.ndarray, yaw_list: np.ndarray):
        """Score multiple candidates and return top results"""
        if pts_b.shape[0] == 0 or cand_xy.shape[0] == 0:
            return []

        results = []
        for yaw in yaw_list:
            for i in range(cand_xy.shape[0]):
                x, y = float(cand_xy[i, 0]), float(cand_xy[i, 1])

                # Keepout rejection before scoring (cheap)
                if bool(self.get_parameter('enforce_keepout_mask').value) and self._keepout_ready():
                    if self._pose_hits_keepout(x, y, float(yaw)):
                        continue

                metrics = self.score_pose_robust(pts_b, x, y, yaw)
                if metrics is not None:
                    results.append({
                        'x': x,
                        'y': y,
                        'yaw': float(yaw),
                        **metrics
                    })

        # Sort by score
        results.sort(key=lambda r: r['score'], reverse=True)
        return results

    # ---------------------------
    # Search
    # ---------------------------
    def multi_resolution_search(self, pts_b: np.ndarray):
        """
        Multi-resolution search: coarse -> medium -> fine
        """
        if not bool(self.get_parameter('use_multi_resolution').value):
            # Fall back to single-resolution
            return self.single_resolution_search(pts_b)

        # Stage 1: Coarse global search
        coarse_cand = self._sample_free_space(int(self.get_parameter('coarse_samples').value))
        if coarse_cand is None or coarse_cand.shape[0] < 100:
            self.get_logger().warn('Insufficient coarse samples')
            return None

        coarse_yaw = np.linspace(
            -math.pi, math.pi,
            int(self.get_parameter('coarse_yaw_steps').value),
            endpoint=False
        )
        scan_axis_yaw = self._estimate_scan_axis_yaw(pts_b)
        coarse_yaw = self._prune_yaw_candidates(coarse_yaw, scan_axis_yaw)

        coarse_results = self.score_candidates_batch(pts_b, coarse_cand, coarse_yaw)
        if not coarse_results:
            return None

        self.get_logger().info(f'Coarse: top score={coarse_results[0]["score"]:.3f}')

        # Stage 2: Medium resolution around top candidates
        medium_radius = float(self.get_parameter('medium_radius_m').value)
        medium_samples = int(self.get_parameter('medium_samples').value)

        # Take top 10 from coarse
        top_coarse = coarse_results[:10]
        medium_cand = []
        for c in top_coarse:
            local = self._sample_around_pose(c['x'], c['y'], medium_radius, medium_samples // 10)
            if local is not None:
                medium_cand.append(local)

        if medium_cand:
            medium_cand = np.vstack(medium_cand)
            medium_yaw = np.linspace(
                -math.pi, math.pi,
                int(self.get_parameter('medium_yaw_steps').value),
                endpoint=False
            )
            medium_yaw = self._prune_yaw_candidates(medium_yaw, scan_axis_yaw)
            medium_results = self.score_candidates_batch(pts_b, medium_cand, medium_yaw)
            if medium_results and medium_results[0]['score'] > coarse_results[0]['score']:
                coarse_results = medium_results
                self.get_logger().info(f'Medium: improved to {coarse_results[0]["score"]:.3f}')

        # Stage 3: Fine refinement around best
        best = coarse_results[0]
        fine_radius = float(self.get_parameter('fine_radius_m').value)
        fine_samples = int(self.get_parameter('fine_samples').value)
        fine_cand = self._sample_around_pose(best['x'], best['y'], fine_radius, fine_samples)

        if fine_cand is not None:
            fine_yaw_span = math.radians(float(self.get_parameter('fine_yaw_span_deg').value))
            fine_yaw_steps = int(self.get_parameter('fine_yaw_steps').value)
            fine_yaw = np.linspace(
                best['yaw'] - fine_yaw_span,
                best['yaw'] + fine_yaw_span,
                fine_yaw_steps
            )
            fine_yaw = self._prune_yaw_candidates(fine_yaw, scan_axis_yaw)
            fine_results = self.score_candidates_batch(pts_b, fine_cand, fine_yaw)
            if fine_results and fine_results[0]['score'] > best['score']:
                coarse_results = fine_results + coarse_results
                coarse_results.sort(key=lambda r: r['score'], reverse=True)
                self.get_logger().info(f'Fine: improved to {coarse_results[0]["score"]:.3f}')

        return coarse_results

    def single_resolution_search(self, pts_b: np.ndarray):
        """Fallback: single resolution search"""
        cand = self._sample_free_space(1500)
        if cand is None:
            return None
        yaw_list = np.linspace(-math.pi, math.pi, 36, endpoint=False)
        yaw_list = self._prune_yaw_candidates(yaw_list, self._estimate_scan_axis_yaw(pts_b))
        return self.score_candidates_batch(pts_b, cand, yaw_list)

    # ---------------------------
    # Particle refinement
    # ---------------------------
    def particle_refinement(self, pts_b: np.ndarray, initial_pose: dict):
        """
        Particle filter refinement around initial pose.
        """
        if not bool(self.get_parameter('use_particle_refinement').value):
            return initial_pose

        num_particles = int(self.get_parameter('num_particles').value)
        xy_std = float(self.get_parameter('particle_xy_std').value)
        yaw_std = math.radians(float(self.get_parameter('particle_yaw_std_deg').value))
        iterations = int(self.get_parameter('particle_iterations').value)

        x0, y0, yaw0 = initial_pose['x'], initial_pose['y'], initial_pose['yaw']

        best_pose = initial_pose
        for _ in range(iterations):
            x_samples = np.random.normal(x0, xy_std, num_particles)
            y_samples = np.random.normal(y0, xy_std, num_particles)
            yaw_samples = np.random.normal(yaw0, yaw_std, num_particles)

            results = []
            for i in range(num_particles):
                x_i = float(x_samples[i])
                y_i = float(y_samples[i])
                yaw_i = float(yaw_samples[i])

                if bool(self.get_parameter('enforce_keepout_mask').value) and self._keepout_ready():
                    if self._pose_hits_keepout(x_i, y_i, yaw_i):
                        continue

                metrics = self.score_pose_robust(pts_b, x_i, y_i, yaw_i)
                if metrics is not None:
                    results.append({
                        'x': x_i,
                        'y': y_i,
                        'yaw': yaw_i,
                        **metrics
                    })

            if not results:
                break

            results.sort(key=lambda r: r['score'], reverse=True)
            if results[0]['score'] > best_pose['score']:
                best_pose = results[0]
                x0, y0, yaw0 = best_pose['x'], best_pose['y'], best_pose['yaw']
                xy_std *= 0.7
                yaw_std *= 0.7

        if best_pose['score'] > initial_pose['score']:
            self.get_logger().info(
                f'Particle refinement improved score: '
                f'{initial_pose["score"]:.3f} -> {best_pose["score"]:.3f}'
            )

        return best_pose

    # ---------------------------
    # Sampling (keepout-aware)
    # ---------------------------
    def _sample_free_space(self, num_samples: int):
        """Sample free space with preference near obstacles for better global candidates."""
        if self.free_mask is None or self.map_msg is None:
            return None

        free_mask = self.free_mask > 0
        free_yx = np.empty((0, 2), dtype=np.int64)

        if self.dist_m is not None:
            band_dist_m = max(0.1, float(self.get_parameter('sample_near_obstacles_dist_m').value))
            band_mask = free_mask & np.isfinite(self.dist_m) & (self.dist_m < band_dist_m)
            free_yx = np.argwhere(band_mask)

        # Fallback to full free-space if the obstacle band is too sparse.
        if free_yx.shape[0] < 100:
            free_yx = np.argwhere(free_mask)
        if free_yx.shape[0] < 100:
            return None

        res = float(self.map_msg.info.resolution)
        ox = float(self.map_msg.info.origin.position.x)
        oy = float(self.map_msg.info.origin.position.y)

        # Try a few rounds to get enough non-keepout samples
        want = int(max(10, num_samples))
        out_xy = []

        rounds = 0
        max_rounds = 6
        skip_keepout = bool(self.get_parameter('skip_keepout_in_sampling').value) and \
                       bool(self.get_parameter('enforce_keepout_mask').value) and self._keepout_ready()

        while len(out_xy) < want and rounds < max_rounds:
            rounds += 1
            # oversample a bit to survive keepout filtering
            oversample = int((want - len(out_xy)) * (2.0 if skip_keepout else 1.0))
            oversample = max(oversample, 50)

            n = min(oversample, free_yx.shape[0])
            if n >= free_yx.shape[0]:
                sel = free_yx
            else:
                idx = self.rng.choice(free_yx.shape[0], size=n, replace=False)
                sel = free_yx[idx]

            x = ox + (sel[:, 1].astype(np.float32) + 0.5) * res
            y = oy + (sel[:, 0].astype(np.float32) + 0.5) * res

            if skip_keepout:
                kept = []
                for i in range(x.shape[0]):
                    if not self._pose_hits_keepout(float(x[i]), float(y[i]), 0.0):
                        kept.append((float(x[i]), float(y[i])))
                out_xy.extend(kept)
            else:
                out_xy.extend([(float(x[i]), float(y[i])) for i in range(x.shape[0])])

        if len(out_xy) < 10:
            return None

        out_xy = out_xy[:want]
        return np.array(out_xy, dtype=np.float32)

    def _sample_around_pose(self, cx: float, cy: float, radius: float, num_samples: int):
        """Sample free space around a pose (keepout-aware)"""
        if self.free_mask is None or self.map_msg is None:
            return None

        res = float(self.map_msg.info.resolution)
        ox = float(self.map_msg.info.origin.position.x)
        oy = float(self.map_msg.info.origin.position.y)
        w, h = int(self.map_msg.info.width), int(self.map_msg.info.height)

        samples = []
        tries = 0
        max_tries = max(300, int(num_samples) * 30)

        skip_keepout = bool(self.get_parameter('skip_keepout_in_sampling').value) and \
                       bool(self.get_parameter('enforce_keepout_mask').value) and self._keepout_ready()

        while len(samples) < num_samples and tries < max_tries:
            tries += 1
            r = radius * math.sqrt(float(self.rng.random()))
            a = float(self.rng.uniform(-math.pi, math.pi))
            wx = cx + r * math.cos(a)
            wy = cy + r * math.sin(a)
            ix = int(math.floor((wx - ox) / res))
            iy = int(math.floor((wy - oy) / res))
            if ix < 0 or ix >= w or iy < 0 or iy >= h:
                continue
            if self.free_mask[iy, ix] == 0:
                continue

            wx_center = ox + (ix + 0.5) * res
            wy_center = oy + (iy + 0.5) * res

            if skip_keepout and self._pose_hits_keepout(wx_center, wy_center, 0.0):
                continue

            samples.append((wx_center, wy_center))

        if len(samples) < 10:
            return None

        return np.array(samples, dtype=np.float32)

    # ---------------------------
    # Ambiguity + validation
    # ---------------------------
    def detect_ambiguity(self, candidates: list):
        """
        Improved ambiguity detection using spatial clustering.
        """
        if len(candidates) < 2:
            return {'has_ambiguity': False, 'num_clusters': 1}

        top_n = min(20, len(candidates))
        top = candidates[:top_n]

        if bool(self.get_parameter('use_spatial_clustering').value):
            positions = np.array([[c['x'], c['y']] for c in top])
            diff = positions[:, None, :] - positions[None, :, :]
            dist_matrix = np.sqrt(np.sum(diff * diff, axis=2))

            eps = float(self.get_parameter('cluster_eps').value)
            clusters = []
            visited = set()

            for i in range(len(top)):
                if i in visited:
                    continue
                queue = [i]
                cluster = []
                visited.add(i)

                while queue:
                    q = queue.pop()
                    cluster.append(q)
                    neighbors = np.where(dist_matrix[q] < eps)[0]
                    for j in neighbors:
                        if j not in visited:
                            visited.add(j)
                            queue.append(int(j))

                clusters.append(cluster)

            cluster_scores = []
            for cluster in clusters:
                max_score = max(top[i]['score'] for i in cluster)
                cluster_scores.append(max_score)

            cluster_scores.sort(reverse=True)

            has_ambiguity = False
            if len(cluster_scores) >= 2:
                margin = cluster_scores[0] - cluster_scores[1]
                min_margin = self._effective_margin_threshold(
                    float(self.get_parameter('min_far_score_margin').value)
                )
                has_ambiguity = margin < min_margin

            return {
                'has_ambiguity': has_ambiguity,
                'num_clusters': len(clusters),
                'cluster_scores': cluster_scores[:3],
            }

        # Simple distance-based (fallback)
        best = top[0]
        ambiguity_dist = float(self.get_parameter('ambiguity_distance_m').value)

        far_best_score = 0.0
        for c in top[1:]:
            dx = c['x'] - best['x']
            dy = c['y'] - best['y']
            if math.hypot(dx, dy) >= ambiguity_dist:
                far_best_score = max(far_best_score, c['score'])

        margin = best['score'] - far_best_score
        min_margin = self._effective_margin_threshold(
            float(self.get_parameter('min_far_score_margin').value)
        )

        return {
            'has_ambiguity': margin < min_margin,
            'num_clusters': 2 if far_best_score > 0 else 1,
            'far_margin': margin,
        }

    def adaptive_threshold_validation(self, candidate: dict, candidates_list: list):
        """
        Adaptive thresholding based on map quality and candidate distribution.
        """
        if not bool(self.get_parameter('use_adaptive_thresholds').value):
            return self._fixed_threshold_validation(candidate)

        if len(candidates_list) < 5:
            self.get_logger().warn('Too few candidates for adaptive thresholds')
            return self._fixed_threshold_validation(candidate)

        scores = [c['score'] for c in candidates_list[:50]]
        score_mean = float(np.mean(scores))
        score_std = float(np.std(scores))

        min_score_base = float(self.get_parameter('min_accept_score').value)
        if score_mean > 0.4:
            min_score = max(min_score_base, score_mean - 1.5 * score_std)
        else:
            min_score = min_score_base * 0.85

        min_margin_base = float(self.get_parameter('min_score_margin').value)
        if score_std < 0.05:
            min_margin = min_margin_base * 0.7
        else:
            min_margin = min_margin_base
        min_margin = self._effective_margin_threshold(min_margin)

        min_known = float(self.get_parameter('min_known_ratio').value)
        max_mean_dist = float(self.get_parameter('max_mean_dist_m').value)
        min_hit = float(self.get_parameter('min_hit_ratio').value)

        score = float(candidate['score'])
        known = float(candidate['known_ratio'])
        mean_dist = float(candidate['mean_dist'])
        hit = float(candidate['hit_ratio'])

        if len(candidates_list) >= 2:
            margin = score - float(candidates_list[1]['score'])
        else:
            margin = score

        reasons = []
        if score < min_score:
            reasons.append(f'score={score:.3f}<{min_score:.3f}')
        if known < min_known:
            reasons.append(f'known={known:.2f}<{min_known:.2f}')
        if margin < min_margin:
            reasons.append(f'margin={margin:.3f}<{min_margin:.3f}')
        if mean_dist > max_mean_dist:
            reasons.append(f'mean_dist={mean_dist:.3f}>{max_mean_dist:.3f}')
        if hit < min_hit:
            reasons.append(f'hit={hit:.2f}<{min_hit:.2f}')

        return {
            'ok': len(reasons) == 0,
            'reasons': reasons,
            'thresholds_used': {
                'min_score': min_score,
                'min_margin': min_margin,
            }
        }

    def _fixed_threshold_validation(self, candidate: dict):
        """Original fixed threshold validation"""
        min_score = float(self.get_parameter('min_accept_score').value)
        min_known = float(self.get_parameter('min_known_ratio').value)
        min_margin = self._effective_margin_threshold(
            float(self.get_parameter('min_score_margin').value)
        )
        max_mean_dist = float(self.get_parameter('max_mean_dist_m').value)
        min_hit = float(self.get_parameter('min_hit_ratio').value)

        score = float(candidate['score'])
        known = float(candidate['known_ratio'])
        margin = float(candidate.get('margin', 0.0))
        mean_dist = float(candidate['mean_dist'])
        hit = float(candidate['hit_ratio'])

        reasons = []
        if score < min_score:
            reasons.append(f'score={score:.3f}<{min_score:.3f}')
        if known < min_known:
            reasons.append(f'known={known:.2f}<{min_known:.2f}')
        if margin < min_margin:
            reasons.append(f'margin={margin:.3f}<{min_margin:.3f}')
        if mean_dist > max_mean_dist:
            reasons.append(f'mean_dist={mean_dist:.3f}>{max_mean_dist:.3f}')
        if hit < min_hit:
            reasons.append(f'hit={hit:.2f}<{min_hit:.2f}')

        return {'ok': len(reasons) == 0, 'reasons': reasons}

    def _effective_margin_threshold(self, base_margin: float) -> float:
        """Scale margin threshold down only when curve-edge leniency is active."""
        base = max(0.0, float(base_margin))
        relax = max(0.0, min(0.95, float(self._last_scan_curve_relax)))
        if relax <= 1e-6:
            return base
        return base * (1.0 - relax)

    def _update_curve_edge_margin_relax(self, scan_points: np.ndarray):
        """Estimate curved-edge dominance from scan spread and cache leniency factor."""
        self._last_scan_curve_relax = 0.0
        self._last_scan_minor_major_ratio = 0.0
        if not bool(self.get_parameter('curve_edge_margin_relax_enabled').value):
            return
        if scan_points is None:
            return

        pts = np.asarray(scan_points, dtype=np.float32)
        if pts.ndim != 2 or pts.shape[1] != 2 or pts.shape[0] < 10:
            return

        centered = pts - np.mean(pts, axis=0, keepdims=True)
        cov = (centered.T @ centered) / float(max(1, centered.shape[0] - 1))
        eigvals = np.linalg.eigvalsh(cov)
        if eigvals.shape[0] < 2:
            return

        major = max(1e-9, float(eigvals[-1]))
        minor = max(0.0, float(eigvals[0]))
        ratio = minor / major
        self._last_scan_minor_major_ratio = ratio

        ratio_start = max(0.0, float(self.get_parameter('curve_edge_ratio_start').value))
        ratio_full = max(ratio_start + 1e-3, float(self.get_parameter('curve_edge_ratio_full').value))
        if ratio <= ratio_start:
            return

        ratio_factor = min(1.0, (ratio - ratio_start) / (ratio_full - ratio_start))
        max_relax = max(
            0.0,
            min(0.95, float(self.get_parameter('curve_edge_margin_relax_max').value)),
        )
        self._last_scan_curve_relax = ratio_factor * max_relax

    # ---------------------------
    # Main service
    # ---------------------------
    def handle_trigger(self, req, resp):
        """Main relocalization handler"""
        del req

        # Core validation
        if self.map_msg is None or self.dist_m is None:
            resp.success = False
            resp.message = 'No map available'
            return resp

        if self.scan is None:
            resp.success = False
            resp.message = 'No scan data'
            return resp

        # Keepout availability gate (optional)
        if bool(self.get_parameter('enforce_keepout_mask').value) and \
           bool(self.get_parameter('require_keepout_mask').value) and \
           (not self._keepout_ready()):
            resp.success = False
            resp.message = 'Keepout mask required but not available yet'
            return resp

        # Build scan points (single scan or temporal bundle).
        pts, bundle_info = self.build_scan_points()

        min_pts = int(self.get_parameter('min_scan_points').value)
        if pts.shape[0] < min_pts:
            resp.success = False
            resp.message = f'Insufficient scan points: {pts.shape[0]} < {min_pts}'
            return resp

        self._update_curve_edge_margin_relax(pts)
        if self._last_scan_curve_relax > 0.0:
            self.get_logger().info(
                'Curve-edge margin leniency active: '
                f'relax={100.0 * self._last_scan_curve_relax:.0f}%, '
                f'ratio={self._last_scan_minor_major_ratio:.2f}'
            )

        self.get_logger().info(
            f'Relocalizing with {pts.shape[0]} scan points '
            f'(bundle front={bundle_info.get("front_scans", 0)}, rear={bundle_info.get("rear_scans", 0)})'
        )

        if bool(self.get_parameter('enforce_keepout_mask').value) and (not self._keepout_ready()):
            if not self._warned_keepout_missing:
                self._warned_keepout_missing = True
                self.get_logger().warn('Keepout enforcement enabled but keepout mask not received yet; proceeding without mask.')

        # Try local-prior search first
        best_candidate = None
        source = None

        if bool(self.get_parameter('use_local_prior_search').value):
            prior_ok, prior, prior_message = self._local_prior_status()
            if not prior_ok:
                self.get_logger().info(f'Skipping local-prior search: {prior_message}')
            else:
                try:
                    px, py, pyaw, prior_source = self._current_prior_pose(prior)
                    self.get_logger().info(
                        f'Local-prior search centered on {prior_source}: '
                        f'x={px:.2f}, y={py:.2f}, yaw={pyaw:.2f} ({prior_message})'
                    )

                    radius = float(self.get_parameter('local_search_radius_m').value)
                    local_cand = self._sample_around_pose(px, py, radius, 400)

                    if local_cand is not None and local_cand.shape[0] >= 50:
                        yaw_span = math.radians(float(self.get_parameter('local_yaw_span_deg').value))
                        yaw_list = np.linspace(pyaw - yaw_span, pyaw + yaw_span, 25)
                        yaw_list = self._prune_yaw_candidates(yaw_list, self._estimate_scan_axis_yaw(pts))

                        local_results = self.score_candidates_batch(pts, local_cand, yaw_list)

                        if local_results:
                            # Keepout filter (already applied in scoring, but keep it safe)
                            local_results = self._filter_candidates_keepout(local_results, max_check=2000)

                            if local_results:
                                local_score_min = float(self.get_parameter('local_min_accept_score').value)
                                local_known_min = float(self.get_parameter('local_min_known_ratio').value)
                                local_ok = (
                                    float(local_results[0]['score']) >= local_score_min and
                                    float(local_results[0]['known_ratio']) >= local_known_min
                                )
                                if local_ok:
                                    best_candidate = local_results[0]
                                    source = 'local'
                                    self.get_logger().info('Accepted local-prior relocalization')
                                else:
                                    self.get_logger().info(
                                        f'Local rejected: score={local_results[0]["score"]:.3f}<{local_score_min:.3f} '
                                        f'or known={local_results[0]["known_ratio"]:.2f}<{local_known_min:.2f}'
                                    )
                except TransformException:
                    self.get_logger().info('No usable prior pose transform, using global search')

        # Global search if local failed
        if best_candidate is None:
            all_results = self.multi_resolution_search(pts)

            if not all_results:
                resp.success = False
                resp.message = 'No candidates found in global search'
                return resp

            refined = self.particle_refinement(pts, all_results[0])
            all_results[0] = refined
            all_results.sort(key=lambda r: r['score'], reverse=True)

            # Keepout filter BEFORE computing margin/ambiguity/validation
            safe_results = self._filter_candidates_keepout(all_results, max_check=12000)
            if bool(self.get_parameter('enforce_keepout_mask').value) and self._keepout_ready():
                if not safe_results:
                    resp.success = False
                    resp.message = 'Global search: all top candidates intersect keepout zones'
                    return resp
            else:
                safe_results = all_results

            # Add margin info using safe candidates
            if len(safe_results) >= 2:
                safe_results[0]['margin'] = safe_results[0]['score'] - safe_results[1]['score']
            else:
                safe_results[0]['margin'] = safe_results[0]['score']

            ambiguity = self.detect_ambiguity(safe_results)
            validation = self.adaptive_threshold_validation(safe_results[0], safe_results)

            # Force acceptance is disabled by default to prevent unsafe pose estimation.
            # Enable only for explicit operator-confirmed recovery scenarios.
            if not validation['ok']:
                force_accept = bool(self.get_parameter('force_accept_on_validation_failure').value)
                if not force_accept:
                    reasons = [str(r) for r in validation.get('reasons', [])]
                    resp.success = False
                    resp.message = f'Relocalization rejected: validation failed: {reasons}'
                    return resp
                reasons = [str(r) for r in validation.get('reasons', [])]
                self.get_logger().warn(
                    f'force_accept_on_validation_failure=true: accepting despite failure: {reasons}'
                )

            if ambiguity['has_ambiguity']:
                self.get_logger().warn(
                    f'Ambiguous result detected: {ambiguity["num_clusters"]} clusters'
                )

            best_candidate = safe_results[0]
            source = 'global'

        # Final keepout assert (paranoid)
        if bool(self.get_parameter('enforce_keepout_mask').value) and self._keepout_ready():
            if self._pose_hits_keepout(float(best_candidate['x']), float(best_candidate['y']), float(best_candidate.get('yaw', 0.0))):
                resp.success = False
                resp.message = 'Internal error: chosen pose intersects keepout (should not happen)'
                return resp

        # Publish /initialpose
        msg = PoseWithCovarianceStamped()
        msg.header.frame_id = 'map'
        msg.header.stamp = self.get_clock().now().to_msg()

        msg.pose.pose.position.x = float(best_candidate['x'])
        msg.pose.pose.position.y = float(best_candidate['y'])

        qx, qy, qz, qw = yaw_to_quat(float(best_candidate['yaw']))
        msg.pose.pose.orientation.x = qx
        msg.pose.pose.orientation.y = qy
        msg.pose.pose.orientation.z = qz
        msg.pose.pose.orientation.w = qw

        # Covariance based on score
        score = float(best_candidate['score'])
        xy_var = float(max(0.03, 0.5 * (1.0 - score)))
        yaw_var = float(max(0.02, 0.3 * (1.0 - score)))

        cov = [0.0] * 36
        cov[0] = xy_var          # xx
        cov[7] = xy_var          # yy
        cov[14] = 99999.0        # zz (unused)
        cov[21] = 99999.0        # rr (unused)
        cov[28] = 99999.0        # pp (unused)
        cov[35] = yaw_var        # yaw-yaw
        msg.pose.covariance = cov

        self.get_logger().info(
            f'Publishing /initialpose repeatedly: x={msg.pose.pose.position.x:.2f}, y={msg.pose.pose.position.y:.2f}, yaw={best_candidate["yaw"]:.2f}'
        )
        publish_count = 5
        publish_interval = 0.08
        for i in range(publish_count):
            self.initpose_pub.publish(msg)
            self.get_logger().info(f'Published /initialpose message ({i+1}/{publish_count})')
            time.sleep(publish_interval)

        resp.success = True
        resp.message = (
            f'Relocalized ({source}): x={best_candidate["x"]:.2f}, '
            f'y={best_candidate["y"]:.2f}, yaw={best_candidate["yaw"]:.2f}, '
            f'score={best_candidate["score"]:.3f}, '
            f'known={best_candidate["known_ratio"]:.2f}, '
            f'hit={best_candidate["hit_ratio"]:.2f}'
        )
        return resp


def main():
    rclpy.init()
    node = ImprovedCorrelativeRelocalizer()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
