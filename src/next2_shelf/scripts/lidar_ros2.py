#!/usr/bin/env python3
from collections import deque
import json
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import rclpy
from action_msgs.msg import GoalStatus
from geometry_msgs.msg import Point, PoseStamped, Quaternion, TransformStamped
from rcl_interfaces.msg import SetParametersResult
from rclpy.action import ActionClient
from rclpy.duration import Duration
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
from rclpy.time import Time
from lifecycle_msgs.msg import Transition as LifecycleTransition
from lifecycle_msgs.srv import ChangeState as LifecycleChangeState, GetState as LifecycleGetState
from sensor_msgs.msg import LaserScan
from std_msgs.msg import Bool, String as StringMsg
from std_srvs.srv import SetBool, Trigger
from tf2_ros import Buffer, TransformBroadcaster, TransformException, TransformListener
from visualization_msgs.msg import Marker, MarkerArray

try:
    from nav2_msgs.action import NavigateToPose
except ImportError:
    NavigateToPose = None

try:
    from next_msgs.action import JackingOperation
except ImportError:
    JackingOperation = None


_CONTROL_MODE_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
    history=HistoryPolicy.KEEP_LAST,
    depth=1,
)


@dataclass
class Hotspot:
    start_idx: int
    end_idx: int
    bright_indices: Tuple[int, ...]
    angle: float
    range_value: float
    x: float
    y: float
    peak_intensity: float
    synthetic: bool = False


@dataclass
class ShelfPose:
    x: float
    y: float
    yaw: float
    frame_id: str
    scan_time: Time
    ordered_points: Tuple[Tuple[float, float], ...]
    front_midpoint: Tuple[float, float]
    back_midpoint: Tuple[float, float]
    front_intensity_sum: float
    back_intensity_sum: float
    intensity_balance_ratio: float


class ShelfDetectorNode(Node):
    def __init__(self) -> None:
        super().__init__('distance_calculator')
        self._declare_parameters()
        self._load_parameters(initial_load=True)
        self.add_on_set_parameters_callback(self._on_parameters_changed)

        self.roi_marker_pub = self.create_publisher(Marker, '/shelf_roi_marker', 10)
        self.marker_array_pub = self.create_publisher(MarkerArray, '/markers', 10)
        self.group_marker_pub = self.create_publisher(MarkerArray, '/group_markers', 10)
        self.shelf_detected_pub = self.create_publisher(Bool, '/shelf_detected', 10)
        self.shelf_status_pub = self.create_publisher(StringMsg, self.status_topic, 10)
        self.tick_pub = self.create_publisher(Bool, '/tick', 10)
        self.goal_pub = self.create_publisher(PoseStamped, '/move_base_simple/goal', 10)
        self.goal_pose_pub = self.create_publisher(PoseStamped, '/goal_pose', 10)
        self.control_mode_pub = self.create_publisher(StringMsg, '/control_mode', _CONTROL_MODE_QOS)

        self.tf_broadcaster = TransformBroadcaster(self)
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        self.scan_sub = self.create_subscription(LaserScan, self.scan_topic, self.lidar_callback, 10)
        self.tick_sub = self.create_subscription(Bool, '/tick', self.tick_callback, 10)
        self.commit_service = self.create_service(Trigger, '/shelf/commit', self._handle_commit_service)
        self.enable_service = self.create_service(SetBool, '/shelf/set_enabled', self._handle_set_enabled_service)
        self.zone_manager_handoff_client = self.create_client(
            Trigger,
            self.zone_manager_goal_handoff_service,
        )
        self.bt_navigator_get_state_client = self.create_client(
            LifecycleGetState,
            '/bt_navigator/get_state',
        )
        self.bt_navigator_change_state_client = self.create_client(
            LifecycleChangeState,
            '/bt_navigator/change_state',
        )

        self.nav_client = None
        if NavigateToPose is not None:
            self.nav_client = ActionClient(self, NavigateToPose, self.navigation_action_name)
        elif self.enable_nav2_action:
            self.get_logger().warn(
                'Nav2 action support requested, but nav2_msgs is not available. '
                'Goal poses will only be published on topics.'
            )

        self.jack_client = None
        if JackingOperation is not None:
            self.jack_client = ActionClient(self, JackingOperation, self.jacking_action_name)
        elif self.enable_jacking_action:
            self.get_logger().warn(
                'Jacking action support requested, but next_msgs is not available. '
                'Jacking requests will be skipped.'
            )

        self.is_navigating = False
        self.auto_trigger_latched = False
        self._nav_goal_handle = None
        self._jack_goal_handle = None
        self._nav_send_inflight = False
        self._nav_send_lock = threading.Lock()
        self._last_published_detected: Optional[bool] = None
        self._last_logged_reason = ''
        self._last_logged_solve_pose: Optional[ShelfPose] = None
        self._last_logged_consistency_count = -1
        self._last_logged_candidate_valid_state: Optional[bool] = None
        self._warned_frame_mismatch = ''
        self._warned_stale_scan_stamp = False

        self.active_scan_frame = str(self.laser_frame or 'laser')
        self.frame_mismatch_warning = ''
        self.max_intensity = 0.0
        self.hotspot_count = 0
        self.latest_hotspots: List[Hotspot] = []
        self.solver_ok = False
        self.last_reason = 'waiting_for_scan' if self.detector_enabled else 'detector_disabled'

        self.candidate_pose: Optional[ShelfPose] = None
        self.candidate_pose_smoothed: Optional[ShelfPose] = None
        self.candidate_scan_time: Optional[Time] = None
        self.candidate_fresh = False
        self.candidate_consistent = False
        self.candidate_valid = False
        self.candidate_hotspot_count = 0
        self.candidate_confidence = 0.0
        self.candidate_confidence_components: Dict[str, float] = {}
        self.candidate_sigma_x = -1.0
        self.candidate_sigma_y = -1.0
        self.candidate_sigma_yaw = -1.0
        self.candidate_uncertainty_samples = 0
        self.consistency_count = 0
        self._consistency_last_pose: Optional[ShelfPose] = None
        self._accepted_pose_history = deque(maxlen=self.pose_uncertainty_window)

        self.committed_target_pose: Optional[ShelfPose] = None
        self.committed_target_time: Optional[Time] = None

        self.timer = self.create_timer(0.1, self._on_timer)
        self._publish_visual_state()
        self._publish_status()

    def _declare_parameters(self) -> None:
        default_jacking_operation = 0
        if JackingOperation is not None and hasattr(JackingOperation.Goal, 'JACK_LOAD'):
            default_jacking_operation = int(JackingOperation.Goal.JACK_LOAD)

        self.declare_parameters(
            namespace='',
            parameters=[
                ('scan_topic', '/scan'),
                ('laser_frame', 'laser'),
                ('goal_frame', 'laser'),
                ('status_topic', '/shelf/status_json'),
                ('goal_transform_timeout_sec', 0.20),
                ('switch_to_nav_mode_on_commit', True),
                ('nav_control_mode', 'zones'),
                ('use_zone_manager_goal_handoff', True),
                ('zone_manager_goal_handoff_service', '/navigate_to_goal_pose'),
                ('goal_pose_handoff_settle_sec', 0.10),
                ('approach_with_current_heading', True),
                ('robot_base_frame', 'base_footprint'),
                ('detector_enabled', False),
                ('use_roi', False),
                ('publish_bounding_box', True),
                ('x_min', -3.0),
                ('x_max', 3.0),
                ('y_min', -1.5),
                ('y_max', 1.5),
                ('intensity_threshold', 35),
                ('marker_lifetime', 0.25),
                ('detection_timeout', 0.50),
                ('candidate_dropout_tolerance_sec', 0.35),
                ('scan_stamp_future_tolerance_sec', 0.10),
                ('scan_stamp_past_tolerance_sec', 0.25),
                ('candidate_consistency_frames', 3),
                ('candidate_center_tolerance_m', 0.015),
                ('candidate_yaw_tolerance_deg', 3.0),
                ('candidate_corner_tolerance_m', 0.03),
                ('pose_smoothing_alpha', 0.45),
                ('pose_uncertainty_window', 8),
                ('dip_bridge_threshold_margin', 5.0),
                ('single_bin_hotspot_boost', 12.0),
                ('hotspot_merge_max_distance_m', 0.12),
                ('hotspot_merge_max_range_delta_m', 0.10),
                ('hotspot_merge_max_angle_deg', 6.0),
                ('front_back_min_separation_m', 0.08),
                ('left_right_min_separation_m', 0.03),
                ('pairing_require_front_brighter', True),
                ('pairing_front_back_intensity_margin', 0.02),
                ('degenerate_area_threshold_m2', 0.01),
                ('clear_tf_on_lost', True),
                ('publish_goal_pose', True),
                ('publish_legacy_goal', True),
                ('enable_nav2_action', False),
                ('auto_trigger_on_detection', False),
                ('navigation_action_name', 'navigate_to_pose'),
                ('nav_goal_accept_attempts', 6),
                ('goal_offset_x', 0.00),
                ('goal_offset_y', 0.00),
                ('pause_during_navigation', True),
                ('enable_jacking_action', False),
                ('jacking_action_name', 'next_jacking_ac_server'),
                ('jacking_speed', 2500),
                ('jacking_timeout', 100),
                ('jacking_operation', default_jacking_operation),
                # Deprecated geometry-trigger compatibility params: retained as no-ops.
                ('eps', 0.25),
                ('proximity_threshold', 0.25),
                ('min_samples', 10),
                ('min_sample', 10),
                ('shelf_width', 0.60),
                ('shelf_width_tolerance', 0.10),
                ('dist_threshold', 0.80),
                ('offset_length', 1.20),
                ('min_distance', 0.60),
                ('max_distance', 0.65),
                ('require_pair_distance_gate', False),
            ],
        )

    def _load_parameters(self, initial_load: bool = False) -> None:
        self.scan_topic = str(self.get_parameter('scan_topic').value)
        self.laser_frame = str(self.get_parameter('laser_frame').value)
        self.goal_frame = str(self.get_parameter('goal_frame').value)
        self.status_topic = str(self.get_parameter('status_topic').value)
        self.goal_transform_timeout_sec = float(self.get_parameter('goal_transform_timeout_sec').value)
        self.switch_to_nav_mode_on_commit = bool(
            self.get_parameter('switch_to_nav_mode_on_commit').value
        )
        self.nav_control_mode = str(self.get_parameter('nav_control_mode').value)
        self.use_zone_manager_goal_handoff = bool(
            self.get_parameter('use_zone_manager_goal_handoff').value
        )
        self.zone_manager_goal_handoff_service = str(
            self.get_parameter('zone_manager_goal_handoff_service').value
        )
        self.goal_pose_handoff_settle_sec = float(
            self.get_parameter('goal_pose_handoff_settle_sec').value
        )
        self.approach_with_current_heading = bool(
            self.get_parameter('approach_with_current_heading').value
        )
        self.robot_base_frame = str(self.get_parameter('robot_base_frame').value)
        self.detector_enabled = bool(self.get_parameter('detector_enabled').value)
        self.use_roi = bool(self.get_parameter('use_roi').value)
        self.publish_bounding_box = bool(self.get_parameter('publish_bounding_box').value)
        self.x_min = float(self.get_parameter('x_min').value)
        self.x_max = float(self.get_parameter('x_max').value)
        self.y_min = float(self.get_parameter('y_min').value)
        self.y_max = float(self.get_parameter('y_max').value)
        self.intensity_threshold = float(self.get_parameter('intensity_threshold').value)
        self.marker_lifetime = float(self.get_parameter('marker_lifetime').value)
        self.detection_timeout = float(self.get_parameter('detection_timeout').value)
        self.candidate_dropout_tolerance_sec = max(
            0.0,
            float(self.get_parameter('candidate_dropout_tolerance_sec').value),
        )
        self.scan_stamp_future_tolerance_sec = float(
            self.get_parameter('scan_stamp_future_tolerance_sec').value
        )
        self.scan_stamp_past_tolerance_sec = float(
            self.get_parameter('scan_stamp_past_tolerance_sec').value
        )
        self.candidate_consistency_frames = int(
            self.get_parameter('candidate_consistency_frames').value
        )
        self.candidate_center_tolerance_m = float(
            self.get_parameter('candidate_center_tolerance_m').value
        )
        self.candidate_yaw_tolerance_deg = float(
            self.get_parameter('candidate_yaw_tolerance_deg').value
        )
        self.candidate_corner_tolerance_m = float(
            self.get_parameter('candidate_corner_tolerance_m').value
        )
        self.pose_smoothing_alpha = float(
            self.get_parameter('pose_smoothing_alpha').value
        )
        self.pose_uncertainty_window = max(
            1,
            int(self.get_parameter('pose_uncertainty_window').value),
        )
        self.dip_bridge_threshold_margin = float(
            self.get_parameter('dip_bridge_threshold_margin').value
        )
        self.single_bin_hotspot_boost = float(
            self.get_parameter('single_bin_hotspot_boost').value
        )
        self.hotspot_merge_max_distance_m = float(
            self.get_parameter('hotspot_merge_max_distance_m').value
        )
        self.hotspot_merge_max_range_delta_m = float(
            self.get_parameter('hotspot_merge_max_range_delta_m').value
        )
        self.hotspot_merge_max_angle_deg = float(
            self.get_parameter('hotspot_merge_max_angle_deg').value
        )
        self.front_back_min_separation_m = float(
            self.get_parameter('front_back_min_separation_m').value
        )
        self.left_right_min_separation_m = float(
            self.get_parameter('left_right_min_separation_m').value
        )
        self.pairing_require_front_brighter = bool(
            self.get_parameter('pairing_require_front_brighter').value
        )
        self.pairing_front_back_intensity_margin = float(
            self.get_parameter('pairing_front_back_intensity_margin').value
        )
        self.degenerate_area_threshold_m2 = float(
            self.get_parameter('degenerate_area_threshold_m2').value
        )
        self.clear_tf_on_lost = bool(self.get_parameter('clear_tf_on_lost').value)
        self.publish_goal_pose = bool(self.get_parameter('publish_goal_pose').value)
        self.publish_legacy_goal = bool(self.get_parameter('publish_legacy_goal').value)
        self.enable_nav2_action = bool(self.get_parameter('enable_nav2_action').value)
        self.auto_trigger_on_detection = bool(self.get_parameter('auto_trigger_on_detection').value)
        self.navigation_action_name = str(self.get_parameter('navigation_action_name').value)
        self.nav_goal_accept_attempts = max(1, int(self.get_parameter('nav_goal_accept_attempts').value))
        self.goal_offset_x = float(self.get_parameter('goal_offset_x').value)
        self.goal_offset_y = float(self.get_parameter('goal_offset_y').value)
        self.pause_during_navigation = bool(self.get_parameter('pause_during_navigation').value)
        self.enable_jacking_action = bool(self.get_parameter('enable_jacking_action').value)
        self.jacking_action_name = str(self.get_parameter('jacking_action_name').value)
        self.jacking_speed = int(self.get_parameter('jacking_speed').value)
        self.jacking_timeout = int(self.get_parameter('jacking_timeout').value)
        self.jacking_operation = int(self.get_parameter('jacking_operation').value)

        self.eps = float(self.get_parameter('eps').value)
        self.proximity_threshold = float(self.get_parameter('proximity_threshold').value)
        self.min_samples = int(self.get_parameter('min_samples').value)
        self.min_sample = int(self.get_parameter('min_sample').value)
        self.shelf_width = float(self.get_parameter('shelf_width').value)
        self.shelf_width_tolerance = float(self.get_parameter('shelf_width_tolerance').value)
        self.dist_threshold = float(self.get_parameter('dist_threshold').value)
        self.offset_length = float(self.get_parameter('offset_length').value)
        self.min_distance = float(self.get_parameter('min_distance').value)
        self.max_distance = float(self.get_parameter('max_distance').value)
        self.require_pair_distance_gate = bool(self.get_parameter('require_pair_distance_gate').value)

        if initial_load:
            self.get_logger().warn(
                'Legacy DBSCAN trigger parameters (eps, proximity_threshold, '
                'min_samples, min_sample, dist_threshold) are deprecated. '
                'Intensity hotspots drive detection, while offset_length, '
                'min_distance/max_distance, and '
                'require_pair_distance_gate still tune the optional 2-leg fallback.'
            )
            if self.goal_frame:
                self.get_logger().warn(
                    'goal_frame controls the published navigation target frame. '
                    'Candidate solving and reflector markers remain in the scan frame.'
                )
            if self.use_zone_manager_goal_handoff:
                self.get_logger().info(
                    f'Shelf navigation handoff via zone_manager service '
                    f'{self.zone_manager_goal_handoff_service}'
                )

        if not initial_load and self.scan_sub.topic_name != self.scan_topic:
            self.get_logger().warn('scan_topic changes require a node restart to take effect.')
        if (
            not initial_load
            and self.zone_manager_handoff_client.srv_name != self.zone_manager_goal_handoff_service
        ):
            self.get_logger().warn(
                'zone_manager_goal_handoff_service changes require a node restart to take effect.'
            )

    def _on_parameters_changed(self, params) -> SetParametersResult:
        if NavigateToPose is None:
            for param in params:
                if param.name == 'enable_nav2_action' and bool(param.value):
                    return SetParametersResult(
                        successful=False,
                        reason='nav2_msgs is not available in this environment.',
                    )

        if JackingOperation is None:
            for param in params:
                if param.name == 'enable_jacking_action' and bool(param.value):
                    return SetParametersResult(
                        successful=False,
                        reason='next_msgs is not available in this environment.',
                    )

        for param in params:
            if param.name in {
                'detection_timeout',
                'scan_stamp_future_tolerance_sec',
                'scan_stamp_past_tolerance_sec',
                'goal_transform_timeout_sec',
                'goal_pose_handoff_settle_sec',
                'candidate_center_tolerance_m',
                'candidate_yaw_tolerance_deg',
                'candidate_corner_tolerance_m',
                'pose_smoothing_alpha',
                'hotspot_merge_max_distance_m',
                'hotspot_merge_max_range_delta_m',
                'hotspot_merge_max_angle_deg',
                'front_back_min_separation_m',
                'left_right_min_separation_m',
                'degenerate_area_threshold_m2',
            } and float(param.value) <= 0.0:
                return SetParametersResult(successful=False, reason=f'{param.name} must be > 0.')
            if param.name == 'pose_smoothing_alpha' and float(param.value) > 1.0:
                return SetParametersResult(
                    successful=False,
                    reason='pose_smoothing_alpha must be <= 1.0.',
                )
            if param.name == 'candidate_dropout_tolerance_sec' and float(param.value) < 0.0:
                return SetParametersResult(
                    successful=False,
                    reason='candidate_dropout_tolerance_sec must be >= 0.',
                )
            if param.name == 'candidate_consistency_frames' and int(param.value) < 1:
                return SetParametersResult(successful=False, reason='candidate_consistency_frames must be >= 1.')
            if param.name == 'pose_uncertainty_window' and int(param.value) < 1:
                return SetParametersResult(successful=False, reason='pose_uncertainty_window must be >= 1.')
            if param.name == 'nav_goal_accept_attempts' and int(param.value) < 1:
                return SetParametersResult(successful=False, reason='nav_goal_accept_attempts must be >= 1.')
            if param.name == 'marker_lifetime' and float(param.value) < 0.0:
                return SetParametersResult(successful=False, reason='marker_lifetime must be >= 0.')

        for param in params:
            name = str(param.name)
            value = param.value
            if name == 'scan_topic':
                self.scan_topic = str(value)
            elif name == 'laser_frame':
                self.laser_frame = str(value)
            elif name == 'goal_frame':
                self.goal_frame = str(value)
            elif name == 'status_topic':
                self.status_topic = str(value)
            elif name == 'goal_transform_timeout_sec':
                self.goal_transform_timeout_sec = float(value)
            elif name == 'switch_to_nav_mode_on_commit':
                self.switch_to_nav_mode_on_commit = bool(value)
            elif name == 'nav_control_mode':
                self.nav_control_mode = str(value)
            elif name == 'use_zone_manager_goal_handoff':
                self.use_zone_manager_goal_handoff = bool(value)
            elif name == 'zone_manager_goal_handoff_service':
                self.zone_manager_goal_handoff_service = str(value)
            elif name == 'goal_pose_handoff_settle_sec':
                self.goal_pose_handoff_settle_sec = float(value)
            elif name == 'approach_with_current_heading':
                self.approach_with_current_heading = bool(value)
            elif name == 'robot_base_frame':
                self.robot_base_frame = str(value)
            elif name == 'detector_enabled':
                self.detector_enabled = bool(value)
            elif name == 'use_roi':
                self.use_roi = bool(value)
            elif name == 'publish_bounding_box':
                self.publish_bounding_box = bool(value)
            elif name == 'x_min':
                self.x_min = float(value)
            elif name == 'x_max':
                self.x_max = float(value)
            elif name == 'y_min':
                self.y_min = float(value)
            elif name == 'y_max':
                self.y_max = float(value)
            elif name == 'intensity_threshold':
                self.intensity_threshold = float(value)
            elif name == 'marker_lifetime':
                self.marker_lifetime = float(value)
            elif name == 'detection_timeout':
                self.detection_timeout = float(value)
            elif name == 'candidate_dropout_tolerance_sec':
                self.candidate_dropout_tolerance_sec = max(0.0, float(value))
            elif name == 'scan_stamp_future_tolerance_sec':
                self.scan_stamp_future_tolerance_sec = float(value)
            elif name == 'scan_stamp_past_tolerance_sec':
                self.scan_stamp_past_tolerance_sec = float(value)
            elif name == 'candidate_consistency_frames':
                self.candidate_consistency_frames = int(value)
            elif name == 'candidate_center_tolerance_m':
                self.candidate_center_tolerance_m = float(value)
            elif name == 'candidate_yaw_tolerance_deg':
                self.candidate_yaw_tolerance_deg = float(value)
            elif name == 'candidate_corner_tolerance_m':
                self.candidate_corner_tolerance_m = float(value)
            elif name == 'pose_smoothing_alpha':
                self.pose_smoothing_alpha = float(value)
            elif name == 'pose_uncertainty_window':
                self.pose_uncertainty_window = max(1, int(value))
                self._resize_accepted_pose_history(self.pose_uncertainty_window)
            elif name == 'dip_bridge_threshold_margin':
                self.dip_bridge_threshold_margin = float(value)
            elif name == 'single_bin_hotspot_boost':
                self.single_bin_hotspot_boost = float(value)
            elif name == 'hotspot_merge_max_distance_m':
                self.hotspot_merge_max_distance_m = float(value)
            elif name == 'hotspot_merge_max_range_delta_m':
                self.hotspot_merge_max_range_delta_m = float(value)
            elif name == 'hotspot_merge_max_angle_deg':
                self.hotspot_merge_max_angle_deg = float(value)
            elif name == 'front_back_min_separation_m':
                self.front_back_min_separation_m = float(value)
            elif name == 'left_right_min_separation_m':
                self.left_right_min_separation_m = float(value)
            elif name == 'pairing_require_front_brighter':
                self.pairing_require_front_brighter = bool(value)
            elif name == 'pairing_front_back_intensity_margin':
                self.pairing_front_back_intensity_margin = float(value)
            elif name == 'degenerate_area_threshold_m2':
                self.degenerate_area_threshold_m2 = float(value)
            elif name == 'clear_tf_on_lost':
                self.clear_tf_on_lost = bool(value)
            elif name == 'publish_goal_pose':
                self.publish_goal_pose = bool(value)
            elif name == 'publish_legacy_goal':
                self.publish_legacy_goal = bool(value)
            elif name == 'enable_nav2_action':
                self.enable_nav2_action = bool(value)
            elif name == 'auto_trigger_on_detection':
                self.auto_trigger_on_detection = bool(value)
            elif name == 'navigation_action_name':
                self.navigation_action_name = str(value)
            elif name == 'nav_goal_accept_attempts':
                self.nav_goal_accept_attempts = max(1, int(value))
            elif name == 'goal_offset_x':
                self.goal_offset_x = float(value)
            elif name == 'goal_offset_y':
                self.goal_offset_y = float(value)
            elif name == 'pause_during_navigation':
                self.pause_during_navigation = bool(value)
            elif name == 'enable_jacking_action':
                self.enable_jacking_action = bool(value)
            elif name == 'jacking_action_name':
                self.jacking_action_name = str(value)
            elif name == 'jacking_speed':
                self.jacking_speed = int(value)
            elif name == 'jacking_timeout':
                self.jacking_timeout = int(value)
            elif name == 'jacking_operation':
                self.jacking_operation = int(value)
            elif name == 'eps':
                self.eps = float(value)
            elif name == 'proximity_threshold':
                self.proximity_threshold = float(value)
            elif name == 'min_samples':
                self.min_samples = int(value)
            elif name == 'min_sample':
                self.min_sample = int(value)
            elif name == 'shelf_width':
                self.shelf_width = float(value)
            elif name == 'shelf_width_tolerance':
                self.shelf_width_tolerance = float(value)
            elif name == 'dist_threshold':
                self.dist_threshold = float(value)
            elif name == 'offset_length':
                self.offset_length = float(value)
            elif name == 'min_distance':
                self.min_distance = float(value)
            elif name == 'max_distance':
                self.max_distance = float(value)
            elif name == 'require_pair_distance_gate':
                self.require_pair_distance_gate = bool(value)

        changed_names = {str(param.name) for param in params}
        if 'detector_enabled' in changed_names:
            self._apply_detector_enabled(self.detector_enabled, source='parameter')
        if 'scan_topic' in changed_names:
            self.get_logger().warn('scan_topic changes require a node restart to take effect.')
        if 'status_topic' in changed_names:
            self.get_logger().warn('status_topic changes require a node restart to take effect.')

        return SetParametersResult(successful=True, reason='')

    def lidar_callback(self, msg: LaserScan) -> None:
        if not self.detector_enabled:
            return

        if self.pause_during_navigation and self.is_navigating:
            return

        receipt_time = self.get_clock().now()
        scan_time = self._coerce_scan_time(msg, receipt_time)
        frame_id = (msg.header.frame_id or '').strip() or self.laser_frame or 'laser'
        self.active_scan_frame = frame_id
        self.frame_mismatch_warning = self._frame_warning_for_scan(frame_id)

        if self.publish_bounding_box:
            self.publish_roi_marker(frame_id)

        hotspots, failure_reason, max_intensity = self._extract_hotspots(msg)
        self.max_intensity = max_intensity
        self.hotspot_count = len(hotspots)
        self.latest_hotspots = list(hotspots)

        if failure_reason is not None:
            self._set_invalid_candidate(
                failure_reason,
                frame_id=frame_id,
                scan_time=scan_time,
                max_intensity=max_intensity,
                hotspot_count=0,
                solver_ok=False,
            )
            return

        solve = None
        solve_failure = None
        if len(hotspots) == 2:
            solve, solve_failure = self._solve_from_two_hotspots(hotspots, frame_id, scan_time)
            if solve is None:
                self._set_invalid_candidate(
                    solve_failure or 'too_few_hotspots',
                    frame_id=frame_id,
                    scan_time=scan_time,
                    max_intensity=max_intensity,
                    hotspot_count=len(hotspots),
                    solver_ok=False,
                )
                return
        elif len(hotspots) == 3:
            solve, solve_failure = self._solve_from_three_hotspots(hotspots, frame_id, scan_time)
            if solve is None:
                self._set_invalid_candidate(
                    solve_failure or 'too_few_hotspots',
                    frame_id=frame_id,
                    scan_time=scan_time,
                    max_intensity=max_intensity,
                    hotspot_count=len(hotspots),
                    solver_ok=False,
                )
                return
        elif len(hotspots) < 4:
            self._set_invalid_candidate(
                'too_few_hotspots',
                frame_id=frame_id,
                scan_time=scan_time,
                max_intensity=max_intensity,
                hotspot_count=len(hotspots),
                solver_ok=False,
            )
            return

        elif len(hotspots) > 4:
            self._set_invalid_candidate(
                'too_many_hotspots',
                frame_id=frame_id,
                scan_time=scan_time,
                max_intensity=max_intensity,
                hotspot_count=len(hotspots),
                solver_ok=False,
            )
            return

        elif len(hotspots) == 4:
            solve, solve_failure = self._solve_from_hotspots(hotspots, frame_id, scan_time)
            if solve is None:
                self._set_invalid_candidate(
                    solve_failure or 'degenerate_geometry',
                    frame_id=frame_id,
                    scan_time=scan_time,
                    max_intensity=max_intensity,
                    hotspot_count=len(hotspots),
                    solver_ok=False,
                )
                return

        consistency_count = self._advance_consistency(solve)
        self.candidate_pose = solve
        self.candidate_scan_time = scan_time
        self.hotspot_count = len(hotspots)
        self.candidate_hotspot_count = len(hotspots)
        self.max_intensity = max_intensity
        self.solver_ok = True
        self.candidate_confidence, self.candidate_confidence_components = self._score_candidate(
            solve,
            hotspot_count=len(hotspots),
            consistency_count=consistency_count,
        )
        self.candidate_fresh = self._is_candidate_fresh(self.get_clock().now())
        self.candidate_consistent = consistency_count >= self.candidate_consistency_frames
        if self.candidate_consistent:
            self._update_smoothed_candidate(solve)
        else:
            self._clear_smoothed_candidate_state()
        self.candidate_valid = self._is_manual_commit_ready()
        self.last_reason = '' if self.candidate_valid else 'inconsistent_solve'

        self.get_logger().debug(
            f'Shelf scan frame={frame_id} hotspots={self.hotspot_count} '
            f'max_intensity={self.max_intensity:.1f} solver_ok={self.solver_ok} '
            f'candidate_valid={self.candidate_valid} reason={self.last_reason or "ok"}'
        )
        self._log_solve_if_changed(solve)

        if self.candidate_valid:
            self._last_logged_reason = ''
            self.auto_trigger_latched = False if not self.auto_trigger_on_detection else self.auto_trigger_latched
        self._publish_visual_state()
        self._publish_status()

        if (
            self.candidate_valid
            and self.candidate_consistent
            and self.auto_trigger_on_detection
            and not self.auto_trigger_latched
        ):
            self._commit_candidate(source='auto-detection')
            self.auto_trigger_latched = True

    def tick_callback(self, msg: Bool) -> None:
        if not msg.data:
            return

        accepted, _message = self._commit_candidate(source='tick')
        if not accepted:
            self.tick_pub.publish(Bool(data=False))
            return

        self.tick_pub.publish(Bool(data=False))

    def _handle_commit_service(self, _request: Trigger.Request, response: Trigger.Response) -> Trigger.Response:
        accepted, message = self._commit_candidate(source='service')
        response.success = bool(accepted)
        response.message = str(message)
        return response

    def _handle_set_enabled_service(
        self,
        request: SetBool.Request,
        response: SetBool.Response,
    ) -> SetBool.Response:
        response.success = True
        response.message = self._apply_detector_enabled(bool(request.data), source='service')
        return response

    def _apply_detector_enabled(self, enabled: bool, *, source: str) -> str:
        requested = bool(enabled)
        previous = bool(getattr(self, 'detector_enabled', False))
        if requested == previous:
            self._publish_status()
            return 'Shelf detector already enabled.' if requested else 'Shelf detector already disabled.'

        self.detector_enabled = requested
        self.candidate_pose = None
        self.candidate_pose_smoothed = None
        self.candidate_scan_time = None
        self.candidate_fresh = False
        self.candidate_consistent = False
        self.candidate_valid = False
        self.candidate_hotspot_count = 0
        self.candidate_confidence = 0.0
        self.candidate_confidence_components = {}
        self.candidate_sigma_x = -1.0
        self.candidate_sigma_y = -1.0
        self.candidate_sigma_yaw = -1.0
        self.candidate_uncertainty_samples = 0
        self.solver_ok = False
        self.hotspot_count = 0
        self.latest_hotspots = []
        self.max_intensity = 0.0
        self.auto_trigger_latched = False
        self._reset_consistency_chain()
        self._accepted_pose_history.clear()
        self._last_logged_solve_pose = None
        self._last_logged_consistency_count = -1
        self._last_logged_candidate_valid_state = None
        self._warned_stale_scan_stamp = False

        if requested:
            self.last_reason = 'waiting_for_scan'
            self.get_logger().info(f'Shelf detector enabled via {source}.')
        else:
            self.last_reason = 'detector_disabled'
            self.get_logger().info(f'Shelf detector disabled via {source}.')

        self._last_logged_reason = self.last_reason
        self._publish_visual_state()
        self._publish_status()
        return 'Shelf detector enabled.' if requested else 'Shelf detector disabled.'

    def _commit_candidate(self, *, source: str) -> Tuple[bool, str]:
        if self.is_navigating or self._nav_send_inflight:
            self.get_logger().warn(
                f'Shelf /tick rejected from {source}: navigation already in progress.'
            )
            self._publish_status()
            return False, 'Shelf navigation already in progress. Wait for the current request to finish.'

        now = self.get_clock().now()
        self.candidate_valid = self._is_manual_commit_ready(now)
        if not self.candidate_valid or self.candidate_pose is None:
            reason = self.last_reason or 'stale_candidate'
            self.get_logger().warn(f'Shelf /tick rejected from {source}: {reason}')
            self._publish_status()
            return False, f'Shelf commit rejected: {reason}'

        if not self.candidate_consistent:
            self.get_logger().warn(
                f'Shelf /tick accepted from {source} using a fresh shelf solve '
                '(consistency latch not stable yet).'
            )

        preferred_pose = self._preferred_candidate_pose()
        if preferred_pose is None:
            self.get_logger().warn(
                f'Shelf /tick rejected from {source}: no preferred candidate pose available.'
            )
            self._publish_status()
            return False, 'Shelf commit rejected: candidate pose missing'

        goal_pose = self._build_goal_pose(preferred_pose)
        committed_pose = self._transform_shelf_pose(
            preferred_pose,
            str(goal_pose.header.frame_id or preferred_pose.frame_id).strip() or preferred_pose.frame_id,
        )
        self.committed_target_pose = committed_pose if committed_pose is not None else preferred_pose
        self.committed_target_time = now

        if self.publish_legacy_goal:
            self.goal_pub.publish(goal_pose)
        if self.publish_goal_pose:
            self.goal_pose_pub.publish(goal_pose)

        self.get_logger().info(
            f'Shelf /tick accepted from {source}: committed target '
            f'x={goal_pose.pose.position.x:.3f} y={goal_pose.pose.position.y:.3f} '
            f'yaw={math.degrees(preferred_pose.yaw):.1f}deg '
            f'frame={goal_pose.header.frame_id}'
        )

        self._publish_visual_state()
        self._publish_status()

        if self.switch_to_nav_mode_on_commit:
            requested_mode = str(self.nav_control_mode or 'zones').strip() or 'zones'
            self.control_mode_pub.publish(StringMsg(data=requested_mode))
            self.get_logger().info(f'Shelf commit requested /control_mode={requested_mode}')

        nav_requested = False
        if self.use_zone_manager_goal_handoff:
            nav_requested = self._request_zone_manager_goal_handoff(goal_pose)
        elif self.enable_nav2_action:
            nav_requested = self._send_nav_goal(goal_pose)

        if self.use_zone_manager_goal_handoff:
            if nav_requested:
                return True, 'Shelf commit accepted. Goal handed off to zone_manager.'
            return False, (
                'Shelf commit was not handed off to zone_manager. '
                'Check zone_manager and shelf detector logs.'
            )

        if self.enable_nav2_action:
            if nav_requested:
                return True, (
                    f'Shelf commit accepted. NavigateToPose request sent in {goal_pose.header.frame_id}.'
                )
            return False, (
                f'Shelf commit was not sent to NavigateToPose. '
                f'Check shelf detector logs.'
            )

        return True, (
            f'Shelf commit accepted. Goal published in {goal_pose.header.frame_id}; '
            f'Nav2 direct action is disabled.'
        )

    def _coerce_scan_time(self, msg: LaserScan, receipt_time: Time) -> Time:
        stamp = msg.header.stamp
        if int(stamp.sec) == 0 and int(stamp.nanosec) == 0:
            return receipt_time

        try:
            scan_time = Time.from_msg(stamp)
        except Exception:
            return receipt_time

        if (scan_time - receipt_time).nanoseconds > int(self.scan_stamp_future_tolerance_sec * 1e9):
            return receipt_time
        if (receipt_time - scan_time).nanoseconds > int(self.scan_stamp_past_tolerance_sec * 1e9):
            if not self._warned_stale_scan_stamp:
                self.get_logger().warn(
                    'LaserScan header stamp is stale relative to receipt time; '
                    'using receipt time for shelf freshness.'
                )
                self._warned_stale_scan_stamp = True
            return receipt_time

        self._warned_stale_scan_stamp = False
        return scan_time

    def _frame_warning_for_scan(self, frame_id: str) -> str:
        warnings: List[str] = []
        configured_laser = str(self.laser_frame or '').strip()
        if configured_laser and configured_laser != frame_id:
            warnings.append(
                f'laser_frame={configured_laser} differs from scan frame {frame_id}; using scan frame.'
            )
        warning_text = ' '.join(warnings).strip()
        if warning_text and warning_text != self._warned_frame_mismatch:
            self.get_logger().warn(warning_text)
            self._warned_frame_mismatch = warning_text
        return warning_text

    def _extract_hotspots(
        self,
        msg: LaserScan,
    ) -> Tuple[List[Hotspot], Optional[str], float]:
        intensities = list(msg.intensities or [])
        max_intensity = 0.0
        if intensities:
            finite_intensities = [float(value) for value in intensities if math.isfinite(float(value))]
            if finite_intensities:
                max_intensity = max(finite_intensities)
        if not intensities:
            self.get_logger().warn('LaserScan does not contain intensities. Shelf detection is disabled.')
            return [], 'no_intensities', max_intensity

        if len(intensities) != len(msg.ranges):
            self.get_logger().warn(
                f'LaserScan intensity length ({len(intensities)}) does not match '
                f'ranges length ({len(msg.ranges)}).'
            )
            return [], 'intensity_length_mismatch', max_intensity

        bright_runs: List[List[int]] = []
        current_run: List[int] = []
        for index, distance in enumerate(msg.ranges):
            if self._bin_is_bright(msg, index):
                if current_run and index == current_run[-1] + 1:
                    current_run.append(index)
                else:
                    if current_run:
                        bright_runs.append(current_run)
                    current_run = [index]
            else:
                if current_run:
                    bright_runs.append(current_run)
                    current_run = []
        if current_run:
            bright_runs.append(current_run)

        merged_runs: List[List[int]] = []
        idx = 0
        while idx < len(bright_runs):
            current = list(bright_runs[idx])
            bridge_used = False
            if idx + 1 < len(bright_runs):
                next_run = bright_runs[idx + 1]
                gap = next_run[0] - current[-1]
                dip_idx = current[-1] + 1
                if (
                    gap == 2
                    and not bridge_used
                    and self._run_is_valid_candidate(msg, current)
                    and self._run_is_valid_candidate(msg, next_run)
                    and self._dip_can_bridge(msg, dip_idx)
                ):
                    current.extend(next_run)
                    bridge_used = True
                    idx += 1
            merged_runs.append(current)
            idx += 1

        hotspots: List[Hotspot] = []
        for run in merged_runs:
            if not self._run_is_valid_candidate(msg, run):
                continue
            hotspot = self._hotspot_from_run(msg, run)
            if hotspot is None:
                continue
            hotspots.append(hotspot)

        hotspots = self._merge_nearby_hotspots(hotspots)
        return hotspots, None, max_intensity

    def _bin_is_bright(self, msg: LaserScan, index: int) -> bool:
        if index < 0 or index >= len(msg.ranges) or index >= len(msg.intensities):
            return False
        distance = float(msg.ranges[index])
        intensity = float(msg.intensities[index])
        if not math.isfinite(distance) or distance <= 0.0:
            return False
        if not math.isfinite(intensity) or intensity < self.intensity_threshold:
            return False
        angle = msg.angle_min + index * msg.angle_increment
        x = distance * math.cos(angle)
        y = distance * math.sin(angle)
        if self.use_roi and not self._point_in_roi(x, y):
            return False
        return True

    def _dip_can_bridge(self, msg: LaserScan, index: int) -> bool:
        if index < 0 or index >= len(msg.ranges) or index >= len(msg.intensities):
            return False
        distance = float(msg.ranges[index])
        intensity = float(msg.intensities[index])
        if not math.isfinite(distance) or distance <= 0.0:
            return False
        if not math.isfinite(intensity):
            return False
        if intensity < (self.intensity_threshold - self.dip_bridge_threshold_margin):
            return False
        angle = msg.angle_min + index * msg.angle_increment
        x = distance * math.cos(angle)
        y = distance * math.sin(angle)
        if self.use_roi and not self._point_in_roi(x, y):
            return False
        return True

    def _run_is_valid_candidate(self, msg: LaserScan, run: Sequence[int]) -> bool:
        if len(run) >= 2:
            return True
        if len(run) != 1:
            return False

        index = int(run[0])
        if index <= 0 or index >= len(msg.intensities) - 1:
            return False

        center = float(msg.intensities[index])
        left = float(msg.intensities[index - 1])
        right = float(msg.intensities[index + 1])
        return (
            center >= (self.intensity_threshold + self.single_bin_hotspot_boost)
            and left < self.intensity_threshold
            and right < self.intensity_threshold
            and center > left
            and center > right
            and self._bin_is_bright(msg, index)
        )

    def _hotspot_from_run(self, msg: LaserScan, run: Sequence[int]) -> Optional[Hotspot]:
        if not run:
            return None

        weighted_sum = 0.0
        total_weight = 0.0
        ranges: List[float] = []
        peak_intensity = -math.inf
        peak_idx = int(run[0])
        for index in run:
            distance = float(msg.ranges[index])
            intensity = float(msg.intensities[index])
            if not math.isfinite(distance) or distance <= 0.0:
                continue
            if not math.isfinite(intensity) or intensity <= 0.0:
                continue
            weighted_sum += float(index) * intensity
            total_weight += intensity
            ranges.append(distance)
            if intensity > peak_intensity:
                peak_intensity = intensity
                peak_idx = int(index)

        if not ranges or total_weight <= 0.0:
            return None

        weighted_index = weighted_sum / total_weight
        angle = msg.angle_min + weighted_index * msg.angle_increment
        median_range = self._median(ranges)
        x = median_range * math.cos(angle)
        y = median_range * math.sin(angle)
        if self.use_roi and not self._point_in_roi(x, y):
            return None

        return Hotspot(
            start_idx=int(run[0]),
            end_idx=int(run[-1]),
            bright_indices=tuple(int(v) for v in run),
            angle=float(angle),
            range_value=float(median_range),
            x=float(x),
            y=float(y),
            peak_intensity=float(peak_intensity),
            synthetic=False,
        )

    def _merge_nearby_hotspots(self, hotspots: Sequence[Hotspot]) -> List[Hotspot]:
        if len(hotspots) <= 1:
            return list(hotspots)

        remaining = sorted(
            list(hotspots),
            key=lambda item: (item.range_value, -item.peak_intensity, item.start_idx),
        )
        merged: List[Hotspot] = []

        while remaining:
            seed = remaining.pop(0)
            group = [seed]
            changed = True
            while changed:
                changed = False
                next_remaining: List[Hotspot] = []
                for candidate in remaining:
                    if any(self._hotspots_should_merge(existing, candidate) for existing in group):
                        group.append(candidate)
                        changed = True
                    else:
                        next_remaining.append(candidate)
                remaining = next_remaining
            merged.append(self._combine_hotspots(group))

        return sorted(merged, key=lambda item: item.angle)

    def _hotspots_should_merge(self, hotspot_a: Hotspot, hotspot_b: Hotspot) -> bool:
        xy_distance = self._distance((hotspot_a.x, hotspot_a.y), (hotspot_b.x, hotspot_b.y))
        if xy_distance > self.hotspot_merge_max_distance_m:
            return False

        range_delta = abs(hotspot_a.range_value - hotspot_b.range_value)
        if range_delta > self.hotspot_merge_max_range_delta_m:
            return False

        angle_delta_deg = math.degrees(abs(self._angle_delta_rad(hotspot_a.angle, hotspot_b.angle)))
        if angle_delta_deg > self.hotspot_merge_max_angle_deg:
            return False

        return True

    def _combine_hotspots(self, group: Sequence[Hotspot]) -> Hotspot:
        if len(group) == 1:
            return group[0]

        total_weight = 0.0
        weighted_angle_x = 0.0
        weighted_angle_y = 0.0
        weighted_x = 0.0
        weighted_y = 0.0
        weighted_range = 0.0
        peak_intensity = -math.inf
        bright_indices: List[int] = []
        start_idx = min(item.start_idx for item in group)
        end_idx = max(item.end_idx for item in group)

        for hotspot in group:
            weight = max(float(hotspot.peak_intensity), 1.0) * max(len(hotspot.bright_indices), 1)
            total_weight += weight
            weighted_angle_x += math.cos(hotspot.angle) * weight
            weighted_angle_y += math.sin(hotspot.angle) * weight
            weighted_x += hotspot.x * weight
            weighted_y += hotspot.y * weight
            weighted_range += hotspot.range_value * weight
            peak_intensity = max(peak_intensity, hotspot.peak_intensity)
            bright_indices.extend(int(v) for v in hotspot.bright_indices)

        if total_weight <= 0.0:
            return max(group, key=lambda item: item.peak_intensity)

        angle = math.atan2(weighted_angle_y, weighted_angle_x)
        return Hotspot(
            start_idx=int(start_idx),
            end_idx=int(end_idx),
            bright_indices=tuple(sorted(set(bright_indices))),
            angle=float(angle),
            range_value=float(weighted_range / total_weight),
            x=float(weighted_x / total_weight),
            y=float(weighted_y / total_weight),
            peak_intensity=float(peak_intensity),
            synthetic=any(bool(item.synthetic) for item in group),
        )

    def _solve_from_two_hotspots(
        self,
        hotspots: Sequence[Hotspot],
        frame_id: str,
        scan_time: Time,
    ) -> Tuple[Optional[ShelfPose], Optional[str]]:
        if len(hotspots) != 2:
            return None, 'too_few_hotspots'

        point_a = (float(hotspots[0].x), float(hotspots[0].y))
        point_b = (float(hotspots[1].x), float(hotspots[1].y))
        visible_width = self._distance(point_a, point_b)
        if visible_width < max(self.left_right_min_separation_m, 1e-3):
            return None, 'ambiguous_ordering'

        if bool(self.require_pair_distance_gate):
            if visible_width < float(self.min_distance) or visible_width > float(self.max_distance):
                return None, 'ambiguous_ordering'

        front_mid = self._midpoint(point_a, point_b)

        segment_x = point_b[0] - point_a[0]
        segment_y = point_b[1] - point_a[1]
        segment_norm = math.hypot(segment_x, segment_y)
        if segment_norm <= 1e-6:
            return None, 'ambiguous_ordering'

        normal_a = (-segment_y / segment_norm, segment_x / segment_norm)
        normal_b = (-normal_a[0], -normal_a[1])
        depth = max(float(self.offset_length), float(self.front_back_min_separation_m))

        candidate_back_a = (
            front_mid[0] + (normal_a[0] * depth),
            front_mid[1] + (normal_a[1] * depth),
        )
        candidate_back_b = (
            front_mid[0] + (normal_b[0] * depth),
            front_mid[1] + (normal_b[1] * depth),
        )

        if math.hypot(candidate_back_a[0], candidate_back_a[1]) >= math.hypot(candidate_back_b[0], candidate_back_b[1]):
            axis_x = candidate_back_a[0] - front_mid[0]
            axis_y = candidate_back_a[1] - front_mid[1]
            back_mid = candidate_back_a
        else:
            axis_x = candidate_back_b[0] - front_mid[0]
            axis_y = candidate_back_b[1] - front_mid[1]
            back_mid = candidate_back_b

        axis_norm = math.hypot(axis_x, axis_y)
        if axis_norm < self.front_back_min_separation_m:
            return None, 'ambiguous_ordering'

        center = (
            front_mid[0] + (0.5 * axis_x),
            front_mid[1] + (0.5 * axis_y),
        )
        left_normal = (-axis_y / axis_norm, axis_x / axis_norm)
        front_ordered = self._order_pair_left_right(hotspots, center, left_normal)
        if front_ordered is None:
            return None, 'ambiguous_ordering'

        front_left, front_right = front_ordered
        back_left_point = (
            float(front_left.x) + axis_x,
            float(front_left.y) + axis_y,
        )
        back_right_point = (
            float(front_right.x) + axis_x,
            float(front_right.y) + axis_y,
        )
        ordered_points = (
            (float(front_left.x), float(front_left.y)),
            (float(front_right.x), float(front_right.y)),
            back_left_point,
            back_right_point,
        )

        yaw = math.atan2(axis_y, axis_x)
        front_intensity_sum = float(front_left.peak_intensity + front_right.peak_intensity)
        back_intensity_sum = 0.0

        return (
            ShelfPose(
                x=float(center[0]),
                y=float(center[1]),
                yaw=float(yaw),
                frame_id=frame_id,
                scan_time=scan_time,
                ordered_points=tuple(ordered_points),
                front_midpoint=(float(front_mid[0]), float(front_mid[1])),
                back_midpoint=(float(back_mid[0]), float(back_mid[1])),
                front_intensity_sum=front_intensity_sum,
                back_intensity_sum=back_intensity_sum,
                intensity_balance_ratio=1.0,
            ),
            None,
        )

    def _reference_pose_for_partial_solve(self, frame_id: str) -> Optional[ShelfPose]:
        now = self.get_clock().now()
        candidates = (
            self._consistency_last_pose,
            self.candidate_pose_smoothed if self.candidate_consistent else None,
            self.candidate_pose if self._is_candidate_fresh(now) else None,
        )
        for pose in candidates:
            if pose is None:
                continue
            if str(pose.frame_id or '') != str(frame_id or ''):
                continue
            return pose
        return None

    def _solve_from_three_hotspots(
        self,
        hotspots: Sequence[Hotspot],
        frame_id: str,
        scan_time: Time,
    ) -> Tuple[Optional[ShelfPose], Optional[str]]:
        if len(hotspots) != 3:
            return None, 'too_few_hotspots'

        items = list(hotspots)
        reference_pose = self._reference_pose_for_partial_solve(frame_id)
        best_solve: Optional[ShelfPose] = None
        best_key: Optional[Tuple[float, ...]] = None

        for anchor_idx in range(len(items)):
            anchor = items[anchor_idx]
            others = [items[idx] for idx in range(len(items)) if idx != anchor_idx]
            inferred_x = float(others[0].x + others[1].x - anchor.x)
            inferred_y = float(others[0].y + others[1].y - anchor.y)
            inferred_hotspot = Hotspot(
                start_idx=-1,
                end_idx=-1,
                bright_indices=tuple(),
                angle=float(math.atan2(inferred_y, inferred_x)),
                range_value=float(math.hypot(inferred_x, inferred_y)),
                x=inferred_x,
                y=inferred_y,
                peak_intensity=0.0,
                synthetic=True,
            )
            solve, solve_failure = self._solve_from_hotspots(
                [*items, inferred_hotspot],
                frame_id,
                scan_time,
            )
            if solve is None:
                continue

            ordered_points = list(solve.ordered_points)
            front_width = self._distance(ordered_points[0], ordered_points[1])
            back_width = self._distance(ordered_points[2], ordered_points[3])
            width_delta = abs(front_width - back_width)
            front_vec = (
                ordered_points[1][0] - ordered_points[0][0],
                ordered_points[1][1] - ordered_points[0][1],
            )
            back_vec = (
                ordered_points[3][0] - ordered_points[2][0],
                ordered_points[3][1] - ordered_points[2][1],
            )
            angle_delta = self._line_angle_delta(front_vec, back_vec)
            ordered_polygon = [
                ordered_points[0],
                ordered_points[1],
                ordered_points[3],
                ordered_points[2],
            ]
            area = self._polygon_area(ordered_polygon)
            synthetic_point = (inferred_x, inferred_y)
            nearest_idx = min(
                range(len(ordered_points)),
                key=lambda idx: self._distance(ordered_points[idx], synthetic_point),
            )
            synthetic_front_penalty = 0.0 if nearest_idx >= 2 else 1.0
            depth_gap = max(
                0.0,
                math.hypot(solve.back_midpoint[0], solve.back_midpoint[1])
                - math.hypot(solve.front_midpoint[0], solve.front_midpoint[1]),
            )
            reference_yaw_delta = 0.0
            reference_center_delta = 0.0
            reference_corner_delta = 0.0
            if reference_pose is not None:
                reference_yaw_delta = abs(
                    self._angle_delta_rad(float(solve.yaw), float(reference_pose.yaw))
                )
                reference_center_delta = self._distance(
                    (float(solve.x), float(solve.y)),
                    (float(reference_pose.x), float(reference_pose.y)),
                )
                reference_corner_delta = max(
                    self._distance(current, previous)
                    for current, previous in zip(ordered_points, reference_pose.ordered_points)
                )
            key = (
                synthetic_front_penalty,
                float(reference_yaw_delta),
                float(reference_center_delta),
                float(reference_corner_delta),
                float(width_delta),
                float(angle_delta),
                -float(area),
                -float(depth_gap),
            )
            if best_key is None or key < best_key:
                best_key = key
                best_solve = solve

        if best_solve is not None:
            return best_solve, None
        return None, 'ambiguous_ordering'

    def _solve_from_hotspots(
        self,
        hotspots: Sequence[Hotspot],
        frame_id: str,
        scan_time: Time,
    ) -> Tuple[Optional[ShelfPose], Optional[str]]:
        if len(hotspots) != 4:
            return None, 'too_few_hotspots'

        points = [(float(h.x), float(h.y)) for h in hotspots]
        hull = self._convex_hull(points)
        if len(hull) != 4:
            return None, 'degenerate_geometry'
        if self._polygon_area(hull) < self.degenerate_area_threshold_m2:
            return None, 'degenerate_geometry'

        pairing = self._select_front_back_pairs(hotspots)
        if pairing is None:
            return None, 'ambiguous_ordering'
        front_pair, back_pair, front_mid, back_mid = pairing

        axis_x = back_mid[0] - front_mid[0]
        axis_y = back_mid[1] - front_mid[1]
        axis_norm = math.hypot(axis_x, axis_y)
        if axis_norm < self.front_back_min_separation_m:
            return None, 'ambiguous_ordering'

        center = (
            sum(point[0] for point in points) / 4.0,
            sum(point[1] for point in points) / 4.0,
        )
        left_normal = (-axis_y / axis_norm, axis_x / axis_norm)

        front_ordered = self._order_pair_left_right(front_pair, center, left_normal)
        if front_ordered is None:
            return None, 'ambiguous_ordering'
        back_ordered = self._order_pair_left_right(back_pair, center, left_normal)
        if back_ordered is None:
            return None, 'ambiguous_ordering'

        front_left, front_right = front_ordered
        back_left, back_right = back_ordered
        ordered_points = (
            (front_left.x, front_left.y),
            (front_right.x, front_right.y),
            (back_left.x, back_left.y),
            (back_right.x, back_right.y),
        )

        front_width = self._distance(ordered_points[0], ordered_points[1])
        back_width = self._distance(ordered_points[2], ordered_points[3])
        if (
            front_width < self.left_right_min_separation_m
            or back_width < self.left_right_min_separation_m
        ):
            return None, 'ambiguous_ordering'

        front_vec = (
            ordered_points[1][0] - ordered_points[0][0],
            ordered_points[1][1] - ordered_points[0][1],
        )
        back_vec = (
            ordered_points[3][0] - ordered_points[2][0],
            ordered_points[3][1] - ordered_points[2][1],
        )
        if self._line_angle_delta(front_vec, back_vec) > math.radians(20.0):
            return None, 'degenerate_geometry'

        ordered_polygon = [
            ordered_points[0],
            ordered_points[1],
            ordered_points[3],
            ordered_points[2],
        ]
        if self._polygon_area(ordered_polygon) < self.degenerate_area_threshold_m2:
            return None, 'degenerate_geometry'

        yaw = math.atan2(back_mid[1] - front_mid[1], back_mid[0] - front_mid[0])
        front_intensity_sum = float(front_left.peak_intensity + front_right.peak_intensity)
        back_intensity_sum = float(back_left.peak_intensity + back_right.peak_intensity)
        intensity_balance_ratio = (
            abs(front_intensity_sum - back_intensity_sum)
            / max(front_intensity_sum, back_intensity_sum, 1.0)
        )
        return (
            ShelfPose(
                x=float(center[0]),
                y=float(center[1]),
                yaw=float(yaw),
                frame_id=frame_id,
                scan_time=scan_time,
                ordered_points=tuple(ordered_points),
                front_midpoint=(float(front_mid[0]), float(front_mid[1])),
                back_midpoint=(float(back_mid[0]), float(back_mid[1])),
                front_intensity_sum=front_intensity_sum,
                back_intensity_sum=back_intensity_sum,
                intensity_balance_ratio=float(intensity_balance_ratio),
            ),
            None,
        )

    def _select_front_back_pairs(
        self,
        hotspots: Sequence[Hotspot],
    ) -> Optional[
        Tuple[
            Tuple[Hotspot, Hotspot],
            Tuple[Hotspot, Hotspot],
            Tuple[float, float],
            Tuple[float, float],
        ]
    ]:
        if len(hotspots) != 4:
            return None

        items = list(hotspots)
        pairings = (
            ((0, 1), (2, 3)),
            ((0, 2), (1, 3)),
            ((0, 3), (1, 2)),
        )
        best_result = None
        best_key = None

        for pair_a_idx, pair_b_idx in pairings:
            pair_a = (items[pair_a_idx[0]], items[pair_a_idx[1]])
            pair_b = (items[pair_b_idx[0]], items[pair_b_idx[1]])

            width_a = self._distance((pair_a[0].x, pair_a[0].y), (pair_a[1].x, pair_a[1].y))
            width_b = self._distance((pair_b[0].x, pair_b[0].y), (pair_b[1].x, pair_b[1].y))
            if (
                width_a < self.left_right_min_separation_m
                or width_b < self.left_right_min_separation_m
            ):
                continue

            mid_a = self._midpoint((pair_a[0].x, pair_a[0].y), (pair_a[1].x, pair_a[1].y))
            mid_b = self._midpoint((pair_b[0].x, pair_b[0].y), (pair_b[1].x, pair_b[1].y))
            mid_sep = self._distance(mid_a, mid_b)
            if mid_sep < self.front_back_min_separation_m:
                continue

            vec_a = (pair_a[1].x - pair_a[0].x, pair_a[1].y - pair_a[0].y)
            vec_b = (pair_b[1].x - pair_b[0].x, pair_b[1].y - pair_b[0].y)
            angle_delta = self._line_angle_delta(vec_a, vec_b)
            if angle_delta > math.radians(20.0):
                continue

            mid_a_dist = math.hypot(mid_a[0], mid_a[1])
            mid_b_dist = math.hypot(mid_b[0], mid_b[1])
            if abs(mid_a_dist - mid_b_dist) < self.front_back_min_separation_m:
                continue

            if mid_a_dist <= mid_b_dist:
                front_pair = pair_a
                back_pair = pair_b
                front_mid = mid_a
                back_mid = mid_b
            else:
                front_pair = pair_b
                back_pair = pair_a
                front_mid = mid_b
                back_mid = mid_a

            front_intensity_sum = float(front_pair[0].peak_intensity + front_pair[1].peak_intensity)
            back_intensity_sum = float(back_pair[0].peak_intensity + back_pair[1].peak_intensity)
            intensity_gap_ratio = (
                (front_intensity_sum - back_intensity_sum)
                / max(front_intensity_sum, back_intensity_sum, 1.0)
            )
            has_synthetic = any(bool(item.synthetic) for item in (*front_pair, *back_pair))
            if (
                self.pairing_require_front_brighter
                and not has_synthetic
                and intensity_gap_ratio < float(self.pairing_front_back_intensity_margin)
            ):
                continue

            avg_width = 0.5 * (width_a + width_b)
            key = (
                1 if has_synthetic else 0,
                0 if intensity_gap_ratio >= 0.0 else 1,
                abs(width_a - width_b),
                -avg_width,
                -intensity_gap_ratio,
                angle_delta,
                mid_sep,
            )
            if best_key is None or key < best_key:
                best_key = key
                best_result = (
                    front_pair,
                    back_pair,
                    (float(front_mid[0]), float(front_mid[1])),
                    (float(back_mid[0]), float(back_mid[1])),
                )

        return best_result

    def _order_pair_left_right(
        self,
        pair: Sequence[Hotspot],
        center: Tuple[float, float],
        left_normal: Tuple[float, float],
    ) -> Optional[Tuple[Hotspot, Hotspot]]:
        if len(pair) != 2:
            return None

        signed_values: List[Tuple[float, Hotspot]] = []
        for hotspot in pair:
            rel_x = hotspot.x - center[0]
            rel_y = hotspot.y - center[1]
            signed_values.append((rel_x * left_normal[0] + rel_y * left_normal[1], hotspot))

        if abs(signed_values[0][0] - signed_values[1][0]) < self.left_right_min_separation_m:
            return None

        signed_values.sort(key=lambda item: item[0], reverse=True)
        return signed_values[0][1], signed_values[1][1]

    def _advance_consistency(self, solve: ShelfPose) -> int:
        if self._consistency_last_pose is None:
            self._consistency_last_pose = solve
            self.consistency_count = 1
            return self.consistency_count

        if solve.frame_id != self._consistency_last_pose.frame_id:
            self._reset_consistency_chain()
            return self.consistency_count

        center_delta = math.hypot(
            solve.x - self._consistency_last_pose.x,
            solve.y - self._consistency_last_pose.y,
        )
        yaw_delta_deg = math.degrees(
            abs(self._angle_delta_rad(solve.yaw, self._consistency_last_pose.yaw))
        )
        corners_ok = all(
            self._distance(current, previous) <= self.candidate_corner_tolerance_m
            for current, previous in zip(solve.ordered_points, self._consistency_last_pose.ordered_points)
        )
        if (
            center_delta <= self.candidate_center_tolerance_m
            and yaw_delta_deg <= self.candidate_yaw_tolerance_deg
            and corners_ok
        ):
            self._consistency_last_pose = solve
            self.consistency_count = min(
                self.consistency_count + 1,
                self.candidate_consistency_frames,
            )
            return self.consistency_count

        self._reset_consistency_chain()
        return self.consistency_count

    def _log_solve_if_changed(self, solve: ShelfPose) -> None:
        should_log = False
        previous = self._last_logged_solve_pose

        if previous is None:
            should_log = True
        elif solve.frame_id != previous.frame_id:
            should_log = True
        elif self.consistency_count != self._last_logged_consistency_count:
            should_log = True
        elif self.candidate_valid != self._last_logged_candidate_valid_state:
            should_log = True
        else:
            center_delta = math.hypot(solve.x - previous.x, solve.y - previous.y)
            yaw_delta_deg = math.degrees(abs(self._angle_delta_rad(solve.yaw, previous.yaw)))
            if (
                center_delta > self.candidate_center_tolerance_m
                or yaw_delta_deg > self.candidate_yaw_tolerance_deg
            ):
                should_log = True

        if not should_log:
            return

        self.get_logger().info(
            f'Shelf solve frame={solve.frame_id} hotspots={self.candidate_hotspot_count} '
            f'center=({solve.x:.3f}, {solve.y:.3f}) '
            f'yaw={math.degrees(solve.yaw):.1f}deg '
            f'consistency={self.consistency_count}/{self.candidate_consistency_frames}'
        )
        self._last_logged_solve_pose = solve
        self._last_logged_consistency_count = int(self.consistency_count)
        self._last_logged_candidate_valid_state = bool(self.candidate_valid)

    def _reset_consistency_chain(self) -> None:
        self._consistency_last_pose = None
        self.consistency_count = 0

    def _preferred_candidate_pose(self) -> Optional[ShelfPose]:
        if self.candidate_consistent and self.candidate_pose_smoothed is not None:
            return self.candidate_pose_smoothed
        return self.candidate_pose

    def _resize_accepted_pose_history(self, window_size: int) -> None:
        retained = list(self._accepted_pose_history)[-max(1, int(window_size)):]
        self._accepted_pose_history = deque(retained, maxlen=max(1, int(window_size)))
        self._update_uncertainty_from_history()

    def _clear_smoothed_candidate_state(self) -> None:
        self.candidate_pose_smoothed = None
        self._accepted_pose_history.clear()
        self.candidate_sigma_x = -1.0
        self.candidate_sigma_y = -1.0
        self.candidate_sigma_yaw = -1.0
        self.candidate_uncertainty_samples = 0

    def _update_smoothed_candidate(self, solve: ShelfPose) -> None:
        self.candidate_pose_smoothed = self._smooth_shelf_pose(
            self.candidate_pose_smoothed,
            solve,
            self.pose_smoothing_alpha,
        )
        self._accepted_pose_history.append(solve)
        self._update_uncertainty_from_history()

    def _smooth_shelf_pose(
        self,
        previous_pose: Optional[ShelfPose],
        current_pose: ShelfPose,
        alpha: float,
    ) -> ShelfPose:
        if previous_pose is None or previous_pose.frame_id != current_pose.frame_id:
            return current_pose

        blend = self._ema_value
        blend_point = lambda prev, curr: (  # noqa: E731
            float(blend(prev[0], curr[0], alpha)),
            float(blend(prev[1], curr[1], alpha)),
        )

        ordered_points = tuple(
            blend_point(previous, current)
            for previous, current in zip(previous_pose.ordered_points, current_pose.ordered_points)
        )
        front_midpoint = blend_point(previous_pose.front_midpoint, current_pose.front_midpoint)
        back_midpoint = blend_point(previous_pose.back_midpoint, current_pose.back_midpoint)

        return ShelfPose(
            x=float(blend(previous_pose.x, current_pose.x, alpha)),
            y=float(blend(previous_pose.y, current_pose.y, alpha)),
            yaw=float(self._ema_angle(previous_pose.yaw, current_pose.yaw, alpha)),
            frame_id=current_pose.frame_id,
            scan_time=current_pose.scan_time,
            ordered_points=ordered_points,
            front_midpoint=front_midpoint,
            back_midpoint=back_midpoint,
            front_intensity_sum=float(current_pose.front_intensity_sum),
            back_intensity_sum=float(current_pose.back_intensity_sum),
            intensity_balance_ratio=float(current_pose.intensity_balance_ratio),
        )

    def _update_uncertainty_from_history(self) -> None:
        history = list(self._accepted_pose_history)
        self.candidate_uncertainty_samples = len(history)
        if not history:
            self.candidate_sigma_x = -1.0
            self.candidate_sigma_y = -1.0
            self.candidate_sigma_yaw = -1.0
            return
        if len(history) == 1:
            self.candidate_sigma_x = 0.0
            self.candidate_sigma_y = 0.0
            self.candidate_sigma_yaw = 0.0
            return

        self.candidate_sigma_x = self._stddev([pose.x for pose in history])
        self.candidate_sigma_y = self._stddev([pose.y for pose in history])
        self.candidate_sigma_yaw = self._circular_stddev([pose.yaw for pose in history])

    def _score_candidate(
        self,
        solve: ShelfPose,
        *,
        hotspot_count: int,
        consistency_count: int,
    ) -> Tuple[float, Dict[str, float]]:
        front_width = self._distance(solve.ordered_points[0], solve.ordered_points[1])
        back_width = self._distance(solve.ordered_points[2], solve.ordered_points[3])
        width_delta = abs(front_width - back_width)
        width_score = self._normalized_inverse_score(
            width_delta,
            max(0.02, float(self.shelf_width_tolerance)),
        )

        front_vec = (
            solve.ordered_points[1][0] - solve.ordered_points[0][0],
            solve.ordered_points[1][1] - solve.ordered_points[0][1],
        )
        back_vec = (
            solve.ordered_points[3][0] - solve.ordered_points[2][0],
            solve.ordered_points[3][1] - solve.ordered_points[2][1],
        )
        angle_delta = self._line_angle_delta(front_vec, back_vec)
        parallel_score = self._normalized_inverse_score(
            angle_delta,
            math.radians(20.0),
        )

        area = self._polygon_area(
            [
                solve.ordered_points[0],
                solve.ordered_points[1],
                solve.ordered_points[3],
                solve.ordered_points[2],
            ]
        )
        area_threshold = max(self.degenerate_area_threshold_m2, 1e-4)
        area_score = self._clamp01(
            (area - area_threshold) / max(area_threshold * 3.0, 1e-3)
        )
        geometry_score = (
            (0.40 * width_score)
            + (0.35 * parallel_score)
            + (0.25 * area_score)
        )

        completeness_score = self._clamp01(float(hotspot_count) / 4.0)
        front_back_gap_ratio = (
            (float(solve.front_intensity_sum) - float(solve.back_intensity_sum))
            / max(float(solve.front_intensity_sum), float(solve.back_intensity_sum), 1.0)
        )
        if self.pairing_require_front_brighter:
            brightness_score = self._clamp01(
                (front_back_gap_ratio - float(self.pairing_front_back_intensity_margin))
                / max(0.10, 1.0 - float(self.pairing_front_back_intensity_margin))
            )
        else:
            brightness_score = self._clamp01(1.0 - float(solve.intensity_balance_ratio))
        temporal_score = self._clamp01(
            float(consistency_count) / max(1.0, float(self.candidate_consistency_frames))
        )

        confidence = (
            (0.40 * geometry_score)
            + (0.15 * completeness_score)
            + (0.15 * brightness_score)
            + (0.30 * temporal_score)
        )
        components = {
            'geometry_consistency': float(geometry_score),
            'hotspot_completeness': float(completeness_score),
            'brightness_consistency': float(brightness_score),
            'temporal_stability': float(temporal_score),
        }
        return float(self._clamp01(confidence)), components

    def _is_manual_commit_ready(self, now: Optional[Time] = None) -> bool:
        current_time = now or self.get_clock().now()
        self.candidate_fresh = self._is_candidate_fresh(current_time)
        return bool(
            self.detector_enabled
            and
            self.candidate_pose
            and self.candidate_fresh
            and self.solver_ok
        )

    def _set_invalid_candidate(
        self,
        reason: str,
        *,
        frame_id: str,
        scan_time: Time,
        max_intensity: float,
        hotspot_count: int,
        solver_ok: bool,
    ) -> None:
        now = self.get_clock().now()
        previously_valid = bool(self.candidate_valid)
        had_candidate_pose = self.candidate_pose is not None
        hold_candidate = self._should_hold_candidate_on_invalid(reason, now)

        if hold_candidate:
            self.active_scan_frame = frame_id
            self.frame_mismatch_warning = self._frame_warning_for_scan(frame_id)
            self.max_intensity = max_intensity
            self.hotspot_count = hotspot_count
            self.solver_ok = self.candidate_pose is not None
            self.candidate_fresh = (
                self.candidate_scan_time is not None and self._is_candidate_fresh(now)
            )
            self.candidate_consistent = (
                self.consistency_count >= self.candidate_consistency_frames
            )
            self.candidate_valid = self._is_manual_commit_ready(now)
            self.last_reason = reason

            if reason != self._last_logged_reason:
                candidate_age_sec = self._age_seconds(now, self.candidate_scan_time)
                self.get_logger().info(
                    f'Shelf candidate transient loss: {reason} '
                    f'(holding last solve, age={candidate_age_sec:.2f}s)'
                )
                self._last_logged_reason = reason

            self._publish_visual_state()
            self._publish_status()
            return

        if reason in {'no_intensities', 'intensity_length_mismatch', 'too_few_hotspots', 'too_many_hotspots'}:
            self.candidate_pose = None
            self.candidate_scan_time = None
            self._reset_consistency_chain()
            self._clear_smoothed_candidate_state()
            self.candidate_hotspot_count = 0
        elif reason in {'ambiguous_ordering', 'degenerate_geometry'}:
            self.candidate_pose = None
            self.candidate_scan_time = scan_time
            self._reset_consistency_chain()
            self._clear_smoothed_candidate_state()
            self.candidate_hotspot_count = 0
        elif reason == 'inconsistent_solve':
            self.candidate_scan_time = scan_time
        elif reason == 'stale_candidate':
            self.candidate_pose = None
            self.candidate_scan_time = None
            self._reset_consistency_chain()
            self._clear_smoothed_candidate_state()
            self.candidate_hotspot_count = 0

        self.active_scan_frame = frame_id
        self.frame_mismatch_warning = self._frame_warning_for_scan(frame_id)
        self.max_intensity = max_intensity
        self.hotspot_count = hotspot_count
        self.solver_ok = solver_ok
        if not solver_ok or self.candidate_pose is None:
            self.candidate_confidence = 0.0
            self.candidate_confidence_components = {}
        self.candidate_fresh = (
            self.candidate_scan_time is not None and self._is_candidate_fresh(now)
        )
        self.candidate_consistent = False
        self.candidate_valid = False
        self.last_reason = reason
        self.auto_trigger_latched = False
        self._last_logged_solve_pose = None
        self._last_logged_consistency_count = -1
        self._last_logged_candidate_valid_state = False

        self.get_logger().debug(
            f'Shelf scan frame={frame_id} hotspots={hotspot_count} '
            f'max_intensity={max_intensity:.1f} candidate_valid={self.candidate_valid} '
            f'solver_ok={self.solver_ok} reason={reason}'
        )

        if reason != self._last_logged_reason or previously_valid or had_candidate_pose:
            if reason == 'stale_candidate' and self.committed_target_pose is not None:
                self.get_logger().info(
                    'Shelf candidate expired after commit; committed target retained.'
                )
            else:
                self.get_logger().info(f'Shelf candidate invalid: {reason}')
            self._last_logged_reason = reason

        self._publish_visual_state()
        self._publish_status()

    def _should_hold_candidate_on_invalid(self, reason: str, now: Time) -> bool:
        if reason not in {
            'too_few_hotspots',
            'too_many_hotspots',
            'ambiguous_ordering',
            'degenerate_geometry',
        }:
            return False
        if self.candidate_pose is None or self.candidate_scan_time is None:
            return False

        dropout_tolerance_sec = min(
            max(0.0, float(self.candidate_dropout_tolerance_sec)),
            max(0.0, float(self.detection_timeout)),
        )
        if dropout_tolerance_sec <= 0.0:
            return False

        return self._age_seconds(now, self.candidate_scan_time) <= dropout_tolerance_sec

    def _is_candidate_fresh(self, now: Time) -> bool:
        if self.candidate_scan_time is None:
            return False
        age_sec = (now - self.candidate_scan_time).nanoseconds / 1e9
        return age_sec <= self.detection_timeout

    def _build_goal_pose(self, shelf_pose: ShelfPose) -> PoseStamped:
        target_x, target_y, target_yaw = self._goal_target_in_shelf_frame(shelf_pose)
        source_goal = PoseStamped()
        source_goal.header.stamp = self.get_clock().now().to_msg()
        source_goal.header.frame_id = shelf_pose.frame_id
        source_goal.pose.position.x = target_x
        source_goal.pose.position.y = target_y
        source_goal.pose.position.z = 0.0
        source_goal.pose.orientation = self._quaternion_msg(
            self._euler_to_quaternion(0.0, 0.0, target_yaw)
        )

        target_frame = str(self.goal_frame or '').strip() or shelf_pose.frame_id
        if target_frame == shelf_pose.frame_id:
            return source_goal

        transformed_goal = self._transform_goal_pose(source_goal, target_frame)
        if transformed_goal is not None:
            return transformed_goal

        self.get_logger().warn(
            f'Failed to transform shelf goal from {shelf_pose.frame_id} to {target_frame}; '
            f'falling back to source frame.'
        )
        return source_goal

    def _goal_target_in_shelf_frame(self, shelf_pose: ShelfPose) -> Tuple[float, float, float]:
        axis_x = math.cos(shelf_pose.yaw)
        axis_y = math.sin(shelf_pose.yaw)
        left_x = -axis_y
        left_y = axis_x

        # Use the solved shelf center as the committed target. zone_manager
        # derives a ghost approach point in front of the shelf from this
        # center target, aligns there, and then performs the straight insert.
        ref_x = float(shelf_pose.x)
        ref_y = float(shelf_pose.y)
        target_x = (
            float(ref_x)
            + (axis_x * float(self.goal_offset_x))
            + (left_x * float(self.goal_offset_y))
        )
        target_y = (
            float(ref_y)
            + (axis_y * float(self.goal_offset_x))
            + (left_y * float(self.goal_offset_y))
        )
        return float(target_x), float(target_y), float(shelf_pose.yaw)

    def _transform_goal_pose(self, goal_pose: PoseStamped, target_frame: str) -> Optional[PoseStamped]:
        source_frame = str(goal_pose.header.frame_id or '').strip() or target_frame
        if source_frame == target_frame:
            return goal_pose

        try:
            transform = self.tf_buffer.lookup_transform(
                target_frame,
                source_frame,
                Time(),
                timeout=Duration(seconds=self.goal_transform_timeout_sec),
            )
        except TransformException as exc:
            self.get_logger().warn(
                f'Unable to transform shelf goal from {source_frame} to {target_frame}: {exc}'
            )
            return None

        tx = float(transform.transform.translation.x)
        ty = float(transform.transform.translation.y)
        q = transform.transform.rotation
        tf_yaw = self._yaw_from_quaternion(q.x, q.y, q.z, q.w)

        local_x = float(goal_pose.pose.position.x)
        local_y = float(goal_pose.pose.position.y)
        world_x = tx + math.cos(tf_yaw) * local_x - math.sin(tf_yaw) * local_y
        world_y = ty + math.sin(tf_yaw) * local_x + math.cos(tf_yaw) * local_y
        local_goal_yaw = self._yaw_from_quaternion(
            goal_pose.pose.orientation.x,
            goal_pose.pose.orientation.y,
            goal_pose.pose.orientation.z,
            goal_pose.pose.orientation.w,
        )
        world_yaw = tf_yaw + local_goal_yaw

        transformed = PoseStamped()
        transformed.header.stamp = self.get_clock().now().to_msg()
        transformed.header.frame_id = target_frame
        transformed.pose.position.x = world_x
        transformed.pose.position.y = world_y
        transformed.pose.position.z = 0.0
        transformed.pose.orientation = self._quaternion_msg(
            self._euler_to_quaternion(0.0, 0.0, world_yaw)
        )
        return transformed

    def _transform_shelf_pose(
        self,
        shelf_pose: ShelfPose,
        target_frame: str,
    ) -> Optional[ShelfPose]:
        source_frame = str(shelf_pose.frame_id or '').strip() or target_frame
        normalized_target_frame = str(target_frame or '').strip() or source_frame
        if source_frame == normalized_target_frame:
            return shelf_pose

        try:
            transform = self.tf_buffer.lookup_transform(
                normalized_target_frame,
                source_frame,
                Time(),
                timeout=Duration(seconds=self.goal_transform_timeout_sec),
            )
        except TransformException as exc:
            self.get_logger().warn(
                f'Unable to transform shelf geometry from {source_frame} to '
                f'{normalized_target_frame}: {exc}'
            )
            return None

        tx = float(transform.transform.translation.x)
        ty = float(transform.transform.translation.y)
        q = transform.transform.rotation
        tf_yaw = self._yaw_from_quaternion(q.x, q.y, q.z, q.w)
        cos_yaw = math.cos(tf_yaw)
        sin_yaw = math.sin(tf_yaw)

        def transform_point(point: Tuple[float, float]) -> Tuple[float, float]:
            px = float(point[0])
            py = float(point[1])
            return (
                tx + (cos_yaw * px) - (sin_yaw * py),
                ty + (sin_yaw * px) + (cos_yaw * py),
            )

        ordered_points = tuple(transform_point(point) for point in shelf_pose.ordered_points)
        center_x, center_y = transform_point((shelf_pose.x, shelf_pose.y))
        front_midpoint = transform_point(shelf_pose.front_midpoint)
        back_midpoint = transform_point(shelf_pose.back_midpoint)

        return ShelfPose(
            x=float(center_x),
            y=float(center_y),
            yaw=float(tf_yaw + float(shelf_pose.yaw)),
            frame_id=normalized_target_frame,
            scan_time=shelf_pose.scan_time,
            ordered_points=ordered_points,
            front_midpoint=front_midpoint,
            back_midpoint=back_midpoint,
            front_intensity_sum=float(shelf_pose.front_intensity_sum),
            back_intensity_sum=float(shelf_pose.back_intensity_sum),
            intensity_balance_ratio=float(shelf_pose.intensity_balance_ratio),
        )

    def _send_nav_goal(self, goal_pose: PoseStamped) -> bool:
        if self.nav_client is None or NavigateToPose is None:
            self.get_logger().warn('Nav2 action client is unavailable. Skipping action request.')
            return False
        with self._nav_send_lock:
            if self.is_navigating or self._nav_send_inflight:
                self.get_logger().warn('Navigation request ignored because another goal is active.')
                return False
            self._nav_send_inflight = True

        threading.Thread(
            target=self._send_nav_goal_worker,
            args=(goal_pose,),
            daemon=True,
        ).start()
        return True

    def _request_zone_manager_goal_handoff(self, goal_pose: PoseStamped) -> bool:
        del goal_pose
        if self.zone_manager_handoff_client is None:
            self.get_logger().warn('zone_manager goal handoff client is unavailable.')
            return False

        with self._nav_send_lock:
            if self._nav_send_inflight:
                self.get_logger().warn('zone_manager goal handoff already in flight.')
                return False
            self._nav_send_inflight = True

        settle_sec = max(0.0, float(self.goal_pose_handoff_settle_sec))
        if settle_sec > 0.0:
            time.sleep(settle_sec)

        if not self.zone_manager_handoff_client.wait_for_service(timeout_sec=1.5):
            with self._nav_send_lock:
                self._nav_send_inflight = False
            self.get_logger().warn(
                f'zone_manager goal handoff service {self.zone_manager_goal_handoff_service} is unavailable.'
            )
            return False

        request = Trigger.Request()
        future = self.zone_manager_handoff_client.call_async(request)
        future.add_done_callback(self._on_zone_manager_handoff_response)
        self.get_logger().info('Queued zone_manager goal handoff request.')
        return True

    def _on_zone_manager_handoff_response(self, future) -> None:
        with self._nav_send_lock:
            self._nav_send_inflight = False

        try:
            response = future.result()
        except Exception as exc:
            self.get_logger().warn(f'zone_manager goal handoff failed: {exc}')
            return

        if response is None:
            self.get_logger().warn('zone_manager goal handoff returned no response.')
            return

        if not bool(getattr(response, 'success', False)):
            self.get_logger().warn(
                f'zone_manager goal handoff rejected: {getattr(response, "message", "")}'
            )
            return

        self.get_logger().info(
            f'zone_manager goal handoff accepted: {getattr(response, "message", "")}'
        )

    def _send_nav_goal_worker(self, goal_pose: PoseStamped) -> None:
        try:
            if not self.nav_client.wait_for_server(timeout_sec=2.0):
                self.get_logger().warn('NavigateToPose action server is not available.')
                return

            bt_ready, bt_msg = self._ensure_bt_navigator_active_blocking()
            if not bt_ready:
                bt_msg_lower = str(bt_msg or '').lower()
                if 'timed out' in bt_msg_lower or 'unavailable' in bt_msg_lower:
                    self.get_logger().warn(
                        f'bt_navigator lifecycle probe failed ({bt_msg}); '
                        'proceeding because NavigateToPose action server is reachable.'
                    )
                else:
                    self.get_logger().warn(f'NavigateToPose unavailable: {bt_msg}')
                    return

            send_pose = self._prepare_approach_pose(goal_pose)
            nav_accept_attempts = max(1, int(self.nav_goal_accept_attempts))
            for accept_attempt in range(1, nav_accept_attempts + 1):
                nav_goal = NavigateToPose.Goal()
                nav_goal.pose = self._clone_pose_stamped(send_pose)
                nav_goal.pose.header.stamp = self.get_clock().now().to_msg()
                self.get_logger().info(
                    f'Sending NavigateToPose goal attempt {accept_attempt}/{nav_accept_attempts} '
                    f'x={nav_goal.pose.pose.position.x:.3f} '
                    f'y={nav_goal.pose.pose.position.y:.3f} '
                    f'frame={nav_goal.pose.header.frame_id}'
                )
                future = self.nav_client.send_goal_async(
                    nav_goal,
                    feedback_callback=self._on_nav_feedback,
                )
                goal_handle = self._wait_future_blocking(future, timeout_sec=2.0)
                if goal_handle is None:
                    self.get_logger().warn(
                        f'NavigateToPose goal attempt {accept_attempt}/{nav_accept_attempts} timed out.'
                    )
                elif bool(getattr(goal_handle, 'accepted', False)):
                    self.is_navigating = True
                    self._nav_goal_handle = goal_handle
                    self.get_logger().info('NavigateToPose goal accepted.')
                    result_future = goal_handle.get_result_async()
                    result_future.add_done_callback(self._on_nav_result)
                    return
                else:
                    self.get_logger().warn(
                        f'NavigateToPose goal was rejected on attempt {accept_attempt}/{nav_accept_attempts}.'
                    )

                if accept_attempt < nav_accept_attempts:
                    time.sleep(0.25)

            self.get_logger().warn(
                f'NavigateToPose goal failed after {nav_accept_attempts} attempts. '
                'Check NAV stack active and localization initialized.'
            )
        finally:
            with self._nav_send_lock:
                if not self.is_navigating:
                    self._nav_send_inflight = False

    def _prepare_approach_pose(self, goal_pose: PoseStamped) -> PoseStamped:
        send_pose = self._clone_pose_stamped(goal_pose)
        if not self.approach_with_current_heading:
            return send_pose

        robot_yaw = self._lookup_robot_yaw_in_frame(send_pose.header.frame_id)
        if robot_yaw is None:
            return send_pose

        send_pose.pose.orientation = self._quaternion_msg(
            self._euler_to_quaternion(0.0, 0.0, robot_yaw)
        )
        self.get_logger().info(
            f'Using current robot heading {math.degrees(robot_yaw):.1f}deg for shelf approach.'
        )
        return send_pose

    def _lookup_robot_yaw_in_frame(self, target_frame: str) -> Optional[float]:
        candidate_frames = []
        preferred = str(self.robot_base_frame or '').strip()
        if preferred:
            candidate_frames.append(preferred)
        for fallback in ('base_footprint', 'base_link'):
            if fallback not in candidate_frames:
                candidate_frames.append(fallback)

        for base_frame in candidate_frames:
            try:
                transform = self.tf_buffer.lookup_transform(
                    target_frame,
                    base_frame,
                    Time(),
                    timeout=Duration(seconds=0.30),
                )
            except TransformException:
                continue

            q = transform.transform.rotation
            return self._yaw_from_quaternion(q.x, q.y, q.z, q.w)

        self.get_logger().warn(
            f'Unable to look up robot heading in frame {target_frame}; using final shelf yaw.'
        )
        return None

    def _wait_future_blocking(self, future, timeout_sec: float):
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        while time.monotonic() < deadline:
            if future.done():
                try:
                    return future.result()
                except Exception as exc:
                    self.get_logger().warn(f'Async operation failed: {exc}')
                    return None
            time.sleep(0.02)
        return None

    def _ensure_bt_navigator_active_blocking(self) -> Tuple[bool, str]:
        if not self.bt_navigator_get_state_client.wait_for_service(timeout_sec=0.5):
            return False, 'bt_navigator lifecycle get_state service unavailable'

        def _query_state() -> Tuple[Optional[int], str, str]:
            get_future = self.bt_navigator_get_state_client.call_async(LifecycleGetState.Request())
            state_resp = self._wait_future_blocking(get_future, timeout_sec=1.0)
            if state_resp is None or getattr(state_resp, 'current_state', None) is None:
                return None, 'unknown', 'bt_navigator lifecycle state query timed out'
            state_id_local = int(getattr(state_resp.current_state, 'id', -1))
            state_label_local = str(getattr(state_resp.current_state, 'label', '') or 'unknown')
            return state_id_local, state_label_local, ''

        state_id, state_label, query_err = _query_state()
        if query_err:
            return False, query_err
        if state_id == 3:
            return True, ''

        startup_wait_sec = 8.0
        deadline = time.monotonic() + startup_wait_sec
        while state_id not in (2, 3) and time.monotonic() < deadline:
            time.sleep(0.25)
            state_id, state_label, query_err = _query_state()
            if query_err:
                continue
            if state_id == 3:
                return True, ''

        if state_id == 3:
            return True, ''
        if state_id != 2:
            return False, (
                f'bt_navigator is not active (state={state_label}[{state_id}]); '
                f'waited {startup_wait_sec:.1f}s for bringup'
            )

        if not self.bt_navigator_change_state_client.wait_for_service(timeout_sec=0.5):
            return False, 'bt_navigator lifecycle change_state service unavailable'

        activate_req = LifecycleChangeState.Request()
        activate_req.transition.id = int(LifecycleTransition.TRANSITION_ACTIVATE)
        activate_req.transition.label = 'activate'
        activate_future = self.bt_navigator_change_state_client.call_async(activate_req)
        activate_resp = self._wait_future_blocking(activate_future, timeout_sec=1.5)
        if activate_resp is None:
            return False, 'bt_navigator activation request timed out'
        if not bool(getattr(activate_resp, 'success', False)):
            return False, 'bt_navigator activation request was rejected'

        verify_deadline = time.monotonic() + 2.5
        verify_id = state_id
        verify_label = state_label
        while time.monotonic() < verify_deadline:
            verify_id, verify_label, query_err = _query_state()
            if not query_err and verify_id == 3:
                self.get_logger().warn('bt_navigator was inactive; auto-activated before navigation goal')
                return True, ''
            time.sleep(0.15)

        return False, f'bt_navigator activation incomplete (state={verify_label}[{verify_id}])'

    @staticmethod
    def _clone_pose_stamped(pose: PoseStamped) -> PoseStamped:
        cloned = PoseStamped()
        cloned.header.frame_id = str(pose.header.frame_id or '')
        cloned.header.stamp = pose.header.stamp
        cloned.pose.position.x = float(pose.pose.position.x)
        cloned.pose.position.y = float(pose.pose.position.y)
        cloned.pose.position.z = float(pose.pose.position.z)
        cloned.pose.orientation.x = float(pose.pose.orientation.x)
        cloned.pose.orientation.y = float(pose.pose.orientation.y)
        cloned.pose.orientation.z = float(pose.pose.orientation.z)
        cloned.pose.orientation.w = float(pose.pose.orientation.w)
        return cloned

    def _on_nav_feedback(self, feedback_msg) -> None:
        feedback = getattr(feedback_msg, 'feedback', None)
        if feedback is None:
            return
        distance_remaining = getattr(feedback, 'distance_remaining', None)
        if distance_remaining is not None:
            self.get_logger().debug(f'Nav2 distance remaining: {distance_remaining:.2f}')

    def _on_nav_goal_response(self, future) -> None:
        goal_handle = future.result()
        if goal_handle is None or not goal_handle.accepted:
            self.is_navigating = False
            self.get_logger().warn('NavigateToPose goal was rejected.')
            return

        self._nav_goal_handle = goal_handle
        self.get_logger().info('NavigateToPose goal accepted.')
        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(self._on_nav_result)

    def _on_nav_result(self, future) -> None:
        self.is_navigating = False
        with self._nav_send_lock:
            self._nav_send_inflight = False
        result = future.result()
        if result is None:
            self.get_logger().warn('NavigateToPose did not return a result.')
            return

        status = result.status
        if status == GoalStatus.STATUS_SUCCEEDED:
            self.get_logger().info('NavigateToPose goal succeeded.')
            self._call_jack_load()
            return

        self.get_logger().warn(f'NavigateToPose finished with status {self._goal_status_name(status)}.')

    def _call_jack_load(self) -> None:
        if not self.enable_jacking_action:
            return
        if self.jack_client is None or JackingOperation is None:
            self.get_logger().warn('Jacking action client is unavailable. Skipping jacking request.')
            return
        if not self.jack_client.wait_for_server(timeout_sec=0.5):
            self.get_logger().warn('Jacking action server is not available.')
            return

        jack_goal = JackingOperation.Goal()
        if hasattr(jack_goal, 'operation'):
            jack_goal.operation = self.jacking_operation
        if hasattr(jack_goal, 'speed'):
            jack_goal.speed = self.jacking_speed
        if hasattr(jack_goal, 'timeout'):
            jack_goal.timeout = self.jacking_timeout

        future = self.jack_client.send_goal_async(jack_goal, feedback_callback=self._on_jack_feedback)
        future.add_done_callback(self._on_jack_goal_response)

    def _on_jack_feedback(self, feedback_msg) -> None:
        feedback = getattr(feedback_msg, 'feedback', None)
        if feedback is None:
            return
        text = getattr(feedback, 'text', None)
        if text:
            self.get_logger().info(f'Jacking feedback: {text}')

    def _on_jack_goal_response(self, future) -> None:
        goal_handle = future.result()
        if goal_handle is None or not goal_handle.accepted:
            self.get_logger().warn('Jacking action goal was rejected.')
            return

        self._jack_goal_handle = goal_handle
        self.get_logger().info('Jacking action goal accepted.')
        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(self._on_jack_result)

    def _on_jack_result(self, future) -> None:
        result = future.result()
        if result is None:
            self.get_logger().warn('Jacking action did not return a result.')
            return

        self.get_logger().info(f'Jacking action completed with status {self._goal_status_name(result.status)}.')

    def publish_roi_marker(self, frame_id: str) -> None:
        marker = Marker()
        marker.header.stamp = self.get_clock().now().to_msg()
        marker.header.frame_id = frame_id
        marker.ns = 'bounding_box'
        marker.id = 0
        marker.type = Marker.LINE_STRIP
        marker.action = Marker.ADD
        marker.scale.x = 0.01
        marker.color.a = 1.0
        marker.color.r = 1.0
        marker.color.g = 0.0
        marker.color.b = 0.0
        marker.points = [
            Point(x=self.x_min, y=self.y_min, z=0.0),
            Point(x=self.x_max, y=self.y_min, z=0.0),
            Point(x=self.x_max, y=self.y_max, z=0.0),
            Point(x=self.x_min, y=self.y_max, z=0.0),
            Point(x=self.x_min, y=self.y_min, z=0.0),
        ]
        if self.marker_lifetime > 0.0:
            marker.lifetime = Duration(seconds=self.marker_lifetime).to_msg()
        self.roi_marker_pub.publish(marker)

    def _publish_visual_state(self) -> None:
        marker_array = MarkerArray()
        delete_marker = Marker()
        delete_marker.action = Marker.DELETEALL
        marker_array.markers.append(delete_marker)

        preferred_candidate = self._preferred_candidate_pose()
        if preferred_candidate and self.candidate_valid and self.candidate_fresh:
            marker_array.markers.extend(self._candidate_markers(preferred_candidate))
            self._publish_candidate_transforms(preferred_candidate)
        elif self.clear_tf_on_lost:
            self._clear_candidate_transforms()

        if self.committed_target_pose is not None:
            marker_array.markers.extend(self._committed_markers(self.committed_target_pose))
            self._publish_committed_transform(self.committed_target_pose)
        elif self.clear_tf_on_lost:
            self._clear_committed_transform()

        self.marker_array_pub.publish(marker_array)
        self.group_marker_pub.publish(marker_array)

    def _candidate_markers(self, pose: ShelfPose) -> List[Marker]:
        markers: List[Marker] = []
        colors = [
            (0.18, 0.74, 0.32),
            (0.18, 0.74, 0.32),
            (0.95, 0.60, 0.18),
            (0.95, 0.60, 0.18),
        ]
        marker_id = 1
        for index, point in enumerate(pose.ordered_points):
            markers.append(
                self._make_cube_marker(
                    marker_id,
                    f'candidate_group_{index}',
                    pose.frame_id,
                    point[0],
                    point[1],
                    colors[index],
                    0.055,
                )
            )
            marker_id += 1

        markers.append(
            self._make_cube_marker(
                marker_id,
                'candidate_centroid',
                pose.frame_id,
                pose.x,
                pose.y,
                (0.18, 0.55, 0.95),
                0.065,
            )
        )
        marker_id += 1
        markers.append(
            self._make_line_marker(
                marker_id,
                'candidate_front_edge',
                pose.frame_id,
                pose.ordered_points[0],
                pose.ordered_points[1],
                (0.18, 0.74, 0.32),
            )
        )
        marker_id += 1
        markers.append(
            self._make_line_marker(
                marker_id,
                'candidate_back_edge',
                pose.frame_id,
                pose.ordered_points[2],
                pose.ordered_points[3],
                (0.95, 0.60, 0.18),
            )
        )
        marker_id += 1
        markers.append(
            self._make_line_marker(
                marker_id,
                'candidate_axis',
                pose.frame_id,
                pose.front_midpoint,
                pose.back_midpoint,
                (0.18, 0.55, 0.95),
            )
        )
        return markers

    def _committed_markers(self, pose: ShelfPose) -> List[Marker]:
        target_x, target_y, target_yaw = self._goal_target_in_shelf_frame(pose)
        yaw_quat = self._quaternion_msg(self._euler_to_quaternion(0.0, 0.0, target_yaw))

        marker = Marker()
        marker.header.stamp = self.get_clock().now().to_msg()
        marker.header.frame_id = pose.frame_id
        marker.ns = 'committed_target'
        marker.id = 200
        marker.type = Marker.ARROW
        marker.action = Marker.ADD
        marker.pose.position.x = target_x
        marker.pose.position.y = target_y
        marker.pose.position.z = 0.0
        marker.pose.orientation = yaw_quat
        marker.scale.x = 0.18
        marker.scale.y = 0.06
        marker.scale.z = 0.06
        marker.color.a = 0.95
        marker.color.r = 0.20
        marker.color.g = 0.62
        marker.color.b = 1.0
        if self.marker_lifetime > 0.0:
            marker.lifetime = Duration(seconds=self.marker_lifetime).to_msg()
        return [marker]

    def _make_cube_marker(
        self,
        marker_id: int,
        namespace: str,
        frame_id: str,
        x: float,
        y: float,
        color: Tuple[float, float, float],
        scale_xy: float,
    ) -> Marker:
        marker = Marker()
        marker.header.stamp = self.get_clock().now().to_msg()
        marker.header.frame_id = frame_id
        marker.ns = namespace
        marker.id = marker_id
        marker.type = Marker.CUBE
        marker.action = Marker.ADD
        marker.pose.position.x = x
        marker.pose.position.y = y
        marker.pose.position.z = 0.0
        marker.pose.orientation.w = 1.0
        marker.scale.x = scale_xy
        marker.scale.y = scale_xy
        marker.scale.z = 0.05
        marker.color.a = 0.95
        marker.color.r = color[0]
        marker.color.g = color[1]
        marker.color.b = color[2]
        if self.marker_lifetime > 0.0:
            marker.lifetime = Duration(seconds=self.marker_lifetime).to_msg()
        return marker

    def _make_line_marker(
        self,
        marker_id: int,
        namespace: str,
        frame_id: str,
        start: Tuple[float, float],
        end: Tuple[float, float],
        color: Tuple[float, float, float],
    ) -> Marker:
        marker = Marker()
        marker.header.stamp = self.get_clock().now().to_msg()
        marker.header.frame_id = frame_id
        marker.ns = namespace
        marker.id = marker_id
        marker.type = Marker.LINE_STRIP
        marker.action = Marker.ADD
        marker.scale.x = 0.02
        marker.color.a = 0.95
        marker.color.r = color[0]
        marker.color.g = color[1]
        marker.color.b = color[2]
        marker.points = [
            Point(x=start[0], y=start[1], z=0.0),
            Point(x=end[0], y=end[1], z=0.0),
        ]
        if self.marker_lifetime > 0.0:
            marker.lifetime = Duration(seconds=self.marker_lifetime).to_msg()
        return marker

    def _publish_candidate_transforms(self, pose: ShelfPose) -> None:
        quaternion = self._euler_to_quaternion(0.0, 0.0, pose.yaw)
        names = [
            ('candidate_group_0', pose.ordered_points[0]),
            ('candidate_group_1', pose.ordered_points[1]),
            ('candidate_group_2', pose.ordered_points[2]),
            ('candidate_group_3', pose.ordered_points[3]),
            ('candidate_shelf', (pose.x, pose.y)),
            ('candidate_front_midpoint', pose.front_midpoint),
            ('candidate_back_midpoint', pose.back_midpoint),
        ]
        for child_frame_id, point in names:
            self._send_transform(
                frame_id=pose.frame_id,
                x=point[0],
                y=point[1],
                child_frame_id=child_frame_id,
                quaternion=quaternion,
            )

    def _publish_committed_transform(self, pose: ShelfPose) -> None:
        target_x, target_y, target_yaw = self._goal_target_in_shelf_frame(pose)
        self._send_transform(
            frame_id=pose.frame_id,
            x=target_x,
            y=target_y,
            child_frame_id='committed_target',
            quaternion=self._euler_to_quaternion(0.0, 0.0, target_yaw),
        )

    def _clear_candidate_transforms(self) -> None:
        quaternion = self._euler_to_quaternion(0.0, 0.0, 0.0)
        for name in [
            'candidate_group_0',
            'candidate_group_1',
            'candidate_group_2',
            'candidate_group_3',
            'candidate_shelf',
            'candidate_front_midpoint',
            'candidate_back_midpoint',
        ]:
            self._send_transform(
                frame_id=self.active_scan_frame,
                x=0.0,
                y=0.0,
                child_frame_id=name,
                quaternion=quaternion,
            )

    def _clear_committed_transform(self) -> None:
        self._send_transform(
            frame_id=self.active_scan_frame,
            x=0.0,
            y=0.0,
            child_frame_id='committed_target',
            quaternion=self._euler_to_quaternion(0.0, 0.0, 0.0),
        )

    def _send_transform(
        self,
        *,
        frame_id: str,
        x: float,
        y: float,
        child_frame_id: str,
        quaternion: Sequence[float],
    ) -> None:
        transform = TransformStamped()
        transform.header.stamp = self.get_clock().now().to_msg()
        transform.header.frame_id = frame_id
        transform.child_frame_id = child_frame_id
        transform.transform.translation.x = x
        transform.transform.translation.y = y
        transform.transform.translation.z = 0.0
        transform.transform.rotation = self._quaternion_msg(quaternion)
        self.tf_broadcaster.sendTransform(transform)

    def _publish_status(self) -> None:
        now = self.get_clock().now()
        self.candidate_valid = self._is_manual_commit_ready(now)
        preferred_pose = self._preferred_candidate_pose()
        payload: Dict[str, Any] = {
            'shelf_detected': bool(self.candidate_valid),
            'detector_enabled': bool(self.detector_enabled),
            'candidate_valid': bool(self.candidate_valid),
            'candidate_fresh': bool(self.candidate_fresh),
            'candidate_consistent': bool(self.candidate_consistent),
            'candidate_age_sec': self._age_seconds(now, self.candidate_scan_time),
            'hotspot_count': int(self.hotspot_count),
            'candidate_hotspot_count': int(self.candidate_hotspot_count),
            'solver_ok': bool(self.solver_ok),
            'last_reason': str(self.last_reason or ''),
            'center_pose': self._pose_to_dict(preferred_pose),
            'center_pose_raw': self._pose_to_dict(self.candidate_pose),
            'center_pose_smoothed': self._pose_to_dict(self.candidate_pose_smoothed),
            'scan_frame_id': str(self.active_scan_frame or ''),
            'committed_target_valid': self.committed_target_pose is not None,
            'committed_target_age_sec': self._age_seconds(now, self.committed_target_time),
            'committed_target_pose': self._pose_to_dict(self.committed_target_pose),
            'max_intensity': float(self.max_intensity),
            'frame_mismatch_warning': str(self.frame_mismatch_warning or ''),
            'candidate_confidence': float(self.candidate_confidence),
            'candidate_confidence_components': dict(self.candidate_confidence_components),
            'candidate_sigma_x': float(self.candidate_sigma_x),
            'candidate_sigma_y': float(self.candidate_sigma_y),
            'candidate_sigma_yaw': float(self.candidate_sigma_yaw),
            'candidate_uncertainty_samples': int(self.candidate_uncertainty_samples),
            'hotspot_points': self._hotspots_to_dicts(self.latest_hotspots),
            'candidate_front_width_m': self._pair_width(self.candidate_pose, 0, 1),
            'candidate_back_width_m': self._pair_width(self.candidate_pose, 2, 3),
            'preferred_front_width_m': self._pair_width(preferred_pose, 0, 1),
            'candidate_front_intensity_sum': (
                float(self.candidate_pose.front_intensity_sum)
                if self.candidate_pose is not None
                else -1.0
            ),
            'candidate_back_intensity_sum': (
                float(self.candidate_pose.back_intensity_sum)
                if self.candidate_pose is not None
                else -1.0
            ),
            'candidate_intensity_balance_ratio': (
                float(self.candidate_pose.intensity_balance_ratio)
                if self.candidate_pose is not None
                else -1.0
            ),
            'goal_offset_x': float(self.goal_offset_x),
            'goal_offset_y': float(self.goal_offset_y),
            'goal_frame': str(self.goal_frame or ''),
            'trigger_topic': '/tick',
            'detected_topic': '/shelf_detected',
            'status_topic': self.status_topic,
        }
        self.shelf_status_pub.publish(StringMsg(data=json.dumps(payload, ensure_ascii=False)))
        if self._last_published_detected != bool(self.candidate_valid):
            self.shelf_detected_pub.publish(Bool(data=bool(self.candidate_valid)))
            self._last_published_detected = bool(self.candidate_valid)

    def _pose_to_dict(self, pose: Optional[ShelfPose]) -> Optional[Dict[str, Any]]:
        if pose is None:
            return None
        return {
            'x': float(pose.x),
            'y': float(pose.y),
            'yaw': float(pose.yaw),
            'frame_id': str(pose.frame_id),
            'ordered_points': [
                [float(point[0]), float(point[1])]
                for point in pose.ordered_points
            ],
            'front_midpoint': [float(pose.front_midpoint[0]), float(pose.front_midpoint[1])],
            'back_midpoint': [float(pose.back_midpoint[0]), float(pose.back_midpoint[1])],
            'front_intensity_sum': float(pose.front_intensity_sum),
            'back_intensity_sum': float(pose.back_intensity_sum),
            'intensity_balance_ratio': float(pose.intensity_balance_ratio),
        }

    def _pair_width(self, pose: Optional[ShelfPose], first_idx: int, second_idx: int) -> float:
        if pose is None or len(pose.ordered_points) <= max(first_idx, second_idx):
            return -1.0
        return float(self._distance(
            pose.ordered_points[first_idx],
            pose.ordered_points[second_idx],
        ))

    def _hotspots_to_dicts(self, hotspots: Sequence[Hotspot]) -> List[Dict[str, float]]:
        payload: List[Dict[str, float]] = []
        for index, hotspot in enumerate(hotspots):
            payload.append({
                'index': int(index),
                'x': float(hotspot.x),
                'y': float(hotspot.y),
                'range_m': float(hotspot.range_value),
                'angle_deg': float(math.degrees(hotspot.angle)),
                'peak_intensity': float(hotspot.peak_intensity),
            })
        return payload

    def _age_seconds(self, now: Time, stamp: Optional[Time]) -> float:
        if stamp is None:
            return -1.0
        return max(0.0, (now - stamp).nanoseconds / 1e9)

    @staticmethod
    def _ema_value(previous_value: float, current_value: float, alpha: float) -> float:
        blend_alpha = max(0.0, min(1.0, float(alpha)))
        return ((1.0 - blend_alpha) * float(previous_value)) + (blend_alpha * float(current_value))

    @staticmethod
    def _ema_angle(previous_angle: float, current_angle: float, alpha: float) -> float:
        blend_alpha = max(0.0, min(1.0, float(alpha)))
        blended_x = (
            ((1.0 - blend_alpha) * math.cos(float(previous_angle)))
            + (blend_alpha * math.cos(float(current_angle)))
        )
        blended_y = (
            ((1.0 - blend_alpha) * math.sin(float(previous_angle)))
            + (blend_alpha * math.sin(float(current_angle)))
        )
        if math.hypot(blended_x, blended_y) <= 1e-9:
            return float(current_angle)
        return float(math.atan2(blended_y, blended_x))

    @staticmethod
    def _stddev(values: Sequence[float]) -> float:
        numeric = [float(value) for value in values]
        if len(numeric) <= 1:
            return 0.0
        mean_value = sum(numeric) / len(numeric)
        variance = sum((value - mean_value) ** 2 for value in numeric) / (len(numeric) - 1)
        return float(math.sqrt(max(0.0, variance)))

    @staticmethod
    def _circular_stddev(values: Sequence[float]) -> float:
        numeric = [float(value) for value in values]
        if len(numeric) <= 1:
            return 0.0
        mean_cos = sum(math.cos(value) for value in numeric) / len(numeric)
        mean_sin = sum(math.sin(value) for value in numeric) / len(numeric)
        mean_resultant = math.hypot(mean_cos, mean_sin)
        mean_resultant = max(1e-9, min(1.0, mean_resultant))
        return float(math.sqrt(max(0.0, -2.0 * math.log(mean_resultant))))

    @staticmethod
    def _clamp01(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    def _normalized_inverse_score(self, value: float, tolerance: float) -> float:
        return self._clamp01(1.0 - (float(value) / max(float(tolerance), 1e-6)))

    def _on_timer(self) -> None:
        now = self.get_clock().now()
        if self.candidate_pose is not None and not self._is_candidate_fresh(now):
            self._set_invalid_candidate(
                'stale_candidate',
                frame_id=self.active_scan_frame,
                scan_time=now,
                max_intensity=self.max_intensity,
                hotspot_count=self.hotspot_count,
                solver_ok=False,
            )
            return
        self._publish_status()

    def _point_in_roi(self, x: float, y: float) -> bool:
        return self.x_min <= x <= self.x_max and self.y_min <= y <= self.y_max

    @staticmethod
    def _distance(point_a: Tuple[float, float], point_b: Tuple[float, float]) -> float:
        return math.dist(point_a, point_b)

    @staticmethod
    def _midpoint(point_a: Tuple[float, float], point_b: Tuple[float, float]) -> Tuple[float, float]:
        return ((point_a[0] + point_b[0]) / 2.0, (point_a[1] + point_b[1]) / 2.0)

    @staticmethod
    def _median(values: Sequence[float]) -> float:
        ordered = sorted(float(v) for v in values)
        if not ordered:
            return 0.0
        mid = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2.0

    @staticmethod
    def _cross(o: Tuple[float, float], a: Tuple[float, float], b: Tuple[float, float]) -> float:
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    def _convex_hull(self, points: Sequence[Tuple[float, float]]) -> List[Tuple[float, float]]:
        unique = sorted(set((round(point[0], 6), round(point[1], 6)) for point in points))
        if len(unique) <= 1:
            return list(unique)

        lower: List[Tuple[float, float]] = []
        for point in unique:
            while len(lower) >= 2 and self._cross(lower[-2], lower[-1], point) <= 0.0:
                lower.pop()
            lower.append(point)

        upper: List[Tuple[float, float]] = []
        for point in reversed(unique):
            while len(upper) >= 2 and self._cross(upper[-2], upper[-1], point) <= 0.0:
                upper.pop()
            upper.append(point)

        return lower[:-1] + upper[:-1]

    @staticmethod
    def _polygon_area(points: Sequence[Tuple[float, float]]) -> float:
        if len(points) < 3:
            return 0.0
        area = 0.0
        for index, point in enumerate(points):
            next_point = points[(index + 1) % len(points)]
            area += point[0] * next_point[1] - next_point[0] * point[1]
        return abs(area) / 2.0

    @staticmethod
    def _line_angle_delta(
        vector_a: Tuple[float, float],
        vector_b: Tuple[float, float],
    ) -> float:
        norm_a = math.hypot(vector_a[0], vector_a[1])
        norm_b = math.hypot(vector_b[0], vector_b[1])
        if norm_a <= 1e-6 or norm_b <= 1e-6:
            return math.pi
        dot = max(-1.0, min(1.0, (vector_a[0] * vector_b[0] + vector_a[1] * vector_b[1]) / (norm_a * norm_b)))
        angle = math.acos(abs(dot))
        return angle

    @staticmethod
    def _angle_delta_rad(angle_a: float, angle_b: float) -> float:
        delta = angle_a - angle_b
        while delta > math.pi:
            delta -= 2.0 * math.pi
        while delta < -math.pi:
            delta += 2.0 * math.pi
        return delta

    @staticmethod
    def _yaw_from_quaternion(x: float, y: float, z: float, w: float) -> float:
        siny_cosp = 2.0 * (w * z + x * y)
        cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
        return math.atan2(siny_cosp, cosy_cosp)

    @staticmethod
    def _euler_to_quaternion(roll: float, pitch: float, yaw: float) -> Tuple[float, float, float, float]:
        qx = math.sin(roll / 2.0) * math.cos(pitch / 2.0) * math.cos(yaw / 2.0) - math.cos(roll / 2.0) * math.sin(pitch / 2.0) * math.sin(yaw / 2.0)
        qy = math.cos(roll / 2.0) * math.sin(pitch / 2.0) * math.cos(yaw / 2.0) + math.sin(roll / 2.0) * math.cos(pitch / 2.0) * math.sin(yaw / 2.0)
        qz = math.cos(roll / 2.0) * math.cos(pitch / 2.0) * math.sin(yaw / 2.0) - math.sin(roll / 2.0) * math.sin(pitch / 2.0) * math.cos(yaw / 2.0)
        qw = math.cos(roll / 2.0) * math.cos(pitch / 2.0) * math.cos(yaw / 2.0) + math.sin(roll / 2.0) * math.sin(pitch / 2.0) * math.sin(yaw / 2.0)
        return (qx, qy, qz, qw)

    @staticmethod
    def _quaternion_msg(values: Sequence[float]) -> Quaternion:
        msg = Quaternion()
        msg.x = float(values[0])
        msg.y = float(values[1])
        msg.z = float(values[2])
        msg.w = float(values[3])
        return msg

    @staticmethod
    def _goal_status_name(status: int) -> str:
        names = {
            GoalStatus.STATUS_UNKNOWN: 'UNKNOWN',
            GoalStatus.STATUS_ACCEPTED: 'ACCEPTED',
            GoalStatus.STATUS_EXECUTING: 'EXECUTING',
            GoalStatus.STATUS_CANCELING: 'CANCELING',
            GoalStatus.STATUS_SUCCEEDED: 'SUCCEEDED',
            GoalStatus.STATUS_CANCELED: 'CANCELED',
            GoalStatus.STATUS_ABORTED: 'ABORTED',
        }
        return names.get(status, f'CODE_{status}')


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ShelfDetectorNode()
    executor = MultiThreadedExecutor(num_threads=2)
    executor.add_node(node)
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            executor.shutdown()
        except Exception:
            pass
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
