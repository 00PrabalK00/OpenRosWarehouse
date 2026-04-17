#!/usr/bin/env python3

import ast
import asyncio
import json
import math
import time
import threading
import tempfile
from typing import Any, Callable, Dict, Optional, Tuple
import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from geometry_msgs.msg import PoseStamped, Point
from nav_msgs.msg import Path as NavPath
from next_ros2ws_interfaces.msg import Zone, Path as PathInfo, Layout as LayoutInfo
from next_ros2ws_interfaces.srv import (
    SaveZone, DeleteZone, UpdateZoneParams, ReorderZones,
    SavePath, DeletePath, SaveLayout, LoadLayout, DeleteLayout,
    SetMaxSpeed, SetMotionIntent, SetSafetyOverride,
    GetZones, GetPaths, GetLayouts, RequestNavigation
)
from next_ros2ws_interfaces.action import GoToZone as GoToZoneAction, FollowPath as FollowPathAction
from nav2_msgs.action import NavigateToPose, FollowPath as Nav2FollowPath
from action_msgs.msg import GoalStatus
from rclpy.action import ActionClient, ActionServer, CancelResponse, GoalResponse
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.task import Future
from std_msgs.msg import Bool as BoolMsg, Int32, String as StringMsg
from visualization_msgs.msg import Marker, MarkerArray
from builtin_interfaces.msg import Duration as RosDuration
from rclpy.qos import QoSProfile, DurabilityPolicy, ReliabilityPolicy
from std_srvs.srv import SetBool, Trigger
from rcl_interfaces.srv import SetParameters, GetParameters
from rcl_interfaces.msg import Parameter, ParameterValue, ParameterType
from lifecycle_msgs.srv import GetState as LifecycleGetState, ChangeState as LifecycleChangeState
from lifecycle_msgs.msg import Transition as LifecycleTransition
import yaml
import os

from tf2_ros import Buffer, TransformListener
from .db_manager import DatabaseManager
from .action_registry import ACTION_MAPPING_PREFIX, merge_action_mappings, normalize_action_id
from next2_shelf_simple.shelf_docking import (
    Pose2D as ShelfPose2D,
    RobotFootprint as ShelfRobotFootprint,
    ShelfGeometry,
    ShelfDockingParameters,
    ShelfDockingPlan,
    ShelfDockingPlanError,
    assess_shelf_docking,
    build_shelf_navigation_targets,
    build_shelf_docking_plan,
    pose_error_in_target_frame as shelf_pose_error_in_target_frame,
)
from .topic_catalog import (
    default_topics as catalog_default_topics,
    merge_topic_overrides,
)


class ZoneManager(Node):
    DEFAULT_TOPICS: Dict[str, str] = catalog_default_topics()
    GOAL_POSE_HANDOFF_NAME = '__goal_pose_handoff__'

    @staticmethod
    def _normalize_robot_namespace(raw_value) -> str:
        ns = str(raw_value or "").strip()
        return ns.strip("/")

    def _endpoint(self, base_name: str) -> str:
        """Resolve endpoint with optional robot namespace prefix."""
        name = str(base_name or "").strip()
        if not name:
            return name
        if not name.startswith("/"):
            name = "/" + name
        if not self.robot_namespace:
            return name
        return f"/{self.robot_namespace}{name}"

    @staticmethod
    def _sanitize_topics(raw: Any) -> Dict[str, str]:
        if not isinstance(raw, dict):
            return {}
        cleaned: Dict[str, str] = {}
        for key, value in raw.items():
            topic_key = str(key or '').strip()
            if not topic_key:
                continue
            cleaned[topic_key] = str(value or '').strip()
        return cleaned

    def _load_topic_config_from_db(self) -> Dict[str, str]:
        try:
            payload = self.db_manager.get_ui_mappings()
        except Exception as exc:
            self.get_logger().warn(f'Failed loading UI topic mappings: {exc}')
            return dict(self.DEFAULT_TOPICS)

        if not isinstance(payload, dict):
            return dict(self.DEFAULT_TOPICS)

        return merge_topic_overrides(payload.get('topics', {}))

    def _topic(self, key: str) -> str:
        configured = str(self.topic_config.get(key, '') or '').strip()
        if configured:
            return configured
        return str(self.DEFAULT_TOPICS.get(key, '') or '').strip()

    def _topic_endpoint(self, key: str) -> str:
        return self._endpoint(self._topic(key))

    def __init__(self):
        super().__init__('zone_manager')
        self._state_lock = threading.RLock()

        self.robot_namespace = self._normalize_robot_namespace(
            self.declare_parameter('robot_namespace', '').value
        )

        db_path = os.path.expanduser(
            str(self.declare_parameter('db_path', '~/DB/robot_data.db').value)
        )
        self.db_manager = DatabaseManager(db_path=db_path)
        self.topic_config = self._load_topic_config_from_db()
        self.controller_set_params_service = self._topic_endpoint('service_controller_set_parameters')
        self.controller_get_params_service = self._topic_endpoint('service_controller_get_parameters')

        # Path segment heading parameter
        self.path_use_waypoint_heading_for_segment = bool(
            self.declare_parameter('path_use_waypoint_heading_for_segment', True).value
        )

        # Subscriber to /goal_pose topic
        self.goal_pose_sub = self.create_subscription(
            PoseStamped,
            self._endpoint('/goal_pose'),
            self.goal_pose_callback,
            10
        )
        self.shelf_status_sub = self.create_subscription(
            StringMsg,
            '/shelf/status_json',
            self._shelf_status_callback,
            10,
        )

        # Service servers
        self.save_zone_srv = self.create_service(
            SaveZone,
            self._endpoint('/save_zone'),
            self.save_zone_callback
        )
        
        self.delete_zone_srv = self.create_service(
            DeleteZone,
            self._endpoint('/delete_zone'),
            self.delete_zone_callback
        )
        
        self.update_zone_params_srv = self.create_service(
            UpdateZoneParams,
            self._endpoint('/update_zone_params'),
            self.update_zone_params_callback
        )
        
        self.reorder_zones_srv = self.create_service(
            ReorderZones,
            self._endpoint('/reorder_zones'),
            self.reorder_zones_callback
        )

        # Path CRUD services
        self.save_path_srv = self.create_service(
            SavePath,
            self._endpoint('/save_path'),
            self.save_path_callback
        )

        self.delete_path_srv = self.create_service(
            DeletePath,
            self._endpoint('/delete_path'),
            self.delete_path_callback
        )
        
        # Layout services
        self.save_layout_srv = self.create_service(
            SaveLayout,
            self._endpoint('/save_layout'),
            self.save_layout_callback
        )
        
        self.load_layout_srv = self.create_service(
            LoadLayout,
            self._endpoint('/load_layout'),
            self.load_layout_callback
        )
        
        self.delete_layout_srv = self.create_service(
            DeleteLayout,
            self._endpoint('/delete_layout'),
            self.delete_layout_callback
        )
        
        # Control services
        self.set_max_speed_srv = self.create_service(
            SetMaxSpeed,
            self._endpoint('/set_max_speed'),
            self.set_max_speed_callback
        )
        
        self.set_safety_override_srv = self.create_service(
            SetSafetyOverride,
            self._endpoint('/set_safety_override'),
            self.set_safety_override_callback
        )
        # Runs in its own callback group so that blocking inside
        # navigate_to_goal_pose_callback does not prevent go_to_zone_goal_callback
        # (in the default group) from executing concurrently.
        self._navigate_goal_pose_cb_group = MutuallyExclusiveCallbackGroup()
        self.navigate_goal_pose_srv = self.create_service(
            Trigger,
            self._endpoint('/navigate_to_goal_pose'),
            self.navigate_to_goal_pose_callback,
            callback_group=self._navigate_goal_pose_cb_group,
        )

        # Real control ownership bindings
        self.safety_override_client = self.create_client(SetBool, self._endpoint('/safety/override'))
        self.shelf_detector_enable_service = str(
            self.declare_parameter('shelf_detector_enable_service', '/shelf/set_enabled').value
            or '/shelf/set_enabled'
        ).strip() or '/shelf/set_enabled'
        self.shelf_enable_client = self.create_client(
            SetBool,
            self.shelf_detector_enable_service,
        )
        self.shelf_commit_client = self.create_client(
            Trigger,
            '/shelf/commit',
        )
        self.controller_motion_intent_service = self._endpoint('/controller_server/set_motion_intent')
        self.controller_motion_intent_client = self.create_client(
            SetMotionIntent,
            self.controller_motion_intent_service,
        )
        self.controller_params_client = self.create_client(
            SetParameters,
            self.controller_set_params_service,
        )
        self.controller_get_params_client = self.create_client(
            GetParameters,
            self.controller_get_params_service,
        )
        self.bt_navigator_get_state_client = self.create_client(
            LifecycleGetState,
            self._endpoint('/bt_navigator/get_state'),
        )
        self.bt_navigator_change_state_client = self.create_client(
            LifecycleChangeState,
            self._endpoint('/bt_navigator/change_state'),
        )
        self.estop_state_sub = self.create_subscription(
            BoolMsg,
            self._endpoint('/set_estop'),
            self._set_estop_state_callback,
            10
        )
        self.obstacle_hold_state_sub = self.create_subscription(
            BoolMsg,
            self._endpoint('/safety/obstacle_stop_active'),
            self._obstacle_hold_state_callback,
            10
        )
        self.safety_override_state_sub = self.create_subscription(
            BoolMsg,
            self._endpoint('/safety/override_active'),
            self._safety_override_state_callback,
            10
        )

        
        # Data retrieval services (single source of truth)
        self.get_zones_srv = self.create_service(
            GetZones,
            self._endpoint('/get_zones'),
            self.get_zones_callback
        )
        
        self.get_paths_srv = self.create_service(
            GetPaths,
            self._endpoint('/get_paths'),
            self.get_paths_callback
        )

        self.get_layouts_srv = self.create_service(
            GetLayouts,
            self._endpoint('/get_layouts'),
            self.get_layouts_callback
        )
        
        # Store settings in memory
        # Default must match desired_linear_vel in nav2_params.yaml; overridden at
        # runtime by the UI /set_max_speed service call.
        self.max_speed = float(self.declare_parameter('default_max_speed', 0.80).value)
        self.safety_override_owner_state = None
        self.estop_active = False
        self.distance_obstacle_hold_active = False
        self.latest_shelf_status: Dict[str, Any] = {}
        self.latest_shelf_status_receipt_monotonic = 0.0
        self.registry_schema_version = 2

        # Legacy file paths for migration fallback
        self.registry_file = os.path.expanduser(
            str(self.declare_parameter('registry_file', '~/registry.yaml').value)
        )
        self.zones_file = os.path.expanduser('~/zones.yaml')
        self.layouts_file = os.path.expanduser('~/layouts.yaml')
        self.paths_file = os.path.expanduser('~/paths.yaml')

        # TF for arrival validation
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

        # Arrival tolerance (metres). Nav2 is authoritative for success; keep
        # these matched tolerances only for diagnostics and non-failing helpers.
        self.arrival_tolerance = 0.40
        self.zone_position_tolerance = max(
            0.01,
            float(self.declare_parameter('zone_position_tolerance', 0.10).value),
        )
        default_zone_footprint = (
            '[[0.40, 0.37], [0.40, -0.37], [-0.40, -0.37], [-0.40, 0.37]]'
        )
        self.zone_arrival_footprint = self._parse_footprint_polygon(
            self.declare_parameter(
                'zone_arrival_footprint_polygon',
                default_zone_footprint,
            ).value
        )
        self.zone_arrival_footprint_padding = max(
            0.0,
            float(self.declare_parameter('zone_arrival_footprint_padding', 0.02).value),
        )
        # Legacy diagnostic-only parameter retained for config compatibility.
        self.zone_nav2_success_max_mismatch_distance = max(
            self.zone_position_tolerance,
            float(self.declare_parameter('zone_nav2_success_max_mismatch_distance', 0.50).value),
        )
        self.goal_pose_handoff_standoff_distance = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_standoff_distance', 0.55).value),
        )
        self.goal_pose_handoff_insertion_extra_standoff = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_insertion_extra_standoff', 0.0).value),
        )
        self.goal_pose_handoff_insertion_path_spacing = max(
            0.02,
            float(self.declare_parameter('goal_pose_handoff_insertion_path_spacing', 0.05).value),
        )
        self.goal_pose_handoff_approach_arrival_tolerance = max(
            0.08,
            float(self.declare_parameter('goal_pose_handoff_approach_arrival_tolerance', 0.20).value),
        )
        self.goal_pose_handoff_local_insert_enabled = bool(
            self.declare_parameter('goal_pose_handoff_local_insert_enabled', True).value
        )
        self.goal_pose_handoff_local_insert_safety_override_enabled = bool(
            self.declare_parameter('goal_pose_handoff_local_insert_safety_override_enabled', False).value
        )
        self.goal_pose_handoff_local_insert_speed = max(
            0.02,
            float(self.declare_parameter('goal_pose_handoff_local_insert_speed', 0.12).value),
        )
        self.goal_pose_handoff_local_insert_max_speed = max(
            self.goal_pose_handoff_local_insert_speed,
            float(self.declare_parameter('goal_pose_handoff_local_insert_max_speed', 0.18).value),
        )
        self.goal_pose_handoff_local_insert_max_turn = max(
            0.10,
            float(self.declare_parameter('goal_pose_handoff_local_insert_max_turn', 0.75).value),
        )
        self.goal_pose_handoff_local_insert_bearing_gain = max(
            0.10,
            float(self.declare_parameter('goal_pose_handoff_local_insert_bearing_gain', 1.8).value),
        )
        self.goal_pose_handoff_local_insert_final_heading_gain = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_local_insert_final_heading_gain', 0.45).value),
        )
        self.goal_pose_handoff_local_insert_heading_tolerance = max(
            0.02,
            float(self.declare_parameter('goal_pose_handoff_local_insert_heading_tolerance', 0.10).value),
        )
        self.goal_pose_handoff_local_insert_prealign_heading_tolerance = max(
            0.02,
            float(self.declare_parameter('goal_pose_handoff_local_insert_prealign_heading_tolerance', 0.08).value),
        )
        self.goal_pose_handoff_local_insert_realign_heading_tolerance = max(
            self.goal_pose_handoff_local_insert_prealign_heading_tolerance,
            float(
                self.declare_parameter(
                    'goal_pose_handoff_local_insert_realign_heading_tolerance',
                    0.22,
                ).value
            ),
        )
        self.goal_pose_handoff_local_insert_align_deadband = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_local_insert_align_deadband', 0.04).value),
        )
        self.goal_pose_handoff_local_insert_align_hold_samples = max(
            1,
            int(self.declare_parameter('goal_pose_handoff_local_insert_align_hold_samples', 4).value),
        )
        self.goal_pose_handoff_local_insert_align_stable_delta = max(
            0.005,
            float(self.declare_parameter('goal_pose_handoff_local_insert_align_stable_delta', 0.02).value),
        )
        self.goal_pose_handoff_local_insert_align_max_turn = max(
            0.05,
            float(self.declare_parameter('goal_pose_handoff_local_insert_align_max_turn', 0.28).value),
        )
        self.goal_pose_handoff_local_insert_position_tolerance = max(
            0.02,
            float(self.declare_parameter('goal_pose_handoff_local_insert_position_tolerance', 0.08).value),
        )
        self.goal_pose_handoff_local_insert_distance = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_local_insert_distance', 0.35).value),
        )
        self.goal_pose_handoff_entry_clearance_margin = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_entry_clearance_margin', 0.04).value),
        )
        self.goal_pose_handoff_opening_width_margin = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_opening_width_margin', 0.04).value),
        )
        self.goal_pose_handoff_footprint_half_width = float(
            self.declare_parameter('goal_pose_handoff_footprint_half_width', -1.0).value
        )
        self.goal_pose_handoff_local_insert_centerline_tolerance = max(
            0.01,
            float(self.declare_parameter('goal_pose_handoff_local_insert_centerline_tolerance', 0.05).value),
        )
        self.goal_pose_handoff_local_insert_centerline_slowdown_tolerance = max(
            self.goal_pose_handoff_local_insert_centerline_tolerance,
            float(self.declare_parameter('goal_pose_handoff_local_insert_centerline_slowdown_tolerance', 0.10).value),
        )
        self.goal_pose_handoff_local_insert_timeout_sec = self._optional_timeout_parameter(
            self.declare_parameter('goal_pose_handoff_local_insert_timeout_sec', 8.0).value,
            minimum=1.0,
        )
        self.goal_pose_handoff_local_insert_no_progress_timeout_sec = self._optional_timeout_parameter(
            self.declare_parameter('goal_pose_handoff_local_insert_no_progress_timeout_sec', 1.5).value,
            minimum=0.5,
        )
        self.goal_pose_handoff_local_insert_progress_epsilon_m = max(
            0.002,
            float(self.declare_parameter('goal_pose_handoff_local_insert_progress_epsilon_m', 0.01).value),
        )
        self.goal_pose_handoff_local_insert_loop_hz = max(
            5.0,
            float(self.declare_parameter('goal_pose_handoff_local_insert_loop_hz', 15.0).value),
        )
        self.goal_pose_handoff_retry_attempts = max(
            1,
            int(self.declare_parameter('goal_pose_handoff_retry_attempts', 3).value),
        )
        self.goal_pose_handoff_retry_delay_sec = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_retry_delay_sec', 0.75).value),
        )
        self.goal_pose_handoff_min_hotspot_count = max(
            3,
            int(self.declare_parameter('goal_pose_handoff_min_hotspot_count', 3).value),
        )
        self.goal_pose_handoff_live_geometry_enabled = bool(
            self.declare_parameter('goal_pose_handoff_live_geometry_enabled', True).value
        )
        self.goal_pose_handoff_live_trim_enabled = bool(
            self.declare_parameter('goal_pose_handoff_live_trim_enabled', True).value
        )
        self.goal_pose_handoff_live_trim_gain = max(
            0.10,
            float(self.declare_parameter('goal_pose_handoff_live_trim_gain', 1.6).value),
        )
        self.goal_pose_handoff_live_trim_max_turn = max(
            0.02,
            float(self.declare_parameter('goal_pose_handoff_live_trim_max_turn', 0.12).value),
        )
        self.goal_pose_handoff_local_insert_max_live_yaw_jump = max(
            0.05,
            float(self.declare_parameter('goal_pose_handoff_local_insert_max_live_yaw_jump', 0.25).value),
        )
        self.goal_pose_handoff_local_insert_max_final_heading_correction = max(
            0.05,
            float(self.declare_parameter('goal_pose_handoff_local_insert_max_final_heading_correction', 0.30).value),
        )
        self.goal_pose_handoff_local_insert_yaw_gate = max(
            0.02,
            float(self.declare_parameter('goal_pose_handoff_local_insert_yaw_gate', math.radians(6.0)).value),
        )
        self.goal_pose_handoff_local_insert_hard_yaw_abort = max(
            self.goal_pose_handoff_local_insert_yaw_gate,
            float(
                self.declare_parameter(
                    'goal_pose_handoff_local_insert_hard_yaw_abort',
                    math.radians(10.0),
                ).value
            ),
        )
        self.goal_pose_handoff_local_insert_lateral_gain = max(
            0.10,
            float(self.declare_parameter('goal_pose_handoff_local_insert_lateral_gain', 1.0).value),
        )
        self.goal_pose_handoff_local_insert_lookahead = max(
            0.05,
            float(self.declare_parameter('goal_pose_handoff_local_insert_lookahead', 0.35).value),
        )
        self.goal_pose_handoff_local_insert_inside_speed_scale = self._clamp(
            float(self.declare_parameter('goal_pose_handoff_local_insert_inside_speed_scale', 0.60).value),
            0.10,
            1.0,
        )
        self.goal_pose_handoff_local_insert_entry_slow_speed = max(
            0.02,
            float(self.declare_parameter('goal_pose_handoff_local_insert_entry_slow_speed', 0.05).value),
        )
        self.follow_path_shelf_check_timeout_sec = max(
            0.5,
            float(self.declare_parameter('follow_path_shelf_check_timeout_sec', 3.0).value),
        )
        self.follow_path_shelf_check_enable_settle_sec = max(
            0.0,
            float(self.declare_parameter('follow_path_shelf_check_enable_settle_sec', 0.20).value),
        )
        self.follow_path_shelf_check_poll_sec = max(
            0.05,
            float(self.declare_parameter('follow_path_shelf_check_poll_sec', 0.10).value),
        )
        self.follow_path_shelf_docking_speed_cap = max(
            0.05,
            float(self.declare_parameter('follow_path_shelf_docking_speed_cap', 0.10).value),
        )
        self.goal_pose_handoff_verify_enabled = bool(
            self.declare_parameter('goal_pose_handoff_verify_enabled', True).value
        )
        self.goal_pose_handoff_verify_wait_timeout_sec = max(
            0.10,
            float(self.declare_parameter('goal_pose_handoff_verify_wait_timeout_sec', 1.20).value),
        )
        self.goal_pose_handoff_verify_poll_sec = max(
            0.05,
            float(self.declare_parameter('goal_pose_handoff_verify_poll_sec', 0.05).value),
        )
        self.goal_pose_handoff_verify_max_corrections = max(
            0,
            int(self.declare_parameter('goal_pose_handoff_verify_max_corrections', 2).value),
        )
        self.goal_pose_handoff_verify_correction_timeout_sec = self._optional_timeout_parameter(
            self.declare_parameter('goal_pose_handoff_verify_correction_timeout_sec', 2.50).value,
            minimum=0.50,
        )
        self.goal_pose_handoff_verify_no_progress_timeout_sec = self._optional_timeout_parameter(
            self.declare_parameter('goal_pose_handoff_verify_no_progress_timeout_sec', 0.75).value,
            minimum=0.25,
        )
        self.goal_pose_handoff_intensity_balance_enabled = bool(
            self.declare_parameter('goal_pose_handoff_intensity_balance_enabled', True).value
        )
        self.goal_pose_handoff_intensity_balance_tolerance = max(
            0.01,
            float(self.declare_parameter('goal_pose_handoff_intensity_balance_tolerance', 0.22).value),
        )
        self.goal_pose_handoff_intensity_balance_min_pair_sum = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_intensity_balance_min_pair_sum', 35.0).value),
        )
        self.goal_pose_handoff_intensity_balance_required_samples = max(
            1,
            int(self.declare_parameter('goal_pose_handoff_intensity_balance_required_samples', 3).value),
        )
        self.goal_pose_handoff_intensity_balance_freshness_sec = max(
            0.10,
            float(self.declare_parameter('goal_pose_handoff_intensity_balance_freshness_sec', 0.60).value),
        )
        self.goal_pose_handoff_intensity_balance_min_travel_m = max(
            0.0,
            float(self.declare_parameter('goal_pose_handoff_intensity_balance_min_travel_m', 0.12).value),
        )

        # Path tuning
        self.path_max_retries = max(
            1,
            int(self.declare_parameter('path_max_retries', 20).value),
        )
        # Custom controllers often return ABORTED intentionally to trigger a replan path.
        # Keep retries optional for that status to avoid repeated duplicate attempts.
        self.path_retry_on_aborted = bool(
            self.declare_parameter('path_retry_on_aborted', True).value
        )
        self.nav_goal_accept_attempts = max(
            1,
            int(self.declare_parameter('nav_goal_accept_attempts', 20).value),
        )
        self.path_no_progress_timeout_sec = max(
            1.0,
            float(self.declare_parameter('path_no_progress_timeout_sec', 8.0).value),
        )
        self.path_no_progress_min_speed_mps = max(
            0.0,
            float(self.declare_parameter('path_no_progress_min_speed_mps', 0.02).value),
        )
        self.path_no_progress_min_distance_delta_m = max(
            0.0,
            float(self.declare_parameter('path_no_progress_min_distance_delta_m', 0.03).value),
        )
        self.path_obstacle_wait_max_sec = max(
            5.0,
            float(self.declare_parameter('path_obstacle_wait_max_sec', 60.0).value),
        )

        # Corridor is a true lateral bound around the active path segment.
        self.path_corridor_width = max(
            0.10,
            float(self.declare_parameter('path_corridor_width', 0.30).value),
        )
        self.path_corridor_half_width = self.path_corridor_width * 0.5
        self.path_enforce_corridor = bool(
            self.declare_parameter('path_enforce_corridor', True).value
        )
        self.path_corridor_arm_tolerance = max(
            self.path_corridor_half_width,
            float(
                self.declare_parameter(
                    'path_corridor_arm_tolerance',
                    self.path_corridor_half_width * 1.5,
                ).value
            ),
        )
        self.path_corridor_violation_confirm_cycles = max(
            1,
            int(self.declare_parameter('path_corridor_violation_confirm_cycles', 4).value),
        )
        self.path_corridor_noarm_warn_interval_sec = max(
            0.1,
            float(self.declare_parameter('path_corridor_noarm_warn_interval_sec', 2.0).value),
        )
        # Optional leniency only near turning waypoints (curved edges).
        self.path_curve_edge_leniency_enabled = bool(
            self.declare_parameter('path_curve_edge_leniency_enabled', True).value
        )
        self.path_curve_turn_threshold_deg = max(
            0.0,
            float(self.declare_parameter('path_curve_turn_threshold_deg', 18.0).value),
        )
        self.path_curve_edge_transition_m = max(
            0.0,
            float(self.declare_parameter('path_curve_edge_transition_m', 0.40).value),
        )
        self.path_curve_edge_extra_width_m = max(
            0.0,
            float(self.declare_parameter('path_curve_edge_extra_width_m', 0.10).value),
        )
        # Default strict route-following behavior:
        # evaluate corridor against the expected local segment (with neighbor tolerance).
        # Enable global segment search only as a recovery mode for unusual localization/path states.
        self.path_corridor_global_segment_search = bool(
            self.declare_parameter('path_corridor_global_segment_search', False).value
        )

        # Waypoint tolerances are distinct from corridor width. Nav2 remains the
        # source of truth for SUCCESS; these values are used only for diagnostics
        # and optional helper behaviors such as semantic align prechecks.
        self.path_waypoint_tolerance = max(
            0.0,
            float(self.declare_parameter('path_waypoint_tolerance', 0.10).value),
        )
        self.path_final_tolerance = max(
            0.0,
            float(self.declare_parameter('path_final_tolerance', 0.10).value),
        )
        # Legacy diagnostic-only parameter retained for config compatibility.
        self.path_nav2_success_max_mismatch_distance = max(
            self.path_waypoint_tolerance,
            float(
                self.declare_parameter(
                    'path_nav2_success_max_mismatch_distance',
                    0.15,
                ).value
            ),
        )
        # Short loop/entry segments can stop slightly farther from the terminal anchor
        # depending on local controller behavior. Keep this separate from full-path final tolerance.
        self.path_short_segment_final_tolerance = max(
            0.0,
            float(self.declare_parameter('path_short_segment_final_tolerance', 0.25).value),
        )

        # Keep user-provided path anchors by default.
        # When enabled for testing, interpolation inserts intermediate checkpoints.
        self.path_interpolation_enabled = False
        self.path_interpolation_spacing = 0.35  # metres

        # Path start/orientation behavior. Path waypoint headings are ignored;
        # only POIs contribute semantic heading targets.
        self.path_nearest_start_enabled = bool(
            self.declare_parameter('path_nearest_start_enabled', False).value
        )
        self.path_force_first_waypoint_entry = bool(
            self.declare_parameter('path_force_first_waypoint_entry', False).value
        )
        self.path_require_pose_in_path_frame = bool(
            self.declare_parameter('path_require_pose_in_path_frame', True).value
        )
        self.path_require_close_start = bool(
            self.declare_parameter('path_require_close_start', False).value
        )
        self.path_require_close_start_distance = max(
            0.01,
            float(
                self.declare_parameter(
                    'path_require_close_start_distance',
                    max(0.10, self.path_waypoint_tolerance),
                ).value
            ),
        )
        self.path_enforce_waypoint_orientation = bool(
            self.declare_parameter('path_enforce_waypoint_orientation', False).value
        )
        self.path_heading_tolerance = max(
            0.01,
            float(self.declare_parameter('path_heading_tolerance', 0.05).value),
        )
        # When disabled, intermediate waypoints are passed through without
        # stop-and-rotate alignment to keep motion continuous.
        self.path_align_intermediate_waypoints = bool(
            self.declare_parameter('path_align_intermediate_waypoints', False).value
        )
        self.path_require_final_heading = bool(
            self.declare_parameter('path_require_final_heading', False).value
        )
        # After Nav2 reports STATUS_SUCCEEDED the robot is still decelerating.
        # This settle wait lets the robot physically stop before heading/position
        # are evaluated, eliminating false-pass heading checks and premature
        # action/stop execution.
        self.path_arrival_settle_sec = max(
            0.0,
            float(self.declare_parameter('path_arrival_settle_sec', 0.30).value),
        )
        self.path_align_spin_time_allowance_sec = max(
            1.0,
            float(self.declare_parameter('path_align_spin_time_allowance_sec', 12.0).value),
        )
        # Stable-heading gate: the heading check only passes when N consecutive
        # readings are ALL within tolerance AND the spread between them is small.
        # This prevents a transient pass-through mid-spin from falsely clearing
        # the alignment check — the robot must be stationary at the target heading.
        self.path_align_stable_samples = max(
            1,
            int(self.declare_parameter('path_align_stable_samples', 3).value),
        )
        self.path_align_stable_interval_sec = max(
            0.02,
            float(self.declare_parameter('path_align_stable_interval_sec', 0.10).value),
        )
        self.zone_heading_tolerance = max(
            0.01,
            float(self.declare_parameter('zone_heading_tolerance', 0.03).value),
        )
        self.zone_two_stage_arrival = bool(
            self.declare_parameter('zone_two_stage_arrival', True).value
        )
        self.zone_two_stage_heading_min_error = max(
            0.0,
            float(self.declare_parameter('zone_two_stage_heading_min_error', 0.05).value),
        )

        # Action clients for Nav2
        self.nav_client = ActionClient(self, NavigateToPose, self._endpoint('/navigate_to_pose'))
        self.nav_follow_path_client = ActionClient(self, Nav2FollowPath, self._endpoint('/follow_path'))
        self.arbitrator_request_client = self.create_client(
            RequestNavigation,
            self._endpoint('/arbitrator/request_goal'),
        )
        self.path_entry_distance_threshold = max(
            0.25,
            float(self.declare_parameter('path_entry_distance_threshold', 1.20).value),
        )
        self.path_follow_segment_spacing = max(
            0.05,
            float(self.declare_parameter('path_follow_segment_spacing', 0.20).value),
        )
        # Curved segments can use looser spacing/orientation handling to reduce stop-realign behavior.
        self.path_curve_motion_leniency_enabled = bool(
            self.declare_parameter('path_curve_motion_leniency_enabled', True).value
        )
        self.path_curve_segment_spacing = max(
            self.path_follow_segment_spacing,
            float(
                self.declare_parameter(
                    'path_curve_segment_spacing',
                    max(0.10, self.path_follow_segment_spacing * 2.0),
                ).value
            ),
        )
        self.path_curve_relax_waypoint_orientation = bool(
            self.declare_parameter('path_curve_relax_waypoint_orientation', False).value
        )
        self.follow_path_server_wait_sec = min(
            3.0,
            max(
                1.0,
                float(self.declare_parameter('follow_path_server_wait_sec', 2.0).value),
            ),
        )
        self.follow_path_controller_id = str(
            self.declare_parameter('follow_path_controller_id', 'FollowPath').value or 'FollowPath'
        ).strip() or 'FollowPath'
        self.follow_path_goal_checker_id = str(
            self.declare_parameter('follow_path_goal_checker_id', '').value or ''
        ).strip()

        # Action servers bridging to Nav2. Dedicated callback groups so inbound
        # goal/cancel callbacks from the arbitrator (ReentrantCallbackGroup) don't
        # block on the default group, and so go_to_zone and zone_follow_path don't
        # starve each other when both are queried.
        self._go_to_zone_cb_group = MutuallyExclusiveCallbackGroup()
        self._follow_path_cb_group = MutuallyExclusiveCallbackGroup()
        self.go_to_zone_action_server = ActionServer(
            self,
            GoToZoneAction,
            self._endpoint('/zone_manager/go_to_zone'),
            execute_callback=self.execute_go_to_zone,
            goal_callback=self.go_to_zone_goal_callback,
            cancel_callback=self.go_to_zone_cancel_callback,
            callback_group=self._go_to_zone_cb_group,
        )

        self.follow_path_action_server = ActionServer(
            self,
            FollowPathAction,
            self._endpoint('/zone_manager/zone_follow_path'),
            execute_callback=self.execute_follow_path,
            goal_callback=self.follow_path_goal_callback,
            cancel_callback=self.follow_path_cancel_callback,
            callback_group=self._follow_path_cb_group,
        )

        self.go_to_next_ros2ws_goal = None
        self.follow_path_nav_goal = None
        self._nav_follow_feedback_distance = float('inf')
        self._nav_follow_feedback_speed = 0.0

        # Action POI support for path following
        self._action_publishers: Dict[Tuple[str, str], Any] = {}
        self._lift_status_raw = 0
        self.zone_action_timeout_s = max(
            1.0, float(self.declare_parameter('zone_action_timeout_s', 45.0).value))
        self.zone_action_poll_hz = max(
            1.0, float(self.declare_parameter('zone_action_poll_hz', 10.0).value))
        _lift_topic = str(
            self.declare_parameter('zone_action_lift_status_topic', '/lift/status').value
            or '/lift/status'
        ).strip() or '/lift/status'
        self.lift_status_sub = self.create_subscription(
            Int32, _lift_topic, self._lift_status_callback, 10)
        
        # In-memory caches (Single Source of Truth for reads)
        self.zones = {'zones': {}}
        self.paths = {}
        self.layouts = {}

        # ── RViz visualization publishers ────────────────────────────────────
        # TRANSIENT_LOCAL = "latched" — RViz receives current state immediately
        # on subscribe, even if it started after the node.
        _viz_latch_qos = QoSProfile(
            depth=1,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            reliability=ReliabilityPolicy.RELIABLE,
        )
        self._viz_zones_pub = self.create_publisher(
            MarkerArray, self._endpoint('/next/viz/zones'), _viz_latch_qos)
        self._viz_paths_pub = self.create_publisher(
            MarkerArray, self._endpoint('/next/viz/paths'), _viz_latch_qos)
        self._viz_active_goal_pub = self.create_publisher(
            MarkerArray, self._endpoint('/next/viz/active_goal'), _viz_latch_qos)
        self._viz_active_path_pub = self.create_publisher(
            NavPath, self._endpoint('/next/viz/active_path'), 10)
        # Initial load from database
        self._load_all_caches()
        
        # Store the last received goal pose
        self.last_goal_pose = None
        self.last_goal_pose_error = None

        self.get_logger().info('Zone Manager node started')
        if self.robot_namespace:
            self.get_logger().info(f'Robot namespace prefix: /{self.robot_namespace}')
        else:
            self.get_logger().info('Robot namespace prefix: <none> (global endpoints)')
        self.get_logger().info(f'Database: {db_path}')
        self.get_logger().info(
            'Controller parameter services: '
            f'set={self.controller_set_params_service}, '
            f'get={self.controller_get_params_service}'
        )
        self.get_logger().info(
            f'Legacy fallback files: registry={self.registry_file}, zones={self.zones_file}, '
            f'paths={self.paths_file}, layouts={self.layouts_file}'
        )
        self.get_logger().info(
            'ZoneManager navigation role: goal/path provider only. '
            'Nav2 BT/controller own execution, recovery, and success validation.'
        )
        self.get_logger().info(
            'FollowPath semantic settings are retained only for config compatibility; '
            'live execution now forwards a single raw Nav2 FollowPath goal.'
        )
        self.get_logger().info(
            f'GoToZone diagnostics retained for compatibility: '
            f'position_tol={self.zone_position_tolerance:.3f}m, '
            f'heading_tol={self.zone_heading_tolerance:.3f}rad. '
            'Nav2 success is authoritative.'
        )

    @staticmethod
    def _goal_status_name(status: int) -> str:
        mapping = {
            GoalStatus.STATUS_UNKNOWN: 'UNKNOWN',
            GoalStatus.STATUS_ACCEPTED: 'ACCEPTED',
            GoalStatus.STATUS_EXECUTING: 'EXECUTING',
            GoalStatus.STATUS_CANCELING: 'CANCELING',
            GoalStatus.STATUS_SUCCEEDED: 'SUCCEEDED',
            GoalStatus.STATUS_CANCELED: 'CANCELED',
            GoalStatus.STATUS_ABORTED: 'ABORTED',
        }
        return mapping.get(status, f'CODE_{status}')

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

    async def _non_blocking_wait(self, seconds: float):
        """Cooperative wait that does not block the executor thread.

        Prefer asyncio sleep when an event loop is available.
        On rclpy executors without a running asyncio loop, use a ROS timer
        and await an rclpy Future.
        """
        delay = max(0.0, float(seconds))
        if delay <= 0.0:
            return

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            wake = Future()
            timer_ref = {'timer': None}

            def _wake():
                timer = timer_ref['timer']
                if timer is not None:
                    try:
                        timer.cancel()
                    except Exception:
                        pass
                    try:
                        self.destroy_timer(timer)
                    except Exception:
                        pass
                    timer_ref['timer'] = None
                if not wake.done():
                    wake.set_result(True)

            timer_ref['timer'] = self.create_timer(delay, _wake)
            try:
                await wake
            finally:
                timer = timer_ref['timer']
                if timer is not None:
                    try:
                        timer.cancel()
                    except Exception:
                        pass
                    try:
                        self.destroy_timer(timer)
                    except Exception:
                        pass
                    timer_ref['timer'] = None
                if not wake.done():
                    wake.set_result(True)
            return

        await asyncio.sleep(delay)

    async def _wait_future_with_timeout(self, future, timeout_sec: float):
        timeout = max(0.0, float(timeout_sec))
        deadline_ns = self.get_clock().now().nanoseconds + int(timeout * 1e9)

        while not future.done():
            if timeout == 0.0 or self.get_clock().now().nanoseconds >= deadline_ns:
                return None
            await self._non_blocking_wait(0.05)

        try:
            return future.result()
        except Exception as exc:
            raise RuntimeError(str(exc))

    async def _ensure_bt_navigator_active(self):
        """Ensure bt_navigator lifecycle node is active before NavigateToPose goals."""
        startup_wait_sec = 8.0
        poll_sec = 0.25

        async def _query_state():
            get_future = self.bt_navigator_get_state_client.call_async(LifecycleGetState.Request())
            state_resp = await self._wait_future_with_timeout(get_future, 1.0)
            if state_resp is None or getattr(state_resp, 'current_state', None) is None:
                return None, None, 'bt_navigator lifecycle state query timed out'
            state_id_local = int(getattr(state_resp.current_state, 'id', -1))
            state_label_local = str(getattr(state_resp.current_state, 'label', '') or 'unknown')
            return state_id_local, state_label_local, ''

        if not self.bt_navigator_get_state_client.wait_for_service(timeout_sec=0.5):
            return False, 'bt_navigator lifecycle get_state service unavailable'

        state_id, state_label, query_err = await _query_state()
        if query_err:
            return False, query_err
        if state_id == 3:
            return True, ''

        # During Nav2 startup bt_navigator can be UNCONFIGURED or in transition.
        # Give lifecycle manager a short grace window to complete bringup.
        if state_id != 2:
            deadline = time.monotonic() + startup_wait_sec
            while time.monotonic() < deadline:
                await self._non_blocking_wait(poll_sec)
                state_id, state_label, query_err = await _query_state()
                if query_err:
                    continue
                if state_id == 3:
                    return True, ''
                if state_id == 2:
                    break

        # Activate from INACTIVE when possible.
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
        activate_resp = await self._wait_future_with_timeout(activate_future, 1.5)
        if activate_resp is None:
            return False, 'bt_navigator activation request timed out'
        if not bool(getattr(activate_resp, 'success', False)):
            return False, 'bt_navigator activation request was rejected'

        verify_id = state_id
        verify_label = state_label
        verify_deadline = time.monotonic() + 2.5
        while time.monotonic() < verify_deadline:
            verify_id, verify_label, query_err = await _query_state()
            if not query_err and verify_id == 3:
                self.get_logger().warn('bt_navigator was inactive; auto-activated before navigation goal')
                return True, ''
            await self._non_blocking_wait(0.15)

        return False, f'bt_navigator activation incomplete (state={verify_label}[{verify_id}])'

    async def _cancel_nav_goal_with_confirmation(self, nav_goal_handle, label: str, timeout_sec: float = 1.0):
        """Cancel a Nav2 goal and confirm terminal canceled/finished state."""
        if nav_goal_handle is None:
            return True, f'{label}: no active goal'

        try:
            cancel_future = nav_goal_handle.cancel_goal_async()
        except Exception as exc:
            return False, f'{label}: cancel request failed: {exc}'

        cancel_response = await self._wait_future_with_timeout(cancel_future, timeout_sec)
        if cancel_response is None:
            return False, f'{label}: cancel request timed out after {timeout_sec:.1f}s'

        return_code = int(getattr(cancel_response, 'return_code', 0))
        error_none = int(getattr(cancel_response, 'ERROR_NONE', 0))
        error_unknown = int(getattr(cancel_response, 'ERROR_UNKNOWN_GOAL_ID', 2))
        error_terminated = int(getattr(cancel_response, 'ERROR_GOAL_TERMINATED', 3))

        if return_code in (error_unknown, error_terminated):
            return True, f'{label}: goal already not active (return_code={return_code})'
        if return_code != error_none:
            return False, f'{label}: cancel rejected (return_code={return_code})'

        result_future = nav_goal_handle.get_result_async()
        wrapped = await self._wait_future_with_timeout(result_future, timeout_sec)
        if wrapped is None:
            return False, f'{label}: cancel acknowledged but no terminal result within {timeout_sec:.1f}s'

        status = int(wrapped.status)
        if status == GoalStatus.STATUS_CANCELED:
            return True, f'{label}: canceled'
        if status in (GoalStatus.STATUS_SUCCEEDED, GoalStatus.STATUS_ABORTED):
            return True, (
                f'{label}: already finished with status {status} '
                f'({self._goal_status_name(status)})'
            )

        return False, f'{label}: cancel failed with status {status} ({self._goal_status_name(status)})'

    def _request_cancel_with_terminal_log(self, nav_goal_handle, label: str):
        """Request cancel without blocking callback thread; log terminal state asynchronously."""
        if nav_goal_handle is None:
            self.get_logger().info(f'{label}: no active goal to cancel')
            return False

        try:
            cancel_future = nav_goal_handle.cancel_goal_async()
        except Exception as exc:
            self.get_logger().warn(f'{label}: cancel request failed: {exc}')
            return False

        def _on_cancel_done(fut):
            try:
                cancel_response = fut.result()
            except Exception as exc:
                self.get_logger().warn(f'{label}: cancel response failed: {exc}')
                return

            if cancel_response is None:
                self.get_logger().warn(f'{label}: cancel returned no response')
                return

            return_code = int(getattr(cancel_response, 'return_code', 0))
            error_none = int(getattr(cancel_response, 'ERROR_NONE', 0))
            error_unknown = int(getattr(cancel_response, 'ERROR_UNKNOWN_GOAL_ID', 2))
            error_terminated = int(getattr(cancel_response, 'ERROR_GOAL_TERMINATED', 3))

            if return_code in (error_unknown, error_terminated):
                self.get_logger().info(
                    f'{label}: goal already not active (return_code={return_code})'
                )
                return

            if return_code != error_none:
                self.get_logger().warn(
                    f'{label}: cancel rejected (return_code={return_code})'
                )
                return

            result_future = nav_goal_handle.get_result_async()

            def _on_result_done(result_fut):
                try:
                    wrapped = result_fut.result()
                except Exception as exc:
                    self.get_logger().warn(f'{label}: terminal result fetch failed: {exc}')
                    return

                if wrapped is None:
                    self.get_logger().warn(f'{label}: terminal result missing after cancel')
                    return

                status = int(getattr(wrapped, 'status', 0))
                if status == GoalStatus.STATUS_CANCELED:
                    self.get_logger().info(f'{label}: canceled')
                elif status in (GoalStatus.STATUS_SUCCEEDED, GoalStatus.STATUS_ABORTED):
                    self.get_logger().info(
                        f'{label}: already finished with status {status} '
                        f'({self._goal_status_name(status)})'
                    )
                else:
                    self.get_logger().warn(
                        f'{label}: cancel terminal status {status} '
                        f'({self._goal_status_name(status)})'
                    )

            result_future.add_done_callback(_on_result_done)

        cancel_future.add_done_callback(_on_cancel_done)
        return True

    def _set_estop_state_callback(self, msg: BoolMsg):
        """Mirror SafetyController E-STOP state for action-goal gating."""
        new_state = bool(msg.data)
        if new_state == self.estop_active:
            return

        self.estop_active = new_state
        if new_state:
            self.get_logger().warn(
                'E-STOP activated: canceling active navigation and clearing shelf runtime state.'
            )
            self._clear_shelf_runtime_state(reason='E-STOP activated')
        else:
            self.get_logger().info('E-STOP cleared.')

    def _disable_shelf_detector_async(self, reason: str) -> None:
        service_name = str(self.shelf_detector_enable_service or '/shelf/set_enabled')
        if not self.shelf_enable_client.wait_for_service(timeout_sec=0.0):
            self.get_logger().warn(
                f'{service_name} unavailable while clearing shelf state ({reason}).'
            )
            return

        req = SetBool.Request()
        req.data = False
        future = self.shelf_enable_client.call_async(req)

        def _done(fut):
            try:
                response = fut.result()
            except Exception as exc:
                self.get_logger().warn(
                    f'{service_name} disable failed during {reason}: {exc}'
                )
                return

            if response is None:
                self.get_logger().warn(
                    f'{service_name} disable returned no response during {reason}'
                )
                return

            if bool(response.success):
                self.get_logger().info(
                    f'{service_name} disabled during {reason}: {response.message}'
                )
            else:
                self.get_logger().warn(
                    f'{service_name} rejected disable during {reason}: {response.message}'
                )

        future.add_done_callback(_done)

    def _clear_shelf_runtime_state(self, *, reason: str) -> None:
        self._clear_active_goal_marker()
        self._clear_active_follow_path()
        self.latest_shelf_status = {}
        self.latest_shelf_status_receipt_monotonic = 0.0

        if self.go_to_next_ros2ws_goal is not None:
            self._request_cancel_with_terminal_log(
                self.go_to_next_ros2ws_goal,
                f'{reason}: cancel active NavigateToPose goal',
            )

        if self.follow_path_nav_goal is not None:
            self._request_cancel_with_terminal_log(
                self.follow_path_nav_goal,
                f'{reason}: cancel active FollowPath goal',
            )

        self._disable_shelf_detector_async(reason)

    def _obstacle_hold_state_callback(self, msg: BoolMsg):
        """Mirror effective obstacle distance hold state from SafetyController."""
        self.distance_obstacle_hold_active = bool(msg.data)

    def _safety_override_state_callback(self, msg: BoolMsg):
        """Mirror SafetyController override state from owner-published topic."""
        self.safety_override_owner_state = bool(msg.data)

    def _shelf_status_callback(self, msg: StringMsg):
        if self.estop_active:
            return

        try:
            payload = json.loads(str(msg.data or '{}'))
        except Exception as exc:
            self.get_logger().debug(f'Ignoring malformed /shelf/status_json payload: {exc}')
            return

        if not isinstance(payload, dict):
            return

        self.latest_shelf_status = payload
        self.latest_shelf_status_receipt_monotonic = time.monotonic()

    def _current_shelf_balance_sample(self) -> Optional[Dict[str, float]]:
        if not bool(self.goal_pose_handoff_intensity_balance_enabled):
            return None
        if not isinstance(self.latest_shelf_status, dict) or not self.latest_shelf_status:
            return None

        receipt_age = time.monotonic() - float(self.latest_shelf_status_receipt_monotonic or 0.0)
        if receipt_age > float(self.goal_pose_handoff_intensity_balance_freshness_sec):
            return None

        try:
            if not bool(self.latest_shelf_status.get('detector_enabled', False)):
                return None
            if self._shelf_candidate_hotspot_count(self.latest_shelf_status) != 4:
                return None
            if not bool(self.latest_shelf_status.get('solver_ok', False)):
                return None

            front_sum = float(self.latest_shelf_status.get('candidate_front_intensity_sum', -1.0))
            back_sum = float(self.latest_shelf_status.get('candidate_back_intensity_sum', -1.0))
            ratio = float(self.latest_shelf_status.get('candidate_intensity_balance_ratio', -1.0))
        except Exception:
            return None

        min_pair_sum = float(self.goal_pose_handoff_intensity_balance_min_pair_sum)
        if front_sum < min_pair_sum or back_sum < min_pair_sum:
            return None
        if not math.isfinite(ratio) or ratio < 0.0:
            ratio = abs(front_sum - back_sum) / max(front_sum, back_sum, 1.0)

        return {
            'front_sum': float(front_sum),
            'back_sum': float(back_sum),
            'ratio': float(ratio),
            'receipt_age_sec': float(receipt_age),
        }

    @staticmethod
    def _shelf_candidate_hotspot_count(payload: Dict[str, Any]) -> int:
        for key in ('candidate_hotspot_count', 'hotspot_count'):
            try:
                return max(0, int(payload.get(key, 0)))
            except Exception:
                continue
        return 0

    def _current_shelf_opening_pose(
        self,
        target_frame: str,
        fallback_yaw: float,
    ) -> Optional[PoseStamped]:
        if not isinstance(self.latest_shelf_status, dict) or not self.latest_shelf_status:
            return None

        pose_dict = None
        if self._shelf_candidate_ready():
            live_pose_dict = self.latest_shelf_status.get('center_pose')
            if isinstance(live_pose_dict, dict) and live_pose_dict:
                pose_dict = live_pose_dict

        if not isinstance(pose_dict, dict) or not pose_dict:
            committed_pose_dict = self.latest_shelf_status.get('committed_target_pose')
            if isinstance(committed_pose_dict, dict) and committed_pose_dict:
                pose_dict = committed_pose_dict

        if not isinstance(pose_dict, dict) or not pose_dict:
            live_pose_dict = self.latest_shelf_status.get('center_pose')
            if isinstance(live_pose_dict, dict) and live_pose_dict:
                pose_dict = live_pose_dict

        if not isinstance(pose_dict, dict) or not pose_dict:
            return None

        midpoint = pose_dict.get('front_midpoint')
        if not isinstance(midpoint, (list, tuple)) or len(midpoint) != 2:
            return None

        source_frame = str(pose_dict.get('frame_id') or '').strip()
        if not source_frame:
            return None

        try:
            px = float(midpoint[0])
            py = float(midpoint[1])
            yaw = float(pose_dict.get('yaw', fallback_yaw))
        except Exception:
            return None

        pose = PoseStamped()
        pose.header.frame_id = source_frame
        pose.header.stamp = self.get_clock().now().to_msg()
        pose.pose.position.x = px
        pose.pose.position.y = py
        pose.pose.position.z = 0.0
        pose.pose.orientation.z = math.sin(yaw * 0.5)
        pose.pose.orientation.w = math.cos(yaw * 0.5)
        self._ensure_pose_orientation(pose.pose)

        if source_frame == target_frame:
            return pose

        if target_frame == 'map':
            mapped, error = self._transform_goal_pose_to_map(pose)
            if mapped is None:
                self.get_logger().warn(
                    f'Unable to transform shelf front midpoint from "{source_frame}" to "map": {error}'
                )
            return mapped

        try:
            transform = self.tf_buffer.lookup_transform(
                target_frame,
                source_frame,
                rclpy.time.Time(),
                timeout=rclpy.duration.Duration(seconds=0.30),
            )
        except Exception as exc:
            self.get_logger().warn(
                f'Unable to transform shelf front midpoint from "{source_frame}" to "{target_frame}": {exc}'
            )
            return None

        q_tf = (
            float(transform.transform.rotation.x),
            float(transform.transform.rotation.y),
            float(transform.transform.rotation.z),
            float(transform.transform.rotation.w),
        )
        out_q = self._quat_normalize(self._quat_multiply(self._quat_normalize(q_tf), (
            float(pose.pose.orientation.x),
            float(pose.pose.orientation.y),
            float(pose.pose.orientation.z),
            float(pose.pose.orientation.w),
        )))
        rx, ry, rz = self._rotate_vector_by_quat(
            float(pose.pose.position.x),
            float(pose.pose.position.y),
            float(pose.pose.position.z),
            q_tf,
        )

        mapped = PoseStamped()
        mapped.header.frame_id = target_frame
        mapped.header.stamp = self.get_clock().now().to_msg()
        mapped.pose.position.x = rx + float(transform.transform.translation.x)
        mapped.pose.position.y = ry + float(transform.transform.translation.y)
        mapped.pose.position.z = rz + float(transform.transform.translation.z)
        mapped.pose.orientation.x = out_q[0]
        mapped.pose.orientation.y = out_q[1]
        mapped.pose.orientation.z = out_q[2]
        mapped.pose.orientation.w = out_q[3]
        self._ensure_pose_orientation(mapped.pose)
        return mapped

    def _call_setbool_service_async(self, client, enabled: bool, service_name: str):
        """Queue SetBool request to the owning node and log result asynchronously."""
        if not client.wait_for_service(timeout_sec=0.5):
            return False, f'{service_name} unavailable'

        req = SetBool.Request()
        req.data = bool(enabled)
        future = client.call_async(req)

        def _done(fut):
            try:
                result = fut.result()
                if result is None:
                    self.get_logger().warn(f'{service_name} returned no result')
                elif result.success:
                    self.get_logger().info(f'{service_name} accepted: {result.message}')
                else:
                    self.get_logger().warn(f'{service_name} rejected: {result.message}')
            except Exception as exc:
                self.get_logger().warn(f'{service_name} call failed: {exc}')

        future.add_done_callback(_done)
        return True, f'{service_name} request queued'

    async def _call_setbool_service(
        self,
        client,
        enabled: bool,
        service_name: str,
        *,
        timeout_sec: float = 1.5,
    ) -> Tuple[bool, str]:
        if not client.wait_for_service(timeout_sec=0.5):
            return False, f'{service_name} unavailable'

        req = SetBool.Request()
        req.data = bool(enabled)
        future = client.call_async(req)
        deadline = time.monotonic() + max(0.1, float(timeout_sec))

        while time.monotonic() < deadline:
            if future.done():
                try:
                    response = future.result()
                except Exception as exc:
                    return False, f'{service_name} call failed: {exc}'

                if response is None:
                    return False, f'{service_name} returned no result'
                return bool(response.success), str(response.message or '')

            await self._non_blocking_wait(0.05)

        return False, f'{service_name} timed out'

    async def _set_motion_intent(
        self,
        mode: str,
        target_pose: PoseStamped,
        *,
        max_linear_speed: float = 0.0,
        max_angular_speed: float = 0.0,
        xy_tolerance: float = 0.0,
        yaw_tolerance: float = 0.0,
        timeout_sec: float = 0.0,
        source: str = 'zone_manager',
    ) -> Tuple[bool, str]:
        if not self.controller_motion_intent_client.wait_for_service(timeout_sec=0.75):
            return False, f'{self.controller_motion_intent_service} unavailable'

        req = SetMotionIntent.Request()
        req.mode = str(mode or '').strip()
        req.target_pose = self._copy_pose_stamped(target_pose)
        req.max_linear_speed = float(max_linear_speed)
        req.max_angular_speed = float(max_angular_speed)
        req.xy_tolerance = float(xy_tolerance)
        req.yaw_tolerance = float(yaw_tolerance)
        req.timeout_sec = float(timeout_sec)
        req.source = str(source or 'zone_manager').strip()

        future = self.controller_motion_intent_client.call_async(req)
        response = await self._wait_future_with_timeout(future, 1.5)
        if response is None:
            return False, f'{self.controller_motion_intent_service} timed out'
        if not bool(getattr(response, 'accepted', False)):
            return False, str(getattr(response, 'message', '') or 'motion intent rejected')
        return True, str(getattr(response, 'message', '') or 'motion intent accepted')

    async def _clear_motion_intent(self, *, source: str = 'zone_manager') -> None:
        if not self.controller_motion_intent_client.wait_for_service(timeout_sec=0.0):
            return

        req = SetMotionIntent.Request()
        req.mode = 'normal'
        req.source = str(source or 'zone_manager').strip()
        try:
            future = self.controller_motion_intent_client.call_async(req)
            await self._wait_future_with_timeout(future, 0.75)
        except Exception:
            pass

    def _shelf_candidate_ready(self) -> bool:
        payload = self.latest_shelf_status
        if not isinstance(payload, dict) or not payload:
            return False

        try:
            if not bool(payload.get('detector_enabled', False)):
                return False
            if not bool(payload.get('candidate_valid', False)):
                return False
        except Exception:
            return False

        pose_dict = payload.get('center_pose')
        return isinstance(pose_dict, dict) and bool(pose_dict)

    def _build_shelf_goal_pose_from_status(
        self,
        target_frame: str,
    ) -> Tuple[Optional[PoseStamped], str]:
        payload = self.latest_shelf_status
        if not isinstance(payload, dict) or not payload:
            return None, 'No shelf status available'
        if not self._shelf_candidate_ready():
            reason = str(payload.get('last_reason') or 'candidate_not_ready')
            return None, f'Shelf candidate not ready: {reason}'

        pose_dict = payload.get('center_pose')
        if not isinstance(pose_dict, dict) or not pose_dict:
            return None, 'Shelf status has no center pose'

        source_frame = str(
            pose_dict.get('frame_id')
            or payload.get('scan_frame_id')
            or target_frame
            or 'map'
        ).strip() or (target_frame or 'map')
        try:
            center_x = float(pose_dict.get('x'))
            center_y = float(pose_dict.get('y'))
            yaw = float(pose_dict.get('yaw', 0.0))
            goal_offset_x = float(payload.get('goal_offset_x', 0.0))
            goal_offset_y = float(payload.get('goal_offset_y', 0.0))
        except Exception as exc:
            return None, f'Invalid shelf pose payload: {exc}'

        axis_x = math.cos(yaw)
        axis_y = math.sin(yaw)
        left_x = -axis_y
        left_y = axis_x

        source_pose = PoseStamped()
        source_pose.header.frame_id = source_frame
        source_pose.header.stamp = self.get_clock().now().to_msg()
        source_pose.pose.position.x = float(center_x + (axis_x * goal_offset_x) + (left_x * goal_offset_y))
        source_pose.pose.position.y = float(center_y + (axis_y * goal_offset_x) + (left_y * goal_offset_y))
        source_pose.pose.position.z = 0.0
        source_pose.pose.orientation.z = math.sin(yaw * 0.5)
        source_pose.pose.orientation.w = math.cos(yaw * 0.5)
        self._ensure_pose_orientation(source_pose.pose)

        normalized_target_frame = str(target_frame or 'map').strip() or 'map'
        if source_frame == normalized_target_frame:
            return source_pose, ''

        if normalized_target_frame == 'map':
            mapped, error = self._transform_goal_pose_to_map(source_pose)
            if mapped is None:
                return None, error or 'Failed transforming shelf goal pose to map'
            return mapped, ''

        try:
            transform = self.tf_buffer.lookup_transform(
                normalized_target_frame,
                source_frame,
                rclpy.time.Time(),
                timeout=rclpy.duration.Duration(seconds=0.30),
            )
        except Exception as exc:
            return None, (
                f'Unable to transform shelf goal pose from "{source_frame}" '
                f'to "{normalized_target_frame}": {exc}'
            )

        q_tf = (
            float(transform.transform.rotation.x),
            float(transform.transform.rotation.y),
            float(transform.transform.rotation.z),
            float(transform.transform.rotation.w),
        )
        out_q = self._quat_normalize(self._quat_multiply(
            self._quat_normalize(q_tf),
            (
                float(source_pose.pose.orientation.x),
                float(source_pose.pose.orientation.y),
                float(source_pose.pose.orientation.z),
                float(source_pose.pose.orientation.w),
            ),
        ))
        rx, ry, rz = self._rotate_vector_by_quat(
            float(source_pose.pose.position.x),
            float(source_pose.pose.position.y),
            float(source_pose.pose.position.z),
            q_tf,
        )

        mapped = PoseStamped()
        mapped.header.frame_id = normalized_target_frame
        mapped.header.stamp = self.get_clock().now().to_msg()
        mapped.pose.position.x = rx + float(transform.transform.translation.x)
        mapped.pose.position.y = ry + float(transform.transform.translation.y)
        mapped.pose.position.z = rz + float(transform.transform.translation.z)
        mapped.pose.orientation.x = out_q[0]
        mapped.pose.orientation.y = out_q[1]
        mapped.pose.orientation.z = out_q[2]
        mapped.pose.orientation.w = out_q[3]
        self._ensure_pose_orientation(mapped.pose)
        return mapped, ''

    def _goal_pose_error_in_goal_frame(
        self,
        goal_pose: PoseStamped,
    ) -> Tuple[Optional[Dict[str, float]], str]:
        return self._pose_error_in_target_frame(goal_pose)

    def _pose_error_in_target_frame(
        self,
        target_pose: PoseStamped,
        *,
        robot_pose: Optional[PoseStamped] = None,
        robot_timeout_sec: float = 0.15,
    ) -> Tuple[Optional[Dict[str, float]], str]:
        """Return robot error expressed in the target pose frame.

        This is equivalent to transforming the robot pose from the global frame
        into the shelf insertion frame whose origin is at ``target_pose`` and
        whose +X axis follows the target yaw.
        """
        frame_id = str(target_pose.header.frame_id or 'map') or 'map'
        if robot_pose is None:
            robot_pose = self._robot_pose_in_frame(frame_id, timeout_sec=robot_timeout_sec)
        if robot_pose is None:
            return None, f'Unable to resolve robot pose in {frame_id}'
        target_pose2d = self._pose_stamped_to_shelf_pose2d(target_pose)
        robot_pose2d = self._pose_stamped_to_shelf_pose2d(robot_pose)
        try:
            target_error = shelf_pose_error_in_target_frame(
                robot_pose2d,
                target_pose2d,
            )
        except ShelfDockingPlanError as exc:
            return None, str(exc)

        return {
            'distance': float(target_error.distance),
            'longitudinal_error': float(target_error.forward_error),
            'lateral_error': float(target_error.left_error),
            'forward_error': float(target_error.forward_error),
            'left_error': float(target_error.left_error),
            'heading_error': float(target_error.heading_error),
            'goal_yaw': float(target_pose2d.yaw),
            'target_yaw': float(target_pose2d.yaw),
            'robot_yaw': float(robot_pose2d.yaw),
            'dx': float(target_error.dx),
            'dy': float(target_error.dy),
            'target_frame_id': frame_id,
        }, ''

    @staticmethod
    def _optional_timeout_parameter(raw_value: Any, minimum: float) -> float:
        try:
            timeout_sec = float(raw_value)
        except Exception:
            timeout_sec = float(minimum)
        if timeout_sec <= 0.0:
            return 0.0
        return max(float(minimum), timeout_sec)

    async def _wait_for_fresh_shelf_goal_pose(
        self,
        target_frame: str,
        min_receipt: float,
    ) -> Tuple[Optional[PoseStamped], float, str]:
        timeout_sec = max(0.10, float(self.goal_pose_handoff_verify_wait_timeout_sec))
        poll_period = max(0.05, float(self.goal_pose_handoff_verify_poll_sec))
        deadline = time.monotonic() + timeout_sec
        last_error = 'waiting_for_shelf'
        fallback_pose = None
        fallback_receipt = float(self.latest_shelf_status_receipt_monotonic or 0.0)

        while time.monotonic() < deadline:
            latest_receipt = float(self.latest_shelf_status_receipt_monotonic or 0.0)
            if self._shelf_candidate_ready():
                pose, pose_err = self._build_shelf_goal_pose_from_status(target_frame)
                if pose is not None:
                    if latest_receipt > float(min_receipt):
                        return pose, latest_receipt, ''
                    fallback_pose = pose
                    fallback_receipt = latest_receipt
                elif pose_err:
                    last_error = pose_err
            elif isinstance(self.latest_shelf_status, dict) and self.latest_shelf_status:
                last_error = str(self.latest_shelf_status.get('last_reason') or last_error)

            await self._non_blocking_wait(poll_period)

        if fallback_pose is not None:
            return fallback_pose, fallback_receipt, 'using latest ready shelf pose without a newer receipt'
        return None, float(self.latest_shelf_status_receipt_monotonic or 0.0), last_error

    async def _execute_goal_pose_verification_correction(
        self,
        goal_pose: PoseStamped,
        goal_handle,
        target_label: str,
        feedback,
        *,
        attempt_index: int,
        attempt_total: int,
        max_speed_cap: Optional[float] = None,
    ) -> Tuple[bool, str]:
        frame_id = str(goal_pose.header.frame_id or 'map') or 'map'
        robot_pose = self._robot_pose_in_frame(frame_id, timeout_sec=0.20)
        if robot_pose is None:
            return False, f'Goal-pose verification correction lost robot pose in {frame_id}.'

        longitudinal_tol = max(0.02, float(self.goal_pose_handoff_local_insert_position_tolerance))
        lateral_tol = max(0.02, float(self.goal_pose_handoff_local_insert_centerline_tolerance))
        heading_tol = max(0.02, float(self.goal_pose_handoff_local_insert_heading_tolerance))
        timeout_sec = self._optional_timeout_parameter(
            self.goal_pose_handoff_verify_correction_timeout_sec,
            minimum=0.50,
        )
        speed_cap = min(0.05, max(0.02, float(self.goal_pose_handoff_local_insert_speed)))
        if max_speed_cap is not None:
            try:
                speed_cap = min(speed_cap, max(0.02, float(max_speed_cap)))
            except Exception:
                pass
        max_turn = min(
            max(0.08, float(self.goal_pose_handoff_local_insert_align_max_turn)),
            0.22,
        )

        nav_path = self._build_segment_follow_path(
            robot_pose,
            goal_pose,
            segment_spacing=float(self.goal_pose_handoff_insertion_path_spacing),
            enforce_end_orientation=True,
        )

        intent_ok, intent_msg = await self._set_motion_intent(
            'alignment',
            goal_pose,
            max_linear_speed=speed_cap,
            max_angular_speed=max_turn,
            xy_tolerance=max(longitudinal_tol, lateral_tol),
            yaw_tolerance=heading_tol,
            timeout_sec=max(timeout_sec, 1.0),
            source='zone_manager:frozen_verify',
        )
        if not intent_ok:
            return False, (
                f'Goal-pose verification correction could not set controller intent: '
                f'{intent_msg}'
            )

        last_feedback_ns = 0

        def _on_feedback(remaining: float, speed: float) -> None:
            nonlocal last_feedback_ns
            now_ns = self.get_clock().now().nanoseconds
            if (now_ns - last_feedback_ns) < int(0.15 * 1e9):
                return
            feedback.progress = 0.995
            status = (
                f'Frozen shelf verify correction {attempt_index}/{attempt_total}: '
                f'"{target_label}"'
            )
            if math.isfinite(remaining):
                status += f' ({remaining:.2f}m remaining'
                if math.isfinite(speed):
                    status += f', v={speed:.2f}m/s'
                status += ')'
            feedback.status = status
            goal_handle.publish_feedback(feedback)
            last_feedback_ns = now_ns

        try:
            return await self._run_nav_follow_path_phase(
                nav_path,
                goal_handle,
                f'Frozen shelf verify correction {attempt_index}/{attempt_total}',
                on_feedback=_on_feedback,
            )
        finally:
            await self._clear_motion_intent(source='zone_manager:frozen_verify')

    async def _execute_frozen_alignment_correction(
        self,
        goal_pose: PoseStamped,
        goal_handle,
        target_label: str,
        feedback,
        attempt_index: int,
        attempt_total: int,
        *,
        max_speed_cap: Optional[float] = None,
    ) -> Tuple[bool, str]:
        return await self._execute_goal_pose_verification_correction(
            goal_pose,
            goal_handle,
            target_label,
            feedback,
            attempt_index=attempt_index,
            attempt_total=attempt_total,
            max_speed_cap=max_speed_cap,
        )

    async def _verify_goal_pose_handoff_alignment(
        self,
        expected_goal_pose: PoseStamped,
        goal_handle,
        target_label: str,
        feedback,
        *,
        max_speed_cap: Optional[float] = None,
    ) -> Tuple[bool, str]:
        if not bool(self.goal_pose_handoff_verify_enabled):
            return True, ''

        frame_id = str(expected_goal_pose.header.frame_id or 'map') or 'map'
        max_attempts = max(0, int(self.goal_pose_handoff_verify_max_corrections))
        receipt_cursor = float(self.latest_shelf_status_receipt_monotonic or 0.0)
        longitudinal_tol = max(
            0.02,
            float(self.goal_pose_handoff_local_insert_position_tolerance),
        )
        lateral_tol = max(
            0.02,
            float(self.goal_pose_handoff_local_insert_centerline_tolerance),
        )
        heading_tol = max(
            0.02,
            float(self.goal_pose_handoff_local_insert_heading_tolerance),
        )
        expected_goal_yaw = self._yaw_from_quaternion(
            float(expected_goal_pose.pose.orientation.x),
            float(expected_goal_pose.pose.orientation.y),
            float(expected_goal_pose.pose.orientation.z),
            float(expected_goal_pose.pose.orientation.w),
        )

        for attempt_index in range(max_attempts + 1):
            observed_goal_pose, receipt_used, observe_msg = await self._wait_for_fresh_shelf_goal_pose(
                frame_id,
                receipt_cursor,
            )
            if observed_goal_pose is None:
                self.get_logger().warn(
                    f'Frozen shelf verification skipped for "{target_label}": {observe_msg}'
                )
                return True, f'Verification skipped: {observe_msg}'

            error, error_msg = self._goal_pose_error_in_goal_frame(observed_goal_pose)
            if error is None:
                return False, f'Frozen shelf verification failed: {error_msg}'

            observed_goal_yaw = self._yaw_from_quaternion(
                float(observed_goal_pose.pose.orientation.x),
                float(observed_goal_pose.pose.orientation.y),
                float(observed_goal_pose.pose.orientation.z),
                float(observed_goal_pose.pose.orientation.w),
            )
            target_shift = math.hypot(
                float(observed_goal_pose.pose.position.x) - float(expected_goal_pose.pose.position.x),
                float(observed_goal_pose.pose.position.y) - float(expected_goal_pose.pose.position.y),
            )
            target_yaw_shift = math.degrees(
                self._abs_angle_delta(observed_goal_yaw, expected_goal_yaw)
            )
            confidence = -1.0
            if isinstance(self.latest_shelf_status, dict) and self.latest_shelf_status:
                try:
                    confidence = float(self.latest_shelf_status.get('candidate_confidence', -1.0))
                except Exception:
                    confidence = -1.0

            longitudinal_error = float(error['longitudinal_error'])
            lateral_error = float(error['lateral_error'])
            heading_error = float(error['heading_error'])
            within_tolerance = (
                abs(longitudinal_error) <= longitudinal_tol
                and abs(lateral_error) <= lateral_tol
                and abs(heading_error) <= heading_tol
            )
            if within_tolerance:
                verify_msg = (
                    f'Verified dock pose'
                    f' (dx={longitudinal_error:.3f}m, dy={lateral_error:.3f}m, '
                    f'dyaw={math.degrees(heading_error):.2f}deg, '
                    f'shelf_shift={target_shift:.3f}m/{target_yaw_shift:.2f}deg'
                )
                if confidence >= 0.0:
                    verify_msg += f', conf={confidence:.2f}'
                verify_msg += ')'
                if observe_msg:
                    verify_msg += f' [{observe_msg}]'
                return True, verify_msg

            if attempt_index >= max_attempts:
                return False, (
                    f'Frozen shelf verification failed after {max_attempts} correction attempts '
                    f'(dx={longitudinal_error:.3f}m, dy={lateral_error:.3f}m, '
                    f'dyaw={math.degrees(heading_error):.2f}deg, '
                    f'shelf_shift={target_shift:.3f}m/{target_yaw_shift:.2f}deg).'
                )

            feedback.progress = 0.992
            feedback.status = (
                f'Frozen shelf verify: correcting "{target_label}" '
                f'(dx={longitudinal_error:.3f}m, dy={lateral_error:.3f}m, '
                f'dyaw={math.degrees(heading_error):.1f}deg, '
                f'attempt {attempt_index + 1}/{max_attempts})'
            )
            goal_handle.publish_feedback(feedback)

            correction_ok, correction_msg = await self._execute_goal_pose_verification_correction(
                observed_goal_pose,
                goal_handle,
                target_label,
                feedback,
                attempt_index=attempt_index + 1,
                attempt_total=max_attempts,
                max_speed_cap=max_speed_cap,
            )
            if not correction_ok:
                return False, correction_msg
            receipt_cursor = max(float(receipt_used), float(self.latest_shelf_status_receipt_monotonic or 0.0))

        return True, ''

    @staticmethod
    def _make_double_parameter(name: str, value: float):
        return Parameter(
            name=name,
            value=ParameterValue(
                type=ParameterType.PARAMETER_DOUBLE,
                double_value=float(value)
            )
        )

    @staticmethod
    def _extract_controller_plugin_ids(get_params_response):
        """Extract controller_plugins parameter values safely."""
        if get_params_response is None or not getattr(get_params_response, 'values', None):
            return []

        try:
            raw = get_params_response.values[0]
            if int(raw.type) != int(ParameterType.PARAMETER_STRING_ARRAY):
                return []
            return [str(name).strip() for name in raw.string_array_value if str(name).strip()]
        except Exception:
            return []

    def _dispatch_controller_speed_update(self, speed_mps: float, plugin_ids):
        unique_plugins = []
        seen = set()
        for plugin in plugin_ids:
            name = str(plugin).strip()
            if not name or name in seen:
                continue
            seen.add(name)
            unique_plugins.append(name)

        if not unique_plugins:
            unique_plugins = ['FollowPath']

        param_updates = []
        for plugin in unique_plugins:
            # Update known speed keys across DWB/RPP/custom controller plugins.
            param_updates.append((f'{plugin}.max_vel_x', float(speed_mps)))
            param_updates.append((f'{plugin}.max_speed_xy', float(speed_mps)))
            param_updates.append((f'{plugin}.desired_linear_vel', float(speed_mps)))
            param_updates.append((f'{plugin}.max_v', float(speed_mps)))
            param_updates.append((f'{plugin}.rpp_desired_linear_vel', float(speed_mps)))

        req = SetParameters.Request()
        req.parameters = [
            self._make_double_parameter(name, value)
            for name, value in param_updates
        ]

        future = self.controller_params_client.call_async(req)

        def _done(fut):
            try:
                result = fut.result()
                if result is None or result.results is None:
                    self.get_logger().warn(
                        f'{self.controller_set_params_service} speed update returned empty result'
                    )
                    return

                successful = []
                failures = []
                for idx, res in enumerate(result.results):
                    param_name = (
                        param_updates[idx][0]
                        if idx < len(param_updates)
                        else f'param_{idx}'
                    )
                    if res.successful:
                        successful.append(param_name)
                    else:
                        reason = res.reason if res.reason else 'unspecified failure'
                        failures.append(f'{param_name}: {reason}')

                if not successful:
                    self.get_logger().warn(
                        f'{self.controller_set_params_service} speed update failed '
                        f'(no params applied): {failures}'
                    )
                    return

                if failures:
                    self.get_logger().warn(
                        f'{self.controller_set_params_service} speed update partially applied. '
                        f'success={successful}, failed={failures}'
                    )
                else:
                    self.get_logger().info(
                        f'{self.controller_set_params_service} speed cap set to {speed_mps:.2f} m/s '
                        f'({", ".join(successful)})'
                    )
            except Exception as exc:
                self.get_logger().warn(
                    f'{self.controller_set_params_service} speed update call failed: {exc}'
                )

        future.add_done_callback(_done)

    def _queue_controller_speed_update(self, speed_mps: float):
        """Apply max-speed cap on Nav2 controller via dynamic parameters."""
        if not self.controller_params_client.wait_for_service(timeout_sec=0.75):
            return False, f'{self.controller_set_params_service} unavailable'

        if not self.controller_get_params_client.wait_for_service(timeout_sec=0.25):
            self._dispatch_controller_speed_update(speed_mps, ['FollowPath'])
            return True, (
                f'{self.controller_get_params_service} unavailable; '
                'speed cap update queued using fallback plugin list '
                '(FollowPath)'
            )

        req = GetParameters.Request()
        req.names = ['controller_plugins']
        future = self.controller_get_params_client.call_async(req)

        def _resolve_plugins(fut):
            plugin_ids = ['FollowPath']
            try:
                response = fut.result()
                discovered = self._extract_controller_plugin_ids(response)
                if discovered:
                    plugin_ids = discovered
            except Exception as exc:
                self.get_logger().warn(
                    f'Failed to read controller_plugins from '
                    f'{self.controller_get_params_service}: {exc}; '
                    'using fallback plugin list'
                )

            self._dispatch_controller_speed_update(speed_mps, plugin_ids)

        future.add_done_callback(_resolve_plugins)
        return True, (
            f'Speed cap update queued via {self.controller_set_params_service} '
            '(all configured controller plugins)'
        )

    def _lookup_robot_transform(self, target_frame: str, timeout_sec: float = 0.5):
        """Lookup transform target_frame -> robot base with base_footprint fallback."""
        last_error = None
        for base_frame in ('base_footprint', 'base_link'):
            try:
                transform = self.tf_buffer.lookup_transform(
                    target_frame,
                    base_frame,
                    rclpy.time.Time(),
                    timeout=rclpy.duration.Duration(seconds=timeout_sec),
                )
                return transform, base_frame, None
            except Exception as e:
                last_error = e
        return None, None, last_error

    def _distance_to_goal(self, goal_pose: PoseStamped) -> float:
        """Return the Euclidean distance from the robot to *goal_pose*.

        Uses TF (map -> base_link/base_footprint). Returns ``float('inf')``
        when the transform is unavailable so callers can treat it as
        "not arrived".
        """
        target_frame = goal_pose.header.frame_id or 'map'
        transform, base_frame, err = self._lookup_robot_transform(
            target_frame=target_frame,
            timeout_sec=0.5,
        )
        if transform is None:
            self.get_logger().warn(
                f'TF lookup for arrival check failed ({target_frame} -> '
                f'base_link/base_footprint): {err}')
            return float('inf')

        try:
            dx = transform.transform.translation.x - goal_pose.pose.position.x
            dy = transform.transform.translation.y - goal_pose.pose.position.y
            return math.hypot(dx, dy)
        except Exception as e:
            self.get_logger().warn(
                f'TF math for arrival check failed ({target_frame} -> {base_frame}): {e}')
            return float('inf')

    @staticmethod
    def _parse_footprint_polygon(raw_value):
        default_points = [
            (0.40, 0.37),
            (0.40, -0.37),
            (-0.40, -0.37),
            (-0.40, 0.37),
        ]
        parsed = raw_value
        if isinstance(raw_value, str):
            text = str(raw_value or '').strip()
            if not text:
                return list(default_points)
            try:
                parsed = ast.literal_eval(text)
            except Exception:
                try:
                    parsed = yaml.safe_load(text)
                except Exception:
                    parsed = None

        points = []
        if isinstance(parsed, (list, tuple)):
            for item in parsed:
                try:
                    if isinstance(item, dict):
                        x = float(item.get('x'))
                        y = float(item.get('y'))
                    else:
                        if not isinstance(item, (list, tuple)) or len(item) < 2:
                            continue
                        x = float(item[0])
                        y = float(item[1])
                    if math.isfinite(x) and math.isfinite(y):
                        points.append((x, y))
                except Exception:
                    continue

        if len(points) < 3:
            return list(default_points)
        return points

    @staticmethod
    def _point_in_polygon(px: float, py: float, polygon_points) -> bool:
        if not isinstance(polygon_points, (list, tuple)) or len(polygon_points) < 3:
            return False

        inside = False
        n = len(polygon_points)
        for i in range(n):
            x1, y1 = polygon_points[i]
            x2, y2 = polygon_points[(i + 1) % n]
            intersects = ((y1 > py) != (y2 > py))
            if not intersects:
                continue
            denom = (y2 - y1)
            if abs(denom) < 1e-12:
                continue
            x_intersection = x1 + ((py - y1) * (x2 - x1) / denom)
            if px < x_intersection:
                inside = not inside
        return inside

    def _distance_from_goal_to_robot_footprint(self, goal_pose: PoseStamped) -> float:
        """Distance from goal point to robot footprint boundary in goal frame.

        Returns 0 when the goal point lies inside the current robot footprint.
        Returns +inf when TF/pose data is unavailable.
        """
        target_frame = goal_pose.header.frame_id or 'map'
        robot_pose = self._robot_pose_in_frame(target_frame, timeout_sec=0.30)
        if robot_pose is None:
            return float('inf')

        footprint_local = list(getattr(self, 'zone_arrival_footprint', []) or [])
        if len(footprint_local) < 3:
            return float('inf')

        try:
            rq = robot_pose.pose.orientation
            robot_yaw = self._yaw_from_quaternion(
                float(rq.x), float(rq.y), float(rq.z), float(rq.w)
            )
            cos_yaw = math.cos(robot_yaw)
            sin_yaw = math.sin(robot_yaw)
            rx = float(robot_pose.pose.position.x)
            ry = float(robot_pose.pose.position.y)
            gx = float(goal_pose.pose.position.x)
            gy = float(goal_pose.pose.position.y)

            footprint_world = []
            for fx, fy in footprint_local:
                wx = rx + (cos_yaw * float(fx)) - (sin_yaw * float(fy))
                wy = ry + (sin_yaw * float(fx)) + (cos_yaw * float(fy))
                footprint_world.append((wx, wy))

            if self._point_in_polygon(gx, gy, footprint_world):
                return 0.0

            min_edge_distance = float('inf')
            for idx in range(len(footprint_world)):
                ax, ay = footprint_world[idx]
                bx, by = footprint_world[(idx + 1) % len(footprint_world)]
                edge_distance = self._point_to_segment_distance(gx, gy, ax, ay, bx, by)
                min_edge_distance = min(min_edge_distance, edge_distance)

            padding = float(getattr(self, 'zone_arrival_footprint_padding', 0.0))
            if not math.isfinite(min_edge_distance):
                return float('inf')
            return max(0.0, min_edge_distance - max(0.0, padding))
        except Exception as exc:
            self.get_logger().warn(
                f'Footprint distance check failed ({target_frame}): {exc}'
            )
            return float('inf')

    def _robot_footprint_extents(self) -> Tuple[float, float, float]:
        footprint_local = list(getattr(self, 'zone_arrival_footprint', []) or [])
        if len(footprint_local) < 3:
            return 0.0, 0.0, 0.0

        try:
            forward_extent = max(float(point[0]) for point in footprint_local)
            rear_extent = max(-float(point[0]) for point in footprint_local)
            half_width = max(abs(float(point[1])) for point in footprint_local)
        except Exception:
            return 0.0, 0.0, 0.0

        padding = max(0.0, float(getattr(self, 'zone_arrival_footprint_padding', 0.0)))
        # Shelf-specific half_width override sourced from URDF (physical truth).
        # The zone_arrival_footprint_polygon is visual-only; when this override
        # is set (> 0) it replaces the polygon lateral extent with no extra padding.
        shelf_hw_override = float(getattr(self, 'goal_pose_handoff_footprint_half_width', -1.0))
        if shelf_hw_override > 0.0:
            return (
                max(0.0, forward_extent + padding),
                max(0.0, rear_extent + padding),
                float(shelf_hw_override),
            )
        return (
            max(0.0, forward_extent + padding),
            max(0.0, rear_extent + padding),
            max(0.0, half_width + padding),
        )

    def _current_shelf_opening_width(self) -> Optional[float]:
        payload = self.latest_shelf_status
        if not isinstance(payload, dict) or not payload:
            return None

        for key in ('preferred_front_width_m', 'candidate_front_width_m', 'front_width_m'):
            try:
                value = float(payload.get(key, float('nan')))
            except Exception:
                continue
            if math.isfinite(value) and value > 0.0:
                return float(value)

        return None

    @staticmethod
    def _yaw_from_quaternion(x: float, y: float, z: float, w: float) -> float:
        siny_cosp = 2.0 * ((w * z) + (x * y))
        cosy_cosp = 1.0 - (2.0 * ((y * y) + (z * z)))
        return math.atan2(siny_cosp, cosy_cosp)

    def _heading_error_to_goal(self, goal_pose: PoseStamped) -> float:
        """Return absolute yaw error between robot heading and goal heading in goal frame."""
        target_frame = goal_pose.header.frame_id or 'map'
        robot_pose = self._robot_pose_in_frame(target_frame, timeout_sec=0.30)
        if robot_pose is None:
            return float('inf')

        try:
            rq = robot_pose.pose.orientation
            gq = goal_pose.pose.orientation
            robot_yaw = self._yaw_from_quaternion(
                float(rq.x), float(rq.y), float(rq.z), float(rq.w)
            )
            goal_yaw = self._yaw_from_quaternion(
                float(gq.x), float(gq.y), float(gq.z), float(gq.w)
            )
            return float(self._abs_angle_delta(robot_yaw, goal_yaw))
        except Exception as exc:
            self.get_logger().warn(
                f'Heading check failed ({target_frame}): {exc}'
            )
            return float('inf')

    @staticmethod
    def _point_to_segment_distance(px: float, py: float, ax: float, ay: float, bx: float, by: float) -> float:
        abx = bx - ax
        aby = by - ay
        denom = (abx * abx) + (aby * aby)
        if denom <= 1e-12:
            return math.hypot(px - ax, py - ay)

        t = ((px - ax) * abx + (py - ay) * aby) / denom
        t = max(0.0, min(1.0, t))
        cx = ax + t * abx
        cy = ay + t * aby
        return math.hypot(px - cx, py - cy)

    @staticmethod
    def _heading_for_segment(waypoints, seg_idx: int):
        if seg_idx < 0 or seg_idx >= (len(waypoints) - 1):
            return None
        ax = float(waypoints[seg_idx].pose.position.x)
        ay = float(waypoints[seg_idx].pose.position.y)
        bx = float(waypoints[seg_idx + 1].pose.position.x)
        by = float(waypoints[seg_idx + 1].pose.position.y)
        return math.atan2(by - ay, bx - ax)

    @staticmethod
    def _abs_angle_delta(a: float, b: float) -> float:
        d = float(a) - float(b)
        while d > math.pi:
            d -= (2.0 * math.pi)
        while d < -math.pi:
            d += (2.0 * math.pi)
        return abs(d)

    @staticmethod
    def _signed_angle_delta(current: float, target: float) -> float:
        d = float(target) - float(current)
        while d > math.pi:
            d -= (2.0 * math.pi)
        while d < -math.pi:
            d += (2.0 * math.pi)
        return d

    def _heading_delta_to_goal(self, goal_pose: PoseStamped) -> float:
        """Return signed yaw delta from current robot heading to goal heading."""
        target_frame = goal_pose.header.frame_id or 'map'
        robot_pose = self._robot_pose_in_frame(target_frame, timeout_sec=0.30)
        if robot_pose is None:
            return float('inf')

        try:
            rq = robot_pose.pose.orientation
            gq = goal_pose.pose.orientation
            robot_yaw = self._yaw_from_quaternion(
                float(rq.x), float(rq.y), float(rq.z), float(rq.w)
            )
            goal_yaw = self._yaw_from_quaternion(
                float(gq.x), float(gq.y), float(gq.z), float(gq.w)
            )
            return float(self._signed_angle_delta(robot_yaw, goal_yaw))
        except Exception as exc:
            self.get_logger().warn(
                f'Heading delta check failed ({target_frame}): {exc}'
            )
            return float('inf')

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))

    @staticmethod
    def _copy_pose_stamped(pose: PoseStamped) -> PoseStamped:
        copied = PoseStamped()
        copied.header.frame_id = str(pose.header.frame_id or '')
        copied.header.stamp = pose.header.stamp
        copied.pose.position.x = float(pose.pose.position.x)
        copied.pose.position.y = float(pose.pose.position.y)
        copied.pose.position.z = float(pose.pose.position.z)
        copied.pose.orientation.x = float(pose.pose.orientation.x)
        copied.pose.orientation.y = float(pose.pose.orientation.y)
        copied.pose.orientation.z = float(pose.pose.orientation.z)
        copied.pose.orientation.w = float(pose.pose.orientation.w)
        return copied

    def _pose_stamped_to_shelf_pose2d(self, pose: PoseStamped) -> ShelfPose2D:
        return ShelfPose2D(
            x=float(pose.pose.position.x),
            y=float(pose.pose.position.y),
            yaw=self._yaw_from_quaternion(
                float(pose.pose.orientation.x),
                float(pose.pose.orientation.y),
                float(pose.pose.orientation.z),
                float(pose.pose.orientation.w),
            ),
            frame_id=str(pose.header.frame_id or 'map') or 'map',
        )

    def _shelf_pose2d_to_pose_stamped(self, pose: ShelfPose2D) -> PoseStamped:
        stamped = PoseStamped()
        stamped.header.frame_id = str(pose.frame_id or 'map') or 'map'
        stamped.header.stamp = self.get_clock().now().to_msg()
        stamped.pose.position.x = float(pose.x)
        stamped.pose.position.y = float(pose.y)
        stamped.pose.position.z = 0.0
        stamped.pose.orientation.z = math.sin(float(pose.yaw) * 0.5)
        stamped.pose.orientation.w = math.cos(float(pose.yaw) * 0.5)
        self._ensure_pose_orientation(stamped.pose)
        return stamped

    def _build_shelf_docking_plan(
        self,
        final_pose: PoseStamped,
    ) -> Tuple[Optional[ShelfDockingPlan], str, str]:
        frame_id = str(final_pose.header.frame_id or 'map') or 'map'
        goal_yaw = self._yaw_from_quaternion(
            float(final_pose.pose.orientation.x),
            float(final_pose.pose.orientation.y),
            float(final_pose.pose.orientation.z),
            float(final_pose.pose.orientation.w),
        )

        opening_line_pose = self._current_shelf_opening_pose(frame_id, goal_yaw)
        opening_source = 'front_midpoint'
        if opening_line_pose is None:
            opening_line_pose = self._copy_pose_stamped(final_pose)
            opening_line_pose.header.stamp = self.get_clock().now().to_msg()
            insert_distance = max(0.0, float(self.goal_pose_handoff_local_insert_distance))
            if insert_distance > 1e-6:
                opening_line_pose.pose.position.x = (
                    float(final_pose.pose.position.x)
                    - (math.cos(goal_yaw) * insert_distance)
                )
                opening_line_pose.pose.position.y = (
                    float(final_pose.pose.position.y)
                    - (math.sin(goal_yaw) * insert_distance)
                )
            opening_source = 'offset_fallback'

        footprint_forward_extent, footprint_rear_extent, footprint_half_width = (
            self._robot_footprint_extents()
        )
        geometry = ShelfGeometry(
            final_pose=self._pose_stamped_to_shelf_pose2d(final_pose),
            opening_line_pose=self._pose_stamped_to_shelf_pose2d(opening_line_pose),
            opening_width=self._current_shelf_opening_width(),
        )
        footprint = ShelfRobotFootprint(
            forward_extent=float(footprint_forward_extent),
            rear_extent=float(footprint_rear_extent),
            half_width=float(footprint_half_width),
        )
        params = ShelfDockingParameters(
            standoff_distance=float(self.goal_pose_handoff_standoff_distance),
            insertion_extra_standoff=float(self.goal_pose_handoff_insertion_extra_standoff),
            local_insert_distance=float(self.goal_pose_handoff_local_insert_distance),
            approach_arrival_tolerance=float(self.goal_pose_handoff_approach_arrival_tolerance),
            entry_clearance_margin=float(self.goal_pose_handoff_entry_clearance_margin),
            opening_width_margin=float(self.goal_pose_handoff_opening_width_margin),
            position_tolerance=max(
                float(self.zone_position_tolerance),
                float(self.goal_pose_handoff_local_insert_position_tolerance),
            ),
            centerline_tolerance=float(self.goal_pose_handoff_local_insert_centerline_tolerance),
            centerline_slowdown_tolerance=float(
                self.goal_pose_handoff_local_insert_centerline_slowdown_tolerance
            ),
            heading_tolerance=float(self.goal_pose_handoff_local_insert_heading_tolerance),
        )
        try:
            return build_shelf_docking_plan(geometry, footprint, params), opening_source, ''
        except ShelfDockingPlanError as exc:
            return None, opening_source, str(exc)

    async def _execute_goal_pose_handoff_follow_path(
        self,
        goal_pose: PoseStamped,
        goal_handle,
        target_label: str,
        *,
        local_insert_speed_cap: Optional[float] = None,
    ) -> Tuple[bool, str]:
        frame_id = str(goal_pose.header.frame_id or 'map') or 'map'
        robot_pose = self._robot_pose_in_frame(frame_id, timeout_sec=0.30)
        if robot_pose is None:
            return False, f'Unable to resolve robot pose in {frame_id} for shelf handoff.'
        final_pose = self._copy_pose_stamped(goal_pose)
        final_pose.header.stamp = self.get_clock().now().to_msg()
        plan, opening_source, plan_error = self._build_shelf_docking_plan(final_pose)
        if plan is None:
            self._clear_active_goal_marker()
            return False, plan_error
        robot_pose2d = self._pose_stamped_to_shelf_pose2d(robot_pose)
        assessment = assess_shelf_docking(
            robot_pose2d,
            plan,
            local_insert_distance=float(self.goal_pose_handoff_local_insert_distance),
            approach_arrival_tolerance=float(self.goal_pose_handoff_approach_arrival_tolerance),
        )

        self.get_logger().info(
            f'Goal-pose handoff plan: center=({plan.final_pose.x:.2f}, {plan.final_pose.y:.2f}) '
            f'opening_line=({plan.opening_line_pose.x:.2f}, {plan.opening_line_pose.y:.2f}) '
            f'entry=({plan.entry_pose.x:.2f}, {plan.entry_pose.y:.2f}) '
            f'approach=({plan.approach_pose.x:.2f}, {plan.approach_pose.y:.2f}) '
            f'heading={math.degrees(plan.final_pose.yaw):.1f}deg '
            f'approach_offset={plan.approach_offset:.2f}m '
            f'entry_guard={plan.entry_guard_distance:.2f}m '
            f'entry_lat_tol={plan.entry_lateral_tolerance:.3f}m '
            f'insert_lat_tol={plan.insert_lateral_tolerance:.3f}m '
            f'opening_source={opening_source}'
        )
        if plan.opening_width is not None:
            self.get_logger().info(
                f'Shelf opening width={plan.opening_width:.2f}m '
                f'required>={plan.required_opening_width:.2f}m '
                f'usable_lateral_clearance={float(plan.usable_lateral_clearance or 0.0):.3f}m'
            )
        navigation_targets = build_shelf_navigation_targets(robot_pose2d, plan)
        if navigation_targets:
            self.get_logger().info(
                'Shelf navigation targets: '
                + ' -> '.join(
                    f'{target.label}({target.pose.x:.2f},{target.pose.y:.2f})'
                    for target in navigation_targets
                )
            )
        else:
            self.get_logger().info('Shelf navigation targets: robot already staged at guarded entry.')

        self._publish_active_goal_marker(target_label, final_pose)
        feedback = GoToZoneAction.Feedback()

        def _phase_acceptance_limits(phase_label: str) -> Tuple[float, float, float]:
            label = str(phase_label or '').strip().lower()
            pos_tol = max(float(plan.position_tolerance), 0.07)
            lat_tol = max(float(plan.entry_lateral_tolerance), 0.05)
            heading_tol = max(float(plan.heading_tolerance), math.radians(10.0))
            if 'retreat' in label:
                pos_tol = max(pos_tol, 0.10)
                heading_tol = max(heading_tol, math.radians(15.0))
            elif 'lineup' in label:
                heading_tol = max(heading_tol, math.radians(12.0))
            elif 'entry alignment' in label:
                pos_tol = max(float(plan.position_tolerance), 0.06)
                lat_tol = max(float(plan.entry_lateral_tolerance), 0.04)
                heading_tol = max(float(plan.heading_tolerance), math.radians(10.0))
            return pos_tol, lat_tol, heading_tol

        def _phase_pose_satisfied(
            target_pose: PoseStamped,
            phase_label: str,
        ) -> Tuple[bool, Optional[Dict[str, float]], str]:
            phase_error, phase_error_msg = self._pose_error_in_target_frame(
                target_pose,
                robot_timeout_sec=0.20,
            )
            if phase_error is None:
                return False, None, phase_error_msg

            pos_tol, lat_tol, heading_tol = _phase_acceptance_limits(phase_label)
            satisfied = (
                float(phase_error['distance']) <= pos_tol
                and abs(float(phase_error['left_error'])) <= lat_tol
                and abs(float(phase_error['heading_error'])) <= heading_tol
            )
            return satisfied, phase_error, ''

        async def _run_follow_path_phase(
            nav_path: NavPath,
            phase_label: str,
            progress_range: Tuple[float, float],
            *,
            target_pose_for_acceptance: Optional[PoseStamped] = None,
        ):
            if not self.nav_follow_path_client.wait_for_server(timeout_sec=self.follow_path_server_wait_sec):
                return False, f'{phase_label} unavailable: Nav2 FollowPath action server not available.'

            follow_goal = Nav2FollowPath.Goal()
            follow_goal.path = nav_path
            if hasattr(follow_goal, 'controller_id'):
                follow_goal.controller_id = str(self.follow_path_controller_id or 'FollowPath')
            if hasattr(follow_goal, 'goal_checker_id'):
                follow_goal.goal_checker_id = str(self.follow_path_goal_checker_id or '')

            dist_state = {'initial': float('inf'), 'remaining': float('inf'), 'speed': 0.0}

            def _follow_feedback(feedback_msg):
                try:
                    nav_fb = getattr(feedback_msg, 'feedback', feedback_msg)
                    dist = getattr(nav_fb, 'distance_to_goal', None)
                    if dist is None:
                        dist = getattr(nav_fb, 'distance_remaining', None)
                    if dist is not None:
                        dist_state['remaining'] = float(dist)
                        if not math.isfinite(dist_state['initial']) and math.isfinite(float(dist)):
                            dist_state['initial'] = max(float(dist), 1e-3)
                    speed = getattr(nav_fb, 'speed', None)
                    if speed is not None:
                        dist_state['speed'] = float(speed)
                except Exception:
                    pass

            self._publish_active_follow_path(nav_path.poses)
            send_future = self.nav_follow_path_client.send_goal_async(
                follow_goal,
                feedback_callback=_follow_feedback,
            )
            nav_goal_handle = await send_future
            if nav_goal_handle is None or not bool(getattr(nav_goal_handle, 'accepted', False)):
                return False, f'{phase_label} was rejected by Nav2 FollowPath.'

            self.follow_path_nav_goal = nav_goal_handle
            nav_result_future = nav_goal_handle.get_result_async()
            last_feedback_ns = 0

            while not nav_result_future.done():
                if goal_handle.is_cancel_requested:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    return False, f'{phase_label} canceled.'

                if self.estop_active:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    return False, f'{phase_label} aborted: E-STOP active.'

                if self.distance_obstacle_hold_active:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    return False, f'{phase_label} aborted: obstacle hold active.'

                now_ns = self.get_clock().now().nanoseconds
                if (now_ns - last_feedback_ns) >= int(0.15 * 1e9):
                    initial = float(dist_state['initial'])
                    remaining = float(dist_state['remaining'])
                    progress = float(progress_range[0])
                    if math.isfinite(initial) and initial > 1e-6 and math.isfinite(remaining):
                        phase_progress = 1.0 - (max(0.0, remaining) / initial)
                        phase_progress = max(0.0, min(1.0, phase_progress))
                        progress = float(progress_range[0] + ((progress_range[1] - progress_range[0]) * phase_progress))
                    feedback.progress = float(progress)
                    status = f'{phase_label}: "{target_label}"'
                    if math.isfinite(remaining):
                        status += f' ({remaining:.2f}m remaining'
                        if math.isfinite(float(dist_state["speed"])):
                            status += f', v={float(dist_state["speed"]):.2f}m/s'
                        status += ')'
                    feedback.status = status
                    goal_handle.publish_feedback(feedback)
                    last_feedback_ns = now_ns

                await self._non_blocking_wait(0.05)

            self.follow_path_nav_goal = None
            try:
                wrapped = nav_result_future.result()
            except Exception as exc:
                return False, f'{phase_label} result failed: {exc}'

            status = int(getattr(wrapped, 'status', GoalStatus.STATUS_UNKNOWN))
            if status != GoalStatus.STATUS_SUCCEEDED:
                if target_pose_for_acceptance is not None:
                    phase_ok, phase_error, _ = _phase_pose_satisfied(
                        target_pose_for_acceptance,
                        phase_label,
                    )
                    if phase_ok and phase_error is not None:
                        self.get_logger().info(
                            f'{phase_label}: accepting Nav2 {self._goal_status_name(status)} '
                            f'because robot is already within short-phase tolerance '
                            f'(dist={float(phase_error["distance"]):.2f}m, '
                            f'left={float(phase_error["left_error"]):.2f}m, '
                            f'heading={math.degrees(float(phase_error["heading_error"])):.1f}deg)'
                        )
                        return True, ''
                return False, (
                    f'{phase_label} failed with status {status} '
                    f'({self._goal_status_name(status)})'
                )
            return True, ''

        async def _execute_navigation_targets_sequence(
            navigation_targets_arg,
            start_pose: PoseStamped,
            *,
            progress_start: float,
            progress_end: float,
            log_prefix: str = 'Shelf handoff',
        ) -> Tuple[Optional[bool], str, Optional[PoseStamped]]:
            if not navigation_targets_arg:
                return True, '', start_pose

            progress_step = (
                (progress_end - progress_start)
                / float(max(1, len(navigation_targets_arg)))
            )
            current_nav_start = progress_start
            current_nav_pose = start_pose

            for index, navigation_target in enumerate(navigation_targets_arg, start=1):
                target_pose = self._shelf_pose2d_to_pose_stamped(navigation_target.pose)
                target_distance = self._distance_to_waypoint_pair(current_nav_pose, target_pose)
                self.get_logger().info(
                    f'{log_prefix} phase {index}/{len(navigation_targets_arg)}: '
                    f'{navigation_target.label} -> '
                    f'({navigation_target.pose.x:.2f}, {navigation_target.pose.y:.2f}) '
                    f'heading={math.degrees(navigation_target.pose.yaw):.1f}deg '
                    f'distance={target_distance:.2f}m'
                )

                phase_ready, phase_error, _ = _phase_pose_satisfied(
                    target_pose,
                    navigation_target.label,
                )
                if phase_ready and phase_error is not None:
                    self.get_logger().info(
                        f'{navigation_target.label}: already satisfied before dispatch '
                        f'(dist={float(phase_error["distance"]):.2f}m, '
                        f'left={float(phase_error["left_error"]):.2f}m, '
                        f'heading={math.degrees(float(phase_error["heading_error"])):.1f}deg)'
                    )
                else:
                    nav_path = self._build_segment_follow_path(
                        current_nav_pose,
                        target_pose,
                        segment_spacing=float(self.goal_pose_handoff_insertion_path_spacing),
                        enforce_end_orientation=True,
                    )
                    nav_ok, nav_msg = await _run_follow_path_phase(
                        nav_path,
                        navigation_target.label,
                        (
                            current_nav_start,
                            min(
                                progress_end,
                                current_nav_start + progress_step,
                            ),
                        ),
                        target_pose_for_acceptance=target_pose,
                    )
                    self._clear_active_follow_path()
                    if not nav_ok:
                        if goal_handle.is_cancel_requested:
                            return None, nav_msg, None
                        return False, nav_msg, None

                current_nav_start = min(progress_end, current_nav_start + progress_step)
                current_nav_pose = self._robot_pose_in_frame(frame_id, timeout_sec=0.30)
                if current_nav_pose is None:
                    return False, (
                        f'Unable to resolve robot pose in {frame_id} after '
                        f'{navigation_target.label.lower()}.'
                    ), None

            return True, '', current_nav_pose

        navigation_progress_start = 0.20
        navigation_progress_end = 0.84
        if navigation_targets:
            nav_ok, nav_msg, current_nav_pose = await _execute_navigation_targets_sequence(
                navigation_targets,
                robot_pose,
                progress_start=navigation_progress_start,
                progress_end=navigation_progress_end,
            )
            if not nav_ok:
                self._clear_active_goal_marker()
                return nav_ok, nav_msg
            if current_nav_pose is not None:
                robot_pose = current_nav_pose

        settle_sec = max(0.20, float(self.path_arrival_settle_sec))
        if settle_sec > 1e-6:
            await self._non_blocking_wait(settle_sec)
            if goal_handle.is_cancel_requested:
                self._clear_active_goal_marker()
                return None, f'Frozen shelf insert canceled during settle for "{target_label}".'
            if self.estop_active:
                self._clear_active_goal_marker()
                return False, f'Frozen shelf insert aborted during settle for "{target_label}": E-STOP active.'

        refined_plan_applied = False
        if bool(self.goal_pose_handoff_live_geometry_enabled):
            refined_goal_pose, refine_err = self._build_shelf_goal_pose_from_status(frame_id)
            if refined_goal_pose is not None:
                final_pose = self._copy_pose_stamped(refined_goal_pose)
                final_pose.header.stamp = self.get_clock().now().to_msg()
                refined_plan, _refined_source, refined_plan_err = self._build_shelf_docking_plan(
                    final_pose,
                )
                if refined_plan is None:
                    self._clear_active_goal_marker()
                    return False, refined_plan_err
                # Guard against yaw flip: reject refinement if yaw changed >90°
                # from committed plan (indicates front/back reflector swap).
                yaw_jump = self._abs_angle_delta(
                    refined_plan.final_pose.yaw, plan.final_pose.yaw
                )
                if yaw_jump > math.pi / 2.0:
                    self.get_logger().warn(
                        f'Frozen shelf insert: rejected live refinement — yaw jumped '
                        f'{math.degrees(yaw_jump):.0f}deg (likely front/back flip). '
                        f'Keeping committed plan for "{target_label}".'
                    )
                else:
                    plan = refined_plan
                    refined_plan_applied = True
                    self.get_logger().info(
                        f'Frozen shelf insert: refined live geometry target to '
                        f'({plan.final_pose.x:.2f}, {plan.final_pose.y:.2f}) '
                        f'heading={math.degrees(plan.final_pose.yaw):.1f}deg for "{target_label}"'
                    )
            elif refine_err:
                self.get_logger().debug(
                    f'Frozen shelf insert kept precomputed target for "{target_label}": {refine_err}'
                )

        robot_pose = self._robot_pose_in_frame(frame_id, timeout_sec=0.20)
        if robot_pose is None:
            self._clear_active_goal_marker()
            return False, f'Unable to resolve robot pose in {frame_id} before straight insertion.'

        assessment = assess_shelf_docking(
            self._pose_stamped_to_shelf_pose2d(robot_pose),
            plan,
            local_insert_distance=float(self.goal_pose_handoff_local_insert_distance),
            approach_arrival_tolerance=float(self.goal_pose_handoff_approach_arrival_tolerance),
        )
        remaining_navigation_targets = build_shelf_navigation_targets(
            self._pose_stamped_to_shelf_pose2d(robot_pose),
            plan,
        )
        if refined_plan_applied and remaining_navigation_targets:
            self.get_logger().info(
                'Shelf refined-plan correction targets: '
                + ' -> '.join(
                    f'{target.label}({target.pose.x:.2f},{target.pose.y:.2f})'
                    for target in remaining_navigation_targets
                )
            )
            correction_ok, correction_msg, corrected_pose = await _execute_navigation_targets_sequence(
                remaining_navigation_targets,
                robot_pose,
                progress_start=0.84,
                progress_end=0.93,
                log_prefix='Shelf refined-plan correction',
            )
            if not correction_ok:
                self._clear_active_goal_marker()
                return correction_ok, correction_msg
            if corrected_pose is not None:
                robot_pose = corrected_pose
            assessment = assess_shelf_docking(
                self._pose_stamped_to_shelf_pose2d(robot_pose),
                plan,
                local_insert_distance=float(self.goal_pose_handoff_local_insert_distance),
                approach_arrival_tolerance=float(self.goal_pose_handoff_approach_arrival_tolerance),
            )
            remaining_navigation_targets = build_shelf_navigation_targets(
                self._pose_stamped_to_shelf_pose2d(robot_pose),
                plan,
            )
        if remaining_navigation_targets:
            self._clear_active_goal_marker()
            return False, (
                'Shelf entry alignment still outside insertion guard after staged approach: '
                + ' -> '.join(target.label for target in remaining_navigation_targets)
                + f' (d_entry={assessment.robot_to_entry:.2f}m, '
                f'entry_left={assessment.entry_error.left_error:.2f}m, '
                f'heading_err={math.degrees(assessment.entry_error.heading_error):.1f}deg).'
            )

        feedback.progress = 0.84
        feedback.status = f'Shelf insertion: driving straight into shelf center for "{target_label}"'
        goal_handle.publish_feedback(feedback)
        insert_ok, insert_msg = await self._execute_shelf_straight_insertion(
            plan,
            goal_handle,
            target_label,
            feedback,
            max_speed_cap=local_insert_speed_cap,
        )
        if insert_ok is None:
            self._clear_active_goal_marker()
            return None, insert_msg
        if not insert_ok:
            self._clear_active_goal_marker()
            return False, insert_msg

        verify_ok, verify_msg = await self._verify_goal_pose_handoff_alignment(
            final_pose,
            goal_handle,
            target_label,
            feedback,
            max_speed_cap=local_insert_speed_cap,
        )
        if not verify_ok:
            self._clear_active_goal_marker()
            if goal_handle.is_cancel_requested:
                return None, verify_msg
            return False, verify_msg

        feedback.progress = 1.0
        feedback.status = f'Shelf insertion completed for "{target_label}"'
        goal_handle.publish_feedback(feedback)
        self._clear_active_goal_marker()
        completion_message = (
            f'Shelf insertion completed to ({final_pose.pose.position.x:.2f}, '
            f'{final_pose.pose.position.y:.2f})'
        )
        if verify_msg and not str(verify_msg).lower().startswith('verification skipped'):
            completion_message = f'{completion_message}; {verify_msg}'
        return True, completion_message

    async def _execute_goal_pose_handoff_with_retries(
        self,
        goal_pose: PoseStamped,
        goal_handle,
        target_label: str,
        *,
        local_insert_speed_cap: Optional[float] = None,
        refresh_from_shelf_status: bool = True,
    ) -> Tuple[Optional[bool], str]:
        max_attempts = max(1, int(self.goal_pose_handoff_retry_attempts))
        retry_delay_sec = max(0.0, float(self.goal_pose_handoff_retry_delay_sec))
        current_goal_pose = self._copy_pose_stamped(goal_pose)
        last_message = ''

        for attempt in range(1, max_attempts + 1):
            if goal_handle.is_cancel_requested:
                return None, f'Goal-pose handoff canceled before attempt {attempt}.'
            if self.estop_active:
                return False, 'Goal-pose handoff aborted before retry: E-STOP active.'

            if refresh_from_shelf_status:
                refreshed_goal_pose, refresh_err = self._build_shelf_goal_pose_from_status(
                    str(current_goal_pose.header.frame_id or 'map') or 'map'
                )
                if refreshed_goal_pose is not None:
                    current_goal_pose = self._copy_pose_stamped(refreshed_goal_pose)
                    current_goal_pose.header.stamp = self.get_clock().now().to_msg()
                elif attempt == 1 and refresh_err:
                    self.get_logger().debug(
                        f'Goal-pose handoff retry wrapper using provided target for "{target_label}": '
                        f'{refresh_err}'
                    )

            if attempt > 1:
                self.get_logger().warn(
                    f'Goal-pose handoff retry {attempt}/{max_attempts} for "{target_label}" '
                    f'using target ({current_goal_pose.pose.position.x:.2f}, '
                    f'{current_goal_pose.pose.position.y:.2f})'
                )
                try:
                    retry_feedback = GoToZoneAction.Feedback()
                    retry_feedback.status = (
                        f'Goal-pose handoff retry {attempt}/{max_attempts} for "{target_label}"'
                    )
                    goal_handle.publish_feedback(retry_feedback)
                except Exception:
                    pass

            handoff_ok, handoff_message = await self._execute_goal_pose_handoff_follow_path(
                current_goal_pose,
                goal_handle,
                target_label,
                local_insert_speed_cap=local_insert_speed_cap,
            )
            last_message = str(handoff_message or '').strip()
            if handoff_ok is True:
                if attempt > 1:
                    suffix = f' after {attempt} attempts'
                    if last_message:
                        last_message = f'{last_message}{suffix}'
                    else:
                        last_message = f'Goal-pose handoff succeeded{suffix}.'
                return True, last_message

            if handoff_ok is None:
                return None, last_message or 'Goal-pose handoff canceled.'

            if goal_handle.is_cancel_requested:
                return None, last_message or 'Goal-pose handoff canceled.'
            if self.estop_active:
                return False, last_message or 'Goal-pose handoff aborted: E-STOP active.'

            if attempt >= max_attempts:
                break

            self.get_logger().warn(
                f'Goal-pose handoff attempt {attempt}/{max_attempts} failed for "{target_label}": '
                f'{last_message or "unknown failure"} -> retrying'
            )
            if retry_delay_sec > 1e-6:
                await self._non_blocking_wait(retry_delay_sec)

        return False, (
            last_message
            or f'Goal-pose handoff failed after {max_attempts} attempts for "{target_label}".'
        )

    @staticmethod
    def _distance_to_waypoint_pair(a: PoseStamped, b: PoseStamped) -> float:
        return math.hypot(
            float(a.pose.position.x) - float(b.pose.position.x),
            float(a.pose.position.y) - float(b.pose.position.y),
        )

    async def _run_nav_follow_path_phase(
        self,
        nav_path: NavPath,
        goal_handle,
        phase_label: str,
        *,
        on_feedback: Optional[Callable[[float, float], None]] = None,
        max_runtime_sec: float = 0.0,
        allow_partial_completion_on_timeout: bool = False,
        clear_motion_intent_before_start: bool = False,
    ) -> Tuple[bool, str]:
        if not self.nav_follow_path_client.wait_for_server(timeout_sec=self.follow_path_server_wait_sec):
            return False, f'{phase_label} unavailable: Nav2 FollowPath action server not available.'

        if clear_motion_intent_before_start:
            await self._clear_motion_intent(source=f'zone_manager:pre_{phase_label}')

        follow_goal = Nav2FollowPath.Goal()
        follow_goal.path = nav_path
        if hasattr(follow_goal, 'controller_id'):
            follow_goal.controller_id = str(self.follow_path_controller_id or 'FollowPath')
        if hasattr(follow_goal, 'goal_checker_id'):
            follow_goal.goal_checker_id = str(self.follow_path_goal_checker_id or '')

        dist_state = {'remaining': float('inf'), 'speed': float('nan')}

        def _follow_feedback(feedback_msg):
            try:
                nav_fb = getattr(feedback_msg, 'feedback', feedback_msg)
                dist = getattr(nav_fb, 'distance_to_goal', None)
                if dist is None:
                    dist = getattr(nav_fb, 'distance_remaining', None)
                if dist is not None:
                    dist_state['remaining'] = float(dist)
                speed = getattr(nav_fb, 'speed', None)
                if speed is not None:
                    dist_state['speed'] = float(speed)
                if on_feedback is not None:
                    on_feedback(float(dist_state['remaining']), float(dist_state['speed']))
            except Exception:
                pass

        self._publish_active_follow_path(nav_path.poses)
        send_future = self.nav_follow_path_client.send_goal_async(
            follow_goal,
            feedback_callback=_follow_feedback,
        )
        nav_goal_handle = await send_future
        if nav_goal_handle is None or not bool(getattr(nav_goal_handle, 'accepted', False)):
            self._clear_active_follow_path()
            return False, f'{phase_label} was rejected by Nav2 FollowPath.'

        self.follow_path_nav_goal = nav_goal_handle
        nav_result_future = nav_goal_handle.get_result_async()
        phase_started_monotonic = time.monotonic()
        try:
            while not nav_result_future.done():
                if goal_handle.is_cancel_requested:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    self._clear_active_follow_path()
                    return False, f'{phase_label} canceled.'

                if self.estop_active:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    self._clear_active_follow_path()
                    return False, f'{phase_label} aborted: E-STOP active.'

                if self.distance_obstacle_hold_active:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    self._clear_active_follow_path()
                    return False, f'{phase_label} aborted: obstacle hold active.'

                if (
                    max_runtime_sec > 0.0
                    and (time.monotonic() - phase_started_monotonic) >= float(max_runtime_sec)
                ):
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    self._clear_active_follow_path()
                    if allow_partial_completion_on_timeout:
                        return True, (
                            f'{phase_label} slice completed after '
                            f'{float(max_runtime_sec):.2f}s'
                        )
                    return False, (
                        f'{phase_label} timed out after '
                        f'{float(max_runtime_sec):.2f}s'
                    )

                await self._non_blocking_wait(0.05)

            wrapped = nav_result_future.result()
            status = int(getattr(wrapped, 'status', GoalStatus.STATUS_UNKNOWN))
            if status != GoalStatus.STATUS_SUCCEEDED:
                return False, (
                    f'{phase_label} failed with status {status} '
                    f'({self._goal_status_name(status)})'
                )
            return True, ''
        except Exception as exc:
            return False, f'{phase_label} result failed: {exc}'
        finally:
            self.follow_path_nav_goal = None
            self._clear_active_follow_path()

    async def _execute_shelf_straight_insertion(
        self,
        plan: ShelfDockingPlan,
        goal_handle,
        target_label: str,
        feedback,
        *,
        max_speed_cap: Optional[float] = None,
    ) -> Tuple[Optional[bool], str]:
        frame_id = str(plan.final_pose.frame_id or 'map') or 'map'
        final_pose = self._shelf_pose2d_to_pose_stamped(plan.final_pose)
        timeout_sec = self._optional_timeout_parameter(
            self.goal_pose_handoff_local_insert_timeout_sec,
            minimum=1.0,
        )
        max_speed = max(0.02, float(self.goal_pose_handoff_local_insert_max_speed))
        if max_speed_cap is not None:
            try:
                max_speed = min(max_speed, max(0.02, float(max_speed_cap)))
            except Exception:
                pass
        max_turn = min(
            max(0.10, float(self.goal_pose_handoff_local_insert_max_turn)),
            max(0.05, float(self.goal_pose_handoff_local_insert_align_max_turn)),
        )
        xy_tolerance = max(
            float(plan.position_tolerance),
            float(plan.insert_lateral_tolerance),
        )
        yaw_tolerance = max(
            max(0.02, float(plan.heading_tolerance)),
            float(self.goal_pose_handoff_local_insert_heading_tolerance),
        )

        robot_pose = self._robot_pose_in_frame(frame_id, timeout_sec=0.20)
        if robot_pose is None:
            return False, f'Unable to resolve robot pose in {frame_id} before shelf insertion.'

        nav_path = self._build_segment_follow_path(
            robot_pose,
            final_pose,
            segment_spacing=float(self.goal_pose_handoff_insertion_path_spacing),
            enforce_end_orientation=True,
        )
        initial_error, _ = self._pose_error_in_target_frame(
            final_pose,
            robot_pose=robot_pose,
        )
        initial_distance = float(initial_error['distance']) if initial_error else float('inf')

        intent_ok, intent_msg = await self._set_motion_intent(
            'insertion',
            final_pose,
            max_linear_speed=max_speed,
            max_angular_speed=max_turn,
            xy_tolerance=xy_tolerance,
            yaw_tolerance=yaw_tolerance,
            timeout_sec=max(timeout_sec, 1.0),
            source='zone_manager:shelf_insert',
        )
        if not intent_ok:
            return False, f'Shelf insertion could not set controller intent: {intent_msg}'

        last_feedback_ns = 0

        def _on_feedback(remaining: float, speed: float) -> None:
            nonlocal last_feedback_ns
            now_ns = self.get_clock().now().nanoseconds
            if (now_ns - last_feedback_ns) < int(0.15 * 1e9):
                return

            progress = 0.84
            if math.isfinite(initial_distance) and initial_distance > 1e-6 and math.isfinite(remaining):
                phase_progress = 1.0 - (max(0.0, remaining) / initial_distance)
                progress = 0.84 + (0.15 * max(0.0, min(1.0, phase_progress)))
            feedback.progress = float(self._clamp(progress, 0.84, 0.99))
            status = f'Shelf insertion: "{target_label}"'
            if math.isfinite(remaining):
                status += f' ({remaining:.2f}m remaining'
                if math.isfinite(speed):
                    status += f', v={speed:.2f}m/s'
                status += ')'
            feedback.status = status
            goal_handle.publish_feedback(feedback)
            last_feedback_ns = now_ns

        try:
            insert_ok, insert_msg = await self._run_nav_follow_path_phase(
                nav_path,
                goal_handle,
                f'Shelf insertion "{target_label}"',
                on_feedback=_on_feedback,
            )
            if not insert_ok and goal_handle.is_cancel_requested:
                return None, insert_msg
            if not insert_ok:
                return False, insert_msg

            final_error, final_error_msg = self._pose_error_in_target_frame(final_pose)
            if final_error is None:
                return False, final_error_msg

            return True, (
                f'Inserted to shelf center at ({plan.final_pose.x:.2f}, '
                f'{plan.final_pose.y:.2f}) with heading err '
                f'{math.degrees(float(final_error["heading_error"])):.1f}deg.'
            )
        finally:
            await self._clear_motion_intent(source='zone_manager:shelf_insert')

    def _curve_edge_extra_corridor_width(
        self,
        waypoints,
        seg_idx: int,
        seg_t: float,
        seg_len: float,
    ) -> float:
        """Return extra half-width near turn edges; straight segments stay unchanged."""
        if not self.path_curve_edge_leniency_enabled:
            return 0.0
        if len(waypoints) < 3:
            return 0.0
        max_extra = float(self.path_curve_edge_extra_width_m)
        if max_extra <= 1e-6:
            return 0.0
        transition_m = float(self.path_curve_edge_transition_m)
        if transition_m <= 1e-6:
            return 0.0

        heading_curr = self._heading_for_segment(waypoints, seg_idx)
        if heading_curr is None:
            return 0.0

        threshold = math.radians(float(self.path_curve_turn_threshold_deg))
        threshold = max(0.0, min(threshold, math.radians(89.0)))
        max_ref = math.radians(90.0)
        denom = max(1e-6, max_ref - threshold)

        seg_len = max(0.0, float(seg_len))
        seg_t = max(0.0, min(1.0, float(seg_t)))
        dist_to_start = seg_t * seg_len
        dist_to_end = (1.0 - seg_t) * seg_len

        extra = 0.0

        if seg_idx > 0:
            heading_prev = self._heading_for_segment(waypoints, seg_idx - 1)
            if heading_prev is not None:
                turn = self._abs_angle_delta(heading_curr, heading_prev)
                if turn > threshold and dist_to_start < transition_m:
                    turn_factor = min(1.0, (turn - threshold) / denom)
                    edge_factor = 1.0 - (dist_to_start / transition_m)
                    extra = max(extra, max_extra * turn_factor * edge_factor)

        if seg_idx < (len(waypoints) - 2):
            heading_next = self._heading_for_segment(waypoints, seg_idx + 1)
            if heading_next is not None:
                turn = self._abs_angle_delta(heading_next, heading_curr)
                if turn > threshold and dist_to_end < transition_m:
                    turn_factor = min(1.0, (turn - threshold) / denom)
                    edge_factor = 1.0 - (dist_to_end / transition_m)
                    extra = max(extra, max_extra * turn_factor * edge_factor)

        return max(0.0, float(extra))

    def _segment_has_curve_transition(self, waypoints, seg_idx: int) -> bool:
        """Return True when a segment is adjacent to a turning waypoint."""
        if len(waypoints) < 3:
            return False
        if seg_idx < 0 or seg_idx >= (len(waypoints) - 1):
            return False

        heading_curr = self._heading_for_segment(waypoints, seg_idx)
        if heading_curr is None:
            return False

        threshold = math.radians(float(self.path_curve_turn_threshold_deg))
        threshold = max(0.0, min(threshold, math.radians(89.0)))

        if seg_idx > 0:
            heading_prev = self._heading_for_segment(waypoints, seg_idx - 1)
            if heading_prev is not None and self._abs_angle_delta(heading_curr, heading_prev) > threshold:
                return True

        if seg_idx < (len(waypoints) - 2):
            heading_next = self._heading_for_segment(waypoints, seg_idx + 1)
            if heading_next is not None and self._abs_angle_delta(heading_next, heading_curr) > threshold:
                return True

        return False

    @staticmethod
    def _project_point_onto_segment(px: float, py: float, ax: float, ay: float, bx: float, by: float):
        """Project a point onto segment AB.

        Returns:
            t (0..1), lateral distance to segment, segment length.
        """
        abx = bx - ax
        aby = by - ay
        denom = (abx * abx) + (aby * aby)
        if denom <= 1e-12:
            return 0.0, math.hypot(px - ax, py - ay), 0.0

        t = ((px - ax) * abx + (py - ay) * aby) / denom
        t = max(0.0, min(1.0, t))
        cx = ax + t * abx
        cy = ay + t * aby
        return float(t), math.hypot(px - cx, py - cy), math.sqrt(denom)

    @staticmethod
    def _waypoint_cumulative_lengths(waypoints):
        """Return cumulative polyline distances for each waypoint anchor."""
        if not waypoints:
            return []

        cumulative = [0.0]
        total = 0.0
        for idx in range(1, len(waypoints)):
            dx = float(waypoints[idx].pose.position.x) - float(waypoints[idx - 1].pose.position.x)
            dy = float(waypoints[idx].pose.position.y) - float(waypoints[idx - 1].pose.position.y)
            total += math.hypot(dx, dy)
            cumulative.append(float(total))
        return cumulative

    def _current_segment_lateral_error(self, waypoints, progress_index: int):
        if len(waypoints) < 2:
            return float('inf'), 0, 0.0, 0.0

        seg_idx = max(0, min(int(progress_index), len(waypoints) - 2))

        frame_id = (
            waypoints[seg_idx].header.frame_id
            or waypoints[seg_idx + 1].header.frame_id
            or 'map'
        )
        robot_pose = self._robot_pose_in_frame(frame_id, timeout_sec=0.12)
        if robot_pose is None:
            return float('inf'), seg_idx, 0.0, 0.0

        px = float(robot_pose.pose.position.x)
        py = float(robot_pose.pose.position.y)

        if self.path_corridor_global_segment_search:
            candidate_segments = range(0, len(waypoints) - 1)
        else:
            candidate_segments = {seg_idx}
            if seg_idx > 0:
                candidate_segments.add(seg_idx - 1)
            if seg_idx < (len(waypoints) - 2):
                candidate_segments.add(seg_idx + 1)

        best_dist = float('inf')
        best_score = float('inf')
        best_seg = seg_idx
        best_t = 0.0
        best_seg_len = 0.0
        # When global search is enabled, bias toward current progress segment
        # to avoid arming/enforcing against a distant "nearest" crossing segment.
        progress_bias_per_segment = max(0.02, float(self.path_corridor_half_width) * 0.25)
        for cand_idx in candidate_segments:
            ax = float(waypoints[cand_idx].pose.position.x)
            ay = float(waypoints[cand_idx].pose.position.y)
            bx = float(waypoints[cand_idx + 1].pose.position.x)
            by = float(waypoints[cand_idx + 1].pose.position.y)
            t, dist, seg_len = self._project_point_onto_segment(px, py, ax, ay, bx, by)
            if self.path_corridor_global_segment_search:
                score = dist + (progress_bias_per_segment * abs(int(cand_idx) - int(seg_idx)))
            else:
                score = dist
            if score < best_score:
                best_score = score
                best_dist = dist
                best_seg = cand_idx
                best_t = t
                best_seg_len = seg_len

        return float(best_dist), int(best_seg), float(best_t), float(best_seg_len)

    def _interpolate_waypoints(self, waypoints):
        """Optionally insert intermediate checkpoints along each segment."""
        if len(waypoints) < 2 or not self.path_interpolation_enabled:
            return list(waypoints)

        if self.path_interpolation_spacing <= 0.0:
            return list(waypoints)

        dense_waypoints = [waypoints[0]]

        for idx in range(1, len(waypoints)):
            prev = waypoints[idx - 1]
            curr = waypoints[idx]
            dx = curr.pose.position.x - prev.pose.position.x
            dy = curr.pose.position.y - prev.pose.position.y
            dist = math.hypot(dx, dy)

            if dist <= self.path_interpolation_spacing:
                dense_waypoints.append(curr)
                continue

            steps = max(2, int(math.ceil(dist / self.path_interpolation_spacing)))
            heading = math.atan2(dy, dx)
            qz = math.sin(heading / 2.0)
            qw = math.cos(heading / 2.0)

            for step in range(1, steps):
                ratio = float(step) / float(steps)
                pose = PoseStamped()
                pose.header.frame_id = curr.header.frame_id
                pose.pose.position.x = prev.pose.position.x + dx * ratio
                pose.pose.position.y = prev.pose.position.y + dy * ratio
                pose.pose.position.z = 0.0
                pose.pose.orientation.x = 0.0
                pose.pose.orientation.y = 0.0
                pose.pose.orientation.z = qz
                pose.pose.orientation.w = qw
                dense_waypoints.append(pose)

            dense_waypoints.append(curr)

        return dense_waypoints

    @staticmethod
    def _quat_multiply(q1, q2):
        x1, y1, z1, w1 = q1
        x2, y2, z2, w2 = q2
        return (
            (w1 * x2) + (x1 * w2) + (y1 * z2) - (z1 * y2),
            (w1 * y2) - (x1 * z2) + (y1 * w2) + (z1 * x2),
            (w1 * z2) + (x1 * y2) - (y1 * x2) + (z1 * w2),
            (w1 * w2) - (x1 * x2) - (y1 * y2) - (z1 * z2),
        )

    @staticmethod
    def _quat_conjugate(q):
        x, y, z, w = q
        return (-x, -y, -z, w)

    @staticmethod
    def _quat_normalize(q):
        x, y, z, w = q
        norm = math.sqrt((x * x) + (y * y) + (z * z) + (w * w))
        if norm <= 1e-12:
            return (0.0, 0.0, 0.0, 1.0)
        inv = 1.0 / norm
        return (x * inv, y * inv, z * inv, w * inv)

    def _rotate_vector_by_quat(self, vx, vy, vz, q):
        qn = self._quat_normalize(q)
        qv = (float(vx), float(vy), float(vz), 0.0)
        rx, ry, rz, _ = self._quat_multiply(
            self._quat_multiply(qn, qv),
            self._quat_conjugate(qn),
        )
        return float(rx), float(ry), float(rz)

    def _transform_goal_pose_to_map(self, msg: PoseStamped):
        source_frame = str(msg.header.frame_id or '').strip()
        if not source_frame:
            return None, 'Goal pose frame_id is empty; cannot transform to map'

        # Fast path: already in map frame.
        if source_frame == 'map':
            mapped = PoseStamped()
            mapped.header.frame_id = 'map'
            mapped.header.stamp = msg.header.stamp
            mapped.pose.position.x = float(msg.pose.position.x)
            mapped.pose.position.y = float(msg.pose.position.y)
            mapped.pose.position.z = float(msg.pose.position.z)
            mapped.pose.orientation.x = float(msg.pose.orientation.x)
            mapped.pose.orientation.y = float(msg.pose.orientation.y)
            mapped.pose.orientation.z = float(msg.pose.orientation.z)
            mapped.pose.orientation.w = float(msg.pose.orientation.w)
            self._ensure_pose_orientation(mapped.pose)
            return mapped, None

        query_times = []
        try:
            query_times.append(rclpy.time.Time.from_msg(msg.header.stamp))
        except Exception:
            pass
        query_times.append(rclpy.time.Time())

        transform = None
        last_error = None
        for query_time in query_times:
            try:
                transform = self.tf_buffer.lookup_transform(
                    'map',
                    source_frame,
                    query_time,
                    timeout=rclpy.duration.Duration(seconds=0.5),
                )
                break
            except Exception as exc:
                last_error = exc

        if transform is None:
            return None, f'Unable to transform goal pose from "{source_frame}" to "map": {last_error}'

        tx = float(transform.transform.translation.x)
        ty = float(transform.transform.translation.y)
        tz = float(transform.transform.translation.z)
        q_tf = (
            float(transform.transform.rotation.x),
            float(transform.transform.rotation.y),
            float(transform.transform.rotation.z),
            float(transform.transform.rotation.w),
        )

        pose_q = (
            float(msg.pose.orientation.x),
            float(msg.pose.orientation.y),
            float(msg.pose.orientation.z),
            float(msg.pose.orientation.w),
        )
        if (abs(pose_q[0]) + abs(pose_q[1]) + abs(pose_q[2]) + abs(pose_q[3])) < 1e-6:
            pose_q = (0.0, 0.0, 0.0, 1.0)

        rx, ry, rz = self._rotate_vector_by_quat(
            float(msg.pose.position.x),
            float(msg.pose.position.y),
            float(msg.pose.position.z),
            q_tf,
        )
        out_q = self._quat_normalize(self._quat_multiply(self._quat_normalize(q_tf), pose_q))

        mapped = PoseStamped()
        mapped.header.frame_id = 'map'
        mapped.header.stamp = self.get_clock().now().to_msg()
        mapped.pose.position.x = rx + tx
        mapped.pose.position.y = ry + ty
        mapped.pose.position.z = rz + tz
        mapped.pose.orientation.x = out_q[0]
        mapped.pose.orientation.y = out_q[1]
        mapped.pose.orientation.z = out_q[2]
        mapped.pose.orientation.w = out_q[3]
        self._ensure_pose_orientation(mapped.pose)

        return mapped, None

    def _goal_pose_zone_payload(self):
        pose = self.last_goal_pose
        if pose is None:
            # Fall back to the committed shelf target so the UI shelf-trigger
            # flow works without requiring a prior RViz /goal_pose.
            # _execute_goal_pose_handoff_with_retries will immediately refresh
            # from latest_shelf_status, so this seed pose just needs to be valid.
            shelf_pose, _ = self._build_shelf_goal_pose_from_status('map')
            if shelf_pose is not None:
                pose = shelf_pose
            else:
                return None, (self.last_goal_pose_error or 'No goal pose received yet.')

        return {
            'position': {
                'x': float(pose.pose.position.x),
                'y': float(pose.pose.position.y),
                'z': float(pose.pose.position.z),
            },
            'orientation': {
                'x': float(pose.pose.orientation.x),
                'y': float(pose.pose.orientation.y),
                'z': float(pose.pose.orientation.z),
                'w': float(pose.pose.orientation.w),
            },
            'frame_id': str(pose.header.frame_id or 'map') or 'map',
        }, ''

    def _resolve_go_to_zone_target(self, zone_name: str):
        requested_name = str(zone_name or '').strip()
        if requested_name == self.GOAL_POSE_HANDOFF_NAME:
            zone_payload, error = self._goal_pose_zone_payload()
            if zone_payload is None:
                return None, 'goal pose', error
            return zone_payload, 'goal pose', ''

        self.load_zones()
        zones = dict(self.zones.get("zones", {}))
        if requested_name not in zones:
            return None, requested_name, f'Zone "{requested_name}" not found'
        return zones[requested_name], requested_name, ''

    def goal_pose_callback(self, msg):
        """Store the last goal pose from RViz, normalized into map frame."""
        mapped_pose, error = self._transform_goal_pose_to_map(msg)
        if error is not None:
            self.last_goal_pose = None
            self.last_goal_pose_error = error
            self.get_logger().warn(f'Rejected goal pose: {error}')
            return

        self.last_goal_pose = mapped_pose
        self.last_goal_pose_error = None

        source_frame = str(msg.header.frame_id or '').strip() or '<empty>'
        if source_frame == 'map':
            self.get_logger().info(
                f'Received goal pose [map]: x={mapped_pose.pose.position.x:.2f}, '
                f'y={mapped_pose.pose.position.y:.2f}')
        else:
            self.get_logger().info(
                f'Received goal pose [{source_frame}] transformed to [map]: '
                f'x={mapped_pose.pose.position.x:.2f}, '
                f'y={mapped_pose.pose.position.y:.2f}')

    def save_zone_callback(self, request, response):
        """Save the last goal pose as a named zone"""
        zone_name = request.name

        if not self.last_goal_pose:
            response.ok = False
            response.message = self.last_goal_pose_error or 'No goal pose received yet. Please set a goal in RViz first.'
            self.get_logger().warn(response.message)
            return response

        zone_data = {
            'position': {
                'x': self.last_goal_pose.pose.position.x,
                'y': self.last_goal_pose.pose.position.y,
                'z': self.last_goal_pose.pose.position.z
            },
            'orientation': {
                'x': self.last_goal_pose.pose.orientation.x,
                'y': self.last_goal_pose.pose.orientation.y,
                'z': self.last_goal_pose.pose.orientation.z,
                'w': self.last_goal_pose.pose.orientation.w
            },
            'frame_id': self.last_goal_pose.header.frame_id
        }

        # Preserve existing metadata (type, speed, action, charge_duration) so
        # that position/heading updates don't silently reset zone properties.
        try:
            existing_zones = self.db_manager.get_all_zones()
            if zone_name in existing_zones:
                existing = existing_zones[zone_name]
                for key in ('type', 'speed', 'action', 'charge_duration'):
                    if key in existing:
                        zone_data[key] = existing[key]
        except Exception as e:
            self.get_logger().warn(f'Could not load existing zone metadata for "{zone_name}": {e}')

        self.db_manager.save_zone(zone_name, zone_data)
        # Update cache
        self.zones.setdefault('zones', {})
        self.zones['zones'][zone_name] = zone_data

        response.ok = True
        response.message = f'Zone "{zone_name}" saved successfully at ({self.last_goal_pose.pose.position.x:.2f}, {self.last_goal_pose.pose.position.y:.2f})'
        self.get_logger().info(response.message)
        self._publish_viz_markers()
        return response

    def go_to_zone_goal_callback(self, goal_request):
        """Validate incoming GoToZone action goals"""
        if self.estop_active:
            self.get_logger().warn('Rejecting go_to_zone goal: E-STOP active')
            return GoalResponse.REJECT

        zone_payload, target_label, error = self._resolve_go_to_zone_target(goal_request.name)
        if zone_payload is None:
            self.get_logger().warn(f'GoToZone goal rejected: {error}')
            return GoalResponse.REJECT

        if self.go_to_next_ros2ws_goal is not None or self.follow_path_nav_goal is not None:
            self.get_logger().warn(
                'Rejecting go_to_zone goal: ZoneManager only forwards one Nav2 goal at a time; '
                'use NavigationArbitrator for serialization'
            )
            return GoalResponse.REJECT

        if str(goal_request.name or '').strip() == self.GOAL_POSE_HANDOFF_NAME:
            self.get_logger().info(
                f'Accepted GoToZone goal-pose request for {target_label} '
                f'({zone_payload["position"]["x"]:.2f}, {zone_payload["position"]["y"]:.2f})'
            )

        return GoalResponse.ACCEPT

    def go_to_zone_cancel_callback(self, goal_handle):
        del goal_handle
        if self.go_to_next_ros2ws_goal is not None:
            self._request_cancel_with_terminal_log(
                self.go_to_next_ros2ws_goal,
                'go_to_zone cancel callback',
            )
        return CancelResponse.ACCEPT

    async def _forward_navigate_to_pose_goal(
        self,
        goal_handle,
        *,
        target_label: str,
        target_pose: PoseStamped,
    ):
        result = GoToZoneAction.Result()

        if not self.nav_client.wait_for_server(timeout_sec=5.0):
            result.success = False
            result.message = 'Nav2 NavigateToPose action server not available'
            goal_handle.abort()
            return result

        bt_ready, bt_msg = await self._ensure_bt_navigator_active()
        if not bt_ready:
            result.success = False
            result.message = f'Nav2 NavigateToPose unavailable: {bt_msg}'
            goal_handle.abort()
            return result

        if goal_handle.is_cancel_requested:
            result.success = False
            result.message = 'GoToZone canceled before dispatch'
            goal_handle.canceled()
            return result

        feedback = GoToZoneAction.Feedback()
        feedback.progress = 0.0
        feedback.status = f'Sending "{target_label}" to Nav2'
        goal_handle.publish_feedback(feedback)

        nav_feedback_state = {
            'initial_distance': self._distance_to_goal(target_pose),
            'last_progress': 0.0,
            'last_publish_ns': 0,
        }
        if (
            not math.isfinite(float(nav_feedback_state['initial_distance']))
            or float(nav_feedback_state['initial_distance']) < 0.05
        ):
            nav_feedback_state['initial_distance'] = None

        def _nav_feedback_cb(nav_feedback_msg):
            if not goal_handle.is_active:
                return

            nav_feedback = getattr(nav_feedback_msg, 'feedback', None)
            if nav_feedback is None:
                return

            dist_remaining = getattr(nav_feedback, 'distance_remaining', None)
            if dist_remaining is None:
                return

            try:
                dist_remaining = float(dist_remaining)
            except (TypeError, ValueError):
                return

            if (
                nav_feedback_state['initial_distance'] is None
                and math.isfinite(dist_remaining)
                and dist_remaining > 0.05
            ):
                nav_feedback_state['initial_distance'] = dist_remaining

            progress = 0.0
            initial_dist = nav_feedback_state['initial_distance']
            if (
                initial_dist is not None
                and math.isfinite(initial_dist)
                and initial_dist > 1e-6
                and math.isfinite(dist_remaining)
            ):
                progress = 1.0 - (max(0.0, dist_remaining) / initial_dist)
                progress = max(0.0, min(0.99, progress))

            now_ns = self.get_clock().now().nanoseconds
            if (
                (now_ns - int(nav_feedback_state['last_publish_ns'])) < int(0.2 * 1e9)
                and abs(progress - float(nav_feedback_state['last_progress'])) < 0.01
            ):
                return

            feedback_msg = GoToZoneAction.Feedback()
            feedback_msg.progress = float(progress)
            if math.isfinite(dist_remaining):
                feedback_msg.status = f'Nav2 driving to "{target_label}" ({dist_remaining:.2f}m remaining)'
            else:
                feedback_msg.status = f'Nav2 driving to "{target_label}"'
            goal_handle.publish_feedback(feedback_msg)

            nav_feedback_state['last_publish_ns'] = now_ns
            nav_feedback_state['last_progress'] = progress

        nav_goal = NavigateToPose.Goal()
        nav_goal.pose = target_pose
        nav_goal.pose.header.stamp = self.get_clock().now().to_msg()

        await self._clear_motion_intent(source='zone_manager:pre_navigate_to_pose')

        send_future = self.nav_client.send_goal_async(
            nav_goal,
            feedback_callback=_nav_feedback_cb,
        )
        nav_goal_handle = await send_future
        if nav_goal_handle is None or (not nav_goal_handle.accepted):
            result.success = False
            result.message = (
                f'Nav2 rejected NavigateToPose goal for "{target_label}" '
                f'({target_pose.pose.position.x:.2f}, {target_pose.pose.position.y:.2f})'
            )
            goal_handle.abort()
            return result

        self.go_to_next_ros2ws_goal = nav_goal_handle
        nav_result_future = nav_goal_handle.get_result_async()
        cancel_sent = False
        estop_abort_requested = False

        while not nav_result_future.done():
            if goal_handle.is_cancel_requested and (not cancel_sent):
                cancel_sent = True
                try:
                    nav_goal_handle.cancel_goal_async()
                except Exception:
                    pass

            if self.estop_active and (not estop_abort_requested):
                estop_abort_requested = True
                try:
                    nav_goal_handle.cancel_goal_async()
                except Exception:
                    pass

            await self._non_blocking_wait(0.05)

        try:
            wrapped = nav_result_future.result()
        except Exception as exc:
            result.success = False
            result.message = f'NavigateToPose result failed for "{target_label}": {exc}'
            goal_handle.abort()
            return result
        finally:
            self.go_to_next_ros2ws_goal = None

        status = int(getattr(wrapped, 'status', GoalStatus.STATUS_UNKNOWN))
        if estop_abort_requested:
            result.success = False
            result.message = 'E-STOP active'
            goal_handle.abort()
            return result

        if goal_handle.is_cancel_requested or status == GoalStatus.STATUS_CANCELED:
            result.success = False
            result.message = f'GoToZone canceled for "{target_label}"'
            goal_handle.canceled()
            return result

        if status == GoalStatus.STATUS_SUCCEEDED:
            feedback.progress = 1.0
            feedback.status = 'Arrived'
            goal_handle.publish_feedback(feedback)
            result.success = True
            result.message = f'Reached "{target_label}" via Nav2 success'
            goal_handle.succeed()
            return result

        result.success = False
        result.message = (
            f'Nav2 failed for "{target_label}" with status {status} '
            f'({self._goal_status_name(status)})'
        )
        goal_handle.abort()
        return result

    async def _forward_follow_path_goal(self, goal_handle, nav_path: NavPath, total: int):
        result = FollowPathAction.Result()

        if not self.nav_follow_path_client.wait_for_server(timeout_sec=5.0):
            result.success = False
            result.completed = 0
            result.message = 'Nav2 FollowPath action server not available (/follow_path)'
            goal_handle.abort()
            return result

        bt_ready, bt_msg = await self._ensure_bt_navigator_active()
        if not bt_ready:
            result.success = False
            result.completed = 0
            result.message = f'Nav2 FollowPath unavailable: {bt_msg}'
            goal_handle.abort()
            return result

        if goal_handle.is_cancel_requested:
            result.success = False
            result.completed = 0
            result.message = 'FollowPath canceled before dispatch'
            goal_handle.canceled()
            return result

        feedback = FollowPathAction.Feedback()
        feedback.current_index = 0
        feedback.total = int(total)
        feedback.status = f'Sending {total} waypoints to Nav2 FollowPath'
        goal_handle.publish_feedback(feedback)

        nav_feedback_state = {
            'initial_distance': None,
            'last_fraction': 0.0,
            'last_publish_ns': 0,
            'distance': float('inf'),
            'speed': float('nan'),
        }

        def _follow_feedback_cb(nav_feedback_msg):
            if not goal_handle.is_active:
                return

            nav_feedback = getattr(nav_feedback_msg, 'feedback', nav_feedback_msg)
            distance = getattr(nav_feedback, 'distance_to_goal', None)
            if distance is None:
                distance = getattr(nav_feedback, 'distance_remaining', None)
            speed = getattr(nav_feedback, 'speed', None)

            try:
                if distance is not None:
                    distance = float(distance)
                    nav_feedback_state['distance'] = distance
                if speed is not None:
                    nav_feedback_state['speed'] = float(speed)
            except (TypeError, ValueError):
                return

            if (
                nav_feedback_state['initial_distance'] is None
                and distance is not None
                and math.isfinite(distance)
                and distance > 0.05
            ):
                nav_feedback_state['initial_distance'] = distance

            fraction = 0.0
            initial_distance = nav_feedback_state['initial_distance']
            current_distance = nav_feedback_state['distance']
            if (
                initial_distance is not None
                and math.isfinite(initial_distance)
                and initial_distance > 1e-6
                and math.isfinite(current_distance)
            ):
                fraction = 1.0 - (max(0.0, current_distance) / initial_distance)
                fraction = max(0.0, min(0.99, fraction))

            now_ns = self.get_clock().now().nanoseconds
            if (
                (now_ns - int(nav_feedback_state['last_publish_ns'])) < int(0.15 * 1e9)
                and abs(fraction - float(nav_feedback_state['last_fraction'])) < 0.02
            ):
                return

            progress_index = 0
            if total > 1:
                progress_index = int(round(fraction * float(total - 1)))
                progress_index = max(0, min(int(total - 1), progress_index))

            feedback_msg = FollowPathAction.Feedback()
            feedback_msg.current_index = int(progress_index)
            feedback_msg.total = int(total)
            status_parts = [f'Nav2 following path ({progress_index + 1}/{total})']
            if math.isfinite(float(nav_feedback_state['distance'])):
                status_parts.append(f'd={float(nav_feedback_state["distance"]):.2f}m')
            if math.isfinite(float(nav_feedback_state['speed'])):
                status_parts.append(f'v={float(nav_feedback_state["speed"]):.2f}m/s')
            feedback_msg.status = ' | '.join(status_parts)
            goal_handle.publish_feedback(feedback_msg)

            nav_feedback_state['last_publish_ns'] = now_ns
            nav_feedback_state['last_fraction'] = fraction

        follow_goal = Nav2FollowPath.Goal()
        follow_goal.path = nav_path
        if hasattr(follow_goal, 'controller_id'):
            follow_goal.controller_id = str(self.follow_path_controller_id or 'FollowPath')
        if hasattr(follow_goal, 'goal_checker_id'):
            follow_goal.goal_checker_id = str(self.follow_path_goal_checker_id or '')

        await self._clear_motion_intent(source='zone_manager:pre_follow_path')

        send_future = self.nav_follow_path_client.send_goal_async(
            follow_goal,
            feedback_callback=_follow_feedback_cb,
        )
        nav_goal_handle = await send_future
        if nav_goal_handle is None or (not nav_goal_handle.accepted):
            result.success = False
            result.completed = 0
            result.message = f'Nav2 rejected FollowPath goal with {total} waypoints'
            goal_handle.abort()
            return result

        self.follow_path_nav_goal = nav_goal_handle
        nav_result_future = nav_goal_handle.get_result_async()
        cancel_sent = False
        estop_abort_requested = False

        while not nav_result_future.done():
            if goal_handle.is_cancel_requested and (not cancel_sent):
                cancel_sent = True
                try:
                    nav_goal_handle.cancel_goal_async()
                except Exception:
                    pass

            if self.estop_active and (not estop_abort_requested):
                estop_abort_requested = True
                try:
                    nav_goal_handle.cancel_goal_async()
                except Exception:
                    pass

            await self._non_blocking_wait(0.05)

        try:
            wrapped = nav_result_future.result()
        except Exception as exc:
            result.success = False
            result.completed = 0
            result.message = f'FollowPath result failed: {exc}'
            goal_handle.abort()
            return result
        finally:
            self.follow_path_nav_goal = None

        status = int(getattr(wrapped, 'status', GoalStatus.STATUS_UNKNOWN))
        if estop_abort_requested:
            result.success = False
            result.completed = 0
            result.message = 'E-STOP active'
            goal_handle.abort()
            return result

        if goal_handle.is_cancel_requested or status == GoalStatus.STATUS_CANCELED:
            result.success = False
            result.completed = 0
            result.message = 'FollowPath canceled'
            goal_handle.canceled()
            return result

        if status == GoalStatus.STATUS_SUCCEEDED:
            feedback.current_index = max(0, total - 1)
            feedback.total = int(total)
            feedback.status = f'Completed {total} waypoints'
            goal_handle.publish_feedback(feedback)
            result.success = True
            result.completed = int(total)
            result.message = f'Completed all {total} waypoints via Nav2 FollowPath'
            goal_handle.succeed()
            return result

        result.success = False
        result.completed = 0
        result.message = (
            f'Nav2 FollowPath failed with status {status} '
            f'({self._goal_status_name(status)})'
        )
        goal_handle.abort()
        return result

    async def execute_go_to_zone(self, goal_handle):
        zone_name = goal_handle.request.name
        target_zone, target_label, resolve_error = self._resolve_go_to_zone_target(zone_name)
        if target_zone is None:
            result = GoToZoneAction.Result()
            result.success = False
            result.message = resolve_error
            goal_handle.abort()
            return result

        if self.estop_active:
            result = GoToZoneAction.Result()
            result.success = False
            result.message = 'E-STOP active'
            goal_handle.abort()
            return result

        target_pose = PoseStamped()
        target_pose.header.frame_id = str(target_zone.get('frame_id', 'map') or 'map')
        target_pose.header.stamp = self.get_clock().now().to_msg()
        target_pose.pose.position.x = float(target_zone['position']['x'])
        target_pose.pose.position.y = float(target_zone['position']['y'])
        target_pose.pose.position.z = float(target_zone['position']['z'])
        target_pose.pose.orientation.x = float(target_zone['orientation']['x'])
        target_pose.pose.orientation.y = float(target_zone['orientation']['y'])
        target_pose.pose.orientation.z = float(target_zone['orientation']['z'])
        target_pose.pose.orientation.w = float(target_zone['orientation']['w'])
        self._ensure_pose_orientation(target_pose.pose)

        if str(zone_name or '').strip() == self.GOAL_POSE_HANDOFF_NAME:
            ok, message = await self._execute_goal_pose_handoff_with_retries(
                target_pose, goal_handle, target_label,
            )
            result = GoToZoneAction.Result()
            result.message = str(message or '').strip()
            if ok is True:
                result.success = True
                goal_handle.succeed()
            elif ok is None:
                result.success = False
                goal_handle.canceled()
            else:
                result.success = False
                goal_handle.abort()
            return result

        self._publish_active_goal_marker(target_label, target_pose)
        try:
            return await self._forward_navigate_to_pose_goal(
                goal_handle,
                target_label=target_label,
                target_pose=target_pose,
            )
        finally:
            self._clear_active_goal_marker()

    def navigate_to_goal_pose_callback(self, _request, response):
        if self.estop_active:
            response.success = False
            response.message = 'Cannot navigate to goal pose while E-STOP is active.'
            return response

        zone_payload, error = self._goal_pose_zone_payload()
        if zone_payload is None:
            response.success = False
            response.message = error
            return response

        if not self.arbitrator_request_client.wait_for_service(timeout_sec=1.5):
            response.success = False
            response.message = 'NavigationArbitrator request service is unavailable.'
            return response

        request_goal = RequestNavigation.Request()
        request_goal.requester = 'zone_manager'
        request_goal.goal_type = 'go_to_zone'
        request_goal.goal_name = self.GOAL_POSE_HANDOFF_NAME

        send_future = self.arbitrator_request_client.call_async(request_goal)
        send_response = self._wait_future_blocking(send_future, 2.0)
        if send_response is None:
            response.success = False
            response.message = 'Timed out queueing goal-pose navigation through NavigationArbitrator.'
            return response

        response.success = bool(send_response.ok)
        response.message = str(send_response.message or '').strip()
        if response.success and not response.message:
            response.message = (
                f'Goal pose queued at '
                f'({zone_payload["position"]["x"]:.2f}, {zone_payload["position"]["y"]:.2f}).'
            )
        if response.success:
            self.get_logger().info(response.message)
        else:
            self.get_logger().warn(response.message)
        return response

    def follow_path_goal_callback(self, goal_request):
        """Validate incoming FollowPath action goals."""
        if self.estop_active:
            self.get_logger().warn('Rejecting follow_path goal: E-STOP active')
            return GoalResponse.REJECT

        if len(goal_request.waypoints) < 2:
            self.get_logger().warn('Rejecting follow_path goal: need at least 2 waypoints')
            return GoalResponse.REJECT

        if self.go_to_next_ros2ws_goal is not None or self.follow_path_nav_goal is not None:
            self.get_logger().warn(
                'Rejecting follow_path goal: ZoneManager only forwards one Nav2 goal at a time; '
                'use NavigationArbitrator for serialization'
            )
            return GoalResponse.REJECT

        return GoalResponse.ACCEPT

    def follow_path_cancel_callback(self, goal_handle):
        del goal_handle
        if self.follow_path_nav_goal is not None:
            self._request_cancel_with_terminal_log(
                self.follow_path_nav_goal,
                'follow_path cancel callback',
            )
        return CancelResponse.ACCEPT

    @staticmethod
    def _ensure_pose_orientation(pose_msg):
        oq = pose_msg.orientation
        if (abs(float(oq.x)) + abs(float(oq.y)) + abs(float(oq.z)) + abs(float(oq.w))) < 1e-6:
            pose_msg.orientation.x = 0.0
            pose_msg.orientation.y = 0.0
            pose_msg.orientation.z = 0.0
            pose_msg.orientation.w = 1.0

    def _robot_pose_in_frame(self, frame_id: str, timeout_sec: float = 0.2):
        transform, _base_frame, _err = self._lookup_robot_transform(
            target_frame=(frame_id or 'map'),
            timeout_sec=timeout_sec,
        )
        if transform is None:
            return None

        pose = PoseStamped()
        pose.header.frame_id = frame_id or 'map'
        pose.header.stamp = self.get_clock().now().to_msg()
        pose.pose.position.x = float(transform.transform.translation.x)
        pose.pose.position.y = float(transform.transform.translation.y)
        pose.pose.position.z = float(transform.transform.translation.z)
        pose.pose.orientation = transform.transform.rotation
        self._ensure_pose_orientation(pose.pose)
        return pose

    def _nearest_waypoint_index(self, waypoints):
        if not waypoints:
            return 0, float('inf')

        frame_id = waypoints[0].header.frame_id or 'map'
        robot_pose = self._robot_pose_in_frame(frame_id, timeout_sec=0.12)
        if robot_pose is None:
            return 0, float('inf')

        rx = float(robot_pose.pose.position.x)
        ry = float(robot_pose.pose.position.y)
        best_idx = 0
        best_dist = float('inf')

        for idx, wp in enumerate(waypoints):
            dx = rx - float(wp.pose.position.x)
            dy = ry - float(wp.pose.position.y)
            dist = math.hypot(dx, dy)
            if dist < best_dist:
                best_dist = dist
                best_idx = idx

        return int(best_idx), float(best_dist)

    def _advance_waypoint_progress(self, waypoints, local_progress_index):
        """Advance progress using projection along the active polyline.

        This is more robust than pure "distance to next anchor" checks when Nav2
        smooths/cuts corners or performs local obstacle-avoidance deviations.
        """
        if not waypoints:
            return -1, float('inf')

        clamped_index = int(min(local_progress_index, len(waypoints) - 1))
        if clamped_index < -1:
            clamped_index = -1
        frame_id = waypoints[0].header.frame_id or 'map'
        robot_pose = self._robot_pose_in_frame(frame_id, timeout_sec=0.12)
        if robot_pose is None:
            return clamped_index, float('inf')

        rx = float(robot_pose.pose.position.x)
        ry = float(robot_pose.pose.position.y)
        tolerance = float(self.path_waypoint_tolerance)
        cumulative = self._waypoint_cumulative_lengths(waypoints)
        if not cumulative:
            return clamped_index, float('inf')

        # Find best projection segment, biased toward expected local progress.
        expected_seg = 0
        if len(waypoints) >= 2:
            expected_seg = max(0, min(max(clamped_index, 0), len(waypoints) - 2))
        segment_progress_bias = max(0.03, 0.35 * tolerance)
        best_score = float('inf')
        best_seg = expected_seg
        best_t = 0.0
        best_seg_len = 0.0

        for seg_idx in range(0, max(0, len(waypoints) - 1)):
            ax = float(waypoints[seg_idx].pose.position.x)
            ay = float(waypoints[seg_idx].pose.position.y)
            bx = float(waypoints[seg_idx + 1].pose.position.x)
            by = float(waypoints[seg_idx + 1].pose.position.y)
            t, lateral_dist, seg_len = self._project_point_onto_segment(rx, ry, ax, ay, bx, by)
            score = lateral_dist + (segment_progress_bias * abs(int(seg_idx) - int(expected_seg)))
            if score < best_score:
                best_score = score
                best_seg = seg_idx
                best_t = t
                best_seg_len = seg_len

        projected_along = float(cumulative[best_seg] + (best_t * best_seg_len))
        if clamped_index >= 0 and clamped_index < len(cumulative):
            projected_along = max(projected_along, float(cumulative[clamped_index]))

        progressed_index = clamped_index
        next_distance = float('inf')
        anchor_slack = max(0.05, tolerance)

        # Preserve "entry gate" semantics for the first anchor.
        if progressed_index < 0 and waypoints:
            first_dx = rx - float(waypoints[0].pose.position.x)
            first_dy = ry - float(waypoints[0].pose.position.y)
            dist_first = math.hypot(first_dx, first_dy)
            if dist_first <= tolerance:
                progressed_index = 0
            elif projected_along >= max(anchor_slack, 0.5 * tolerance):
                # Robot has measurably progressed along the path polyline even if
                # it did not enter the first anchor tolerance disk exactly.
                progressed_index = 0
            else:
                next_distance = float(dist_first)
                return progressed_index, next_distance

        while progressed_index + 1 < len(waypoints):
            next_anchor_along = float(cumulative[progressed_index + 1])
            if projected_along + anchor_slack < next_anchor_along:
                next_distance = max(0.0, next_anchor_along - projected_along)
                break
            progressed_index += 1

        if progressed_index + 1 >= len(waypoints):
            next_distance = 0.0

        return progressed_index, float(next_distance)

    def _build_segment_follow_path(
        self,
        start_pose: PoseStamped,
        end_pose: PoseStamped,
        *,
        segment_spacing: float = None,
        enforce_end_orientation: bool = None,
    ):
        frame_id = end_pose.header.frame_id or start_pose.header.frame_id or 'map'
        stamp = self.get_clock().now().to_msg()

        sx = float(start_pose.pose.position.x)
        sy = float(start_pose.pose.position.y)
        ex = float(end_pose.pose.position.x)
        ey = float(end_pose.pose.position.y)

        dx = ex - sx
        dy = ey - sy
        distance = math.hypot(dx, dy)
        base_spacing = self.path_follow_segment_spacing if segment_spacing is None else segment_spacing
        spacing = max(0.05, float(base_spacing))
        steps = max(1, int(math.ceil(distance / spacing)))
        # Keep heading aligned with segment travel direction to avoid oscillatory
        # steering when robot initial yaw differs from path direction.
        if distance > 1e-6:
            yaw = math.atan2(dy, dx)
        else:
            oq = start_pose.pose.orientation
            if (abs(float(oq.x)) + abs(float(oq.y)) + abs(float(oq.z)) + abs(float(oq.w))) > 1e-6:
                yaw = self._yaw_from_quaternion(float(oq.x), float(oq.y), float(oq.z), float(oq.w))
            else:
                yaw = 0.0
        qz = math.sin(yaw / 2.0)
        qw = math.cos(yaw / 2.0)

        poses = []
        for step in range(steps + 1):
            ratio = float(step) / float(steps)
            pose = PoseStamped()
            pose.header.frame_id = frame_id
            pose.header.stamp = stamp
            pose.pose.position.x = sx + dx * ratio
            pose.pose.position.y = sy + dy * ratio
            pose.pose.position.z = 0.0
            pose.pose.orientation.x = 0.0
            pose.pose.orientation.y = 0.0
            pose.pose.orientation.z = qz
            pose.pose.orientation.w = qw
            poses.append(pose)

        # Keep path geometry aligned with the segment, but when route semantics require
        # a terminal heading, expose that to the controller via the final pose orientation.
        should_enforce_end_orientation = (
            bool(self.path_enforce_waypoint_orientation)
            if enforce_end_orientation is None
            else bool(enforce_end_orientation)
        )
        if should_enforce_end_orientation:
            final_oq = end_pose.pose.orientation
            if (abs(float(final_oq.x)) + abs(float(final_oq.y)) + abs(float(final_oq.z)) + abs(float(final_oq.w))) > 1e-6:
                poses[-1].pose.orientation = final_oq

        nav_path = NavPath()
        nav_path.header.frame_id = frame_id
        nav_path.header.stamp = stamp
        nav_path.poses = poses
        return nav_path


    def _build_alignment_approach_path(
        self,
        start_pose: PoseStamped,
        end_pose: PoseStamped,
        *,
        segment_spacing: float = None,
    ):
        """Build an approach path where ALL poses carry the goal orientation.

        Unlike ``_build_segment_follow_path`` (which stamps intermediate poses
        with the segment travel bearing), this method stamps every pose —
        including intermediate densification points — with ``end_pose``'s
        orientation.  The Nav2 controller therefore tracks the desired heading
        for the entire approach, preventing the overshoot that occurs when the
        travel bearing diverges from the target yaw.

        Used exclusively by ALIGN_AT_POINT when the robot still needs to close
        an XY gap before it can do a pure in-place heading correction.
        """
        frame_id = end_pose.header.frame_id or start_pose.header.frame_id or 'map'
        stamp = self.get_clock().now().to_msg()

        sx = float(start_pose.pose.position.x)
        sy = float(start_pose.pose.position.y)
        ex = float(end_pose.pose.position.x)
        ey = float(end_pose.pose.position.y)

        dx = ex - sx
        dy = ey - sy
        distance = math.hypot(dx, dy)
        base_spacing = self.path_follow_segment_spacing if segment_spacing is None else segment_spacing
        spacing = max(0.05, float(base_spacing))
        steps = max(1, int(math.ceil(distance / spacing)))

        # Goal orientation — applied to EVERY pose, not just the last one.
        goal_oq = end_pose.pose.orientation

        poses = []
        for step in range(steps + 1):
            ratio = float(step) / float(steps)
            pose = PoseStamped()
            pose.header.frame_id = frame_id
            pose.header.stamp = stamp
            pose.pose.position.x = sx + dx * ratio
            pose.pose.position.y = sy + dy * ratio
            pose.pose.position.z = 0.0
            pose.pose.orientation.x = float(goal_oq.x)
            pose.pose.orientation.y = float(goal_oq.y)
            pose.pose.orientation.z = float(goal_oq.z)
            pose.pose.orientation.w = float(goal_oq.w)
            poses.append(pose)

        nav_path = NavPath()
        nav_path.header.frame_id = frame_id
        nav_path.header.stamp = stamp
        nav_path.poses = poses
        return nav_path


    def _build_full_follow_path(self, waypoints):
        """Build a single absolute nav_msgs/Path across all waypoints.

        Uses segment densification only when spacing implies extra points.
        """
        if not waypoints:
            return NavPath()

        base_frame = waypoints[0].header.frame_id or 'map'
        stamped_waypoints = []
        stamp = self.get_clock().now().to_msg()

        for idx, wp in enumerate(waypoints):
            frame_id = wp.header.frame_id or base_frame
            if frame_id != base_frame:
                raise ValueError(
                    f'Waypoint frame mismatch at index {idx + 1}: '
                    f'{frame_id} != {base_frame}'
                )

            pose = PoseStamped()
            pose.header.frame_id = base_frame
            pose.header.stamp = stamp
            pose.pose.position.x = float(wp.pose.position.x)
            pose.pose.position.y = float(wp.pose.position.y)
            pose.pose.position.z = float(wp.pose.position.z)
            pose.pose.orientation.x = 0.0
            pose.pose.orientation.y = 0.0
            pose.pose.orientation.z = 0.0
            pose.pose.orientation.w = 1.0
            stamped_waypoints.append(pose)

        nav_path = NavPath()
        nav_path.header.frame_id = base_frame
        nav_path.header.stamp = stamp

        if len(stamped_waypoints) == 1:
            nav_path.poses = stamped_waypoints
            return nav_path

        full_poses = []
        for idx in range(1, len(stamped_waypoints)):
            seg_idx = idx - 1
            is_terminal_segment = idx >= (len(stamped_waypoints) - 1)
            is_curve_segment = bool(
                self.path_curve_motion_leniency_enabled
                and self._segment_has_curve_transition(stamped_waypoints, seg_idx)
            )
            segment_spacing = (
                self.path_curve_segment_spacing
                if is_curve_segment
                else self.path_follow_segment_spacing
            )
            enforce_end_orientation = bool(self.path_enforce_waypoint_orientation)
            if is_terminal_segment and bool(self.path_require_final_heading):
                enforce_end_orientation = True
            elif (not is_terminal_segment) and bool(self.path_align_intermediate_waypoints):
                enforce_end_orientation = True
            if (
                is_curve_segment
                and self.path_curve_relax_waypoint_orientation
                and (not is_terminal_segment)
            ):
                enforce_end_orientation = False

            seg = self._build_segment_follow_path(
                stamped_waypoints[idx - 1],
                stamped_waypoints[idx],
                segment_spacing=segment_spacing,
                enforce_end_orientation=enforce_end_orientation,
            ).poses
            if idx > 1 and seg:
                seg = seg[1:]
            full_poses.extend(seg)

        if not full_poses:
            full_poses = stamped_waypoints

        nav_path.poses = full_poses
        return nav_path

    # ------------------------------------------------------------------
    # Action POI helpers for path following
    # ------------------------------------------------------------------

    def _lift_status_callback(self, msg: Int32):
        self._lift_status_raw = int(msg.data)

    def _load_action_mappings_for_path(self) -> Dict[str, Dict[str, str]]:
        try:
            payload = self.db_manager.get_ui_mappings()
        except Exception as exc:
            self.get_logger().warn(f'Failed loading UI mappings for path action: {exc}')
            payload = {}
        raw_mappings = payload.get('mappings', {}) if isinstance(payload, dict) else {}
        if not isinstance(raw_mappings, dict):
            raw_mappings = {}
        return merge_action_mappings(raw_mappings)

    def _resolve_action_publisher_for_path(self, topic: str, msg_type: str):
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
            msg_cls = BoolMsg
            normalized_type = 'std_msgs/msg/Bool'
        else:
            return None, None, f'Unsupported action message type "{normalized_type}"'
        key = (normalized_topic, normalized_type)
        publisher = self._action_publishers.get(key)
        if publisher is None:
            publisher = self.create_publisher(msg_cls, normalized_topic, 10)
            self._action_publishers[key] = publisher
        return publisher, msg_cls, ''

    def _publish_zone_action_for_path(self, action_id: str) -> Tuple[bool, str]:
        normalized = normalize_action_id(action_id)
        mappings = self._load_action_mappings_for_path()
        mapping_key = f'{ACTION_MAPPING_PREFIX}{normalized}'
        mapping = mappings.get(mapping_key)
        if not isinstance(mapping, dict):
            return False, f'Action mapping "{mapping_key}" not found'
        topic = str(mapping.get('topic', '')).strip()
        msg_type = str(mapping.get('type', 'std_msgs/msg/Int32')).strip()
        field = str(mapping.get('field', '')).strip()
        publisher, msg_cls, err = self._resolve_action_publisher_for_path(topic, msg_type)
        if publisher is None or msg_cls is None:
            return False, err
        # Parse field payload
        payload: Dict[str, Any] = {}
        raw = str(field or '').strip()
        if raw.startswith('{') or raw.startswith('['):
            try:
                parsed = json.loads(raw)
                payload = parsed if isinstance(parsed, dict) else {'data': parsed}
            except Exception:
                pass
        elif ':' in raw:
            k, v = raw.split(':', 1)
            try:
                payload = {str(k).strip() or 'data': int(float(v.strip()))}
            except Exception:
                payload = {str(k).strip() or 'data': v.strip()}
        elif raw:
            try:
                payload = {'data': int(float(raw))}
            except Exception:
                payload = {'data': raw}
        try:
            if msg_cls is Int32:
                value = payload.get('data', None)
                if value is None and payload:
                    value = next(iter(payload.values()))
                msg = Int32()
                msg.data = int(float(value if value is not None else 0))
                publisher.publish(msg)
            elif msg_cls is BoolMsg:
                value = payload.get('data', None)
                if value is None and payload:
                    value = next(iter(payload.values()))
                msg = BoolMsg()
                msg.data = bool(value if value is not None else False)
                publisher.publish(msg)
            else:
                return False, 'Unsupported message class'
        except Exception as exc:
            return False, f'Failed publishing action "{normalized}": {exc}'
        return True, f'Action "{normalized}" published to {topic}'

    def _resolve_zone_names_from_waypoints(
        self,
        raw_waypoints,
        zone_names_list: list,
    ) -> list:
        """Auto-enrich zone_names_list by proximity-matching raw waypoints against POIs.

        For each waypoint whose zone-name entry is empty, check whether it falls within
        path_waypoint_tolerance metres of a POI that should trigger semantic path-side
        behaviour: heading alignment, stop timer, or an action. When a match is found the
        zone name is filled in automatically.

        This lets paths that were saved as bare coordinates (without explicit zone-name
        metadata) still trigger POI heading/stop-timer/action behaviour when passing through
        a matching POI.
        """
        tolerance = float(getattr(self, 'path_waypoint_tolerance', 0.40))
        try:
            self.load_zones()
            all_zones = dict((self.zones or {}).get('zones', {}))
        except Exception:
            all_zones = {}

        path_zones = {
            name: data
            for name, data in all_zones.items()
            if isinstance(data, dict)
            and (
                isinstance(data.get('orientation', {}), dict)
                and any(key in data.get('orientation', {}) for key in ('x', 'y', 'z', 'w'))
                or self._safe_float(data.get('charge_duration', 0.0), 0.0) > 0.0
                or str(data.get('type', '') or '').strip().lower() == 'action'
                or bool(normalize_action_id(data.get('action', '')))
            )
        }

        if not path_zones:
            return list(zone_names_list)

        n_raw = len(raw_waypoints)
        result: list = list(zone_names_list)
        # Ensure list is at least as long as raw_waypoints.
        while len(result) < n_raw:
            result.append('')

        for idx in range(n_raw):
            if result[idx]:
                # Already has an explicit zone name — honour it.
                continue
            try:
                wx = float(raw_waypoints[idx].pose.position.x)
                wy = float(raw_waypoints[idx].pose.position.y)
            except Exception:
                continue

            best_name = ''
            best_dist = float('inf')
            for name, data in path_zones.items():
                pos = data.get('position', {})
                try:
                    zx = float(pos.get('x', 0.0))
                    zy = float(pos.get('y', 0.0))
                    dist = math.hypot(wx - zx, wy - zy)
                    if dist < best_dist and dist <= tolerance:
                        best_dist = dist
                        best_name = name
                except Exception:
                    continue

            if best_name:
                result[idx] = best_name
                self.get_logger().info(
                    f'FollowPath: auto-resolved zone "{best_name}" '
                    f'at raw waypoint {idx + 1}/{n_raw} '
                    f'(dist {best_dist:.3f}m <= tol {tolerance:.3f}m)'
                )

        return result

    async def _execute_named_path_action(self, action_id: str) -> Tuple[bool, str]:
        """Async action execution during path following. Blocks until complete."""
        normalized = normalize_action_id(action_id)
        if not normalized:
            return False, 'Action id is empty after normalization'
        ok, message = self._publish_zone_action_for_path(normalized)
        if not ok:
            return False, message
        if normalized in ('lift_up', 'lift_down'):
            target_name = 'top' if normalized == 'lift_up' else 'bottom'
            deadline = time.monotonic() + float(self.zone_action_timeout_s)
            poll_period = max(0.02, 1.0 / float(self.zone_action_poll_hz))
            stable_hits = 0
            while time.monotonic() < deadline:
                if self.estop_active:
                    return False, 'E-STOP during lift action'
                packed = int(self._lift_status_raw)
                at_top = bool(packed & 0x01)
                at_bottom = bool(packed & 0x02)
                direction = (packed >> 8) & 0xFF
                if direction >= 0x80:
                    direction -= 0x100
                at_target = at_top if target_name == 'top' else at_bottom
                if at_target and direction == 0:
                    stable_hits += 1
                    if stable_hits >= 2:
                        return True, f'Lift reached {target_name}'
                else:
                    stable_hits = 0
                await self._non_blocking_wait(poll_period)
            return False, f'Timeout waiting for lift {target_name} ({self.zone_action_timeout_s:.1f}s)'
        self.get_logger().info(f'No completion wait for action "{normalized}", continuing')
        return True, message

    async def _execute_path_zone_action(self, zone_name: str) -> Tuple[bool, str]:
        """Async zone action execution during path following. Blocks until complete."""
        # zone_name is the POI name (e.g. "A1"); look up the zone to get its action id.
        self.load_zones()
        zones = self.zones.get('zones', {})
        zone_meta = zones.get(zone_name, {}) if isinstance(zones, dict) else {}
        action_id = str(zone_meta.get('action', '') or '').strip()
        if not action_id:
            return False, f'Zone "{zone_name}" has no action configured'
        return await self._execute_named_path_action(action_id)

    async def _execute_follow_path_segmented(self, goal_handle):
        """Semantic per-waypoint FollowPath: MOVE_TO_POINT → ALIGN_AT_POINT → ZONE_ACTION per segment."""
        raw_waypoints = goal_handle.request.waypoints
        total = len(raw_waypoints)
        result = FollowPathAction.Result()
        zone_names_list = list(getattr(goal_handle.request, 'zone_names', []) or [])
        shelf_checks_list = list(getattr(goal_handle.request, 'shelf_checks', []) or [])
        zone_names_list = self._resolve_zone_names_from_waypoints(raw_waypoints, zone_names_list)
        try:
            self.load_zones()
            zones_for_path = dict((self.zones or {}).get('zones', {}))
        except Exception:
            zones_for_path = {}
        while len(shelf_checks_list) < total:
            shelf_checks_list.append(False)
        actioned_indices: set = set()
        completed_count = 1 if total > 0 else 0
        shelf_flip_flop_up_next = True

        self.get_logger().info(
            f'FollowPath (segmented): {total} waypoints with controller-owned motion'
        )

        if total == 0:
            result.success = False
            result.message = 'No waypoints provided'
            goal_handle.abort()
            return result

        if self.estop_active:
            result.success = False
            result.message = 'E-STOP active'
            goal_handle.abort()
            return result

        # Preempt any active go_to_zone.
        if self.go_to_next_ros2ws_goal is not None:
            self.get_logger().warn(
                'FollowPath requested while go_to_zone is active; preempting go_to_zone'
            )
            cancel_ok, cancel_msg = await self._cancel_nav_goal_with_confirmation(
                self.go_to_next_ros2ws_goal,
                'go_to_zone preemption',
                timeout_sec=1.0,
            )
            if cancel_ok:
                self.go_to_next_ros2ws_goal = None
            else:
                result.success = False
                result.message = f'Cannot start FollowPath: {cancel_msg}'
                goal_handle.abort()
                return result

        if not self.nav_follow_path_client.wait_for_server(timeout_sec=5.0):
            result.success = False
            result.message = 'Nav2 FollowPath action server not available (/follow_path)'
            goal_handle.abort()
            return result
        bt_ready, bt_msg = await self._ensure_bt_navigator_active()
        if not bt_ready:
            result.success = False
            result.message = f'Nav2 FollowPath unavailable: {bt_msg}'
            goal_handle.abort()
            return result

        # Normalize waypoints and ensure consistent frame.
        base_frame = str(raw_waypoints[0].header.frame_id or 'map')
        stamped_waypoints = []
        stamp = self.get_clock().now().to_msg()
        for idx, wp in enumerate(raw_waypoints):
            frame_id = str(wp.header.frame_id or base_frame)
            if frame_id != base_frame:
                result.success = False
                result.completed = int(max(0, completed_count))
                result.message = (
                    f'Waypoint frame mismatch at index {idx + 1}: '
                    f'{frame_id} != {base_frame}'
                )
                goal_handle.abort()
                return result

            pose = PoseStamped()
            pose.header.frame_id = base_frame
            pose.header.stamp = stamp
            pose.pose.position.x = float(wp.pose.position.x)
            pose.pose.position.y = float(wp.pose.position.y)
            pose.pose.position.z = float(wp.pose.position.z)
            pose.pose.orientation.x = 0.0
            pose.pose.orientation.y = 0.0
            pose.pose.orientation.z = 0.0
            pose.pose.orientation.w = 1.0
            stamped_waypoints.append(pose)

        _, tf_base_frame, tf_err = self._lookup_robot_transform(
            target_frame=base_frame,
            timeout_sec=0.30,
        )
        if tf_base_frame is None:
            result.success = False
            result.message = (
                f'Nav2 TF not ready ({base_frame} -> base_link/base_footprint unavailable): {tf_err}'
            )
            goal_handle.abort()
            return result

        try:
            active_viz_path = self._build_full_follow_path(stamped_waypoints)
            self._viz_active_path_pub.publish(active_viz_path)
        except Exception as exc:
            self.get_logger().warn(f'Failed to publish active FollowPath visualization: {exc}')

        feedback = FollowPathAction.Feedback()
        feedback.total = total

        def _clamp_feedback_index(index: int) -> int:
            if total <= 0:
                return 0
            return int(max(0, min(int(index), total - 1)))

        def _publish_feedback(index: int, status_text: str):
            feedback.current_index = _clamp_feedback_index(index)
            feedback.status = str(status_text or '')
            goal_handle.publish_feedback(feedback)

        _publish_feedback(0, f'SEGMENTED: starting at waypoint 1/{total}')

        terminal_error = '__terminal__'
        obstacle_wait_retry = '__obstacle_wait_retry__'

        async def _wait_for_obstacle_hold_clear(wait_index: int, phase_label: str):
            last_feedback_ns = 0
            feedback_period_ns = int(0.20 * 1e9)
            wait_started = time.monotonic()
            obstacle_wait_max_sec = float(self.path_obstacle_wait_max_sec)

            while self.distance_obstacle_hold_active:
                if goal_handle.is_cancel_requested:
                    goal_handle.canceled()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = 'FollowPath canceled during obstacle wait'
                    return terminal_error

                if self.estop_active:
                    goal_handle.abort()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = 'E-STOP active during obstacle wait'
                    return terminal_error
                

                if (time.monotonic() - wait_started) >= obstacle_wait_max_sec:
                    self.get_logger().warn(
                        f'{phase_label}: obstacle hold not cleared after '
                        f'{obstacle_wait_max_sec:.0f}s at waypoint {wait_index + 1}/{total}; '
                        f'aborting path.'
                    )
                    goal_handle.abort()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = (
                        f'Obstacle not cleared within {obstacle_wait_max_sec:.0f}s '
                        f'at waypoint {wait_index + 1}/{total}'
                    )
                    return terminal_error

                now_ns = self.get_clock().now().nanoseconds
                if (now_ns - last_feedback_ns) >= feedback_period_ns:
                    elapsed = time.monotonic() - wait_started
                    _publish_feedback(
                        wait_index,
                        f'{phase_label}: waiting for obstacle to clear at waypoint '
                        f'{wait_index + 1}/{total} ({elapsed:.0f}s)',
                    )
                    last_feedback_ns = now_ns

                await self._non_blocking_wait(0.10)

            return ''

        def _resolve_segment_start_pose(segment_index: int):
            """Use live robot pose when far from the nominal segment anchor."""
            nominal_start = stamped_waypoints[segment_index]
            robot_pose = self._robot_pose_in_frame(base_frame, timeout_sec=0.20)
            if robot_pose is None:
                return nominal_start, 0.0, False

            start_dx = float(robot_pose.pose.position.x) - float(nominal_start.pose.position.x)
            start_dy = float(robot_pose.pose.position.y) - float(nominal_start.pose.position.y)
            start_gap = math.hypot(start_dx, start_dy)
            start_gap_threshold = max(0.03, float(self.path_waypoint_tolerance))
            if start_gap <= start_gap_threshold:
                return nominal_start, start_gap, False

            live_start = PoseStamped()
            live_start.header.frame_id = base_frame
            live_start.header.stamp = self.get_clock().now().to_msg()
            live_start.pose.position.x = float(robot_pose.pose.position.x)
            live_start.pose.position.y = float(robot_pose.pose.position.y)
            live_start.pose.position.z = float(robot_pose.pose.position.z)
            live_start.pose.orientation = robot_pose.pose.orientation
            self._ensure_pose_orientation(live_start.pose)
            return live_start, start_gap, True

        def _zone_name_at(index: int) -> str:
            if index < 0 or index >= len(zone_names_list):
                return ''
            return str(zone_names_list[index] or '').strip()

        def _zone_meta_at(index: int) -> Dict[str, Any]:
            zone_name = _zone_name_at(index)
            if not zone_name:
                return {}
            zone_meta = zones_for_path.get(zone_name, {})
            return zone_meta if isinstance(zone_meta, dict) else {}

        def _zone_stop_timer_at(index: int) -> Tuple[float, str]:
            zone_name = _zone_name_at(index)
            zone_meta = _zone_meta_at(index)
            stop_timer_s = self._safe_float(zone_meta.get('charge_duration', 0.0), 0.0)
            return max(0.0, float(stop_timer_s)), zone_name

        def _shelf_check_enabled_at(index: int) -> bool:
            if index < 0 or index >= len(shelf_checks_list):
                return False
            return bool(shelf_checks_list[index])

        async def _validate_follow_move_arrival(
            waypoint_index: int,
            phase_label: str,
            *,
            target_pose: PoseStamped,
            is_terminal_segment: bool,
        ) -> str:
            settle_sec = max(0.0, float(getattr(self, 'path_arrival_settle_sec', 0.30)))
            if settle_sec > 0.0:
                await self._non_blocking_wait(settle_sec)

            if goal_handle.is_cancel_requested:
                goal_handle.canceled()
                result.success = False
                result.completed = int(max(0, completed_count))
                result.message = 'FollowPath canceled during arrival validation'
                return terminal_error

            if self.estop_active:
                goal_handle.abort()
                result.success = False
                result.completed = int(max(0, completed_count))
                result.message = 'E-STOP active during arrival validation'
                return terminal_error

            strict_pos_tol = float(
                self.path_final_tolerance
                if is_terminal_segment
                else self.path_waypoint_tolerance
            )
            max_nav2_success_mismatch = max(
                strict_pos_tol,
                float(getattr(self, 'path_nav2_success_max_mismatch_distance', strict_pos_tol)),
            )
            center_dist = self._distance_to_goal(target_pose)

            if not math.isfinite(center_dist):
                self.get_logger().warn(
                    f'Nav2 reported success for phase "{phase_label}" at waypoint '
                    f'{waypoint_index + 1}/{total}, but robot center could not be validated'
                )
                return (
                    f'Phase "{phase_label}" finished but robot center could not be validated '
                    f'at waypoint {waypoint_index + 1}/{total}'
                )

            if center_dist <= strict_pos_tol:
                return ''

            tol_label = 'final' if is_terminal_segment else 'waypoint'
            if center_dist > max_nav2_success_mismatch:
                self.get_logger().warn(
                    f'Nav2 reported success for phase "{phase_label}" at waypoint '
                    f'{waypoint_index + 1}/{total}, but robot center mismatch was '
                    f'{center_dist:.3f}m > max allowed {max_nav2_success_mismatch:.3f}m '
                    f'(strict {tol_label} tol {strict_pos_tol:.3f}m)'
                )
                return (
                    f'Phase "{phase_label}" finished but robot center mismatch '
                    f'{center_dist:.2f}m exceeds max allowed '
                    f'{max_nav2_success_mismatch:.2f}m at waypoint '
                    f'{waypoint_index + 1}/{total} (strict {tol_label} tol '
                    f'{strict_pos_tol:.2f}m)'
                )

            self.get_logger().warn(
                f'Nav2 reported success for phase "{phase_label}" at waypoint '
                f'{waypoint_index + 1}/{total}, but robot center is still '
                f'{center_dist:.3f}m from target (strict {tol_label} tol '
                f'{strict_pos_tol:.3f}m); retrying'
            )
            return (
                f'Phase "{phase_label}" finished with robot center still '
                f'{center_dist:.2f}m from target at waypoint '
                f'{waypoint_index + 1}/{total} (strict {tol_label} tol '
                f'{strict_pos_tol:.2f}m)'
            )

        def _semantic_align_target_at(index: int) -> Tuple[Optional[PoseStamped], float, str]:
            zone_name = _zone_name_at(index)
            zone_meta = _zone_meta_at(index)
            orientation = zone_meta.get('orientation', {})
            if (
                not zone_name
                or (not isinstance(orientation, dict))
                or (not any(key in orientation for key in ('x', 'y', 'z', 'w')))
            ):
                return None, 0.0, ''

            align_pose = PoseStamped()
            align_pose.header.frame_id = base_frame
            align_pose.header.stamp = self.get_clock().now().to_msg()
            align_pose.pose.position.x = float(stamped_waypoints[index].pose.position.x)
            align_pose.pose.position.y = float(stamped_waypoints[index].pose.position.y)
            align_pose.pose.position.z = float(stamped_waypoints[index].pose.position.z)
            align_pose.pose.orientation.x = self._safe_float(orientation.get('x', 0.0))
            align_pose.pose.orientation.y = self._safe_float(orientation.get('y', 0.0))
            align_pose.pose.orientation.z = self._safe_float(orientation.get('z', 0.0))
            align_pose.pose.orientation.w = self._safe_float(orientation.get('w', 1.0), 1.0)
            self._ensure_pose_orientation(align_pose.pose)
            return align_pose, float(self.zone_heading_tolerance), zone_name

        async def _heading_stable_within_tol(align_pose_arg: PoseStamped, heading_tol_arg: float) -> tuple:
            """Return (stable_ok: bool, last_err: float).

            Takes `path_align_stable_samples` heading readings spaced
            `path_align_stable_interval_sec` apart.  Only returns True when:
              1. Every sample is within heading_tol_arg, AND
              2. The spread (max - min) is less than heading_tol_arg * 0.5,
                 confirming the robot is stationary, not spinning through the target.
            This prevents a single transient mid-spin reading from falsely
            clearing the alignment check.
            """
            n = max(1, int(getattr(self, 'path_align_stable_samples', 3)))
            interval = max(0.02, float(getattr(self, 'path_align_stable_interval_sec', 0.10)))
            samples = []
            for i in range(n):
                err = self._heading_error_to_goal(align_pose_arg)
                if not math.isfinite(err):
                    return False, float('inf')
                samples.append(err)
                if i < n - 1:
                    await self._non_blocking_wait(interval)
            last_err = samples[-1]
            # All samples must be within tolerance
            if any(s > heading_tol_arg for s in samples):
                return False, last_err
            # Spread check: if heading is still drifting, the robot is still rotating
            spread = max(samples) - min(samples)
            if spread > heading_tol_arg * 0.5:
                return False, last_err
            return True, last_err

        async def _run_align_spin_phase(
            waypoint_index: int,
            align_pose_arg: PoseStamped,
            heading_tol_arg: float,
            poi_name_arg: str,
        ) -> str:
            align_timeout = max(
                1.0,
                float(getattr(self, 'path_align_spin_time_allowance_sec', 12.0)),
            )
            heading_delta = self._heading_delta_to_goal(align_pose_arg)
            if not math.isfinite(heading_delta):
                return (
                    f'ALIGN_AT_POINT could not compute heading delta for waypoint '
                    f'{waypoint_index + 1}/{total}'
                )
            if abs(heading_delta) <= heading_tol_arg:
                return ''

            robot_pose = self._robot_pose_in_frame(base_frame, timeout_sec=0.20)
            if robot_pose is None:
                return (
                    f'ALIGN_AT_POINT lost robot pose before controller align at waypoint '
                    f'{waypoint_index + 1}/{total}'
                )

            intent_ok, intent_msg = await self._set_motion_intent(
                'alignment',
                align_pose_arg,
                # Heading cleanup should not leave the controller stuck in a
                # crawl-speed envelope. Use the intent only for mode/tolerance,
                # not as a translational speed cap.
                max_linear_speed=0.0,
                # Do not reuse the slow shelf-insertion yaw cap here.
                # ALIGN_AT_POINT may need a near in-place 90-180deg correction,
                # and clamping angular speed too low causes Nav2 progress aborts
                # before the heading change completes.
                max_angular_speed=0.0,
                xy_tolerance=max(0.05, float(getattr(self, 'path_waypoint_tolerance', 0.40))),
                yaw_tolerance=float(heading_tol_arg),
                timeout_sec=float(align_timeout),
                source='zone_manager:path_align',
            )
            if not intent_ok:
                return (
                    f'ALIGN_AT_POINT could not set controller align intent at waypoint '
                    f'{waypoint_index + 1}/{total}: {intent_msg}'
                )

            last_feedback_ns = 0

            def _align_feedback(remaining: float, speed: float) -> None:
                nonlocal last_feedback_ns
                now_ns = self.get_clock().now().nanoseconds
                if (now_ns - last_feedback_ns) < int(0.15 * 1e9):
                    return
                status = (
                    f'ALIGN_AT_POINT: aligning POI "{poi_name_arg}" | '
                    f'heading_rem={max(0.0, abs(self._heading_delta_to_goal(align_pose_arg))):.2f}rad'
                )
                if math.isfinite(remaining):
                    status += f', path_rem={remaining:.2f}m'
                if math.isfinite(speed):
                    status += f', v={speed:.2f}m/s'
                _publish_feedback(waypoint_index, status)
                last_feedback_ns = now_ns

            try:
                # Send a SINGLE FollowPath goal and let the controller's
                # rotate-to-heading state machine handle the full rotation
                # uninterrupted.  The controller bypasses its own progress-stall
                # detection during rotate-to-heading (intentional_rotation_rpp)
                # and bypasses the Nav2 goal_checker in alignment mode.
                #
                # However, Nav2's server-side SimpleProgressChecker will fire
                # STATUS_ABORTED after movement_time_allowance (8 s) because the
                # robot's XY position doesn't change during in-place rotation.
                # We handle that gracefully: after any abort, re-check heading
                # and retry if the error has improved or is within tolerance.
                align_deadline = time.monotonic() + align_timeout
                last_err = abs(heading_delta)
                max_single_goal_sec = 7.5  # just under the 8 s progress checker

                while time.monotonic() < align_deadline:
                    # ── Check if already aligned ──
                    stable_ok, last_err = await _heading_stable_within_tol(
                        align_pose_arg, heading_tol_arg)
                    if stable_ok:
                        return ''

                    if goal_handle.is_cancel_requested:
                        goal_handle.canceled()
                        result.success = False
                        result.completed = int(max(0, completed_count))
                        result.message = 'FollowPath canceled during ALIGN_AT_POINT align'
                        return terminal_error
                    if self.estop_active:
                        goal_handle.abort()
                        result.success = False
                        result.completed = int(max(0, completed_count))
                        result.message = 'E-STOP active during ALIGN_AT_POINT align'
                        return terminal_error

                    # ── Build path ──
                    robot_pose = self._robot_pose_in_frame(base_frame, timeout_sec=0.20)
                    if robot_pose is None:
                        return (
                            f'ALIGN_AT_POINT lost robot pose during controller align at waypoint '
                            f'{waypoint_index + 1}/{total}'
                        )

                    # Degenerate path — two copies of the goal pose.
                    # The controller creates a synthetic 1 cm fallback segment in
                    # the goal yaw direction, immediately enters arrived_xy, then
                    # rotate-to-heading handles the rest.
                    nav_path = NavPath()
                    nav_path.header.frame_id = align_pose_arg.header.frame_id
                    nav_path.header.stamp = self.get_clock().now().to_msg()
                    nav_path.poses = [
                        self._copy_pose_stamped(align_pose_arg),
                        self._copy_pose_stamped(align_pose_arg),
                    ]

                    remaining = max(0.0, align_deadline - time.monotonic())
                    if remaining <= 0.0:
                        break
                    goal_runtime = min(max_single_goal_sec, remaining)

                    align_ok, align_msg = await self._run_nav_follow_path_phase(
                        nav_path,
                        goal_handle,
                        f'ALIGN_AT_POINT "{poi_name_arg}"',
                        on_feedback=_align_feedback,
                        max_runtime_sec=goal_runtime,
                        allow_partial_completion_on_timeout=True,
                    )

                    if not align_ok:
                        if goal_handle.is_cancel_requested:
                            goal_handle.canceled()
                            result.success = False
                            result.completed = int(max(0, completed_count))
                            result.message = 'FollowPath canceled during ALIGN_AT_POINT align'
                            return terminal_error
                        if self.estop_active:
                            goal_handle.abort()
                            result.success = False
                            result.completed = int(max(0, completed_count))
                            result.message = 'E-STOP active during ALIGN_AT_POINT align'
                            return terminal_error
                        if 'obstacle hold active' in align_msg:
                            obstacle_wait_started = time.monotonic()
                            wait_err = await _wait_for_obstacle_hold_clear(
                                waypoint_index, f'ALIGN_AT_POINT "{poi_name_arg}"'
                            )
                            if wait_err:
                                return wait_err
                            align_deadline += time.monotonic() - obstacle_wait_started
                            continue
                        # Nav2 progress checker abort (status 6) or other
                        # non-fatal failure — check heading and retry if improved.
                        current_err = self._heading_error_to_goal(align_pose_arg)
                        if math.isfinite(current_err) and current_err <= heading_tol_arg:
                            # Actually converged despite the abort status.
                            self.get_logger().info(
                                f'ALIGN_AT_POINT: Nav2 reported failure ({align_msg}) but '
                                f'heading err {current_err:.2f}rad is within tol '
                                f'{heading_tol_arg:.2f}rad — accepting'
                            )
                            # Verify with stable gate
                            stable_ok2, _ = await _heading_stable_within_tol(
                                align_pose_arg, heading_tol_arg)
                            if stable_ok2:
                                return ''
                        if math.isfinite(current_err):
                            last_err = current_err
                        self.get_logger().warn(
                            f'ALIGN_AT_POINT: Nav2 reported failure ({align_msg}), '
                            f'heading err {last_err:.2f}rad — retrying'
                        )
                        await self._non_blocking_wait(0.15)
                        continue

                    # Goal completed (controller declared success) — verify heading
                    # is actually stable before accepting.
                    stable_final, final_err = await _heading_stable_within_tol(
                        align_pose_arg, heading_tol_arg)
                    if math.isfinite(final_err):
                        last_err = final_err
                    if stable_final:
                        return ''
                    # Controller thought it was done but heading is unstable —
                    # loop around for another goal.
                    self.get_logger().info(
                        f'ALIGN_AT_POINT: Nav2 succeeded but heading unstable '
                        f'(err {last_err:.2f}rad) — issuing another goal'
                    )

                return (
                    f'ALIGN_AT_POINT could not converge within {align_timeout:.1f}s at waypoint '
                    f'{waypoint_index + 1}/{total}: heading err {last_err:.2f}rad'
                )
            finally:
                await self._clear_motion_intent(source='zone_manager:path_align')

        async def _run_semantic_align_phase_with_retries(waypoint_index: int):
            align_pose, heading_tol, poi_name = _semantic_align_target_at(waypoint_index)
            if align_pose is None:
                return ''

            settle_sec = max(0.0, float(getattr(self, 'path_arrival_settle_sec', 0.30)))

            # ── Settle: wait for the robot to physically stop ───────────────────────
            # Nav2 declares STATUS_SUCCEEDED while the robot is still decelerating.
            # Evaluating heading while the robot is coasting produces transient TF
            # readings that randomly pass or fail the tolerance check, causing:
            #   - alignment to be falsely skipped → action/stop fires at wrong heading
            #   - align to run but heading_after check reads a moving pose → retry storm
            if settle_sec > 0.0:
                await self._non_blocking_wait(settle_sec)

            # Check cancel/estop after settle (robot could have been stopped)
            if goal_handle.is_cancel_requested:
                goal_handle.canceled()
                result.success = False
                result.completed = int(max(0, completed_count))
                result.message = 'FollowPath canceled during align settle'
                return terminal_error
            if self.estop_active:
                goal_handle.abort()
                result.success = False
                result.completed = int(max(0, completed_count))
                result.message = 'E-STOP active during align settle'
                return terminal_error

            # ── Position precheck: re-approach if robot overshot or arrived off-axis ─
            # MOVE_TO_POINT has xy_goal_tolerance but does not enforce final heading.
            # The robot may stop slightly off the waypoint XY.  If it is too far,
            # a re-approach is needed before we attempt heading alignment.
            pos_tol = max(float(getattr(self, 'path_waypoint_tolerance', 0.40)), 0.08)
            robot_pose_pre = self._robot_pose_in_frame(base_frame, timeout_sec=0.20)
            if robot_pose_pre is not None:
                pdx = (float(robot_pose_pre.pose.position.x)
                       - float(stamped_waypoints[waypoint_index].pose.position.x))
                pdy = (float(robot_pose_pre.pose.position.y)
                       - float(stamped_waypoints[waypoint_index].pose.position.y))
                pos_dist = math.hypot(pdx, pdy)
                if pos_dist > pos_tol:
                    self.get_logger().warn(
                        f'ALIGN_AT_POINT precheck: robot {pos_dist:.2f}m from waypoint '
                        f'{waypoint_index + 1}/{total} (tol {pos_tol:.2f}m) → re-approach'
                    )
                    _wrapped, phase_err = await _run_follow_segment_phase_with_retries(
                        max(0, waypoint_index - 1),
                        waypoint_index,
                        'MOVE_TO_POINT',
                        max(0, waypoint_index - 1),
                        enforce_end_orientation_override=False,
                    )
                    if phase_err:
                        return phase_err
                    # Settle again after the corrective approach
                    if settle_sec > 0.0:
                        await self._non_blocking_wait(settle_sec)

            max_attempts = max(1, int(self.path_max_retries))
            last_heading_err = float('inf')

            for attempt in range(1, max_attempts + 1):
                # ── Pre-check: stable heading gate (not a transient pass-through) ──
                # A single snapshot can read within tolerance while the robot is
                # still spinning through the target heading.  The stable gate takes
                # N spaced readings and requires all of them within tolerance AND
                # the spread to be small (robot must be stationary at the heading).
                stable_before, heading_before = await _heading_stable_within_tol(
                    align_pose, heading_tol
                )
                if math.isfinite(heading_before):
                    last_heading_err = float(heading_before)
                if stable_before:
                    _publish_feedback(
                        waypoint_index,
                        f'ALIGN_AT_POINT: POI "{poi_name}" stably aligned '
                        f'(err {heading_before:.2f}rad <= {heading_tol:.2f}rad, '
                        f'stable over {getattr(self, "path_align_stable_samples", 3)} samples)',
                    )
                    return ''
                if math.isfinite(heading_before):
                    _publish_feedback(
                        waypoint_index,
                        f'ALIGN_AT_POINT: POI "{poi_name}" heading err '
                        f'{heading_before:.2f}rad > {heading_tol:.2f}rad (or unstable)',
                    )
                else:
                    _publish_feedback(
                        waypoint_index,
                        f'ALIGN_AT_POINT: POI "{poi_name}" heading check unavailable, '
                        'running controller-owned align',
                    )

                heading_delta = self._heading_delta_to_goal(align_pose)
                if math.isfinite(heading_delta) and abs(heading_delta) <= heading_tol:
                    if settle_sec > 0.0:
                        await self._non_blocking_wait(settle_sec * 0.5)
                else:
                    phase_err = await _run_align_spin_phase(
                        waypoint_index,
                        align_pose,
                        heading_tol,
                        poi_name,
                    )
                    if phase_err:
                        # A convergence timeout is NOT fatal if we have retries
                        # remaining — the overshoot detection + path fix may
                        # have gotten closer.  Only propagate hard failures
                        # (cancel, estop, lost pose, obstacle timeout).
                        is_convergence_timeout = 'could not converge' in phase_err
                        if is_convergence_timeout and attempt < max_attempts:
                            self.get_logger().warn(
                                f'ALIGN_AT_POINT spin timeout on attempt '
                                f'{attempt}/{max_attempts}: {phase_err} -> retrying'
                            )
                            await self._non_blocking_wait(0.30)
                            continue
                        return phase_err

                # ── Post-align settle: wait for rotation to physically finish ──
                if settle_sec > 0.0:
                    await self._non_blocking_wait(settle_sec * 0.8)

                # ── Post-align stable gate ──
                # Same multi-sample check: only accept if the robot is stationary
                # at the final heading, not still spinning through it.
                stable_after, heading_after = await _heading_stable_within_tol(
                    align_pose, heading_tol
                )
                if not math.isfinite(heading_after):
                    self.get_logger().warn(
                        f'ALIGN_AT_POINT completed for POI "{poi_name}" but heading '
                        'could not be validated; accepting Nav2 success'
                    )
                    return ''

                last_heading_err = float(heading_after)
                if stable_after:
                    return ''

                if attempt < max_attempts:
                    self.get_logger().warn(
                        f'ALIGN_AT_POINT attempt {attempt}/{max_attempts} heading '
                        f'unstable or outside tolerance at waypoint '
                        f'{waypoint_index + 1}/{total}: '
                        f'err {heading_after:.2f}rad (tol {heading_tol:.2f}rad) -> retrying'
                    )
                    await self._non_blocking_wait(0.50)

            return (
                f'ALIGN_AT_POINT failed after {max_attempts} attempts at waypoint '
                f'{waypoint_index + 1}/{total}: heading err {last_heading_err:.2f}rad '
                f'exceeds tolerance {heading_tol:.2f}rad'
            )

        async def _run_semantic_stop_timer_phase(waypoint_index: int):
            stop_timer_s, poi_name = _zone_stop_timer_at(waypoint_index)
            if stop_timer_s <= 0.0 or not poi_name:
                return ''

            # Ensure we are actually at the POI before starting the stop timer.
            pos_tol = float(getattr(self, 'path_waypoint_tolerance', 0.40))
            heading_tol = float(getattr(self, 'zone_heading_tolerance', 0.20))
            max_attempts = max(1, int(self.path_max_retries))

            for attempt in range(1, max_attempts + 1):
                align_pose, _, _ = _semantic_align_target_at(waypoint_index)
                robot_pose = self._robot_pose_in_frame(base_frame, timeout_sec=0.20)
                dist_err = float('inf')
                heading_err = float('inf')

                if robot_pose is not None:
                    dx = float(robot_pose.pose.position.x) - float(stamped_waypoints[waypoint_index].pose.position.x)
                    dy = float(robot_pose.pose.position.y) - float(stamped_waypoints[waypoint_index].pose.position.y)
                    dist_err = math.hypot(dx, dy)

                if align_pose is not None:
                    heading_err = float(self._heading_error_to_goal(align_pose))

                if math.isfinite(dist_err) and dist_err > pos_tol:
                    self.get_logger().warn(
                        f'STOP_TIMER precheck: distance {dist_err:.2f}m > {pos_tol:.2f}m '
                        f'at waypoint {waypoint_index + 1}/{total} -> retry move/align'
                    )
                    _wrapped, phase_err = await _run_follow_segment_phase_with_retries(
                        max(0, waypoint_index - 1),
                        waypoint_index,
                        'MOVE_TO_POINT',
                        max(0, waypoint_index - 1),
                        enforce_end_orientation_override=False,
                    )
                    if phase_err:
                        return phase_err

                    phase_err = await _run_semantic_align_phase_with_retries(waypoint_index)
                    if phase_err:
                        return phase_err
                    continue

                if align_pose is not None and math.isfinite(heading_err) and heading_err > heading_tol:
                    self.get_logger().warn(
                        f'STOP_TIMER precheck: heading err {heading_err:.2f}rad > {heading_tol:.2f}rad '
                        f'at waypoint {waypoint_index + 1}/{total} -> retry align'
                    )
                    phase_err = await _run_semantic_align_phase_with_retries(waypoint_index)
                    if phase_err:
                        return phase_err
                    continue

                break

            _publish_feedback(
                waypoint_index,
                f'STOP_TIMER: "{poi_name}" waiting {stop_timer_s:.1f}s '
                f'at waypoint {waypoint_index + 1}/{total}',
            )

            wait_deadline = time.monotonic() + float(stop_timer_s)
            last_wait_feedback_ns = 0
            wait_feedback_period_ns = int(0.2 * 1e9)

            while time.monotonic() < wait_deadline:
                if goal_handle.is_cancel_requested:
                    goal_handle.canceled()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = (
                        f'FollowPath canceled during stop timer at "{poi_name}"'
                    )
                    return terminal_error

                if self.estop_active:
                    goal_handle.abort()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = (
                        f'E-STOP active during stop timer at "{poi_name}"'
                    )
                    return terminal_error

                now_ns = self.get_clock().now().nanoseconds
                if (now_ns - last_wait_feedback_ns) >= wait_feedback_period_ns:
                    remaining_s = max(0.0, wait_deadline - time.monotonic())
                    _publish_feedback(
                        waypoint_index,
                        f'STOP_TIMER: "{poi_name}" {remaining_s:.1f}s remaining '
                        f'at waypoint {waypoint_index + 1}/{total}',
                    )
                    last_wait_feedback_ns = now_ns

                remaining = max(0.0, wait_deadline - time.monotonic())
                await self._non_blocking_wait(min(0.10, remaining))

            self.get_logger().info(
                f'FollowPath stop timer complete: "{poi_name}" ({stop_timer_s:.1f}s)'
            )
            return ''

        async def _run_optional_shelf_check_phase(waypoint_index: int):
            nonlocal shelf_flip_flop_up_next

            if not _shelf_check_enabled_at(waypoint_index):
                return ''

            target_label = _zone_name_at(waypoint_index) or f'waypoint {waypoint_index + 1}'
            detector_service_name = str(self.shelf_detector_enable_service or '/shelf/set_enabled')
            enable_receipt_before = float(self.latest_shelf_status_receipt_monotonic or 0.0)

            _publish_feedback(
                waypoint_index,
                f'SHELF_CHECK: enabling detector at waypoint {waypoint_index + 1}/{total}',
            )

            enable_ok, enable_msg = await self._call_setbool_service(
                self.shelf_enable_client,
                True,
                detector_service_name,
                timeout_sec=1.5,
            )
            if not enable_ok:
                return (
                    f'SHELF_CHECK failed to enable detector at waypoint '
                    f'{waypoint_index + 1}/{total}: {enable_msg}'
                )

            try:
                settle_sec = max(0.0, float(self.follow_path_shelf_check_enable_settle_sec))
                if settle_sec > 1e-6:
                    await self._non_blocking_wait(settle_sec)

                wait_deadline = time.monotonic() + float(self.follow_path_shelf_check_timeout_sec)
                poll_period = max(0.05, float(self.follow_path_shelf_check_poll_sec))
                last_feedback_ns = 0
                candidate_ready = False

                while time.monotonic() < wait_deadline:
                    if goal_handle.is_cancel_requested:
                        goal_handle.canceled()
                        result.success = False
                        result.completed = int(max(0, completed_count))
                        result.message = 'FollowPath canceled during shelf check'
                        return terminal_error

                    if self.estop_active:
                        goal_handle.abort()
                        result.success = False
                        result.completed = int(max(0, completed_count))
                        result.message = 'E-STOP active during shelf check'
                        return terminal_error

                    latest_receipt = float(self.latest_shelf_status_receipt_monotonic or 0.0)
                    if latest_receipt > enable_receipt_before and self._shelf_candidate_ready():
                        candidate_ready = True
                        break

                    now_ns = self.get_clock().now().nanoseconds
                    if (now_ns - last_feedback_ns) >= int(0.20 * 1e9):
                        last_reason = 'waiting_for_shelf'
                        if isinstance(self.latest_shelf_status, dict) and self.latest_shelf_status:
                            last_reason = str(self.latest_shelf_status.get('last_reason') or last_reason)
                        _publish_feedback(
                            waypoint_index,
                            f'SHELF_CHECK: scanning at waypoint {waypoint_index + 1}/{total} '
                            f'({last_reason})',
                        )
                        last_feedback_ns = now_ns

                    await self._non_blocking_wait(poll_period)

                if not candidate_ready:
                    self.get_logger().info(
                        f'FollowPath shelf check: no shelf candidate at waypoint '
                        f'{waypoint_index + 1}/{total}; continuing'
                    )
                    _publish_feedback(
                        waypoint_index,
                        f'SHELF_CHECK: no shelf detected at waypoint {waypoint_index + 1}/{total}; continuing',
                    )
                    return ''

                goal_pose, goal_pose_err = self._build_shelf_goal_pose_from_status(base_frame)
                if goal_pose is None:
                    return (
                        f'SHELF_CHECK failed to resolve shelf goal at waypoint '
                        f'{waypoint_index + 1}/{total}: {goal_pose_err}'
                    )

                _publish_feedback(
                    waypoint_index,
                    f'SHELF_CHECK: docking through controller at waypoint '
                    f'{waypoint_index + 1}/{total}',
                )
                dock_ok, dock_msg = await self._execute_goal_pose_handoff_with_retries(
                    goal_pose,
                    goal_handle,
                    target_label,
                )
                if dock_ok is None:
                    goal_handle.canceled()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = 'FollowPath canceled during shelf insertion'
                    return terminal_error
                if not dock_ok:
                    return (
                        f'SHELF_CHECK docking failed at waypoint '
                        f'{waypoint_index + 1}/{total}: {dock_msg}'
                    )

                lift_action_id = 'lift_up' if shelf_flip_flop_up_next else 'lift_down'
                _publish_feedback(
                    waypoint_index,
                    f'SHELF_CHECK: running {lift_action_id} after shelf dock at '
                    f'waypoint {waypoint_index + 1}/{total}',
                )
                lift_ok, lift_msg = await self._execute_named_path_action(lift_action_id)
                if not lift_ok:
                    return (
                        f'SHELF_CHECK lift action "{lift_action_id}" failed at waypoint '
                        f'{waypoint_index + 1}/{total}: {lift_msg}'
                    )

                shelf_flip_flop_up_next = not shelf_flip_flop_up_next
                self.get_logger().info(
                    f'FollowPath shelf check complete at waypoint {waypoint_index + 1}/{total}: '
                    f'dock="{dock_msg}" lift="{lift_msg}" next_action='
                    f'{"lift_up" if shelf_flip_flop_up_next else "lift_down"}'
                )
                return ''
            finally:
                disable_ok, disable_msg = await self._call_setbool_service(
                    self.shelf_enable_client,
                    False,
                    detector_service_name,
                    timeout_sec=1.5,
                )
                if not disable_ok:
                    self.get_logger().warn(
                        f'Failed disabling shelf detector after waypoint '
                        f'{waypoint_index + 1}/{total}: {disable_msg}'
                    )

        async def _run_follow_segment_phase(
            segment_index: int,
            waypoint_index: int,
            phase_label: str,
            progress_index: int,
            *,
            end_pose_override: Optional[PoseStamped] = None,
            enforce_end_orientation_override: Optional[bool] = None,
        ):
            self._nav_follow_feedback_distance = float('inf')
            self._nav_follow_feedback_speed = float('nan')
            nav_goal_handle = None

            if self.distance_obstacle_hold_active:
                wait_err = await _wait_for_obstacle_hold_clear(progress_index, phase_label)
                if wait_err:
                    return None, wait_err
                return None, obstacle_wait_retry

            is_terminal_segment = waypoint_index >= (total - 1)
            is_curve_segment = bool(
                self.path_curve_motion_leniency_enabled
                and self._segment_has_curve_transition(stamped_waypoints, segment_index)
            )
            segment_spacing = (
                self.path_curve_segment_spacing
                if is_curve_segment
                else self.path_follow_segment_spacing
            )
            if enforce_end_orientation_override is None:
                enforce_end_orientation = bool(self.path_enforce_waypoint_orientation)
                if is_terminal_segment and bool(self.path_require_final_heading):
                    enforce_end_orientation = True
                elif (not is_terminal_segment) and bool(self.path_align_intermediate_waypoints):
                    enforce_end_orientation = True
            else:
                enforce_end_orientation = bool(enforce_end_orientation_override)
            if (
                is_curve_segment
                and self.path_curve_relax_waypoint_orientation
                and (not is_terminal_segment)
            ):
                enforce_end_orientation = False

            segment_start_pose, start_gap, used_live_start = _resolve_segment_start_pose(segment_index)
            target_pose = end_pose_override if end_pose_override is not None else stamped_waypoints[waypoint_index]
            if used_live_start:
                self.get_logger().warn(
                    f'FollowPath segment {segment_index + 1}/{max(1, total - 1)} using live start pose '
                    f'(gap {start_gap:.2f}m from anchor) to avoid heading bias'
                )
            segment_path = self._build_segment_follow_path(
                segment_start_pose,
                target_pose,
                segment_spacing=segment_spacing,
                enforce_end_orientation=enforce_end_orientation,
            )

            def _follow_feedback(feedback_msg):
                try:
                    nav_fb = getattr(feedback_msg, 'feedback', feedback_msg)
                    dist = getattr(nav_fb, 'distance_to_goal', None)
                    if dist is None:
                        dist = getattr(nav_fb, 'distance_remaining', None)
                    if dist is not None:
                        self._nav_follow_feedback_distance = float(dist)
                    speed = getattr(nav_fb, 'speed', None)
                    if speed is not None:
                        self._nav_follow_feedback_speed = float(speed)
                except Exception:
                    pass

            nav_accept_attempts = max(1, int(self.nav_goal_accept_attempts))
            for accept_attempt in range(1, nav_accept_attempts + 1):
                follow_goal = Nav2FollowPath.Goal()
                follow_goal.path = segment_path
                if hasattr(follow_goal, 'controller_id'):
                    follow_goal.controller_id = str(self.follow_path_controller_id or 'FollowPath')
                if hasattr(follow_goal, 'goal_checker_id'):
                    follow_goal.goal_checker_id = str(self.follow_path_goal_checker_id or '')

                await self._clear_motion_intent(
                    source=f'zone_manager:pre_segment:{phase_label}'
                )

                send_future = self.nav_follow_path_client.send_goal_async(
                    follow_goal,
                    feedback_callback=_follow_feedback,
                )
                nav_goal_handle = await send_future
                if nav_goal_handle.accepted:
                    break
                self.get_logger().warn(
                    f'FollowPath phase "{phase_label}" rejected attempt '
                    f'{accept_attempt}/{nav_accept_attempts} at waypoint '
                    f'{waypoint_index + 1}/{total}'
                )
                if accept_attempt < nav_accept_attempts:
                    await self._non_blocking_wait(0.25)

            if nav_goal_handle is None or (not nav_goal_handle.accepted):
                return None, (
                    f'Nav2 rejected phase "{phase_label}" at waypoint '
                    f'{waypoint_index + 1}/{total}'
                )

            self.follow_path_nav_goal = nav_goal_handle
            nav_result_future = nav_goal_handle.get_result_async()
            last_feedback_ns = 0
            feedback_period_ns = int(0.15 * 1e9)

            while not nav_result_future.done():
                if goal_handle.is_cancel_requested:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    goal_handle.canceled()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = 'FollowPath canceled'
                    return None, terminal_error

                if self.estop_active:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    goal_handle.abort()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = 'E-STOP active'
                    return None, terminal_error

                if self.distance_obstacle_hold_active:
                    try:
                        await nav_goal_handle.cancel_goal_async()
                    except Exception:
                        pass
                    self.follow_path_nav_goal = None
                    wait_err = await _wait_for_obstacle_hold_clear(progress_index, phase_label)
                    if wait_err:
                        return None, wait_err
                    return None, obstacle_wait_retry

                now_ns = self.get_clock().now().nanoseconds
                if (now_ns - last_feedback_ns) >= feedback_period_ns:
                    nav_parts = []
                    dist_v = float(self._nav_follow_feedback_distance)
                    speed_v = float(self._nav_follow_feedback_speed)
                    if math.isfinite(dist_v):
                        nav_parts.append(f'd={dist_v:.2f}m')
                    if math.isfinite(speed_v):
                        nav_parts.append(f'v={speed_v:.2f}m/s')
                    if phase_label == 'MOVE_TO_POINT' and progress_index != waypoint_index:
                        base_status = (
                            f'{phase_label}: waypoint {progress_index + 1}->{waypoint_index + 1}/{total}'
                        )
                    else:
                        base_status = f'{phase_label}: waypoint {waypoint_index + 1}/{total}'
                    _publish_feedback(
                        progress_index,
                        base_status + (f' | {", ".join(nav_parts)}' if nav_parts else ''),
                    )
                    last_feedback_ns = now_ns

                await self._non_blocking_wait(0.05)

            self.follow_path_nav_goal = None
            try:
                wrapped = nav_result_future.result()
            except Exception as exc:
                return None, f'Phase "{phase_label}" result failed: {exc}'

            status = int(getattr(wrapped, 'status', GoalStatus.STATUS_UNKNOWN))
            if status != GoalStatus.STATUS_SUCCEEDED:
                return None, (
                    f'Phase "{phase_label}" failed with status {status} '
                    f'({self._goal_status_name(status)})'
                )

            if phase_label == 'MOVE_TO_POINT':
                arrival_err = await _validate_follow_move_arrival(
                    waypoint_index,
                    phase_label,
                    target_pose=target_pose,
                    is_terminal_segment=is_terminal_segment,
                )
                if arrival_err:
                    return None, arrival_err

            return wrapped, ''

        async def _run_follow_segment_phase_with_retries(
            segment_index: int,
            waypoint_index: int,
            phase_label: str,
            progress_index: int,
            *,
            end_pose_override: Optional[PoseStamped] = None,
            enforce_end_orientation_override: Optional[bool] = None,
            retry_on_aborted_override: Optional[bool] = None,
        ):
            max_attempts = max(1, int(self.path_max_retries))
            last_err = ''
            attempt = 1
            while attempt <= max_attempts:
                wrapped, phase_err = await _run_follow_segment_phase(
                    segment_index,
                    waypoint_index,
                    phase_label,
                    progress_index,
                    end_pose_override=end_pose_override,
                    enforce_end_orientation_override=enforce_end_orientation_override,
                )
                if not phase_err:
                    if attempt > 1:
                        self.get_logger().info(
                            f'{phase_label} recovered on attempt {attempt}/{max_attempts} '
                            f'at waypoint {waypoint_index + 1}/{total}'
                        )
                    return wrapped, ''

                if phase_err == obstacle_wait_retry:
                    self.get_logger().info(
                        f'{phase_label}: obstacle cleared, resuming at waypoint '
                        f'{waypoint_index + 1}/{total}'
                    )
                    continue

                if phase_err == terminal_error:
                    return None, terminal_error

                last_err = str(phase_err)
                is_aborted = '(ABORTED)' in last_err
                if is_aborted and self.distance_obstacle_hold_active:
                    wait_err = await _wait_for_obstacle_hold_clear(progress_index, phase_label)
                    if wait_err:
                        return None, wait_err
                    self.get_logger().info(
                        f'{phase_label}: obstacle-cleared retry at waypoint '
                        f'{waypoint_index + 1}/{total}'
                    )
                    continue
                retry_on_aborted = (
                    bool(self.path_retry_on_aborted)
                    if retry_on_aborted_override is None
                    else bool(retry_on_aborted_override)
                )
                if is_aborted and (not retry_on_aborted):
                    return None, last_err

                if attempt >= max_attempts:
                    break

                self.get_logger().warn(
                    f'{phase_label} attempt {attempt}/{max_attempts} failed at waypoint '
                    f'{waypoint_index + 1}/{total}: {last_err} -> retrying'
                )
                await self._non_blocking_wait(1.0)
                attempt += 1

            return None, (
                f'{phase_label} failed after {max_attempts} attempts at waypoint '
                f'{waypoint_index + 1}/{total}: {last_err}'
            )

        try:
            # Execute each segment in strict order.
            for seg_idx in range(total - 1):
                if goal_handle.is_cancel_requested:
                    goal_handle.canceled()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = 'FollowPath canceled'
                    return result

                if self.estop_active:
                    goal_handle.abort()
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = 'E-STOP active'
                    return result

                target_idx = seg_idx + 1

                # Phase 1: MOVE_TO_POINT using a segment FollowPath so the controller
                # retains lookahead. Semantic POI alignment is handled explicitly
                # afterward in this state machine, not implicitly in the move phase.
                _wrapped, phase_err = await _run_follow_segment_phase_with_retries(
                    seg_idx,
                    target_idx,
                    'MOVE_TO_POINT',
                    seg_idx,
                    enforce_end_orientation_override=False,
                )
                if phase_err:
                    if phase_err == terminal_error:
                        return result
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = phase_err
                    goal_handle.abort()
                    return result

                # Phase 2: semantic POI heading alignment only if the reached anchor
                # has an explicit POI heading and the current robot heading is outside
                # tolerance.
                phase_err = await _run_semantic_align_phase_with_retries(target_idx)
                if phase_err:
                    if phase_err == terminal_error:
                        return result
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = phase_err
                    goal_handle.abort()
                    return result

                # Phase 3: optional POI stop timer at reached waypoint.
                zone_name_at = _zone_name_at(target_idx)
                zone_meta_at = _zone_meta_at(target_idx)

                phase_err = await _run_semantic_stop_timer_phase(target_idx)
                if phase_err:
                    if phase_err == terminal_error:
                        return result
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = phase_err
                    goal_handle.abort()
                    return result

                # Phase 4: optional shelf detection/docking after arrival.
                phase_err = await _run_optional_shelf_check_phase(target_idx)
                if phase_err:
                    if phase_err == terminal_error:
                        return result
                    result.success = False
                    result.completed = int(max(0, completed_count))
                    result.message = phase_err
                    goal_handle.abort()
                    return result

                # Phase 5: optional POI action after move/align/stop timer/shelf check.
                zone_action_id = normalize_action_id(zone_meta_at.get('action', ''))
                zone_type = str(zone_meta_at.get('type', 'normal') or 'normal').strip().lower()
                should_run_action = bool(zone_action_id) or zone_type == 'action'
                if zone_name_at and target_idx not in actioned_indices:
                    if should_run_action:
                        actioned_indices.add(target_idx)
                        _publish_feedback(
                            target_idx,
                            f'ZONE_ACTION: "{zone_name_at}" at waypoint {target_idx + 1}/{total}',
                        )
                        action_ok, action_msg = await self._execute_path_zone_action(zone_name_at)
                        if action_ok:
                            self.get_logger().info(f'FollowPath zone action OK: {action_msg}')
                        else:
                            self.get_logger().warn(f'FollowPath zone action failed: {action_msg}')

                completed_count = max(completed_count, target_idx + 1)
                _publish_feedback(
                    target_idx,
                    f'WAYPOINT_READY: waypoint {target_idx + 1}/{total}',
                )

                # Next segment dispatch is implicit in the next loop iteration.
                if target_idx < (total - 1):
                    _publish_feedback(
                        target_idx,
                        f'MOVE_TO_NEXT_SEGMENT: segment {target_idx + 1}/{total - 1}',
                    )

            self.follow_path_nav_goal = None
            result.success = True
            result.completed = int(total)
            result.message = f'Completed all {total} waypoints with semantic path execution'
            goal_handle.succeed()
            return result
        finally:
            self._clear_active_follow_path()

    async def execute_follow_path(self, goal_handle):
        """Execute FollowPath using the per-waypoint state machine (MOVE_TO_POINT → ALIGN_AT_POINT → ZONE_ACTION)."""
        return await self._execute_follow_path_segmented(goal_handle)

    def delete_zone_callback(self, request, response):
        """Delete a zone"""
        zone_name = request.name
        zones = self.db_manager.get_all_zones()

        if zone_name not in zones:
            response.ok = False
            response.message = f'Zone "{zone_name}" not found'
            self.get_logger().warn(response.message)
            return response

        self.db_manager.delete_zone(zone_name)
        # Update cache
        if 'zones' in self.zones:
            self.zones['zones'].pop(zone_name, None)

        response.ok = True
        response.message = f'Zone "{zone_name}" deleted'
        self.get_logger().info(response.message)
        self._publish_viz_markers()
        return response

    def update_zone_params_callback(self, request, response):
        """Update zone parameters (type, speed, action, charge_duration)"""
        zone_name = request.name
        zones = self.db_manager.get_all_zones()

        if zone_name not in zones:
            response.ok = False
            response.message = f'Zone "{zone_name}" not found'
            return response

        zone_data = zones[zone_name]
        # Update metadata
        zone_data['type'] = request.type
        zone_data['speed'] = request.speed
        zone_data['charge_duration'] = max(0.0, float(request.charge_duration))

        if request.type == 'action' and request.action:
            zone_data['action'] = request.action
        else:
            zone_data.pop('action', None)

        # Keep charge_duration as a generic per-zone stop timer.
        if zone_data['charge_duration'] <= 0.0:
            zone_data.pop('charge_duration', None)

        self.db_manager.save_zone(zone_name, zone_data)
        # Update cache
        if 'zones' in self.zones:
            self.zones['zones'][zone_name] = zone_data

        response.ok = True
        response.message = f'Zone "{zone_name}" parameters updated'
        self.get_logger().info(response.message)
        return response

    def reorder_zones_callback(self, request, response):
        """Persist requested zone order and refresh in-memory cache."""
        try:
            ordered_names = [str(name).strip() for name in request.zone_names if str(name).strip()]
            ok = self.db_manager.reorder_zones(ordered_names)
            if not ok:
                response.ok = False
                response.message = 'Failed to reorder zones in database'
                self.get_logger().warn(response.message)
                return response

            self.load_zones()
            self._publish_viz_markers()
            response.ok = True
            response.message = f'Zones reordered ({len(self.zones.get("zones", {}))} total)'
            self.get_logger().info(response.message)
            return response
        except Exception as exc:
            response.ok = False
            response.message = f'Failed to reorder zones: {exc}'
            self.get_logger().error(response.message)
            return response

    def save_path_callback(self, request, response):
        """Save or update a named path from x/y coordinate arrays."""
        try:
            path_name = (request.name or '').strip()
            if not path_name:
                response.ok = False
                response.message = 'Path name is required'
                return response

            x_coords = list(request.x_coords)
            y_coords = list(request.y_coords)

            if len(x_coords) != len(y_coords):
                response.ok = False
                response.message = 'x_coords and y_coords length mismatch'
                return response

            if len(x_coords) < 2:
                response.ok = False
                response.message = 'Path must contain at least 2 points'
                return response

            points = []
            for x, y in zip(x_coords, y_coords):
                points.append({'x': float(x), 'y': float(y), 'z': 0.0})

            path_record = {'frame_id': 'map', 'points': points}
            self.db_manager.save_path(path_name, path_record)
            # Update cache
            self.paths[path_name] = path_record

            response.ok = True
            response.message = f'Path "{path_name}" saved ({len(points)} points)'
            self.get_logger().info(response.message)
            self._publish_viz_markers()
            return response
        except Exception as e:
            response.ok = False
            response.message = f'Failed to save path: {str(e)}'
            self.get_logger().error(response.message)
            return response

    def delete_path_callback(self, request, response):
        """Delete a named saved path."""
        try:
            path_name = (request.name or '').strip()
            if not path_name:
                response.ok = False
                response.message = 'Path name is required'
                return response

            self.db_manager.delete_path(path_name)
            # Update cache
            self.paths.pop(path_name, None)

            response.ok = True
            response.message = f'Path "{path_name}" deleted'
            self.get_logger().info(response.message)
            self._publish_viz_markers()
            return response
        except Exception as e:
            response.ok = False
            response.message = f'Failed to delete path: {str(e)}'
            self.get_logger().error(response.message)
            return response

    def save_layout_callback(self, request, response):
        """Save current zones+paths as a named layout."""
        try:
            # Use cached data for creating layout
            zones = self.zones.get('zones', {})
            paths = self.paths
            layout_data = {
                'description': request.description,
                'zones': zones,
                'paths': paths,
            }
            self.db_manager.save_layout(request.name, layout_data)
            # Update cache
            self.layouts[request.name] = layout_data

            response.ok = True
            response.message = f'Layout "{request.name}" saved'
            self.get_logger().info(response.message)
        except Exception as e:
            response.ok = False
            response.message = f'Failed to save layout: {str(e)}'
            self.get_logger().error(response.message)
        return response

    def load_layout_callback(self, request, response):
        """Load a layout: restore its zones and paths into the database."""
        try:
            layouts = self._normalize_layouts_map(self.db_manager.get_all_layouts())
            if request.name not in layouts:
                response.ok = False
                response.message = f'Layout "{request.name}" not found'
                return response

            layout = layouts[request.name]
            zones = self._normalize_zones_map(layout.get('zones', {}))
            paths = self._normalize_paths_map(layout.get('paths', {}))

            self.db_manager.replace_zones_and_paths(zones, paths)

            # Update caches
            self.zones = {'zones': zones}
            self.paths = paths
            # self.layouts cache remains unchanged

            response.ok = True
            response.message = f'Layout "{request.name}" loaded'
            self.get_logger().info(response.message)
        except Exception as e:
            response.ok = False
            response.message = f'Failed to load layout: {str(e)}'
            self.get_logger().error(response.message)
        return response

    def delete_layout_callback(self, request, response):
        """Delete a layout."""
        try:
            if request.name not in self.layouts:
                response.ok = False
                response.message = f'Layout "{request.name}" not found'
                return response

            self.db_manager.delete_layout(request.name)
            # Update cache
            self.layouts.pop(request.name, None)

            response.ok = True
            response.message = f'Layout "{request.name}" deleted'
            self.get_logger().info(response.message)
        except Exception as e:
            response.ok = False
            response.message = f'Failed to delete layout: {str(e)}'
            self.get_logger().error(response.message)
        return response
    def set_max_speed_callback(self, request, response):
        """Set max translational speed and apply it to controller runtime params."""
        if request.speed <= 0 or request.speed > 2.0:
            response.ok = False
            response.message = 'Speed must be between 0.01 and 2.0 m/s'
            return response

        self.max_speed = float(request.speed)
        queued, detail = self._queue_controller_speed_update(self.max_speed)
        response.ok = bool(queued)
        response.message = (
            f'Max speed set to {self.max_speed:.2f} m/s; {detail}'
            if queued else
            f'Failed to apply max speed: {detail}'
        )
        if response.ok:
            self.get_logger().info(response.message)
        else:
            self.get_logger().warn(response.message)
        return response

    def set_safety_override_callback(self, request, response):
        """Proxy safety override to SafetyController (single owner)."""
        enabled = bool(request.override_active)
        queued, detail = self._call_setbool_service_async(
            self.safety_override_client,
            enabled,
            self._endpoint('/safety/override')
        )
        response.ok = bool(queued)
        if queued:
            requested = 'enable' if enabled else 'disable'
            if self.safety_override_owner_state is None:
                owner_state = f"unknown (awaiting {self._endpoint('/safety/override_active')})"
            else:
                owner_state = 'enabled' if self.safety_override_owner_state else 'disabled'
            response.message = (
                f'Safety override request ({requested}) queued; {detail}; '
                f'owner state: {owner_state}'
            )
            self.get_logger().info(response.message)
        else:
            response.message = f'Failed to set safety override: {detail}'
            self.get_logger().warn(response.message)
        return response

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _zone_to_msg(self, name, zone_data):
        msg = Zone()
        msg.name = str(name)

        if not isinstance(zone_data, dict):
            msg.frame_id = 'map'
            msg.type = 'normal'
            return msg

        position = zone_data.get('position', {}) if isinstance(zone_data.get('position'), dict) else {}
        orientation = zone_data.get('orientation', {}) if isinstance(zone_data.get('orientation'), dict) else {}

        msg.position.x = self._safe_float(position.get('x', 0.0))
        msg.position.y = self._safe_float(position.get('y', 0.0))
        msg.position.z = self._safe_float(position.get('z', 0.0))

        msg.orientation.x = self._safe_float(orientation.get('x', 0.0))
        msg.orientation.y = self._safe_float(orientation.get('y', 0.0))
        msg.orientation.z = self._safe_float(orientation.get('z', 0.0))
        msg.orientation.w = self._safe_float(orientation.get('w', 1.0), default=1.0)

        msg.frame_id = str(zone_data.get('frame_id', 'map'))
        msg.type = str(zone_data.get('type', 'normal'))
        msg.speed = self._safe_float(zone_data.get('speed', 0.0))
        msg.action = str(zone_data.get('action', ''))
        msg.charge_duration = self._safe_float(zone_data.get('charge_duration', 0.0))
        return msg

    def _normalize_path_entry(self, path_data, default_frame_id='map'):
        """Normalize path storage to canonical {frame_id, points} shape."""
        frame_id = str(default_frame_id or 'map')
        raw_points = []

        if isinstance(path_data, dict):
            frame_id = str(path_data.get('frame_id', frame_id) or frame_id)
            if isinstance(path_data.get('points'), list):
                raw_points = path_data['points']
            elif isinstance(path_data.get('waypoints'), list):
                # Backward compatibility for older key names.
                raw_points = path_data['waypoints']
        elif isinstance(path_data, list):
            # Backward compatibility for list-only legacy paths.
            raw_points = path_data

        points = []
        for entry in raw_points:
            if isinstance(entry, dict):
                x = self._safe_float(entry.get('x', 0.0))
                y = self._safe_float(entry.get('y', 0.0))
                z = self._safe_float(entry.get('z', 0.0))
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                x = self._safe_float(entry[0], 0.0)
                y = self._safe_float(entry[1], 0.0)
                z = self._safe_float(entry[2], 0.0) if len(entry) > 2 else 0.0
            else:
                continue

            points.append({
                'x': float(x),
                'y': float(y),
                'z': float(z),
            })

        return {
            'frame_id': frame_id,
            'points': points,
        }

    def _normalize_paths_map(self, paths_data):
        """Normalize all path entries keyed by path name."""
        if not isinstance(paths_data, dict):
            return {}

        normalized = {}
        for path_name, path_data in paths_data.items():
            normalized[str(path_name)] = self._normalize_path_entry(path_data, default_frame_id='map')
        return normalized

    def _normalize_zones_map(self, zones_data):
        """Normalize zones map keyed by zone name."""
        if not isinstance(zones_data, dict):
            return {}

        # Backward compatibility: allow nested {"zones": {...}} payloads.
        payload = zones_data.get('zones') if 'zones' in zones_data else zones_data
        if not isinstance(payload, dict):
            return {}

        normalized = {}
        for zone_name, zone_data in payload.items():
            normalized[str(zone_name)] = zone_data if isinstance(zone_data, dict) else {}
        return normalized

    def _normalize_layout_entry(self, layout_data):
        """Normalize a single layout payload."""
        if not isinstance(layout_data, dict):
            return {
                'description': '',
                'map_path': '',
                'zones': {},
                'paths': {},
            }

        return {
            'description': str(layout_data.get('description', '')),
            'map_path': str(layout_data.get('map_path', '')),
            'zones': self._normalize_zones_map(layout_data.get('zones', {})),
            'paths': self._normalize_paths_map(layout_data.get('paths', {})),
        }

    def _normalize_layouts_map(self, layouts_data):
        """Normalize all layouts keyed by layout name."""
        if not isinstance(layouts_data, dict):
            return {}

        normalized = {}
        for layout_name, layout_data in layouts_data.items():
            normalized[str(layout_name)] = self._normalize_layout_entry(layout_data)
        return normalized

    def _path_to_msg(self, name, path_data):
        msg = PathInfo()
        msg.name = str(name)

        normalized = self._normalize_path_entry(path_data, default_frame_id='map')
        msg.frame_id = str(normalized.get('frame_id', 'map'))

        for entry in normalized.get('points', []):
            point = Point()
            point.x = self._safe_float(entry.get('x', 0.0))
            point.y = self._safe_float(entry.get('y', 0.0))
            point.z = self._safe_float(entry.get('z', 0.0))
            msg.points.append(point)

        return msg

    def get_zones_callback(self, request, response):
        """Get all zones from cache (instant)."""
        try:
            zones_dict = self.zones.get('zones', {})
            zones_msgs = [
                self._zone_to_msg(name, zone_data)
                for name, zone_data in zones_dict.items()
            ]

            response.success = True
            response.zones = zones_msgs
            response.message = f'Retrieved {len(response.zones)} zones'
            return response
        except Exception as e:
            response.success = False
            response.zones = []
            response.message = f'Failed to get zones: {str(e)}'
            return response

    def get_paths_callback(self, request, response):
        """Get all saved paths from cache (instant)."""
        try:
            path_msgs = [
                self._path_to_msg(name, path_data)
                for name, path_data in self.paths.items()
            ]

            response.success = True
            response.paths = path_msgs
            response.message = f'Retrieved {len(response.paths)} paths'
            return response
        except Exception as e:
            response.success = False
            response.paths = []
            response.message = f'Failed to get paths: {str(e)}'
            return response
    def _layout_to_msg(self, name, layout_data):
        msg = LayoutInfo()
        msg.name = str(name)

        if not isinstance(layout_data, dict):
            return msg

        msg.description = str(layout_data.get('description', ''))
        msg.map_path = str(layout_data.get('map_path', ''))

        zones = layout_data.get('zones', {}) if isinstance(layout_data.get('zones'), dict) else {}
        paths = layout_data.get('paths', {}) if isinstance(layout_data.get('paths'), dict) else {}

        msg.zones = [
            self._zone_to_msg(zone_name, zone_data)
            for zone_name, zone_data in zones.items()
        ]
        msg.paths = [
            self._path_to_msg(path_name, path_data)
            for path_name, path_data in paths.items()
        ]
        return msg

    def get_layouts_callback(self, request, response):
        """Get all saved layouts from cache (instant)."""
        try:
            layout_msgs = [
                self._layout_to_msg(name, layout_data)
                for name, layout_data in self.layouts.items()
            ]

            response.success = True
            response.layouts = layout_msgs
            response.message = f"Retrieved {len(response.layouts)} layouts"
            return response
        except Exception as e:
            response.success = False
            response.layouts = []
            response.message = f"Failed to get layouts: {str(e)}"
            return response

    def _atomic_yaml_write(self, path, data):
        """Atomically write YAML to disk to avoid partial/corrupt files."""
        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=directory)
        try:
            with os.fdopen(fd, "w") as f:
                yaml.safe_dump(data, f, sort_keys=False, default_flow_style=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def _default_registry(self):
        return {
            'version': int(self.registry_schema_version),
            'zones': {},
            'paths': {},
            'layouts': {},
        }

    def _normalize_registry(self, raw_registry):
        """Normalize registry payload to canonical schema."""
        registry = self._default_registry()
        if not isinstance(raw_registry, dict):
            return registry

        # Backward compatible read: accept either `version` (new) or `schema_version` (legacy).
        schema_raw = raw_registry.get('version', raw_registry.get('schema_version', self.registry_schema_version))
        try:
            schema_version = int(schema_raw)
        except (TypeError, ValueError):
            schema_version = int(self.registry_schema_version)
        registry['version'] = max(1, schema_version)

        registry['zones'] = self._normalize_zones_map(raw_registry.get('zones', {}))
        registry['paths'] = self._normalize_paths_map(raw_registry.get('paths', {}))
        registry['layouts'] = self._normalize_layouts_map(raw_registry.get('layouts', {}))
        return registry

    def _load_legacy_registry_snapshot(self):
        """Load legacy split files and project them into unified registry schema."""
        registry = self._default_registry()
        found_legacy_data = False

        if os.path.exists(self.zones_file):
            try:
                with open(self.zones_file, 'r') as f:
                    zones_raw = yaml.safe_load(f) or {}
                registry['zones'] = self._normalize_zones_map(zones_raw)
                found_legacy_data = found_legacy_data or bool(registry['zones'])
            except Exception as e:
                self.get_logger().warn(f'Failed reading legacy zones file "{self.zones_file}": {e}')

        if os.path.exists(self.paths_file):
            try:
                with open(self.paths_file, 'r') as f:
                    paths_raw = yaml.safe_load(f) or {}
                payload = paths_raw.get('paths', paths_raw) if isinstance(paths_raw, dict) else {}
                registry['paths'] = self._normalize_paths_map(payload)
                found_legacy_data = found_legacy_data or bool(registry['paths'])
            except Exception as e:
                self.get_logger().warn(f'Failed reading legacy paths file "{self.paths_file}": {e}')

        if os.path.exists(self.layouts_file):
            try:
                with open(self.layouts_file, 'r') as f:
                    layouts_raw = yaml.safe_load(f) or {}
                payload = layouts_raw.get('layouts', layouts_raw) if isinstance(layouts_raw, dict) else {}
                registry['layouts'] = self._normalize_layouts_map(payload)
                found_legacy_data = found_legacy_data or bool(registry['layouts'])
            except Exception as e:
                self.get_logger().warn(f'Failed reading legacy layouts file "{self.layouts_file}": {e}')

        return registry, bool(found_legacy_data)

    def _load_registry(self):
        """Load unified registry with automatic legacy migration fallback."""
        if os.path.exists(self.registry_file):
            try:
                with open(self.registry_file, 'r') as f:
                    raw = yaml.safe_load(f) or {}
                return self._normalize_registry(raw)
            except Exception as e:
                self.get_logger().error(f'Failed to load registry "{self.registry_file}": {e}')

        legacy_registry, has_legacy_data = self._load_legacy_registry_snapshot()
        if has_legacy_data:
            self.get_logger().warn(
                f'Unified registry "{self.registry_file}" missing/unreadable; '
                'migrating data from legacy split files'
            )
            try:
                self._atomic_yaml_write(self.registry_file, legacy_registry)
                self.get_logger().info(
                    f'Legacy data migrated to unified registry "{self.registry_file}"'
                )
            except Exception as e:
                self.get_logger().error(
                    f'Failed writing migrated registry "{self.registry_file}": {e}'
                )
            return legacy_registry

        return self._default_registry()

    def _write_registry(self, registry_payload):
        """Persist unified registry atomically."""
        normalized = self._normalize_registry(registry_payload)
        self._atomic_yaml_write(self.registry_file, normalized)

    def _load_all_caches(self):
        """Load all caches from database."""
        try:
            self.load_zones()
            self.paths = self._normalize_paths_map(self.db_manager.get_all_paths())
            self.layouts = self._normalize_layouts_map(self.db_manager.get_all_layouts())
            self.get_logger().info(f'Loaded {len(self.zones.get("zones", {}))} zones, {len(self.paths)} paths, {len(self.layouts)} layouts')
            self._publish_viz_markers()
        except Exception as e:
            self.get_logger().error(f'Failed to load caches: {e}')

    def load_zones(self, announce: bool = False):
        """Load zones from database into cache."""
        try:
            zones_map = self.db_manager.get_all_zones()
            
            # If database is empty, try loading from legacy files (migration)
            if not zones_map:
                self.get_logger().info('Database empty, attempting legacy file migration...')
                registry = self._load_registry()
                zones_map = self._normalize_zones_map(registry.get('zones', {}))
                
                # Save to database
                for zone_name, zone_data in zones_map.items():
                    self.db_manager.save_zone(zone_name, zone_data)
                
                if zones_map:
                    self.get_logger().info(f'Migrated {len(zones_map)} zones from legacy files to database')
            
            self.zones = {"zones": zones_map}
            if announce:
                zone_count = len(zones_map)
                self.get_logger().info(f'Loaded {zone_count} zones from database')
        except Exception as e:
            self.get_logger().error(f'Failed to load zones: {e}')
            self.zones = {"zones": {}}

    # ── RViz visualization helpers ────────────────────────────────────────────

    def _publish_viz_markers(self):
        """Publish all zones and saved paths as visualization_msgs/MarkerArray.

        Uses TRANSIENT_LOCAL QoS so RViz receives the latest state immediately
        on subscribe even if it started after this node.
        """
        try:
            self._viz_zones_pub.publish(self._build_zone_markers())
            self._viz_paths_pub.publish(self._build_path_markers())
        except Exception as exc:
            self.get_logger().debug(f'viz marker publish failed: {exc}')

    def _build_zone_markers(self) -> MarkerArray:
        """Build a MarkerArray for all zones: green sphere + white text label."""
        arr = MarkerArray()
        zones_dict = self.zones.get('zones', {})
        now = self.get_clock().now().to_msg()
        persistent = RosDuration(sec=0, nanosec=0)  # 0 = never expire
        for idx, (name, zone) in enumerate(zones_dict.items()):
            pos = zone.get('position', {})
            x = float(pos.get('x', 0.0))
            y = float(pos.get('y', 0.0))
            z = float(pos.get('z', 0.0))
            frame = str(zone.get('frame_id', 'map'))

            # Sphere at zone position
            m = Marker()
            m.header.frame_id = frame
            m.header.stamp = now
            m.ns = 'zones'
            m.id = idx * 2
            m.type = Marker.SPHERE
            m.action = Marker.ADD
            m.pose.position.x = x
            m.pose.position.y = y
            m.pose.position.z = z
            m.pose.orientation.w = 1.0
            m.scale.x = 0.25
            m.scale.y = 0.25
            m.scale.z = 0.25
            m.color.r = 0.1
            m.color.g = 0.85
            m.color.b = 0.3
            m.color.a = 0.9
            m.lifetime = persistent
            arr.markers.append(m)

            # Text label floating above the sphere
            t = Marker()
            t.header.frame_id = frame
            t.header.stamp = now
            t.ns = 'zones'
            t.id = idx * 2 + 1
            t.type = Marker.TEXT_VIEW_FACING
            t.action = Marker.ADD
            t.pose.position.x = x
            t.pose.position.y = y
            t.pose.position.z = z + 0.35
            t.pose.orientation.w = 1.0
            t.scale.z = 0.18
            t.color.r = 1.0
            t.color.g = 1.0
            t.color.b = 1.0
            t.color.a = 1.0
            t.text = name
            t.lifetime = persistent
            arr.markers.append(t)
        return arr

    def _build_path_markers(self) -> MarkerArray:
        """Build a MarkerArray for all saved paths: coloured line strip + dot list + name label."""
        arr = MarkerArray()
        # Distinct colours (R, G, B) cycling across saved paths
        palette = [
            (0.20, 0.60, 1.00),  # blue
            (1.00, 0.60, 0.10),  # orange
            (0.20, 0.90, 0.40),  # green
            (1.00, 0.30, 0.30),  # red
            (0.80, 0.30, 1.00),  # purple
            (0.00, 0.90, 0.90),  # cyan
            (1.00, 0.90, 0.10),  # yellow
            (1.00, 0.50, 0.70),  # pink
        ]
        now = self.get_clock().now().to_msg()
        persistent = RosDuration(sec=0, nanosec=0)
        for path_idx, (path_name, path_data) in enumerate(self.paths.items()):
            if not isinstance(path_data, dict):
                continue
            points_raw = path_data.get('points', [])
            frame = str(path_data.get('frame_id', 'map'))
            if len(points_raw) < 2:
                continue
            r, g, b = palette[path_idx % len(palette)]
            base_id = path_idx * 3

            from geometry_msgs.msg import Point as RosPoint
            ros_points = []
            for pt in points_raw:
                p = RosPoint()
                p.x = float(pt.get('x', 0.0))
                p.y = float(pt.get('y', 0.0))
                p.z = float(pt.get('z', 0.0))
                ros_points.append(p)

            # Line strip connecting waypoints
            line = Marker()
            line.header.frame_id = frame
            line.header.stamp = now
            line.ns = 'paths'
            line.id = base_id
            line.type = Marker.LINE_STRIP
            line.action = Marker.ADD
            line.pose.orientation.w = 1.0
            line.scale.x = 0.06
            line.color.r = r
            line.color.g = g
            line.color.b = b
            line.color.a = 0.85
            line.lifetime = persistent
            line.points = ros_points
            arr.markers.append(line)

            # Sphere at each waypoint
            dots = Marker()
            dots.header.frame_id = frame
            dots.header.stamp = now
            dots.ns = 'paths'
            dots.id = base_id + 1
            dots.type = Marker.SPHERE_LIST
            dots.action = Marker.ADD
            dots.pose.orientation.w = 1.0
            dots.scale.x = 0.12
            dots.scale.y = 0.12
            dots.scale.z = 0.12
            dots.color.r = r
            dots.color.g = g
            dots.color.b = b
            dots.color.a = 1.0
            dots.lifetime = persistent
            dots.points = ros_points
            arr.markers.append(dots)

            # Name text near the midpoint of the path
            mid = points_raw[len(points_raw) // 2]
            label = Marker()
            label.header.frame_id = frame
            label.header.stamp = now
            label.ns = 'path_labels'
            label.id = path_idx
            label.type = Marker.TEXT_VIEW_FACING
            label.action = Marker.ADD
            label.pose.position.x = float(mid.get('x', 0.0))
            label.pose.position.y = float(mid.get('y', 0.0))
            label.pose.position.z = float(mid.get('z', 0.0)) + 0.25
            label.pose.orientation.w = 1.0
            label.scale.z = 0.15
            label.color.r = r
            label.color.g = g
            label.color.b = b
            label.color.a = 1.0
            label.text = path_name
            label.lifetime = persistent
            arr.markers.append(label)
        return arr

    def _publish_active_goal_marker(self, zone_name: str, pose_stamped):
        """Publish a gold arrow + label showing the current GoToZone target in RViz."""
        arr = MarkerArray()
        now = self.get_clock().now().to_msg()
        persistent = RosDuration(sec=0, nanosec=0)
        frame = pose_stamped.header.frame_id or 'map'

        arrow = Marker()
        arrow.header.frame_id = frame
        arrow.header.stamp = now
        arrow.ns = 'active_goal'
        arrow.id = 0
        arrow.type = Marker.ARROW
        arrow.action = Marker.ADD
        arrow.pose = pose_stamped.pose
        arrow.scale.x = 0.55
        arrow.scale.y = 0.14
        arrow.scale.z = 0.14
        arrow.color.r = 1.0
        arrow.color.g = 0.75
        arrow.color.b = 0.0
        arrow.color.a = 1.0
        arrow.lifetime = persistent
        arr.markers.append(arrow)

        label = Marker()
        label.header.frame_id = frame
        label.header.stamp = now
        label.ns = 'active_goal'
        label.id = 1
        label.type = Marker.TEXT_VIEW_FACING
        label.action = Marker.ADD
        label.pose.position.x = pose_stamped.pose.position.x
        label.pose.position.y = pose_stamped.pose.position.y
        label.pose.position.z = pose_stamped.pose.position.z + 0.45
        label.pose.orientation.w = 1.0
        label.scale.z = 0.22
        label.color.r = 1.0
        label.color.g = 1.0
        label.color.b = 0.0
        label.color.a = 1.0
        label.text = f'\u2192 {zone_name}'
        label.lifetime = persistent
        arr.markers.append(label)
        self._viz_active_goal_pub.publish(arr)

    def _clear_active_goal_marker(self):
        """Remove the active-goal markers from RViz."""
        arr = MarkerArray()
        now = self.get_clock().now().to_msg()
        for mid in (0, 1):
            m = Marker()
            m.header.frame_id = 'map'
            m.header.stamp = now
            m.ns = 'active_goal'
            m.id = mid
            m.action = Marker.DELETE
            arr.markers.append(m)
        self._viz_active_goal_pub.publish(arr)

    def _publish_active_follow_path(self, waypoints):
        """Publish the active FollowPath waypoints as nav_msgs/Path on /next/viz/active_path.

        This lets RViz display the exact path the robot is about to execute,
        completely independent of the web UI.
        """
        path = NavPath()
        path.header.frame_id = 'map'
        path.header.stamp = self.get_clock().now().to_msg()
        for wp in waypoints:
            ps = PoseStamped()
            ps.header = path.header
            ps.pose.position.x = float(wp.pose.position.x)
            ps.pose.position.y = float(wp.pose.position.y)
            ps.pose.position.z = float(wp.pose.position.z)
            ps.pose.orientation = wp.pose.orientation
            path.poses.append(ps)
        self._viz_active_path_pub.publish(path)

    def _clear_active_follow_path(self):
        """Publish an empty nav_msgs/Path to clear the active path in RViz."""
        path = NavPath()
        path.header.frame_id = 'map'
        path.header.stamp = self.get_clock().now().to_msg()
        self._viz_active_path_pub.publish(path)

    # ─────────────────────────────────────────────────────────────────────────

    def save_zones(self):
        """Save zones to database."""
        try:
            zones_map = self.zones.get('zones', {})
            for zone_name, zone_data in zones_map.items():
                self.db_manager.save_zone(zone_name, zone_data)
            
            zone_count = len(zones_map)
            self.get_logger().info(f'Saved {zone_count} zones to database')
        except Exception as e:
            self.get_logger().error(f'Failed to save zones: {e}')


def main(args=None):
    rclpy.init(args=args)
    node = ZoneManager()
    executor = MultiThreadedExecutor(num_threads=4)
    executor.add_node(node)
    
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            executor.shutdown()
        except:
            pass
        try:
            node.destroy_node()
        except:
            pass
        try:
            rclpy.shutdown()
        except:
            pass  # Already shut down


if __name__ == '__main__':
    main()
