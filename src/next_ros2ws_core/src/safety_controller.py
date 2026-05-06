#!/usr/bin/env python3
import rclpy
from rclpy.node import Node
from rclpy.duration import Duration
from rclpy.time import Time
from geometry_msgs.msg import Twist, PoseWithCovarianceStamped, Point
from sensor_msgs.msg import LaserScan, PointCloud2
from sensor_msgs_py import point_cloud2
from std_msgs.msg import String, Bool, UInt8
from std_srvs.srv import Trigger, SetBool
from rcl_interfaces.msg import SetParametersResult
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from visualization_msgs.msg import Marker, MarkerArray
import math
from tf2_ros import Buffer, TransformException, TransformListener

# LiDAR drivers typically publish with BEST_EFFORT reliability.
# Subscribing with RELIABLE (rclpy default) causes a QoS incompatibility
# and results in zero messages — leaving the safety controller obstacle-blind.
_SENSOR_QOS = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    history=HistoryPolicy.KEEP_LAST,
    depth=5,
)

# Transient-local QoS for /control_mode: late-joining nodes must receive the
# last-published mode immediately without waiting for the next publish cycle.
_MODE_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
    history=HistoryPolicy.KEEP_LAST,
    depth=1,
)

_STATE_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
    history=HistoryPolicy.KEEP_LAST,
    depth=1,
)


class SafetyController(Node):
    """
    Safety controller that monitors robot state and publishes emergency stops
    directly to the wheel controller command topic.
    
    Monitors:
    - Localization confidence (AMCL)
    - Obstacle proximity (LIDAR)
    - Emergency stop requests (service)

    Safety priority (highest → lowest):
    1) E-STOP (human override)
    2) LiDAR distance (collision avoidance, VFH-style scan histogram)
    3) Localization confidence (graph-based SLAM covariance proxy)
    """
    
    def __init__(self):
        super().__init__('safety_controller')
        
        # Output topic for safety-gated wheel commands.  Configurable so the sim
        # launch can remap to /diff_cont/cmd_vel_unstamped without touching this node.
        self.cmd_vel_out_topic = str(
            self.declare_parameter('cmd_vel_output_topic', '/wheel_controller/cmd_vel_unstamped').value
        )
        # Publisher for safety-gated wheel commands (single writer to hardware topic).
        self.safety_pub = self.create_publisher(Twist, self.cmd_vel_out_topic, 10)

        # Input velocity stream from upstream mux/teleop/nav.
        self.cmd_vel_in_topic = str(self.declare_parameter('cmd_vel_input_topic', '/cmd_vel_mux_out').value)
        self.cmd_vel_in_sub = self.create_subscription(
            Twist,
            self.cmd_vel_in_topic,
            self.cmd_vel_input_callback,
            10
        )

        # Safety state

        # Manual E-STOP command state (from service).
        self.estop_active = False
        self.low_confidence_stop = False
        self.obstacle_stop = False
        self.manual_override = bool(self.declare_parameter('manual_override_on_startup', False).value)
        self.control_mode = 'unknown'
        self.distance_safety_enabled = bool(
            self.declare_parameter('distance_safety_enabled', True).value
        )
        self.nav2_filter_bridge_ready = False
        self.lenient_mode = False
        self.lenient_scale = float(self.declare_parameter('slam_lenient_scale', 0.1).value)
        legacy_disable_autonomous = bool(
            self.declare_parameter('disable_autonomous_safety_stop', False).value
        )
        legacy_lenient_manual = bool(
            self.declare_parameter('lenient_in_manual', False).value
        )
        if legacy_disable_autonomous or legacy_lenient_manual:
            raise RuntimeError(
                'Deprecated safety parameters disable_autonomous_safety_stop and '
                'lenient_in_manual are no longer supported. Remove them from the config.'
            )
        
        # Localization tracking. Keep defaults aligned with UI safety gate to avoid
        # navigation goals being accepted but immediately zeroed by safety stop.
        #this is important because our lidar has the robot in the back so if we remove this we wont be able to move anywhere cos lidar will keeep detecting obstacle!Jus
        self.localization_confidence = 100.0
        self.confidence_threshold = float(self.declare_parameter('confidence_threshold', 10.0).value)
        self.confidence_resume_threshold = float(self.declare_parameter('confidence_resume_threshold', 11.0).value)
        if self.confidence_resume_threshold <= self.confidence_threshold:
            self.confidence_resume_threshold = self.confidence_threshold + 1.0

        # Localization should only trigger a stop at true zero confidence.
        # Keep threshold params for backward compatibility/diagnostics only.
        self.localization_zero_stop_epsilon = max(
            0.0,
            float(self.declare_parameter('localization_zero_stop_epsilon', 0.0).value),
        )
        
        # Obstacle detection thresholds.
        # Critical/warning radii are still used, but final stop logic also considers
        # time-to-collision (TTC) with commanded velocity (Nav2-style approach model).
        self.autonomous_critical_zone_m = max(
            0.05,
            float(self.declare_parameter('autonomous_critical_zone_m', 0.22).value),
        )
        self.base_critical_zone = self.autonomous_critical_zone_m
        self.manual_critical_zone_m = max(
            0.0,
            float(self.declare_parameter('manual_critical_zone_m', 0.05).value),
        )  # used in manual/joystick mode
        self.manual_mode_same_obstacle_gate = bool(
            self.declare_parameter('manual_mode_same_obstacle_gate', True).value
        )
        self.manual_directional_escape_enabled = bool(
            self.declare_parameter('manual_directional_escape_enabled', True).value
        )
        self.base_warning_zone = 0.60
        self.base_safe_zone = 1.00
        self.critical_zone = self.base_critical_zone
        self.warning_zone = self.base_warning_zone
        self.safe_zone = self.base_safe_zone

        self.min_obstacle_distance_front = float('inf')
        self.min_obstacle_distance_rear = float('inf')
        self.raw_obstacle_distance_front = float('inf')
        self.raw_obstacle_distance_rear = float('inf')
        self.critical_obstacles_front = 0
        self.critical_obstacles_rear = 0
        self.warning_obstacles_front = 0
        self.warning_obstacles_rear = 0

        self.obstacle_min_points = max(1, int(self.declare_parameter('obstacle_min_points', 4).value))
        self.obstacle_clear_margin_m = float(self.declare_parameter('obstacle_clear_margin_m', 0.15).value)
        self.obstacle_stop_confirm_frames = max(1, int(self.declare_parameter('obstacle_stop_confirm_frames', 2).value))
        self.obstacle_clear_confirm_frames = max(1, int(self.declare_parameter('obstacle_clear_confirm_frames', 3).value))

        # Ignore very-close returns that are usually robot-body echoes.
        self.obstacle_valid_min_range = float(self.declare_parameter('obstacle_valid_min_range', 0.14).value)
        self.obstacle_median_window = max(1, int(self.declare_parameter('obstacle_median_window', 5).value))
        self.robot_body_length_m = max(0.10, float(self.declare_parameter('robot_body_length_m', 0.50).value))
        self.robot_body_width_m = max(0.10, float(self.declare_parameter('robot_body_width_m', 0.30).value))
        self.obstacle_body_padding_m = max(0.0, float(self.declare_parameter('obstacle_body_padding_m', 0.06).value))
        self.obstacle_body_exclusion_radius_m = max(0.0, float(self.declare_parameter('obstacle_body_exclusion_radius_m', 0.22).value))
        self.raw_scan_self_echo_range_m = max(
            0.0,
            float(self.declare_parameter('raw_scan_self_echo_range_m', 0.18).value),
        )
        self.raw_scan_opening_half_angle_deg = min(
            89.0,
            max(5.0, float(self.declare_parameter('raw_scan_opening_half_angle_deg', 42.0).value)),
        )
        self.carried_profile_active = bool(
            self.declare_parameter('carried_profile_active', False).value
        )
        self.carried_body_length_m = max(
            self.robot_body_length_m,
            float(self.declare_parameter('carried_body_length_m', self.robot_body_length_m).value),
        )
        self.carried_body_width_m = max(
            self.robot_body_width_m,
            float(self.declare_parameter('carried_body_width_m', self.robot_body_width_m).value),
        )
        self.carried_body_padding_m = max(
            0.0,
            float(self.declare_parameter('carried_body_padding_m', self.obstacle_body_padding_m).value),
        )
        self.carried_body_exclusion_radius_m = max(
            0.0,
            float(self.declare_parameter('carried_body_exclusion_radius_m', self.obstacle_body_exclusion_radius_m).value),
        )
        # Drop stale obstacle state if scans stay invalid for a few frames.
        self.invalid_scan_clear_frames = max(1, int(self.declare_parameter('invalid_scan_clear_frames', 3).value))
        # Evaluate front/rear blocking based on commanded direction.
        self.obstacle_use_directional_blocking = bool(
            self.declare_parameter('obstacle_use_directional_blocking', True).value
        )
        self.obstacle_hard_stop_distance_m = max(
            0.0,
            float(self.declare_parameter('obstacle_hard_stop_distance_m', 0.35).value),
        )
        self.obstacle_cloud_topic = str(
            self.declare_parameter('obstacle_cloud_topic', '/scan_combined_cloud').value
        )
        self.obstacle_scan_topic = str(self.declare_parameter('obstacle_scan_topic', '').value).strip()
        self.obstacle_prefer_cloud_timeout_sec = max(
            0.1,
            float(self.declare_parameter('obstacle_prefer_cloud_timeout_sec', 0.35).value),
        )
        self.obstacle_prefer_combined_timeout_sec = max(
            0.1,
            float(self.declare_parameter('obstacle_prefer_combined_timeout_sec', 0.35).value),
        )
        self.obstacle_forward_half_angle_deg = min(
            175.0,
            max(10.0, float(self.declare_parameter('obstacle_forward_half_angle_deg', 70.0).value)),
        )
        self.obstacle_rear_half_angle_deg = min(
            175.0,
            max(10.0, float(self.declare_parameter('obstacle_rear_half_angle_deg', 70.0).value)),
        )
        self.obstacle_forward_half_angle_rad = math.radians(self.obstacle_forward_half_angle_deg)
        self.obstacle_rear_half_angle_rad = math.radians(self.obstacle_rear_half_angle_deg)
        self.robot_base_frame = str(self.declare_parameter('robot_base_frame', 'base_link').value or 'base_link')
        self.tf_buffer = Buffer(cache_time=Duration(seconds=5.0))
        self.tf_listener = TransformListener(self.tf_buffer, self, spin_thread=True)

        self.ttc_stop_sec = max(0.1, float(self.declare_parameter('ttc_stop_sec', 0.9).value))
        self.ttc_warn_sec = max(self.ttc_stop_sec, float(self.declare_parameter('ttc_warn_sec', 1.6).value))
        self.ttc_min_speed_mps = max(0.01, float(self.declare_parameter('ttc_min_speed_mps', 0.05).value))
        self.obstacle_ttc_min_distance_m = max(0.0, float(self.declare_parameter('obstacle_ttc_min_distance_m', 0.32).value))
        self.obstacle_slowdown_enabled = bool(
            self.declare_parameter('obstacle_slowdown_enabled', True).value
        )
        self.obstacle_slowdown_min_scale = min(
            1.0,
            max(0.0, float(self.declare_parameter('obstacle_slowdown_min_scale', 0.25).value)),
        )
        self.obstacle_slowdown_distance_buffer_m = max(
            0.05,
            float(self.declare_parameter('obstacle_slowdown_distance_buffer_m', 0.25).value),
        )
        self.obstacle_slowdown_ttc_buffer_sec = max(
            0.1,
            float(self.declare_parameter('obstacle_slowdown_ttc_buffer_sec', 0.7).value),
        )
        self.obstacle_gate_strict_stop_only = bool(
            self.declare_parameter('obstacle_gate_strict_stop_only', True).value
        )
        self.obstacle_emergency_stop_distance_m = max(
            0.0,
            float(self.declare_parameter('obstacle_emergency_stop_distance_m', 0.08).value),
        )
        self.safety_debug_enabled = bool(
            self.declare_parameter('safety_debug_enabled', True).value
        )

        self._distance_ema_alpha = min(1.0, max(0.05, float(self.declare_parameter('obstacle_distance_ema_alpha', 0.35).value)))
        self.last_cmd_linear_x = 0.0
        self.last_cmd_angular_z = 0.0
        self.last_nonzero_cmd_linear_x = 0.0
        self.last_ttc_sec = float('inf')
        self.last_speed_scale = 1.0
        self._obstacle_stop_frames = 0
        self._obstacle_clear_frames = 0
        self._obstacle_reason = 'none'
        self._obstacle_blocked_direction = 'none'
        self._manual_blocked_direction = 'none'
        self._blocked_zones = set()
        self._front_blocked_zones = set()
        self._rear_blocked_zones = set()
        self._blocked_motion_components = set()
        self._allowed_motion_components = set(['forward', 'reverse', 'left_turn', 'right_turn'])
        self._obstacle_emergency_full_stop = False
        self._gate_debug = 'clear'
        self._last_cmd_direction = 'stopped'
        self._front_obstacle_present = False
        self._rear_obstacle_present = False
        self._reverse_allowed = True
        self._reverse_block_reason = 'clear'
        self._invalid_front_scan_frames = 0
        self._invalid_rear_scan_frames = 0
        self._last_combined_cloud_ns = None
        self._last_combined_scan_ns = None
        self._front_obstacle_debug = self._make_obstacle_debug_state('front')
        self._rear_obstacle_debug = self._make_obstacle_debug_state('rear')
        self.add_on_set_parameters_callback(self._on_runtime_parameters_changed)
        
        # Subscribe to AMCL pose with correct QoS
        qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=1
        )
        self.pose_sub = self.create_subscription(
            PoseWithCovarianceStamped,
            '/amcl_pose',
            self.pose_callback,
            qos
        )
        
        self.scan_cloud_sub = self.create_subscription(
            PointCloud2,
            self.obstacle_cloud_topic,
            self.scan_cloud_callback,
            _SENSOR_QOS,
        )

        # Prefer merged scan in base_link to avoid front/rear frame ambiguities.
        # Use BEST_EFFORT QoS: the scan_merger publishes with volatile/best-effort.
        self.scan_combined_sub = None
        if self.obstacle_scan_topic:
            self.scan_combined_sub = self.create_subscription(
                LaserScan,
                self.obstacle_scan_topic,
                self.scan_combined_callback,
                _SENSOR_QOS,
            )

        # Subscribe to BOTH front and rear lidars as fallback if merged scan is stale.
        self.scan_front_sub = self.create_subscription(
            LaserScan,
            '/scan',
            self.scan_front_callback,
            _SENSOR_QOS,
        )
        
        self.scan_rear_sub = self.create_subscription(
            LaserScan,
            '/scan2',
            self.scan_rear_callback,
            _SENSOR_QOS,
        )

        # Track control mode to optionally relax safety during manual/SLAM mapping.
        # Use transient-local QoS so this node receives the current mode even if it
        # starts after ControlModeManager has already published.
        self.control_mode_sub = self.create_subscription(
            String,
            '/control_mode',
            self.control_mode_callback,
            _MODE_QOS
        )
        self.nav2_filter_bridge_ready_sub = self.create_subscription(
            Bool,
            '/safety/nav2_filter_bridge_ready',
            self._nav2_filter_bridge_ready_callback,
            _STATE_QOS,
        )
        
        self.get_logger().info(f'Preferred obstacle cloud: {self.obstacle_cloud_topic}')
        if self.obstacle_scan_topic:
            self.get_logger().info(f'Preferred obstacle scan fallback: {self.obstacle_scan_topic}')
        else:
            self.get_logger().info('Preferred obstacle scan fallback: disabled (raw /scan + /scan2 only)')
        self.get_logger().info('Fallback FRONT lidar: /scan')
        self.get_logger().info('Fallback REAR lidar: /scan2')
        self.get_logger().info(f'Command gating input: {self.cmd_vel_in_topic} -> {self.cmd_vel_out_topic}')
        self.get_logger().info(
            f'Body echo filter: len={self.robot_body_length_m:.2f}m, width={self.robot_body_width_m:.2f}m, '
            f'pad={self.obstacle_body_padding_m:.2f}m, radius={self.obstacle_body_exclusion_radius_m:.2f}m'
        )
        self.get_logger().info(
            f'Dynamic slowdown: {"enabled" if self.obstacle_slowdown_enabled else "disabled"} '
            f'(min_scale={self.obstacle_slowdown_min_scale:.2f}, '
            f'distance_buffer={self.obstacle_slowdown_distance_buffer_m:.2f}m, '
            f'ttc_buffer={self.obstacle_slowdown_ttc_buffer_sec:.2f}s)'
        )
        self.get_logger().info(
            f'Distance safety: {"enabled" if self.distance_safety_enabled else "disabled"}'
        )
        self.get_logger().info(
            f'Hard-stop distance (autonomous/nav): {self.obstacle_hard_stop_distance_m:.2f}m'
        )
        self.get_logger().info(
            f'Obstacle gate policy: {"strict stop-only" if self.obstacle_gate_strict_stop_only else "slowdown + stop"}; '
            f'manual gate={"same as autonomous" if self.manual_mode_same_obstacle_gate else "manual-specific"}, '
            f'manual directional escape={"enabled" if self.manual_directional_escape_enabled else "disabled"}'
        )
        
        # Services
        self.estop_srv = self.create_service(SetBool, 'safety/emergency_stop', self.estop_callback)
        self.override_srv = self.create_service(SetBool, 'safety/override', self.override_callback)
        self.status_srv = self.create_service(Trigger, 'safety/status', self.status_callback)
        self.clear_state_srv = self.create_service(Trigger, 'safety/clear_state', self.clear_state_callback)

        # Publish ROS-native safety states for other nodes/UI consumers.
        # /set_estop: HARD operator E-STOP only — never mix with sensor-derived stops.
        # /safety/localization_invalid: localization confidence at zero (separate semantic).
        # /safety/obstacle_stop_active: LiDAR/TTC obstacle stop.
        # /safety/override_active: manual override engaged.
        self.set_estop_pub = self.create_publisher(Bool, '/set_estop', 10)
        self.localization_invalid_pub = self.create_publisher(Bool, '/safety/localization_invalid', 10)
        self.obstacle_stop_pub = self.create_publisher(Bool, '/safety/obstacle_stop_active', 10)
        self.override_state_pub = self.create_publisher(Bool, '/safety/override_active', 10)
        self.nav2_keepout_pub = self.create_publisher(
            Bool, '/safety/nav2_keepout_active', _STATE_QOS
        )
        self.nav2_keepout_direction_pub = self.create_publisher(
            String, '/safety/nav2_keepout_direction', _STATE_QOS
        )
        self.nav2_speed_limit_pub = self.create_publisher(
            UInt8, '/safety/nav2_speed_limit_percent', _STATE_QOS
        )
        self.debug_markers_pub = self.create_publisher(
            MarkerArray, '/safety/debug_markers', 10
        )
        
        # Timer to continuously publish stops when safety conditions are active
        self.safety_timer = self.create_timer(0.1, self.safety_check_callback)
        
        # Timer to continuously publish E-STOP state
        self.estop_publish_timer = self.create_timer(0.1, self._publish_estop_state)
        
        self.get_logger().info('Safety Controller active - monitoring robot safety')
        self.get_logger().info('Services: safety/emergency_stop, safety/override, safety/status, safety/clear_state')
        self.get_logger().info(
            f'Localization stop policy: zero-only (confidence <= {self.localization_zero_stop_epsilon:.2f}%)'
        )
        self.get_logger().info(
            'Nav2 filter bridge topics: '
            '/safety/nav2_keepout_active, '
            '/safety/nav2_keepout_direction, '
            '/safety/nav2_speed_limit_percent'
        )
        if self.manual_override:
            self.get_logger().warn(
                '⚠️  manual_override_on_startup=true: obstacle/localization gating disabled (E-STOP still active)'
            )

    def _graph_slam_confidence_from_covariance(self, covariance):
        """Approximate graph-SLAM confidence from pose covariance (proxy)."""
        # Use planar position covariance (x,y) as a confidence proxy.
        pos_var = float(covariance[0] + covariance[7])
        # Exponential falloff: high confidence at low variance, decays smoothly.
        # k tuned to keep ~100% near 0.01 and ~50% near 1.0.
        k = 2.2
        confidence = 100.0 * math.exp(-k * max(0.0, pos_var))
        return max(0.0, min(100.0, confidence))

    def _transform_msg_metadata_to_base(self, msg):
        if msg is None:
            return None
        frame_id = str(getattr(msg.header, 'frame_id', '') or '').strip()
        if not frame_id:
            return None
        if frame_id == self.robot_base_frame:
            return (0.0, 0.0, 0.0)
        try:
            transform = self.tf_buffer.lookup_transform(
                self.robot_base_frame,
                frame_id,
                Time.from_msg(msg.header.stamp),
                timeout=Duration(seconds=0.2),
            )
        except TransformException:
            return None

        translation = transform.transform.translation
        rotation = transform.transform.rotation
        qx = float(rotation.x)
        qy = float(rotation.y)
        qz = float(rotation.z)
        qw = float(rotation.w)
        siny_cosp = 2.0 * ((qw * qz) + (qx * qy))
        cosy_cosp = 1.0 - (2.0 * ((qy * qy) + (qz * qz)))
        yaw = math.atan2(siny_cosp, cosy_cosp)
        return (float(translation.x), float(translation.y), yaw)

    def _make_obstacle_debug_state(self, side: str) -> dict:
        return {
            'side': str(side),
            'source': 'none',
            'frame_id': '',
            'closest': None,
            'ignored': None,
            'blocked_zones': set(),
        }

    def _point_debug(self, *, source: str, frame_id: str, x_m: float, y_m: float, distance_m: float,
                     clearance_m: float, reason: str = '') -> dict:
        return {
            'source': str(source),
            'frame_id': str(frame_id or ''),
            'x_m': float(x_m),
            'y_m': float(y_m),
            'distance_m': float(distance_m),
            'clearance_m': float(clearance_m),
            'reason': str(reason or ''),
        }

    def _footprint_contains_point(self, x_m: float, y_m: float, padding_m: float = 0.0) -> bool:
        distance = math.hypot(float(x_m), float(y_m))
        return bool(distance <= self._body_support_radius_m(x_m, y_m, padding_m=float(padding_m)))

    def _format_point_debug(self, debug_point) -> str:
        if not debug_point:
            return 'none'
        reason = f", reason={debug_point['reason']}" if debug_point.get('reason') else ''
        return (
            f"{debug_point['source']} frame={debug_point['frame_id']} "
            f"x={debug_point['x_m']:.2f} y={debug_point['y_m']:.2f} "
            f"dist={debug_point['distance_m']:.2f}m clearance={debug_point['clearance_m']:.2f}m"
            f"{reason}"
        )

    def _set_scan_debug(self, side: str, source: str, frame_id: str, closest_point, ignored_point, blocked_zones=None) -> None:
        state = self._front_obstacle_debug if str(side).strip().lower() == 'front' else self._rear_obstacle_debug
        state['source'] = str(source)
        state['frame_id'] = str(frame_id or '')
        state['closest'] = closest_point
        state['ignored'] = ignored_point
        state['blocked_zones'] = set(blocked_zones or [])

    @staticmethod
    def _zone_sort_key(zone: str):
        order = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        try:
            return order.index(str(zone))
        except ValueError:
            return len(order)

    def _classify_zone(self, x_m: float, y_m: float) -> str:
        angle_deg = math.degrees(math.atan2(float(y_m), float(x_m)))
        if -22.5 <= angle_deg < 22.5:
            return 'N'
        if 22.5 <= angle_deg < 67.5:
            return 'NW'
        if 67.5 <= angle_deg < 112.5:
            return 'W'
        if 112.5 <= angle_deg < 157.5:
            return 'SW'
        if angle_deg >= 157.5 or angle_deg < -157.5:
            return 'S'
        if -157.5 <= angle_deg < -112.5:
            return 'SE'
        if -112.5 <= angle_deg < -67.5:
            return 'E'
        return 'NE'

    def _blocked_motion_from_zones(self, zones) -> set:
        zones = set(zones or [])
        blocked = set()
        if zones & {'N', 'NE', 'NW'}:
            blocked.add('forward')
        if zones & {'S', 'SE', 'SW'}:
            blocked.add('reverse')
        if zones & {'W', 'NW', 'SW'}:
            blocked.add('left_turn')
        if zones & {'E', 'NE', 'SE'}:
            blocked.add('right_turn')
        return blocked

    def _blocked_motion_from_direction(self, direction: str) -> set:
        direction = str(direction or 'none').strip().lower()
        if direction == 'forward':
            return {'forward'}
        if direction == 'backward':
            return {'reverse'}
        if direction == 'both':
            return {'forward', 'reverse'}
        return set()

    def _directional_obstacle_gate_active(self) -> bool:
        return bool(
            self.distance_safety_enabled
            and self.obstacle_stop
            and not self.manual_override
        )

    @staticmethod
    def _zone_angle_bounds(zone: str):
        bounds_deg = {
            'N': (-22.5, 22.5),
            'NW': (22.5, 67.5),
            'W': (67.5, 112.5),
            'SW': (112.5, 157.5),
            'S': (157.5, 202.5),
            'SE': (202.5, 247.5),
            'E': (247.5, 292.5),
            'NE': (292.5, 337.5),
        }.get(str(zone), None)
        if bounds_deg is None:
            return None
        return tuple(math.radians(v) for v in bounds_deg)

    def _zone_fill_points(self, zone: str, stop_distance: float):
        bounds = self._zone_angle_bounds(zone)
        if bounds is None:
            return []
        start, end = bounds
        samples = 8
        points = []
        for i in range(samples):
            a0 = start + ((end - start) * float(i) / float(samples))
            a1 = start + ((end - start) * float(i + 1) / float(samples))
            u0x = math.cos(a0)
            u0y = math.sin(a0)
            u1x = math.cos(a1)
            u1y = math.sin(a1)
            inner0 = self._body_support_radius_m(u0x, u0y, padding_m=0.0)
            inner1 = self._body_support_radius_m(u1x, u1y, padding_m=0.0)
            outer0 = inner0 + float(stop_distance)
            outer1 = inner1 + float(stop_distance)
            p_inner0 = self._marker_point(inner0 * u0x, inner0 * u0y, 0.01)
            p_outer0 = self._marker_point(outer0 * u0x, outer0 * u0y, 0.01)
            p_inner1 = self._marker_point(inner1 * u1x, inner1 * u1y, 0.01)
            p_outer1 = self._marker_point(outer1 * u1x, outer1 * u1y, 0.01)
            points.extend([p_inner0, p_outer0, p_outer1, p_inner0, p_outer1, p_inner1])
        return points

    @staticmethod
    def _marker_point(x_m: float, y_m: float, z_m: float = 0.0) -> Point:
        point = Point()
        point.x = float(x_m)
        point.y = float(y_m)
        point.z = float(z_m)
        return point

    def _sector_boundary_points(self, sector_center: float, half_angle: float, stop_distance: float):
        points = [self._marker_point(0.0, 0.0, 0.02)]
        samples = 18
        start = sector_center - half_angle
        end = sector_center + half_angle
        for i in range(samples + 1):
            angle = start + ((end - start) * float(i) / float(samples))
            ux = math.cos(angle)
            uy = math.sin(angle)
            radius = self._body_support_radius_m(ux, uy, padding_m=0.0) + float(stop_distance)
            points.append(self._marker_point(radius * ux, radius * uy, 0.02))
        points.append(self._marker_point(0.0, 0.0, 0.02))
        return points

    def _publish_debug_markers(self):
        if not self.safety_debug_enabled:
            return

        markers = MarkerArray()
        stamp = self.get_clock().now().to_msg()
        stop_distance = self._current_hard_stop_distance_m()

        footprint = Marker()
        footprint.header.frame_id = self.robot_base_frame
        footprint.header.stamp = stamp
        footprint.ns = 'safety_debug'
        footprint.id = 1
        footprint.type = Marker.LINE_STRIP
        footprint.action = Marker.ADD
        footprint.scale.x = 0.02
        footprint.color.r = 1.0
        footprint.color.g = 1.0
        footprint.color.b = 0.0
        footprint.color.a = 0.9
        half_len = 0.5 * self._effective_body_length_m()
        half_wid = 0.5 * self._effective_body_width_m()
        footprint.points = [
            self._marker_point(half_len, half_wid),
            self._marker_point(half_len, -half_wid),
            self._marker_point(-half_len, -half_wid),
            self._marker_point(-half_len, half_wid),
            self._marker_point(half_len, half_wid),
        ]
        markers.markers.append(footprint)

        for marker_id, (center, half_angle, color_rgb) in enumerate(
            (
                (0.0, self.obstacle_forward_half_angle_rad, (1.0, 0.1, 0.1)),
                (math.pi, self.obstacle_rear_half_angle_rad, (1.0, 0.5, 0.1)),
            ),
            start=2,
        ):
            marker = Marker()
            marker.header.frame_id = self.robot_base_frame
            marker.header.stamp = stamp
            marker.ns = 'safety_debug'
            marker.id = marker_id
            marker.type = Marker.LINE_STRIP
            marker.action = Marker.ADD
            marker.scale.x = 0.02
            marker.color.r = float(color_rgb[0])
            marker.color.g = float(color_rgb[1])
            marker.color.b = float(color_rgb[2])
            marker.color.a = 0.85
            marker.points = self._sector_boundary_points(center, half_angle, stop_distance)
            markers.markers.append(marker)

        closest_candidates = [
            state.get('closest')
            for state in (self._front_obstacle_debug, self._rear_obstacle_debug)
            if state.get('closest')
        ]
        ignored_candidates = [
            state.get('ignored')
            for state in (self._front_obstacle_debug, self._rear_obstacle_debug)
            if state.get('ignored')
        ]

        for marker_id, point_debug, color_rgb in (
            (4, min(closest_candidates, key=lambda p: p['clearance_m']) if closest_candidates else None, (0.0, 1.0, 0.0)),
            (5, min(ignored_candidates, key=lambda p: p['distance_m']) if ignored_candidates else None, (1.0, 1.0, 0.0)),
        ):
            marker = Marker()
            marker.header.frame_id = self.robot_base_frame
            marker.header.stamp = stamp
            marker.ns = 'safety_debug'
            marker.id = marker_id
            marker.type = Marker.SPHERE
            marker.action = Marker.ADD if point_debug else Marker.DELETE
            marker.scale.x = 0.08
            marker.scale.y = 0.08
            marker.scale.z = 0.08
            marker.color.r = float(color_rgb[0])
            marker.color.g = float(color_rgb[1])
            marker.color.b = float(color_rgb[2])
            marker.color.a = 0.95
            if point_debug:
                marker.pose.position = self._marker_point(point_debug['x_m'], point_debug['y_m'], 0.08)
                marker.pose.orientation.w = 1.0
            markers.markers.append(marker)

        blocked_region = Marker()
        blocked_region.header.frame_id = self.robot_base_frame
        blocked_region.header.stamp = stamp
        blocked_region.ns = 'safety_debug'
        blocked_region.id = 6
        blocked_region.type = Marker.TRIANGLE_LIST
        blocked_region.action = Marker.ADD if self._blocked_zones else Marker.DELETE
        blocked_region.pose.orientation.w = 1.0
        blocked_region.color.r = 1.0
        blocked_region.color.g = 0.0
        blocked_region.color.b = 0.0
        blocked_region.color.a = 0.22
        blocked_region.points = []
        for zone in sorted(self._blocked_zones, key=self._zone_sort_key):
            blocked_region.points.extend(self._zone_fill_points(zone, stop_distance))
        markers.markers.append(blocked_region)

        self.debug_markers_pub.publish(markers)

    def _vfh_classify_scan(self, msg, sector: str):
        """Classify raw scan points in base_footprint for a front/rear sector."""
        if msg is None or not msg.ranges:
            self._set_scan_debug(sector, 'raw_scan', '', None, None)
            return float('inf'), 0, 0, False

        transform_meta = self._transform_msg_metadata_to_base(msg)
        if transform_meta is None:
            self._set_scan_debug(
                sector,
                'raw_scan',
                str(getattr(getattr(msg, 'header', None), 'frame_id', '') or ''),
                None,
                None,
            )
            return float('inf'), 0, 0, False
        tx, ty, yaw = transform_meta

        min_dist = float('inf')
        critical_count = 0
        warning_count = 0
        closest_point = None
        closest_ignored = None
        blocked_zones = set()
        angle = msg.angle_min
        angle_inc = msg.angle_increment or 0.0
        valid_min = max(0.0, float(msg.range_min), float(self.obstacle_valid_min_range))
        valid_max = float(msg.range_max)
        sector_name = 'rear' if str(sector).strip().lower() == 'rear' else 'front'
        half_angle = self.obstacle_rear_half_angle_rad if sector_name == 'rear' else self.obstacle_forward_half_angle_rad
        sector_center = math.pi if sector_name == 'rear' else 0.0
        hard_stop_distance = self._current_hard_stop_distance_m()
        source = '/scan2' if sector_name == 'rear' else '/scan'
        frame_id = str(getattr(getattr(msg, 'header', None), 'frame_id', '') or '')

        for distance in msg.ranges:
            try:
                distance = float(distance)
            except Exception:
                angle += angle_inc
                continue

            if (not math.isfinite(distance)) or distance <= valid_min or distance >= valid_max:
                angle += angle_inc
                continue

            lx = float(distance * math.cos(angle))
            ly = float(distance * math.sin(angle))
            bx = tx + (math.cos(yaw) * lx) - (math.sin(yaw) * ly)
            by = ty + (math.sin(yaw) * lx) + (math.cos(yaw) * ly)
            base_angle = math.atan2(by, bx)
            sector_delta = abs(self._wrap_angle(base_angle - sector_center))
            point_distance = math.hypot(bx, by)
            clearance = self._obstacle_clearance_m(bx, by)

            if self._footprint_contains_point(bx, by, padding_m=0.0):
                ignored = self._point_debug(
                    source=source,
                    frame_id=frame_id,
                    x_m=bx,
                    y_m=by,
                    distance_m=point_distance,
                    clearance_m=clearance,
                    reason='inside_footprint',
                )
                if closest_ignored is None or ignored['distance_m'] < closest_ignored['distance_m']:
                    closest_ignored = ignored
                angle += angle_inc
                continue

            if sector_delta > half_angle:
                ignored = self._point_debug(
                    source=source,
                    frame_id=frame_id,
                    x_m=bx,
                    y_m=by,
                    distance_m=point_distance,
                    clearance_m=clearance,
                    reason='outside_sector',
                )
                if closest_ignored is None or ignored['distance_m'] < closest_ignored['distance_m']:
                    closest_ignored = ignored
                angle += angle_inc
                continue

            accepted = self._point_debug(
                source=source,
                frame_id=frame_id,
                x_m=bx,
                y_m=by,
                distance_m=point_distance,
                clearance_m=clearance,
                reason='accepted',
            )
            if closest_point is None or accepted['clearance_m'] < closest_point['clearance_m']:
                closest_point = accepted

            min_dist = min(min_dist, clearance)
            if clearance <= hard_stop_distance:
                critical_count += 1
                blocked_zones.add(self._classify_zone(bx, by))
            elif clearance <= self.warning_zone:
                warning_count += 1
            angle += angle_inc

        self._set_scan_debug(sector_name, source, frame_id, closest_point, closest_ignored, blocked_zones)
        return min_dist, critical_count, warning_count, math.isfinite(min_dist)

    def _smooth_distance(self, previous: float, current: float) -> float:
        if not math.isfinite(current):
            return previous
        if not math.isfinite(previous):
            return current
        a = self._distance_ema_alpha
        return (a * current) + ((1.0 - a) * previous)

    def _effective_body_length_m(self) -> float:
        if self.carried_profile_active:
            return max(float(self.robot_body_length_m), float(self.carried_body_length_m))
        return float(self.robot_body_length_m)

    def _effective_body_width_m(self) -> float:
        if self.carried_profile_active:
            return max(float(self.robot_body_width_m), float(self.carried_body_width_m))
        return float(self.robot_body_width_m)

    def _effective_body_padding_m(self) -> float:
        if self.carried_profile_active:
            return max(float(self.obstacle_body_padding_m), float(self.carried_body_padding_m))
        return float(self.obstacle_body_padding_m)

    def _effective_body_exclusion_radius_m(self) -> float:
        if self.carried_profile_active:
            return max(float(self.obstacle_body_exclusion_radius_m), float(self.carried_body_exclusion_radius_m))
        return float(self.obstacle_body_exclusion_radius_m)

    def _body_support_radius_m(self, x_m: float, y_m: float, padding_m: float = 0.0) -> float:
        half_len = max(0.0, (0.5 * self._effective_body_length_m()) + float(padding_m))
        half_wid = max(0.0, (0.5 * self._effective_body_width_m()) + float(padding_m))
        distance = math.hypot(float(x_m), float(y_m))
        if distance <= 1e-9:
            return max(float(self._effective_body_exclusion_radius_m()), half_len, half_wid)
        ux = float(x_m) / distance
        uy = float(y_m) / distance
        rectangular_extent = (half_len * abs(ux)) + (half_wid * abs(uy))
        return max(rectangular_extent, float(self._effective_body_exclusion_radius_m()))

    def _obstacle_clearance_m(self, x_m: float, y_m: float) -> float:
        distance = math.hypot(float(x_m), float(y_m))
        body_extent = self._body_support_radius_m(x_m, y_m, padding_m=0.0)
        return distance - body_extent

    def _is_body_echo_point(self, x_m: float, y_m: float) -> bool:
        # Ignore points that land inside the physical robot footprint in base_footprint.
        return self._footprint_contains_point(x_m, y_m, padding_m=0.0)

    def _on_runtime_parameters_changed(self, params):
        try:
            for param in list(params or []):
                name = str(getattr(param, 'name', '') or '').strip()
                value = getattr(param, 'value', None)
                if name == 'robot_body_length_m':
                    self.robot_body_length_m = max(0.10, float(value))
                elif name == 'robot_body_width_m':
                    self.robot_body_width_m = max(0.10, float(value))
                elif name == 'obstacle_body_padding_m':
                    self.obstacle_body_padding_m = max(0.0, float(value))
                elif name == 'obstacle_body_exclusion_radius_m':
                    self.obstacle_body_exclusion_radius_m = max(0.0, float(value))
                elif name == 'carried_profile_active':
                    self.carried_profile_active = bool(value)
                elif name == 'carried_body_length_m':
                    self.carried_body_length_m = max(0.10, float(value))
                elif name == 'carried_body_width_m':
                    self.carried_body_width_m = max(0.10, float(value))
                elif name == 'carried_body_padding_m':
                    self.carried_body_padding_m = max(0.0, float(value))
                elif name == 'carried_body_exclusion_radius_m':
                    self.carried_body_exclusion_radius_m = max(0.0, float(value))
            return SetParametersResult(successful=True)
        except Exception as exc:
            return SetParametersResult(successful=False, reason=str(exc))

    @staticmethod
    def _wrap_angle(angle: float) -> float:
        return math.atan2(math.sin(angle), math.cos(angle))

    def _sector_bin_strength(self, bin_counts) -> int:
        counts = [int(v) for v in list(bin_counts or [])]
        if not counts:
            return 0
        if len(counts) == 1:
            return max(0, counts[0])

        weighted_total = 0.0
        denom = max(1, len(counts) - 1)
        for idx, count in enumerate(counts):
            if count <= 0:
                continue
            center_ratio = 1.0 - (float(idx) / float(denom))
            weight = 0.5 + center_ratio
            weighted_total += float(count) * weight

        return max(max(counts), int(round(weighted_total)))

    def _median_valid_range(self, ranges, idx: int, valid_min: float, valid_max: float) -> float:
        window = max(1, int(self.obstacle_median_window))
        if window <= 1:
            value = float(ranges[idx])
            if math.isfinite(value) and (valid_min < value < valid_max):
                return value
            return float('nan')

        half = window // 2
        start = max(0, idx - half)
        end = min(len(ranges) - 1, idx + half)
        values = []
        for j in range(start, end + 1):
            value = float(ranges[j])
            if math.isfinite(value) and (valid_min < value < valid_max):
                values.append(value)

        if not values:
            return float('nan')
        values.sort()
        return float(values[len(values) // 2])

    def _classify_combined_scan(self, msg):
        if msg is None or not msg.ranges:
            return float('inf'), 0, 0, float('inf'), 0, 0

        bins = 24
        front_crit_bins = [0] * bins
        front_warn_bins = [0] * bins
        rear_crit_bins = [0] * bins
        rear_warn_bins = [0] * bins

        front_min = float('inf')
        rear_min = float('inf')

        ranges = list(msg.ranges)
        valid_min = max(float(msg.range_min), float(self.obstacle_valid_min_range))
        valid_max = float(msg.range_max)
        angle = float(msg.angle_min)
        angle_inc = float(msg.angle_increment)

        for i in range(len(ranges)):
            distance = self._median_valid_range(ranges, i, valid_min, valid_max)
            if not math.isfinite(distance):
                angle += angle_inc
                continue

            bx = float(distance * math.cos(angle))
            by = float(distance * math.sin(angle))
            if self._is_body_echo_point(bx, by):
                angle += angle_inc
                continue

            clearance = self._obstacle_clearance_m(bx, by)
            front_delta = abs(self._wrap_angle(angle))
            if front_delta <= self.obstacle_forward_half_angle_rad:
                front_min = min(front_min, clearance)
                if self.obstacle_forward_half_angle_rad > 1e-6:
                    front_ratio = front_delta / self.obstacle_forward_half_angle_rad
                else:
                    front_ratio = 0.0
                front_idx = min(bins - 1, max(0, int(front_ratio * bins)))
                if clearance < self.critical_zone:
                    front_crit_bins[front_idx] += 1
                elif clearance < self.warning_zone:
                    front_warn_bins[front_idx] += 1

            rear_delta = abs(self._wrap_angle(angle - math.pi))
            if rear_delta <= self.obstacle_rear_half_angle_rad:
                rear_min = min(rear_min, clearance)
                if self.obstacle_rear_half_angle_rad > 1e-6:
                    rear_ratio = rear_delta / self.obstacle_rear_half_angle_rad
                else:
                    rear_ratio = 0.0
                rear_idx = min(bins - 1, max(0, int(rear_ratio * bins)))
                if clearance < self.critical_zone:
                    rear_crit_bins[rear_idx] += 1
                elif clearance < self.warning_zone:
                    rear_warn_bins[rear_idx] += 1

            angle += angle_inc

        front_critical = self._sector_bin_strength(front_crit_bins)
        front_warning = self._sector_bin_strength(front_warn_bins)
        rear_critical = self._sector_bin_strength(rear_crit_bins)
        rear_warning = self._sector_bin_strength(rear_warn_bins)
        return front_min, front_critical, front_warning, rear_min, rear_critical, rear_warning

    def _classify_union_cloud(self, msg):
        if msg is None or int(getattr(msg, 'width', 0)) <= 0:
            return float('inf'), 0, 0, float('inf'), 0, 0, False

        transform_meta = self._transform_msg_metadata_to_base(msg)
        if transform_meta is None:
            return float('inf'), 0, 0, float('inf'), 0, 0, False
        tx, ty, yaw = transform_meta
        cy = math.cos(yaw)
        sy = math.sin(yaw)

        bins = 24
        front_crit_bins = [0] * bins
        front_warn_bins = [0] * bins
        rear_crit_bins = [0] * bins
        rear_warn_bins = [0] * bins

        front_min = float('inf')
        rear_min = float('inf')
        valid_points = 0

        for point in point_cloud2.read_points(msg, field_names=('x', 'y', 'z'), skip_nans=True):
            try:
                px = float(point[0])
                py = float(point[1])
            except Exception:
                continue
            if not (math.isfinite(px) and math.isfinite(py)):
                continue

            bx = tx + (cy * px) - (sy * py)
            by = ty + (sy * px) + (cy * py)
            if self._is_body_echo_point(bx, by):
                continue

            distance = math.hypot(bx, by)
            if distance <= float(self.obstacle_valid_min_range):
                continue
            valid_points += 1

            clearance = self._obstacle_clearance_m(bx, by)
            angle = math.atan2(by, bx)
            front_delta = abs(self._wrap_angle(angle))
            if front_delta <= self.obstacle_forward_half_angle_rad:
                front_min = min(front_min, clearance)
                front_ratio = (front_delta / self.obstacle_forward_half_angle_rad) if self.obstacle_forward_half_angle_rad > 1e-6 else 0.0
                front_idx = min(bins - 1, max(0, int(front_ratio * bins)))
                if clearance < self.critical_zone:
                    front_crit_bins[front_idx] += 1
                elif clearance < self.warning_zone:
                    front_warn_bins[front_idx] += 1

            rear_delta = abs(self._wrap_angle(angle - math.pi))
            if rear_delta <= self.obstacle_rear_half_angle_rad:
                rear_min = min(rear_min, clearance)
                rear_ratio = (rear_delta / self.obstacle_rear_half_angle_rad) if self.obstacle_rear_half_angle_rad > 1e-6 else 0.0
                rear_idx = min(bins - 1, max(0, int(rear_ratio * bins)))
                if clearance < self.critical_zone:
                    rear_crit_bins[rear_idx] += 1
                elif clearance < self.warning_zone:
                    rear_warn_bins[rear_idx] += 1

        front_critical = self._sector_bin_strength(front_crit_bins)
        front_warning = self._sector_bin_strength(front_warn_bins)
        rear_critical = self._sector_bin_strength(rear_crit_bins)
        rear_warning = self._sector_bin_strength(rear_warn_bins)
        return (
            front_min,
            front_critical,
            front_warning,
            rear_min,
            rear_critical,
            rear_warning,
            valid_points > 0,
        )

    def _combined_cloud_fresh(self) -> bool:
        if self._last_combined_cloud_ns is None:
            return False
        now_ns = int(self.get_clock().now().nanoseconds)
        age_sec = max(0.0, float(now_ns - int(self._last_combined_cloud_ns)) / 1e9)
        return age_sec <= float(self.obstacle_prefer_cloud_timeout_sec)

    def _combined_scan_fresh(self) -> bool:
        if self._last_combined_scan_ns is None:
            return False
        now_ns = int(self.get_clock().now().nanoseconds)
        age_sec = max(0.0, float(now_ns - int(self._last_combined_scan_ns)) / 1e9)
        return age_sec <= float(self.obstacle_prefer_combined_timeout_sec)

    def _preferred_union_source_fresh(self) -> bool:
        return bool(self._combined_cloud_fresh() or self._combined_scan_fresh())

    def scan_cloud_callback(self, msg):
        (
            front_min,
            front_critical,
            front_warning,
            rear_min,
            rear_critical,
            rear_warning,
            valid_points,
        ) = self._classify_union_cloud(msg)

        self._last_combined_cloud_ns = int(self.get_clock().now().nanoseconds)
        if not valid_points:
            return

        self._invalid_front_scan_frames = 0
        self._invalid_rear_scan_frames = 0

        if math.isfinite(front_min):
            self.raw_obstacle_distance_front = float(front_min)
            self.min_obstacle_distance_front = self._smooth_distance(self.min_obstacle_distance_front, front_min)
        else:
            self.raw_obstacle_distance_front = float('inf')
            self.min_obstacle_distance_front = float('inf')
        self.critical_obstacles_front = int(front_critical)
        self.warning_obstacles_front = int(front_warning)

        if math.isfinite(rear_min):
            self.raw_obstacle_distance_rear = float(rear_min)
            self.min_obstacle_distance_rear = self._smooth_distance(self.min_obstacle_distance_rear, rear_min)
        else:
            self.raw_obstacle_distance_rear = float('inf')
            self.min_obstacle_distance_rear = float('inf')
        self.critical_obstacles_rear = int(rear_critical)
        self.warning_obstacles_rear = int(rear_warning)

        self._update_obstacle_state()

    def scan_combined_callback(self, msg):
        """Preferred obstacle source: classify front/rear cones in base_link scan."""
        if self._combined_cloud_fresh():
            return
        (
            front_min,
            front_critical,
            front_warning,
            rear_min,
            rear_critical,
            rear_warning,
        ) = self._classify_combined_scan(msg)

        self._last_combined_scan_ns = int(self.get_clock().now().nanoseconds)

        if math.isfinite(front_min):
            self._invalid_front_scan_frames = 0
            self.raw_obstacle_distance_front = float(front_min)
            self.min_obstacle_distance_front = self._smooth_distance(self.min_obstacle_distance_front, front_min)
            self.critical_obstacles_front = int(front_critical)
            self.warning_obstacles_front = int(front_warning)
        else:
            self._invalid_front_scan_frames += 1
            self.critical_obstacles_front = 0
            self.warning_obstacles_front = 0
            if self._invalid_front_scan_frames >= self.invalid_scan_clear_frames:
                self.raw_obstacle_distance_front = float('inf')
                self.min_obstacle_distance_front = float('inf')

        if math.isfinite(rear_min):
            self._invalid_rear_scan_frames = 0
            self.raw_obstacle_distance_rear = float(rear_min)
            self.min_obstacle_distance_rear = self._smooth_distance(self.min_obstacle_distance_rear, rear_min)
            self.critical_obstacles_rear = int(rear_critical)
            self.warning_obstacles_rear = int(rear_warning)
        else:
            self._invalid_rear_scan_frames += 1
            self.critical_obstacles_rear = 0
            self.warning_obstacles_rear = 0
            if self._invalid_rear_scan_frames >= self.invalid_scan_clear_frames:
                self.raw_obstacle_distance_rear = float('inf')
                self.min_obstacle_distance_rear = float('inf')

        self._update_obstacle_state()

    def _obstacle_side_state(self, side: str) -> dict:
        side_name = 'rear' if str(side).strip().lower() == 'rear' else 'front'
        if side_name == 'rear':
            raw_distance = float(self.raw_obstacle_distance_rear)
            smooth_distance = float(self.min_obstacle_distance_rear)
            critical_count = int(self.critical_obstacles_rear)
        else:
            raw_distance = float(self.raw_obstacle_distance_front)
            smooth_distance = float(self.min_obstacle_distance_front)
            critical_count = int(self.critical_obstacles_front)

        hard_stop_distance = self._current_hard_stop_distance_m()
        trigger = critical_count > 0
        hard_stop = math.isfinite(raw_distance) and raw_distance <= hard_stop_distance
        active = bool(trigger or hard_stop)
        clear = (not math.isfinite(smooth_distance)) or (
            smooth_distance > (self.critical_zone + self.obstacle_clear_margin_m)
        )

        return {
            'side': side_name,
            'raw_distance': raw_distance,
            'smooth_distance': smooth_distance,
            'critical_count': critical_count,
            'trigger': bool(trigger),
            'hard_stop': bool(hard_stop),
            'active': bool(active),
            'clear': bool(clear),
        }

    def _evaluate_ttc_state(self) -> tuple:
        moving_forward = self.last_cmd_linear_x > self.ttc_min_speed_mps
        moving_backward = self.last_cmd_linear_x < -self.ttc_min_speed_mps

        ttc_side = ''
        ttc_value = float('inf')
        if moving_forward and math.isfinite(self.min_obstacle_distance_front):
            if self.min_obstacle_distance_front >= self.obstacle_ttc_min_distance_m:
                ttc_side = 'front'
                ttc_value = self.min_obstacle_distance_front / max(self.last_cmd_linear_x, self.ttc_min_speed_mps)
        elif moving_backward and math.isfinite(self.min_obstacle_distance_rear):
            if self.min_obstacle_distance_rear >= self.obstacle_ttc_min_distance_m:
                ttc_side = 'rear'
                ttc_value = self.min_obstacle_distance_rear / max(abs(self.last_cmd_linear_x), self.ttc_min_speed_mps)

        ttc_warning = math.isfinite(ttc_value) and ttc_value <= self.ttc_warn_sec
        ttc_trigger = math.isfinite(ttc_value) and ttc_value <= self.ttc_stop_sec
        self.last_ttc_sec = ttc_value
        return ttc_side, ttc_value, ttc_warning, ttc_trigger

    def _evaluate_obstacle_trigger(self):
        front = self._obstacle_side_state('front')
        rear = self._obstacle_side_state('rear')
        ttc_side, ttc_value, ttc_warning, ttc_trigger = self._evaluate_ttc_state()

        front_active = bool(front['active'] or ttc_side == 'front')
        rear_active = bool(rear['active'] or ttc_side == 'rear')
        blocked_direction = self._derive_blocked_direction(
            {
                'front_active': front_active,
                'rear_active': rear_active,
                'ttc_side': ttc_side,
            }
        )

        should_trigger = bool(front['active'] or rear['active'] or ttc_trigger)
        immediate_trigger = bool(front['hard_stop'] or rear['hard_stop'])
        clear_ready = (not should_trigger) and front['clear'] and rear['clear']

        reasons = []
        if front['hard_stop']:
            reasons.append(f'front_hard_stop={front["raw_distance"]:.2f}m')
        if rear['hard_stop']:
            reasons.append(f'rear_hard_stop={rear["raw_distance"]:.2f}m')
        if front['trigger']:
            reasons.append(f'front_critical={front["critical_count"]}')
        if rear['trigger']:
            reasons.append(f'rear_critical={rear["critical_count"]}')
        if ttc_trigger and ttc_side:
            reasons.append(f'ttc_{ttc_side}={ttc_value:.2f}s')

        reason = ', '.join(reasons) if reasons else 'none'
        details = {
            'front_trigger': bool(front['trigger']),
            'rear_trigger': bool(rear['trigger']),
            'front_hard_stop': bool(front['hard_stop']),
            'rear_hard_stop': bool(rear['hard_stop']),
            'front_active': bool(front_active),
            'rear_active': bool(rear_active),
            'front_clear': bool(front['clear']),
            'rear_clear': bool(rear['clear']),
            'ttc_side': str(ttc_side or ''),
            'blocked_direction': str(blocked_direction or 'none'),
        }
        return should_trigger, clear_ready, reason, ttc_warning, immediate_trigger, details

    def _manual_directional_gate_active(self) -> bool:
        return bool(
            self._directional_obstacle_gate_active()
            and self._blocked_motion_components
            and not self._obstacle_emergency_full_stop
        )

    def _control_mode_normalized(self) -> str:
        return str(self.control_mode or '').strip().lower()

    def _nav2_filter_handoff_enabled(self) -> bool:
        return bool(
            self.distance_safety_enabled
            and self.nav2_filter_bridge_ready
            and not self.manual_override
            and self._control_mode_normalized() != 'manual'
        )

    def _current_cmd_gate_state(self):
        """Return whether safety must still hard-gate wheel commands."""
        if self.estop_active:
            return True, ['E-STOP']

        if self.low_confidence_stop and not self.manual_override:
            return True, [f'LOW_CONFIDENCE({self.localization_confidence:.1f}%)']

        if self._distance_hold_active():
            if self._manual_directional_gate_active():
                return False, []
            return True, [f'OBSTACLE({self._obstacle_reason})']

        return False, []

    def _derive_blocked_direction(self, details) -> str:
        front_active = bool(details.get('front_active') or details.get('ttc_side') == 'front')
        rear_active = bool(details.get('rear_active') or details.get('ttc_side') == 'rear')
        if (not self.obstacle_use_directional_blocking) and (front_active or rear_active):
            return 'both'
        if front_active and rear_active:
            return 'both'
        if front_active:
            return 'forward'
        if rear_active:
            return 'backward'
        return 'none'

    def _apply_manual_directional_gate(self, msg: Twist) -> Twist:
        gated = Twist()
        gated.linear.x = float(msg.linear.x)
        gated.linear.y = float(msg.linear.y)
        gated.linear.z = float(msg.linear.z)
        gated.angular.x = float(msg.angular.x)
        gated.angular.y = float(msg.angular.y)
        gated.angular.z = float(msg.angular.z)

        blocked = set(self._blocked_motion_components or [])
        reasons = []
        command_parts = []
        if gated.linear.x > self.ttc_min_speed_mps:
            command_parts.append('forward')
        elif gated.linear.x < -self.ttc_min_speed_mps:
            command_parts.append('reverse')
        if gated.angular.z > self.ttc_min_speed_mps:
            command_parts.append('left_turn')
        elif gated.angular.z < -self.ttc_min_speed_mps:
            command_parts.append('right_turn')
        self._last_cmd_direction = '+'.join(command_parts) if command_parts else 'stationary'
        self._reverse_allowed = True
        self._reverse_block_reason = 'not_reversing'
        if 'forward' in blocked and gated.linear.x > self.ttc_min_speed_mps:
            gated.linear.x = 0.0
            reasons.append('forward')
        elif 'reverse' in blocked and gated.linear.x < -self.ttc_min_speed_mps:
            gated.linear.x = 0.0
            reasons.append('reverse')
            self._reverse_allowed = False
            rear_zones = sorted(self._rear_blocked_zones, key=self._zone_sort_key)
            if rear_zones:
                self._reverse_block_reason = f"rear_danger_zone:{','.join(rear_zones)}"
            elif self._obstacle_emergency_full_stop:
                self._reverse_block_reason = 'boxed_in_full_stop'
            else:
                self._reverse_block_reason = f"latched_block:{self._obstacle_blocked_direction}"
        elif gated.linear.x < -self.ttc_min_speed_mps:
            self._reverse_allowed = True
            self._reverse_block_reason = (
                f"rear_clear(front_zones={','.join(sorted(self._front_blocked_zones, key=self._zone_sort_key)) or 'none'})"
            )

        if 'left_turn' in blocked and gated.angular.z > self.ttc_min_speed_mps:
            gated.angular.z = 0.0
            reasons.append('left_turn')
        elif 'right_turn' in blocked and gated.angular.z < -self.ttc_min_speed_mps:
            gated.angular.z = 0.0
            reasons.append('right_turn')

        self._gate_debug = (
            f"cmd={self._last_cmd_direction} "
            f"blocked={','.join(sorted(reasons)) or 'none'} "
            f"allowed={','.join(sorted(self._allowed_motion_components, key=str)) or 'none'} "
            f"zones={','.join(sorted(self._blocked_zones, key=self._zone_sort_key)) or 'none'} "
            f"reverse={'allowed' if self._reverse_allowed else 'blocked'} "
            f"reason={self._reverse_block_reason}"
        )

        return gated

    def _manual_blocked_direction_for_reason(self, derived_direction: str, reason: str) -> str:
        blocked = str(derived_direction or 'none').strip().lower()
        if blocked not in ('forward', 'backward', 'both'):
            return 'none'
        return blocked

    def _update_obstacle_state(self):
        if not self.distance_safety_enabled:
            self.obstacle_stop = False
            self._obstacle_stop_frames = 0
            self._obstacle_clear_frames = 0
            self._obstacle_reason = 'distance_safety_disabled'
            self._obstacle_blocked_direction = 'none'
            self._manual_blocked_direction = 'none'
            self._blocked_zones = set()
            self._front_blocked_zones = set()
            self._rear_blocked_zones = set()
            self._blocked_motion_components = set()
            self._allowed_motion_components = set(['forward', 'reverse', 'left_turn', 'right_turn'])
            self._obstacle_emergency_full_stop = False
            self._gate_debug = 'distance_safety_disabled'
            self.last_ttc_sec = float('inf')
            return

        should_trigger, clear_ready, reason, ttc_warning, immediate_trigger, details = self._evaluate_obstacle_trigger()
        self._front_blocked_zones = set(self._front_obstacle_debug.get('blocked_zones') or [])
        self._rear_blocked_zones = set(self._rear_obstacle_debug.get('blocked_zones') or [])
        self._blocked_zones = set(self._front_blocked_zones) | set(self._rear_blocked_zones)
        self._front_obstacle_present = bool(details.get('front_active') or self._front_blocked_zones)
        self._rear_obstacle_present = bool(details.get('rear_active') or self._rear_blocked_zones)
        self._blocked_motion_components = self._blocked_motion_from_zones(self._blocked_zones)
        all_motion_components = {'forward', 'reverse', 'left_turn', 'right_turn'}
        self._allowed_motion_components = all_motion_components - set(self._blocked_motion_components)
        self._obstacle_emergency_full_stop = bool(self._blocked_motion_components == all_motion_components)
        if self._obstacle_emergency_full_stop:
            self._blocked_motion_components = set(all_motion_components)
            self._allowed_motion_components = set()
            self._gate_debug = (
                f"emergency_full_stop zones={','.join(sorted(self._blocked_zones, key=self._zone_sort_key)) or 'none'}"
            )

        requested_direction = self._derive_blocked_direction(details)

        if should_trigger:
            if requested_direction in ('forward', 'backward', 'both'):
                self._obstacle_blocked_direction = requested_direction
            if (
                self._control_mode_normalized() == 'manual'
                and self.manual_directional_escape_enabled
            ):
                self._manual_blocked_direction = self._manual_blocked_direction_for_reason(
                    self._obstacle_blocked_direction,
                    reason,
                )
            self._obstacle_stop_frames = (
                self.obstacle_stop_confirm_frames if immediate_trigger else (self._obstacle_stop_frames + 1)
            )
            self._obstacle_clear_frames = 0
            self._obstacle_reason = reason
            if (not self.obstacle_stop) and (self._obstacle_stop_frames >= self.obstacle_stop_confirm_frames):
                self.obstacle_stop = True
                self.get_logger().warn(
                    f'🚨 SAFETY STOP: obstacle condition ({self._obstacle_reason}) '
                    f'[front={self.min_obstacle_distance_front:.2f}m rear={self.min_obstacle_distance_rear:.2f}m]'
                )
        else:
            self._obstacle_stop_frames = 0
            if self.obstacle_stop and clear_ready:
                self._obstacle_clear_frames += 1
                if self._obstacle_clear_frames >= self.obstacle_clear_confirm_frames:
                    self.obstacle_stop = False
                    self._obstacle_reason = 'cleared'
                    self._obstacle_blocked_direction = 'none'
                    self._manual_blocked_direction = 'none'
                    self.get_logger().info(
                        f'✓ Obstacles cleared [front={self.min_obstacle_distance_front:.2f}m '
                        f'rear={self.min_obstacle_distance_rear:.2f}m]'
                    )
            else:
                self._obstacle_clear_frames = 0
                if self.obstacle_stop and requested_direction in ('forward', 'backward', 'both'):
                    self._obstacle_blocked_direction = requested_direction
                    if (
                        self._control_mode_normalized() == 'manual'
                        and self.manual_directional_escape_enabled
                    ):
                        self._manual_blocked_direction = self._manual_blocked_direction_for_reason(
                            requested_direction,
                            self._obstacle_reason,
                        )
                if not self.obstacle_stop:
                    self._obstacle_blocked_direction = 'none'
                    self._manual_blocked_direction = 'none'

        if (not self._blocked_motion_components) and self.obstacle_stop:
            self._blocked_motion_components = self._blocked_motion_from_direction(self._obstacle_blocked_direction)
            self._allowed_motion_components = all_motion_components - set(self._blocked_motion_components)
        if not self._obstacle_emergency_full_stop:
            self._gate_debug = (
                f"zones={','.join(sorted(self._blocked_zones, key=self._zone_sort_key)) or 'none'} "
                f"blocked={','.join(sorted(self._blocked_motion_components)) or 'none'} "
                f"allowed={','.join(sorted(self._allowed_motion_components)) or 'none'}"
            )

        if ttc_warning and (not self.obstacle_stop):
            if int(self.get_clock().now().nanoseconds / 1e9) % 2 == 0:
                self.get_logger().debug(
                    f'⚠️  TTC warning ({self.last_ttc_sec:.2f}s) '
                    f'cmd_vx={self.last_cmd_linear_x:.2f}m/s'
                )

    def _current_hard_stop_distance_m(self) -> float:
        mode = self._control_mode_normalized()
        if mode == 'manual' and (not self.manual_mode_same_obstacle_gate):
            return max(0.0, float(self.manual_critical_zone_m))
        return max(float(self.critical_zone), float(self.obstacle_hard_stop_distance_m))

    def _active_obstacle_distance(self, cmd_linear_x: float) -> float:
        if cmd_linear_x > self.ttc_min_speed_mps:
            return float(self.min_obstacle_distance_front)
        if cmd_linear_x < -self.ttc_min_speed_mps:
            return float(self.min_obstacle_distance_rear)
        return float('inf')

    def _compute_dynamic_speed_scale(self, cmd_linear_x: float) -> float:
        if not self.distance_safety_enabled:
            return 1.0
        if not self.obstacle_slowdown_enabled:
            return 1.0
        if abs(cmd_linear_x) < self.ttc_min_speed_mps:
            return 1.0

        nearest_distance = self._active_obstacle_distance(cmd_linear_x)
        min_scale = float(self.obstacle_slowdown_min_scale)

        distance_scale = 1.0
        if math.isfinite(nearest_distance):
            slowdown_start_distance = self.warning_zone + self.obstacle_slowdown_distance_buffer_m
            slowdown_stop_distance = self.critical_zone + max(0.01, self.obstacle_clear_margin_m * 0.5)
            distance_span = max(1e-3, slowdown_start_distance - slowdown_stop_distance)
            distance_ratio = (nearest_distance - slowdown_stop_distance) / distance_span
            distance_scale = max(min_scale, min(1.0, distance_ratio))

        ttc_scale = 1.0
        if math.isfinite(nearest_distance) and nearest_distance >= self.obstacle_ttc_min_distance_m:
            speed = max(abs(cmd_linear_x), self.ttc_min_speed_mps)
            ttc_value = nearest_distance / speed
            slowdown_start_ttc = self.ttc_warn_sec + self.obstacle_slowdown_ttc_buffer_sec
            ttc_span = max(1e-3, slowdown_start_ttc - self.ttc_stop_sec)
            ttc_ratio = (ttc_value - self.ttc_stop_sec) / ttc_span
            ttc_scale = max(min_scale, min(1.0, ttc_ratio))

        return max(min_scale, min(1.0, min(distance_scale, ttc_scale)))

    def _distance_hold_active(self) -> bool:
        return bool(self.distance_safety_enabled and self.obstacle_stop and (not self.manual_override))

    def _nav2_speed_limit_percent(self, cmd_linear_x: float) -> int:
        if not self._nav2_filter_handoff_enabled():
            return 100
        if self._distance_hold_active():
            return 100

        scale = self._compute_dynamic_speed_scale(float(cmd_linear_x))
        return max(1, min(100, int(round(scale * 100.0))))

    def _apply_dynamic_slowdown(self, msg: Twist) -> Twist:
        mode = str(self.control_mode or '').strip().lower()
        if self.obstacle_gate_strict_stop_only:
            self.last_speed_scale = 1.0
            return msg
        if self.manual_override or (not self.distance_safety_enabled) or mode == 'manual':
            self.last_speed_scale = 1.0
            return msg

        scale = self._compute_dynamic_speed_scale(float(msg.linear.x))
        self.last_speed_scale = scale
        if scale >= 0.999:
            return msg

        slowed = Twist()
        slowed.linear.x = float(msg.linear.x) * scale
        slowed.linear.y = float(msg.linear.y)
        slowed.linear.z = float(msg.linear.z)
        slowed.angular.x = float(msg.angular.x)
        slowed.angular.y = float(msg.angular.y)
        # Preserve turning authority to let upstream controller steer around obstacles.
        slowed.angular.z = float(msg.angular.z) * max(0.60, scale)

        if int(self.get_clock().now().nanoseconds / 1e9) % 2 == 0:
            self.get_logger().debug(
                f'⚠️  Dynamic slowdown scale={scale:.2f} '
                f'vx_in={msg.linear.x:.2f} vx_out={slowed.linear.x:.2f} '
                f'front={self.min_obstacle_distance_front:.2f} rear={self.min_obstacle_distance_rear:.2f}'
            )

        return slowed

    def _apply_lenient_zones(self):
        scale = float(self.lenient_scale)
        if scale <= 0.0:
            scale = 0.1
        elif scale > 1.0:
            scale = 1.0

        if self.lenient_mode:
            self.critical_zone = self.base_critical_zone * scale
            self.warning_zone = self.base_warning_zone * scale
            self.safe_zone = self.base_safe_zone * scale
        else:
            self.critical_zone = self.base_critical_zone
            self.warning_zone = self.base_warning_zone
            self.safe_zone = self.base_safe_zone

    def _set_lenient_mode(self, enabled, reason=''):
        enabled = bool(enabled)
        if enabled == self.lenient_mode:
            return
        self.lenient_mode = enabled
        self._apply_lenient_zones()
        suffix = f' ({reason})' if reason else ''
        if enabled:
            self.get_logger().warn(
                f'Safety leniency enabled{suffix}: '
                f'critical<{self.critical_zone:.2f}m, warning<{self.warning_zone:.2f}m'
            )
        else:
            self.get_logger().info(
                f'Safety leniency disabled{suffix}: '
                f'critical<{self.critical_zone:.2f}m, warning<{self.warning_zone:.2f}m'
            )

    def control_mode_callback(self, msg):
        mode = (msg.data or '').strip().lower()
        if not mode:
            return
        self.control_mode = mode
        # Default policy keeps the same gate in manual and autonomous modes.
        if mode == 'manual' and (not self.manual_mode_same_obstacle_gate):
            self.base_critical_zone = self.manual_critical_zone_m
        else:
            self.base_critical_zone = self.autonomous_critical_zone_m
        self._apply_lenient_zones()
    
    def pose_callback(self, msg):
        """Monitor localization confidence from AMCL"""
        # Calculate confidence from covariance (graph-SLAM proxy)
        cov = msg.pose.covariance
        self.localization_confidence = self._graph_slam_confidence_from_covariance(cov)
        
        # Localization stop policy: only stop at true zero confidence (or configured epsilon).
        zero_confidence = self.localization_confidence <= self.localization_zero_stop_epsilon

        if zero_confidence:
            if not self.low_confidence_stop and not self.manual_override:
                self.low_confidence_stop = True
                self.get_logger().warn(
                    f'⚠️  SAFETY: Localization confidence at zero ({self.localization_confidence:.1f}%) - stopping robot'
                )
        else:
            if self.low_confidence_stop:
                self.low_confidence_stop = False
                self.get_logger().info(
                    f'✓ Localization confidence above zero ({self.localization_confidence:.1f}%) - safety clear'
                )
    
    def scan_front_callback(self, msg):
        """Update FRONT lidar obstacle metrics and refresh the gate state."""
        if self._preferred_union_source_fresh():
            return
        min_dist, critical_count, warning_count, valid = self._vfh_classify_scan(msg, 'front')

        if valid and math.isfinite(min_dist):
            self._invalid_front_scan_frames = 0
            self.raw_obstacle_distance_front = float(min_dist)
            self.min_obstacle_distance_front = self._smooth_distance(self.min_obstacle_distance_front, min_dist)
            self.critical_obstacles_front = int(critical_count)
            self.warning_obstacles_front = int(warning_count)
        else:
            self._invalid_front_scan_frames += 1
            self.critical_obstacles_front = 0
            self.warning_obstacles_front = 0
            if self._invalid_front_scan_frames >= self.invalid_scan_clear_frames:
                self.raw_obstacle_distance_front = float('inf')
                self.min_obstacle_distance_front = float('inf')
        self._update_obstacle_state()
    
    def scan_rear_callback(self, msg):
        """Update REAR lidar obstacle metrics and refresh the gate state."""
        if self._preferred_union_source_fresh():
            return
        min_dist, critical_count, warning_count, valid = self._vfh_classify_scan(msg, 'rear')

        if valid and math.isfinite(min_dist):
            self._invalid_rear_scan_frames = 0
            self.raw_obstacle_distance_rear = float(min_dist)
            self.min_obstacle_distance_rear = self._smooth_distance(self.min_obstacle_distance_rear, min_dist)
            self.critical_obstacles_rear = int(critical_count)
            self.warning_obstacles_rear = int(warning_count)
        else:
            self._invalid_rear_scan_frames += 1
            self.critical_obstacles_rear = 0
            self.warning_obstacles_rear = 0
            if self._invalid_rear_scan_frames >= self.invalid_scan_clear_frames:
                self.raw_obstacle_distance_rear = float('inf')
                self.min_obstacle_distance_rear = float('inf')
        self._update_obstacle_state()

    def _current_stop_state(self):
        """Evaluate safety stop state and reasons in priority order."""
        should_stop = False
        reasons = []

        # Priority order:
        # 1) E-STOP (human override, cannot be bypassed)
        # 2) LiDAR/TTC obstacle risk (bypassable by manual override)
        # 3) Localization at zero confidence (bypassable by manual override)
        if self.estop_active:
            should_stop = True
            reasons = ['E-STOP']
        elif self._distance_hold_active() and (not self._manual_directional_gate_active()):
            should_stop = True
            reasons = [f'OBSTACLE({self._obstacle_reason})']
        elif self.low_confidence_stop and not self.manual_override:
            should_stop = True
            reasons = [f'LOW_CONFIDENCE({self.localization_confidence:.1f}%)']

        return should_stop, reasons

    def _effective_estop_state(self):
        """Hard operator E-STOP state only — localization invalidity is a separate concern.

        /set_estop must carry only human/operator hard-stop commands so that
        MissionManager and ZoneManager can distinguish operator intent from
        sensor-derived drive inhibits.  Localization invalidity is published
        separately on /safety/localization_invalid.
        """
        return bool(self.estop_active)
    
    def cmd_vel_input_callback(self, msg):
        """Forward velocity to hardware only when safety state allows motion."""
        self.last_cmd_linear_x = float(msg.linear.x)
        self.last_cmd_angular_z = float(msg.angular.z)
        if abs(self.last_cmd_linear_x) > self.ttc_min_speed_mps:
            self.last_nonzero_cmd_linear_x = float(self.last_cmd_linear_x)

        should_stop, _reasons = self._current_cmd_gate_state()
        if should_stop:
            self.last_speed_scale = 0.0
            self.safety_pub.publish(Twist())
            return

        if self._manual_directional_gate_active():
            gated_msg = self._apply_manual_directional_gate(msg)
            if abs(float(gated_msg.linear.x)) < self.ttc_min_speed_mps and abs(float(msg.linear.x)) > self.ttc_min_speed_mps:
                self.last_speed_scale = 0.0
            else:
                self.last_speed_scale = 1.0
            self.safety_pub.publish(gated_msg)
            return

        if self._nav2_filter_handoff_enabled():
            self.last_speed_scale = self._nav2_speed_limit_percent(float(msg.linear.x)) / 100.0
            self.safety_pub.publish(msg)
            return

        safe_msg = self._apply_dynamic_slowdown(msg)
        self.safety_pub.publish(safe_msg)
    
    def safety_check_callback(self):
        """Continuously enforce zero velocity while hard-stop gating is active.

        NOTE: _update_obstacle_state() is intentionally NOT called here.
        Obstacle state is updated in scan callbacks. Calling it here too would double-increment the
        stop/clear debounce frame counters on every timer tick, causing
        premature stop confirmation on a single scan frame.
        """
        should_stop, reasons = self._current_cmd_gate_state()

        if should_stop:
            twist = Twist()  # All zeros = stop
            self.safety_pub.publish(twist)

            # Log periodically (every 2 seconds)
            if int(self.get_clock().now().nanoseconds / 1e9) % 2 == 0:
                self.get_logger().debug(f'🛑 Safety stop active: {", ".join(reasons)}')
    
    def _publish_estop_state(self):
        """Continuously publish canonical safety states for system consumers.

        Topics:
          /set_estop              — hard operator E-STOP only
          /safety/localization_invalid — localization confidence at zero
          /safety/obstacle_stop_active — LiDAR/TTC obstacle stop
          /safety/override_active — manual override engaged
          /safety/nav2_keepout_active — obstacle stop exported to Nav2 costmap filters
          /safety/nav2_keepout_direction — direction for dynamic keepout overlay
          /safety/nav2_speed_limit_percent — slowdown exported to Nav2 SpeedFilter
        """
        estop_msg = Bool()
        estop_msg.data = self._effective_estop_state()
        self.set_estop_pub.publish(estop_msg)

        loc_invalid_msg = Bool()
        loc_invalid_msg.data = bool(self.low_confidence_stop and not self.manual_override)
        self.localization_invalid_pub.publish(loc_invalid_msg)

        obstacle_msg = Bool()
        obstacle_msg.data = self._distance_hold_active()
        self.obstacle_stop_pub.publish(obstacle_msg)

        override_msg = Bool()
        override_msg.data = bool(self.manual_override)
        self.override_state_pub.publish(override_msg)

        nav2_keepout_msg = Bool()
        nav2_keepout_msg.data = bool(self._nav2_filter_handoff_enabled() and self._distance_hold_active())
        self.nav2_keepout_pub.publish(nav2_keepout_msg)

        nav2_direction_msg = String()
        nav2_direction_msg.data = (
            self._obstacle_blocked_direction if nav2_keepout_msg.data else 'none'
        )
        self.nav2_keepout_direction_pub.publish(nav2_direction_msg)

        nav2_speed_msg = UInt8()
        nav2_speed_msg.data = int(self._nav2_speed_limit_percent(self.last_cmd_linear_x))
        self.nav2_speed_limit_pub.publish(nav2_speed_msg)
        self._publish_debug_markers()
    
    def estop_callback(self, request, response):
        """Service to activate/deactivate emergency stop."""
        self.estop_active = request.data

        # Immediately publish state change to /set_estop
        self._publish_estop_state()

        if self.estop_active:
            # Hard stop immediately on activation.
            self.safety_pub.publish(Twist())
            self.get_logger().error('🚨 EMERGENCY STOP ACTIVATED')
            response.success = True
            response.message = 'Emergency stop activated - robot stopped'
        else:
            should_stop, _ = self._current_stop_state()
            if should_stop:
                self.get_logger().warn('Manual E-STOP reset, but safety stop remains active')
                response.success = True
                response.message = (
                    'Manual E-STOP reset, but safety stop is still active '
                    '(obstacle/low-confidence)'
                )
            else:
                self.get_logger().info('✓ Emergency stop deactivated')
                response.success = True
                response.message = 'Emergency stop deactivated - robot can move'

        return response

    def clear_state_callback(self, request, response):
        """Force-clear latched non-critical safety states and publish STOP once."""
        del request

        # Clear all safety latches except persistent sensor inputs.
        self.estop_active = False
        self.low_confidence_stop = False
        self.obstacle_stop = False

        # Clear-state should return to normal guarded operation, not latch override.
        self.manual_override = False

        # Reset debounce/latch helpers.
        self._obstacle_stop_frames = 0
        self._obstacle_clear_frames = 0
        self._obstacle_reason = 'manual_clear_state'
        self._obstacle_blocked_direction = 'none'
        self._manual_blocked_direction = 'none'
        self.last_ttc_sec = float('inf')
        self._invalid_front_scan_frames = 0
        self._invalid_rear_scan_frames = 0
        self.last_speed_scale = 1.0

        # Publish current canonical state and one immediate zero command.
        self._publish_estop_state()
        self.safety_pub.publish(Twist())

        self.get_logger().warn(
            '⚠️  CLEAR_STATE invoked: estop/off, obstacle/off, low_conf/off, manual_override/off'
        )

        response.success = True
        response.message = 'Safety state cleared. Manual override disabled; normal safety checks active.'
        return response
        
    def override_callback(self, request, response):
        """Service to override safety checks (use with caution!)"""
        self.manual_override = request.data
        
        if self.manual_override:
            self.get_logger().warn('⚠️  SAFETY OVERRIDE ENABLED - non-E-STOP checks bypassed')
            self.low_confidence_stop = False
            response.success = True
            response.message = 'Safety override enabled - non-E-STOP checks bypassed'
        else:
            self.get_logger().info('✓ Safety override disabled - normal safety checks active')
            response.success = True
            response.message = 'Safety override disabled'
        
        return response
    
    def status_callback(self, request, response):
        """Service to get current safety status"""
        obstacle_source_label = (
            f'{self.obstacle_cloud_topic} -> {self.obstacle_scan_topic} -> /scan+/scan2'
            if self.obstacle_scan_topic
            else f'{self.obstacle_cloud_topic} -> /scan+/scan2'
        )

        status_lines = [
            f"Safety Controller Status:",
            f"  Hard Stop (/set_estop): {'ACTIVE' if self._effective_estop_state() else 'inactive'}",
            f"  Manual E-Stop Command: {'ACTIVE' if self.estop_active else 'inactive'}",
            f"  Localization Stop (zero-only): {'ACTIVE' if self.low_confidence_stop else 'inactive'}",
            f"  Obstacle Stop: {'ACTIVE' if self.obstacle_stop else 'inactive'}",
            f"  Manual Override: {'ENABLED' if self.manual_override else 'disabled'}",
            f"  Control Mode: {self.control_mode}",
            f"  Distance Safety: {'ENABLED' if self.distance_safety_enabled else 'disabled'}",
            f"  Distance Hold Active: {'ACTIVE' if self._distance_hold_active() else 'inactive'}",
            f"  Carry Shelf Profile: {'ACTIVE' if self.carried_profile_active else 'inactive'} "
            f"(L={self._effective_body_length_m():.2f}m W={self._effective_body_width_m():.2f}m)",
            f"  Nav2 Filter Bridge: {'READY' if self.nav2_filter_bridge_ready else 'fallback only'}",
            f"  Nav2 Handoff Active: {'YES' if self._nav2_filter_handoff_enabled() else 'no'}",
            f"  Gate Policy: {'STRICT stop-only' if self.obstacle_gate_strict_stop_only else 'slowdown + stop'}",
            f"  Manual Gate Policy: {'same as autonomous' if self.manual_mode_same_obstacle_gate else 'manual-specific'}",
            f"  Manual Directional Escape: {'ENABLED' if self.manual_directional_escape_enabled else 'disabled'}",
            f"  Obstacle Blocked Direction: {self._obstacle_blocked_direction}",
            f"  Manual Blocked Direction: {self._manual_blocked_direction}",
            f"  Lenient Mode: {'ENABLED' if self.lenient_mode else 'disabled'} (scale: {self.lenient_scale:.2f})",
            f"",
            f"Sensors:",
            f"  Localization: {self.localization_confidence:.1f}% (stop <= {self.localization_zero_stop_epsilon:.2f}%)",
            f"",
            f"Obstacle Detection (distance + TTC):",
            f"  Last cmd: vx={self.last_cmd_linear_x:.2f}m/s wz={self.last_cmd_angular_z:.2f}rad/s",
            f"  Dynamic slowdown scale: {self.last_speed_scale:.2f}",
            f"  Nav2 speed limit: {self._nav2_speed_limit_percent(self.last_cmd_linear_x)}%",
            f"  TTC stop/warn: <{self.ttc_stop_sec:.2f}s / <{self.ttc_warn_sec:.2f}s (last={self.last_ttc_sec:.2f}s)",
            f"  Hard-stop distance: {self._current_hard_stop_distance_m():.2f}m",
            f"  Emergency full-stop distance: {self.obstacle_emergency_stop_distance_m:.2f}m",
            f"  Command direction: {self._last_cmd_direction}",
            f"  Front obstacle present: {'YES' if self._front_obstacle_present else 'no'}",
            f"  Rear obstacle present: {'YES' if self._rear_obstacle_present else 'no'}",
            f"  Reverse gate: {'ALLOWED' if self._reverse_allowed else 'BLOCKED'} ({self._reverse_block_reason})",
            f"  Front sector (source {obstacle_source_label}): raw={self.raw_obstacle_distance_front:.2f}m, nearest={self.min_obstacle_distance_front:.2f}m, critical={self.critical_obstacles_front}, warning={self.warning_obstacles_front}",
            f"  Rear sector (source {obstacle_source_label}): raw={self.raw_obstacle_distance_rear:.2f}m, nearest={self.min_obstacle_distance_rear:.2f}m, critical={self.critical_obstacles_rear}, warning={self.warning_obstacles_rear}",
            f"  Blocked zones: {','.join(sorted(self._blocked_zones, key=self._zone_sort_key)) or 'none'}",
            f"  Front blocked zones: {','.join(sorted(self._front_blocked_zones, key=self._zone_sort_key)) or 'none'}",
            f"  Rear blocked zones: {','.join(sorted(self._rear_blocked_zones, key=self._zone_sort_key)) or 'none'}",
            f"  Blocked motion: {','.join(sorted(self._blocked_motion_components)) or 'none'}",
            f"  Allowed motion: {','.join(sorted(self._allowed_motion_components)) or 'none'}",
            f"  Directional gate: {self._gate_debug}",
            f"  Front closest point: {self._format_point_debug(self._front_obstacle_debug.get('closest'))}",
            f"  Front closest ignored: {self._format_point_debug(self._front_obstacle_debug.get('ignored'))}",
            f"  Rear closest point: {self._format_point_debug(self._rear_obstacle_debug.get('closest'))}",
            f"  Rear closest ignored: {self._format_point_debug(self._rear_obstacle_debug.get('ignored'))}",
            f"  Stop reason: {self._obstacle_reason}"
        ]
        
        response.success = True
        response.message = '\n'.join(status_lines)
        return response

    def _nav2_filter_bridge_ready_callback(self, msg: Bool):
        self.nav2_filter_bridge_ready = bool(msg.data)


def main(args=None):
    rclpy.init(args=args)
    node = SafetyController()
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
            pass  # Already shut down


if __name__ == '__main__':
    main()
