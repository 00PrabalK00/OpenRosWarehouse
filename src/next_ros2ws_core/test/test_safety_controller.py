import math
import sys
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parents[1] / 'src'
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from safety_controller import SafetyController  # noqa: E402


class _Scan:
    def __init__(self, ranges, angle_min=0.0, angle_increment=0.0, range_min=0.05, range_max=40.0):
        self.ranges = list(ranges)
        self.angle_min = angle_min
        self.angle_increment = angle_increment
        self.range_min = range_min
        self.range_max = range_max
        self.header = type('Header', (), {'frame_id': 'laser_test_frame'})()


class _Logger:
    def debug(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def warn(self, *_args, **_kwargs):
        return None


class _Now:
    nanoseconds = 0


class _Clock:
    def now(self):
        return _Now()


def _make_controller():
    controller = SafetyController.__new__(SafetyController)
    controller.estop_active = False
    controller.low_confidence_stop = False
    controller.manual_override = False
    controller.obstacle_stop = False
    controller.control_mode = 'auto'
    controller.nav2_filter_bridge_ready = True
    controller.distance_safety_enabled = True
    controller.manual_directional_escape_enabled = True
    controller.manual_mode_same_obstacle_gate = True
    controller._manual_blocked_direction = 'none'
    controller._obstacle_blocked_direction = 'none'
    controller._obstacle_reason = 'none'
    controller._blocked_zones = set()
    controller._front_blocked_zones = set()
    controller._rear_blocked_zones = set()
    controller._blocked_motion_components = set()
    controller._allowed_motion_components = {'forward', 'reverse', 'left_turn', 'right_turn'}
    controller._obstacle_emergency_full_stop = False
    controller._gate_debug = 'clear'
    controller.last_cmd_linear_x = 0.0
    controller.last_nonzero_cmd_linear_x = 0.0
    controller.ttc_min_speed_mps = 0.05
    controller.ttc_stop_sec = 1.1
    controller.ttc_warn_sec = 1.8
    controller.obstacle_ttc_min_distance_m = 0.5
    controller.obstacle_use_directional_blocking = True
    controller.obstacle_emergency_stop_distance_m = 0.08
    controller.obstacle_valid_min_range = 0.05
    controller.obstacle_min_points = 4
    controller.critical_zone = 0.22
    controller.warning_zone = 0.6
    controller.obstacle_clear_margin_m = 0.15
    controller.obstacle_stop_confirm_frames = 2
    controller.obstacle_clear_confirm_frames = 3
    controller.robot_body_length_m = 0.80
    controller.robot_body_width_m = 0.72
    controller.obstacle_body_padding_m = 0.06
    controller.obstacle_body_exclusion_radius_m = 0.22
    controller.carried_profile_active = False
    controller.carried_body_length_m = 0.80
    controller.carried_body_width_m = 0.72
    controller.carried_body_padding_m = 0.06
    controller.carried_body_exclusion_radius_m = 0.22
    controller.raw_scan_self_echo_range_m = 0.18
    controller.raw_scan_opening_half_angle_deg = 42.0
    controller.safety_debug_enabled = False
    controller.robot_base_frame = 'base_footprint'
    controller.raw_obstacle_distance_front = float('inf')
    controller.raw_obstacle_distance_rear = float('inf')
    controller.min_obstacle_distance_front = float('inf')
    controller.min_obstacle_distance_rear = float('inf')
    controller.critical_obstacles_front = 0
    controller.critical_obstacles_rear = 0
    controller.warning_obstacles_front = 0
    controller.warning_obstacles_rear = 0
    controller._obstacle_stop_frames = 0
    controller._obstacle_clear_frames = 0
    controller.last_ttc_sec = float('inf')
    controller._current_hard_stop_distance_m = lambda: 0.5
    controller.get_logger = lambda: _Logger()
    controller.get_clock = lambda: _Clock()
    controller._front_obstacle_debug = controller._make_obstacle_debug_state('front')
    controller._rear_obstacle_debug = controller._make_obstacle_debug_state('rear')
    return controller


def test_cmd_gate_keeps_hard_stop_active_even_with_nav2_handoff_ready():
    controller = _make_controller()
    controller.obstacle_stop = True
    controller._obstacle_reason = 'front_critical=4'
    controller._obstacle_emergency_full_stop = True

    should_stop, reasons = controller._current_cmd_gate_state()

    assert should_stop is True
    assert reasons == ['OBSTACLE(front_critical=4)']


def test_cmd_gate_allows_directional_gating_without_full_stop():
    controller = _make_controller()
    controller.obstacle_stop = True
    controller._obstacle_reason = 'front_left_leg'
    controller._blocked_motion_components = {'forward', 'left_turn'}
    controller._allowed_motion_components = {'reverse', 'right_turn'}

    should_stop, reasons = controller._current_cmd_gate_state()

    assert should_stop is False
    assert reasons == []


def test_stationary_robot_still_arms_front_obstacle_stop():
    controller = _make_controller()
    controller.critical_obstacles_front = 4
    controller.min_obstacle_distance_front = 0.20
    controller.raw_obstacle_distance_front = 0.80

    should_trigger, clear_ready, _reason, _ttc_warning, immediate_trigger, details = (
        controller._evaluate_obstacle_trigger()
    )

    assert should_trigger is True
    assert clear_ready is False
    assert immediate_trigger is False
    assert details['front_trigger'] is True
    assert details['front_active'] is True
    assert details['rear_active'] is False
    assert math.isinf(controller.last_ttc_sec)


def test_stationary_robot_hard_stops_for_close_front_object():
    controller = _make_controller()
    controller.raw_obstacle_distance_front = 0.35
    controller.min_obstacle_distance_front = 0.35

    should_trigger, clear_ready, reason, _ttc_warning, immediate_trigger, details = (
        controller._evaluate_obstacle_trigger()
    )

    assert should_trigger is True
    assert clear_ready is False
    assert immediate_trigger is True
    assert reason == 'front_hard_stop=0.35m'
    assert details['front_hard_stop'] is True
    assert details['rear_hard_stop'] is False


def test_front_raw_scan_uses_tf_yaw_for_front_sector():
    controller = _make_controller()
    controller.obstacle_forward_half_angle_rad = math.radians(70.0)
    controller.obstacle_rear_half_angle_rad = math.radians(70.0)
    controller.critical_zone = 0.5
    controller._transform_msg_metadata_to_base = lambda _msg: (0.0, 0.0, math.pi / 2.0)
    controller._footprint_contains_point = lambda *_args, **_kwargs: False
    controller._obstacle_clearance_m = lambda x, y: math.hypot(x, y)

    min_dist, critical_count, warning_count, valid = controller._vfh_classify_scan(
        _Scan([0.35], angle_min=-(math.pi / 2.0), angle_increment=0.0),
        'front',
    )

    assert valid is True
    assert min_dist <= 0.35
    assert critical_count == 1
    assert warning_count == 0


def test_rear_raw_scan_uses_tf_yaw_for_rear_sector():
    controller = _make_controller()
    controller.obstacle_forward_half_angle_rad = math.radians(70.0)
    controller.obstacle_rear_half_angle_rad = math.radians(70.0)
    controller.critical_zone = 0.5
    controller._transform_msg_metadata_to_base = lambda _msg: (0.0, 0.0, -(math.pi / 2.0))
    controller._footprint_contains_point = lambda *_args, **_kwargs: False
    controller._obstacle_clearance_m = lambda x, y: math.hypot(x, y)

    min_dist, critical_count, warning_count, valid = controller._vfh_classify_scan(
        _Scan([0.35], angle_min=-(math.pi / 2.0), angle_increment=0.0),
        'rear',
    )

    assert valid is True
    assert min_dist <= 0.35
    assert critical_count == 1
    assert warning_count == 0


def test_front_sector_rejects_points_outside_configured_half_angle():
    controller = _make_controller()
    controller.obstacle_forward_half_angle_rad = math.radians(70.0)
    controller.critical_zone = 0.5
    controller._transform_msg_metadata_to_base = lambda _msg: (0.0, 0.0, 0.0)
    controller._footprint_contains_point = lambda *_args, **_kwargs: False
    controller._obstacle_clearance_m = lambda x, y: math.hypot(x, y)

    inside_scan = _Scan(
        [0.4, 0.4, 0.4],
        angle_min=math.radians(-5.0),
        angle_increment=math.radians(5.0),
    )
    outside_scan = _Scan(
        [0.4, 0.4, 0.4, 0.4],
        angle_min=math.radians(75.0),
        angle_increment=math.radians(4.0),
    )

    _min_inside, inside_critical, _inside_warning, inside_valid = controller._vfh_classify_scan(
        inside_scan,
        'front',
    )
    _min_outside, outside_critical, _outside_warning, outside_valid = controller._vfh_classify_scan(
        outside_scan,
        'front',
    )

    assert inside_valid is True
    assert inside_critical == 3
    assert outside_valid is False
    assert outside_critical == 0


def test_single_raw_scan_point_inside_stop_distance_triggers_immediately():
    controller = _make_controller()
    controller.obstacle_forward_half_angle_rad = math.radians(70.0)
    controller.critical_zone = 0.5
    controller._transform_msg_metadata_to_base = lambda _msg: (0.0, 0.0, 0.0)
    controller._footprint_contains_point = lambda *_args, **_kwargs: False
    controller._obstacle_clearance_m = lambda x, y: math.hypot(x, y)

    min_dist, critical_count, _warning_count, valid = controller._vfh_classify_scan(
        _Scan(
            [0.35, 0.35, 0.35],
            angle_min=math.radians(-6.0),
            angle_increment=math.radians(6.0),
        ),
        'front',
    )

    assert valid is True
    assert min_dist <= 0.35
    assert critical_count == 3


def test_points_inside_robot_footprint_are_ignored():
    controller = _make_controller()
    controller.obstacle_forward_half_angle_rad = math.radians(70.0)
    assert controller._is_body_echo_point(0.10, 0.0) is True
    assert controller._is_body_echo_point(0.39, 0.0) is True
    assert controller._is_body_echo_point(0.46, 0.0) is False


def test_body_echo_filter_keeps_close_front_obstacle_near_bumper():
    controller = _make_controller()
    controller._transform_msg_metadata_to_base = lambda _msg: (0.0, 0.0, 0.0)
    controller.obstacle_forward_half_angle_rad = math.radians(70.0)

    min_dist, critical_count, _warning_count, valid = controller._vfh_classify_scan(
        _Scan([0.46], angle_min=0.0, angle_increment=0.0),
        'front',
    )

    assert valid is True
    assert min_dist <= 0.06
    assert critical_count == 1


def test_manual_stop_blocks_forward_but_keeps_backward_escape():
    controller = _make_controller()
    controller._blocked_motion_components = {'forward', 'left_turn'}
    controller._allowed_motion_components = {'reverse', 'right_turn'}

    msg = type('TwistLike', (), {})()
    msg.linear = type('Linear', (), {'x': 0.4, 'y': 0.0, 'z': 0.0})()
    msg.angular = type('Angular', (), {'x': 0.0, 'y': 0.0, 'z': 0.3})()

    gated = controller._apply_manual_directional_gate(msg)

    assert gated.linear.x == 0.0
    assert gated.angular.z == 0.0


def test_manual_stop_blocks_backward_but_keeps_forward_escape():
    controller = _make_controller()
    controller._blocked_motion_components = {'reverse'}
    controller._allowed_motion_components = {'forward', 'left_turn', 'right_turn'}

    msg = type('TwistLike', (), {})()
    msg.linear = type('Linear', (), {'x': -0.4, 'y': 0.0, 'z': 0.0})()
    msg.angular = type('Angular', (), {'x': 0.0, 'y': 0.0, 'z': -0.3})()

    gated = controller._apply_manual_directional_gate(msg)

    assert gated.linear.x == 0.0
    assert gated.angular.z == -0.3


def test_zone_block_mapping_for_front_left():
    controller = _make_controller()

    blocked = controller._blocked_motion_from_zones({'NW'})

    assert blocked == {'forward', 'left_turn'}


def test_zone_block_mapping_for_right_side_only():
    controller = _make_controller()

    blocked = controller._blocked_motion_from_zones({'E'})

    assert blocked == {'right_turn'}


def test_latched_front_stop_does_not_broaden_to_both_while_waiting_to_clear():
    controller = _make_controller()
    controller.control_mode = 'manual'
    controller.raw_obstacle_distance_front = 0.30
    controller.min_obstacle_distance_front = 0.30

    controller._update_obstacle_state()
    assert controller.obstacle_stop is True
    assert controller._manual_blocked_direction == 'forward'

    controller.raw_obstacle_distance_front = float('inf')
    controller.min_obstacle_distance_front = controller.critical_zone + 0.05
    controller.critical_obstacles_front = 0

    controller._update_obstacle_state()

    assert controller.obstacle_stop is True
    assert controller._obstacle_blocked_direction == 'forward'
    assert controller._manual_blocked_direction == 'forward'


def test_latched_front_stop_keeps_reverse_escape_open():
    controller = _make_controller()
    controller.control_mode = 'manual'
    controller.raw_obstacle_distance_front = 0.30
    controller.min_obstacle_distance_front = 0.30

    controller._update_obstacle_state()
    assert controller.obstacle_stop is True
    assert controller._blocked_motion_components == {'forward'}

    controller.raw_obstacle_distance_front = float('inf')
    controller.min_obstacle_distance_front = controller.critical_zone + 0.05
    controller.critical_obstacles_front = 0

    controller._update_obstacle_state()

    assert controller.obstacle_stop is True
    assert controller._blocked_motion_components == {'forward'}
    assert 'reverse' not in controller._blocked_motion_components

    msg = type('TwistLike', (), {})()
    msg.linear = type('Linear', (), {'x': -0.4, 'y': 0.0, 'z': 0.0})()
    msg.angular = type('Angular', (), {'x': 0.0, 'y': 0.0, 'z': 0.0})()

    gated = controller._apply_manual_directional_gate(msg)

    assert gated.linear.x == -0.4
    assert controller._reverse_allowed is True
    assert controller._reverse_block_reason.startswith('rear_clear')


def test_front_only_close_obstacle_does_not_force_global_full_stop():
    controller = _make_controller()
    controller._front_obstacle_debug['blocked_zones'] = {'N'}
    controller._front_obstacle_debug['closest'] = {
        'source': 'raw_scan',
        'frame_id': 'front_lidar',
        'x_m': 0.28,
        'y_m': 0.0,
        'distance_m': 0.28,
        'clearance_m': 0.02,
        'reason': '',
    }
    controller._rear_obstacle_debug['blocked_zones'] = set()
    controller._rear_obstacle_debug['closest'] = None
    controller._evaluate_obstacle_trigger = lambda: (
        True,
        False,
        'front_hard_stop=0.28m',
        False,
        True,
        {'front_active': True, 'rear_active': False, 'front_hard_stop': True, 'rear_hard_stop': False},
    )

    controller._update_obstacle_state()

    assert controller._obstacle_emergency_full_stop is False
    assert controller._blocked_motion_components == {'forward'}
    assert controller._allowed_motion_components == {'reverse', 'left_turn', 'right_turn'}
