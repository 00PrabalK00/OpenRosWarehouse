import math
import sys
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parents[1] / 'src'
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from docking_contracts import CommittedDockFrame, DockingAbortCategory, ShelfPerceptionEstimate  # noqa: E402
from docking_insertion_controller import (  # noqa: E402
    FrozenInsertionController,
    FrozenInsertionControllerConfig,
)


def _frame() -> CommittedDockFrame:
    return CommittedDockFrame(
        frame_id='map',
        mouth_center_x=10.0,
        mouth_center_y=5.0,
        entry_x=9.6,
        entry_y=5.0,
        entry_yaw=0.0,
        final_x=10.35,
        final_y=5.0,
        final_yaw=0.0,
        opening_width_m=0.70,
        expected_insertion_depth_m=0.35,
        left_clearance_m=0.03,
        right_clearance_m=0.03,
        committed_at_sec=1.0,
        valid_until_sec=4.0,
        perception=ShelfPerceptionEstimate(
            x=10.0,
            y=5.0,
            yaw=0.0,
            frame_id='map',
            stamp_sec=1.0,
            hotspot_count=4,
        ),
    )


def test_compute_dock_frame_errors_uses_mouth_center_as_origin():
    controller = FrozenInsertionController()
    errors = controller.compute_dock_frame_errors(
        _frame(),
        robot_x=10.10,
        robot_y=5.02,
        robot_yaw=math.radians(1.0),
    )
    assert abs(errors.progress_along_dock_m - 0.10) < 1e-6
    assert abs(errors.lateral_error_m - 0.02) < 1e-6
    assert abs(errors.heading_error_rad - math.radians(-1.0)) < 1e-6
    assert abs(errors.remaining_distance_m - 0.25) < 1e-6


def test_abort_happens_before_nonzero_command_when_lateral_error_is_bad():
    controller = FrozenInsertionController()
    output = controller.compute_from_pose(
        _frame(),
        robot_x=10.05,
        robot_y=5.05,
        robot_yaw=0.0,
        perception_confidence=0.95,
        now_sec=1.0,
    )
    assert output.stop is True
    assert output.linear_speed_mps == 0.0
    assert output.angular_speed_rad_s == 0.0
    assert output.abort_category == DockingAbortCategory.MID_INSERT_DRIFT


def test_heading_only_mode_uses_tighter_angular_cap():
    controller = FrozenInsertionController(
        FrozenInsertionControllerConfig(
            insert_speed_mps=0.06,
            min_insert_speed_mps=0.03,
            max_angular_speed_rad_s=0.08,
            heading_only_lateral_threshold_m=0.01,
            heading_only_max_angular_speed_rad_s=0.04,
            heading_gain=2.5,
            lateral_gain=3.0,
        )
    )
    output = controller.compute_from_pose(
        _frame(),
        robot_x=10.05,
        robot_y=5.001,
        robot_yaw=math.radians(-3.0),
        perception_confidence=0.95,
        now_sec=1.0,
    )
    assert output.stop is False
    assert abs(output.raw_angular_speed_rad_s) <= 0.04 + 1e-6


def test_linear_command_is_jerk_limited_toward_insert_speed():
    controller = FrozenInsertionController(
        FrozenInsertionControllerConfig(
            insert_speed_mps=0.08,
            min_insert_speed_mps=0.03,
            max_linear_accel_mps2=0.10,
            max_linear_jerk_mps3=0.20,
        )
    )
    first = controller.compute_from_pose(
        _frame(),
        robot_x=10.02,
        robot_y=5.0,
        robot_yaw=0.0,
        perception_confidence=0.95,
        now_sec=1.0,
    )
    second = controller.compute_from_pose(
        _frame(),
        robot_x=10.04,
        robot_y=5.0,
        robot_yaw=0.0,
        perception_confidence=0.95,
        now_sec=1.1,
    )
    assert first.stop is False
    assert second.stop is False
    assert second.linear_speed_mps >= first.linear_speed_mps
    assert second.linear_speed_mps <= 0.08 + 1e-6


def test_target_depth_reached_requests_verify_instead_of_declaring_success():
    controller = FrozenInsertionController(
        FrozenInsertionControllerConfig(
            insert_speed_mps=0.06,
            stop_distance_m=0.01,
        )
    )
    output = controller.compute_from_pose(
        _frame(),
        robot_x=10.34,
        robot_y=5.0,
        robot_yaw=0.0,
        perception_confidence=0.95,
        now_sec=1.0,
    )
    assert output.stop is True
    assert output.success is False
    assert output.target_depth_reached is True
    assert output.reason == 'target_depth_reached_verify_required'


def test_low_confidence_is_ignored_once_robot_is_deep_in_insert():
    controller = FrozenInsertionController(
        FrozenInsertionControllerConfig(
            insert_speed_mps=0.06,
            min_perception_confidence=0.80,
            perception_confidence_ignore_after_progress_m=0.10,
            perception_confidence_ignore_inside_remaining_m=0.08,
        )
    )
    output = controller.compute_from_pose(
        _frame(),
        robot_x=10.28,
        robot_y=5.0,
        robot_yaw=0.0,
        perception_confidence=0.10,
        now_sec=1.0,
    )
    assert output.stop is False


def test_forward_pose_jump_is_tolerated_when_lateral_jump_is_small():
    controller = FrozenInsertionController(
        FrozenInsertionControllerConfig(
            max_pose_jump_m=0.02,
            max_pose_jump_lateral_m=0.015,
            max_pose_jump_backward_m=0.03,
        )
    )
    output = controller.compute_from_pose(
        _frame(),
        robot_x=10.20,
        robot_y=5.0,
        robot_yaw=0.0,
        perception_confidence=0.95,
        pose_jump_m=0.04,
        pose_jump_forward_m=0.04,
        pose_jump_lateral_m=0.003,
        now_sec=1.0,
    )
    assert output.stop is False


def test_lateral_pose_jump_still_aborts_insert():
    controller = FrozenInsertionController(
        FrozenInsertionControllerConfig(
            max_pose_jump_m=0.05,
            max_pose_jump_lateral_m=0.015,
        )
    )
    output = controller.compute_from_pose(
        _frame(),
        robot_x=10.20,
        robot_y=5.0,
        robot_yaw=0.0,
        perception_confidence=0.95,
        pose_jump_m=0.025,
        pose_jump_forward_m=0.010,
        pose_jump_lateral_m=0.020,
        now_sec=1.0,
    )
    assert output.stop is True
    assert output.reason == 'pose_jump_lateral_exceeded'


def test_braking_zone_tapers_below_min_insert_speed_floor():
    controller = FrozenInsertionController(
        FrozenInsertionControllerConfig(
            insert_speed_mps=0.08,
            min_insert_speed_mps=0.03,
            braking_distance_m=0.08,
        )
    )
    output = controller.compute_from_pose(
        _frame(),
        robot_x=10.33,
        robot_y=5.0,
        robot_yaw=0.0,
        perception_confidence=0.95,
        now_sec=1.0,
    )
    assert output.stop is False
    assert 0.0 <= output.raw_linear_speed_mps < 0.03
