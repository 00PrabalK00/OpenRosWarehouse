import importlib.util
import math
import sys
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'shelf_docking.py'
MODULE_NAME = 'next2_shelf.shelf_docking'

spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
module = importlib.util.module_from_spec(spec)
sys.modules.setdefault(MODULE_NAME, module)
assert spec.loader is not None
spec.loader.exec_module(module)

Pose2D = module.Pose2D
RobotFootprint = module.RobotFootprint
ShelfGeometry = module.ShelfGeometry
ShelfDockingParameters = module.ShelfDockingParameters
ShelfDockingPlanError = module.ShelfDockingPlanError
assess_shelf_docking = module.assess_shelf_docking
build_shelf_navigation_targets = module.build_shelf_navigation_targets
build_shelf_docking_plan = module.build_shelf_docking_plan
pose_error_in_target_frame = module.pose_error_in_target_frame


def _make_params():
    return ShelfDockingParameters(
        standoff_distance=0.55,
        insertion_extra_standoff=0.10,
        local_insert_distance=0.50,
        approach_arrival_tolerance=0.20,
        entry_clearance_margin=0.04,
        opening_width_margin=0.04,
        position_tolerance=0.08,
        centerline_tolerance=0.05,
        centerline_slowdown_tolerance=0.10,
        heading_tolerance=0.10,
    )


def _make_footprint():
    return RobotFootprint(
        forward_extent=0.40,
        rear_extent=0.40,
        half_width=0.37,
    )


def test_build_shelf_docking_plan_offsets_and_guards():
    geometry = ShelfGeometry(
        final_pose=Pose2D(x=2.0, y=3.0, yaw=0.0, frame_id='map'),
        opening_line_pose=Pose2D(x=1.5, y=3.0, yaw=0.0, frame_id='map'),
        opening_width=1.10,
    )

    plan = build_shelf_docking_plan(geometry, _make_footprint(), _make_params())

    assert plan.entry_guard_distance == pytest.approx(0.44)
    assert plan.approach_offset == pytest.approx(0.65)
    assert plan.entry_pose.x == pytest.approx(1.06)
    assert plan.entry_pose.y == pytest.approx(3.0)
    assert plan.approach_pose.x == pytest.approx(0.85)
    assert plan.approach_pose.y == pytest.approx(3.0)
    assert plan.required_opening_width == pytest.approx(0.82)
    assert plan.usable_lateral_clearance == pytest.approx(0.14)
    assert plan.entry_lateral_tolerance == pytest.approx(0.05)
    assert plan.insert_lateral_tolerance == pytest.approx(0.05)


def test_build_shelf_docking_plan_rejects_narrow_opening():
    geometry = ShelfGeometry(
        final_pose=Pose2D(x=2.0, y=3.0, yaw=0.0, frame_id='map'),
        opening_line_pose=Pose2D(x=1.5, y=3.0, yaw=0.0, frame_id='map'),
        opening_width=0.80,
    )

    with pytest.raises(ShelfDockingPlanError, match='too narrow'):
        build_shelf_docking_plan(geometry, _make_footprint(), _make_params())


def test_pose_error_in_target_frame_uses_target_axes():
    robot_pose = Pose2D(x=0.0, y=0.0, yaw=0.0, frame_id='map')
    target_pose = Pose2D(x=1.0, y=1.0, yaw=math.pi / 2.0, frame_id='map')

    error = pose_error_in_target_frame(robot_pose, target_pose)

    assert error.distance == pytest.approx(math.sqrt(2.0))
    assert error.forward_error == pytest.approx(1.0)
    assert error.left_error == pytest.approx(-1.0)
    assert error.heading_error == pytest.approx(math.pi / 2.0)


def test_assess_shelf_docking_requests_approach_when_far():
    geometry = ShelfGeometry(
        final_pose=Pose2D(x=2.0, y=3.0, yaw=0.0, frame_id='map'),
        opening_line_pose=Pose2D(x=1.5, y=3.0, yaw=0.0, frame_id='map'),
        opening_width=1.10,
    )
    plan = build_shelf_docking_plan(geometry, _make_footprint(), _make_params())
    robot_pose = Pose2D(x=0.0, y=3.0, yaw=0.0, frame_id='map')

    assessment = assess_shelf_docking(
        robot_pose,
        plan,
        local_insert_distance=0.50,
        approach_arrival_tolerance=0.20,
    )

    assert assessment.should_run_approach is True
    assert assessment.should_run_entry_navigation is True


def test_assess_shelf_docking_skips_nav_when_staged_at_entry():
    geometry = ShelfGeometry(
        final_pose=Pose2D(x=2.0, y=3.0, yaw=0.0, frame_id='map'),
        opening_line_pose=Pose2D(x=1.5, y=3.0, yaw=0.0, frame_id='map'),
        opening_width=1.10,
    )
    plan = build_shelf_docking_plan(geometry, _make_footprint(), _make_params())
    robot_pose = Pose2D(
        x=plan.entry_pose.x,
        y=plan.entry_pose.y,
        yaw=plan.entry_pose.yaw,
        frame_id='map',
    )

    assessment = assess_shelf_docking(
        robot_pose,
        plan,
        local_insert_distance=0.50,
        approach_arrival_tolerance=0.20,
    )

    assert assessment.should_run_approach is False
    assert assessment.should_run_entry_navigation is False


def test_build_shelf_navigation_targets_uses_safe_corridor_when_offset():
    geometry = ShelfGeometry(
        final_pose=Pose2D(x=2.0, y=3.0, yaw=0.0, frame_id='map'),
        opening_line_pose=Pose2D(x=1.5, y=3.0, yaw=0.0, frame_id='map'),
        opening_width=1.10,
    )
    plan = build_shelf_docking_plan(geometry, _make_footprint(), _make_params())
    robot_pose = Pose2D(x=1.35, y=3.24, yaw=0.0, frame_id='map')

    targets = build_shelf_navigation_targets(robot_pose, plan)

    assert [target.label for target in targets] == [
        'Shelf retreat',
        'Shelf lineup',
        'Shelf entry alignment',
    ]
    assert targets[0].pose.x == pytest.approx(plan.approach_pose.x)
    assert targets[0].pose.y == pytest.approx(robot_pose.y)
    assert targets[1].pose.x == pytest.approx(plan.approach_pose.x)
    assert targets[1].pose.y == pytest.approx(plan.approach_pose.y)
    assert targets[2].pose.x == pytest.approx(plan.entry_pose.x)
    assert targets[2].pose.y == pytest.approx(plan.entry_pose.y)


def test_build_shelf_navigation_targets_empty_when_at_entry():
    geometry = ShelfGeometry(
        final_pose=Pose2D(x=2.0, y=3.0, yaw=0.0, frame_id='map'),
        opening_line_pose=Pose2D(x=1.5, y=3.0, yaw=0.0, frame_id='map'),
        opening_width=1.10,
    )
    plan = build_shelf_docking_plan(geometry, _make_footprint(), _make_params())
    robot_pose = Pose2D(
        x=plan.entry_pose.x,
        y=plan.entry_pose.y,
        yaw=plan.entry_pose.yaw,
        frame_id='map',
    )

    targets = build_shelf_navigation_targets(robot_pose, plan)

    assert targets == []
