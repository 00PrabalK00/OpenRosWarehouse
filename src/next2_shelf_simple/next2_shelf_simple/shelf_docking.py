import math
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Pose2D:
    x: float
    y: float
    yaw: float
    frame_id: str = 'map'


@dataclass(frozen=True)
class PoseError:
    distance: float
    forward_error: float
    left_error: float
    heading_error: float
    dx: float
    dy: float


@dataclass(frozen=True)
class RobotFootprint:
    forward_extent: float
    rear_extent: float
    half_width: float

    @property
    def width(self) -> float:
        return 2.0 * float(self.half_width)


@dataclass(frozen=True)
class ShelfGeometry:
    final_pose: Pose2D
    opening_line_pose: Pose2D
    opening_width: Optional[float]


@dataclass(frozen=True)
class ShelfDockingParameters:
    standoff_distance: float
    insertion_extra_standoff: float
    local_insert_distance: float
    approach_arrival_tolerance: float
    entry_clearance_margin: float
    opening_width_margin: float
    position_tolerance: float
    centerline_tolerance: float
    centerline_slowdown_tolerance: float
    heading_tolerance: float


@dataclass(frozen=True)
class ShelfDockingPlan:
    final_pose: Pose2D
    opening_line_pose: Pose2D
    entry_pose: Pose2D
    approach_pose: Pose2D
    opening_width: Optional[float]
    required_opening_width: float
    usable_lateral_clearance: Optional[float]
    entry_guard_distance: float
    approach_offset: float
    position_tolerance: float
    heading_tolerance: float
    entry_lateral_tolerance: float
    insert_lateral_tolerance: float
    centerline_slowdown_tolerance: float


@dataclass(frozen=True)
class ShelfDockingAssessment:
    robot_to_approach: float
    robot_to_entry: float
    entry_error: PoseError
    final_error: PoseError
    should_run_approach: bool
    should_run_entry_navigation: bool


@dataclass(frozen=True)
class ShelfNavigationTarget:
    label: str
    pose: Pose2D


class ShelfDockingPlanError(ValueError):
    pass


def _normalize_angle(angle: float) -> float:
    wrapped = float(angle)
    while wrapped > math.pi:
        wrapped -= 2.0 * math.pi
    while wrapped < -math.pi:
        wrapped += 2.0 * math.pi
    return wrapped


def shift_along_heading(pose: Pose2D, distance: float) -> Pose2D:
    heading_x = math.cos(float(pose.yaw))
    heading_y = math.sin(float(pose.yaw))
    return Pose2D(
        x=float(pose.x) + (heading_x * float(distance)),
        y=float(pose.y) + (heading_y * float(distance)),
        yaw=float(pose.yaw),
        frame_id=str(pose.frame_id or 'map') or 'map',
    )


def offset_pose_in_reference_frame(
    reference_pose: Pose2D,
    *,
    forward_offset: float,
    left_offset: float,
) -> Pose2D:
    heading_x = math.cos(float(reference_pose.yaw))
    heading_y = math.sin(float(reference_pose.yaw))
    left_x = -heading_y
    left_y = heading_x
    return Pose2D(
        x=float(reference_pose.x) + (heading_x * float(forward_offset)) + (left_x * float(left_offset)),
        y=float(reference_pose.y) + (heading_y * float(forward_offset)) + (left_y * float(left_offset)),
        yaw=float(reference_pose.yaw),
        frame_id=str(reference_pose.frame_id or 'map') or 'map',
    )


def pose_error_in_target_frame(robot_pose: Pose2D, target_pose: Pose2D) -> PoseError:
    if str(robot_pose.frame_id or 'map') != str(target_pose.frame_id or 'map'):
        raise ShelfDockingPlanError(
            f'Pose frame mismatch: robot={robot_pose.frame_id} target={target_pose.frame_id}'
        )

    dx = float(target_pose.x) - float(robot_pose.x)
    dy = float(target_pose.y) - float(robot_pose.y)
    target_forward_x = math.cos(float(target_pose.yaw))
    target_forward_y = math.sin(float(target_pose.yaw))
    target_left_x = -target_forward_y
    target_left_y = target_forward_x

    forward_error = float((dx * target_forward_x) + (dy * target_forward_y))
    left_error = float((dx * target_left_x) + (dy * target_left_y))
    heading_error = _normalize_angle(float(target_pose.yaw) - float(robot_pose.yaw))

    return PoseError(
        distance=float(math.hypot(dx, dy)),
        forward_error=forward_error,
        left_error=left_error,
        heading_error=heading_error,
        dx=dx,
        dy=dy,
    )


def build_shelf_docking_plan(
    geometry: ShelfGeometry,
    footprint: RobotFootprint,
    params: ShelfDockingParameters,
) -> ShelfDockingPlan:
    frame_id = str(geometry.final_pose.frame_id or 'map') or 'map'
    if str(geometry.opening_line_pose.frame_id or frame_id) != frame_id:
        raise ShelfDockingPlanError(
            f'Shelf frame mismatch: final={frame_id} opening={geometry.opening_line_pose.frame_id}'
        )

    final_pose = Pose2D(
        x=float(geometry.final_pose.x),
        y=float(geometry.final_pose.y),
        yaw=float(geometry.final_pose.yaw),
        frame_id=frame_id,
    )
    opening_line_pose = Pose2D(
        x=float(geometry.opening_line_pose.x),
        y=float(geometry.opening_line_pose.y),
        yaw=float(final_pose.yaw),
        frame_id=frame_id,
    )

    required_opening_width = (
        float(footprint.width)
        + (2.0 * float(params.opening_width_margin))
    )
    opening_width = None
    if geometry.opening_width is not None:
        opening_width = float(geometry.opening_width)
        if opening_width < required_opening_width:
            raise ShelfDockingPlanError(
                'Shelf opening too narrow for robot footprint: '
                f'opening={opening_width:.2f}m required>={required_opening_width:.2f}m.'
            )

    entry_guard_distance = max(
        0.02,
        float(footprint.forward_extent) + float(params.entry_clearance_margin),
    )
    entry_pose = shift_along_heading(opening_line_pose, -entry_guard_distance)

    total_standoff = (
        float(params.standoff_distance)
        + float(params.insertion_extra_standoff)
    )
    approach_offset = max(entry_guard_distance, total_standoff)
    approach_pose = shift_along_heading(opening_line_pose, -approach_offset)

    usable_lateral_clearance = None
    entry_lateral_tolerance = max(0.01, float(params.centerline_tolerance))
    insert_lateral_tolerance = min(
        entry_lateral_tolerance,
        max(0.01, float(params.centerline_tolerance) * 0.75),
    )
    if opening_width is not None:
        side_clearance = 0.5 * (opening_width - float(footprint.width))
        usable_lateral_clearance = (
            side_clearance - float(params.opening_width_margin)
        )
        if usable_lateral_clearance <= 0.0:
            raise ShelfDockingPlanError(
                'Shelf opening leaves no usable centering tolerance after clearance margin: '
                f'opening={opening_width:.2f}m robot_width={footprint.width:.2f}m '
                f'margin={params.opening_width_margin:.2f}m.'
            )
        entry_lateral_tolerance = min(
            float(params.centerline_tolerance),
            usable_lateral_clearance,
        )
        insert_lateral_tolerance = min(
            entry_lateral_tolerance,
            usable_lateral_clearance * 0.75,
        )
        if insert_lateral_tolerance <= 0.0:
            raise ShelfDockingPlanError(
                'Shelf opening does not leave enough lateral guard for straight insertion.'
            )

    return ShelfDockingPlan(
        final_pose=final_pose,
        opening_line_pose=opening_line_pose,
        entry_pose=entry_pose,
        approach_pose=approach_pose,
        opening_width=opening_width,
        required_opening_width=float(required_opening_width),
        usable_lateral_clearance=(
            None
            if usable_lateral_clearance is None
            else float(usable_lateral_clearance)
        ),
        entry_guard_distance=float(entry_guard_distance),
        approach_offset=float(approach_offset),
        position_tolerance=max(0.02, float(params.position_tolerance)),
        heading_tolerance=max(0.02, float(params.heading_tolerance)),
        entry_lateral_tolerance=max(0.005, float(entry_lateral_tolerance)),
        insert_lateral_tolerance=max(0.005, float(insert_lateral_tolerance)),
        centerline_slowdown_tolerance=max(
            max(0.005, float(insert_lateral_tolerance)),
            float(params.centerline_slowdown_tolerance),
        ),
    )


def assess_shelf_docking(
    robot_pose: Pose2D,
    plan: ShelfDockingPlan,
    *,
    local_insert_distance: float,
    approach_arrival_tolerance: float,
) -> ShelfDockingAssessment:
    entry_error = pose_error_in_target_frame(robot_pose, plan.entry_pose)
    final_error = pose_error_in_target_frame(robot_pose, plan.final_pose)
    approach_dx = float(plan.approach_pose.x) - float(robot_pose.x)
    approach_dy = float(plan.approach_pose.y) - float(robot_pose.y)
    robot_to_approach = float(math.hypot(approach_dx, approach_dy))
    robot_to_entry = float(entry_error.distance)
    stage_distance = max(
        float(approach_arrival_tolerance),
        float(local_insert_distance) * 0.75,
    )
    staged_at_entry = (
        robot_to_entry <= float(plan.position_tolerance)
        and abs(float(entry_error.left_error)) <= float(plan.entry_lateral_tolerance)
        and abs(float(entry_error.heading_error)) <= float(plan.heading_tolerance)
    )
    should_run_approach = (
        not staged_at_entry
        and (
            robot_to_approach > float(approach_arrival_tolerance)
            or robot_to_entry > stage_distance
            or abs(float(entry_error.left_error)) > (float(plan.entry_lateral_tolerance) * 1.5)
            or float(entry_error.forward_error) > max(stage_distance, 0.10)
        )
    )
    should_run_entry_navigation = (
        robot_to_entry > float(plan.position_tolerance)
        or abs(float(entry_error.left_error)) > float(plan.entry_lateral_tolerance)
        or abs(float(entry_error.heading_error)) > float(plan.heading_tolerance)
    )

    return ShelfDockingAssessment(
        robot_to_approach=robot_to_approach,
        robot_to_entry=robot_to_entry,
        entry_error=entry_error,
        final_error=final_error,
        should_run_approach=should_run_approach,
        should_run_entry_navigation=should_run_entry_navigation,
    )


def build_shelf_navigation_targets(
    robot_pose: Pose2D,
    plan: ShelfDockingPlan,
) -> List[ShelfNavigationTarget]:
    opening_error = pose_error_in_target_frame(robot_pose, plan.opening_line_pose)
    entry_error = pose_error_in_target_frame(robot_pose, plan.entry_pose)
    robot_forward_from_opening = -float(opening_error.forward_error)
    robot_left_from_opening = -float(opening_error.left_error)
    safe_forward_from_opening = -float(plan.approach_offset)
    retreat_required = (
        robot_forward_from_opening
        > (safe_forward_from_opening + float(plan.position_tolerance))
    )
    lineup_required = (
        retreat_required
        or abs(robot_left_from_opening) > float(plan.entry_lateral_tolerance)
        or abs(robot_forward_from_opening - safe_forward_from_opening) > float(plan.position_tolerance)
    )
    entry_required = (
        float(entry_error.distance) > float(plan.position_tolerance)
        or abs(float(entry_error.left_error)) > float(plan.entry_lateral_tolerance)
        or abs(float(entry_error.heading_error)) > float(plan.heading_tolerance)
    )
    if not entry_required:
        return []

    targets: List[ShelfNavigationTarget] = []
    if retreat_required:
        targets.append(
            ShelfNavigationTarget(
                label='Shelf retreat',
                pose=offset_pose_in_reference_frame(
                    plan.opening_line_pose,
                    forward_offset=safe_forward_from_opening,
                    left_offset=robot_left_from_opening,
                ),
            )
        )
    if lineup_required:
        targets.append(
            ShelfNavigationTarget(
                label='Shelf lineup',
                pose=plan.approach_pose,
            )
        )
    if entry_required:
        targets.append(
            ShelfNavigationTarget(
                label='Shelf entry alignment',
                pose=plan.entry_pose,
            )
        )

    deduped: List[ShelfNavigationTarget] = []
    last_pose: Optional[Pose2D] = None
    for target in targets:
        pose = target.pose
        if last_pose is not None:
            same_frame = str(last_pose.frame_id or 'map') == str(pose.frame_id or 'map')
            same_position = math.hypot(float(last_pose.x) - float(pose.x), float(last_pose.y) - float(pose.y)) <= 0.01
            same_heading = abs(_normalize_angle(float(last_pose.yaw) - float(pose.yaw))) <= math.radians(1.0)
            if same_frame and same_position and same_heading:
                continue
        deduped.append(target)
        last_pose = pose
    return deduped
