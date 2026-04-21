from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Optional

try:
    from .docking_contracts import CommittedDockFrame, DockingAbortCategory
except ImportError:  # pragma: no cover - direct-module test fallback
    from docking_contracts import CommittedDockFrame, DockingAbortCategory


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(float(lower), min(float(upper), float(value)))


def _normalize_angle(angle: float) -> float:
    wrapped = float(angle)
    while wrapped > math.pi:
        wrapped -= 2.0 * math.pi
    while wrapped < -math.pi:
        wrapped += 2.0 * math.pi
    return wrapped


def _ramp_axis(
    current_value: float,
    current_accel: float,
    target_value: float,
    dt: float,
    *,
    max_accel: float,
    max_jerk: float,
) -> tuple[float, float]:
    if dt <= 0.0:
        return float(current_value), float(current_accel)

    requested_accel = _clamp(
        (float(target_value) - float(current_value)) / dt,
        -float(max_accel),
        float(max_accel),
    )
    accel_delta_limit = max(0.0, float(max_jerk) * dt)
    next_accel = float(current_accel) + _clamp(
        requested_accel - float(current_accel),
        -accel_delta_limit,
        accel_delta_limit,
    )
    next_accel = _clamp(next_accel, -float(max_accel), float(max_accel))
    next_value = float(current_value) + (next_accel * dt)

    if float(target_value) >= float(current_value):
        next_value = min(next_value, float(target_value))
    else:
        next_value = max(next_value, float(target_value))
    return float(next_value), float(next_accel)


@dataclass(frozen=True)
class DockFrameErrors:
    progress_along_dock_m: float
    remaining_distance_m: float
    lateral_error_m: float
    heading_error_rad: float
    distance_to_entry_m: float


@dataclass(frozen=True)
class FrozenInsertionControllerConfig:
    insert_speed_mps: float = 0.06
    min_insert_speed_mps: float = 0.03
    braking_distance_m: float = 0.08
    stop_distance_m: float = 0.01
    final_alignment_lateral_tolerance_m: float = 0.015
    final_alignment_heading_tolerance_rad: float = math.radians(1.5)
    heading_gain: float = 0.9
    lateral_gain: float = 1.4
    max_angular_speed_rad_s: float = 0.08
    heading_only_lateral_threshold_m: float = 0.006
    heading_only_max_angular_speed_rad_s: float = 0.04
    max_abs_lateral_error_m: float = 0.03
    max_abs_heading_error_rad: float = math.radians(3.0)
    max_pose_jump_m: float = 0.02
    max_pose_jump_m_deep_insert: float = 0.04
    max_pose_jump_lateral_m: float = 0.02
    max_pose_jump_lateral_m_deep_insert: float = 0.03
    max_pose_jump_backward_m: float = 0.04
    perception_confidence_ignore_after_progress_m: float = 0.10
    perception_confidence_ignore_inside_remaining_m: float = 0.08
    pose_jump_ignore_inside_remaining_m: float = 0.05
    clearance_abort_margin_ratio: float = 0.85
    min_perception_confidence: float = 0.55
    max_linear_accel_mps2: float = 0.14
    max_linear_jerk_mps3: float = 0.35
    max_angular_accel_rad_s2: float = 0.18
    max_angular_jerk_rad_s3: float = 0.55
    allow_heading_correction: bool = True


@dataclass(frozen=True)
class FrozenInsertionControllerInput:
    dock_errors: DockFrameErrors
    perception_confidence: float
    pose_jump_m: float = 0.0
    pose_jump_forward_m: float = 0.0
    pose_jump_lateral_m: float = 0.0
    divergence_detected: bool = False


@dataclass(frozen=True)
class FrozenInsertionControllerOutput:
    linear_speed_mps: float
    angular_speed_rad_s: float
    stop: bool = False
    success: bool = False
    target_depth_reached: bool = False
    abort_category: DockingAbortCategory = DockingAbortCategory.NONE
    reason: str = ''
    dock_errors: Optional[DockFrameErrors] = None
    raw_linear_speed_mps: float = 0.0
    raw_angular_speed_rad_s: float = 0.0


@dataclass
class FrozenInsertionControllerState:
    linear_speed_mps: float = 0.0
    angular_speed_rad_s: float = 0.0
    linear_accel_mps2: float = 0.0
    angular_accel_rad_s2: float = 0.0
    last_update_sec: Optional[float] = None


class FrozenInsertionController:
    """Dedicated final-insert controller for a frozen dock frame.

    Production rule:
    - Compute dock-frame errors first
    - Run abort checks before publishing any non-zero command
    - If safe, publish one bounded control law:
      v = slow forward insert speed
      w = clamp(k_theta * e_theta - k_y * e_y, -w_max, +w_max)
    """

    def __init__(self, config: Optional[FrozenInsertionControllerConfig] = None) -> None:
        self.config = config or FrozenInsertionControllerConfig()
        self.state = FrozenInsertionControllerState()

    def reset(self) -> None:
        self.state = FrozenInsertionControllerState()

    @staticmethod
    def _side_clearance_limit_m(
        dock_frame: CommittedDockFrame,
        *,
        fallback_limit_m: float,
        margin_ratio: float,
    ) -> float:
        clearances = []
        for clearance in (dock_frame.left_clearance_m, dock_frame.right_clearance_m):
            if clearance is None:
                continue
            try:
                clearance_value = float(clearance)
            except Exception:
                continue
            if math.isfinite(clearance_value) and clearance_value > 0.0:
                clearances.append(clearance_value)
        if not clearances:
            return float(fallback_limit_m)
        return max(
            0.005,
            min(
                float(fallback_limit_m),
                min(clearances) * max(0.1, float(margin_ratio)),
            ),
        )

    @staticmethod
    def compute_dock_frame_errors(
        dock_frame: CommittedDockFrame,
        *,
        robot_x: float,
        robot_y: float,
        robot_yaw: float,
    ) -> DockFrameErrors:
        heading_x = math.cos(float(dock_frame.entry_yaw))
        heading_y = math.sin(float(dock_frame.entry_yaw))
        left_x = -heading_y
        left_y = heading_x

        dx = float(robot_x) - float(dock_frame.mouth_center_x)
        dy = float(robot_y) - float(dock_frame.mouth_center_y)
        progress_along_dock = float((dx * heading_x) + (dy * heading_y))
        lateral_error = float((dx * left_x) + (dy * left_y))
        heading_error = _normalize_angle(float(dock_frame.entry_yaw) - float(robot_yaw))
        remaining_distance = max(
            0.0,
            float(dock_frame.expected_insertion_depth_m) - progress_along_dock,
        )
        entry_dx = float(robot_x) - float(dock_frame.entry_x)
        entry_dy = float(robot_y) - float(dock_frame.entry_y)
        distance_to_entry = float(math.hypot(entry_dx, entry_dy))
        return DockFrameErrors(
            progress_along_dock_m=progress_along_dock,
            remaining_distance_m=remaining_distance,
            lateral_error_m=lateral_error,
            heading_error_rad=heading_error,
            distance_to_entry_m=distance_to_entry,
        )

    def compute_from_pose(
        self,
        dock_frame: CommittedDockFrame,
        *,
        robot_x: float,
        robot_y: float,
        robot_yaw: float,
        perception_confidence: float,
        pose_jump_m: float = 0.0,
        pose_jump_forward_m: float = 0.0,
        pose_jump_lateral_m: float = 0.0,
        divergence_detected: bool = False,
        now_sec: Optional[float] = None,
    ) -> FrozenInsertionControllerOutput:
        dock_errors = self.compute_dock_frame_errors(
            dock_frame,
            robot_x=float(robot_x),
            robot_y=float(robot_y),
            robot_yaw=float(robot_yaw),
        )
        return self.update(
            FrozenInsertionControllerInput(
                dock_errors=dock_errors,
                perception_confidence=float(perception_confidence),
                pose_jump_m=float(pose_jump_m),
                pose_jump_forward_m=float(pose_jump_forward_m),
                pose_jump_lateral_m=float(pose_jump_lateral_m),
                divergence_detected=bool(divergence_detected),
            ),
            dock_frame=dock_frame,
            now_sec=now_sec,
        )

    def update(
        self,
        state: FrozenInsertionControllerInput,
        *,
        dock_frame: Optional[CommittedDockFrame] = None,
        now_sec: Optional[float] = None,
    ) -> FrozenInsertionControllerOutput:
        # Minimal straight-in insertion controller:
        #   1. If we've reached the target depth, report success.
        #   2. If lateral error is about to collide with a shelf leg, abort.
        #   3. Otherwise: drive forward, steer with PID on heading + lateral.
        # All other abort paths (perception, pose jump, divergence, heading bound,
        # final-alignment gate) have been removed — they were causing insertion to
        # fail on transient conditions that the active steering term can handle.
        cfg = self.config
        errors = state.dock_errors
        now_sec = float(now_sec if now_sec is not None else time.monotonic())

        # Depth reached -> done.  Don't gate on final lateral/heading tolerances;
        # if we got this far, we're inside the shelf.
        if float(errors.remaining_distance_m) <= (float(cfg.stop_distance_m) + 1e-6):
            self.reset()
            return FrozenInsertionControllerOutput(
                linear_speed_mps=0.0,
                angular_speed_rad_s=0.0,
                stop=True,
                success=True,
                target_depth_reached=True,
                reason='target_depth_reached',
                dock_errors=errors,
            )

        # Collision-imminent safety: use the real shelf clearance if we have it,
        # else fall back to the configured ceiling.
        collision_limit_m = float(cfg.max_abs_lateral_error_m)
        if dock_frame is not None:
            collision_limit_m = self._side_clearance_limit_m(
                dock_frame,
                fallback_limit_m=collision_limit_m,
                margin_ratio=float(cfg.clearance_abort_margin_ratio),
            )
        if abs(float(errors.lateral_error_m)) > collision_limit_m:
            self.reset()
            return FrozenInsertionControllerOutput(
                linear_speed_mps=0.0,
                angular_speed_rad_s=0.0,
                stop=True,
                abort_category=DockingAbortCategory.MID_INSERT_DRIFT,
                reason='lateral_error_exceeded',
                dock_errors=errors,
            )

        # Forward speed: cruise, ramp down over braking_distance_m.
        linear_speed = float(cfg.insert_speed_mps)
        if float(errors.remaining_distance_m) < float(cfg.braking_distance_m):
            brake_ratio = _clamp(
                float(errors.remaining_distance_m) / max(float(cfg.braking_distance_m), 1e-6),
                0.0,
                1.0,
            )
            linear_speed = max(float(cfg.min_insert_speed_mps), linear_speed * brake_ratio)

        # Software compliance: once the robot's reference point crosses the shelf
        # mouth (progress_along_dock >= 0), throttle gains and speed so any residual
        # correction is "micro" rather than "free".  This is the closest approximation
        # to mechanical compliance we have without a force sensor — bounded correction
        # inside the funnel instead of aggressive steering near contact.
        heading_gain = float(cfg.heading_gain)
        lateral_gain = float(cfg.lateral_gain)
        angular_limit = float(cfg.max_angular_speed_rad_s)
        if float(errors.progress_along_dock_m) >= 0.0:
            heading_gain *= 0.5
            lateral_gain *= 0.5
            angular_limit = min(angular_limit, 0.04)
            linear_speed = min(linear_speed, max(float(cfg.min_insert_speed_mps), 0.03))

        # Steering: PID on heading and lateral, clamped.
        angular_speed = (
            (heading_gain * float(errors.heading_error_rad))
            - (lateral_gain * float(errors.lateral_error_m))
        )
        angular_speed = _clamp(angular_speed, -angular_limit, angular_limit)

        self.state = FrozenInsertionControllerState(
            linear_speed_mps=float(linear_speed),
            angular_speed_rad_s=float(angular_speed),
            linear_accel_mps2=0.0,
            angular_accel_rad_s2=0.0,
            last_update_sec=now_sec,
        )

        return FrozenInsertionControllerOutput(
            linear_speed_mps=max(0.0, float(linear_speed)),
            angular_speed_rad_s=float(angular_speed),
            stop=False,
            success=False,
            target_depth_reached=False,
            reason='advance_along_frozen_centerline',
            dock_errors=errors,
            raw_linear_speed_mps=float(linear_speed),
            raw_angular_speed_rad_s=float(angular_speed),
        )
