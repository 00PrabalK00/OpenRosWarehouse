from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class DockingState(str, Enum):
    IDLE = 'idle'
    SEARCH = 'search'
    CANDIDATE_TRACKING = 'candidate_tracking'
    QUALIFY = 'qualify'
    PRE_ALIGN_CENTERLINE = 'pre_align_centerline'
    HOLD_STABLE = 'hold_stable'
    COMMIT_DOCK_FRAME = 'commit_dock_frame'
    MOVE_TO_ENTRY = 'move_to_entry'
    INSERT = 'insert'
    VERIFY_SEATED = 'verify_seated'
    SUCCESS = 'success'
    ABORT_STOP = 'abort_stop'
    CONTROLLED_RETREAT = 'controlled_retreat'
    FAULT = 'fault'


class MotionAuthorityOwner(str, Enum):
    NONE = 'none'
    NAV2 = 'nav2'
    DOCKING_SUPERVISOR = 'docking_supervisor'
    INSERTION_CONTROLLER = 'insertion_controller'
    JOYSTICK = 'joystick'
    SAFETY_ARBITER = 'safety_arbiter'
    ESTOP = 'estop'


class DockingAbortCategory(str, Enum):
    NONE = 'none'
    WEAK_COMMIT = 'weak_commit'
    PREALIGN_UNSTABLE = 'prealign_unstable'
    AUTHORITY_CONFLICT = 'authority_conflict'
    MID_INSERT_DRIFT = 'mid_insert_drift'
    MODEL_INVALIDATED = 'model_invalidated'
    SUSPECTED_CONTACT = 'suspected_contact'
    LOCALIZATION_INCONSISTENCY = 'localization_inconsistency'
    MECHANICAL_BIAS = 'mechanical_bias'
    TIMEOUT = 'timeout'
    CANCELED = 'canceled'
    FAULT = 'fault'


@dataclass(frozen=True)
class ShelfPerceptionEstimate:
    x: float
    y: float
    yaw: float
    frame_id: str
    stamp_sec: float
    hotspot_count: int
    front_width_m: Optional[float] = None
    back_width_m: Optional[float] = None
    depth_m: Optional[float] = None
    track_id: int = 0
    detectability_confidence: float = 0.0
    control_usability_confidence: float = 0.0
    stability_score: float = 0.0
    stable_observation_count: int = 0
    stable_tail_count: int = 0
    stable_hold_sec: float = 0.0
    position_std_m: float = 0.0
    yaw_std_rad: float = 0.0
    model_valid: bool = False
    source: str = 'shelf_detector'
    notes: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CommittedDockFrame:
    frame_id: str
    mouth_center_x: float
    mouth_center_y: float
    entry_x: float
    entry_y: float
    entry_yaw: float
    final_x: float
    final_y: float
    final_yaw: float
    opening_width_m: Optional[float]
    expected_insertion_depth_m: float
    left_clearance_m: Optional[float]
    right_clearance_m: Optional[float]
    committed_at_sec: float
    valid_until_sec: Optional[float]
    perception: ShelfPerceptionEstimate
    source_state: DockingState = DockingState.COMMIT_DOCK_FRAME

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MotionAuthorityLease:
    owner: MotionAuthorityOwner
    state: DockingState
    epoch: int
    granted_at_sec: float
    expires_at_sec: Optional[float]
    exclusive: bool = True
    source: str = ''

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MotionCommandEnvelope:
    owner: MotionAuthorityOwner
    state: DockingState
    epoch: int
    stamp_sec: float
    linear_speed_mps: float
    angular_speed_rad_s: float
    source: str = ''
    valid_for_sec: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PreEntryGateSample:
    stamp_sec: float
    lateral_error_m: float
    heading_error_rad: float
    entry_distance_m: float
    forward_error_m: float
    odom_linear_speed_mps: float
    odom_angular_speed_rad_s: float
    commanded_linear_speed_mps: float
    commanded_angular_speed_rad_s: float
    perception_stability_score: float
    localization_disagreement_m: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PreEntryGateMetrics:
    sample_count: int
    hold_duration_sec: float
    mean_lateral_error_m: float
    max_abs_lateral_error_m: float
    lateral_std_m: float
    mean_heading_error_rad: float
    max_abs_heading_error_rad: float
    heading_std_rad: float
    mean_entry_distance_m: float
    max_abs_entry_distance_m: float
    mean_forward_error_m: float
    max_abs_forward_error_m: float
    forward_std_m: float
    mean_perception_stability_score: float
    max_localization_disagreement_m: float
    angular_command_zero_crossings: int
    settled: bool
    qualifies: bool
    rejection_reasons: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DockingAbortRecord:
    stamp_sec: float
    category: DockingAbortCategory
    state: DockingState
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DockingAttemptTelemetry:
    attempt_id: str
    target_label: str
    started_at_sec: float
    ended_at_sec: Optional[float] = None
    final_state: DockingState = DockingState.IDLE
    committed_frame: Optional[CommittedDockFrame] = None
    motion_lease: Optional[MotionAuthorityLease] = None
    gate_metrics: Optional[PreEntryGateMetrics] = None
    abort: Optional[DockingAbortRecord] = None
    result: str = ''
    notes: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
