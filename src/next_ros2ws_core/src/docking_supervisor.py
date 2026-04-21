from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Dict, Optional, Set

try:
    from .docking_contracts import (
        CommittedDockFrame,
        DockingAbortCategory,
        DockingAbortRecord,
        DockingAttemptTelemetry,
        DockingState,
        MotionAuthorityLease,
        MotionAuthorityOwner,
        PreEntryGateMetrics,
    )
except ImportError:  # pragma: no cover - direct-module test fallback
    from docking_contracts import (
        CommittedDockFrame,
        DockingAbortCategory,
        DockingAbortRecord,
        DockingAttemptTelemetry,
        DockingState,
        MotionAuthorityLease,
        MotionAuthorityOwner,
        PreEntryGateMetrics,
    )


_ALLOWED_TRANSITIONS: Dict[DockingState, Set[DockingState]] = {
    DockingState.IDLE: {
        DockingState.SEARCH,
        DockingState.CANDIDATE_TRACKING,
        DockingState.FAULT,
    },
    DockingState.SEARCH: {
        DockingState.CANDIDATE_TRACKING,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.CANDIDATE_TRACKING: {
        DockingState.SEARCH,
        DockingState.QUALIFY,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.QUALIFY: {
        DockingState.SEARCH,
        DockingState.PRE_ALIGN_CENTERLINE,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.PRE_ALIGN_CENTERLINE: {
        DockingState.HOLD_STABLE,
        DockingState.CONTROLLED_RETREAT,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.HOLD_STABLE: {
        DockingState.PRE_ALIGN_CENTERLINE,
        DockingState.COMMIT_DOCK_FRAME,
        DockingState.CONTROLLED_RETREAT,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.COMMIT_DOCK_FRAME: {
        DockingState.MOVE_TO_ENTRY,
        DockingState.CONTROLLED_RETREAT,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.MOVE_TO_ENTRY: {
        DockingState.INSERT,
        DockingState.HOLD_STABLE,
        DockingState.CONTROLLED_RETREAT,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.INSERT: {
        DockingState.VERIFY_SEATED,
        DockingState.CONTROLLED_RETREAT,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.VERIFY_SEATED: {
        DockingState.SUCCESS,
        DockingState.CONTROLLED_RETREAT,
        DockingState.ABORT_STOP,
        DockingState.FAULT,
    },
    DockingState.SUCCESS: {DockingState.IDLE},
    DockingState.ABORT_STOP: {
        DockingState.CONTROLLED_RETREAT,
        DockingState.FAULT,
        DockingState.IDLE,
    },
    DockingState.CONTROLLED_RETREAT: {
        DockingState.SEARCH,
        DockingState.IDLE,
        DockingState.FAULT,
    },
    DockingState.FAULT: {DockingState.IDLE},
}


@dataclass
class DockingSupervisorSnapshot:
    state: DockingState
    attempt_id: str
    target_label: str
    committed: bool
    motion_owner: MotionAuthorityOwner
    motion_epoch: int
    note: str = ''


@dataclass
class DockingSupervisor:
    attempt_id: str
    target_label: str
    state: DockingState = DockingState.IDLE
    telemetry: DockingAttemptTelemetry = field(init=False)
    committed_frame: Optional[CommittedDockFrame] = None
    motion_lease: Optional[MotionAuthorityLease] = None
    latest_gate_metrics: Optional[PreEntryGateMetrics] = None
    latest_note: str = ''

    def __post_init__(self) -> None:
        self.telemetry = DockingAttemptTelemetry(
            attempt_id=str(self.attempt_id),
            target_label=str(self.target_label),
            started_at_sec=time.time(),
            final_state=self.state,
        )

    def transition(self, next_state: DockingState, *, note: str = '') -> DockingState:
        next_state = DockingState(next_state)
        allowed = _ALLOWED_TRANSITIONS.get(self.state, set())
        if next_state not in allowed and next_state != self.state:
            raise ValueError(
                f'Invalid docking transition: {self.state.value} -> {next_state.value}'
            )
        self.state = next_state
        self.latest_note = str(note or '')
        self.telemetry = replace(
            self.telemetry,
            final_state=self.state,
            notes=tuple(
                list(self.telemetry.notes)
                + ([self.latest_note] if self.latest_note else [])
            ),
        )
        return self.state

    def grant_motion_authority(
        self,
        owner: MotionAuthorityOwner,
        *,
        epoch: Optional[int] = None,
        ttl_sec: Optional[float] = None,
        source: str = '',
        now_sec: Optional[float] = None,
    ) -> MotionAuthorityLease:
        now_sec = float(now_sec if now_sec is not None else time.time())
        next_epoch = int(epoch if epoch is not None else ((self.motion_lease.epoch + 1) if self.motion_lease else 1))
        expires_at_sec = None if ttl_sec is None else (now_sec + max(0.0, float(ttl_sec)))
        self.motion_lease = MotionAuthorityLease(
            owner=MotionAuthorityOwner(owner),
            state=self.state,
            epoch=next_epoch,
            granted_at_sec=now_sec,
            expires_at_sec=expires_at_sec,
            exclusive=True,
            source=str(source or ''),
        )
        self.telemetry = replace(self.telemetry, motion_lease=self.motion_lease)
        return self.motion_lease

    def record_gate_metrics(self, metrics: PreEntryGateMetrics) -> None:
        self.latest_gate_metrics = metrics
        self.telemetry = replace(self.telemetry, gate_metrics=metrics)

    def commit_dock_frame(self, frame: CommittedDockFrame) -> CommittedDockFrame:
        self.committed_frame = frame
        self.telemetry = replace(self.telemetry, committed_frame=frame)
        return frame

    def record_abort(
        self,
        category: DockingAbortCategory,
        reason: str,
        *,
        stamp_sec: Optional[float] = None,
    ) -> DockingAbortRecord:
        abort_note = str(reason or '')
        if self.state != DockingState.ABORT_STOP:
            try:
                self.transition(DockingState.ABORT_STOP, note=abort_note)
            except ValueError:
                self.state = DockingState.ABORT_STOP
                self.latest_note = abort_note
                self.telemetry = replace(
                    self.telemetry,
                    final_state=self.state,
                    notes=tuple(
                        list(self.telemetry.notes)
                        + ([self.latest_note] if self.latest_note else [])
                    ),
                )
        abort = DockingAbortRecord(
            stamp_sec=float(stamp_sec if stamp_sec is not None else time.time()),
            category=DockingAbortCategory(category),
            state=self.state,
            reason=abort_note,
        )
        self.telemetry = replace(
            self.telemetry,
            abort=abort,
            final_state=self.state,
        )
        return abort

    def finish(self, state: DockingState, *, result: str = '', stamp_sec: Optional[float] = None) -> DockingAttemptTelemetry:
        self.state = DockingState(state)
        self.telemetry = replace(
            self.telemetry,
            ended_at_sec=float(stamp_sec if stamp_sec is not None else time.time()),
            final_state=self.state,
            result=str(result or ''),
        )
        return self.telemetry

    def snapshot(self) -> DockingSupervisorSnapshot:
        return DockingSupervisorSnapshot(
            state=self.state,
            attempt_id=str(self.attempt_id),
            target_label=str(self.target_label),
            committed=self.committed_frame is not None,
            motion_owner=(
                self.motion_lease.owner if self.motion_lease is not None else MotionAuthorityOwner.NONE
            ),
            motion_epoch=(self.motion_lease.epoch if self.motion_lease is not None else 0),
            note=str(self.latest_note or ''),
        )
