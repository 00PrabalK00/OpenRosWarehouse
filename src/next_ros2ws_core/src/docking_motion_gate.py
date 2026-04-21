from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

try:
    from .docking_contracts import DockingState, MotionAuthorityLease, MotionAuthorityOwner, MotionCommandEnvelope
except ImportError:  # pragma: no cover - direct-module test fallback
    from docking_contracts import DockingState, MotionAuthorityLease, MotionAuthorityOwner, MotionCommandEnvelope


@dataclass(frozen=True)
class MotionGateDecision:
    accepted: bool
    reason: str
    applied_linear_speed_mps: float
    applied_angular_speed_rad_s: float


class DockingMotionAuthorityGate:
    """Hard gate for docking-time motion ownership.

    Production rule:
    - if docking is active, only the current lease owner may publish motion
    - if docking is inactive, docking-only owners are rejected
    - safety / estop owners may always force zero motion and never pass through non-zero twist
    """

    def __init__(self) -> None:
        self.lease: Optional[MotionAuthorityLease] = None

    def set_lease(self, lease: Optional[MotionAuthorityLease]) -> None:
        self.lease = lease

    def clear(self) -> None:
        self.lease = None

    def evaluate(
        self,
        command: MotionCommandEnvelope,
        *,
        docking_active: bool,
        now_sec: Optional[float] = None,
    ) -> MotionGateDecision:
        now_sec = float(now_sec if now_sec is not None else time.time())
        owner = MotionAuthorityOwner(command.owner)

        if owner in (MotionAuthorityOwner.SAFETY_ARBITER, MotionAuthorityOwner.ESTOP):
            return MotionGateDecision(
                accepted=True,
                reason='safety_zero_override',
                applied_linear_speed_mps=0.0,
                applied_angular_speed_rad_s=0.0,
            )

        docking_owners = {
            MotionAuthorityOwner.DOCKING_SUPERVISOR,
            MotionAuthorityOwner.INSERTION_CONTROLLER,
        }
        if not bool(docking_active):
            if owner in docking_owners:
                return MotionGateDecision(
                    accepted=False,
                    reason='docking_inactive',
                    applied_linear_speed_mps=0.0,
                    applied_angular_speed_rad_s=0.0,
                )
            return MotionGateDecision(
                accepted=True,
                reason='non_docking_motion',
                applied_linear_speed_mps=float(command.linear_speed_mps),
                applied_angular_speed_rad_s=float(command.angular_speed_rad_s),
            )

        if self.lease is None:
            return MotionGateDecision(
                accepted=False,
                reason='no_active_motion_lease',
                applied_linear_speed_mps=0.0,
                applied_angular_speed_rad_s=0.0,
            )

        if self.lease.expires_at_sec is not None and float(self.lease.expires_at_sec) < now_sec:
            return MotionGateDecision(
                accepted=False,
                reason='motion_lease_expired',
                applied_linear_speed_mps=0.0,
                applied_angular_speed_rad_s=0.0,
            )

        if owner != MotionAuthorityOwner(self.lease.owner):
            return MotionGateDecision(
                accepted=False,
                reason='motion_owner_mismatch',
                applied_linear_speed_mps=0.0,
                applied_angular_speed_rad_s=0.0,
            )

        if int(command.epoch) != int(self.lease.epoch):
            return MotionGateDecision(
                accepted=False,
                reason='motion_epoch_mismatch',
                applied_linear_speed_mps=0.0,
                applied_angular_speed_rad_s=0.0,
            )

        if DockingState(command.state) != DockingState(self.lease.state):
            return MotionGateDecision(
                accepted=False,
                reason='motion_state_mismatch',
                applied_linear_speed_mps=0.0,
                applied_angular_speed_rad_s=0.0,
            )

        if command.valid_for_sec is not None:
            command_expires = float(command.stamp_sec) + max(0.0, float(command.valid_for_sec))
            if command_expires < now_sec:
                return MotionGateDecision(
                    accepted=False,
                    reason='stale_motion_command',
                    applied_linear_speed_mps=0.0,
                    applied_angular_speed_rad_s=0.0,
                )

        return MotionGateDecision(
            accepted=True,
            reason='lease_owner_match',
            applied_linear_speed_mps=float(command.linear_speed_mps),
            applied_angular_speed_rad_s=float(command.angular_speed_rad_s),
        )
