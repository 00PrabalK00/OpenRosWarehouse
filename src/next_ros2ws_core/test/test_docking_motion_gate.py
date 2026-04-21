import sys
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parents[1] / 'src'
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from docking_contracts import DockingState, MotionAuthorityLease, MotionAuthorityOwner, MotionCommandEnvelope  # noqa: E402
from docking_motion_gate import DockingMotionAuthorityGate  # noqa: E402


def test_motion_gate_rejects_docking_command_without_active_lease():
    gate = DockingMotionAuthorityGate()
    decision = gate.evaluate(
        MotionCommandEnvelope(
            owner=MotionAuthorityOwner.INSERTION_CONTROLLER,
            state=DockingState.INSERT,
            epoch=1,
            stamp_sec=1.0,
            linear_speed_mps=0.05,
            angular_speed_rad_s=0.01,
            source='test',
            valid_for_sec=0.2,
        ),
        docking_active=True,
        now_sec=1.05,
    )
    assert decision.accepted is False
    assert decision.reason == 'no_active_motion_lease'


def test_motion_gate_accepts_matching_owner_and_epoch():
    gate = DockingMotionAuthorityGate()
    gate.set_lease(
        MotionAuthorityLease(
            owner=MotionAuthorityOwner.INSERTION_CONTROLLER,
            state=DockingState.INSERT,
            epoch=7,
            granted_at_sec=1.0,
            expires_at_sec=2.0,
            source='test',
        )
    )
    decision = gate.evaluate(
        MotionCommandEnvelope(
            owner=MotionAuthorityOwner.INSERTION_CONTROLLER,
            state=DockingState.INSERT,
            epoch=7,
            stamp_sec=1.1,
            linear_speed_mps=0.05,
            angular_speed_rad_s=0.01,
            source='test',
            valid_for_sec=0.2,
        ),
        docking_active=True,
        now_sec=1.15,
    )
    assert decision.accepted is True
    assert decision.reason == 'lease_owner_match'


def test_motion_gate_rejects_stale_or_wrong_epoch_commands():
    gate = DockingMotionAuthorityGate()
    gate.set_lease(
        MotionAuthorityLease(
            owner=MotionAuthorityOwner.INSERTION_CONTROLLER,
            state=DockingState.INSERT,
            epoch=3,
            granted_at_sec=1.0,
            expires_at_sec=2.0,
            source='test',
        )
    )
    wrong_epoch = gate.evaluate(
        MotionCommandEnvelope(
            owner=MotionAuthorityOwner.INSERTION_CONTROLLER,
            state=DockingState.INSERT,
            epoch=4,
            stamp_sec=1.1,
            linear_speed_mps=0.05,
            angular_speed_rad_s=0.01,
            source='test',
            valid_for_sec=0.2,
        ),
        docking_active=True,
        now_sec=1.15,
    )
    stale = gate.evaluate(
        MotionCommandEnvelope(
            owner=MotionAuthorityOwner.INSERTION_CONTROLLER,
            state=DockingState.INSERT,
            epoch=3,
            stamp_sec=1.0,
            linear_speed_mps=0.05,
            angular_speed_rad_s=0.01,
            source='test',
            valid_for_sec=0.05,
        ),
        docking_active=True,
        now_sec=1.2,
    )
    assert wrong_epoch.accepted is False
    assert wrong_epoch.reason == 'motion_epoch_mismatch'
    assert stale.accepted is False
    assert stale.reason == 'stale_motion_command'


def test_motion_gate_clamps_safety_and_estop_to_zero_motion():
    gate = DockingMotionAuthorityGate()
    for owner in (MotionAuthorityOwner.SAFETY_ARBITER, MotionAuthorityOwner.ESTOP):
        decision = gate.evaluate(
            MotionCommandEnvelope(
                owner=owner,
                state=DockingState.ABORT_STOP,
                epoch=0,
                stamp_sec=1.0,
                linear_speed_mps=0.22,
                angular_speed_rad_s=-0.35,
                source='test',
                valid_for_sec=0.5,
            ),
            docking_active=True,
            now_sec=1.1,
        )
        assert decision.accepted is True
        assert decision.reason == 'safety_zero_override'
        assert decision.applied_linear_speed_mps == 0.0
        assert decision.applied_angular_speed_rad_s == 0.0


def test_motion_gate_rejects_matching_owner_with_wrong_state():
    gate = DockingMotionAuthorityGate()
    gate.set_lease(
        MotionAuthorityLease(
            owner=MotionAuthorityOwner.INSERTION_CONTROLLER,
            state=DockingState.INSERT,
            epoch=5,
            granted_at_sec=1.0,
            expires_at_sec=2.0,
            source='test',
        )
    )
    decision = gate.evaluate(
        MotionCommandEnvelope(
            owner=MotionAuthorityOwner.INSERTION_CONTROLLER,
            state=DockingState.MOVE_TO_ENTRY,
            epoch=5,
            stamp_sec=1.1,
            linear_speed_mps=0.05,
            angular_speed_rad_s=0.01,
            source='test',
            valid_for_sec=0.2,
        ),
        docking_active=True,
        now_sec=1.15,
    )
    assert decision.accepted is False
    assert decision.reason == 'motion_state_mismatch'
