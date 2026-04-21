import sys
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parents[1] / 'src'
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from docking_contracts import DockingAbortCategory, DockingState  # noqa: E402
from docking_supervisor import DockingSupervisor  # noqa: E402


def test_record_abort_updates_runtime_state_and_telemetry():
    supervisor = DockingSupervisor(
        attempt_id='attempt-1',
        target_label='goal pose',
    )
    supervisor.transition(DockingState.SEARCH)
    supervisor.transition(DockingState.CANDIDATE_TRACKING)
    supervisor.transition(DockingState.QUALIFY)
    abort = supervisor.record_abort(
        DockingAbortCategory.PREALIGN_UNSTABLE,
        'alignment contract failed',
        stamp_sec=5.0,
    )
    assert abort.state == DockingState.ABORT_STOP
    assert supervisor.state == DockingState.ABORT_STOP
    assert supervisor.telemetry.final_state == DockingState.ABORT_STOP
    assert supervisor.telemetry.abort is not None
    assert supervisor.telemetry.abort.reason == 'alignment contract failed'
