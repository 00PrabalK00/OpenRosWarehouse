import math
import sys
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parents[1] / 'src'
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from docking_contracts import PreEntryGateSample  # noqa: E402
from docking_gate import PreEntryGateThresholds, PreEntryQualificationWindow  # noqa: E402


def _sample(
    stamp_sec: float,
    *,
    lateral_error_m: float = 0.005,
    heading_error_deg: float = 0.4,
    entry_distance_m: float = 0.015,
    forward_error_m: float = 0.020,
    odom_angular_speed_rad_s: float = 0.01,
    commanded_angular_speed_rad_s: float = 0.0,
    perception_stability_score: float = 0.92,
    localization_disagreement_m: float = 0.004,
) -> PreEntryGateSample:
    return PreEntryGateSample(
        stamp_sec=float(stamp_sec),
        lateral_error_m=float(lateral_error_m),
        heading_error_rad=math.radians(float(heading_error_deg)),
        entry_distance_m=float(entry_distance_m),
        forward_error_m=float(forward_error_m),
        odom_linear_speed_mps=0.01,
        odom_angular_speed_rad_s=float(odom_angular_speed_rad_s),
        commanded_linear_speed_mps=0.02,
        commanded_angular_speed_rad_s=float(commanded_angular_speed_rad_s),
        perception_stability_score=float(perception_stability_score),
        localization_disagreement_m=float(localization_disagreement_m),
    )


def test_preentry_gate_qualifies_when_window_is_stable():
    gate = PreEntryQualificationWindow(
        thresholds=PreEntryGateThresholds(
            min_samples=5,
            min_hold_sec=0.4,
        ),
        max_samples=10,
    )

    metrics = gate.extend(
        [
            _sample(0.00),
            _sample(0.12, lateral_error_m=0.004, heading_error_deg=0.5),
            _sample(0.24, lateral_error_m=0.006, heading_error_deg=0.3),
            _sample(0.36, lateral_error_m=0.005, heading_error_deg=0.4),
            _sample(0.48, lateral_error_m=0.005, heading_error_deg=0.2),
        ]
    )

    assert metrics.qualifies is True
    assert metrics.settled is True
    assert metrics.angular_command_zero_crossings == 0
    assert metrics.rejection_reasons == ()


def test_preentry_gate_rejects_angular_hunting():
    gate = PreEntryQualificationWindow(
        thresholds=PreEntryGateThresholds(
            min_samples=5,
            min_hold_sec=0.4,
            max_angular_command_zero_crossings=0,
        ),
        max_samples=10,
    )

    metrics = gate.extend(
        [
            _sample(0.00, commanded_angular_speed_rad_s=0.08),
            _sample(0.12, commanded_angular_speed_rad_s=-0.09),
            _sample(0.24, commanded_angular_speed_rad_s=0.08),
            _sample(0.36, commanded_angular_speed_rad_s=-0.08),
            _sample(0.48, commanded_angular_speed_rad_s=0.07),
        ]
    )

    assert metrics.qualifies is False
    assert 'angular_hunting' in metrics.rejection_reasons
    assert metrics.angular_command_zero_crossings > 0


def test_preentry_gate_rejects_low_perception_stability():
    gate = PreEntryQualificationWindow(
        thresholds=PreEntryGateThresholds(
            min_samples=4,
            min_hold_sec=0.3,
            min_mean_perception_stability_score=0.85,
        ),
        max_samples=8,
    )

    metrics = gate.extend(
        [
            _sample(0.00, perception_stability_score=0.40),
            _sample(0.10, perception_stability_score=0.50),
            _sample(0.20, perception_stability_score=0.55),
            _sample(0.32, perception_stability_score=0.60),
        ]
    )

    assert metrics.qualifies is False
    assert 'perception_stability' in metrics.rejection_reasons
