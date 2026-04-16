import importlib.util
import math
import sys
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'lidar_ros2.py'
MODULE_NAME = 'next2_shelf.scripts.lidar_ros2'

spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
module = importlib.util.module_from_spec(spec)
sys.modules.setdefault(MODULE_NAME, module)
assert spec.loader is not None
spec.loader.exec_module(module)

evaluate_commit_gate = module.evaluate_commit_gate
compute_adaptive_intensity_threshold = module.compute_adaptive_intensity_threshold
compute_range_aware_dbscan_eps = module.compute_range_aware_dbscan_eps


def test_evaluate_commit_gate_accepts_stable_candidate():
    ready, reason = evaluate_commit_gate(
        detector_enabled=True,
        candidate_present=True,
        candidate_fresh=True,
        solver_ok=True,
        candidate_consistent=True,
        candidate_hotspot_count=4,
        min_hotspot_count=3,
        candidate_confidence=0.82,
        min_confidence=0.70,
        candidate_sigma_x=0.01,
        max_sigma_x_m=0.03,
        candidate_sigma_y=0.02,
        max_sigma_y_m=0.03,
        candidate_sigma_yaw=math.radians(1.5),
        max_sigma_yaw_rad=math.radians(3.0),
    )

    assert ready is True
    assert reason == ''


def test_evaluate_commit_gate_rejects_inconsistent_candidate():
    ready, reason = evaluate_commit_gate(
        detector_enabled=True,
        candidate_present=True,
        candidate_fresh=True,
        solver_ok=True,
        candidate_consistent=False,
        candidate_hotspot_count=4,
        min_hotspot_count=3,
        candidate_confidence=0.90,
        min_confidence=0.70,
        candidate_sigma_x=0.01,
        max_sigma_x_m=0.03,
        candidate_sigma_y=0.01,
        max_sigma_y_m=0.03,
        candidate_sigma_yaw=math.radians(1.0),
        max_sigma_yaw_rad=math.radians(3.0),
    )

    assert ready is False
    assert reason == 'candidate_not_consistent'


def test_evaluate_commit_gate_rejects_low_confidence_candidate():
    ready, reason = evaluate_commit_gate(
        detector_enabled=True,
        candidate_present=True,
        candidate_fresh=True,
        solver_ok=True,
        candidate_consistent=True,
        candidate_hotspot_count=4,
        min_hotspot_count=3,
        candidate_confidence=0.61,
        min_confidence=0.70,
        candidate_sigma_x=0.01,
        max_sigma_x_m=0.03,
        candidate_sigma_y=0.01,
        max_sigma_y_m=0.03,
        candidate_sigma_yaw=math.radians(1.0),
        max_sigma_yaw_rad=math.radians(3.0),
    )

    assert ready is False
    assert reason == 'candidate_confidence_below_threshold'


def test_evaluate_commit_gate_rejects_two_hotspot_commit():
    ready, reason = evaluate_commit_gate(
        detector_enabled=True,
        candidate_present=True,
        candidate_fresh=True,
        solver_ok=True,
        candidate_consistent=True,
        candidate_hotspot_count=2,
        min_hotspot_count=3,
        candidate_confidence=0.92,
        min_confidence=0.70,
        candidate_sigma_x=0.01,
        max_sigma_x_m=0.03,
        candidate_sigma_y=0.01,
        max_sigma_y_m=0.03,
        candidate_sigma_yaw=math.radians(1.0),
        max_sigma_yaw_rad=math.radians(3.0),
    )

    assert ready is False
    assert reason == 'insufficient_hotspots_for_commit'


def test_evaluate_commit_gate_rejects_large_yaw_uncertainty():
    ready, reason = evaluate_commit_gate(
        detector_enabled=True,
        candidate_present=True,
        candidate_fresh=True,
        solver_ok=True,
        candidate_consistent=True,
        candidate_hotspot_count=4,
        min_hotspot_count=3,
        candidate_confidence=0.84,
        min_confidence=0.70,
        candidate_sigma_x=0.01,
        max_sigma_x_m=0.03,
        candidate_sigma_y=0.02,
        max_sigma_y_m=0.03,
        candidate_sigma_yaw=math.radians(4.5),
        max_sigma_yaw_rad=math.radians(3.0),
    )

    assert ready is False
    assert reason == 'candidate_sigma_yaw_too_high'


def test_compute_adaptive_intensity_threshold_raises_with_bright_tail():
    threshold = compute_adaptive_intensity_threshold(
        [10.0, 12.0, 14.0, 16.0, 80.0, 100.0],
        base_threshold=35.0,
        percentile=95.0,
        scale=0.90,
    )

    assert threshold > 35.0
    assert threshold == threshold  # finite


def test_compute_adaptive_intensity_threshold_respects_base_floor():
    threshold = compute_adaptive_intensity_threshold(
        [5.0, 8.0, 12.0, 20.0],
        base_threshold=35.0,
        percentile=97.0,
        scale=0.90,
    )

    assert threshold == 35.0


def test_compute_range_aware_dbscan_eps_shrinks_for_close_returns():
    close_eps = compute_range_aware_dbscan_eps(
        base_eps_m=0.12,
        eps_min_m=0.02,
        angular_scale=2.5,
        angle_increment=0.004,
        range_a=1.0,
        range_b=1.1,
    )
    far_eps = compute_range_aware_dbscan_eps(
        base_eps_m=0.12,
        eps_min_m=0.02,
        angular_scale=2.5,
        angle_increment=0.004,
        range_a=5.0,
        range_b=5.1,
    )

    assert close_eps == 0.02
    assert far_eps > close_eps
    assert far_eps <= 0.12
