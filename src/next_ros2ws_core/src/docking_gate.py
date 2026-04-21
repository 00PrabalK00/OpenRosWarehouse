from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, Iterable, List, Optional

try:
    from .docking_contracts import PreEntryGateMetrics, PreEntryGateSample
except ImportError:  # pragma: no cover - direct-module test fallback
    from docking_contracts import PreEntryGateMetrics, PreEntryGateSample


def _mean(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _std(values: Iterable[float], *, mean_value: Optional[float] = None) -> float:
    values = list(values)
    if len(values) < 2:
        return 0.0
    if mean_value is None:
        mean_value = _mean(values)
    variance = sum((float(value) - float(mean_value)) ** 2 for value in values) / len(values)
    return float(math.sqrt(max(0.0, variance)))


def _zero_crossings(values: Iterable[float], *, deadband: float = 1e-4) -> int:
    crossings = 0
    previous_sign = 0
    for value in values:
        if value > deadband:
            current_sign = 1
        elif value < -deadband:
            current_sign = -1
        else:
            current_sign = 0
        if current_sign == 0:
            continue
        if previous_sign != 0 and current_sign != previous_sign:
            crossings += 1
        previous_sign = current_sign
    return int(crossings)


@dataclass(frozen=True)
class PreEntryGateThresholds:
    min_samples: int = 5
    min_hold_sec: float = 0.50
    max_abs_lateral_error_m: float = 0.020
    max_abs_heading_error_rad: float = math.radians(2.0)
    max_abs_entry_distance_m: float = 0.040
    max_abs_forward_error_m: float = 0.060
    max_lateral_std_m: float = 0.006
    max_heading_std_rad: float = math.radians(0.60)
    max_forward_std_m: float = 0.010
    min_mean_perception_stability_score: float = 0.75
    max_localization_disagreement_m: float = 0.020
    max_abs_odom_angular_speed_rad_s: float = 0.080
    max_abs_commanded_angular_speed_rad_s: float = 0.050
    max_angular_command_zero_crossings: int = 0


class PreEntryQualificationWindow:
    def __init__(
        self,
        *,
        thresholds: Optional[PreEntryGateThresholds] = None,
        max_samples: int = 20,
    ) -> None:
        self.thresholds = thresholds or PreEntryGateThresholds()
        self._samples: Deque[PreEntryGateSample] = deque(maxlen=max(1, int(max_samples)))

    def clear(self) -> None:
        self._samples.clear()

    def add_sample(self, sample: PreEntryGateSample) -> PreEntryGateMetrics:
        self._samples.append(sample)
        return self.snapshot_metrics()

    def extend(self, samples: Iterable[PreEntryGateSample]) -> PreEntryGateMetrics:
        for sample in samples:
            self._samples.append(sample)
        return self.snapshot_metrics()

    @property
    def samples(self) -> List[PreEntryGateSample]:
        return list(self._samples)

    def snapshot_metrics(self) -> PreEntryGateMetrics:
        samples = list(self._samples)
        if not samples:
            return PreEntryGateMetrics(
                sample_count=0,
                hold_duration_sec=0.0,
                mean_lateral_error_m=0.0,
                max_abs_lateral_error_m=0.0,
                lateral_std_m=0.0,
                mean_heading_error_rad=0.0,
                max_abs_heading_error_rad=0.0,
                heading_std_rad=0.0,
                mean_entry_distance_m=0.0,
                max_abs_entry_distance_m=0.0,
                mean_forward_error_m=0.0,
                max_abs_forward_error_m=0.0,
                forward_std_m=0.0,
                mean_perception_stability_score=0.0,
                max_localization_disagreement_m=0.0,
                angular_command_zero_crossings=0,
                settled=False,
                qualifies=False,
                rejection_reasons=('no_samples',),
            )

        lateral_values = [float(sample.lateral_error_m) for sample in samples]
        heading_values = [float(sample.heading_error_rad) for sample in samples]
        entry_values = [float(sample.entry_distance_m) for sample in samples]
        forward_values = [float(sample.forward_error_m) for sample in samples]
        stability_values = [float(sample.perception_stability_score) for sample in samples]
        localization_values = [abs(float(sample.localization_disagreement_m)) for sample in samples]
        odom_angular_values = [abs(float(sample.odom_angular_speed_rad_s)) for sample in samples]
        commanded_angular_values = [float(sample.commanded_angular_speed_rad_s) for sample in samples]

        hold_duration_sec = max(0.0, float(samples[-1].stamp_sec) - float(samples[0].stamp_sec))
        mean_lateral = _mean(lateral_values)
        mean_heading = _mean(heading_values)
        mean_entry = _mean(entry_values)
        mean_forward = _mean(forward_values)
        mean_stability = _mean(stability_values)
        max_abs_lateral = max(abs(value) for value in lateral_values)
        max_abs_heading = max(abs(value) for value in heading_values)
        max_abs_entry = max(abs(value) for value in entry_values)
        max_abs_forward = max(abs(value) for value in forward_values)
        max_localization = max(localization_values) if localization_values else 0.0
        lateral_std = _std(lateral_values, mean_value=mean_lateral)
        heading_std = _std(heading_values, mean_value=mean_heading)
        forward_std = _std(forward_values, mean_value=mean_forward)
        zero_crossings = _zero_crossings(commanded_angular_values)

        thresholds = self.thresholds
        rejection_reasons = []
        if len(samples) < int(thresholds.min_samples):
            rejection_reasons.append('insufficient_samples')
        if hold_duration_sec < float(thresholds.min_hold_sec):
            rejection_reasons.append('hold_too_short')
        if max_abs_lateral > float(thresholds.max_abs_lateral_error_m):
            rejection_reasons.append('lateral_error')
        if max_abs_heading > float(thresholds.max_abs_heading_error_rad):
            rejection_reasons.append('heading_error')
        if max_abs_entry > float(thresholds.max_abs_entry_distance_m):
            rejection_reasons.append('entry_distance')
        if max_abs_forward > float(thresholds.max_abs_forward_error_m):
            rejection_reasons.append('forward_error')
        if lateral_std > float(thresholds.max_lateral_std_m):
            rejection_reasons.append('lateral_variance')
        if heading_std > float(thresholds.max_heading_std_rad):
            rejection_reasons.append('heading_variance')
        if forward_std > float(thresholds.max_forward_std_m):
            rejection_reasons.append('forward_variance')
        if mean_stability < float(thresholds.min_mean_perception_stability_score):
            rejection_reasons.append('perception_stability')
        if max_localization > float(thresholds.max_localization_disagreement_m):
            rejection_reasons.append('localization_disagreement')
        if odom_angular_values and max(odom_angular_values) > float(thresholds.max_abs_odom_angular_speed_rad_s):
            rejection_reasons.append('odom_angular_rate')
        if commanded_angular_values and max(abs(value) for value in commanded_angular_values) > float(thresholds.max_abs_commanded_angular_speed_rad_s):
            rejection_reasons.append('commanded_angular_rate')
        if zero_crossings > int(thresholds.max_angular_command_zero_crossings):
            rejection_reasons.append('angular_hunting')

        settled = not any(
            reason in rejection_reasons
            for reason in (
                'lateral_variance',
                'heading_variance',
                'forward_variance',
                'odom_angular_rate',
                'commanded_angular_rate',
                'angular_hunting',
            )
        )
        qualifies = len(rejection_reasons) == 0
        return PreEntryGateMetrics(
            sample_count=len(samples),
            hold_duration_sec=hold_duration_sec,
            mean_lateral_error_m=mean_lateral,
            max_abs_lateral_error_m=max_abs_lateral,
            lateral_std_m=lateral_std,
            mean_heading_error_rad=mean_heading,
            max_abs_heading_error_rad=max_abs_heading,
            heading_std_rad=heading_std,
            mean_entry_distance_m=mean_entry,
            max_abs_entry_distance_m=max_abs_entry,
            mean_forward_error_m=mean_forward,
            max_abs_forward_error_m=max_abs_forward,
            forward_std_m=forward_std,
            mean_perception_stability_score=mean_stability,
            max_localization_disagreement_m=max_localization,
            angular_command_zero_crossings=zero_crossings,
            settled=settled,
            qualifies=qualifies,
            rejection_reasons=tuple(rejection_reasons),
        )
