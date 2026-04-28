#!/usr/bin/env python3
"""
Manual spin calibration reporter.

Usage:
  python3 spin_test.py

The script auto-arms each trial. Rotate the robot to the physical mark,
then press c to capture. The schedule is 20 at 90 deg, 20 at 180 deg,
and 10 at 360 deg. Press q to quit.
"""

import csv
import math
import select
import sys
import termios
import threading
import tty
from dataclasses import dataclass
from pathlib import Path

import rclpy
from nav_msgs.msg import Odometry
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data
import tf2_ros


def yaw_from_quat(q):
    siny_cosp = 2.0 * (q.w * q.z + q.x * q.y)
    cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
    return math.atan2(siny_cosp, cosy_cosp)


def wrap_pi(angle):
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


def stamp_to_sec(stamp):
    return float(stamp.sec) + float(stamp.nanosec) * 1e-9


@dataclass
class YawSample:
    yaw: float
    stamp_sec: float
    source: str


class SpinReporter(Node):
    MAX_SAMPLE_AGE_SEC = 0.10
    MAX_TIME_SKEW_SEC = 0.05
    TARGET_SEQUENCE_DEG = ([90.0] * 20) + ([180.0] * 20) + ([360.0] * 10)

    def __init__(self):
        super().__init__('spin_reporter')

        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(
            self.tf_buffer,
            self,
            spin_thread=False,
        )

        self.filtered_sample = None
        self.wheel_sample = None

        self.create_subscription(
            Odometry, '/odometry/filtered', self.on_filtered_odom, qos_profile_sensor_data
        )
        self.create_subscription(
            Odometry, '/wheel_controller/odom', self.on_wheel_odom, qos_profile_sensor_data
        )

        self.last_tf_sample = None
        self.last_filtered_sample = None
        self.last_wheel_sample = None
        self.sample_count = 0
        self.trial_armed = False
        self.test_complete = False
        self.accumulated_tf_deg = 0.0
        self.accumulated_ekf_deg = 0.0
        self.accumulated_wheel_deg = 0.0
        self.last_accum_tf_yaw = None
        self.last_accum_ekf_yaw = None
        self.last_accum_wheel_yaw = None
        self.log_path = Path.cwd() / 'spin_calibration_samples.csv'
        self.wheel_ratios = []
        self.ratios_by_target = {90.0: [], 180.0: [], 360.0: []}
        self.waiting_for_sources = True
        self.last_waiting_message_sec = 0.0
        self._init_csv_log()

    def on_filtered_odom(self, msg):
        self.filtered_sample = YawSample(
            yaw=yaw_from_quat(msg.pose.pose.orientation),
            stamp_sec=stamp_to_sec(msg.header.stamp),
            source='EKF /odometry/filtered',
        )

    def on_wheel_odom(self, msg):
        self.wheel_sample = YawSample(
            yaw=yaw_from_quat(msg.pose.pose.orientation),
            stamp_sec=stamp_to_sec(msg.header.stamp),
            source='Wheel /wheel_controller/odom',
        )

    def get_tf_sample(self):
        try:
            t = self.tf_buffer.lookup_transform('odom', 'base_footprint', rclpy.time.Time())
            return YawSample(
                yaw=yaw_from_quat(t.transform.rotation),
                stamp_sec=stamp_to_sec(t.header.stamp),
                source='RViz TF (odom->base_footprint)',
            )
        except Exception:
            return None

    def _init_csv_log(self):
        with self.log_path.open('w', newline='') as handle:
            writer = csv.writer(handle)
            writer.writerow([
                'sample_id',
                'physical_target_deg',
                'accumulated_tf_deg',
                'accumulated_ekf_deg',
                'accumulated_wheel_deg',
                'final_wrapped_tf_deg',
                'final_wrapped_ekf_deg',
                'final_wrapped_wheel_deg',
                'tf_age_sec',
                'ekf_age_sec',
                'wheel_age_sec',
                'tf_ekf_skew_sec',
                'tf_wheel_skew_sec',
                'ekf_wheel_skew_sec',
                'tf_ratio',
                'ekf_ratio',
                'wheel_ratio',
            ])

    def current_target_deg(self):
        if self.sample_count >= len(self.TARGET_SEQUENCE_DEG):
            return None
        return self.TARGET_SEQUENCE_DEG[self.sample_count]

    def sample_delta_deg(self, sample, baseline):
        if sample is None or baseline is None:
            return None
        return math.degrees(wrap_pi(sample.yaw - baseline.yaw))

    def sample_age_sec(self, sample, capture_sec):
        if sample is None:
            return None
        return capture_sec - sample.stamp_sec

    def source_skews(self, tf_sample, filtered_sample, wheel_sample):
        return {
            'tf_ekf': (
                abs(tf_sample.stamp_sec - filtered_sample.stamp_sec)
                if tf_sample is not None and filtered_sample is not None
                else None
            ),
            'tf_wheel': (
                abs(tf_sample.stamp_sec - wheel_sample.stamp_sec)
                if tf_sample is not None and wheel_sample is not None
                else None
            ),
            'ekf_wheel': (
                abs(filtered_sample.stamp_sec - wheel_sample.stamp_sec)
                if filtered_sample is not None and wheel_sample is not None
                else None
            ),
        }

    def sample_is_usable(self, capture_sec, tf_sample, filtered_sample, wheel_sample):
        if tf_sample is None or filtered_sample is None or wheel_sample is None:
            return False

        ages = [
            self.sample_age_sec(tf_sample, capture_sec),
            self.sample_age_sec(filtered_sample, capture_sec),
            self.sample_age_sec(wheel_sample, capture_sec),
        ]
        if any(age is None or age > self.MAX_SAMPLE_AGE_SEC for age in ages):
            return False

        skews = self.source_skews(tf_sample, filtered_sample, wheel_sample)
        return all(
            skew is not None and skew <= self.MAX_TIME_SKEW_SEC
            for skew in skews.values()
        )

    def reset_baseline(self):
        self.last_tf_sample = self.get_tf_sample()
        self.last_filtered_sample = self.filtered_sample
        self.last_wheel_sample = self.wheel_sample
        self.accumulated_tf_deg = 0.0
        self.accumulated_ekf_deg = 0.0
        self.accumulated_wheel_deg = 0.0
        self.last_accum_tf_yaw = self.last_tf_sample.yaw if self.last_tf_sample is not None else None
        self.last_accum_ekf_yaw = self.last_filtered_sample.yaw if self.last_filtered_sample is not None else None
        self.last_accum_wheel_yaw = self.last_wheel_sample.yaw if self.last_wheel_sample is not None else None

    def update_accumulated_yaw(self):
        tf_sample = self.get_tf_sample()
        filtered_sample = self.filtered_sample
        wheel_sample = self.wheel_sample

        if tf_sample is not None:
            if self.last_accum_tf_yaw is not None:
                self.accumulated_tf_deg += abs(
                    math.degrees(wrap_pi(tf_sample.yaw - self.last_accum_tf_yaw))
                )
            self.last_accum_tf_yaw = tf_sample.yaw

        if filtered_sample is not None:
            if self.last_accum_ekf_yaw is not None:
                self.accumulated_ekf_deg += abs(
                    math.degrees(wrap_pi(filtered_sample.yaw - self.last_accum_ekf_yaw))
                )
            self.last_accum_ekf_yaw = filtered_sample.yaw

        if wheel_sample is not None:
            if self.last_accum_wheel_yaw is not None:
                self.accumulated_wheel_deg += abs(
                    math.degrees(wrap_pi(wheel_sample.yaw - self.last_accum_wheel_yaw))
                )
            self.last_accum_wheel_yaw = wheel_sample.yaw

    def missing_sources(self):
        missing = []
        if self.get_tf_sample() is None:
            missing.append('TF odom->base_footprint')
        if self.filtered_sample is None:
            missing.append('/odometry/filtered')
        if self.wheel_sample is None:
            missing.append('/wheel_controller/odom')
        return missing

    def arm_trial(self, *, announce=True):
        if self.test_complete:
            print('Run already completed. Restart the script for a fresh 50-sample set.')
            return

        self.reset_baseline()
        missing = self.missing_sources()
        if missing:
            return False

        self.trial_armed = True
        self.waiting_for_sources = False
        target_deg = self.current_target_deg()
        if announce:
            print('Trial armed.')
            print(
                f'Target schedule: 20x 90 deg, 20x 180 deg, 10x 360 deg. '
                f'Current physical target: {target_deg:.0f} deg. '
                'Rotate to the physical mark, then press c to capture.'
            )
            capture_sec = self.get_clock().now().nanoseconds * 1e-9
            self.print_timing_line('TF', self.last_tf_sample, capture_sec)
            self.print_timing_line('EKF', self.last_filtered_sample, capture_sec)
            self.print_timing_line('Wheel', self.last_wheel_sample, capture_sec)
            self.print_skew_line(
                self.last_tf_sample,
                self.last_filtered_sample,
                self.last_wheel_sample,
            )
        return True

    def maybe_auto_arm(self):
        if self.test_complete or self.trial_armed:
            return

        if self.arm_trial(announce=True):
            return

        now_sec = self.get_clock().now().nanoseconds * 1e-9
        if now_sec - self.last_waiting_message_sec < 1.0:
            return

        missing = self.missing_sources()
        if missing:
            print('Waiting for ROS data before first trial: ' + ', '.join(missing))
        else:
            print('Waiting to arm next trial...')
        self.last_waiting_message_sec = now_sec

    def print_sample_line(self, label, sample, baseline):
        if sample is None or baseline is None:
            print(f'{label}: unavailable')
            return

        delta_deg = math.degrees(wrap_pi(sample.yaw - baseline.yaw))
        print(f'{sample.source}: {delta_deg:8.2f} deg')

    def print_timing_line(self, label, sample, capture_sec):
        if sample is None:
            print(f'{label} stamp: unavailable')
            return

        age_sec = self.sample_age_sec(sample, capture_sec)
        print(f'{label} stamp: {sample.stamp_sec:.3f} s (age {age_sec:.3f} s)')

    def print_skew_line(self, tf_sample, filtered_sample, wheel_sample):
        skews = self.source_skews(tf_sample, filtered_sample, wheel_sample)
        pair_values = [
            ('TF-EKF', skews['tf_ekf']),
            ('TF-Wheel', skews['tf_wheel']),
            ('EKF-Wheel', skews['ekf_wheel']),
        ]
        pair_values = [(name, value) for name, value in pair_values if value is not None]

        if pair_values:
            print(
                'Source skew: ' +
                ', '.join(f'{name} {value:.3f} s' for name, value in pair_values)
            )
            if any(value > self.MAX_TIME_SKEW_SEC for _, value in pair_values):
                print(
                    f'Warning: source skew exceeds {self.MAX_TIME_SKEW_SEC:.3f} s; '
                    'delta comparison may be stale.'
                )
        else:
            print('Source skew: unavailable')

    def print_age_warning(self, capture_sec, *samples):
        stale_sources = []
        for sample in samples:
            if sample is None:
                continue
            age_sec = self.sample_age_sec(sample, capture_sec)
            if age_sec > self.MAX_SAMPLE_AGE_SEC:
                stale_sources.append(f'{sample.source} age {age_sec:.3f} s')

        if stale_sources:
            print('Warning: stale samples: ' + ', '.join(stale_sources))

    def log_sample(
        self,
        sample_id,
        target_deg,
        accumulated_tf_deg,
        accumulated_ekf_deg,
        accumulated_wheel_deg,
        final_wrapped_tf_deg,
        final_wrapped_ekf_deg,
        final_wrapped_wheel_deg,
        tf_age,
        ekf_age,
        wheel_age,
        skews,
        tf_ratio,
        ekf_ratio,
        wheel_ratio,
    ):
        with self.log_path.open('a', newline='') as handle:
            writer = csv.writer(handle)
            writer.writerow([
                sample_id,
                f'{target_deg:.2f}',
                f'{accumulated_tf_deg:.4f}',
                f'{accumulated_ekf_deg:.4f}',
                f'{accumulated_wheel_deg:.4f}',
                '' if final_wrapped_tf_deg is None else f'{final_wrapped_tf_deg:.4f}',
                '' if final_wrapped_ekf_deg is None else f'{final_wrapped_ekf_deg:.4f}',
                '' if final_wrapped_wheel_deg is None else f'{final_wrapped_wheel_deg:.4f}',
                '' if tf_age is None else f'{tf_age:.4f}',
                '' if ekf_age is None else f'{ekf_age:.4f}',
                '' if wheel_age is None else f'{wheel_age:.4f}',
                '' if skews['tf_ekf'] is None else f'{skews["tf_ekf"]:.4f}',
                '' if skews['tf_wheel'] is None else f'{skews["tf_wheel"]:.4f}',
                '' if skews['ekf_wheel'] is None else f'{skews["ekf_wheel"]:.4f}',
                '' if tf_ratio is None else f'{tf_ratio:.6f}',
                '' if ekf_ratio is None else f'{ekf_ratio:.6f}',
                '' if wheel_ratio is None else f'{wheel_ratio:.6f}',
            ])

    def print_ratio_summary(self, target_deg, tf_ratio, ekf_ratio, wheel_ratio):
        ratios = []
        if tf_ratio is not None:
            ratios.append(f'TF {tf_ratio:.6f}')
        if ekf_ratio is not None:
            ratios.append(f'EKF {ekf_ratio:.6f}')
        if wheel_ratio is not None:
            ratios.append(f'Wheel {wheel_ratio:.6f}')
        if ratios:
            print('Measured / physical ratio: ' + ', '.join(ratios))

        if wheel_ratio is not None:
            self.wheel_ratios.append(wheel_ratio)
            self.ratios_by_target[target_deg].append(wheel_ratio)
            overall_avg = sum(self.wheel_ratios) / len(self.wheel_ratios)
            target_avg = sum(self.ratios_by_target[target_deg]) / len(self.ratios_by_target[target_deg])
            print(
                f'Wheel ratio averages: overall {overall_avg:.6f}, '
                f'{target_deg:.0f} deg group {target_avg:.6f}'
            )

    def print_final_summary(self):
        if not self.wheel_ratios:
            print(f'No valid wheel samples logged to {self.log_path}.')
            return

        overall_avg = sum(self.wheel_ratios) / len(self.wheel_ratios)
        print(f'CSV log saved to {self.log_path}')
        print(f'Final wheel ratio average: {overall_avg:.6f}')
        for target_deg in (90.0, 180.0, 360.0):
            ratios = self.ratios_by_target[target_deg]
            if ratios:
                print(
                    f'{target_deg:.0f} deg wheel ratio average: '
                    f'{(sum(ratios) / len(ratios)):.6f} from {len(ratios)} sample(s)'
                )

    def capture_or_report(self, target_deg=None):
        if not self.trial_armed:
            return

        self.update_accumulated_yaw()

        capture_sec = self.get_clock().now().nanoseconds * 1e-9
        tf_sample = self.get_tf_sample()
        filtered_sample = self.filtered_sample
        wheel_sample = self.wheel_sample

        if tf_sample is None and filtered_sample is None and wheel_sample is None:
            print('No TF/odom data yet.')
            return

        if not self.sample_is_usable(capture_sec, tf_sample, filtered_sample, wheel_sample):
            print(
                'Capture rejected: timing is not fresh enough. '
                f'Require ages <= {self.MAX_SAMPLE_AGE_SEC:.3f} s and '
                f'skew <= {self.MAX_TIME_SKEW_SEC:.3f} s.'
            )
            self.print_timing_line('TF', tf_sample, capture_sec)
            self.print_timing_line('EKF', filtered_sample, capture_sec)
            self.print_timing_line('Wheel', wheel_sample, capture_sec)
            self.print_skew_line(tf_sample, filtered_sample, wheel_sample)
            self.print_age_warning(capture_sec, tf_sample, filtered_sample, wheel_sample)
            return

        self.sample_count += 1
        target_desc = ''
        if target_deg is not None:
            target_desc = f' target {target_deg:.0f} deg'
        print(f'\nSample {self.sample_count}/50 ({target_desc.strip() or "manual"}):')

        tf_deg = self.sample_delta_deg(tf_sample, self.last_tf_sample)
        ekf_deg = self.sample_delta_deg(filtered_sample, self.last_filtered_sample)
        wheel_deg = self.sample_delta_deg(wheel_sample, self.last_wheel_sample)
        tf_age = self.sample_age_sec(tf_sample, capture_sec)
        ekf_age = self.sample_age_sec(filtered_sample, capture_sec)
        wheel_age = self.sample_age_sec(wheel_sample, capture_sec)
        skews = self.source_skews(tf_sample, filtered_sample, wheel_sample)

        self.print_sample_line('RViz TF (odom->base_footprint)', tf_sample, self.last_tf_sample)
        self.print_sample_line('EKF /odometry/filtered', filtered_sample, self.last_filtered_sample)
        self.print_sample_line('Wheel /wheel_controller/odom', wheel_sample, self.last_wheel_sample)
        self.print_timing_line('TF', tf_sample, capture_sec)
        self.print_timing_line('EKF', filtered_sample, capture_sec)
        self.print_timing_line('Wheel', wheel_sample, capture_sec)
        self.print_skew_line(tf_sample, filtered_sample, wheel_sample)
        self.print_age_warning(capture_sec, tf_sample, filtered_sample, wheel_sample)
        if target_deg is not None:
            print(f'Physical reference target: {target_deg:.0f} deg floor mark')
            print(
                f'Accumulated yaw: TF {self.accumulated_tf_deg:.2f} deg, '
                f'EKF {self.accumulated_ekf_deg:.2f} deg, '
                f'Wheel {self.accumulated_wheel_deg:.2f} deg'
            )

        tf_ratio = (self.accumulated_tf_deg / target_deg) if target_deg else None
        ekf_ratio = (self.accumulated_ekf_deg / target_deg) if target_deg else None
        wheel_ratio = (self.accumulated_wheel_deg / target_deg) if target_deg else None
        if target_deg is not None:
            self.print_ratio_summary(target_deg, tf_ratio, ekf_ratio, wheel_ratio)
            self.log_sample(
                self.sample_count,
                target_deg,
                self.accumulated_tf_deg,
                self.accumulated_ekf_deg,
                self.accumulated_wheel_deg,
                tf_deg,
                ekf_deg,
                wheel_deg,
                tf_age,
                ekf_age,
                wheel_age,
                skews,
                tf_ratio,
                ekf_ratio,
                wheel_ratio,
            )

        self.last_tf_sample = tf_sample
        self.last_filtered_sample = filtered_sample
        self.last_wheel_sample = wheel_sample
        if self.sample_count >= len(self.TARGET_SEQUENCE_DEG):
            self.trial_armed = False
            self.test_complete = True
            print('\nCompleted 50 calibration captures.')
            self.print_final_summary()
        else:
            next_target_deg = self.current_target_deg()
            self.arm_trial(announce=False)
            print(f'Next target queued: {next_target_deg:.0f} deg. Rotate to the mark, then press c to capture.')


def get_key_nonblocking(timeout=0.05):
    dr, _, _ = select.select([sys.stdin], [], [], timeout)
    if dr:
        return sys.stdin.read(1)
    return None


def main():
    rclpy.init()
    node = SpinReporter()
    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(node)
    spin_thread = threading.Thread(target=executor.spin, daemon=True)
    spin_thread.start()

    print('Manual spin reporter started.')
    print('Waiting for TF, EKF, and wheel odom...')
    print('When the script says "Trial armed", rotate to the physical mark and press c to capture.')
    print('Press q to quit.')

    old_settings = termios.tcgetattr(sys.stdin)
    tty.setcbreak(sys.stdin.fileno())

    try:
        while rclpy.ok():
            node.maybe_auto_arm()
            key = get_key_nonblocking(0.02)
            if key in ('c', 'C'):
                node.capture_or_report(node.current_target_deg())
            elif key in ('q', 'Q'):
                break
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        executor.shutdown()
        spin_thread.join(timeout=1.0)
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
