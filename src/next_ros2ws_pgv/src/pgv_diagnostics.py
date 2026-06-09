#!/usr/bin/env python3
"""pgv_diagnostics - health monitor for the PGV localization stack.

Publishes a standard diagnostic_msgs/DiagnosticArray on /diagnostics at a
fixed rate. It mirrors the alarm conditions a production PGV stack reports:

  * stale / no data        -> ERROR  (RS-485 comm lost or driver dead)
  * no code detected        -> WARN   (PGV cannot find codes)
  * unknown / wrong tag id  -> WARN   (tag not in map -> mismatch)
  * healthy, code seen      -> OK

Reads the same tag map as the localizer so "unknown tag" is meaningful.
"""

import rclpy
from rclpy.node import Node

from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue
from geometry_msgs.msg import PoseStamped
from std_msgs.msg import Bool, Int64

from next_ros2ws_pgv.tag_map import TagMapError, load_tag_map


class PgvDiagnostics(Node):
    def __init__(self):
        super().__init__("pgv_diagnostics")

        self.declare_parameter("tag_map_path", "")
        self.declare_parameter("stale_timeout_s", 2.0)
        self.declare_parameter("publish_rate_hz", 1.0)

        path = self.get_parameter("tag_map_path").value
        self.stale_timeout = float(self.get_parameter("stale_timeout_s").value)
        rate = float(self.get_parameter("publish_rate_hz").value)

        self.tags = {}
        if path:
            try:
                _frame, self.tags = load_tag_map(path)
            except TagMapError as exc:
                self.get_logger().error(f"Failed to load tag map: {exc}")

        self.last_detected_time = None
        self.code_detected = False
        self.last_tag = None
        self.unknown_tag = False

        self.create_subscription(Bool, "pgv/code_detected", self._on_detected, 10)
        self.create_subscription(Int64, "pgv/tag", self._on_tag, 10)
        self.create_subscription(PoseStamped, "pgv/pose", self._on_pose, 10)

        self.pub = self.create_publisher(DiagnosticArray, "diagnostics", 10)
        period = 1.0 / rate if rate > 0 else 1.0
        self.timer = self.create_timer(period, self._tick)

    def _now(self):
        return self.get_clock().now().nanoseconds / 1e9

    def _on_detected(self, msg):
        self.last_detected_time = self._now()
        self.code_detected = msg.data

    def _on_tag(self, msg):
        self.last_tag = int(msg.data)
        if self.tags:
            self.unknown_tag = self.last_tag != 0 and self.last_tag not in self.tags

    def _on_pose(self, _msg):
        # Pose only arrives when a code is read; used here only as liveness.
        self.last_detected_time = self._now()

    def _tick(self):
        status = DiagnosticStatus()
        status.name = "pgv: localization"
        status.hardware_id = "pgv_r3138"

        values = []
        now = self._now()
        if self.last_detected_time is None:
            age = None
        else:
            age = now - self.last_detected_time

        if age is None or age > self.stale_timeout:
            status.level = DiagnosticStatus.ERROR
            status.message = "No PGV data (stale / RS-485 comm lost)"
        elif self.code_detected and self.unknown_tag:
            status.level = DiagnosticStatus.WARN
            status.message = f"Detected tag {self.last_tag} not in tag map"
        elif not self.code_detected:
            status.level = DiagnosticStatus.WARN
            status.message = "PGV online but no code detected"
        else:
            status.level = DiagnosticStatus.OK
            status.message = f"OK, tag {self.last_tag}"

        values.append(KeyValue(key="code_detected", value=str(self.code_detected)))
        values.append(KeyValue(key="last_tag", value=str(self.last_tag)))
        values.append(
            KeyValue(key="data_age_s", value=("n/a" if age is None else f"{age:.2f}"))
        )
        values.append(KeyValue(key="tags_in_map", value=str(len(self.tags))))
        status.values = values

        arr = DiagnosticArray()
        arr.header.stamp = self.get_clock().now().to_msg()
        arr.status.append(status)
        self.pub.publish(arr)


def main(args=None):
    rclpy.init(args=args)
    node = PgvDiagnostics()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
