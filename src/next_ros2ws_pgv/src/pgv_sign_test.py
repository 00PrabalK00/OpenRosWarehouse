#!/usr/bin/env python3
"""PGV sign verification helper.

Run this node while the robot is positioned over a known Matrix Tag and manually
jog the robot in base_link directions. It prints the PGV raw reading so you can
determine the correct pose_*_sign settings for pgv_reader.

Procedure:
1. Place robot so the PGV sees a tag.
2. Run: ros2 run next_ros2ws_pgv pgv_sign_test
3. Jog the robot FORWARD (+X in base_link). Watch pgv/pose.position.x.
   - If x INCREASES, pose_x_sign should be +1.0
   - If x DECREASES, pose_x_sign should be -1.0
4. Jog the robot LEFT (+Y in base_link). Watch pgv/pose.position.y.
   - If y INCREASES, pose_y_sign should be +1.0
   - If y DECREASES, pose_y_sign should be -1.0
5. Rotate robot COUNTER-CLOCKWISE (+yaw). Watch pgv/angle_deg.
   - If angle INCREASES, pose_yaw_sign should be +1.0
   - If angle DECREASES, pose_yaw_sign should be -1.0
6. Set the signs in pgv_localization.yaml and restart pgv_reader.

The goal: pgv_link is a clean ROS frame aligned with base_link:
  +X forward, +Y left, +Z up, yaw CCW positive.
"""

import rclpy
from rclpy.node import Node
from geometry_msgs.msg import PoseStamped
from std_msgs.msg import Bool, Float32, Int64


class PgvSignTest(Node):
    def __init__(self):
        super().__init__("pgv_sign_test")
        self.code_detected = False
        self.current_tag = 0
        self.pose = None
        self.angle_deg = 0.0

        self.create_subscription(Bool, "pgv/code_detected", self._on_detected, 10)
        self.create_subscription(Int64, "pgv/tag", self._on_tag, 10)
        self.create_subscription(PoseStamped, "pgv/pose", self._on_pose, 10)
        self.create_subscription(Float32, "pgv/angle_deg", self._on_angle, 10)

        self.create_timer(0.5, self._print)
        self.get_logger().info(
            "PGV sign test running. Place robot over a tag, then jog forward/left/rotate "
            "and observe the printed signs."
        )

    def _on_detected(self, msg):
        self.code_detected = msg.data

    def _on_tag(self, msg):
        self.current_tag = int(msg.data)

    def _on_pose(self, msg):
        self.pose = msg.pose

    def _on_angle(self, msg):
        self.angle_deg = float(msg.data)

    def _print(self):
        if not self.code_detected or self.pose is None:
            self.get_logger().info("No code detected — position robot over a tag.")
            return
        self.get_logger().info(
            f"tag={self.current_tag}  "
            f"x={self.pose.position.x:+.4f}  "
            f"y={self.pose.position.y:+.4f}  "
            f"angle_deg={self.angle_deg:+.1f}"
        )


def main(args=None):
    rclpy.init(args=args)
    node = PgvSignTest()
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
