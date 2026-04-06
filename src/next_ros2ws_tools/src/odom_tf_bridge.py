#!/usr/bin/env python3
"""
Publish a TF transform from a nav_msgs/Odometry topic onto a configurable TF topic.
"""

import rclpy
from nav_msgs.msg import Odometry
from rclpy.node import Node
from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
from tf2_msgs.msg import TFMessage
from geometry_msgs.msg import TransformStamped


_ODOM_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    history=HistoryPolicy.KEEP_LAST,
    depth=20,
)

_TF_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    history=HistoryPolicy.KEEP_LAST,
    depth=50,
)


class OdomTfBridge(Node):
    def __init__(self):
        super().__init__('odom_tf_bridge')

        self.input_odom_topic = str(
            self.declare_parameter('input_odom_topic', '/odometry/filtered').value
            or '/odometry/filtered'
        ).strip() or '/odometry/filtered'
        self.output_tf_topic = str(
            self.declare_parameter('output_tf_topic', '/tf').value or '/tf'
        ).strip() or '/tf'
        self.parent_frame = str(
            self.declare_parameter('parent_frame', '').value or ''
        ).strip()
        self.child_frame = str(
            self.declare_parameter('child_frame', '').value or ''
        ).strip()
        self.use_current_time = bool(
            self.declare_parameter('use_current_time', False).value
        )
        if self.use_current_time:
            self.get_logger().warn(
                'Parameter use_current_time=true is deprecated and ignored; '
                'odom_tf_bridge always publishes the source odometry timestamp.',
            )
        self.use_current_time = False

        self.tf_pub = self.create_publisher(TFMessage, self.output_tf_topic, _TF_QOS)
        self.odom_sub = self.create_subscription(
            Odometry,
            self.input_odom_topic,
            self._odom_cb,
            _ODOM_QOS,
        )

        self.get_logger().info(
            f'Bridging {self.input_odom_topic} -> {self.output_tf_topic} '
            f'(parent={self.parent_frame or "<odom.header.frame_id>"}, '
            f'child={self.child_frame or "<odom.child_frame_id>"}, '
            f'stamp=odom.header.stamp)'
        )

    def _odom_cb(self, msg: Odometry):
        parent_frame = self.parent_frame or str(msg.header.frame_id or '').strip()
        child_frame = self.child_frame or str(msg.child_frame_id or '').strip()
        if not parent_frame or not child_frame:
            self.get_logger().warn(
                'Skipping odom TF publish because frame_id or child_frame_id is empty',
                throttle_duration_sec=5.0,
            )
            return

        transform = TransformStamped()
        transform.header = msg.header
        transform.header.frame_id = parent_frame
        transform.child_frame_id = child_frame
        transform.transform.translation.x = float(msg.pose.pose.position.x)
        transform.transform.translation.y = float(msg.pose.pose.position.y)
        transform.transform.translation.z = float(msg.pose.pose.position.z)
        transform.transform.rotation = msg.pose.pose.orientation
        self.tf_pub.publish(TFMessage(transforms=[transform]))


def main():
    rclpy.init()
    node = OdomTfBridge()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
