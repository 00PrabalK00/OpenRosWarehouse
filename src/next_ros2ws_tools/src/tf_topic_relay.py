#!/usr/bin/env python3
"""
Relay TF messages between topics, optionally filtering to specific frame pairs.
"""

import rclpy
from rclpy.duration import Duration
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
from tf2_msgs.msg import TFMessage


def _build_qos(transient_local: bool) -> QoSProfile:
    return QoSProfile(
        reliability=ReliabilityPolicy.RELIABLE,
        durability=(
            DurabilityPolicy.TRANSIENT_LOCAL
            if transient_local
            else DurabilityPolicy.VOLATILE
        ),
        history=HistoryPolicy.KEEP_LAST,
        depth=100 if transient_local else 50,
    )


class TfTopicRelay(Node):
    def __init__(self):
        super().__init__('tf_topic_relay')

        self.input_tf_topic = str(
            self.declare_parameter('input_tf_topic', '/tf').value or '/tf'
        ).strip() or '/tf'
        self.output_tf_topic = str(
            self.declare_parameter('output_tf_topic', '/tf').value or '/tf'
        ).strip() or '/tf'
        self.transient_local = bool(
            self.declare_parameter('transient_local', False).value
        )
        allowed_pairs_param = self.declare_parameter('allowed_pairs', []).value or []
        self.allowed_pairs = {
            str(pair).strip()
            for pair in allowed_pairs_param
            if str(pair).strip()
        }

        qos = _build_qos(self.transient_local)
        self.tf_pub = self.create_publisher(TFMessage, self.output_tf_topic, qos)
        self.tf_sub = self.create_subscription(
            TFMessage,
            self.input_tf_topic,
            self._tf_cb,
            qos,
        )

        allow_desc = ', '.join(sorted(self.allowed_pairs)) if self.allowed_pairs else 'ALL'
        self.get_logger().info(
            f'Relaying {self.input_tf_topic} -> {self.output_tf_topic} '
            f'(pairs={allow_desc}, transient_local={self.transient_local})'
        )

    def _tf_cb(self, msg: TFMessage):
        transforms = []
        for transform in msg.transforms:
            pair = f'{transform.header.frame_id}->{transform.child_frame_id}'
            if self.allowed_pairs and pair not in self.allowed_pairs:
                continue
            transforms.append(transform)

        if not transforms:
            return

        self.tf_pub.publish(TFMessage(transforms=transforms))


def main():
    rclpy.init()
    node = TfTopicRelay()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
