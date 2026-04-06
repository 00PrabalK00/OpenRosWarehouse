#!/usr/bin/env python3
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy
from std_msgs.msg import String
from next_ros2ws_interfaces.srv import SetControlMode

# Transient-local so any node joining after startup immediately receives the
# current mode without waiting for the next publish cycle.
_MODE_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
    history=HistoryPolicy.KEEP_LAST,
    depth=1,
)


class ControlModeManager(Node):
    def __init__(self):
        super().__init__('control_mode_manager')

        self.valid_modes = ('manual', 'zones', 'sequence', 'path')
        self.aliases = {
            'auto': 'zones',
            'autonomous': 'zones',
            'nav': 'zones',
        }

        self.mode_pub = self.create_publisher(String, '/control_mode', _MODE_QOS)
        self.mode_sub = self.create_subscription(String, '/control_mode', self.mode_callback, _MODE_QOS)
        self.set_control_mode_srv = self.create_service(
            SetControlMode,
            '/set_control_mode',
            self.set_control_mode_callback
        )

        self.current_mode = None
        startup_mode = 'zones'
        self.publish_mode(startup_mode)

        self.get_logger().info('ControlModeManager started')
        self.get_logger().info('Service: /set_control_mode')
        self.get_logger().info('Topic: /control_mode')
        self.get_logger().info(f'Valid modes: {", ".join(self.valid_modes)}')

    def _normalize_mode(self, mode: str) -> str:
        mode = (mode or '').strip().lower()
        return self.aliases.get(mode, mode)

    def publish_mode(self, mode: str):
        normalized = self._normalize_mode(mode)
        msg = String()
        msg.data = normalized
        self.mode_pub.publish(msg)
        self.current_mode = normalized
        self.get_logger().info(f'Published /control_mode: {normalized}')

    def mode_callback(self, msg: String):
        mode = self._normalize_mode(msg.data)
        if mode in self.valid_modes:
            self.current_mode = mode
            return
        self.get_logger().warn(
            f'Ignoring invalid /control_mode "{msg.data}". '
            f'Allowed modes: {", ".join(self.valid_modes)}'
        )

    def set_control_mode_callback(self, request, response):
        mode = self._normalize_mode(request.mode)
        if mode not in self.valid_modes:
            response.ok = False
            response.message = f'Invalid mode. Use: {", ".join(self.valid_modes)}'
            return response

        self.publish_mode(mode)
        response.ok = True
        response.message = f'Control mode published: {mode}'
        return response


def main(args=None):
    rclpy.init(args=args)
    node = ControlModeManager()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.destroy_node()
        except Exception:
            pass
        try:
            rclpy.shutdown()
        except Exception:
            pass


if __name__ == '__main__':
    main()
