import itertools
from typing import List, Tuple

import rclpy
from geometry_msgs.msg import Twist
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node


RouteSegment = Tuple[float, float, float]


class TestRouteDriver(Node):
    def __init__(self) -> None:
        super().__init__("test_route_driver")
        self.declare_parameter("cmd_vel_topic", "cmd_vel")
        self.declare_parameter("auto_start", False)
        self.declare_parameter("loop", True)

        self.publisher = self.create_publisher(Twist, self.get_parameter("cmd_vel_topic").value, 10)
        self.enabled = bool(self.get_parameter("auto_start").value)
        self.loop = bool(self.get_parameter("loop").value)
        self.route: List[RouteSegment] = [
            (8.0, 0.35, 0.00),
            (2.8, 0.00, 0.55),
            (6.0, 0.32, 0.00),
            (2.0, 0.00, -0.45),
            (9.0, 0.36, 0.00),
            (2.6, 0.00, 0.55),
            (5.0, 0.30, 0.00),
            (2.2, 0.00, 0.55),
        ]
        self.segment_iter = itertools.cycle(self.route) if self.loop else iter(self.route)
        self.current_segment = None
        self.segment_end_sec = 0.0
        self.create_timer(0.1, self._tick)
        if self.enabled:
            self.get_logger().info("Auto route driver enabled")
        else:
            self.get_logger().info("Auto route driver idle; use auto_route:=true to enable it")

    def _tick(self) -> None:
        if not self.enabled:
            return
        now = self.get_clock().now().nanoseconds * 1e-9
        if self.current_segment is None or now >= self.segment_end_sec:
            try:
                self.current_segment = next(self.segment_iter)
            except StopIteration:
                self.enabled = False
                self.publisher.publish(Twist())
                self.get_logger().info("Auto route complete")
                return
            duration, linear_x, angular_z = self.current_segment
            self.segment_end_sec = now + duration
            self.get_logger().info(
                f"Route segment: {duration:.1f}s linear={linear_x:.2f} angular={angular_z:.2f}"
            )

        _, linear_x, angular_z = self.current_segment
        msg = Twist()
        msg.linear.x = linear_x
        msg.angular.z = angular_z
        self.publisher.publish(msg)


def main(args=None) -> None:
    rclpy.init(args=args)
    node = TestRouteDriver()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        if rclpy.ok():
            node.publisher.publish(Twist())
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
