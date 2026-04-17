#!/usr/bin/env python3
"""
Compatibility shim for the retired shelf_inserter node.

Shelf insertion now runs through zone_manager + Nav2 controller motion intents,
so this node intentionally never publishes Twist.
"""

import rclpy
from rclpy.node import Node
from std_msgs.msg import String
from std_srvs.srv import Trigger


class ShelfInserter(Node):
    IDLE = 'IDLE'
    ABORTED = 'ABORTED'

    def __init__(self):
        super().__init__('shelf_inserter')
        self.state_pub = self.create_publisher(String, '/shelf_simple/state', 10)
        self.start_srv = self.create_service(
            Trigger, '/shelf_simple/start', self._start_cb
        )
        self.abort_srv = self.create_service(
            Trigger, '/shelf_simple/abort', self._abort_cb
        )
        self.state = self.IDLE
        self._publish_state()
        self.get_logger().warn(
            'shelf_inserter is disabled: shelf docking now runs through '
            'zone_manager and the Nav2 controller only.'
        )

    def _publish_state(self) -> None:
        msg = String()
        msg.data = str(self.state)
        self.state_pub.publish(msg)

    def _start_cb(self, _request, response):
        self.state = self.ABORTED
        self._publish_state()
        response.success = False
        response.message = (
            'shelf_inserter is deprecated; request shelf docking through '
            'zone_manager / NavigationArbitrator instead.'
        )
        return response

    def _abort_cb(self, _request, response):
        self.state = self.ABORTED
        self._publish_state()
        response.success = True
        response.message = 'No active shelf_inserter motion to abort.'
        return response


def main(args=None):
    rclpy.init(args=args)
    node = ShelfInserter()
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
