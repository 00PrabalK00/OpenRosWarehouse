#!/usr/bin/env python3
"""tag_map_server - holds known Matrix Tag poses and visualizes them.

Loads the tag map YAML and publishes a latched MarkerArray so the codes show
up in RViz at their true map poses. Each tag gets a square marker, a direction
arrow, and an id label so operators can verify both placement and orientation.
It is the single source of truth that ``pgv_localizer`` and ``pgv_diagnostics``
also read from the same file.

Standalone: nothing in the workspace is required to subscribe to it.
"""

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSProfile

from visualization_msgs.msg import Marker, MarkerArray

from next_ros2ws_pgv.tag_map import TagMapError, load_tag_map
from next_ros2ws_pgv.transforms2d import yaw_to_quat


class TagMapServer(Node):
    def __init__(self):
        super().__init__("tag_map_server")

        self.declare_parameter("tag_map_path", "")
        self.declare_parameter("marker_size", 0.15)
        self.declare_parameter("arrow_length", 0.28)

        path = self.get_parameter("tag_map_path").value
        self.marker_size = float(self.get_parameter("marker_size").value)
        self.arrow_length = float(self.get_parameter("arrow_length").value)

        self.frame_id = "map"
        self.tags = {}
        if not path:
            self.get_logger().error("tag_map_path parameter is empty")
        else:
            try:
                self.frame_id, self.tags = load_tag_map(path)
                self.get_logger().info(
                    f"Loaded {len(self.tags)} tags from {path} (frame '{self.frame_id}')"
                )
            except TagMapError as exc:
                self.get_logger().error(f"Failed to load tag map: {exc}")

        # Latched so late RViz subscribers still get the markers.
        latched = QoSProfile(depth=1)
        latched.durability = QoSDurabilityPolicy.TRANSIENT_LOCAL
        self.pub = self.create_publisher(MarkerArray, "pgv/tag_markers", latched)

        self._publish_markers()
        # Republish periodically so it survives RViz restarts without relying
        # solely on transient-local delivery.
        self.timer = self.create_timer(5.0, self._publish_markers)

    def _publish_markers(self):
        arr = MarkerArray()
        now = self.get_clock().now().to_msg()
        for tag_id, (x, y, yaw) in sorted(self.tags.items()):
            cube = Marker()
            cube.header.frame_id = self.frame_id
            cube.header.stamp = now
            cube.ns = "pgv_tags"
            cube.id = tag_id
            cube.type = Marker.CUBE
            cube.action = Marker.ADD
            cube.pose.position.x = x
            cube.pose.position.y = y
            cube.pose.position.z = 0.005
            qz, qw = yaw_to_quat(yaw)
            cube.pose.orientation.z = qz
            cube.pose.orientation.w = qw
            cube.scale.x = self.marker_size
            cube.scale.y = self.marker_size
            cube.scale.z = 0.01
            cube.color.r = 0.1
            cube.color.g = 0.6
            cube.color.b = 1.0
            cube.color.a = 0.9
            arr.markers.append(cube)

            arrow = Marker()
            arrow.header.frame_id = self.frame_id
            arrow.header.stamp = now
            arrow.ns = "pgv_tag_directions"
            arrow.id = tag_id
            arrow.type = Marker.ARROW
            arrow.action = Marker.ADD
            arrow.pose.position.x = x
            arrow.pose.position.y = y
            arrow.pose.position.z = 0.035
            arrow.pose.orientation.z = qz
            arrow.pose.orientation.w = qw
            arrow.scale.x = self.arrow_length
            arrow.scale.y = 0.035
            arrow.scale.z = 0.035
            arrow.color.r = 0.1
            arrow.color.g = 1.0
            arrow.color.b = 0.35
            arrow.color.a = 0.95
            arr.markers.append(arrow)

            label = Marker()
            label.header.frame_id = self.frame_id
            label.header.stamp = now
            label.ns = "pgv_tag_labels"
            label.id = tag_id
            label.type = Marker.TEXT_VIEW_FACING
            label.action = Marker.ADD
            label.pose.position.x = x
            label.pose.position.y = y
            label.pose.position.z = 0.15
            label.scale.z = 0.1
            label.color.r = 1.0
            label.color.g = 1.0
            label.color.b = 1.0
            label.color.a = 1.0
            label.text = str(tag_id)
            arr.markers.append(label)

        self.pub.publish(arr)


def main(args=None):
    rclpy.init(args=args)
    node = TagMapServer()
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
