import random
import math
from typing import Optional, Tuple

import rclpy
from geometry_msgs.msg import PoseWithCovarianceStamped, TransformStamped
from nav_msgs.msg import Odometry
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from tf2_ros import TransformBroadcaster

try:
    from next_ros2ws_reflector.reflector_localizer import quaternion_from_yaw, yaw_from_quaternion
except ImportError:
    from reflector_localizer import quaternion_from_yaw, yaw_from_quaternion

def normalize_angle(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle

Pose2D = Tuple[float, float, float]


class MapOdomBroadcaster(Node):
    def __init__(self) -> None:
        super().__init__("map_odom_broadcaster")
        self.declare_parameter("map_frame", "map")
        self.declare_parameter("odom_frame", "odom")
        self.declare_parameter("base_frame", "base_link")
        self.declare_parameter("odom_topic", "odom")
        self.declare_parameter("corrected_pose_topic", "reflector_pose")
        self.declare_parameter("publish_identity_until_first_correction", True)
        self.declare_parameter("broadcast_rate_hz", 20.0)
        self.declare_parameter("max_pos_correction_per_update", 0.05)
        self.declare_parameter("max_yaw_correction_per_update", math.radians(3.0))

        self.map_frame = self.get_parameter("map_frame").value
        self.odom_frame = self.get_parameter("odom_frame").value
        self.max_pos_step = self.get_parameter("max_pos_correction_per_update").value
        self.max_yaw_step = self.get_parameter("max_yaw_correction_per_update").value
        
        self.last_odom_base: Optional[Pose2D] = None
        self.last_map_base: Optional[Pose2D] = None
        
        self.target_map_odom: Optional[Pose2D] = None
        self.current_map_odom: Optional[Pose2D] = None

        self.broadcaster = TransformBroadcaster(self)
        self.create_subscription(Odometry, self.get_parameter("odom_topic").value, self._on_odom, 30)
        self.create_subscription(
            PoseWithCovarianceStamped,
            self.get_parameter("corrected_pose_topic").value,
            self._on_corrected_pose,
            10,
        )
        period = 1.0 / float(self.get_parameter("broadcast_rate_hz").value)
        self.create_timer(period, self._broadcast)

    def _on_odom(self, msg: Odometry) -> None:
        pose = msg.pose.pose
        self.last_odom_base = (pose.position.x, pose.position.y, yaw_from_quaternion(pose.orientation))
        if random.random() < 0.01: self.get_logger().info(f"Odom Received: {self.last_odom_base}")
        if getattr(self, '_needs_map_odom_update', False):
            self._update_map_odom()

    def _on_corrected_pose(self, msg: PoseWithCovarianceStamped) -> None:
        pose = msg.pose.pose
        self.get_logger().info(f"RECEIVED CORRECTED POSE: x={pose.position.x:.2f}, y={pose.position.y:.2f}")
        self.last_map_base = (pose.position.x, pose.position.y, yaw_from_quaternion(pose.orientation))
        self._needs_map_odom_update = True
        self._update_map_odom()

    def _update_map_odom(self) -> None:
        if self.last_map_base is None or self.last_odom_base is None:
            return
        map_x, map_y, map_yaw = self.last_map_base
        odom_x, odom_y, odom_yaw = self.last_odom_base
        map_odom_yaw = normalize_angle(map_yaw - odom_yaw)
        c = math.cos(map_odom_yaw)
        s = math.sin(map_odom_yaw)
        rotated_odom_x = c * odom_x - s * odom_y
        rotated_odom_y = s * odom_x + c * odom_y
        self.target_map_odom = (map_x - rotated_odom_x, map_y - rotated_odom_y, map_odom_yaw)
        self._needs_map_odom_update = False

    def _broadcast(self) -> None:
        if self.target_map_odom is None:
            if self.current_map_odom is not None:
                # Keep broadcasting last known transform instead of snapping to identity
                pass
            else:
                if random.random() < 0.01: self.get_logger().info("Broadcasting identity (no target yet)")
                if not bool(self.get_parameter("publish_identity_until_first_correction").value):
                    return
                self.current_map_odom = (0.0, 0.0, 0.0)
        else:
            if self.current_map_odom is None:
                self.current_map_odom = self.target_map_odom
            else:
                cx, cy, cyaw = self.current_map_odom
                tx, ty, tyaw = self.target_map_odom
                
                dx = tx - cx
                dy = ty - cy
                dyaw = normalize_angle(tyaw - cyaw)
                
                # Apply jump limits, but SNAP if it's a massive jump (manual reloc)
                dist = math.hypot(dx, dy)
                if dist > 0.5 or abs(dyaw) > 0.5:
                    pass # Bypass limits for manual relocalization snap
                else:
                    if dist > self.max_pos_step:
                        dx = dx / dist * self.max_pos_step
                        dy = dy / dist * self.max_pos_step
                    
                    if abs(dyaw) > self.max_yaw_step:
                        dyaw = math.copysign(self.max_yaw_step, dyaw)
                    
                self.current_map_odom = (cx + dx, cy + dy, normalize_angle(cyaw + dyaw))

        if self.current_map_odom is None:
            return

        transform = TransformStamped()
        transform.header.stamp = self.get_clock().now().to_msg()
        transform.header.frame_id = self.map_frame
        transform.child_frame_id = self.odom_frame
        transform.transform.translation.x = self.current_map_odom[0]
        transform.transform.translation.y = self.current_map_odom[1]
        transform.transform.translation.z = 0.0
        transform.transform.rotation = quaternion_from_yaw(self.current_map_odom[2])
        self.broadcaster.sendTransform(transform)


def main(args=None) -> None:
    rclpy.init(args=args)
    node = MapOdomBroadcaster()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
