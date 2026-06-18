import math
from typing import Optional, Tuple

import rclpy
from gazebo_msgs.msg import EntityState
from gazebo_msgs.srv import SetEntityState
from geometry_msgs.msg import TransformStamped, Twist
from nav_msgs.msg import Odometry
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from tf2_ros import TransformBroadcaster

from reflector_localization.reflector_localizer import quaternion_from_yaw


class KinematicSimDriver(Node):
    """Deterministic sim-only base driver.

    The supplied URDF uses simple wheel/caster collisions that do not produce
    reliable Gazebo traction in all environments. This node keeps the simulation
    controllable by integrating /cmd_vel, publishing odometry, and moving the
    Gazebo entity so mounted sensors traverse the warehouse.
    """

    def __init__(self) -> None:
        super().__init__("kinematic_sim_driver")
        self.declare_parameter("entity_name", "reflector_agv")
        self.declare_parameter("cmd_vel_topic", "cmd_vel")
        self.declare_parameter("odom_topic", "odom")
        self.declare_parameter("odom_frame", "odom")
        self.declare_parameter("base_frame", "base_link")
        self.declare_parameter("publish_tf", True)
        self.declare_parameter("update_rate_hz", 30.0)
        self.declare_parameter("command_timeout_sec", 0.5)
        self.declare_parameter("initial_x", 0.0)
        self.declare_parameter("initial_y", 0.0)
        self.declare_parameter("initial_z", 0.05)
        self.declare_parameter("initial_yaw", 0.0)

        self.entity_name = self.get_parameter("entity_name").value
        self.odom_frame = self.get_parameter("odom_frame").value
        self.base_frame = self.get_parameter("base_frame").value
        self.z = float(self.get_parameter("initial_z").value)
        self.x = float(self.get_parameter("initial_x").value)
        self.y = float(self.get_parameter("initial_y").value)
        self.yaw = float(self.get_parameter("initial_yaw").value)
        self.cmd = Twist()
        self.last_cmd_sec: Optional[float] = None
        self.last_update_sec: Optional[float] = None
        self.pending_state_request = False

        self.odom_pub = self.create_publisher(Odometry, self.get_parameter("odom_topic").value, 30)
        self.tf_broadcaster = TransformBroadcaster(self)
        self.state_client = self.create_client(SetEntityState, "/set_entity_state")
        self.create_subscription(Twist, self.get_parameter("cmd_vel_topic").value, self._on_cmd_vel, 10)

        period = 1.0 / float(self.get_parameter("update_rate_hz").value)
        self.create_timer(period, self._tick)
        self.get_logger().info(f"Kinematic sim driver controlling Gazebo entity [{self.entity_name}]")

    def _on_cmd_vel(self, msg: Twist) -> None:
        self.cmd = msg
        self.last_cmd_sec = self._now_sec()

    def _tick(self) -> None:
        now_sec = self._now_sec()
        if self.last_update_sec is None:
            self.last_update_sec = now_sec
            self._publish_outputs(now_sec, 0.0, 0.0)
            return

        dt = max(0.0, min(0.2, now_sec - self.last_update_sec))
        self.last_update_sec = now_sec
        linear_x, angular_z = self._active_command(now_sec)

        if dt > 0.0:
            self.x += linear_x * math.cos(self.yaw) * dt
            self.y += linear_x * math.sin(self.yaw) * dt
            self.yaw = self._normalize(self.yaw + angular_z * dt)

        self._publish_outputs(now_sec, linear_x, angular_z)
        self._send_gazebo_state(linear_x, angular_z)

    def _active_command(self, now_sec: float) -> Tuple[float, float]:
        timeout = float(self.get_parameter("command_timeout_sec").value)
        if self.last_cmd_sec is None or now_sec - self.last_cmd_sec > timeout:
            return (0.0, 0.0)
        return (float(self.cmd.linear.x), float(self.cmd.angular.z))

    def _publish_outputs(self, now_sec: float, linear_x: float, angular_z: float) -> None:
        stamp = self.get_clock().now().to_msg()
        odom = Odometry()
        odom.header.stamp = stamp
        odom.header.frame_id = self.odom_frame
        odom.child_frame_id = self.base_frame
        odom.pose.pose.position.x = self.x
        odom.pose.pose.position.y = self.y
        odom.pose.pose.position.z = 0.0
        odom.pose.pose.orientation = quaternion_from_yaw(self.yaw)
        odom.twist.twist.linear.x = linear_x
        odom.twist.twist.angular.z = angular_z
        odom.pose.covariance[0] = 0.02
        odom.pose.covariance[7] = 0.02
        odom.pose.covariance[35] = 0.04
        odom.twist.covariance[0] = 0.02
        odom.twist.covariance[35] = 0.04
        self.odom_pub.publish(odom)

        if bool(self.get_parameter("publish_tf").value):
            transform = TransformStamped()
            transform.header.stamp = stamp
            transform.header.frame_id = self.odom_frame
            transform.child_frame_id = self.base_frame
            transform.transform.translation.x = self.x
            transform.transform.translation.y = self.y
            transform.transform.translation.z = 0.0
            transform.transform.rotation = quaternion_from_yaw(self.yaw)
            self.tf_broadcaster.sendTransform(transform)

    def _send_gazebo_state(self, linear_x: float, angular_z: float) -> None:
        if self.pending_state_request:
            return
        if not self.state_client.service_is_ready():
            self.state_client.wait_for_service(timeout_sec=0.0)
            return

        state = EntityState()
        state.name = self.entity_name
        state.reference_frame = "world"
        state.pose.position.x = self.x
        state.pose.position.y = self.y
        state.pose.position.z = self.z
        state.pose.orientation = quaternion_from_yaw(self.yaw)
        state.twist.linear.x = linear_x
        state.twist.angular.z = angular_z

        request = SetEntityState.Request()
        request.state = state
        self.pending_state_request = True
        future = self.state_client.call_async(request)
        future.add_done_callback(self._on_state_set)

    def _on_state_set(self, future) -> None:
        self.pending_state_request = False
        try:
            result = future.result()
        except Exception as exc:  # pragma: no cover - runtime diagnostics only.
            self.get_logger().warn(f"Failed to set Gazebo entity state: {exc}")
            return
        if not result.success:
            self.get_logger().debug(f"Gazebo set_entity_state rejected update: {result.status_message}")

    def _now_sec(self) -> float:
        return self.get_clock().now().nanoseconds * 1e-9

    @staticmethod
    def _normalize(angle: float) -> float:
        while angle > math.pi:
            angle -= 2.0 * math.pi
        while angle < -math.pi:
            angle += 2.0 * math.pi
        return angle


def main(args=None) -> None:
    rclpy.init(args=args)
    node = KinematicSimDriver()
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
