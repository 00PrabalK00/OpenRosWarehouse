#!/usr/bin/env python3
"""pgv_calibrator - estimate the base_link -> pgv_link mounting transform.

Operator procedure (matches the standard PGV calibration routine):

  1. Place the robot so the PGV sees the center of one fixed floor code.
  2. Call the start service.
  3. Drive the robot forward and backward a few times.
  4. Spin the robot in place one full turn (keep the code in view).
  5. Call the finish service.

On finish the node solves the 2D hand-eye problem, writes the result to a YAML
file (ready to drop into pgv_localization.yaml) and renders a fit-quality plot
(measured vs fitted) so the calibration can be checked visually. While
collecting it also publishes the recovered code positions as RViz markers.

Services (std_srvs/Trigger):
    ~/start   begin collecting samples
    ~/finish  stop, solve, save YAML + plot
    ~/cancel  discard the current session

This node is standalone and never auto-applies the result; the operator copies
the saved values into the localizer params.
"""

import math
import os

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSProfile
from rclpy.time import Time

import yaml

from geometry_msgs.msg import Point, PoseStamped
from nav_msgs.msg import Odometry
from std_msgs.msg import Bool, Int64
from std_srvs.srv import Trigger
from tf2_ros import Buffer, TransformException, TransformListener
from visualization_msgs.msg import Marker, MarkerArray

from next_ros2ws_pgv.calibration import fit_base_to_pgv, make_fit_plot
from next_ros2ws_pgv.transforms2d import compose, quat_to_yaw


class PgvCalibrator(Node):
    def __init__(self):
        super().__init__("pgv_calibrator")

        self.declare_parameter("odom_topic", "/odom")
        self.declare_parameter("output_yaml", "/tmp/pgv_calibration.yaml")
        self.declare_parameter("plot_path", "/tmp/pgv_calibration_fit.png")
        self.declare_parameter("min_samples", 15)
        # Only keep a new sample once the robot has moved/turned at least this
        # much, so a stationary robot does not flood the buffer.
        self.declare_parameter("min_trans_delta_m", 0.01)
        self.declare_parameter("min_rot_delta_deg", 1.0)
        self.declare_parameter("odom_frame", "odom")
        self.declare_parameter("base_frame", "base_link")
        self.declare_parameter("pgv_frame", "pgv_link")
        self.declare_parameter("use_tf_mount", True)
        self.declare_parameter("base_to_pgv_x", -0.52)
        self.declare_parameter("base_to_pgv_y", 0.0)
        self.declare_parameter("base_to_pgv_yaw_deg", 0.0)

        self.odom_topic = self.get_parameter("odom_topic").value
        self.output_yaml = self.get_parameter("output_yaml").value
        self.plot_path = self.get_parameter("plot_path").value
        self.min_samples = int(self.get_parameter("min_samples").value)
        self.min_trans = float(self.get_parameter("min_trans_delta_m").value)
        self.min_rot = math.radians(float(self.get_parameter("min_rot_delta_deg").value))
        self.odom_frame = self.get_parameter("odom_frame").value
        self.base_frame = self.get_parameter("base_frame").value
        self.pgv_frame = self.get_parameter("pgv_frame").value
        self.use_tf_mount = bool(self.get_parameter("use_tf_mount").value)
        self.param_base_to_pgv = (
            float(self.get_parameter("base_to_pgv_x").value),
            float(self.get_parameter("base_to_pgv_y").value),
            math.radians(float(self.get_parameter("base_to_pgv_yaw_deg").value)),
        )

        self.active = False
        self.code_detected = False
        self.current_tag = None
        self.latest_odom = None       # (x, y, yaw)
        self.A = []                   # odom->base samples
        self.B = []                   # pgv->tag samples
        self._last_kept = None

        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

        self.create_subscription(Odometry, self.odom_topic, self._on_odom, 20)
        self.create_subscription(Bool, "pgv/code_detected", self._on_detected, 10)
        self.create_subscription(Int64, "pgv/tag", self._on_tag, 10)
        self.create_subscription(PoseStamped, "pgv/pose", self._on_pose, 20)

        latched = QoSProfile(depth=1)
        latched.durability = QoSDurabilityPolicy.TRANSIENT_LOCAL
        self.pub_markers = self.create_publisher(
            MarkerArray, "pgv/calibration_samples", latched
        )

        self.create_service(Trigger, "~/start", self._srv_start)
        self.create_service(Trigger, "~/finish", self._srv_finish)
        self.create_service(Trigger, "~/cancel", self._srv_cancel)

        self.get_logger().info(
            f"pgv_calibrator ready. odom='{self.odom_topic}'. "
            f"mount_reference={self._format_mount(self.param_base_to_pgv)} "
            f"(param fallback, TF {self.base_frame}->{self.pgv_frame} preferred). "
            "Call ~/start, drive forward/back, spin once, then ~/finish."
        )

    # --- subscriptions -------------------------------------------------
    def _on_odom(self, msg):
        q = msg.pose.pose.orientation
        yaw = quat_to_yaw(q.z, q.w)
        self.latest_odom = (
            msg.pose.pose.position.x,
            msg.pose.pose.position.y,
            yaw,
        )

    def _on_detected(self, msg):
        self.code_detected = msg.data

    def _on_tag(self, msg):
        self.current_tag = int(msg.data)

    def _on_pose(self, msg):
        if not self.active or not self.code_detected or self.latest_odom is None:
            return
        base = self.latest_odom
        if self._last_kept is not None:
            dx = base[0] - self._last_kept[0]
            dy = base[1] - self._last_kept[1]
            dyaw = abs(math.atan2(
                math.sin(base[2] - self._last_kept[2]),
                math.cos(base[2] - self._last_kept[2]),
            ))
            if math.hypot(dx, dy) < self.min_trans and dyaw < self.min_rot:
                return
        pyaw = quat_to_yaw(msg.pose.orientation.z, msg.pose.orientation.w)
        self.A.append(base)
        self.B.append((msg.pose.position.x, msg.pose.position.y, pyaw))
        self._last_kept = base
        self._publish_markers()
        if len(self.A) % 5 == 0:
            self.get_logger().info(f"Collected {len(self.A)} samples")

    # --- services ------------------------------------------------------
    def _srv_start(self, _req, resp):
        self.A = []
        self.B = []
        self._last_kept = None
        self.active = True
        resp.success = True
        resp.message = "Calibration started. Drive forward/back, then spin once."
        self.get_logger().info(resp.message)
        return resp

    def _srv_cancel(self, _req, resp):
        self.active = False
        self.A = []
        self.B = []
        self._last_kept = None
        resp.success = True
        resp.message = "Calibration cancelled; samples discarded."
        self.get_logger().info(resp.message)
        return resp

    def _srv_finish(self, _req, resp):
        self.active = False
        n = len(self.A)
        if n < self.min_samples:
            resp.success = False
            resp.message = (
                f"Only {n} samples (< min_samples {self.min_samples}); "
                "not enough motion. Try again with more driving + a full spin."
            )
            self.get_logger().warning(resp.message)
            return resp
        try:
            mount_guess, mount_source = self._current_mount_reference()
            result = fit_base_to_pgv(self.A, self.B, x0=mount_guess)
        except Exception as exc:  # noqa: BLE001
            resp.success = False
            resp.message = f"Fit failed: {exc}"
            self.get_logger().error(resp.message)
            return resp

        mount_delta = self._mount_delta(result, mount_guess)
        self._write_yaml(result, mount_guess, mount_source, mount_delta)
        try:
            make_fit_plot(self.A, self.B, result, self.plot_path)
            plot_note = f" Plot: {self.plot_path}"
        except Exception as exc:  # noqa: BLE001
            plot_note = f" (plot failed: {exc})"

        resp.success = True
        resp.message = (
            f"base->pgv x={result['x']*1000:.1f}mm y={result['y']*1000:.1f}mm "
            f"yaw={math.degrees(result['yaw']):.2f}deg "
            f"(delta from {mount_source}: dx={mount_delta[0]*1000:.1f}mm "
            f"dy={mount_delta[1]*1000:.1f}mm "
            f"dyaw={math.degrees(mount_delta[2]):.2f}deg) "
            f"RMS={result['rms_position_m']*1000:.1f}mm n={n}. "
            f"Saved {self.output_yaml}.{plot_note}"
        )
        self.get_logger().info(resp.message)
        return resp

    # --- helpers -------------------------------------------------------
    def _current_mount_reference(self):
        if self.use_tf_mount:
            try:
                tf = self.tf_buffer.lookup_transform(
                    self.base_frame, self.pgv_frame, Time()
                )
                tr = tf.transform.translation
                q = tf.transform.rotation
                return (tr.x, tr.y, quat_to_yaw(q.z, q.w)), "urdf_tf"
            except TransformException as exc:
                self.get_logger().warning(
                    f"Could not read TF {self.base_frame}->{self.pgv_frame}; "
                    f"using base_to_pgv_* params as calibration reference: {exc}"
                )
        return self.param_base_to_pgv, "params"

    @staticmethod
    def _wrap(theta):
        return math.atan2(math.sin(theta), math.cos(theta))

    def _mount_delta(self, result, reference):
        return (
            result["x"] - reference[0],
            result["y"] - reference[1],
            self._wrap(result["yaw"] - reference[2]),
        )

    def _format_mount(self, mount):
        return (
            f"x={mount[0]*1000:.1f}mm y={mount[1]*1000:.1f}mm "
            f"yaw={math.degrees(mount[2]):.2f}deg"
        )

    def _write_yaml(self, result, mount_reference, mount_source, mount_delta):
        doc = {
            "pgv_localizer": {
                "ros__parameters": {
                    "base_to_pgv_x": round(result["x"], 6),
                    "base_to_pgv_y": round(result["y"], 6),
                    "base_to_pgv_yaw_deg": round(math.degrees(result["yaw"]), 4),
                }
            },
            "calibration_quality": {
                "rms_position_m": round(result["rms_position_m"], 6),
                "rms_angle_deg": round(result["rms_angle_deg"], 4),
                "num_samples": result["num_samples"],
                "initial_guess_source": mount_source,
            },
            "mount_reference": {
                "source": mount_source,
                "base_to_pgv_x": round(mount_reference[0], 6),
                "base_to_pgv_y": round(mount_reference[1], 6),
                "base_to_pgv_yaw_deg": round(math.degrees(mount_reference[2]), 4),
            },
            "mount_delta_from_reference": {
                "dx_m": round(mount_delta[0], 6),
                "dy_m": round(mount_delta[1], 6),
                "dyaw_deg": round(math.degrees(mount_delta[2]), 4),
            },
        }
        os.makedirs(os.path.dirname(os.path.abspath(self.output_yaml)), exist_ok=True)
        with open(self.output_yaml, "w") as handle:
            yaml.safe_dump(doc, handle, default_flow_style=False, sort_keys=False)

    def _publish_markers(self):
        # Recovered code position per sample (uncalibrated), so the operator can
        # watch the spread shrink as motion is added.
        arr = MarkerArray()
        now = self.get_clock().now().to_msg()
        pts = Marker()
        pts.header.frame_id = self.odom_frame
        pts.header.stamp = now
        pts.ns = "pgv_calib_samples"
        pts.id = 0
        pts.type = Marker.POINTS
        pts.action = Marker.ADD
        pts.scale.x = 0.02
        pts.scale.y = 0.02
        pts.color.r = 1.0
        pts.color.g = 0.6
        pts.color.b = 0.0
        pts.color.a = 1.0
        for a, b in zip(self.A, self.B):
            c = compose(a, b)  # uncalibrated tag-in-odom
            pts.points.append(Point(x=c[0], y=c[1], z=0.0))
        arr.markers.append(pts)
        self.pub_markers.publish(arr)


def main(args=None):
    rclpy.init(args=args)
    node = PgvCalibrator()
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
