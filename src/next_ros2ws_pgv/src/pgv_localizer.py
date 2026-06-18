#!/usr/bin/env python3
"""pgv_localizer - turn a PGV Matrix Tag reading into a corrected map pose.

Pipeline (all 2D, x/y/yaw):

    T_map_base = T_map_tag  o  inverse(T_pgv_tag)  o  inverse(T_base_pgv)

where
    T_map_tag   known map pose of the detected tag (from the tag map)
    T_pgv_tag   measured tag pose in the PGV sensor frame (/pgv/pose)
    T_base_pgv  fixed mounting offset of pgv_link from base_link, read from
                URDF/TF. Parameters are only a fallback if TF is unavailable.

The corrected pose is published as a PoseWithCovarianceStamped and, when
``publish_initialpose`` is true, mirrored to /initialpose so it nudges AMCL
exactly like a manual "2D Pose Estimate". This corrects the map->odom drift
without ever touching wheel odometry.

Corrections are gated so a bad or spurious read cannot teleport the robot:
  * a code must currently be detected (NP == 0)
  * the tag id must exist in the tag map
  * the measured offset magnitude must be within range
  * the same tag must be seen for N consecutive frames (stability)
  * the corrected pose must lie inside the localization zone (if enabled)
"""

from collections import deque
import math
import os

import rclpy
from rclpy.node import Node
from rclpy.time import Time

from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue
from geometry_msgs.msg import PoseStamped, PoseWithCovarianceStamped, TransformStamped
from nav_msgs.msg import Odometry
from std_msgs.msg import Bool, Int64, String
from tf2_ros import Buffer, TransformBroadcaster, TransformException, TransformListener
from visualization_msgs.msg import Marker, MarkerArray

try:
    from next_ros2ws_pgv.tag_map import TagMapError, load_tag_map
    from next_ros2ws_pgv.transforms2d import (
        compose,
        inverse,
        quat_to_yaw,
        yaw_to_quat,
    )
except ModuleNotFoundError:  # pragma: no cover - direct source-tree tests
    from tag_map import TagMapError, load_tag_map
    from transforms2d import compose, inverse, quat_to_yaw, yaw_to_quat


class PgvLocalizer(Node):
    def __init__(self):
        super().__init__("pgv_localizer")

        self.declare_parameter("tag_map_path", "")
        # The normal mounting source is URDF/TF: base_link -> pgv_link.
        self.declare_parameter("base_frame", "base_link")
        self.declare_parameter("pgv_frame", "pgv_link")
        self.declare_parameter("use_tf_mount", True)
        self.declare_parameter("allow_param_mount_fallback", True)
        # Fallback base_link -> pgv_link mounting offset.
        self.declare_parameter("base_to_pgv_x", 0.0)
        self.declare_parameter("base_to_pgv_y", 0.0)
        self.declare_parameter("base_to_pgv_yaw_deg", 0.0)
        self.declare_parameter("use_pgv_yaw", False)
        # Derive robot heading from the R3138 relative angle only.
        # Formula: robot_yaw = pgv_angle - pgv_camera_mount_yaw
        # The R3138 reports the relative angle between camera and tag.
        # That angle encodes absolute robot heading once mount offset is removed.
        # Tag map yaw is NOT used — it is irrelevant to heading.
        # pgv_camera_mount_yaw_deg: set 180.0 if computed heading is 180° off.
        self.declare_parameter("use_pgv_angle_for_yaw", True)
        self.declare_parameter("pgv_camera_mount_yaw_deg", 180.0)
        self.declare_parameter("max_reader_age_s", 0.5)
        # Gates.
        self.declare_parameter("max_offset_m", 0.30)
        self.declare_parameter("stable_frames", 2)
        self.declare_parameter("zone_enabled", False)
        self.declare_parameter("zone_min_x", -1e9)
        self.declare_parameter("zone_min_y", -1e9)
        self.declare_parameter("zone_max_x", 1e9)
        self.declare_parameter("zone_max_y", 1e9)
        # Output.
        self.declare_parameter("map_frame", "map")
        self.declare_parameter("publish_initialpose", True)
        self.declare_parameter("position_covariance", 0.0025)  # ~5 cm std
        self.declare_parameter("yaw_covariance", 0.0025)       # ~2.9 deg std
        # Fused-mode correction (Option B): AMCL stays the primary localizer and
        # runs continuously on lidar+odom; PGV only re-anchors it when it has
        # drifted past the deadband. Readings are EMA-smoothed so reader jitter
        # never reaches AMCL, and a correction is emitted at most once per tag
        # detection event - not once per poll. This keeps the estimate
        # progressively accurate instead of resetting the particle cloud blindly.
        self.declare_parameter("amcl_pose_topic", "amcl_pose")
        self.declare_parameter("correction_deadband_xy", 0.02)       # m
        self.declare_parameter("correction_deadband_yaw_deg", 1.0)   # deg
        self.declare_parameter("smoothing_alpha", 0.4)               # EMA weight on new reading
        # Hard safety gate: a real tag read only nudges the pose. A correction
        # that would move the robot further than this from the current AMCL
        # estimate is a bad frame / wrong tag / bad mount, not a real fix - drop
        # it so AMCL can never teleport to a random point. <=0 disables.
        self.declare_parameter("max_correction_jump_m", 1.0)
        # Localization mode:
        #   "amcl"     - publish /initialpose corrections (AMCL stays primary on
        #                lidar; tags re-anchor it - hybrid).
        #   "pure_pgv" - this node owns localization: predict on wheel odometry,
        #                correct at tags, and broadcast a filtered map->odom TF
        #                continuously. AMCL must be off in this mode.
        self.declare_parameter("localization_mode", "amcl")
        self.declare_parameter("odom_topic", "/odometry/filtered")
        self.declare_parameter("odom_frame", "odom")
        self.declare_parameter("tf_publish_rate_hz", 20.0)
        self.declare_parameter("publish_odom_base_tf", True)
        # Set true when the wheel controller integrates with a 180°-inverted
        # heading (odom reports -π at physical 0°). Applies R(π) to the whole
        # odom pose so the localizer sees physically-correct x, y, yaw.
        self.declare_parameter("odom_flip_180", False)
        # Kept for config compatibility only. Pure PGV localization stays OK
        # after a valid tag fix while wheel odom carries the robot between
        # absolute Matrix Tag updates.
        self.declare_parameter("max_dead_reckon_m", 2.0)
        self.declare_parameter("max_dead_reckon_s", 10.0)

        path = self.get_parameter("tag_map_path").value
        self.base_frame = str(self.get_parameter("base_frame").value or "base_link")
        self.pgv_frame = str(self.get_parameter("pgv_frame").value or "pgv_link")
        self.use_tf_mount = bool(self.get_parameter("use_tf_mount").value)
        self.allow_param_mount_fallback = bool(
            self.get_parameter("allow_param_mount_fallback").value
        )
        self.use_pgv_yaw = bool(self.get_parameter("use_pgv_yaw").value)
        self.use_pgv_angle_for_yaw = bool(self.get_parameter("use_pgv_angle_for_yaw").value)
        self.pgv_camera_mount_yaw = math.radians(
            float(self.get_parameter("pgv_camera_mount_yaw_deg").value)
        )
        self.max_reader_age_s = float(self.get_parameter("max_reader_age_s").value)
        self.param_base_to_pgv = (
            float(self.get_parameter("base_to_pgv_x").value),
            float(self.get_parameter("base_to_pgv_y").value),
            math.radians(float(self.get_parameter("base_to_pgv_yaw_deg").value)),
        )
        self._last_tf_warn_time = None
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        self.max_offset = float(self.get_parameter("max_offset_m").value)
        self.stable_frames = int(self.get_parameter("stable_frames").value)
        self.zone_enabled = bool(self.get_parameter("zone_enabled").value)
        self.zone = (
            float(self.get_parameter("zone_min_x").value),
            float(self.get_parameter("zone_min_y").value),
            float(self.get_parameter("zone_max_x").value),
            float(self.get_parameter("zone_max_y").value),
        )
        self.map_frame = self.get_parameter("map_frame").value
        self.publish_initialpose = bool(
            self.get_parameter("publish_initialpose").value
        )
        self.position_cov = float(self.get_parameter("position_covariance").value)
        self.yaw_cov = float(self.get_parameter("yaw_covariance").value)
        self.amcl_pose_topic = str(
            self.get_parameter("amcl_pose_topic").value or "amcl_pose"
        )
        self.deadband_xy = float(self.get_parameter("correction_deadband_xy").value)
        self.deadband_yaw = math.radians(
            float(self.get_parameter("correction_deadband_yaw_deg").value)
        )
        self.smoothing_alpha = min(
            1.0, max(0.0, float(self.get_parameter("smoothing_alpha").value))
        )
        self.max_correction_jump = float(
            self.get_parameter("max_correction_jump_m").value
        )
        self.localization_mode = str(
            self.get_parameter("localization_mode").value or "amcl"
        ).strip().lower()
        self.odom_topic = str(self.get_parameter("odom_topic").value or "/odometry/filtered")
        self.odom_frame = str(self.get_parameter("odom_frame").value or "odom")
        self.odom_flip_180 = bool(self.get_parameter("odom_flip_180").value)
        self.tf_publish_rate = float(self.get_parameter("tf_publish_rate_hz").value)
        self.publish_odom_base_tf = bool(
            self.get_parameter("publish_odom_base_tf").value
        )
        self.max_dead_reckon_m = float(self.get_parameter("max_dead_reckon_m").value)
        self.max_dead_reckon_s = float(self.get_parameter("max_dead_reckon_s").value)

        self.tags = {}
        self._tag_map_path = path
        self._tag_map_mtime = None
        if not path:
            self.get_logger().error("tag_map_path parameter is empty")
        else:
            try:
                _frame, self.tags = load_tag_map(path)
                self._tag_map_mtime = os.path.getmtime(path)
                self.get_logger().info(f"Loaded {len(self.tags)} tags from {path}")
            except TagMapError as exc:
                self.get_logger().error(f"Failed to load tag map: {exc}")
            except OSError:
                pass
        # Live reload: pick up tag placements/edits without restarting PGV mode.
        self.create_timer(1.0, self._maybe_reload_tag_map)

        self.code_detected = False
        self.current_tag = None
        self._streak_tag = None
        self._streak = 0
        # Latest AMCL estimate (x, y, yaw) used as the fusion reference, and the
        # EMA-smoothed correction state for the current tag detection.
        self._amcl_pose = None
        self._ema = None          # smoothed (x, y, yaw)
        self._ema_tag = None      # tag id the EMA belongs to
        self._corrected_for_tag = None  # tag whose correction was already applied
        self._last_accepted_tag_id = -1
        self._last_accepted_pose = None
        self._last_accepted_stamp = None
        self._last_residual_m = 0.0
        # pure_pgv mode state.
        self._odom_pose = None        # latest T_odom_base (x, y, yaw)
        self._odom_msg = None         # latest wheel/EKF odometry for optional TF bridge
        self._map_odom = None         # filtered T_map_odom correction (x, y, yaw)
        self._last_tag_odom = None    # odom (x, y) at the last accepted tag
        self._dist_since_tag = 0.0    # metres dead-reckoned since last tag
        self._last_odom_xy = None
        self._last_tag_time = None
        self._localization_ok = False
        self._localization_state = "INIT"
        self._localization_detail = "waiting for first valid Matrix Tag"
        self._pending_manual_pose = None

        self.create_subscription(Bool, "pgv/code_detected", self._on_detected, 10)
        self.create_subscription(Int64, "pgv/tag", self._on_tag, 10)
        self.create_subscription(
            PoseWithCovarianceStamped, "pgv/set_pose", self._on_manual_pose, 10
        )
        # Correction is triggered by /pgv/pose, which the driver only publishes
        # when a code is actually read.
        self.create_subscription(PoseStamped, "pgv/pose", self._on_pose, 10)
        # AMCL estimate - the deadband is measured against this so PGV only
        # corrects when lidar localization has actually drifted.
        self.create_subscription(
            PoseWithCovarianceStamped, self.amcl_pose_topic, self._on_amcl, 10
        )

        self.pub_corrected = self.create_publisher(
            PoseWithCovarianceStamped, "pgv/corrected_pose", 10
        )
        self.pub_accepted_tag = self.create_publisher(Int64, "pgv/accepted_tag", 10)
        self.pub_initialpose = self.create_publisher(
            PoseWithCovarianceStamped, "initialpose", 10
        )
        self.pub_status = self.create_publisher(String, "pgv/localization_status", 10)
        # Rich acceptance/rejection telemetry (ported from the SIM localizer) so a
        # field operator can see *why* a Matrix Tag was rejected, and an RViz
        # marker confirming the corrected pose + which tag produced it.
        # Publish on the shared /diagnostics topic (same convention as
        # pgv_diagnostics) so the existing PGV PIP surfaces the acceptance /
        # rejection telemetry with no UI change.
        self.pub_diag = self.create_publisher(DiagnosticArray, "/diagnostics", 10)
        self.pub_markers = self.create_publisher(
            MarkerArray, "pgv/localization_markers", 10
        )
        self.accepted = 0
        self.rejected = 0
        self.last_rejection_reason = "none"
        self.last_rejected_tag_id = -1
        self._accept_times = deque(maxlen=200)
        self.create_timer(0.5, self._update_localization_status)
        self.create_timer(0.5, self._publish_diagnostics)

        # pure_pgv mode: own map->odom via odom prediction + tag correction.
        self.tf_broadcaster = None
        if self.localization_mode == "pure_pgv":
            self.tf_broadcaster = TransformBroadcaster(self)
            period = 1.0 / self.tf_publish_rate if self.tf_publish_rate > 0 else 0.05
            self.create_timer(period, self._broadcast_map_odom)
        # Always subscribe to odometry: it is the trusted yaw source when
        # use_pgv_yaw is false (normal operation), and the prediction source
        # in pure_pgv mode.
        self.create_subscription(Odometry, self.odom_topic, self._on_odom, 20)

        self.get_logger().info(
            "pgv_localizer ready "
            f"(mode={self.localization_mode}, "
            f"mount={self.base_frame}->{self.pgv_frame}, "
            f"use_tf_mount={self.use_tf_mount}, "
            f"param_fallback={self.param_base_to_pgv}, "
            f"max_offset={self.max_offset} m, "
            f"stable_frames={self.stable_frames}, zone_enabled={self.zone_enabled}, "
            f"publish_initialpose={self.publish_initialpose}, "
            f"fused: deadband={self.deadband_xy*1000:.0f} mm/"
            f"{math.degrees(self.deadband_yaw):.1f} deg vs {self.amcl_pose_topic}, "
            f"ema_alpha={self.smoothing_alpha})"
        )

    def _on_detected(self, msg):
        was = self.code_detected
        self.code_detected = msg.data
        # When the code leaves the camera, clear the per-event state so the next
        # tag (or a revisit of the same tag) is corrected afresh.
        if was and not self.code_detected:
            self._corrected_for_tag = None
            self._ema = None
            self._ema_tag = None
            self._reset_streak()

    def _on_amcl(self, msg):
        self._amcl_pose = (
            msg.pose.pose.position.x,
            msg.pose.pose.position.y,
            quat_to_yaw(msg.pose.pose.orientation.z, msg.pose.pose.orientation.w),
        )

    def _on_odom(self, msg):
        self._odom_msg = msg
        x = msg.pose.pose.position.x
        y = msg.pose.pose.position.y
        yaw = quat_to_yaw(msg.pose.pose.orientation.z, msg.pose.pose.orientation.w)
        if self.odom_flip_180:
            # Wheel controller integrates with a 180°-inverted heading, so
            # odom positions accumulate in the physically-opposite direction.
            # Apply R(π) to the full pose to recover physical-frame values.
            x = -x
            y = -y
            yaw = math.atan2(math.sin(yaw + math.pi), math.cos(yaw + math.pi))
        self._odom_pose = (x, y, yaw)
        # Accumulate dead-reckoned distance since the last accepted tag.
        if self._last_odom_xy is not None:
            self._dist_since_tag += math.hypot(x - self._last_odom_xy[0], y - self._last_odom_xy[1])
        self._last_odom_xy = (x, y)
        if self._pending_manual_pose is not None:
            pending = self._pending_manual_pose
            self._pending_manual_pose = None
            self._apply_manual_pose_seed(*pending)

    def _map_estimate(self):
        """Current best map-frame base pose (for the pure_pgv jump gate)."""
        if self._map_odom is not None and self._odom_pose is not None:
            return compose(self._map_odom, self._odom_pose)
        return None

    @staticmethod
    def _ang_norm(a):
        return math.atan2(math.sin(a), math.cos(a))

    def _stamp_age_s(self, stamp):
        try:
            if stamp is None:
                return None
            msg_time = Time.from_msg(stamp)
        except Exception:  # noqa: BLE001 - malformed or zero stamp
            return None
        age = (self.get_clock().now() - msg_time).nanoseconds / 1e9
        return max(0.0, float(age))

    def _trusted_yaw(self):
        if self._odom_pose is not None:
            return self._odom_pose[2]
        if self._amcl_pose is not None:
            return self._amcl_pose[2]
        return None

    def _pose_distance(self, a, b):
        if a is None or b is None:
            return None
        return math.hypot(float(a[0]) - float(b[0]), float(a[1]) - float(b[1]))

    def _status_snapshot(self):
        if self._last_accepted_stamp is None:
            return "INIT", "waiting for first valid Matrix Tag"
        age_s = self._stamp_age_s(self._last_accepted_stamp.to_msg())
        if age_s is None:
            age_s = 0.0
        tag_label = "manual seed" if self._last_accepted_tag_id < 0 else f"tag {self._last_accepted_tag_id}"
        if (
            (self.max_dead_reckon_s > 0.0 and age_s > self.max_dead_reckon_s)
            or (self.max_dead_reckon_m > 0.0 and self._dist_since_tag > self.max_dead_reckon_m)
        ):
            return (
                "STALE",
                f"{tag_label} · {self._dist_since_tag:.2f} m on odom, {age_s:.1f} s since last fix",
            )
        return (
            "OK",
            f"{tag_label} · {self._dist_since_tag:.2f} m on odom",
        )

    def _update_localization_status(self):
        state, detail = self._status_snapshot()
        self._localization_state = state
        self._localization_detail = detail
        self._localization_ok = state == "OK"
        self._publish_status(state, detail)

    def _on_tag(self, msg):
        self.current_tag = int(msg.data)

    def _on_manual_pose(self, msg):
        x = float(msg.pose.pose.position.x)
        y = float(msg.pose.pose.position.y)
        yaw = quat_to_yaw(msg.pose.pose.orientation.z, msg.pose.pose.orientation.w)
        if self.localization_mode != "pure_pgv":
            return
        if self._odom_pose is None:
            self._pending_manual_pose = (x, y, yaw)
            self.get_logger().warning(
                "Manual PGV pose seed received before odometry; applying after "
                f"first odom sample on {self.odom_topic}"
            )
            return
        self._apply_manual_pose_seed(x, y, yaw)

    def _on_pose(self, msg):
        if not self.code_detected:
            return

        tag_id = self.current_tag
        if tag_id is None:
            return
        if int(tag_id) == 0:
            self._reject("tag 0 is the PGV no-code/floor sentinel; ignoring", tag_id, gate="TAG_ID")
            return
        if tag_id not in self.tags:
            self._reject(f"tag {tag_id} not in tag map", tag_id, gate="TAG_MAP")
            return

        reader_age_s = self._stamp_age_s(msg.header.stamp)
        if (
            self.max_reader_age_s > 0.0
            and reader_age_s is not None
            and reader_age_s > self.max_reader_age_s
        ):
            self._reject(
                f"tag {tag_id} reader timestamp stale ({reader_age_s:.2f}s > "
                f"{self.max_reader_age_s:.2f}s)",
                tag_id,
                gate="STALE",
            )
            return

        # Measured tag pose in the PGV frame.
        px = msg.pose.position.x
        py = msg.pose.position.y
        if not (math.isfinite(px) and math.isfinite(py)):
            self._reject(f"tag {tag_id} has invalid offset values", tag_id, gate="INVALID")
            return
        offset = math.hypot(px, py)
        if offset > self.max_offset:
            self._reject(
                f"tag {tag_id} offset {offset:.3f} m exceeds max {self.max_offset:.3f} m",
                tag_id,
                gate="OFFSET",
            )
            return

        # Stability gate: require the same tag for N consecutive frames.
        if tag_id == self._streak_tag:
            self._streak += 1
        else:
            self._streak_tag = tag_id
            self._streak = 1
        if self._streak < self.stable_frames:
            # Stability rejections are intentionally silent per-frame (noisy).
            # Log only when we are one away from acceptance so the operator sees progress.
            if self._streak == self.stable_frames - 1:
                self.get_logger().debug(
                    f"tag {tag_id} stability {self._streak}/{self.stable_frames}; "
                    "one more frame needed"
                )
            return

        reference_pose = self._map_estimate() or self._amcl_pose or self._last_accepted_pose
        pyaw = quat_to_yaw(msg.pose.orientation.z, msg.pose.orientation.w)
        t_base_pgv = self._base_to_pgv()
        if t_base_pgv is None:
            self._reject(f"tag {tag_id} base->pgv mount unavailable", tag_id, gate="MOUNT")
            return

        if self.use_pgv_yaw:
            t_pgv_tag = (px, py, pyaw)
            t_map_tag = self.tags[tag_id]

            # T_map_base = T_map_tag o inv(T_pgv_tag) o inv(T_base_pgv)
            t_map_pgv = compose(t_map_tag, inverse(t_pgv_tag))
            t_map_base = compose(t_map_pgv, inverse(t_base_pgv))
            bx, by, byaw = t_map_base
        else:
            # Position-only mode. Snap base_link to the tag's map coordinate,
            # offset by the rear-camera mount translation.
            tx, ty, _tyaw = self.tags[tag_id]

            if self.use_pgv_angle_for_yaw:
                # robot_heading = sensor_frame_offset - pgv_angle
                #
                # The R3138 reports the tag's angle in the SENSOR frame, so as
                # the camera rotates one way the reported angle rotates the
                # OTHER way (a frame-relative inversion). On this diff-drive
                # robot the camera is rear-mounted, so a turn swings the rear
                # camera opposite to the robot's heading change and the sign
                # flip becomes visible: without negation the robot turned right
                # but the corrected pose spun left. Negating pgv_angle makes the
                # correction's rotation direction match the robot's. At a static
                # 180° this is invisible (±180° coincide), which is why the bug
                # only showed up mid-turn.
                #
                # pgv_camera_mount_yaw_deg is the additive offset that lines up
                # the static heading; the negation fixes the turn DIRECTION.
                mount_yaw = self.pgv_camera_mount_yaw
                byaw = math.atan2(
                    math.sin(mount_yaw - pyaw),
                    math.cos(mount_yaw - pyaw),
                )
                odom_yaw = self._odom_pose[2] if self._odom_pose is not None else float('nan')
                self.get_logger().info(
                    f"[PGV YAW] tag={tag_id} "
                    f"pgv_angle={math.degrees(pyaw):.1f}° "
                    f"mount_yaw(cfg)={math.degrees(mount_yaw):.1f}° "
                    f"→ computed={math.degrees(byaw):.1f}° "
                    f"odom={math.degrees(odom_yaw):.1f}°"
                )
            else:
                map_est = self._map_estimate()
                byaw = map_est[2] if map_est is not None else (
                    self._odom_pose[2] if self._odom_pose is not None else 0.0
                )

            # Only the mount TRANSLATION matters for placing the camera origin;
            # strip any rotation so it cannot flip the 0.52 m offset.
            mount_xy = (t_base_pgv[0], t_base_pgv[1], 0.0)
            bx, by, _ = compose((tx, ty, byaw), inverse(mount_xy))

        residual = 0.0
        if reference_pose is not None:
            residual = math.hypot(bx - reference_pose[0], by - reference_pose[1])
            if self.max_correction_jump > 0.0 and residual > self.max_correction_jump:
                self._reject(
                    f"tag {tag_id} correction jump {residual:.2f} m exceeds "
                    f"max {self.max_correction_jump:.2f} m",
                    tag_id,
                    gate="JUMP",
                )
                return

        if self.zone_enabled:
            zmnx, zmny, zmxx, zmxy = self.zone
            if not (zmnx <= bx <= zmxx and zmny <= by <= zmxy):
                self._reject(
                    f"tag {tag_id} corrected pose ({bx:.2f}, {by:.2f}) outside "
                    "localization zone",
                    tag_id,
                    gate="ZONE",
                )
                return

        # EMA-smooth the raw reading so reader jitter never reaches AMCL. The
        # filter is per detection event - reset when a different tag appears.
        if self._ema is None or self._ema_tag != tag_id:
            self._ema = (bx, by, byaw)
            self._ema_tag = tag_id
        else:
            a = self.smoothing_alpha
            ex, ey, eyaw = self._ema
            sx = a * bx + (1.0 - a) * ex
            sy = a * by + (1.0 - a) * ey
            # Wrap-safe angular EMA.
            syaw = eyaw + a * self._ang_norm(byaw - eyaw)
            self._ema = (sx, sy, self._ang_norm(syaw))
        fx, fy, fyaw = self._ema

        # Accepted reading: a real tag passed every gate and produced a fix.
        self.accepted += 1
        self.last_rejection_reason = "none"
        self.last_rejected_tag_id = -1
        self._last_accepted_tag_id = tag_id
        self._last_accepted_pose = (fx, fy, fyaw)
        self._last_accepted_stamp = self.get_clock().now()
        self._last_residual_m = residual
        self._accept_times.append(self.get_clock().now().nanoseconds / 1e9)

        # Always publish the smoothed estimate for telemetry / RViz.
        self._publish_corrected_pose(fx, fy, fyaw, msg.header.stamp)
        self._publish_markers(fx, fy, fyaw, tag_id, msg.header.stamp)
        # Publish accepted tag so route_follower can use it.
        accepted_msg = Int64()
        accepted_msg.data = int(tag_id)
        self.pub_accepted_tag.publish(accepted_msg)
        self._update_localization_status()
        self._publish_diagnostics()

        # pure_pgv mode: correct the map->odom transform from this tag.
        if self.localization_mode == "pure_pgv":
            self._apply_pure_pgv(fx, fy, fyaw, tag_id)
            return

        # Fused-mode gate: only re-anchor AMCL when it has actually drifted past
        # the deadband, and at most once per tag detection event. This refines
        # the estimate where it has drifted instead of resetting it every frame.
        if not self.publish_initialpose:
            return
        if self._corrected_for_tag == tag_id:
            return

        if self._amcl_pose is not None:
            ax, ay, ayaw = self._amcl_pose
            derr = math.hypot(fx - ax, fy - ay)
            yerr = abs(self._ang_norm(fyaw - ayaw))
            if self.max_correction_jump > 0.0 and derr > self.max_correction_jump:
                self.get_logger().warning(
                    f"Tag {tag_id}: correction to ({fx:.2f}, {fy:.2f}) is "
                    f"{derr:.2f} m from AMCL (> {self.max_correction_jump:.2f} m); "
                    "rejecting as a bad read / wrong tag / bad mount"
                )
                self._reset_streak()
                return
            if derr <= self.deadband_xy and yerr <= self.deadband_yaw:
                self.get_logger().debug(
                    f"Tag {tag_id}: AMCL within deadband "
                    f"(d={derr*1000:.0f} mm, yaw={math.degrees(yerr):.1f} deg); "
                    "no correction"
                )
                self._corrected_for_tag = tag_id
                return
            drift_info = (
                f" (AMCL drift d={derr*1000:.0f} mm, yaw={math.degrees(yerr):.1f} deg)"
            )
        else:
            drift_info = " (no AMCL pose yet; bootstrapping)"

        self.pub_initialpose.publish(self._build_pose_msg(fx, fy, fyaw, msg.header.stamp))
        self._corrected_for_tag = tag_id
        self.get_logger().info(
            f"Correction from tag {tag_id}: x={fx:.3f} y={fy:.3f} "
            f"yaw={math.degrees(fyaw):.1f} deg{drift_info}"
        )

    def _build_pose_msg(self, x, y, yaw, stamp):
        out = PoseWithCovarianceStamped()
        out.header.stamp = stamp
        out.header.frame_id = self.map_frame
        out.pose.pose.position.x = x
        out.pose.pose.position.y = y
        qz, qw = yaw_to_quat(yaw)
        out.pose.pose.orientation.z = qz
        out.pose.pose.orientation.w = qw
        cov = [0.0] * 36
        cov[0] = self.position_cov    # x
        cov[7] = self.position_cov    # y
        cov[35] = self.yaw_cov        # yaw
        out.pose.covariance = cov
        return out

    def _publish_corrected_pose(self, x, y, yaw, stamp):
        self.pub_corrected.publish(self._build_pose_msg(x, y, yaw, stamp))

    def _maybe_reload_tag_map(self):
        """Reload the tag map when the file changes (live placement, no restart)."""
        if not self._tag_map_path:
            return
        try:
            mtime = os.path.getmtime(self._tag_map_path)
        except OSError:
            return
        if self._tag_map_mtime is not None and mtime == self._tag_map_mtime:
            return
        try:
            _frame, tags = load_tag_map(self._tag_map_path)
        except TagMapError as exc:
            self.get_logger().warning(f"Tag map reload failed: {exc}")
            return
        self._tag_map_mtime = mtime
        self.tags = tags
        self.get_logger().info(f"Reloaded {len(self.tags)} tags (live update)")

    def _publish_status(self, state, detail=""):
        msg = String()
        msg.data = state if not detail else f"{state}: {detail}"
        self.pub_status.publish(msg)

    def _reject(self, reason, tag_id=-1, gate=""):
        """Record a rejected reading, log it, and reset the stability streak."""
        self.rejected += 1
        self.last_rejection_reason = reason
        self.last_rejected_tag_id = int(tag_id)
        log_msg = f"REJECTED [{gate}] {reason}"
        self.get_logger().warning(log_msg)
        self._reset_streak()
        # Publish diagnostics immediately on rejection so operators see why.
        self._publish_diagnostics()

    def _current_detected_tag_id(self):
        return int(self.current_tag) if (self.code_detected and self.current_tag is not None) else -1

    def _rolling_hz(self):
        if len(self._accept_times) < 2:
            return 0.0
        span = self._accept_times[-1] - self._accept_times[0]
        return (len(self._accept_times) - 1) / span if span > 1e-6 else 0.0

    def _publish_diagnostics(self):
        diag = DiagnosticArray()
        diag.header.stamp = self.get_clock().now().to_msg()
        status = DiagnosticStatus()
        status.name = "pgv_localizer"
        status.hardware_id = "pgv"
        # Compute level from state.
        if self._localization_state == "STALE":
            status.level = DiagnosticStatus.WARN
        elif self.last_rejection_reason != "none":
            status.level = DiagnosticStatus.WARN
        elif self._localization_state == "OK":
            status.level = DiagnosticStatus.OK
        else:
            status.level = DiagnosticStatus.WARN

        status.message = (
            f"localizer {self._localization_state.lower()}"
            if self.last_rejection_reason == "none"
            else self.last_rejection_reason
        )
        accepted_age_s = 0.0
        if self._last_accepted_stamp is not None:
            age = self._stamp_age_s(self._last_accepted_stamp.to_msg())
            accepted_age_s = 0.0 if age is None else age
        accepted_pose = self._last_accepted_pose or (None, None, None)
        status.values = [
            KeyValue(key="tag_map_loaded", value="true" if self.tags else "false"),
            KeyValue(key="number_of_tags_loaded", value=str(len(self.tags))),
            KeyValue(key="localization_mode", value=self.localization_mode),
            KeyValue(key="use_pgv_yaw", value="true" if self.use_pgv_yaw else "false"),
            KeyValue(key="localization_status", value=self._localization_state),
            KeyValue(key="localization_detail", value=self._localization_detail),
            KeyValue(key="current_detected_tag_id", value=str(self._current_detected_tag_id())),
            KeyValue(
                key="last_accepted_tag_id",
                value="manual" if self._last_accepted_tag_id < 0 else str(self._last_accepted_tag_id),
            ),
            KeyValue(
                key="last_accepted_pose",
                value=(
                    "none"
                    if accepted_pose[0] is None
                    else f"{accepted_pose[0]:.3f},{accepted_pose[1]:.3f},{math.degrees(accepted_pose[2]):.1f}"
                ),
            ),
            KeyValue(key="last_accepted_age_s", value=f"{accepted_age_s:.2f}"),
            KeyValue(key="distance_since_fix_m", value=f"{self._dist_since_tag:.3f}"),
            KeyValue(key="last_residual_m", value=f"{self._last_residual_m:.3f}"),
            KeyValue(key="accepted_detections_count", value=str(self.accepted)),
            KeyValue(key="rejected_detections_count", value=str(self.rejected)),
            KeyValue(key="last_rejection_reason", value=self.last_rejection_reason),
            KeyValue(key="last_rejected_tag_id", value=str(self.last_rejected_tag_id)),
            KeyValue(key="pgv_update_rate_hz", value=f"{self._rolling_hz():.2f}"),
        ]
        diag.status.append(status)
        self.pub_diag.publish(diag)

    def _publish_markers(self, x, y, yaw, tag_id, stamp):
        markers = MarkerArray()
        arrow = Marker()
        arrow.header.frame_id = self.map_frame
        arrow.header.stamp = stamp
        arrow.ns = "pgv_pose"
        arrow.id = 0
        arrow.type = Marker.ARROW
        arrow.action = Marker.ADD
        arrow.pose.position.x = x
        arrow.pose.position.y = y
        arrow.pose.position.z = 0.15
        qz, qw = yaw_to_quat(yaw)
        arrow.pose.orientation.z = qz
        arrow.pose.orientation.w = qw
        arrow.scale.x = 0.40
        arrow.scale.y = 0.10
        arrow.scale.z = 0.10
        arrow.color.r = 0.05
        arrow.color.g = 0.95
        arrow.color.b = 0.95
        arrow.color.a = 1.0
        markers.markers.append(arrow)

        text = Marker()
        text.header = arrow.header
        text.ns = "pgv_status"
        text.id = 1
        text.type = Marker.TEXT_VIEW_FACING
        text.action = Marker.ADD
        text.pose.position.x = x
        text.pose.position.y = y
        text.pose.position.z = 0.40
        text.scale.z = 0.12
        text.color.r = text.color.g = text.color.b = 1.0
        text.color.a = 1.0
        text.text = f"PGV tag {tag_id}"
        markers.markers.append(text)
        self.pub_markers.publish(markers)

    def _apply_manual_pose_seed(self, x, y, yaw):
        """Seed pure-PGV map->odom from a clicked map-frame base pose."""
        if self._odom_pose is None:
            self._pending_manual_pose = (x, y, yaw)
            return
        self._map_odom = compose((x, y, yaw), inverse(self._odom_pose))
        self._dist_since_tag = 0.0
        self._last_tag_time = self.get_clock().now()
        self._localization_ok = True
        self._corrected_for_tag = None
        self._ema = None
        self._ema_tag = None
        self._last_accepted_tag_id = -1
        self._last_accepted_pose = (x, y, yaw)
        self._last_accepted_stamp = self.get_clock().now()
        self._last_residual_m = 0.0
        self._publish_corrected_pose(x, y, yaw, self.get_clock().now().to_msg())
        self._publish_status("OK", "manual seed")
        self._update_localization_status()
        self.get_logger().info(
            f"Manual pure_pgv seed: x={x:.3f} y={y:.3f} "
            f"yaw={math.degrees(yaw):.1f} deg"
        )

    def _apply_pure_pgv(self, fx, fy, fyaw, tag_id):
        """Correct the map->odom transform from a tag (predict-correct model)."""
        if self._odom_pose is None:
            self.get_logger().warning(
                f"Tag {tag_id}: no odometry on {self.odom_topic} yet; "
                "cannot anchor map->odom"
            )
            return
        # In pure-PGV mode, a valid mapped Matrix Tag is the localization source
        # of truth. Do not reject it because the dead-reckoned estimate disagrees:
        # that is exactly when the tag must re-anchor map->odom. Bad reads are
        # already gated earlier by checksum, known tag id, reader offset, and
        # optional localization zone.

        # T_map_odom = T_map_base o inverse(T_odom_base)
        new_corr = compose((fx, fy, fyaw), inverse(self._odom_pose))
        if self._map_odom is None or tag_id != self._corrected_for_tag:
            # First fix, or a NEW mapped tag: a defined tag is absolute ground
            # truth, so snap map->odom fully onto it - no blending. This is what
            # makes "I am on tag N" mean exactly tag N's pose, killing the
            # cross-tag drift from partially-applied corrections.
            self._map_odom = new_corr
        else:
            # Same tag seen again while parked on it: EMA-filter reader jitter
            # only, so the locked pose does not twitch frame to frame.
            a = self.smoothing_alpha
            cx, cy, cyaw = self._map_odom
            nx = a * new_corr[0] + (1.0 - a) * cx
            ny = a * new_corr[1] + (1.0 - a) * cy
            nyaw = cyaw + a * self._ang_norm(new_corr[2] - cyaw)
            self._map_odom = (nx, ny, self._ang_norm(nyaw))

        self._dist_since_tag = 0.0
        self._last_tag_time = self.get_clock().now()
        self._localization_ok = True
        self._corrected_for_tag = tag_id
        self._publish_status("OK", f"tag {tag_id}")
        self.get_logger().info(
            f"Correction from tag {tag_id}: x={fx:.3f} y={fy:.3f} "
            f"yaw={math.degrees(fyaw):.1f} deg (pure_pgv map->odom)"
        )

    def _broadcast_map_odom(self):
        """Timer: continuously publish map->odom + a dead-reckoning watchdog."""
        if self._map_odom is None:
            self._localization_state = "INIT"
            self._localization_detail = "no tag fix yet"
            self._localization_ok = False
            self._publish_status("INIT", "no tag fix yet")
            return
        mx, my, myaw = self._map_odom
        tf = TransformStamped()
        tf.header.stamp = self.get_clock().now().to_msg()
        tf.header.frame_id = self.map_frame
        tf.child_frame_id = self.odom_frame
        tf.transform.translation.x = mx
        tf.transform.translation.y = my
        qz, qw = yaw_to_quat(myaw)
        tf.transform.rotation.z = qz
        tf.transform.rotation.w = qw
        self.tf_broadcaster.sendTransform(tf)
        self._broadcast_odom_base()

        # Publish the freshness state separately from the TF bridge.
        self._update_localization_status()

    def _broadcast_odom_base(self):
        """Optionally mirror the odometry topic into TF for PGV-only stacks."""
        if (
            not self.publish_odom_base_tf
            or self.tf_broadcaster is None
            or self._odom_msg is None
        ):
            return
        child = str(self._odom_msg.child_frame_id or "").strip()
        if not child:
            child = "base_footprint"
        parent = str(self._odom_msg.header.frame_id or "").strip() or self.odom_frame

        tf = TransformStamped()
        tf.header.stamp = self.get_clock().now().to_msg()
        tf.header.frame_id = parent
        tf.child_frame_id = child
        tf.transform.translation.x = float(self._odom_msg.pose.pose.position.x)
        tf.transform.translation.y = float(self._odom_msg.pose.pose.position.y)
        tf.transform.translation.z = float(self._odom_msg.pose.pose.position.z)
        tf.transform.rotation = self._odom_msg.pose.pose.orientation
        self.tf_broadcaster.sendTransform(tf)

    def _base_to_pgv(self):
        """Return the 2D base_link -> pgv_link transform.

        The URDF/static TF is the source of truth. Parameters exist only so the
        node can still run in bench tests or during bringup before TF is alive.
        """
        if self.use_tf_mount:
            try:
                transform = self.tf_buffer.lookup_transform(
                    self.base_frame,
                    self.pgv_frame,
                    Time(),
                )
                return transform_to_pose2d(transform)
            except TransformException as exc:
                if self.allow_param_mount_fallback:
                    self._warn_tf_fallback(exc)
                    return self.param_base_to_pgv
                self.get_logger().warning(
                    f"Missing TF {self.base_frame}->{self.pgv_frame}; "
                    f"cannot apply PGV correction: {exc}"
                )
                return None
        return self.param_base_to_pgv

    def _warn_tf_fallback(self, exc):
        now = self.get_clock().now()
        if (
            self._last_tf_warn_time is not None
            and (now - self._last_tf_warn_time).nanoseconds < 5_000_000_000
        ):
            return
        self._last_tf_warn_time = now
        self.get_logger().warning(
            f"TF {self.base_frame}->{self.pgv_frame} unavailable; using "
            f"parameter fallback {self.param_base_to_pgv}: {exc}"
        )

    def _reset_streak(self):
        self._streak_tag = None
        self._streak = 0


def transform_to_pose2d(transform):
    """Convert a TransformStamped to an (x, y, yaw) tuple."""
    trans = transform.transform.translation
    rot = transform.transform.rotation
    yaw = math.atan2(
        2.0 * (rot.w * rot.z + rot.x * rot.y),
        1.0 - 2.0 * (rot.y * rot.y + rot.z * rot.z),
    )
    return (float(trans.x), float(trans.y), yaw)


def main(args=None):
    rclpy.init(args=args)
    node = PgvLocalizer()
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
