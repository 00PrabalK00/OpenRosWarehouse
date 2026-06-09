#!/usr/bin/env python3
"""route_follower - graph-based PGV AGV motion.

This is the "fixed route" controller for PGV mode: instead of free-space Nav2
planning, the robot follows a known graph of floor Matrix Tags. The mission
planner hands it an ordered list of tag ids (which already encodes any branch
decisions at junctions); the follower drives edge by edge between them.

Per edge (tag A -> tag B):
  * predict pose from wheel odometry (map->base via TF, fed by the pure_pgv
    localizer's map->odom + odom->base),
  * steer to null the cross-track error to the A->B line and the heading error
    to the edge direction,
  * detect arrival when the PGV reads tag B (localization snaps the pose),
  * advance to the next edge.

Failure logic (the part sparse PGV needs):
  * if the robot overshoots the expected edge length without seeing tag B, it
    slows, then stops and reports PGV_LOST,
  * if a wrong tag is read, it stops,
  * cross-track beyond max_lateral_error stops the run.

Velocity is published on a dedicated lane (default /cmd_vel_tracker) so it never
fights Nav2's /cmd_vel.

Command interface (std_msgs/String JSON on /pgv_route/command):
    {"action": "follow", "tags": [101, 102, 105]}
    {"action": "stop"}
Status is published on /pgv_route/status (String).
A std_srvs/Trigger on ~/stop also halts the run.
"""

import json
import math
import os

import rclpy
from rclpy.node import Node
from rclpy.time import Time

from geometry_msgs.msg import Twist
from nav_msgs.msg import Odometry
from std_msgs.msg import Bool, Int64, String
from std_srvs.srv import Trigger
from tf2_ros import Buffer, TransformException, TransformListener

try:
    from next_ros2ws_pgv.tag_map import TagMapError, load_tag_map
    from next_ros2ws_pgv.transforms2d import quat_to_yaw
except ModuleNotFoundError:  # pragma: no cover - direct source-tree tests
    from tag_map import TagMapError, load_tag_map
    from transforms2d import quat_to_yaw


# State machine states.
IDLE = "IDLE"
APPROACH = "APPROACH"   # off-tag start: drive current pose -> first route tag
FOLLOW = "FOLLOW"
RECOVERY = "RECOVERY"
DONE = "DONE"
STOPPED = "STOPPED"
LOST = "LOST"


def _ang_norm(a):
    return math.atan2(math.sin(a), math.cos(a))


class RouteFollower(Node):
    def __init__(self):
        super().__init__("route_follower")

        self.declare_parameter("tag_map_path", "")
        self.declare_parameter("map_frame", "map")
        self.declare_parameter("base_frame", "base_link")
        self.declare_parameter("cmd_vel_topic", "/cmd_vel_tracker")
        self.declare_parameter("odom_topic", "/odometry/filtered")
        self.declare_parameter("control_rate_hz", 20.0)

        # Control gains / limits.
        self.declare_parameter("cruise_speed", 0.25)        # m/s on a straight edge
        self.declare_parameter("approach_speed", 0.10)      # m/s near a tag
        self.declare_parameter("max_angular", 0.6)          # rad/s
        self.declare_parameter("k_lateral", 1.5)            # cross-track -> yaw rate
        self.declare_parameter("k_heading", 1.2)            # heading err -> yaw rate
        self.declare_parameter("arrive_radius", 0.20)       # m: "near tag B"
        self.declare_parameter("max_lateral_error", 0.12)   # m: abort if exceeded
        self.declare_parameter("max_yaw_error_deg", 45.0)   # deg: creep if exceeded
        # Missed-tag failure: how far past the edge length to keep looking.
        self.declare_parameter("overshoot_margin", 0.40)    # m
        self.declare_parameter("recovery_creep_m", 0.25)    # m of slow search
        self.declare_parameter("target_confirm_frames", 3)
        self.declare_parameter("target_confirm_min_duration_s", 0.30)

        self.map_frame = str(self.get_parameter("map_frame").value or "map")
        self.base_frame = str(self.get_parameter("base_frame").value or "base_link")
        self.cmd_vel_topic = str(self.get_parameter("cmd_vel_topic").value or "/cmd_vel_tracker")
        self.odom_topic = str(self.get_parameter("odom_topic").value or "/odometry/filtered")
        self.control_rate = float(self.get_parameter("control_rate_hz").value)
        self.cruise_speed = float(self.get_parameter("cruise_speed").value)
        self.approach_speed = float(self.get_parameter("approach_speed").value)
        self.max_angular = float(self.get_parameter("max_angular").value)
        self.k_lateral = float(self.get_parameter("k_lateral").value)
        self.k_heading = float(self.get_parameter("k_heading").value)
        self.arrive_radius = float(self.get_parameter("arrive_radius").value)
        self.max_lateral_error = float(self.get_parameter("max_lateral_error").value)
        self.max_yaw_error = math.radians(float(self.get_parameter("max_yaw_error_deg").value))
        self.overshoot_margin = float(self.get_parameter("overshoot_margin").value)
        self.recovery_creep_m = float(self.get_parameter("recovery_creep_m").value)
        self.target_confirm_frames = max(1, int(self.get_parameter("target_confirm_frames").value))
        self.target_confirm_min_duration_s = max(
            0.0, float(self.get_parameter("target_confirm_min_duration_s").value)
        )

        # Tag map (node poses).
        self.tags = {}
        path = self.get_parameter("tag_map_path").value
        self._tag_map_path = path
        self._tag_map_mtime = None
        if path:
            try:
                _frame, self.tags = load_tag_map(path)
                self._tag_map_mtime = os.path.getmtime(path)
                self.get_logger().info(f"Loaded {len(self.tags)} tags from {path}")
            except TagMapError as exc:
                self.get_logger().error(f"Failed to load tag map: {exc}")
            except OSError:
                pass
        else:
            self.get_logger().error("tag_map_path is empty; route follower cannot run")
        # Live reload: pick up tag placements/edits without restarting PGV mode.
        self.create_timer(1.0, self._maybe_reload_tag_map)

        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

        # Run state.
        self.state = IDLE
        self.route = []            # ordered tag ids
        self.virtual_start_xy = None  # (x, y) anchor for an off-tag APPROACH edge
        self.edge_index = 0        # current edge = route[edge_index] -> route[edge_index+1]
        self.current_tag = None
        self.accepted_tag = None   # only tags that passed localizer gates
        self.code_detected = False
        self.odom_xy = None
        self.edge_start_dist = 0.0
        self.travelled = 0.0       # cumulative odom distance
        self.recovery_start = 0.0
        self.current_segment_start = None
        self.current_segment_target = None
        self.previous_confirmed_tag = None
        self.target_tag = None
        self.allowed_tags = []
        self.unexpected_tag_counter = 0
        self.target_confirm_counter = 0
        self.target_confirm_started_ns = None
        self.localization_status = "INIT"
        self.distance_to_target = None
        self.cmd_vel_active = False
        self.route_name = ""

        self.cmd_pub = self.create_publisher(Twist, self.cmd_vel_topic, 10)
        self.status_pub = self.create_publisher(String, "pgv_route/status", 10)

        # Use accepted tags from the localizer, not raw reader tags, so only
        # tags that passed every gate (known id, offset, stability, zone) trigger
        # arrival and route advancement.
        self.create_subscription(Int64, "pgv/accepted_tag", self._on_accepted_tag, 10)
        # Keep raw subscriptions for diagnostics / unexpected-tag detection only.
        self.create_subscription(Int64, "pgv/tag", self._on_tag, 10)
        self.create_subscription(Bool, "pgv/code_detected", self._on_detected, 10)
        self.create_subscription(String, "pgv/localization_status", self._on_localization_status, 10)
        self.create_subscription(Odometry, self.odom_topic, self._on_odom, 20)
        self.create_subscription(String, "pgv_route/command", self._on_command, 10)
        self.create_service(Trigger, "~/stop", self._on_stop_srv)

        period = 1.0 / self.control_rate if self.control_rate > 0 else 0.05
        self.create_timer(period, self._control_loop)

        self.get_logger().info(
            f"route_follower ready (cmd_vel={self.cmd_vel_topic}, "
            f"cruise={self.cruise_speed} m/s, max_lat={self.max_lateral_error} m, "
            f"{len(self.tags)} tags)"
        )

    # ----- callbacks -----

    def _on_accepted_tag(self, msg):
        self.accepted_tag = int(msg.data)

    def _on_tag(self, msg):
        self.current_tag = int(msg.data)

    def _on_detected(self, msg):
        self.code_detected = bool(msg.data)

    def _on_localization_status(self, msg):
        state = str(msg.data or "").strip().split(":", 1)[0].strip().upper()
        self.localization_status = state or "INIT"

    def _on_odom(self, msg):
        x = msg.pose.pose.position.x
        y = msg.pose.pose.position.y
        if self.odom_xy is not None:
            self.travelled += math.hypot(x - self.odom_xy[0], y - self.odom_xy[1])
        self.odom_xy = (x, y)

    def _on_command(self, msg):
        try:
            data = json.loads(msg.data) if msg.data else {}
        except (ValueError, TypeError):
            self.get_logger().warning(f"Bad route command: {msg.data!r}")
            return
        action = str(data.get("action", "")).lower()
        if action == "stop":
            self._halt(STOPPED, "stop commanded")
            return
        if action == "follow":
            tags = data.get("tags") or []
            try:
                route = [int(t) for t in tags]
            except (TypeError, ValueError):
                self.get_logger().warning("follow: tags must be integers")
                return
            self._start_route(route)
            return
        self.get_logger().warning(f"Unknown route action: {action!r}")

    def _on_stop_srv(self, request, response):
        self._halt(STOPPED, "stop service called")
        response.success = True
        response.message = "Route follower stopped"
        return response

    # ----- run control -----

    def _start_route(self, route):
        if len(route) < 2:
            self._publish_status(STOPPED, "route needs >= 2 tags")
            return
        unknown = [t for t in route if t not in self.tags]
        if unknown:
            self._publish_status(STOPPED, f"tags not in map: {unknown}")
            return
        self.route = route
        self.edge_index = 0
        self.edge_start_dist = self.travelled
        self.route_name = " -> ".join(str(t) for t in route)
        self.cmd_vel_active = False

        # Off-tag start: if the robot is not currently sitting on (or right next
        # to) the first tag, drive there first. The robot already knows where it
        # is from odometry, so we treat "no tag" (tag 0) as a virtual start node
        # and run a 0 -> first-tag approach edge before the real sequence.
        first_tag = route[0]
        pose = self._robot_pose()
        on_first = (self.accepted_tag == first_tag)
        near_first = False
        if pose is not None:
            ft = self.tags[first_tag]
            near_first = math.hypot(pose[0] - ft[0], pose[1] - ft[1]) <= self.arrive_radius
        if on_first or near_first:
            self.state = FOLLOW
            self._configure_segment(route[0], route[1])
            self.get_logger().info(f"Following route {route}")
            self._publish_status(FOLLOW, f"edge {route[0]}->{route[1]}")
        else:
            # Anchor the approach line at the current pose (or the tag itself if
            # TF is briefly unavailable) and head for the first tag.
            self.virtual_start_xy = (pose[0], pose[1]) if pose is not None else (
                self.tags[first_tag][0], self.tags[first_tag][1]
            )
            self.state = APPROACH
            self._configure_segment(0, first_tag)
            self.get_logger().info(
                f"Off-tag start: approaching first tag {first_tag} before route {route}"
            )
            self._publish_status(APPROACH, f"approach 0->{first_tag}")

    def _configure_segment(self, segment_start, segment_target):
        self.current_segment_start = int(segment_start)
        self.current_segment_target = int(segment_target)
        self.previous_confirmed_tag = int(segment_start)
        self.target_tag = int(segment_target)
        self.allowed_tags = [int(segment_start), int(segment_target)]
        self.unexpected_tag_counter = 0
        self.target_confirm_counter = 0
        self.target_confirm_started_ns = None
        self.distance_to_target = None

    def _advance_segment(self):
        self.edge_index += 1
        self.edge_start_dist = self.travelled
        if self.edge_index + 1 >= len(self.route):
            self._halt(DONE, f"arrived at final tag {self.current_segment_target}")
            return
        next_start = self.route[self.edge_index]
        next_target = self.route[self.edge_index + 1]
        self._configure_segment(next_start, next_target)
        self.state = FOLLOW
        self._publish_status(FOLLOW, f"edge {next_start}->{next_target}")

    def _halt(self, state, detail=""):
        self.state = state
        self._stop_motion()
        self.cmd_vel_active = False
        self._publish_status(state, detail)
        if detail:
            self.get_logger().info(f"Route {state}: {detail}")

    def _stop_motion(self):
        self.cmd_pub.publish(Twist())
        self.cmd_vel_active = False

    def _robot_pose(self):
        """map->base_link as (x, y, yaw), or None if TF is unavailable."""
        try:
            tf = self.tf_buffer.lookup_transform(self.map_frame, self.base_frame, Time())
        except TransformException:
            return None
        return (
            tf.transform.translation.x,
            tf.transform.translation.y,
            quat_to_yaw(tf.transform.rotation.z, tf.transform.rotation.w),
        )

    def _control_loop(self):
        if self.state not in (FOLLOW, RECOVERY, APPROACH):
            return
        if self.localization_status == "STALE":
            self._halt(STOPPED, "PGV localization stale")
            return
        if self.state != APPROACH and self.edge_index + 1 >= len(self.route):
            self._halt(DONE, "route complete")
            return

        if self.state == APPROACH:
            # Virtual edge: current-pose anchor -> first tag. from_tag 0 means
            # "no tag / on the floor"; to_tag is the first real route tag.
            from_tag = 0
            to_tag = self.current_segment_target
            a = self.virtual_start_xy if self.virtual_start_xy is not None else self.tags[to_tag]
            b = self.tags[to_tag]
        else:
            from_tag = self.current_segment_start if self.current_segment_start is not None else self.route[self.edge_index]
            to_tag = self.current_segment_target if self.current_segment_target is not None else self.route[self.edge_index + 1]
            a = self.tags[from_tag]
            b = self.tags[to_tag]

        pose = self._robot_pose()
        if pose is None:
            self._stop_motion()
            self._publish_status(self.state, f"no {self.map_frame}->{self.base_frame} TF")
            return
        rx, ry, ryaw = pose

        # Edge geometry: line a -> b.
        ex, ey = b[0] - a[0], b[1] - a[1]
        edge_len = math.hypot(ex, ey)
        if edge_len < 1e-6:
            self._halt(STOPPED, f"degenerate edge {from_tag}->{to_tag}")
            return
        ux, uy = ex / edge_len, ey / edge_len
        edge_heading = math.atan2(uy, ux)

        # Progress along edge and cross-track error.
        dx, dy = rx - a[0], ry - a[1]
        along = dx * ux + dy * uy
        cross = -dx * uy + dy * ux            # +left of travel direction
        remaining = edge_len - along
        heading_err = _ang_norm(edge_heading - ryaw)
        self.distance_to_target = math.hypot(rx - b[0], ry - b[1])

        # Route semantics: only confirm the target when the robot is close and
        # the same valid target tag persists long enough.
        # Use accepted_tag (from localizer) for arrival, not raw current_tag.
        arrival_tag = getattr(self, "accepted_tag", None)
        if arrival_tag is not None:
            if arrival_tag == to_tag:
                # The PGV camera only reports a tag while it is physically over
                # it (offset gate in the localizer). So a CONFIRMED read of the
                # target tag means the robot IS at the destination - arrival is
                # the tag read itself, NOT base_link reaching arrive_radius.
                # With the rear-mounted camera, base_link sits ~0.52 m ahead of
                # the tag, so a distance gate would force the robot to overshoot
                # the final QR before it ever "arrives". Confirm on tag id only.
                now_ns = self.get_clock().now().nanoseconds
                if self.target_confirm_started_ns is None:
                    self.target_confirm_started_ns = now_ns
                self.target_confirm_counter += 1
                elapsed_s = (now_ns - self.target_confirm_started_ns) / 1e9
                if (
                    self.target_confirm_counter >= self.target_confirm_frames
                    or elapsed_s >= self.target_confirm_min_duration_s
                ):
                    self.get_logger().info(f"Reached tag {to_tag}")
                    self.previous_confirmed_tag = to_tag
                    # Stop dead on the tag before advancing/finishing so the
                    # robot never creeps forward past the final QR.
                    self._stop_motion()
                    if self.state == APPROACH:
                        # Reached the first tag from an off-tag start; now begin
                        # the real sequence at edge 0 (route[0] -> route[1]).
                        self.virtual_start_xy = None
                        self.edge_index = 0
                        self.edge_start_dist = self.travelled
                        self.state = FOLLOW
                        self._configure_segment(self.route[0], self.route[1])
                        self._publish_status(
                            FOLLOW, f"edge {self.route[0]}->{self.route[1]}"
                        )
                    else:
                        self._advance_segment()
                    return
            elif arrival_tag in (from_tag, self.previous_confirmed_tag):
                self.target_confirm_counter = 0
                self.target_confirm_started_ns = None
            elif arrival_tag != 0:
                self.unexpected_tag_counter += 1
                if self.unexpected_tag_counter == 1 or self.unexpected_tag_counter % 5 == 0:
                    self.get_logger().warning(
                        f"Unexpected accepted tag {arrival_tag} on edge {from_tag}->{to_tag}; holding route"
                    )

        # Hard lateral abort.
        if abs(cross) > self.max_lateral_error:
            self._halt(STOPPED, f"cross-track {cross*100:.0f} cm exceeds limit on {from_tag}->{to_tag}")
            return

        # Missed-tag failure: overshot the edge without reading tag B.
        dist_on_edge = self.travelled - self.edge_start_dist
        if dist_on_edge > edge_len + self.overshoot_margin:
            if self.state != RECOVERY:
                self.state = RECOVERY
                self.recovery_start = self.travelled
                self.get_logger().warning(
                    f"Tag {to_tag} not seen after {dist_on_edge:.2f} m "
                    f"(edge {edge_len:.2f} m); recovery creep"
                )
                self._publish_status(RECOVERY, f"searching for tag {to_tag}")
            if self.travelled - self.recovery_start > self.recovery_creep_m:
                self._halt(LOST, f"PGV lost: tag {to_tag} not found")
                return

        # Steering: null heading + cross-track.
        ang = self.k_heading * heading_err - self.k_lateral * cross
        ang = max(-self.max_angular, min(self.max_angular, ang))

        # Speed: only reach cruise once the robot is actually ON the line and
        # pointing down it. When it is still rotating into the edge or off to the
        # side, creep - otherwise it lunges forward before it is aligned and
        # drives off the line (near-collisions seen in testing). Scale linearly
        # from approach->cruise by how aligned we are in heading AND cross-track.
        heading_align = max(0.0, 1.0 - abs(heading_err) / self.max_yaw_error)
        lateral_align = max(0.0, 1.0 - abs(cross) / self.max_lateral_error)
        align = heading_align * lateral_align
        speed = self.approach_speed + (self.cruise_speed - self.approach_speed) * align
        # Always creep near the destination tag and during recovery searches.
        if remaining < self.arrive_radius * 3.0 or self.state == RECOVERY:
            speed = min(speed, self.approach_speed)
        # Rotate in place only when GROSSLY misaligned (e.g. starting an edge
        # facing the wrong way). The speed already scales down with heading
        # error via heading_align above, so for normal tracking the robot keeps
        # driving forward while steering - a tight heading gate here stalls the
        # whole route because position-only PGV yaw is never edge-perfect.
        if abs(heading_err) > math.radians(60.0):
            speed = 0.0
        # Stop forward motion only just before the hard lateral abort.
        if abs(cross) > self.max_lateral_error * 0.9:
            speed = 0.0

        cmd = Twist()
        cmd.linear.x = speed
        cmd.angular.z = ang
        self.cmd_pub.publish(cmd)
        self.cmd_vel_active = True

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
        body = {"state": state, "detail": detail}
        if self.route:
            body["route"] = list(self.route)
            body["route_name"] = self.route_name
        if self.route and self.edge_index + 1 < len(self.route):
            body["edge"] = [self.route[self.edge_index], self.route[self.edge_index + 1]]
            body["next_tag"] = self.route[self.edge_index + 1]
        if self.current_segment_start is not None:
            body["current_segment_start"] = self.current_segment_start
        if self.current_segment_target is not None:
            body["current_segment_target"] = self.current_segment_target
        if self.previous_confirmed_tag is not None:
            body["previous_confirmed_tag"] = self.previous_confirmed_tag
        if self.target_tag is not None:
            body["target_tag"] = self.target_tag
        body["allowed_tags"] = list(self.allowed_tags)
        body["unexpected_tag_counter"] = int(self.unexpected_tag_counter)
        body["target_confirm_counter"] = int(self.target_confirm_counter)
        body["localization_status"] = self.localization_status
        body["distance_to_target"] = self.distance_to_target
        body["cmd_vel_active"] = bool(self.cmd_vel_active)
        msg.data = json.dumps(body)
        self.status_pub.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = RouteFollower()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node._stop_motion()
        except Exception:  # noqa: BLE001
            pass
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
