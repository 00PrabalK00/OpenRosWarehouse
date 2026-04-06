#!/usr/bin/env python3
"""
Zone Publisher for Nav2 Costmap Filters (Keepout + Speed)

This node:
- Subscribes to /map (OccupancyGrid) to match its size/resolution/origin
- Loads zones from the SQLite database (default: ~/DB/robot_data.db)
- Rasterizes zones into two masks:
    /keepout_filter_mask   (0 free, 100 keepout)
    /speed_filter_mask     (1..100 speed percent; 100 = full speed)
- Publishes CostmapFilterInfo:
    Keepout: /costmap_filter_info  (type=0)  <- matches your Nav2 keepout_filter.filter_info_topic
    Speed:   /speed_filter_info    (type=1)

Important:
- Your Nav2 params currently show:
    keepout_filter.filter_info_topic = /costmap_filter_info
    speed_filter.filter_info_topic   = /speed_filter_info
  So we publish exactly those topics.
"""

import os
import math

import rclpy
from rclpy.duration import Duration
from rclpy.node import Node
from rclpy.qos import QoSProfile, DurabilityPolicy, ReliabilityPolicy, HistoryPolicy
from rclpy.time import Time

from nav2_msgs.msg import CostmapFilterInfo
from nav_msgs.msg import OccupancyGrid
from std_msgs.msg import Bool, String, UInt8
from tf2_ros import Buffer, TransformException, TransformListener

from next_ros2ws_core.db_manager import DatabaseManager


class ZonePublisher(Node):
    def __init__(self):
        super().__init__("zone_publisher")

        # -----------------------
        # Parameters
        # -----------------------
        self.declare_parameter("db_path", os.path.expanduser("~/DB/robot_data.db"))
        self.declare_parameter("enable_speed_filter", True)
        self.declare_parameter("max_speed_mps", 0.30)   # used to convert % -> m/s
        self.declare_parameter("publish_rate_hz", 1.0)  # re-publish periodically for robustness
        self.declare_parameter("dynamic_safety_filters_enabled", True)
        self.declare_parameter("robot_base_frame", "base_footprint")
        self.declare_parameter("dynamic_keepout_radius_m", 0.35)
        self.declare_parameter("dynamic_keepout_front_offset_m", 0.55)
        self.declare_parameter("dynamic_keepout_rear_offset_m", 0.55)
        self.declare_parameter("dynamic_speed_filter_radius_m", 0.45)
        self.declare_parameter("dynamic_pose_yaw_bin_deg", 10.0)

        db_path = os.path.expanduser(str(self.get_parameter("db_path").value or "~/DB/robot_data.db"))
        self.db_manager = DatabaseManager(db_path=db_path)
        self.enable_speed_filter = bool(self.get_parameter("enable_speed_filter").value)
        self.max_speed_mps = float(self.get_parameter("max_speed_mps").value)
        self.publish_rate_hz = float(self.get_parameter("publish_rate_hz").value)
        self.dynamic_safety_filters_enabled = bool(
            self.get_parameter("dynamic_safety_filters_enabled").value
        )
        self.robot_base_frame = str(
            self.get_parameter("robot_base_frame").value or "base_footprint"
        )
        self.dynamic_keepout_radius_m = max(
            0.05, float(self.get_parameter("dynamic_keepout_radius_m").value)
        )
        self.dynamic_keepout_front_offset_m = max(
            0.0, float(self.get_parameter("dynamic_keepout_front_offset_m").value)
        )
        self.dynamic_keepout_rear_offset_m = max(
            0.0, float(self.get_parameter("dynamic_keepout_rear_offset_m").value)
        )
        self.dynamic_speed_filter_radius_m = max(
            0.05, float(self.get_parameter("dynamic_speed_filter_radius_m").value)
        )
        self.dynamic_pose_yaw_bin_rad = math.radians(
            max(1.0, float(self.get_parameter("dynamic_pose_yaw_bin_deg").value))
        )

        # -----------------------
        # QoS
        # -----------------------
        # /map is typically latched by map_server
        self.map_qos = QoSProfile(
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )

        # costmap filters should be latched so Nav2 can join later
        self.latched_qos = QoSProfile(
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )
        self.state_qos = QoSProfile(
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )

        # -----------------------
        # Publishers
        # -----------------------
        # Keepout info MUST be on /costmap_filter_info because your keepout_filter listens there
        self.keepout_info_pub = self.create_publisher(
            CostmapFilterInfo, "/costmap_filter_info", self.latched_qos
        )

        # Speed info is already on /speed_filter_info
        self.speed_info_pub = self.create_publisher(
            CostmapFilterInfo, "/speed_filter_info", self.latched_qos
        )

        self.keepout_mask_pub = self.create_publisher(
            OccupancyGrid, "/keepout_filter_mask", self.latched_qos
        )
        self.speed_mask_pub = self.create_publisher(
            OccupancyGrid, "/speed_filter_mask", self.latched_qos
        )
        self.bridge_ready_pub = self.create_publisher(
            Bool, "/safety/nav2_filter_bridge_ready", self.latched_qos
        )

        # -----------------------
        # Subscriber
        # -----------------------
        self._map_msg = None
        self.create_subscription(OccupancyGrid, "/map", self._on_map, self.map_qos)
        self._dynamic_keepout_active = False
        self._dynamic_keepout_direction = "none"
        self._dynamic_speed_limit_percent = 100
        self._dynamic_state_dirty = True
        self._bridge_ready = None
        self._last_dynamic_pose_key = None

        self.tf_buffer = None
        self.tf_listener = None
        if self.dynamic_safety_filters_enabled:
            self.tf_buffer = Buffer(cache_time=Duration(seconds=5.0))
            self.tf_listener = TransformListener(self.tf_buffer, self, spin_thread=True)
            self.create_subscription(
                Bool,
                "/safety/nav2_keepout_active",
                self._on_dynamic_keepout_active,
                self.state_qos,
            )
            self.create_subscription(
                String,
                "/safety/nav2_keepout_direction",
                self._on_dynamic_keepout_direction,
                self.state_qos,
            )
            self.create_subscription(
                UInt8,
                "/safety/nav2_speed_limit_percent",
                self._on_dynamic_speed_limit_percent,
                self.state_qos,
            )

        # -----------------------
        # Layers tracking
        # -----------------------
        self._layers = {"no_go_zones": [], "restricted": [], "slow_zones": []}
        self._layers_hash = None
        self._last_map_id = None
        self._force_update = True
        self._static_keepout_mask = None
        self._static_speed_mask = None

        # Periodic publish (helps even if Nav2 restarts later)
        period = 1.0 / max(self.publish_rate_hz, 0.1)
        self.timer = self.create_timer(period, self._tick)

        self.get_logger().info("ZonePublisher running (SQLite backend).")
        self.get_logger().info(f"database: {db_path}")
        self.get_logger().info(f"speed_filter: {self.enable_speed_filter} (max_speed_mps={self.max_speed_mps})")
        self.get_logger().info("Publishing keepout info on /costmap_filter_info (type=0)")
        self.get_logger().info("Publishing speed info   on /speed_filter_info (type=1)")
        if self.dynamic_safety_filters_enabled:
            self.get_logger().info(
                f"Dynamic safety filter bridge enabled for {self.robot_base_frame}: "
                f"keepout_radius={self.dynamic_keepout_radius_m:.2f}m "
                f"front_offset={self.dynamic_keepout_front_offset_m:.2f}m "
                f"rear_offset={self.dynamic_keepout_rear_offset_m:.2f}m "
                f"speed_radius={self.dynamic_speed_filter_radius_m:.2f}m"
            )
        else:
            self.get_logger().warn("Dynamic safety filter bridge disabled; only static map layers will be published")
        self._set_bridge_ready(False)

    # -----------------------
    # Callbacks / main loop
    # -----------------------
    def _on_map(self, msg: OccupancyGrid):
        if msg.info.width <= 0 or msg.info.height <= 0:
            return

        first_map = self._map_msg is None
        map_changed = False
        if self._map_msg is not None:
            prev = self._map_msg.info
            curr = msg.info
            map_changed = bool(
                prev.width != curr.width
                or prev.height != curr.height
                or abs(float(prev.resolution) - float(curr.resolution)) > 1e-9
                or abs(float(prev.origin.position.x) - float(curr.origin.position.x)) > 1e-9
                or abs(float(prev.origin.position.y) - float(curr.origin.position.y)) > 1e-9
                or str(self._map_msg.header.frame_id or "map") != str(msg.header.frame_id or "map")
            )

        self._map_msg = msg
        if first_map:
            self.get_logger().info(
                f"Got /map: {msg.info.width}x{msg.info.height} res={msg.info.resolution} frame={msg.header.frame_id or 'map'}"
            )
        elif map_changed:
            self.get_logger().info(
                f"/map geometry changed: {msg.info.width}x{msg.info.height} res={msg.info.resolution} frame={msg.header.frame_id or 'map'}"
            )
            self._force_update = True

    def _nav2_costmaps_present(self) -> bool:
        # Correct check: name + namespace
        for name, ns in self.get_node_names_and_namespaces():
            if name == "local_costmap" and ns == "/local_costmap":
                return True
            if name == "global_costmap" and ns == "/global_costmap":
                return True
        return False

    def _tick(self):
        if self._map_msg is None:
            self._set_bridge_ready(False)
            return
        if not self._nav2_costmaps_present():
            # Wait until Nav2 costmaps exist (avoids timing weirdness)
            self._set_bridge_ready(False)
            return

        layers_changed = self._reload_layers_if_changed()

        # Check if map changed (simple check on object identity or timestamp)
        map_changed = False
        current_map_id = id(self._map_msg)
        if current_map_id != self._last_map_id:
            msg_time = f"{self._map_msg.header.stamp.sec}.{self._map_msg.header.stamp.nanosec}"
            self.get_logger().info(f"Map updated (time={msg_time}), triggering layer publish")
            self._last_map_id = current_map_id
            map_changed = True

        if layers_changed or map_changed or self._static_keepout_mask is None:
            self._rebuild_static_masks()

        dynamic_pose = self._lookup_robot_pose_in_map() if self.dynamic_safety_filters_enabled else None
        bridge_ready = bool(self.dynamic_safety_filters_enabled and dynamic_pose is not None)
        bridge_ready_changed = self._set_bridge_ready(bridge_ready)

        dynamic_pose_changed = False
        if self._dynamic_overlay_active() and dynamic_pose is not None:
            pose_key = self._dynamic_pose_key(dynamic_pose)
            if pose_key != self._last_dynamic_pose_key:
                self._last_dynamic_pose_key = pose_key
                dynamic_pose_changed = True
        elif self._last_dynamic_pose_key is not None:
            self._last_dynamic_pose_key = None
            dynamic_pose_changed = True

        if (
            layers_changed
            or map_changed
            or self._force_update
            or self._dynamic_state_dirty
            or dynamic_pose_changed
            or bridge_ready_changed
        ):
            self._publish_all(dynamic_pose=dynamic_pose, bridge_ready=bridge_ready)
            self._force_update = False
            self._dynamic_state_dirty = False

    # -----------------------
    # Publish
    # -----------------------
    def _publish_all(self, dynamic_pose=None, bridge_ready=False):
        now = self.get_clock().now().to_msg()
        map_frame = self._map_msg.header.frame_id or "map"

        if self._static_keepout_mask is None or (self.enable_speed_filter and self._static_speed_mask is None):
            self._rebuild_static_masks()

        keepout_mask = self._clone_mask(self._static_keepout_mask)
        keepout_mask.header.stamp = now
        keepout_mask.header.frame_id = map_frame

        speed_mask = None
        if self.enable_speed_filter:
            speed_mask = self._clone_mask(self._static_speed_mask)
            speed_mask.header.stamp = now
            speed_mask.header.frame_id = map_frame

        if bridge_ready and dynamic_pose is not None:
            self._apply_dynamic_safety_overlays(keepout_mask, speed_mask, dynamic_pose)

        self.keepout_mask_pub.publish(keepout_mask)
        if speed_mask is not None:
            self.speed_mask_pub.publish(speed_mask)

        # Keepout FilterInfo (type=0)
        k = CostmapFilterInfo()
        k.header.stamp = now
        k.header.frame_id = map_frame
        k.type = 0
        k.filter_mask_topic = "/keepout_filter_mask"
        k.base = 0.0
        k.multiplier = 1.0
        self.keepout_info_pub.publish(k)

        # Speed FilterInfo (type=1) => mask value is % of speed.
        # We convert percent -> m/s using multiplier.
        if self.enable_speed_filter:
            s = CostmapFilterInfo()
            s.header.stamp = now
            s.header.frame_id = map_frame
            s.type = 1
            s.filter_mask_topic = "/speed_filter_mask"
            s.base = 0.0
            s.multiplier = self.max_speed_mps / 100.0
            self.speed_info_pub.publish(s)

    # -----------------------
    # Layers database
    # -----------------------
    def _reload_layers_if_changed(self):
        try:
            data = self.db_manager.get_map_layers()
            new_hash = str(data)
            if new_hash == self._layers_hash:
                return False
            self._layers_hash = new_hash
            self._layers = {
                "no_go_zones": data.get("no_go_zones", []),
                "restricted": data.get("restricted", []),
                "slow_zones": data.get("slow_zones", []),
            }
            total = sum(len(v) for v in self._layers.values())
            self.get_logger().info(f"Loaded layers from database: {total} objects (hash changed)")
            return True
        except Exception as e:
            self.get_logger().error(f"Failed to load layers from database: {e}")
            return False

    def _rebuild_static_masks(self):
        self._static_keepout_mask = self._build_keepout_mask(self._map_msg, self._layers)
        if self.enable_speed_filter:
            self._static_speed_mask = self._build_speed_mask(self._map_msg, self._layers)
        else:
            self._static_speed_mask = None

    # -----------------------
    # Mask building
    # -----------------------
    def _clone_mask(self, mask_template: OccupancyGrid) -> OccupancyGrid:
        mask = OccupancyGrid()
        mask.info = mask_template.info
        mask.data = list(mask_template.data)
        return mask

    def _blank_mask_like_map(self, map_msg: OccupancyGrid, fill_val: int) -> OccupancyGrid:
        mask = OccupancyGrid()
        mask.info = map_msg.info
        size = map_msg.info.width * map_msg.info.height
        mask.data = [int(fill_val)] * size
        return mask

    def _build_keepout_mask(self, map_msg: OccupancyGrid, layers: dict) -> OccupancyGrid:
        # 0 free everywhere by default
        mask = self._blank_mask_like_map(map_msg, 0)

        # Mark keepout cells as 100
        for layer_name in ("no_go_zones", "restricted"):
            for obj in layers.get(layer_name, []):
                self._apply_shape(mask, obj, 100, combine="override")

        return mask

    def _build_speed_mask(self, map_msg: OccupancyGrid, layers: dict) -> OccupancyGrid:
        # 100% speed everywhere by default
        mask = self._blank_mask_like_map(map_msg, 100)

        # In slow zones, set a smaller percent (e.g., 30 = 30% of max speed)
        for obj in layers.get("slow_zones", []):
            pct = int(obj.get("speed_percent", 50))
            pct = max(1, min(pct, 100))
            self._apply_shape(mask, obj, pct, combine="min")  # stricter (lower) wins

        return mask

    # -----------------------
    # Rasterization
    # -----------------------
    def _apply_shape(self, mask: OccupancyGrid, obj: dict, value: int, combine: str):
        info = mask.info
        ox, oy = info.origin.position.x, info.origin.position.y
        res = info.resolution
        w, h = info.width, info.height

        def set_cell(gx, gy):
            if gx < 0 or gy < 0 or gx >= w or gy >= h:
                return
            idx = gx + gy * w
            if combine == "min":
                mask.data[idx] = min(mask.data[idx], value)
            elif combine == "max":
                mask.data[idx] = max(mask.data[idx], value)
            else:
                mask.data[idx] = value

        t = obj.get("type", "")
        if t == "rectangle":
            x1, y1 = float(obj.get("x1", 0.0)), float(obj.get("y1", 0.0))
            x2, y2 = float(obj.get("x2", 0.0)), float(obj.get("y2", 0.0))
            self._fill_rect(set_cell, ox, oy, res, x1, y1, x2, y2)

        elif t == "circle":
            cx, cy = float(obj.get("x", 0.0)), float(obj.get("y", 0.0))
            r = float(obj.get("radius", 0.0))
            self._fill_circle(set_cell, ox, oy, res, cx, cy, r)

        elif t == "polygon":
            points = obj.get("points", [])
            poly = [(float(p.get("x", 0.0)), float(p.get("y", 0.0))) for p in points]
            if len(poly) >= 3:
                self._fill_polygon(set_cell, ox, oy, res, poly)

    # -----------------------
    # Dynamic safety overlays
    # -----------------------
    def _dynamic_overlay_active(self) -> bool:
        return bool(self._dynamic_keepout_active or self._dynamic_speed_limit_percent < 100)

    def _lookup_robot_pose_in_map(self):
        if self.tf_buffer is None or self._map_msg is None:
            return None

        map_frame = self._map_msg.header.frame_id or "map"
        try:
            transform = self.tf_buffer.lookup_transform(
                map_frame,
                self.robot_base_frame,
                Time(),
                timeout=Duration(seconds=0.05),
            )
        except TransformException:
            return None

        translation = transform.transform.translation
        rotation = transform.transform.rotation
        yaw = math.atan2(
            2.0 * (rotation.w * rotation.z + rotation.x * rotation.y),
            1.0 - 2.0 * (rotation.y * rotation.y + rotation.z * rotation.z),
        )
        return float(translation.x), float(translation.y), float(yaw)

    def _dynamic_pose_key(self, pose):
        x, y, yaw = pose
        resolution = max(1e-3, float(self._map_msg.info.resolution))
        yaw_bin = max(1e-3, self.dynamic_pose_yaw_bin_rad)
        return (
            int(round(x / resolution)),
            int(round(y / resolution)),
            int(round(yaw / yaw_bin)),
        )

    def _apply_dynamic_safety_overlays(self, keepout_mask, speed_mask, dynamic_pose):
        x, y, yaw = dynamic_pose
        cos_yaw = math.cos(yaw)
        sin_yaw = math.sin(yaw)

        if self._dynamic_keepout_active:
            direction = str(self._dynamic_keepout_direction or "none").strip().lower()
            centers = []
            if direction in ("forward", "both"):
                centers.append((
                    x + cos_yaw * self.dynamic_keepout_front_offset_m,
                    y + sin_yaw * self.dynamic_keepout_front_offset_m,
                ))
            if direction in ("backward", "both"):
                centers.append((
                    x - cos_yaw * self.dynamic_keepout_rear_offset_m,
                    y - sin_yaw * self.dynamic_keepout_rear_offset_m,
                ))
            if not centers:
                centers = [
                    (
                        x + cos_yaw * self.dynamic_keepout_front_offset_m,
                        y + sin_yaw * self.dynamic_keepout_front_offset_m,
                    ),
                    (
                        x - cos_yaw * self.dynamic_keepout_rear_offset_m,
                        y - sin_yaw * self.dynamic_keepout_rear_offset_m,
                    ),
                ]

            for cx, cy in centers:
                self._apply_shape(
                    keepout_mask,
                    {"type": "circle", "x": cx, "y": cy, "radius": self.dynamic_keepout_radius_m},
                    100,
                    combine="override",
                )

        if speed_mask is not None and self._dynamic_speed_limit_percent < 100:
            self._apply_shape(
                speed_mask,
                {
                    "type": "circle",
                    "x": x,
                    "y": y,
                    "radius": self.dynamic_speed_filter_radius_m,
                },
                int(max(1, min(100, self._dynamic_speed_limit_percent))),
                combine="min",
            )

    def _set_bridge_ready(self, ready: bool) -> bool:
        ready = bool(ready)
        if self._bridge_ready is ready:
            return False

        previous = self._bridge_ready
        self._bridge_ready = ready
        msg = Bool()
        msg.data = ready
        self.bridge_ready_pub.publish(msg)

        if previous is None:
            state = "ready" if ready else "waiting for map/costmaps/TF"
            self.get_logger().info(f"Dynamic safety filter bridge: {state}")
        elif ready:
            self.get_logger().info("Dynamic safety filter bridge ready")
        else:
            self.get_logger().warn(
                "Dynamic safety filter bridge unavailable; SafetyController will fall back to direct cmd_vel gating"
            )
        return True

    def _on_dynamic_keepout_active(self, msg: Bool):
        value = bool(msg.data)
        if value != self._dynamic_keepout_active:
            self._dynamic_keepout_active = value
            self._dynamic_state_dirty = True

    def _on_dynamic_keepout_direction(self, msg: String):
        value = str(msg.data or "none").strip().lower() or "none"
        if value != self._dynamic_keepout_direction:
            self._dynamic_keepout_direction = value
            self._dynamic_state_dirty = True

    def _on_dynamic_speed_limit_percent(self, msg: UInt8):
        value = int(max(1, min(100, int(msg.data))))
        if value != self._dynamic_speed_limit_percent:
            self._dynamic_speed_limit_percent = value
            self._dynamic_state_dirty = True

    def _fill_rect(self, set_cell, ox, oy, res, x1, y1, x2, y2):
        minx, maxx = min(x1, x2), max(x1, x2)
        miny, maxy = min(y1, y2), max(y1, y2)

        gx_min = int(math.floor((minx - ox) / res))
        gx_max = int(math.floor((maxx - ox) / res))
        gy_min = int(math.floor((miny - oy) / res))
        gy_max = int(math.floor((maxy - oy) / res))

        for gy in range(gy_min, gy_max + 1):
            for gx in range(gx_min, gx_max + 1):
                set_cell(gx, gy)

    def _fill_circle(self, set_cell, ox, oy, res, cx, cy, radius):
        if radius <= 0.0:
            return

        gx_min = int(math.floor((cx - radius - ox) / res))
        gx_max = int(math.floor((cx + radius - ox) / res))
        gy_min = int(math.floor((cy - radius - oy) / res))
        gy_max = int(math.floor((cy + radius - oy) / res))
        r2 = radius * radius

        for gy in range(gy_min, gy_max + 1):
            wy = oy + (gy + 0.5) * res
            for gx in range(gx_min, gx_max + 1):
                wx = ox + (gx + 0.5) * res
                if (wx - cx) ** 2 + (wy - cy) ** 2 <= r2:
                    set_cell(gx, gy)

    def _fill_polygon(self, set_cell, ox, oy, res, poly):
        xs = [p[0] for p in poly]
        ys = [p[1] for p in poly]
        minx, maxx = min(xs), max(xs)
        miny, maxy = min(ys), max(ys)

        gx_min = int(math.floor((minx - ox) / res))
        gx_max = int(math.floor((maxx - ox) / res))
        gy_min = int(math.floor((miny - oy) / res))
        gy_max = int(math.floor((maxy - oy) / res))

        for gy in range(gy_min, gy_max + 1):
            wy = oy + (gy + 0.5) * res
            for gx in range(gx_min, gx_max + 1):
                wx = ox + (gx + 0.5) * res
                if self._point_in_poly(wx, wy, poly):
                    set_cell(gx, gy)

    def _point_in_poly(self, x, y, poly):
        inside = False
        j = len(poly) - 1
        for i in range(len(poly)):
            xi, yi = poly[i]
            xj, yj = poly[j]
            if ((yi > y) != (yj > y)) and (
                x < (xj - xi) * (y - yi) / ((yj - yi) + 1e-12) + xi
            ):
                inside = not inside
            j = i
        return inside


def main():
    rclpy.init()
    node = ZonePublisher()
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
            # Context may already be shut down by launch/system shutdown.
            pass


if __name__ == "__main__":
    main()
