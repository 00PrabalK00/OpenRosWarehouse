#!/usr/bin/env python3

import json
import math
import os
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import rclpy
from rclpy.duration import Duration
from rclpy.node import Node
from rclpy.time import Time
from tf2_ros import Buffer, TransformException, TransformListener

from next_ros2ws_interfaces.srv import (
    AddMapLayerObject,
    ClearMapLayer,
    DeleteMapLayerObject,
    GetMapLayers,
    SetStackMode,
)
from .db_manager import DatabaseManager


class MapLayerManager(Node):
    STACK_MODE_BY_LOCALIZATION_MODE = {
        'point_cloud': 'nav',
        'reflector': 'reflector_nav',
        'qr': 'pgv_nav',
    }

    VALID_LAYERS = (
        'no_go_zones', 'restricted', 'slow_zones', 'safety_zones',
        'localization_zones', 'locate_config', 'reflector', 'tag_area', 'do_area',
        'di_area', 'clean_area', 'description_area'
    )

    def __init__(self):
        super().__init__('map_layer_manager')

        # Initialize database manager
        db_path = os.path.expanduser(
            str(self.declare_parameter('db_path', '~/DB/robot_data.db').value)
        )
        self.db_manager = DatabaseManager(db_path=db_path)

        self._lock = threading.Lock()
        self._layers = self._load_layers_from_database()

        self.get_layers_srv = self.create_service(GetMapLayers, '/map_layers/get', self.get_layers_callback)
        self.add_layer_srv = self.create_service(AddMapLayerObject, '/map_layers/add', self.add_layer_callback)
        self.delete_layer_srv = self.create_service(DeleteMapLayerObject, '/map_layers/delete', self.delete_layer_callback)
        self.clear_layer_srv = self.create_service(ClearMapLayer, '/map_layers/clear', self.clear_layer_callback)

        self.auto_switch_localization_zones = self._as_bool(
            self.declare_parameter('auto_switch_localization_zones', True).value,
            default=True,
        )
        self.localization_zone_map_frame = str(
            self.declare_parameter('localization_zone_map_frame', 'map').value or 'map'
        ).strip() or 'map'
        self.localization_zone_base_frame = str(
            self.declare_parameter('localization_zone_base_frame', 'base_footprint').value or 'base_footprint'
        ).strip() or 'base_footprint'
        check_period = max(0.1, float(self.declare_parameter('localization_zone_check_period_sec', 0.5).value))
        self.localization_zone_required_inside_sec = max(
            0.0,
            float(self.declare_parameter('localization_zone_required_inside_sec', 0.75).value),
        )
        self.localization_zone_switch_cooldown_sec = max(
            0.0,
            float(self.declare_parameter('localization_zone_switch_cooldown_sec', 5.0).value),
        )
        self.localization_zone_reassert_sec = max(
            1.0,
            float(self.declare_parameter('localization_zone_reassert_sec', 15.0).value),
        )

        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        self.stack_mode_client = self.create_client(SetStackMode, '/stack/set_mode')
        self._pending_localization_zone_key = ''
        self._pending_localization_zone_since = 0.0
        self._last_requested_stack_mode = ''
        self._last_successful_switch_time = 0.0
        self._last_switch_attempt_time = 0.0
        self._switch_inflight = False
        self._last_stack_unavailable_log = 0.0
        if self.auto_switch_localization_zones:
            self.create_timer(check_period, self._localization_zone_timer_callback)

        count = sum(len(v) for v in self._layers.values())
        self.get_logger().info('MapLayerManager started')
        self.get_logger().info(f'Database: {db_path}')
        self.get_logger().info(f'Loaded layers: {count} objects')
        if self.auto_switch_localization_zones:
            self.get_logger().info(
                'Localization zone auto-switch enabled '
                f'({self.localization_zone_map_frame}->{self.localization_zone_base_frame})'
            )

    @staticmethod
    def _empty_layers() -> Dict[str, List[Dict[str, Any]]]:
        return {
            'no_go_zones': [],
            'restricted': [],
            'slow_zones': [],
            'safety_zones': [],
            'localization_zones': [],
            'locate_config': [],
            'reflector': [],
            'tag_area': [],
            'do_area': [],
            'di_area': [],
            'clean_area': [],
            'description_area': [],
        }

    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value if value is not None else '').strip().lower()
        if text in {'1', 'true', 'yes', 'on'}:
            return True
        if text in {'0', 'false', 'no', 'off'}:
            return False
        return bool(default)

    def _sanitize_layers(self, data: Any) -> Dict[str, List[Dict[str, Any]]]:
        layers = self._empty_layers()
        if not isinstance(data, dict):
            return layers

        for layer in self.VALID_LAYERS:
            raw_items = data.get(layer, [])
            if not isinstance(raw_items, list):
                continue
            cleaned = []
            for raw in raw_items:
                if not isinstance(raw, dict):
                    continue
                norm, _ = self._normalize_object(layer, raw)
                if norm is not None:
                    cleaned.append(norm)
            layers[layer] = cleaned
        return layers

    def _load_layers_from_database(self) -> Dict[str, List[Dict[str, Any]]]:
        """Load map layers strictly from database storage."""
        try:
            data = self.db_manager.get_map_layers()
            return self._sanitize_layers(data)
        except Exception as exc:
            self.get_logger().error(f'Failed to load layers from database: {exc}')
            return self._empty_layers()

    def _save_layers_to_database(self) -> Tuple[bool, str]:
        """Save map layers to database."""
        try:
            self.db_manager.save_map_layers(self._layers)
            return True, 'Layers saved'
        except Exception as exc:
            return False, f'Failed to save layers: {exc}'

    def _layers_json(self) -> str:
        return json.dumps(self._layers)

    def _parse_object_json(self, raw: str) -> Tuple[Optional[Dict[str, Any]], str]:
        text = str(raw or '').strip()
        if not text:
            return None, 'object_json is required'
        try:
            data = json.loads(text)
        except Exception as exc:
            return None, f'Invalid object_json: {exc}'
        if not isinstance(data, dict):
            return None, 'object_json must decode to a JSON object'
        return data, ''

    @staticmethod
    def _to_float(value: Any, field: str) -> Tuple[Optional[float], str]:
        try:
            return float(value), ''
        except (TypeError, ValueError):
            return None, f'{field} must be numeric'

    @staticmethod
    def _normalize_points(points: Any) -> Tuple[Optional[List[Dict[str, float]]], str]:
        if not isinstance(points, list) or len(points) < 3:
            return None, 'polygon requires at least 3 points'

        normalized = []
        for idx, point in enumerate(points):
            if not isinstance(point, dict):
                return None, f'point {idx} must be an object'
            x, err = MapLayerManager._to_float(point.get('x'), f'points[{idx}].x')
            if err:
                return None, err
            y, err = MapLayerManager._to_float(point.get('y'), f'points[{idx}].y')
            if err:
                return None, err
            normalized.append({'x': x, 'y': y})
        return normalized, ''

    def _normalize_object(self, layer: str, raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        shape = str(raw.get('type', '') or '').strip().lower()
        if shape not in ('rectangle', 'polygon', 'circle', 'point'):
            return None, 'type must be rectangle, polygon, circle, or point'

        obj: Dict[str, Any] = {
            'id': str(raw.get('id') or uuid.uuid4().hex[:10]),
            'type': shape,
        }

        if shape == 'rectangle':
            x1, err = self._to_float(raw.get('x1'), 'x1')
            if err:
                return None, err
            y1, err = self._to_float(raw.get('y1'), 'y1')
            if err:
                return None, err
            x2, err = self._to_float(raw.get('x2'), 'x2')
            if err:
                return None, err
            y2, err = self._to_float(raw.get('y2'), 'y2')
            if err:
                return None, err
            obj.update({'x1': x1, 'y1': y1, 'x2': x2, 'y2': y2})

        elif shape == 'polygon':
            points, err = self._normalize_points(raw.get('points'))
            if err:
                return None, err
            obj['points'] = points

        elif shape == 'circle':
            x, err = self._to_float(raw.get('x'), 'x')
            if err:
                return None, err
            y, err = self._to_float(raw.get('y'), 'y')
            if err:
                return None, err
            radius, err = self._to_float(raw.get('radius'), 'radius')
            if err:
                return None, err
            if radius <= 0.0:
                return None, 'radius must be > 0'
            obj.update({'x': x, 'y': y, 'radius': radius})

        elif shape == 'point':
            x, err = self._to_float(raw.get('x'), 'x')
            if err:
                return None, err
            y, err = self._to_float(raw.get('y'), 'y')
            if err:
                return None, err
            obj.update({'x': x, 'y': y})

        if layer == 'slow_zones':
            try:
                pct = int(raw.get('speed_percent', 50))
            except (TypeError, ValueError):
                return None, 'speed_percent must be an integer'
            pct = max(10, min(100, pct))
            obj['speed_percent'] = pct

        if layer == 'restricted':
            try:
                pct = int(raw.get('speed_percent', 30))
            except (TypeError, ValueError):
                pct = 30
            obj['speed_percent'] = max(10, min(90, pct))

        if layer == 'safety_zones':
            try:
                dist = float(raw.get('safety_distance_m', 0.5))
            except (TypeError, ValueError):
                return None, 'safety_distance_m must be a number'
            dist = max(0.01, min(3.0, dist))
            obj['safety_distance_m'] = round(dist, 3)
            obj['lidar_safety_off'] = bool(raw.get('lidar_safety_off', False))

        if layer == 'localization_zones':
            obj['localization_mode'] = str(raw.get('localization_mode', 'point_cloud') or 'point_cloud').strip().lower()
            if obj['localization_mode'] not in ('point_cloud', 'reflector', 'qr'):
                obj['localization_mode'] = 'point_cloud'
            obj['label'] = str(raw.get('label', '') or '').strip()[:80]

        if layer == 'do_area':
            try:
                obj['channel'] = max(1, min(8, int(raw.get('channel', 1))))
            except (TypeError, ValueError):
                obj['channel'] = 1
            obj['event'] = str(raw.get('event', 'enter') or 'enter').strip().lower()
            if obj['event'] not in ('enter', 'exit', 'inside'):
                obj['event'] = 'enter'

        if layer == 'di_area':
            try:
                obj['channel'] = max(1, min(8, int(raw.get('channel', 1))))
            except (TypeError, ValueError):
                obj['channel'] = 1
            obj['condition'] = str(raw.get('condition', 'high') or 'high').strip().lower()
            if obj['condition'] not in ('high', 'low', 'rising', 'falling'):
                obj['condition'] = 'high'

        if layer == 'tag_area':
            try:
                obj['tag_id'] = max(0, int(raw.get('tag_id', 0)))
            except (TypeError, ValueError):
                obj['tag_id'] = 0
            obj['tag_type'] = str(raw.get('tag_type', 'qr') or 'qr').strip().lower()
            if obj['tag_type'] not in ('qr', 'aruco', 'apriltag'):
                obj['tag_type'] = 'qr'

        if layer == 'locate_config':
            obj['mode'] = str(raw.get('mode', 'normal') or 'normal').strip().lower()
            if obj['mode'] not in ('normal', 'enhanced', 'lidar_only', 'visual_only'):
                obj['mode'] = 'normal'

        if layer == 'reflector':
            obj['reflector_type'] = str(raw.get('reflector_type', 'standard') or 'standard').strip().lower()
            if obj['reflector_type'] not in ('standard', 'corner_cube', 'tape'):
                obj['reflector_type'] = 'standard'
            try:
                obj['reflector_id'] = max(0, int(raw.get('reflector_id', 0)))
            except (TypeError, ValueError):
                obj['reflector_id'] = 0

        if layer == 'clean_area':
            obj['clean_mode'] = str(raw.get('clean_mode', 'sweep') or 'sweep').strip().lower()
            if obj['clean_mode'] not in ('sweep', 'scrub', 'vacuum'):
                obj['clean_mode'] = 'sweep'
            try:
                obj['passes'] = max(1, min(10, int(raw.get('passes', 1))))
            except (TypeError, ValueError):
                obj['passes'] = 1

        if layer == 'description_area':
            obj['label'] = str(raw.get('label', '') or '').strip()[:80]

        return obj, ''

    @staticmethod
    def _point_in_polygon(x: float, y: float, points: List[Dict[str, float]]) -> bool:
        inside = False
        count = len(points)
        if count < 3:
            return False
        j = count - 1
        for i in range(count):
            xi = float(points[i].get('x', 0.0))
            yi = float(points[i].get('y', 0.0))
            xj = float(points[j].get('x', 0.0))
            yj = float(points[j].get('y', 0.0))
            intersects = ((yi > y) != (yj > y)) and (
                x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-9) + xi
            )
            if intersects:
                inside = not inside
            j = i
        return inside

    @staticmethod
    def _shape_area(obj: Dict[str, Any]) -> float:
        shape = str(obj.get('type', 'rectangle') or 'rectangle')
        if shape == 'rectangle':
            return abs(float(obj.get('x2', 0.0)) - float(obj.get('x1', 0.0))) * abs(
                float(obj.get('y2', 0.0)) - float(obj.get('y1', 0.0))
            )
        if shape == 'circle':
            radius = abs(float(obj.get('radius', 0.0)))
            return math.pi * radius * radius
        if shape == 'polygon':
            points = obj.get('points', [])
            if not isinstance(points, list) or len(points) < 3:
                return float('inf')
            total = 0.0
            for index, point in enumerate(points):
                nxt = points[(index + 1) % len(points)]
                total += float(point.get('x', 0.0)) * float(nxt.get('y', 0.0))
                total -= float(nxt.get('x', 0.0)) * float(point.get('y', 0.0))
            return abs(total) * 0.5
        if shape == 'point':
            return 0.0
        return float('inf')

    @classmethod
    def _contains_point(cls, obj: Dict[str, Any], x: float, y: float) -> bool:
        shape = str(obj.get('type', 'rectangle') or 'rectangle')
        try:
            if shape == 'rectangle':
                x1, x2 = float(obj.get('x1', 0.0)), float(obj.get('x2', 0.0))
                y1, y2 = float(obj.get('y1', 0.0)), float(obj.get('y2', 0.0))
                return min(x1, x2) <= x <= max(x1, x2) and min(y1, y2) <= y <= max(y1, y2)
            if shape == 'circle':
                dx = x - float(obj.get('x', 0.0))
                dy = y - float(obj.get('y', 0.0))
                radius = float(obj.get('radius', 0.0))
                return dx * dx + dy * dy <= radius * radius
            if shape == 'polygon':
                points = obj.get('points', [])
                return isinstance(points, list) and cls._point_in_polygon(x, y, points)
            if shape == 'point':
                dx = x - float(obj.get('x', 0.0))
                dy = y - float(obj.get('y', 0.0))
                return math.hypot(dx, dy) <= 0.30
        except (TypeError, ValueError):
            return False
        return False

    def _lookup_robot_xy(self) -> Optional[Tuple[float, float]]:
        frame_candidates = [self.localization_zone_base_frame]
        if self.localization_zone_base_frame != 'base_link':
            frame_candidates.append('base_link')

        for base_frame in frame_candidates:
            try:
                transform = self.tf_buffer.lookup_transform(
                    self.localization_zone_map_frame,
                    base_frame,
                    Time(),
                    timeout=Duration(seconds=0.05),
                )
            except TransformException:
                continue
            translation = transform.transform.translation
            return float(translation.x), float(translation.y)
        return None

    def _active_localization_zone(self, x: float, y: float) -> Optional[Dict[str, Any]]:
        with self._lock:
            zones = list(self._layers.get('localization_zones', []))
        matches = [zone for zone in zones if self._contains_point(zone, x, y)]
        if not matches:
            return None
        return min(matches, key=lambda zone: (self._shape_area(zone), str(zone.get('id', ''))))

    @staticmethod
    def _zone_key(zone: Dict[str, Any]) -> str:
        zone_id = str(zone.get('id', '') or '').strip()
        if zone_id:
            return zone_id
        return json.dumps(zone, sort_keys=True, separators=(',', ':'))

    def _localization_zone_timer_callback(self) -> None:
        if not self.auto_switch_localization_zones or self._switch_inflight:
            return

        pose = self._lookup_robot_xy()
        if pose is None:
            return

        zone = self._active_localization_zone(pose[0], pose[1])
        if zone is None:
            self._pending_localization_zone_key = ''
            self._pending_localization_zone_since = 0.0
            return

        localization_mode = str(zone.get('localization_mode', 'point_cloud') or 'point_cloud').strip().lower()
        target_stack_mode = self.STACK_MODE_BY_LOCALIZATION_MODE.get(localization_mode, 'nav')
        zone_key = self._zone_key(zone)
        now = time.monotonic()

        if zone_key != self._pending_localization_zone_key:
            self._pending_localization_zone_key = zone_key
            self._pending_localization_zone_since = now
            if self.localization_zone_required_inside_sec > 0.0:
                return

        if now - self._pending_localization_zone_since < self.localization_zone_required_inside_sec:
            return
        if now - self._last_switch_attempt_time < self.localization_zone_switch_cooldown_sec:
            return
        if (
            target_stack_mode == self._last_requested_stack_mode
            and now - self._last_successful_switch_time < self.localization_zone_reassert_sec
        ):
            return
        if not self.stack_mode_client.service_is_ready():
            if now - self._last_stack_unavailable_log > 10.0:
                self._last_stack_unavailable_log = now
                self.get_logger().warn('Stack mode service not ready; localization zone switch skipped')
            return

        req = SetStackMode.Request()
        req.mode = target_stack_mode
        label = str(zone.get('label', '') or localization_mode).strip()
        zone_id = str(zone.get('id', '') or zone_key).strip()
        self._switch_inflight = True
        self._last_switch_attempt_time = now
        future = self.stack_mode_client.call_async(req)
        future.add_done_callback(
            lambda done, mode=target_stack_mode, zid=zone_id, name=label: self._on_stack_switch_done(done, mode, zid, name)
        )

    def _on_stack_switch_done(self, future, target_stack_mode: str, zone_id: str, label: str) -> None:
        self._switch_inflight = False
        try:
            response = future.result()
        except Exception as exc:
            self.get_logger().warn(f'Localization zone stack switch failed: {exc}')
            return

        if response is not None and bool(response.ok):
            self._last_requested_stack_mode = target_stack_mode
            self._last_successful_switch_time = time.monotonic()
            self.get_logger().info(
                f'Localization zone "{label}" ({zone_id}) selected stack mode {target_stack_mode}'
            )
            return

        message = response.message if response is not None else 'no response'
        self.get_logger().warn(
            f'Localization zone "{label}" ({zone_id}) failed to switch stack to {target_stack_mode}: {message}'
        )

    def get_layers_callback(self, _req, response):
        with self._lock:
            response.success = True
            response.message = 'Layers retrieved'
            response.layers_json = self._layers_json()
        return response

    def add_layer_callback(self, request, response):
        layer = str(request.layer or '').strip()
        if layer not in self.VALID_LAYERS:
            response.ok = False
            response.message = f'Invalid layer: {layer}'
            response.object_id = ''
            response.layers_json = self._layers_json()
            return response

        raw_obj, err = self._parse_object_json(request.object_json)
        if err:
            response.ok = False
            response.message = err
            response.object_id = ''
            response.layers_json = self._layers_json()
            return response

        with self._lock:
            obj, err = self._normalize_object(layer, raw_obj)
            if err:
                response.ok = False
                response.message = err
                response.object_id = ''
                response.layers_json = self._layers_json()
                return response

            self._layers[layer].append(obj)
            saved, save_msg = self._save_layers_to_database()
            response.ok = bool(saved)
            response.message = 'Filter added' if saved else save_msg
            response.object_id = str(obj.get('id') or '')
            response.layers_json = self._layers_json()
        return response

    def delete_layer_callback(self, request, response):
        layer = str(request.layer or '').strip()
        object_id = str(request.object_id or '').strip()

        if layer not in self.VALID_LAYERS:
            response.ok = False
            response.message = f'Invalid layer: {layer}'
            response.layers_json = self._layers_json()
            return response
        if not object_id:
            response.ok = False
            response.message = 'object_id is required'
            response.layers_json = self._layers_json()
            return response

        with self._lock:
            items = self._layers.get(layer, [])
            before = len(items)
            self._layers[layer] = [obj for obj in items if str(obj.get('id', '')) != object_id]
            if len(self._layers[layer]) == before:
                response.ok = False
                response.message = f'Filter id "{object_id}" not found in {layer}'
                response.layers_json = self._layers_json()
                return response

            saved, save_msg = self._save_layers_to_database()
            response.ok = bool(saved)
            response.message = 'Filter deleted' if saved else save_msg
            response.layers_json = self._layers_json()
        return response

    def clear_layer_callback(self, request, response):
        layer = str(request.layer or '').strip().lower() or 'all'

        with self._lock:
            if layer == 'all':
                for key in self.VALID_LAYERS:
                    self._layers[key] = []
            elif layer in self.VALID_LAYERS:
                self._layers[layer] = []
            else:
                response.ok = False
                response.message = f'Invalid layer: {layer}'
                response.layers_json = self._layers_json()
                return response

            saved, save_msg = self._save_layers_to_database()
            response.ok = bool(saved)
            response.message = 'Filters cleared' if saved else save_msg
            response.layers_json = self._layers_json()
        return response


def main():
    rclpy.init()
    node = MapLayerManager()
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
