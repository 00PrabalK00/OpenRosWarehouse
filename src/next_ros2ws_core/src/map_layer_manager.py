#!/usr/bin/env python3

import json
import os
import threading
import uuid
from typing import Any, Dict, List, Optional, Tuple

import rclpy
from rclpy.node import Node

from next_ros2ws_interfaces.srv import (
    AddMapLayerObject,
    ClearMapLayer,
    DeleteMapLayerObject,
    GetMapLayers,
)
from .db_manager import DatabaseManager


class MapLayerManager(Node):
    VALID_LAYERS = ('no_go_zones', 'restricted', 'slow_zones')

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

        count = sum(len(v) for v in self._layers.values())
        self.get_logger().info('MapLayerManager started')
        self.get_logger().info(f'Database: {db_path}')
        self.get_logger().info(f'Loaded layers: {count} objects')

    @staticmethod
    def _empty_layers() -> Dict[str, List[Dict[str, Any]]]:
        return {
            'no_go_zones': [],
            'restricted': [],
            'slow_zones': [],
        }

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
        if shape not in ('rectangle', 'polygon', 'circle'):
            return None, 'type must be rectangle, polygon, or circle'

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

        if layer == 'slow_zones':
            try:
                pct = int(raw.get('speed_percent', 50))
            except (TypeError, ValueError):
                return None, 'speed_percent must be an integer'
            pct = max(1, min(100, pct))
            obj['speed_percent'] = pct

        return obj, ''

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
