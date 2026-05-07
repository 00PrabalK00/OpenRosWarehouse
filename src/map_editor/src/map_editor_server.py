#!/usr/bin/env python3
"""
Professional Map Editor for Warehouse Robots
- Draw obstacles and no-go zones
- Set speed limit zones
- Edit map metadata
- Export/import map layers
"""

import base64
import io
import json
import math
import os
import threading
import sys

import numpy as np
import rclpy
import yaml
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from PIL import Image, ImageDraw
from rclpy.node import Node

# Add next_ros2ws_web/src to sys.path so we can import the primary dotted_map.py
_module_dir = os.path.dirname(os.path.abspath(__file__))
_web_src_dir = os.path.abspath(os.path.join(_module_dir, '..', '..', 'next_ros2ws_web', 'src'))
if _web_src_dir not in sys.path:
    sys.path.insert(0, _web_src_dir)

try:
    from dotted_map import generate_dotted_map
except ImportError:
    from map_editor.dotted_map import generate_dotted_map


def _resolve_map_editor_map_path(explicit_path: str = '') -> str:
    """Resolve a usable map YAML path without assuming a developer-specific home dir."""
    candidates = []

    raw_explicit = os.path.expanduser(str(explicit_path or '').strip())
    if raw_explicit:
        candidates.append(raw_explicit)

    env_override = os.path.expanduser(os.getenv('MAP_EDITOR_MAP_PATH', '').strip())
    if env_override:
        candidates.append(env_override)

    bringup_roots = []
    try:
        from ament_index_python.packages import get_package_share_directory
        bringup_roots.append(get_package_share_directory('ugv_bringup'))
    except Exception:
        pass

    module_dir = os.path.dirname(os.path.abspath(__file__))
    bringup_roots.append(os.path.normpath(os.path.join(module_dir, '..', '..', 'ui_ws')))

    for root in bringup_roots:
        maps_dir = os.path.join(root, 'maps')
        active_cfg = os.path.join(root, 'config', 'active_map_config.yaml')
        if os.path.isfile(active_cfg):
            try:
                with open(active_cfg, 'r', encoding='utf-8') as f:
                    active_payload = yaml.safe_load(f) or {}
                active_map = str(active_payload.get('active_map', '') or '').strip()
                if active_map:
                    if os.path.isabs(active_map):
                        candidates.append(active_map)
                    else:
                        candidates.append(os.path.join(maps_dir, active_map))
            except Exception:
                pass
        candidates.append(os.path.join(maps_dir, 'smr_map.yaml'))

    seen = set()
    normalized = []
    for candidate in candidates:
        expanded = os.path.abspath(os.path.expanduser(candidate))
        if expanded in seen:
            continue
        seen.add(expanded)
        normalized.append(expanded)

    for candidate in normalized:
        if os.path.isfile(candidate):
            return candidate

    return normalized[0] if normalized else os.path.abspath(os.path.expanduser('~/smr_map.yaml'))


def _load_map_bundle(map_path: str) -> dict:
    """Load a map bundle from a YAML map file or a direct image path."""
    resolved_path = os.path.abspath(os.path.expanduser(str(map_path or '').strip()))
    if not resolved_path:
        raise ValueError('Map path is required')
    if not os.path.exists(resolved_path):
        raise FileNotFoundError(f'Map path does not exist: {resolved_path}')

    lower_path = resolved_path.lower()
    map_data = {}
    image_path = resolved_path

    if lower_path.endswith(('.yaml', '.yml')):
        with open(resolved_path, 'r', encoding='utf-8') as f:
            map_data = yaml.safe_load(f) or {}
        image_path = str(map_data.get('image', '') or '').strip()
        if not image_path:
            raise ValueError(f'Map YAML does not define an image: {resolved_path}')
        if not os.path.isabs(image_path):
            image_path = os.path.join(os.path.dirname(resolved_path), image_path)
    elif lower_path.endswith(('.pgm', '.png', '.jpg', '.jpeg', '.bmp', '.tif', '.tiff')):
        map_data = {
            'resolution': 0.05,
            'origin': [0.0, 0.0, 0.0],
            'negate': 0,
            'occupied_thresh': 0.65,
            'free_thresh': 0.196,
        }
    else:
        raise ValueError('Unsupported map format. Use a map YAML or an image file.')

    image_path = os.path.abspath(os.path.expanduser(image_path))
    if not os.path.exists(image_path):
        raise FileNotFoundError(f'Map image does not exist: {image_path}')

    image = Image.open(image_path).convert('L')
    resolution = float(map_data.get('resolution', 0.05) or 0.05)
    origin = map_data.get('origin', [0.0, 0.0, 0.0]) or [0.0, 0.0, 0.0]
    if len(origin) < 3:
        origin = list(origin) + [0.0] * (3 - len(origin))

    return {
        'map_path': resolved_path,
        'image_path': image_path,
        'image': image,
        'width': image.width,
        'height': image.height,
        'resolution': resolution,
        'origin': [float(origin[0]), float(origin[1]), float(origin[2])],
        'negate': int(map_data.get('negate', 0) or 0),
        'occupied_thresh': float(map_data.get('occupied_thresh', 0.65) or 0.65),
        'free_thresh': float(map_data.get('free_thresh', 0.196) or 0.196),
    }


class MapEditorServer(Node):
    def __init__(self):
        super().__init__('map_editor_server')
        
        # Map storage
        configured_map = str(self.declare_parameter('map_yaml_path', '').value or '').strip()
        self.map_path = _resolve_map_editor_map_path(configured_map)
        self.original_map = None
        self.edited_map = None
        self.map_info = self.load_map_info()
        self.baseMap = self.original_map.copy() if self.original_map is not None else None

        # Map layers (separate editable layers)
        self.layers = {
            'obstacles': [],      # Drawn obstacles
            'no_go_zones': [],    # Red no-go areas
            'slow_zones': [],     # Yellow slow-speed zones
            'restricted': []      # Orange restricted areas
        }

        self.layers_file = os.path.expanduser('~/map_layers.json')
        self.load_layers()
        self._reset_stitch_state('idle')

        self.get_logger().info('Map Editor Server started')
        self.get_logger().info(f'Map: {self.map_info.get("image_path", "Not found")}')

    def load_map_info(self):
        """Load map YAML configuration"""
        try:
            bundle = _load_map_bundle(self.map_path)
            self.original_map = bundle['image']
            self.edited_map = self.original_map.copy()
            return {
                'image_path': bundle['image_path'],
                'resolution': bundle['resolution'],
                'origin': bundle['origin'],
                'negate': bundle['negate'],
                'occupied_thresh': bundle['occupied_thresh'],
                'free_thresh': bundle['free_thresh'],
            }
        except Exception as e:
            self.get_logger().error(f'Failed to load map: {e}')
            return {}
    
    def load_layers(self):
        """Load saved map layers"""
        if os.path.exists(self.layers_file):
            try:
                with open(self.layers_file, 'r') as f:
                    self.layers = json.load(f)
                self.get_logger().info(f'Loaded {sum(len(v) for v in self.layers.values())} layer objects')
            except Exception as e:
                self.get_logger().error(f'Failed to load layers: {e}')
    
    def save_layers(self):
        """Save map layers to file"""
        try:
            with open(self.layers_file, 'w') as f:
                json.dump(self.layers, f, indent=2)
            self.get_logger().info('Map layers saved')
            return True
        except Exception as e:
            self.get_logger().error(f'Failed to save layers: {e}')
            return False
    
    def add_layer_object(self, layer_type, obj):
        """Add object to a layer"""
        if layer_type in self.layers:
            self.layers[layer_type].append(obj)
            self.save_layers()
            return True
        return False
    
    def clear_layer(self, layer_type):
        """Clear all objects from a layer"""
        if layer_type in self.layers:
            self.layers[layer_type] = []
            self.save_layers()
            return True
        return False

    def _base_info(self):
        return {
            'resolution': float(self.map_info.get('resolution', 0.05) or 0.05),
            'origin': list(self.map_info.get('origin', [0.0, 0.0, 0.0]) or [0.0, 0.0, 0.0]),
            'width': int(self.baseMap.width if self.baseMap is not None else 0),
            'height': int(self.baseMap.height if self.baseMap is not None else 0),
        }

    @staticmethod
    def _map_center_world(map_meta: dict) -> tuple:
        resolution = float(map_meta.get('resolution', 0.05) or 0.05)
        origin = map_meta.get('origin', [0.0, 0.0, 0.0]) or [0.0, 0.0, 0.0]
        width = float(map_meta.get('width', 0) or 0)
        height = float(map_meta.get('height', 0) or 0)
        center_x = float(origin[0]) + (width * resolution * 0.5)
        center_y = float(origin[1]) + (height * resolution * 0.5)
        return center_x, center_y

    def _stitched_center_world(self) -> tuple:
        if not self.stitchedMap:
            return 0.0, 0.0
        return self._map_center_world(self.stitchedMap)

    def _occupancy_threshold(self, meta: dict) -> float:
        return 255.0 * float(meta.get('occupied_thresh', 0.65) or 0.65)

    def _map_world_to_pixel(self, map_meta: dict, wx: float, wy: float) -> tuple:
        resolution = float(map_meta.get('resolution', 0.05) or 0.05)
        origin = map_meta.get('origin', [0.0, 0.0, 0.0]) or [0.0, 0.0, 0.0]
        height = float(map_meta.get('height', 0) or 0)
        px = (float(wx) - float(origin[0])) / resolution
        py = height - ((float(wy) - float(origin[1])) / resolution)
        return px, py

    def _map_pixel_to_world(self, map_meta: dict, px: float, py: float) -> tuple:
        resolution = float(map_meta.get('resolution', 0.05) or 0.05)
        origin = map_meta.get('origin', [0.0, 0.0, 0.0]) or [0.0, 0.0, 0.0]
        height = float(map_meta.get('height', 0) or 0)
        wx = float(origin[0]) + (float(px) * resolution)
        wy = float(origin[1]) + ((height - float(py)) * resolution)
        return wx, wy

    def _apply_transform_to_world(self, wx: float, wy: float, transform: dict = None) -> tuple:
        transform = transform or self.stitchTransform
        center_x, center_y = self._stitched_center_world()
        dx = float(wx) - center_x
        dy = float(wy) - center_y
        rotation = math.radians(float(transform.get('rotation', 0.0) or 0.0))
        scale = float(transform.get('scale', 1.0) or 1.0)
        cos_theta = math.cos(rotation)
        sin_theta = math.sin(rotation)
        rx = scale * ((cos_theta * dx) - (sin_theta * dy))
        ry = scale * ((sin_theta * dx) + (cos_theta * dy))
        tx = float(transform.get('x', 0.0) or 0.0)
        ty = float(transform.get('y', 0.0) or 0.0)
        return center_x + tx + rx, center_y + ty + ry

    def _invert_transform_to_world(self, wx: float, wy: float, transform: dict = None) -> tuple:
        transform = transform or self.stitchTransform
        center_x, center_y = self._stitched_center_world()
        tx = float(transform.get('x', 0.0) or 0.0)
        ty = float(transform.get('y', 0.0) or 0.0)
        dx = float(wx) - center_x - tx
        dy = float(wy) - center_y - ty
        rotation = math.radians(float(transform.get('rotation', 0.0) or 0.0))
        scale = float(transform.get('scale', 1.0) or 1.0)
        safe_scale = scale if abs(scale) > 1e-9 else 1.0
        cos_theta = math.cos(rotation)
        sin_theta = math.sin(rotation)
        ix = (cos_theta * dx + sin_theta * dy) / safe_scale
        iy = (-sin_theta * dx + cos_theta * dy) / safe_scale
        return center_x + ix, center_y + iy

    def _empty_transform(self) -> dict:
        return {'x': 0.0, 'y': 0.0, 'rotation': 0.0, 'scale': 1.0}

    def _centered_transform(self) -> dict:
        transform = self._empty_transform()
        if self.baseMap is None or not self.stitchedMap:
            return transform
        base_center = self._map_center_world(self._base_info())
        stitched_center = self._stitched_center_world()
        transform['x'] = base_center[0] - stitched_center[0]
        transform['y'] = base_center[1] - stitched_center[1]
        return transform

    def _stitch_state_payload(self, include_image: bool = False) -> dict:
        payload = {
            'state': self.stitchState,
            'alignmentMode': self.alignmentMode,
            'stitchTransform': dict(self.stitchTransform),
            'regionMatching': dict(self.regionMatching),
            'referencePointMatching': {
                'pairCount': len(self.referencePointMatching.get('pairs', [])),
                'lastError': self.referencePointMatching.get('lastError'),
            },
            'stitchedMap': None,
        }
        if self.stitchedMap:
            stitched_payload = {
                'mapPath': self.stitchedMap.get('map_path'),
                'imagePath': self.stitchedMap.get('image_path'),
                'width': self.stitchedMap.get('width'),
                'height': self.stitchedMap.get('height'),
                'resolution': self.stitchedMap.get('resolution'),
                'origin': self.stitchedMap.get('origin'),
            }
            if include_image:
                img_io = io.BytesIO()
                self.stitchedMap['image'].save(img_io, 'PNG')
                stitched_payload['image'] = base64.b64encode(img_io.getvalue()).decode('utf-8')
            payload['stitchedMap'] = stitched_payload
        return payload

    def _reset_stitch_state(self, state: str):
        self.stitchedMap = None
        self.stitchTransform = self._empty_transform()
        self.stitchState = state
        self.alignmentMode = None
        self.regionMatching = {'selection': None, 'lastScore': None}
        self.referencePointMatching = {'pairs': [], 'lastError': None}
        return self._stitch_state_payload()

    def cancelStitch(self):
        return self._reset_stitch_state('cancelled')

    def startStitch(self, map_path: str) -> dict:
        if self.baseMap is None:
            raise RuntimeError('Base map is not loaded')
        bundle = _load_map_bundle(map_path)
        self.stitchedMap = bundle
        self.stitchTransform = self._centered_transform()
        self.stitchState = 'stitching_map_loaded'
        self.alignmentMode = None
        self.regionMatching = {'selection': None, 'lastScore': None}
        self.referencePointMatching = {'pairs': [], 'lastError': None}
        return self._stitch_state_payload(include_image=True)

    def confirm_rough_placement(self):
        if not self.stitchedMap:
            raise RuntimeError('Load a stitched map before confirming placement')
        self.stitchState = 'alignment_active'
        return self._stitch_state_payload()

    def update_stitch_transform(self, transform: dict):
        if not self.stitchedMap:
            raise RuntimeError('Load a stitched map before moving it')
        self.stitchTransform = {
            'x': float(transform.get('x', self.stitchTransform.get('x', 0.0)) or 0.0),
            'y': float(transform.get('y', self.stitchTransform.get('y', 0.0)) or 0.0),
            'rotation': float(transform.get('rotation', self.stitchTransform.get('rotation', 0.0)) or 0.0),
            'scale': float(transform.get('scale', self.stitchTransform.get('scale', 1.0)) or 1.0),
        }
        if self.stitchState in ('stitching_map_loaded', 'idle', 'cancelled'):
            self.stitchState = 'rough_placement'
        return self._stitch_state_payload()

    def _similarity_transform_general(self, pairs: list, allow_scale: bool = True) -> tuple:
        if len(pairs) < 2:
            raise ValueError('At least 2 valid point pairs are required')

        stitched_points = np.array(
            [[float(pair['stitched']['x']), float(pair['stitched']['y'])] for pair in pairs],
            dtype=np.float64,
        )
        base_points = np.array(
            [[float(pair['base']['x']), float(pair['base']['y'])] for pair in pairs],
            dtype=np.float64,
        )

        stitched_mean = stitched_points.mean(axis=0)
        base_mean = base_points.mean(axis=0)
        centered_stitched = stitched_points - stitched_mean
        centered_base = base_points - base_mean

        covariance = centered_stitched.T @ centered_base / len(pairs)
        u, singular_values, vt = np.linalg.svd(covariance)
        rotation_matrix = vt.T @ u.T
        if np.linalg.det(rotation_matrix) < 0:
            vt[-1, :] *= -1
            rotation_matrix = vt.T @ u.T

        scale = 1.0
        if allow_scale:
            variance = np.sum(centered_stitched ** 2) / len(pairs)
            if variance > 1e-12:
                scale = float(np.sum(singular_values) / variance)

        translation = base_mean - (scale * (rotation_matrix @ stitched_mean))
        rotation_deg = math.degrees(math.atan2(rotation_matrix[1, 0], rotation_matrix[0, 0]))
        prediction = (scale * (stitched_points @ rotation_matrix.T)) + translation
        rms_error = float(np.sqrt(np.mean(np.sum((prediction - base_points) ** 2, axis=1))))
        return rotation_deg, scale, translation, rms_error

    def _general_to_centered_transform(self, rotation_deg: float, scale: float, translation: np.ndarray) -> dict:
        center = np.array(self._stitched_center_world(), dtype=np.float64)
        theta = math.radians(float(rotation_deg))
        rotation_matrix = np.array(
            [[math.cos(theta), -math.sin(theta)], [math.sin(theta), math.cos(theta)]],
            dtype=np.float64,
        )
        centered_translation = translation - center + (scale * (rotation_matrix @ center))
        return {
            'x': float(centered_translation[0]),
            'y': float(centered_translation[1]),
            'rotation': float(rotation_deg),
            'scale': float(scale),
        }

    def apply_reference_point_matching(self, pairs: list, allow_scale: bool = True):
        if not self.stitchedMap:
            raise RuntimeError('Load a stitched map before reference point matching')
        valid_pairs = [
            pair for pair in pairs
            if isinstance(pair, dict)
            and isinstance(pair.get('base'), dict)
            and isinstance(pair.get('stitched'), dict)
        ]
        if len(valid_pairs) < 2:
            raise ValueError('At least 2 valid point pairs are required. 3 or more are recommended.')

        rotation_deg, scale, translation, rms_error = self._similarity_transform_general(valid_pairs, allow_scale=allow_scale)
        self.stitchTransform = self._general_to_centered_transform(rotation_deg, scale, translation)
        self.alignmentMode = 'referencePointMatching'
        self.stitchState = 'reference_point_matching_active'
        self.referencePointMatching = {'pairs': valid_pairs, 'lastError': rms_error}
        return self._stitch_state_payload()

    def _transformed_mask(self, transform: dict = None, crop_bounds: tuple = None) -> np.ndarray:
        if not self.stitchedMap:
            raise RuntimeError('No stitched map is loaded')
        transform = transform or self.stitchTransform
        occupancy_threshold = self._occupancy_threshold(self.stitchedMap)
        stitched_mask = self.stitchedMap['image'].point(lambda px: 255 if px <= occupancy_threshold else 0, mode='L')

        full_matrix = self._affine_output_to_input(transform=transform)
        output_width = int(self.baseMap.width)
        output_height = int(self.baseMap.height)
        matrix = full_matrix
        if crop_bounds is not None:
            min_x, min_y, max_x, max_y = crop_bounds
            output_width = max(1, int(max_x - min_x))
            output_height = max(1, int(max_y - min_y))
            offset = np.array([[1.0, 0.0, float(min_x)], [0.0, 1.0, float(min_y)], [0.0, 0.0, 1.0]], dtype=np.float64)
            matrix = full_matrix @ offset

        transformed = stitched_mask.transform(
            (output_width, output_height),
            Image.AFFINE,
            data=tuple(matrix[:2, :].reshape(-1)),
            resample=Image.NEAREST,
            fillcolor=0,
        )
        return np.array(transformed, dtype=np.uint8)

    def _affine_output_to_input(self, transform: dict = None) -> np.ndarray:
        if not self.stitchedMap:
            raise RuntimeError('No stitched map is loaded')
        transform = transform or self.stitchTransform
        stitched_meta = self.stitchedMap
        base_meta = self._base_info()

        source_points = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]
        target_points = []
        for px, py in source_points:
            wx, wy = self._map_pixel_to_world(stitched_meta, px, py)
            twx, twy = self._apply_transform_to_world(wx, wy, transform=transform)
            bx, by = self._map_world_to_pixel(base_meta, twx, twy)
            target_points.append((bx, by))

        forward = np.array(
            [
                [target_points[1][0] - target_points[0][0], target_points[2][0] - target_points[0][0], target_points[0][0]],
                [target_points[1][1] - target_points[0][1], target_points[2][1] - target_points[0][1], target_points[0][1]],
                [0.0, 0.0, 1.0],
            ],
            dtype=np.float64,
        )
        return np.linalg.inv(forward)

    def apply_region_matching(self, selection: dict):
        if not self.stitchedMap:
            raise RuntimeError('Load a stitched map before region matching')
        required_keys = ('x1', 'y1', 'x2', 'y2')
        if not selection or any(key not in selection for key in required_keys):
            raise ValueError('Region matching requires a valid selection rectangle')

        base_meta = self._base_info()
        p1 = self._map_world_to_pixel(base_meta, float(selection['x1']), float(selection['y1']))
        p2 = self._map_world_to_pixel(base_meta, float(selection['x2']), float(selection['y2']))
        min_x = max(0, int(math.floor(min(p1[0], p2[0]))))
        max_x = min(base_meta['width'], int(math.ceil(max(p1[0], p2[0]))))
        min_y = max(0, int(math.floor(min(p1[1], p2[1]))))
        max_y = min(base_meta['height'], int(math.ceil(max(p1[1], p2[1]))))
        if max_x - min_x < 8 or max_y - min_y < 8:
            raise ValueError('Selected rectangle is too small for region matching')

        base_array = np.array(self.baseMap, dtype=np.uint8)
        crop_bounds = (min_x, min_y, max_x, max_y)
        base_region = base_array[min_y:max_y, min_x:max_x]
        base_occ = base_region <= self._occupancy_threshold(self.map_info)
        if int(base_occ.sum()) < 10:
            raise ValueError('Selected region does not contain enough shared base-map features')

        best_transform = dict(self.stitchTransform)
        best_score = -1e18
        translation_span = max(float(self.map_info.get('resolution', 0.05) or 0.05) * 12.0, 0.6)
        translation_fine = max(float(self.map_info.get('resolution', 0.05) or 0.05) * 3.0, 0.15)
        stages = [
            {'angle_span': 8.0, 'angle_step': 2.0, 'translation_span': translation_span, 'translation_step': translation_span / 4.0},
            {'angle_span': 2.0, 'angle_step': 0.5, 'translation_span': translation_fine, 'translation_step': translation_fine / 3.0},
        ]

        for stage in stages:
            candidate_seed = dict(best_transform)
            angle_offsets = np.arange(-stage['angle_span'], stage['angle_span'] + 1e-9, stage['angle_step'])
            translation_offsets = np.arange(
                -stage['translation_span'],
                stage['translation_span'] + 1e-9,
                max(stage['translation_step'], 1e-6),
            )
            for angle_offset in angle_offsets:
                for delta_x in translation_offsets:
                    for delta_y in translation_offsets:
                        candidate = dict(candidate_seed)
                        candidate['rotation'] = float(candidate_seed['rotation']) + float(angle_offset)
                        candidate['x'] = float(candidate_seed['x']) + float(delta_x)
                        candidate['y'] = float(candidate_seed['y']) + float(delta_y)
                        stitch_mask = self._transformed_mask(transform=candidate, crop_bounds=crop_bounds) > 0
                        overlap = np.logical_and(base_occ, stitch_mask).sum()
                        total = int(base_occ.sum()) + int(stitch_mask.sum())
                        if total <= 0:
                            continue
                        mismatch = np.logical_xor(base_occ, stitch_mask).sum()
                        score = (2.0 * float(overlap) / float(total)) - (0.15 * float(mismatch) / float(total))
                        if score > best_score:
                            best_score = score
                            best_transform = candidate

        self.stitchTransform = best_transform
        self.alignmentMode = 'regionMatching'
        self.stitchState = 'region_matching_active'
        self.regionMatching = {'selection': dict(selection), 'lastScore': float(best_score)}
        return self._stitch_state_payload()

    def _merge_stitched_map(self):
        if self.baseMap is None or not self.stitchedMap:
            raise RuntimeError('Base and stitched maps must both exist before merging')
        stitched_array = np.array(self.stitchedMap['image'], dtype=np.uint8)
        mask = self._transformed_mask()
        transformed_image = self.stitchedMap['image'].transform(
            (self.baseMap.width, self.baseMap.height),
            Image.AFFINE,
            data=tuple(self._affine_output_to_input()[:2, :].reshape(-1)),
            resample=Image.BILINEAR,
            fillcolor=255,
        )
        _ = stitched_array  # keeps intent explicit for future extensions
        transformed_array = np.array(transformed_image, dtype=np.uint8)
        base_array = np.array(self.baseMap, dtype=np.uint8)
        merge_mask = mask > 0
        base_array[merge_mask] = np.minimum(base_array[merge_mask], transformed_array[merge_mask])
        self.baseMap = Image.fromarray(base_array.astype(np.uint8), mode='L')
        self.original_map = self.baseMap.copy()
        self.edited_map = self.baseMap.copy()

    def confirmStitch(self):
        if not self.stitchedMap:
            raise RuntimeError('Load a stitched map before confirming the stitch')
        self._merge_stitched_map()
        self.stitchState = 'confirmed'
        payload = self._stitch_state_payload()
        self.cancelStitch()
        payload['state'] = 'confirmed'
        payload['merged'] = True
        payload['stitchedMap'] = None
        payload['alignmentMode'] = None
        return payload

    def render_map_with_layers(self):
        """Render map with all layers overlaid"""
        if self.baseMap is None:
            return None
        
        # Start with original map
        img = self.baseMap.copy().convert('RGB')
        draw = ImageDraw.Draw(img, 'RGBA')
        
        resolution = self.map_info.get('resolution', 0.05)
        origin = self.map_info.get('origin', [0, 0, 0])
        height = img.height
        
        def world_to_pixel(wx, wy):
            px = int((wx - origin[0]) / resolution)
            py = int(height - (wy - origin[1]) / resolution)
            return (px, py)
        
        # Draw obstacles (black)
        for obj in self.layers.get('obstacles', []):
            if obj['type'] == 'rectangle':
                p1 = world_to_pixel(obj['x1'], obj['y1'])
                p2 = world_to_pixel(obj['x2'], obj['y2'])
                draw.rectangle([p1, p2], fill=(0, 0, 0, 255))
            elif obj['type'] == 'circle':
                center = world_to_pixel(obj['x'], obj['y'])
                radius_px = int(obj['radius'] / resolution)
                bbox = [center[0] - radius_px, center[1] - radius_px,
                       center[0] + radius_px, center[1] + radius_px]
                draw.ellipse(bbox, fill=(0, 0, 0, 255))
            elif obj['type'] == 'polygon':
                points = [world_to_pixel(p['x'], p['y']) for p in obj['points']]
                draw.polygon(points, fill=(0, 0, 0, 255))
            elif obj['type'] == 'line':
                p1 = world_to_pixel(obj['x1'], obj['y1'])
                p2 = world_to_pixel(obj['x2'], obj['y2'])
                draw.line([p1, p2], fill=(0, 0, 0, 255), width=2)
        
        # Draw no-go zones (red transparent)
        for obj in self.layers.get('no_go_zones', []):
            if obj['type'] == 'rectangle':
                p1 = world_to_pixel(obj['x1'], obj['y1'])
                p2 = world_to_pixel(obj['x2'], obj['y2'])
                draw.rectangle([p1, p2], fill=(255, 0, 0, 100), outline=(255, 0, 0, 255), width=2)
            elif obj['type'] == 'polygon':
                points = [world_to_pixel(p['x'], p['y']) for p in obj['points']]
                draw.polygon(points, fill=(255, 0, 0, 100), outline=(255, 0, 0, 255))
        
        # Draw slow zones (yellow transparent)
        for obj in self.layers.get('slow_zones', []):
            if obj['type'] == 'rectangle':
                p1 = world_to_pixel(obj['x1'], obj['y1'])
                p2 = world_to_pixel(obj['x2'], obj['y2'])
                draw.rectangle([p1, p2], fill=(255, 255, 0, 80), outline=(255, 200, 0, 255), width=2)
                # Draw speed limit text
                speed = obj.get('speed_limit', 0.2)
                center_x = (p1[0] + p2[0]) // 2
                center_y = (p1[1] + p2[1]) // 2
                draw.text((center_x, center_y), f"{speed}m/s", fill=(0, 0, 0, 255))
        
        # Draw restricted zones (orange transparent)
        for obj in self.layers.get('restricted', []):
            if obj['type'] == 'rectangle':
                p1 = world_to_pixel(obj['x1'], obj['y1'])
                p2 = world_to_pixel(obj['x2'], obj['y2'])
                draw.rectangle([p1, p2], fill=(255, 165, 0, 100), outline=(255, 140, 0, 255), width=2)
        
        return img
    
    def export_edited_map(self, output_path):
        """Export edited map as new PGM file"""
        try:
            rendered = self.render_map_with_layers()
            if rendered is None:
                return False
            
            # Convert back to grayscale
            gray_map = rendered.convert('L')
            
            # Save as PGM
            gray_map.save(output_path)
            
            # Create corresponding YAML
            yaml_path = output_path.replace('.pgm', '.yaml')
            yaml_data = {
                'image': output_path,
                'resolution': self.map_info.get('resolution', 0.05),
                'origin': self.map_info.get('origin', [0, 0, 0]),
                'negate': 0,
                'occupied_thresh': 0.65,
                'free_thresh': 0.196
            }
            
            with open(yaml_path, 'w') as f:
                yaml.dump(yaml_data, f, default_flow_style=False)
            
            self.get_logger().info(f'Exported edited map to {output_path}')
            return True
        except Exception as e:
            self.get_logger().error(f'Failed to export map: {e}')
            return False


def _resolve_web_root():
    """Resolve web asset directory for both installed and source runs."""
    candidates = []
    try:
        from ament_index_python.packages import get_package_share_directory
        share_dir = get_package_share_directory('map_editor')
        candidates.append(os.path.join(share_dir, 'web'))
    except Exception:
        pass

    module_dir = os.path.dirname(os.path.abspath(__file__))
    candidates.append(os.path.normpath(os.path.join(module_dir, '..', 'web')))
    candidates.append(os.path.normpath(os.path.join(module_dir, 'web')))

    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate
    return candidates[-1]


_WEB_ROOT = _resolve_web_root()

# Create Flask app
app = Flask(
    __name__,
    template_folder=os.path.join(_WEB_ROOT, 'templates'),
    static_folder=os.path.join(_WEB_ROOT, 'static'),
)
CORS(app)

# Global node reference
editor_node = None


@app.route('/')
def index():
    return render_template('map_editor.html')


@app.route('/api/map/info')
def get_map_info():
    """Get current map information"""
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    
    return jsonify({
        'resolution': editor_node.map_info.get('resolution'),
        'origin': editor_node.map_info.get('origin'),
        'width': editor_node.baseMap.width if editor_node.baseMap else 0,
        'height': editor_node.baseMap.height if editor_node.baseMap else 0
    })


@app.route('/api/map/preview')
def get_map_preview():
    """Get rendered map with layers.

    Optional query parameters for dotted display mode:
      dotted=true          — enable dotted wall visualization
      spacing=15           — pixels between dots (default 15)
      dot_radius=2         — marker size in pixels (default 2)
      marker=circle|plus   — marker shape (default circle)
      thin=true|false      — skeletonize thick walls first (default true)
    """
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500

    try:
        rendered = editor_node.render_map_with_layers()
        if rendered is None:
            return jsonify({'error': 'No map loaded'}), 500

        dotted = request.args.get('dotted', 'false').lower() in ('1', 'true', 'yes')
        if dotted:
            spacing   = int(request.args.get('spacing', 8))
            dot_radius = int(request.args.get('dot_radius', 2))
            marker    = request.args.get('marker', 'circle')
            thin      = request.args.get('thin', 'false').lower() not in ('0', 'false', 'no')

            # Collect user-drawn wall lines (stored as world coords) and convert
            # to pixel coords so generate_dotted_map can dot them geometrically.
            resolution = editor_node.map_info.get('resolution', 0.05)
            origin     = editor_node.map_info.get('origin', [0, 0, 0])
            img_height = rendered.height

            def _w2p(wx, wy):
                px = int((wx - origin[0]) / resolution)
                py = int(img_height - (wy - origin[1]) / resolution)
                return (px, py)

            extra_lines = []
            for obj in editor_node.layers.get('obstacles', []):
                if obj.get('type') == 'line':
                    p1 = _w2p(obj['x1'], obj['y1'])
                    p2 = _w2p(obj['x2'], obj['y2'])
                    extra_lines.append((p1[0], p1[1], p2[0], p2[1]))

            rendered = generate_dotted_map(
                rendered,
                map_resolution_m_per_px=resolution,
                dot_radius=max(1, dot_radius),
                marker_type=marker if marker in ('circle', 'plus') else 'circle',
                extra_lines=extra_lines or None,
            )

        img_io = io.BytesIO()
        rendered.save(img_io, 'PNG')
        img_io.seek(0)
        img_base64 = base64.b64encode(img_io.getvalue()).decode('utf-8')

        return jsonify({
            'image': img_base64,
            'width': rendered.width,
            'height': rendered.height
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/layers', methods=['GET'])
def get_layers():
    """Get all map layers"""
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    
    return jsonify(editor_node.layers)


@app.route('/api/layer/add', methods=['POST'])
def add_layer_object():
    """Add object to a layer"""
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    
    data = request.json
    layer_type = data.get('layer')
    obj = data.get('object')
    
    if not layer_type or not obj:
        return jsonify({'error': 'Missing layer or object'}), 400
    
    success = editor_node.add_layer_object(layer_type, obj)
    return jsonify({'ok': success})


@app.route('/api/layer/clear', methods=['POST'])
def clear_layer():
    """Clear all objects from a layer"""
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    
    data = request.json
    layer_type = data.get('layer')
    
    if not layer_type:
        return jsonify({'error': 'Missing layer type'}), 400
    
    success = editor_node.clear_layer(layer_type)
    return jsonify({'ok': success})


@app.route('/api/map/export', methods=['POST'])
def export_map():
    """Export edited map to new PGM file"""
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    
    data = request.json
    output_path = data.get('path', '/tmp/edited_map.pgm')
    
    success = editor_node.export_edited_map(output_path)
    return jsonify({'ok': success, 'path': output_path})


@app.route('/api/stitch/state', methods=['GET'])
def get_stitch_state():
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    return jsonify(editor_node._stitch_state_payload(include_image=True))


@app.route('/api/stitch/load', methods=['POST'])
def stitch_load():
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    try:
        data = request.json or {}
        path = str(data.get('path', '') or '').strip()
        return jsonify(editor_node.startStitch(path))
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.route('/api/stitch/transform', methods=['POST'])
def stitch_transform():
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    try:
        data = request.json or {}
        return jsonify(editor_node.update_stitch_transform(data.get('stitchTransform') or {}))
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.route('/api/stitch/confirm-placement', methods=['POST'])
def stitch_confirm_placement():
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    try:
        return jsonify(editor_node.confirm_rough_placement())
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.route('/api/stitch/align/region', methods=['POST'])
def stitch_align_region():
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    try:
        data = request.json or {}
        return jsonify(editor_node.apply_region_matching(data.get('selection') or {}))
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.route('/api/stitch/align/reference', methods=['POST'])
def stitch_align_reference():
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    try:
        data = request.json or {}
        pairs = data.get('pairs') or []
        allow_scale = bool(data.get('allowScale', True))
        return jsonify(editor_node.apply_reference_point_matching(pairs, allow_scale=allow_scale))
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.route('/api/stitch/confirm', methods=['POST'])
def stitch_confirm():
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    try:
        return jsonify(editor_node.confirmStitch())
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.route('/api/stitch/cancel', methods=['POST'])
def stitch_cancel():
    if editor_node is None:
        return jsonify({'error': 'Node not initialized'}), 500
    return jsonify(editor_node.cancelStitch())


def run_flask():
    """Run Flask server"""
    app.run(host='0.0.0.0', port=5001, debug=False, use_reloader=False)


def main(args=None):
    global editor_node
    
    rclpy.init(args=args)
    editor_node = MapEditorServer()
    
    # Start Flask in separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    editor_node.get_logger().info('Map Editor UI available at: http://localhost:5001')
    
    try:
        rclpy.spin(editor_node)
    except KeyboardInterrupt:
        pass
    finally:
        editor_node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
