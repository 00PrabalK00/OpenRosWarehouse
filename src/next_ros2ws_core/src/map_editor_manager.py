#!/usr/bin/env python3

import base64
import io
import os
import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import rclpy
import yaml
from ament_index_python.packages import PackageNotFoundError, get_package_share_directory
from rclpy.node import Node

from next_ros2ws_core.map_dotted_truth import (
    build_dotted_truth_plan,
    ensure_dotted_truth_map,
)
from next_ros2ws_interfaces.srv import (
    ExportEditorMap,
    GetActiveMap,
    GetEditorMapPreview,
    OverwriteEditorMap,
    ReloadEditorMap,
    SaveCurrentMap,
)

try:
    from PIL import Image
except Exception:
    Image = None


def _looks_like_workspace_root(path: str) -> bool:
    candidate = os.path.abspath(os.path.expanduser(path or os.getcwd()))
    return (
        os.path.isdir(os.path.join(candidate, 'maps'))
        and os.path.isdir(os.path.join(candidate, 'config'))
    )


def _prefer_canonical_workspace_root(initial_root: str) -> str:
    root = os.path.abspath(os.path.expanduser(initial_root or os.getcwd()))
    marker = f'{os.sep}install{os.sep}'

    candidates = []
    if marker in root:
        repo_root = root.split(marker, 1)[0]
        candidates.append(os.path.join(repo_root, 'src', 'ui_ws'))

    wrapper = os.path.join(root, 'scripts', 'nav2_launch_wrapper.sh')
    if os.path.exists(wrapper):
        candidates.append(os.path.dirname(os.path.dirname(os.path.realpath(wrapper))))

    active_cfg = os.path.join(root, 'config', 'active_map_config.yaml')
    if os.path.exists(active_cfg):
        candidates.append(os.path.dirname(os.path.dirname(os.path.realpath(active_cfg))))

    candidates.append(root)

    seen = set()
    for candidate in candidates:
        if not candidate:
            continue
        candidate_abs = os.path.abspath(candidate)
        if candidate_abs in seen:
            continue
        seen.add(candidate_abs)
        if _looks_like_workspace_root(candidate_abs):
            return candidate_abs

    return root


def _canonicalize_map_yaml_path(raw_path: str, workspace_root: str) -> str:
    candidate = str(raw_path or '').strip()
    if not candidate:
        return ''

    expanded = os.path.abspath(os.path.expanduser(candidate))
    base_name = os.path.basename(expanded)
    if not base_name:
        return expanded

    candidates = []
    marker = f'{os.sep}install{os.sep}'
    if marker in expanded:
        repo_root = expanded.split(marker, 1)[0]
        candidates.append(os.path.join(repo_root, 'src', 'ui_ws', 'maps', base_name))

    if workspace_root:
        candidates.append(os.path.join(workspace_root, 'maps', base_name))

    candidates.append(expanded)

    seen = set()
    for path in candidates:
        resolved = os.path.abspath(os.path.expanduser(path))
        if resolved in seen:
            continue
        seen.add(resolved)
        if os.path.exists(resolved):
            return resolved

    return expanded


def _resolve_workspace_root() -> str:
    env_root = os.getenv('NEXT_UI_ROOT')
    if env_root:
        return _prefer_canonical_workspace_root(env_root)

    try:
        return _prefer_canonical_workspace_root(get_package_share_directory('ugv_bringup'))
    except PackageNotFoundError:
        pass

    cur = os.path.abspath(os.path.dirname(__file__))
    for _ in range(8):
        if _looks_like_workspace_root(cur):
            return _prefer_canonical_workspace_root(cur)
        cur = os.path.dirname(cur)

    return _prefer_canonical_workspace_root(os.getcwd())


class MapEditorManager(Node):
    """Owns editor map staging and persistence operations."""

    def __init__(self):
        super().__init__('map_editor_manager')

        self.workspace_root = _resolve_workspace_root()
        self.maps_dir = os.path.join(self.workspace_root, 'maps')
        os.makedirs(self.maps_dir, exist_ok=True)

        self.stage_image_file = os.path.join(self.workspace_root, '.map_editor_stage.png')
        self.stage_meta_file = os.path.join(self.workspace_root, '.map_editor_stage.yaml')

        self._lock = threading.RLock()
        self._stage_image = None
        self._stage_origin = [0.0, 0.0, 0.0]
        self._stage_resolution = 0.05
        self._stage_map_yaml_path = ''

        self.get_active_map_client = self.create_client(GetActiveMap, '/map_manager/get_active_map')

        self.preview_srv = self.create_service(GetEditorMapPreview, '/map_editor/preview', self.preview_callback)
        self.overwrite_srv = self.create_service(OverwriteEditorMap, '/map_editor/overwrite', self.overwrite_callback)
        self.save_current_srv = self.create_service(SaveCurrentMap, '/map_editor/save_current', self.save_current_callback)
        self.reload_srv = self.create_service(ReloadEditorMap, '/map_editor/reload', self.reload_callback)
        self.export_srv = self.create_service(ExportEditorMap, '/map_editor/export', self.export_callback)

        self._load_stage_cache()

        self.get_logger().info('MapEditorManager started')
        self.get_logger().info(f'workspace_root: {self.workspace_root}')

    def _ensure_dotted_truth(self, map_yaml_path: str, *, context: str) -> Dict[str, Any]:
        result = ensure_dotted_truth_map(
            map_yaml_path,
            workspace_root=self.workspace_root,
            logger=self.get_logger(),
        )
        if bool(result.get('ok')):
            if bool(result.get('generator_ran')) or bool(result.get('image_updated')):
                self.get_logger().info(
                    f'Dotted truth map ready for {context}: '
                    f'{result.get("dotted_image_path", "")}'
                )
            return result

        self.get_logger().error(f'{context}: {result.get("message", "Failed to prepare dotted truth map")}')
        return result

    def _wait_future(self, future, timeout_sec: float) -> bool:
        end = time.time() + max(0.0, float(timeout_sec))
        while time.time() < end:
            if future.done():
                return True
            time.sleep(0.01)
        return False

    def _call_service(self, client, request, *, wait_timeout: float, response_timeout: float):
        if not client.wait_for_service(timeout_sec=wait_timeout):
            return None
        future = client.call_async(request)
        if not self._wait_future(future, response_timeout):
            return None
        try:
            return future.result()
        except Exception:
            return None

    def _resolve_active_map_yaml(self) -> Optional[str]:
        req = GetActiveMap.Request()
        response = self._call_service(
            self.get_active_map_client,
            req,
            wait_timeout=0.5,
            response_timeout=1.0,
        )
        if response is not None and bool(response.success):
            candidate = _canonicalize_map_yaml_path(str(response.map_yaml_path or '').strip(), self.workspace_root)
            if candidate and os.path.exists(candidate):
                return os.path.abspath(candidate)

        cfg_file = os.path.join(self.workspace_root, 'config', 'active_map_config.yaml')
        default_map = os.path.join(self.maps_dir, 'smr_map.yaml')

        if os.path.exists(cfg_file):
            try:
                with open(cfg_file, 'r', encoding='utf-8') as f:
                    cfg = yaml.safe_load(f) or {}
                configured = str(cfg.get('active_map', '') or '').strip()
                if configured:
                    if os.path.isabs(configured):
                        candidate = configured
                    else:
                        candidate = os.path.join(self.maps_dir, configured)
                    candidate = _canonicalize_map_yaml_path(candidate, self.workspace_root)
                    if os.path.exists(candidate):
                        return os.path.abspath(candidate)
            except Exception as exc:
                self.get_logger().warn(f'Failed reading active_map_config.yaml: {exc}')

        if os.path.exists(default_map):
            return os.path.abspath(default_map)

        for name in sorted(os.listdir(self.maps_dir)):
            if name.endswith('.yaml'):
                path = os.path.join(self.maps_dir, name)
                if os.path.exists(path):
                    return os.path.abspath(path)
        return None

    @staticmethod
    def _normalize_origin(raw: Any, fallback: Optional[list] = None) -> list:
        base = list(fallback or [0.0, 0.0, 0.0])
        while len(base) < 3:
            base.append(0.0)

        if not isinstance(raw, (list, tuple)):
            return [float(base[0]), float(base[1]), float(base[2])]

        out = [float(base[0]), float(base[1]), float(base[2])]
        for idx in range(min(3, len(raw))):
            try:
                out[idx] = float(raw[idx])
            except (TypeError, ValueError):
                continue
        return out

    def _load_map_bundle(self, map_yaml_path: str) -> Tuple[Optional[Dict[str, Any]], str]:
        if Image is None:
            return None, 'Pillow is not available (python3-pil missing)'

        if not map_yaml_path or not os.path.exists(map_yaml_path):
            return None, f'Map YAML not found: {map_yaml_path}'

        try:
            with open(map_yaml_path, 'r', encoding='utf-8') as f:
                map_yaml = yaml.safe_load(f) or {}
        except Exception as exc:
            return None, f'Failed to read map yaml: {exc}'

        image_ref = str(map_yaml.get('image', '') or '').strip()
        if not image_ref:
            return None, 'Map YAML is missing image field'

        if os.path.isabs(image_ref):
            image_path = image_ref
        else:
            image_path = os.path.join(os.path.dirname(map_yaml_path), image_ref)

        if not os.path.exists(image_path):
            return None, f'Map image not found: {image_path}'

        try:
            image = Image.open(image_path).convert('L')
        except Exception as exc:
            return None, f'Failed to open map image: {exc}'

        bundle = {
            'yaml_path': os.path.abspath(map_yaml_path),
            'yaml_data': map_yaml,
            'image_path': os.path.abspath(image_path),
            'image': image,
            'resolution': float(map_yaml.get('resolution', 0.05)),
            'origin': self._normalize_origin(map_yaml.get('origin', [0.0, 0.0, 0.0])),
        }
        return bundle, ''

    @staticmethod
    def _image_to_png_b64(image) -> str:
        out = io.BytesIO()
        image.convert('RGB').save(out, format='PNG')
        return base64.b64encode(out.getvalue()).decode('ascii')

    def _save_stage_cache_locked(self):
        if self._stage_image is None:
            return

        os.makedirs(os.path.dirname(self.stage_meta_file) or '.', exist_ok=True)
        try:
            self._stage_image.save(self.stage_image_file)
            with open(self.stage_meta_file, 'w', encoding='utf-8') as f:
                yaml.safe_dump(
                    {
                        'map_yaml_path': self._stage_map_yaml_path,
                        'resolution': float(self._stage_resolution),
                        'origin': list(self._stage_origin),
                        'saved_at': time.time(),
                    },
                    f,
                    default_flow_style=False,
                )
        except Exception as exc:
            self.get_logger().warn(f'Failed saving editor stage cache: {exc}')

    def _load_stage_cache(self):
        if Image is None:
            return
        if not os.path.exists(self.stage_image_file) or not os.path.exists(self.stage_meta_file):
            return

        try:
            with open(self.stage_meta_file, 'r', encoding='utf-8') as f:
                meta = yaml.safe_load(f) or {}
            image = Image.open(self.stage_image_file).convert('L')
        except Exception:
            return

        with self._lock:
            self._stage_image = image
            cached_yaml = str(meta.get('map_yaml_path', '') or '').strip()
            self._stage_map_yaml_path = os.path.abspath(cached_yaml) if cached_yaml else ''
            self._stage_resolution = float(meta.get('resolution', 0.05) or 0.05)
            self._stage_origin = self._normalize_origin(meta.get('origin', [0.0, 0.0, 0.0]))

    def _select_target_map_yaml_locked(self) -> Optional[str]:
        """Pick where staged edits should be persisted."""
        active_yaml = self._resolve_active_map_yaml()
        if active_yaml:
            active_yaml = os.path.abspath(active_yaml)
            self._stage_map_yaml_path = active_yaml
            return active_yaml

        stage_yaml = str(self._stage_map_yaml_path or '').strip()
        if stage_yaml:
            stage_yaml = os.path.abspath(stage_yaml)
            if os.path.exists(stage_yaml):
                self._stage_map_yaml_path = stage_yaml
                return stage_yaml

        self._stage_map_yaml_path = ''
        return None

    def _ensure_stage_loaded_locked(self, *, force_reload: bool = False) -> Tuple[bool, str]:
        if not force_reload and self._stage_image is not None:
            active_yaml = self._resolve_active_map_yaml()
            if active_yaml:
                active_yaml = os.path.abspath(active_yaml)

            stage_yaml = str(self._stage_map_yaml_path or '').strip()
            if stage_yaml:
                stage_yaml = os.path.abspath(stage_yaml)

            if stage_yaml and not os.path.exists(stage_yaml):
                force_reload = True
            elif active_yaml and stage_yaml and stage_yaml != active_yaml:
                force_reload = True
            elif active_yaml and not stage_yaml:
                self._stage_map_yaml_path = active_yaml
                self._save_stage_cache_locked()

            if not force_reload:
                return True, 'Stage ready'

        map_yaml_path = self._resolve_active_map_yaml()
        if not map_yaml_path:
            return False, 'No active map configured'

        bundle, err = self._load_map_bundle(map_yaml_path)
        if bundle is None:
            return False, err

        self._stage_image = bundle['image']
        self._stage_origin = list(bundle['origin'])
        self._stage_resolution = float(bundle['resolution'])
        self._stage_map_yaml_path = str(bundle['yaml_path'])
        self._save_stage_cache_locked()
        return True, 'Stage loaded from active map'

    @staticmethod
    def _decode_data_url(raw: str) -> Tuple[Optional[bytes], str]:
        text = str(raw or '').strip()
        if not text:
            return None, 'image is required'

        payload = text
        if text.startswith('data:'):
            parts = text.split(',', 1)
            if len(parts) != 2:
                return None, 'Invalid image data URL'
            payload = parts[1]

        try:
            return base64.b64decode(payload), ''
        except Exception as exc:
            return None, f'Invalid base64 image payload: {exc}'

    def _payload_locked(self):
        if self._stage_image is None:
            return None
        return {
            'image': self._image_to_png_b64(self._stage_image),
            'width': int(self._stage_image.width),
            'height': int(self._stage_image.height),
            'resolution': float(self._stage_resolution),
            'origin': [float(v) for v in self._stage_origin],
            'map_yaml_path': str(self._stage_map_yaml_path),
        }

    def _write_yaml(self, path: str, data: dict):
        tmp = f'{path}.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            yaml.safe_dump(data, f, default_flow_style=False)
        os.replace(tmp, path)

    def preview_callback(self, _request, response):
        with self._lock:
            ok, msg = self._ensure_stage_loaded_locked(force_reload=False)
            if not ok:
                response.ok = False
                response.message = msg
                response.image = ''
                response.width = 0
                response.height = 0
                response.resolution = 0.0
                response.origin = []
                response.map_yaml_path = ''
                return response

            payload = self._payload_locked()
            response.ok = True
            response.message = 'Editor preview ready'
            response.image = payload['image']
            response.width = payload['width']
            response.height = payload['height']
            response.resolution = float(payload['resolution'])
            response.origin = [float(v) for v in payload['origin']]
            response.map_yaml_path = str(payload['map_yaml_path'])
            return response

    def reload_callback(self, _request, response):
        with self._lock:
            ok, msg = self._ensure_stage_loaded_locked(force_reload=True)
            if not ok:
                response.ok = False
                response.message = msg
                response.image = ''
                response.width = 0
                response.height = 0
                response.resolution = 0.0
                response.origin = []
                response.map_yaml_path = ''
                return response

            payload = self._payload_locked()
            response.ok = True
            response.message = 'Editor map reloaded from active map'
            response.image = payload['image']
            response.width = payload['width']
            response.height = payload['height']
            response.resolution = float(payload['resolution'])
            response.origin = [float(v) for v in payload['origin']]
            response.map_yaml_path = str(payload['map_yaml_path'])
            return response

    def overwrite_callback(self, request, response):
        if Image is None:
            response.ok = False
            response.message = 'Pillow is not available (python3-pil missing)'
            return response

        image_bytes, err = self._decode_data_url(request.image)
        if err:
            response.ok = False
            response.message = err
            return response

        try:
            image = Image.open(io.BytesIO(image_bytes)).convert('L')
        except Exception as exc:
            response.ok = False
            response.message = f'Failed to decode image: {exc}'
            return response

        with self._lock:
            ok, msg = self._ensure_stage_loaded_locked(force_reload=False)
            if not ok:
                response.ok = False
                response.message = msg
                return response

            self._stage_image = image
            self._stage_origin = self._normalize_origin(list(request.origin), fallback=self._stage_origin)
            self._save_stage_cache_locked()

            response.ok = True
            response.message = 'Editor map staging updated'
            return response

    @staticmethod
    def _resolve_image_path(map_yaml_path: str, image_ref: str) -> str:
        image_ref = str(image_ref or '').strip()
        if os.path.isabs(image_ref):
            return image_ref
        return os.path.join(os.path.dirname(map_yaml_path), image_ref)

    def save_current_callback(self, _request, response):
        if Image is None:
            response.ok = False
            response.message = 'Pillow is not available (python3-pil missing)'
            response.map_yaml_path = ''
            response.map_image_path = ''
            return response

        with self._lock:
            ok, msg = self._ensure_stage_loaded_locked(force_reload=False)
            if not ok:
                response.ok = False
                response.message = msg
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            map_yaml_path = self._select_target_map_yaml_locked()
            if not map_yaml_path:
                response.ok = False
                response.message = 'No active map configured'
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            try:
                with open(map_yaml_path, 'r', encoding='utf-8') as f:
                    map_yaml = yaml.safe_load(f) or {}
            except Exception as exc:
                response.ok = False
                response.message = f'Failed to read map yaml: {exc}'
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            if not str(map_yaml.get('image', '') or '').strip():
                response.ok = False
                response.message = 'Map yaml missing image field'
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            try:
                save_plan = build_dotted_truth_plan(map_yaml_path, map_yaml)
            except Exception as exc:
                response.ok = False
                response.message = f'Failed to prepare dotted truth save plan: {exc}'
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            source_image_path = save_plan['source_image_path']
            try:
                os.makedirs(os.path.dirname(source_image_path) or '.', exist_ok=True)
                self._stage_image.save(source_image_path)
                map_yaml['origin'] = [float(v) for v in self._stage_origin]
                map_yaml['resolution'] = float(self._stage_resolution)
                self._write_yaml(map_yaml_path, map_yaml)
            except Exception as exc:
                response.ok = False
                response.message = f'Failed to save map edits: {exc}'
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            dotted_result = self._ensure_dotted_truth(map_yaml_path, context='save current editor map')
            if not bool(dotted_result.get('ok')):
                response.ok = False
                response.message = str(dotted_result.get('message', 'Failed to prepare dotted truth map'))
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            self._stage_map_yaml_path = os.path.abspath(map_yaml_path)
            self._save_stage_cache_locked()

            response.ok = True
            response.message = 'Map edits saved to active map'
            response.map_yaml_path = os.path.abspath(map_yaml_path)
            response.map_image_path = os.path.abspath(
                str(dotted_result.get('dotted_image_path') or save_plan['dotted_image_path'])
            )
            return response

    @staticmethod
    def _sanitize_map_name(raw: str) -> str:
        text = str(raw or '').strip()
        if not text:
            text = datetime.now().strftime('edited_map_%Y%m%d_%H%M%S')
        clean = re.sub(r'[^A-Za-z0-9_\-]', '_', text)
        return clean[:80]

    def export_callback(self, request, response):
        if Image is None:
            response.ok = False
            response.message = 'Pillow is not available (python3-pil missing)'
            response.map_yaml_path = ''
            response.map_image_path = ''
            return response

        with self._lock:
            ok, msg = self._ensure_stage_loaded_locked(force_reload=False)
            if not ok:
                response.ok = False
                response.message = msg
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            source_yaml = self._select_target_map_yaml_locked()
            if not source_yaml:
                response.ok = False
                response.message = 'No active map configured'
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            try:
                with open(source_yaml, 'r', encoding='utf-8') as f:
                    src_yaml = yaml.safe_load(f) or {}
            except Exception:
                src_yaml = {}

            map_name = self._sanitize_map_name(request.map_name)
            yaml_path = os.path.join(self.maps_dir, f'{map_name}.yaml')

            yaml_data = {
                'image': f'{map_name}.pgm',
                'resolution': float(self._stage_resolution),
                'origin': [float(v) for v in self._stage_origin],
                'negate': int(src_yaml.get('negate', 0) or 0),
                'occupied_thresh': float(src_yaml.get('occupied_thresh', 0.65) or 0.65),
                'free_thresh': float(src_yaml.get('free_thresh', 0.196) or 0.196),
            }

            try:
                save_plan = build_dotted_truth_plan(yaml_path, yaml_data)
                os.makedirs(os.path.dirname(save_plan['source_image_path']) or '.', exist_ok=True)
                self._stage_image.save(save_plan['source_image_path'])
                self._write_yaml(yaml_path, yaml_data)
            except Exception as exc:
                response.ok = False
                response.message = f'Failed to export edited map: {exc}'
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            dotted_result = self._ensure_dotted_truth(yaml_path, context=f'export editor map {map_name}')
            if not bool(dotted_result.get('ok')):
                response.ok = False
                response.message = str(dotted_result.get('message', 'Failed to prepare dotted truth map'))
                response.map_yaml_path = ''
                response.map_image_path = ''
                return response

            response.ok = True
            response.message = f'Edited map exported as {map_name}'
            response.map_yaml_path = os.path.abspath(yaml_path)
            response.map_image_path = os.path.abspath(
                str(dotted_result.get('dotted_image_path') or save_plan['dotted_image_path'])
            )
            return response


def main(args=None):
    rclpy.init(args=args)
    node = MapEditorManager()
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
