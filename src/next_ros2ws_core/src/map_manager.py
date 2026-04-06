#!/usr/bin/env python3
"""
Map Manager Service
Centralized service for managing maps across the entire system.
- Upload new maps (PGM + YAML)
- Switch active map dynamically
- Persist the selected active map for the next Nav2 restart
"""

import rclpy
from rclpy.node import Node
from next_ros2ws_interfaces.srv import UploadMap, SetActiveMap, GetActiveMap
import os
import yaml
import base64
import shutil
from ament_index_python.packages import get_package_share_directory, PackageNotFoundError

from next_ros2ws_core.map_dotted_truth import (
    DOTTED_TRUTH_META_KEY,
    ensure_dotted_truth_map,
)


def _looks_like_workspace_root(path):
    return (
        os.path.isdir(os.path.join(path, 'maps')) and
        os.path.isdir(os.path.join(path, 'config'))
    )


def _prefer_canonical_workspace_root(initial_root):
    root = os.path.abspath(os.path.expanduser(initial_root or os.getcwd()))
    marker = f'{os.sep}install{os.sep}'

    candidates = []

    # Prefer source workspace over installed share if both are available.
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


def _resolve_workspace_root():
    env_root = os.getenv('NEXT_UI_ROOT')
    if env_root:
        return _prefer_canonical_workspace_root(env_root)

    try:
        share_root = get_package_share_directory('ugv_bringup')
        return _prefer_canonical_workspace_root(share_root)
    except PackageNotFoundError:
        pass

    cur = os.path.abspath(os.path.dirname(__file__))
    for _ in range(8):
        if _looks_like_workspace_root(cur):
            return _prefer_canonical_workspace_root(cur)
        cur = os.path.dirname(cur)

    return _prefer_canonical_workspace_root(os.getcwd())


class MapManager(Node):
    def __init__(self):
        super().__init__('map_manager')
        
        # Base paths
        self.workspace_root = _resolve_workspace_root()
        self.maps_dir = os.path.join(self.workspace_root, 'maps')
        self.config_dir = os.path.join(self.workspace_root, 'config')
        os.makedirs(self.maps_dir, exist_ok=True)
        os.makedirs(self.config_dir, exist_ok=True)

        # MapManager writes to one canonical maps dir only.
        self.alternate_maps_dirs = self._discover_alternate_maps_dirs()
        self._import_maps_from_alternate_dirs()

        # Active map configuration file
        self.active_map_config = os.path.join(self.workspace_root, 'config', 'active_map_config.yaml')

        # Load or initialize active map config
        self.active_map_path = self._load_active_map_config()
        self.active_map_path = self._materialize_map_into_canonical_dir(self.active_map_path)
        self._ensure_dotted_truth(self.active_map_path, fail_hard=False, context='startup')
        self._save_active_map_config(self.active_map_path)
        
        # Services
        self.upload_map_srv = self.create_service(
            UploadMap,
            '/map_manager/upload_map',
            self.upload_map_callback
        )
        
        self.set_active_map_srv = self.create_service(
            SetActiveMap,
            '/map_manager/set_active_map',
            self.set_active_map_callback
        )
        
        self.get_active_map_srv = self.create_service(
            GetActiveMap,
            '/map_manager/get_active_map',
            self.get_active_map_callback
        )
        
        self.get_logger().info('Map Manager Service started')
        self.get_logger().info(f'Maps directory: {self.maps_dir}')
        self.get_logger().info(f'Active map: {self.active_map_path}')

    @staticmethod
    def _extra_image_refs(meta):
        refs = []
        if not isinstance(meta, dict):
            return refs
        dotted_meta = meta.get(DOTTED_TRUTH_META_KEY)
        dotted_meta = dotted_meta if isinstance(dotted_meta, dict) else {}
        source_ref = str(dotted_meta.get('source_image', '') or '').strip()
        dotted_ref = str(dotted_meta.get('image', '') or '').strip()
        if source_ref:
            refs.append((DOTTED_TRUTH_META_KEY, 'source_image', source_ref))
        if dotted_ref:
            refs.append((DOTTED_TRUTH_META_KEY, 'image', dotted_ref))
        return refs

    def _copy_image_ref_into_canonical_dir(self, source_yaml_path, ref):
        image_ref = str(ref or '').strip()
        if not image_ref:
            return None

        source_path = image_ref if os.path.isabs(image_ref) else os.path.join(os.path.dirname(source_yaml_path), image_ref)
        if not os.path.exists(source_path):
            return None

        dest_path = os.path.join(self.maps_dir, os.path.basename(source_path))
        if not os.path.exists(dest_path):
            shutil.copy2(source_path, dest_path)
        return os.path.basename(dest_path)

    def _ensure_dotted_truth(self, map_yaml_path, *, fail_hard, context):
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
            return True, str(result.get('message', 'Dotted truth map ready'))

        message = str(result.get('message', 'Failed to prepare dotted truth map'))
        log_method = self.get_logger().error if fail_hard else self.get_logger().warn
        log_method(f'{context}: {message}')
        return False, message
    
    def _discover_alternate_maps_dirs(self):
        """Find non-canonical map folders used by old layouts/build artifacts."""
        candidate_dirs = []

        try:
            share_root = os.path.abspath(get_package_share_directory('ugv_bringup'))
            candidate_dirs.append(os.path.join(share_root, 'maps'))
        except PackageNotFoundError:
            pass

        marker_src = f'{os.sep}src{os.sep}ui_ws'
        marker_install = f'{os.sep}install{os.sep}'

        if marker_src in self.workspace_root:
            repo_root = self.workspace_root.split(marker_src, 1)[0]
            candidate_dirs.append(os.path.join(repo_root, 'install', 'ugv_bringup', 'share', 'ugv_bringup', 'maps'))
        if marker_install in self.workspace_root:
            repo_root = self.workspace_root.split(marker_install, 1)[0]
            candidate_dirs.append(os.path.join(repo_root, 'src', 'ui_ws', 'maps'))

        unique_existing = []
        seen = set()
        canonical = os.path.abspath(self.maps_dir)

        for folder in candidate_dirs:
            if not folder:
                continue
            folder_abs = os.path.abspath(folder)
            if folder_abs == canonical:
                continue
            if folder_abs in seen:
                continue
            seen.add(folder_abs)
            if os.path.isdir(folder_abs):
                unique_existing.append(folder_abs)

        return unique_existing

    def _import_maps_from_alternate_dirs(self):
        """Copy existing maps from alternate dirs into canonical dir if missing."""
        imported = 0

        for alt_dir in self.alternate_maps_dirs:
            for name in os.listdir(alt_dir):
                if not name.lower().endswith('.yaml'):
                    continue

                src_yaml = os.path.join(alt_dir, name)
                dst_yaml = os.path.join(self.maps_dir, name)

                try:
                    if not os.path.exists(dst_yaml):
                        shutil.copy2(src_yaml, dst_yaml)
                        imported += 1

                    with open(src_yaml, 'r') as f:
                        meta = yaml.safe_load(f) or {}
                    if not isinstance(meta, dict):
                        continue

                    img = str(meta.get('image', '') or '').strip()
                    if not img:
                        continue

                    copied_name = self._copy_image_ref_into_canonical_dir(src_yaml, img)
                    if copied_name:
                        meta['image'] = copied_name

                    for meta_key, field_name, extra_ref in self._extra_image_refs(meta):
                        copied_name = self._copy_image_ref_into_canonical_dir(src_yaml, extra_ref)
                        if copied_name:
                            dotted_meta = meta.get(meta_key)
                            if isinstance(dotted_meta, dict):
                                dotted_meta[field_name] = copied_name

                    with open(dst_yaml, 'w') as f:
                        yaml.dump(meta, f, default_flow_style=False)
                except Exception as exc:
                    self.get_logger().warn(f'Failed importing map from {src_yaml}: {exc}')

        if imported > 0:
            self.get_logger().info(
                f'Imported {imported} map YAML files into canonical maps dir: {self.maps_dir}'
            )

    def _materialize_map_into_canonical_dir(self, map_yaml_path):
        """Ensure map YAML (and image) live in canonical maps dir; return canonical path."""
        if not map_yaml_path:
            return os.path.join(self.maps_dir, 'smr_map.yaml')

        src_yaml = os.path.abspath(self._normalize_map_path(map_yaml_path))
        canonical_prefix = os.path.abspath(self.maps_dir) + os.sep
        if src_yaml.startswith(canonical_prefix):
            return src_yaml

        if not os.path.exists(src_yaml):
            return src_yaml

        dst_yaml = os.path.join(self.maps_dir, os.path.basename(src_yaml))

        try:
            with open(src_yaml, 'r') as f:
                meta = yaml.safe_load(f) or {}
            if not isinstance(meta, dict):
                meta = {}

            img = str(meta.get('image', '') or '').strip()
            if img:
                copied_name = self._copy_image_ref_into_canonical_dir(src_yaml, img)
                if copied_name:
                    meta['image'] = copied_name

            for meta_key, field_name, extra_ref in self._extra_image_refs(meta):
                copied_name = self._copy_image_ref_into_canonical_dir(src_yaml, extra_ref)
                if copied_name:
                    dotted_meta = meta.get(meta_key)
                    if isinstance(dotted_meta, dict):
                        dotted_meta[field_name] = copied_name

            with open(dst_yaml, 'w') as f:
                yaml.dump(meta, f, default_flow_style=False)

            self.get_logger().info(f'Materialized map into canonical dir: {dst_yaml}')
            return dst_yaml
        except Exception as exc:
            self.get_logger().warn(f'Failed to materialize map into canonical dir: {exc}')
            return src_yaml

    def _load_active_map_config(self):
        """Load the active map configuration"""
        default_map = os.path.join(self.maps_dir, 'smr_map.yaml')
        if os.path.exists(self.active_map_config):
            try:
                with open(self.active_map_config, 'r') as f:
                    config = yaml.safe_load(f) or {}
                requested = self._normalize_map_path(config.get('active_map', default_map))
                resolved = self._resolve_existing_map_path(requested)
                return resolved or requested
            except Exception as e:
                self.get_logger().error(f'Failed to load active map config: {e}')

        # Default map
        return default_map

    def _normalize_map_path(self, map_path):
        if not map_path:
            return os.path.join(self.maps_dir, 'smr_map.yaml')
        if os.path.isabs(map_path):
            return map_path
        return os.path.join(self.maps_dir, map_path)

    def _relativize_map_path(self, map_path):
        map_path = self._normalize_map_path(map_path)
        try:
            rel = os.path.relpath(map_path, self.maps_dir)
            if not rel.startswith('..'):
                return rel
        except Exception:
            pass
        return map_path

    def _resolve_existing_map_path(self, requested_map_path):
        """Resolve map YAML path from absolute/relative/basename inputs."""
        raw = (requested_map_path or '').strip()
        if not raw:
            return None

        candidates = []

        # As-is
        candidates.append(raw)
        # Normalized relative-to-maps behavior
        candidates.append(self._normalize_map_path(raw))

        # If extension is missing, try adding .yaml
        if not raw.lower().endswith('.yaml'):
            candidates.append(raw + '.yaml')
            candidates.append(self._normalize_map_path(raw + '.yaml'))

        # Try basename in known map folders
        base = os.path.basename(raw)
        if base:
            if not base.lower().endswith('.yaml'):
                base = base + '.yaml'
            candidates.append(os.path.join(self.maps_dir, base))
            for alt_dir in self.alternate_maps_dirs:
                candidates.append(os.path.join(alt_dir, base))
            candidates.append(os.path.join(os.path.expanduser('~'), 'Downloads', 'DownloadedMaps', base))

        for candidate in candidates:
            if candidate and os.path.exists(candidate):
                return os.path.abspath(candidate)

        return None
    
    def _save_active_map_config(self, map_path):
        """Save the active map configuration"""
        try:
            map_path = self._normalize_map_path(map_path)
            config = {'active_map': self._relativize_map_path(map_path)}
            with open(self.active_map_config, 'w') as f:
                yaml.dump(config, f, default_flow_style=False)
            self.active_map_path = map_path
            self.get_logger().info(f'Active map config saved: {map_path}')
            return True
        except Exception as e:
            self.get_logger().error(f'Failed to save active map config: {e}')
            return False
    
    def upload_map_callback(self, request, response):
        """Handle map upload request"""
        try:
            map_name = request.map_name
            
            # Validate map name
            if not map_name or '/' in map_name or '\\' in map_name:
                response.success = False
                response.message = 'Invalid map name'
                return response
            
            # Decode and save PGM file
            pgm_path = os.path.join(self.maps_dir, f'{map_name}.pgm')
            try:
                os.makedirs(self.maps_dir, exist_ok=True)
                pgm_data = base64.b64decode(request.pgm_base64)
                with open(pgm_path, 'wb') as f:
                    f.write(pgm_data)
                self.get_logger().info(f'Saved PGM: {pgm_path}')
            except Exception as e:
                response.success = False
                response.message = f'Failed to save PGM file: {e}'
                return response
            
            # Parse and save YAML file
            yaml_path = os.path.join(self.maps_dir, f'{map_name}.yaml')
            try:
                # Parse YAML content
                yaml_data = yaml.safe_load(request.yaml_content)
                
                # Ensure image path is relative and correct
                yaml_data['image'] = f'{map_name}.pgm'
                
                with open(yaml_path, 'w') as f:
                    yaml.dump(yaml_data, f, default_flow_style=False)
                self.get_logger().info(f'Saved YAML: {yaml_path}')
            except Exception as e:
                response.success = False
                response.message = f'Failed to save YAML file: {e}'
                # Cleanup PGM if YAML failed
                if os.path.exists(pgm_path):
                    os.remove(pgm_path)
                return response

            ok, dotted_msg = self._ensure_dotted_truth(yaml_path, fail_hard=True, context=f'upload map {map_name}')
            if not ok:
                response.success = False
                response.message = dotted_msg
                response.map_path = yaml_path
                return response
            
            response.success = True
            response.message = f'Map "{map_name}" uploaded successfully'
            response.map_path = yaml_path
            
            self.get_logger().info(f'✓ Map uploaded: {map_name}')
            
            return response
            
        except Exception as e:
            response.success = False
            response.message = f'Upload failed: {e}'
            self.get_logger().error(response.message)
            return response
    
    def set_active_map_callback(self, request, response):
        """Switch the active map and update all configurations"""
        try:
            requested_map_path = request.map_yaml_path
            new_map_path = self._resolve_existing_map_path(requested_map_path)

            # Validate map exists
            if not new_map_path:
                response.success = False
                response.message = f'Map file not found for request: {requested_map_path}'
                return response

            # Update active map config
            new_map_path = self._materialize_map_into_canonical_dir(new_map_path)
            ok, dotted_msg = self._ensure_dotted_truth(new_map_path, fail_hard=True, context=f'set active map {requested_map_path}')
            if not ok:
                response.success = False
                response.message = dotted_msg
                return response
            if not self._save_active_map_config(new_map_path):
                response.success = False
                response.message = 'Failed to save active map configuration'
                return response
            
            response.success = True
            response.message = (
                f'Active map switched to {os.path.basename(new_map_path)}. '
                'Restart Nav2 to apply changes.'
            )
            
            self.get_logger().info(f'✓ Active map set to: {new_map_path}')
            self.get_logger().warn('⚠️ You must restart Nav2 for changes to take effect')
            
            return response
            
        except Exception as e:
            response.success = False
            response.message = f'Failed to set active map: {e}'
            self.get_logger().error(response.message)
            return response
    
    def get_active_map_callback(self, request, response):
        """Get the currently active map path"""
        try:
            response.success = True
            response.map_yaml_path = self.active_map_path
            response.message = f'Active map: {os.path.basename(self.active_map_path)}'
            return response
        except Exception as e:
            response.success = False
            response.message = f'Failed to get active map: {e}'
            return response


def main(args=None):
    rclpy.init(args=args)
    map_manager = MapManager()
    
    try:
        rclpy.spin(map_manager)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            map_manager.destroy_node()
        except:
            pass
        try:
            rclpy.shutdown()
        except:
            pass  # Already shut down


if __name__ == '__main__':
    main()
