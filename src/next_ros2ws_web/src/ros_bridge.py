#!/usr/bin/env python3

import base64
import copy
import concurrent.futures
import hashlib
import io
import json
import importlib
import math
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import threading
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from ament_index_python.packages import get_package_share_directory, PackageNotFoundError
from geometry_msgs.msg import PoseStamped, PoseWithCovarianceStamped, Twist
from nav_msgs.msg import OccupancyGrid, Path as NavPath, Odometry
from nav2_msgs.srv import ClearEntireCostmap, LoadMap
from next_ros2ws_core.map_dotted_truth import ensure_dotted_truth_map
from next_ros2ws_core.topic_catalog import (
    default_topics as catalog_default_topics,
    extract_topic_overrides,
    merge_topic_overrides,
    sanitize_topic_overrides,
)
from next_ros2ws_interfaces.action import GoToZone as GoToZoneAction, FollowPath as FollowPathAction
from next_ros2ws_interfaces.srv import (
    AddMapLayerObject,
    ClearMapLayer,
    ClearMissionState,
    DeleteLayout,
    DeleteMapLayerObject,
    DeletePath,
    DeleteZone,
    ExportEditorMap,
    GetActiveMap,
    GetEditorMapPreview,
    GetLayouts,
    GetMapLayers,
    GetMissionStatus,
    GetPaths,
    GetSequenceStatus,
    GetUiMappings,
    GetZones,
    LoadLayout,
    OverwriteEditorMap,
    ReloadEditorMap,
    ReorderZones,
    ResumeMission,
    SaveCurrentMap,
    SaveLayout,
    SavePath,
    SaveUiMappings,
    SaveZone,
    SetActiveMap,
    SetControlMode,
    SetStackMode,
    StartSequence,
    StopSequence,
    UpdateZoneParams,
    UploadMap,
)
from action_msgs.msg import GoalStatus
from rclpy.action import ActionClient
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.duration import Duration as RosDuration
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from rclpy.time import Time as RosTime
from rcl_interfaces.msg import Parameter, ParameterType, ParameterValue
from rcl_interfaces.srv import GetParameters, SetParameters
from sensor_msgs.msg import LaserScan, PointCloud2
from sensor_msgs_py import point_cloud2
from slam_toolbox.srv import SaveMap as SlamSaveMap, SerializePoseGraph as SlamSerializePoseGraph, DeserializePoseGraph as SlamDeserializePoseGraph
from std_msgs.msg import Bool as BoolMsg, String
from std_srvs.srv import SetBool, Trigger
from tf2_msgs.msg import TFMessage
from tf2_ros import Buffer, TransformException, TransformListener

import xacro

try:
    from PIL import Image
except Exception:
    Image = None

try:
    _action_registry = importlib.import_module('next_ros2ws_core.action_registry')
except Exception:
    _action_registry = None

try:
    _db_manager_module = importlib.import_module('next_ros2ws_core.db_manager')
    _DatabaseManager = getattr(_db_manager_module, 'DatabaseManager', None)
except Exception:
    _DatabaseManager = None


def default_action_mappings() -> Dict[str, Dict[str, str]]:
    if _action_registry and hasattr(_action_registry, 'default_action_mappings'):
        try:
            return dict(_action_registry.default_action_mappings())
        except Exception:
            return {}
    return {}


def merge_action_mappings(mappings: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    if _action_registry and hasattr(_action_registry, 'merge_action_mappings'):
        try:
            return dict(_action_registry.merge_action_mappings(mappings))
        except Exception:
            return mappings
    return mappings


_BRINGUP_ENV_VARS = ('NEXT_BRINGUP_PACKAGE', 'NEXT_ROBOT_PACKAGE')
_DEFAULT_BRINGUP_PACKAGE = 'ugv_bringup'


def _package_name_from_xml(package_xml_path: str) -> str:
    try:
        root = ET.parse(package_xml_path).getroot()
    except Exception:
        return ''
    name_node = root.find('name')
    if name_node is None:
        return ''
    return str(name_node.text or '').strip()


def _candidate_bringup_packages(search_roots: Optional[List[str]] = None):
    for env_name in _BRINGUP_ENV_VARS:
        value = str(os.getenv(env_name, '') or '').strip()
        if value:
            yield value

    roots: List[str] = []
    for root in (search_roots or []):
        if root:
            roots.append(os.path.abspath(os.path.expanduser(str(root))))

    cur = os.path.abspath(os.path.dirname(__file__))
    for _ in range(10):
        roots.append(cur)
        cur = os.path.dirname(cur)

    seen_roots: Set[str] = set()
    for root in roots:
        if not root or root in seen_roots:
            continue
        seen_roots.add(root)
        for package_xml in (
            os.path.join(root, 'package.xml'),
            os.path.join(root, 'src', 'ui_ws', 'package.xml'),
        ):
            package_name = _package_name_from_xml(package_xml)
            if package_name:
                yield package_name

    yield _DEFAULT_BRINGUP_PACKAGE


def _resolve_bringup_package_share(search_roots: Optional[List[str]] = None) -> Tuple[str, str]:
    seen: Set[str] = set()
    for package_name in _candidate_bringup_packages(search_roots):
        if not package_name or package_name in seen:
            continue
        seen.add(package_name)
        try:
            return package_name, get_package_share_directory(package_name)
        except PackageNotFoundError:
            continue
        except Exception:
            continue
    return _DEFAULT_BRINGUP_PACKAGE, ''


def _looks_like_ui_root(path: str) -> bool:
    candidate = os.path.abspath(os.path.expanduser(path or os.getcwd()))
    return (
        os.path.isdir(os.path.join(candidate, 'maps'))
        and os.path.isdir(os.path.join(candidate, 'config'))
    )


def _prefer_canonical_ui_root(initial_root: str) -> str:
    root = os.path.abspath(os.path.expanduser(initial_root or os.getcwd()))
    marker = f'{os.sep}install{os.sep}'

    candidates: List[str] = []
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

    seen: Set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        resolved = os.path.abspath(candidate)
        if resolved in seen:
            continue
        seen.add(resolved)
        if _looks_like_ui_root(resolved):
            return resolved

    return root


def _canonicalize_map_yaml_path(raw_path: str, ui_root: str) -> str:
    candidate = str(raw_path or '').strip()
    if not candidate:
        return ''

    expanded = os.path.abspath(os.path.expanduser(candidate))
    base_name = os.path.basename(expanded)
    if not base_name:
        return expanded

    candidates: List[str] = []
    marker = f'{os.sep}install{os.sep}'
    if marker in expanded:
        repo_root = expanded.split(marker, 1)[0]
        candidates.append(os.path.join(repo_root, 'src', 'ui_ws', 'maps', base_name))

    if ui_root:
        candidates.append(os.path.join(ui_root, 'maps', base_name))

    candidates.append(expanded)

    seen: Set[str] = set()
    for path in candidates:
        resolved = os.path.abspath(os.path.expanduser(path))
        if resolved in seen:
            continue
        seen.add(resolved)
        if os.path.exists(resolved):
            return resolved

    return expanded


def _resolve_ui_root() -> str:
    env_root = os.getenv('NEXT_UI_ROOT')
    if env_root:
        return _prefer_canonical_ui_root(env_root)

    _package_name, share_dir = _resolve_bringup_package_share(search_roots=[os.getcwd()])
    if share_dir:
        candidate = _prefer_canonical_ui_root(share_dir)
        if _looks_like_ui_root(candidate):
            return candidate

    cur = os.path.abspath(os.path.dirname(__file__))
    for _ in range(8):
        if _looks_like_ui_root(cur):
            return _prefer_canonical_ui_root(cur)
        cur = os.path.dirname(cur)

    return _prefer_canonical_ui_root(os.getcwd())


class RosBridge(Node):
    def _send_followpath_goal(self, goal_msg):
            # Guard before sending
            with self._nav_goal_lock:
                if getattr(self, "_follow_goal_inflight", False):
                    return False
                self._path_goal_seq += 1
                goal_seq = int(self._path_goal_seq)
                self._follow_goal_inflight = True
                self._active_goal_handle = None
                self._path_goal_handle = None
                self._follow_cancel_requested = False

            future = self.follow_path_action_client.send_goal_async(
                goal_msg,
                feedback_callback=self._follow_path_feedback
            )
            future.add_done_callback(lambda fut, _seq=goal_seq: self._on_follow_goal_response(fut, _seq))
            return True

    def _reset_loop_runtime(self):
            """Clear runtime loop state after terminal loop outcomes."""
            self._loop_enabled = False
            self._loop_type = 'none'
            self._loop_stop_requested = False
            self._loop_restart_inflight = False
            self._loop_path_points = []
            self._loop_display_waypoint_count = 0
            self._loop_current_index = 0
            self._loop_target_index = 1
            self._loop_direction = 1
            self._loop_retry_fail_streak = 0
            self._pending_loop_restart = False

    def _set_follow_path_failure_state(self, message: str, *, stop_loop: bool):
            if stop_loop:
                self._reset_loop_runtime()
                self.path_state['loop_mode'] = False
                self.path_state['loop_type'] = 'none'
            self.path_state['active'] = False
            self.path_state['success'] = False
            self.path_state['message'] = str(message or 'Path following failed')

    @staticmethod
    def _goal_status_is_terminal(status: Any) -> bool:
            try:
                status_value = int(status)
            except Exception:
                return False
            return status_value in {
                GoalStatus.STATUS_SUCCEEDED,
                GoalStatus.STATUS_ABORTED,
                GoalStatus.STATUS_CANCELED,
            }

    def _goal_handle_can_cancel(self, goal_handle) -> bool:
            if goal_handle is None:
                return False
            if not bool(getattr(goal_handle, 'accepted', True)):
                return False
            status = getattr(goal_handle, 'status', None)
            if status is None:
                return True
            return not self._goal_status_is_terminal(status)

    def _cancel_client_goal(self, goal_handle, *, timeout_sec: float, label: str):
            if goal_handle is None:
                return True, f'No active {label}'
            if not self._goal_handle_can_cancel(goal_handle):
                return True, f'{label} already finished'
            try:
                cancel_future = goal_handle.cancel_goal_async()
                if not self._wait_future(cancel_future, timeout_sec):
                    return False, f'Timeout cancelling {label}'
                try:
                    cancel_future.result()
                except Exception:
                    pass
            except Exception as exc:
                return False, f'Failed cancelling {label}: {exc}'
            return True, f'{label} cancel requested'

    def _on_follow_goal_response(self, future, goal_seq: int):
            try:
                goal_handle = future.result()
            except Exception as e:
                with self._nav_goal_lock:
                    if goal_seq != int(self._path_goal_seq):
                        return
                    self._follow_goal_inflight = False
                    self._active_goal_handle = None
                    self._path_goal_handle = None
                    self._follow_cancel_requested = False
                self.get_logger().error(f"FollowPath goal response exception: {e}")
                self._set_follow_path_failure_state(
                    f'FollowPath goal response exception: {e}',
                    stop_loop=bool(self._loop_enabled and not self._loop_stop_requested),
                )
                return

            if not goal_handle.accepted:
                with self._nav_goal_lock:
                    if goal_seq != int(self._path_goal_seq):
                        return
                    self._follow_goal_inflight = False
                    self._active_goal_handle = None
                    self._path_goal_handle = None
                    self._follow_cancel_requested = False
                self.get_logger().warn("FollowPath goal REJECTED")
                reject_message = 'FollowPath goal rejected'
                if bool(getattr(self, 'estop_active', False)):
                    reject_message = 'FollowPath goal rejected while E-STOP active'
                self._set_follow_path_failure_state(
                    reject_message,
                    stop_loop=bool(self._loop_enabled and not self._loop_stop_requested),
                )
                return

            auto_cancel = False
            with self._nav_goal_lock:
                if goal_seq != int(self._path_goal_seq):
                    auto_cancel = True
                else:
                    self._active_goal_handle = goal_handle
                    self._path_goal_handle = goal_handle
                    auto_cancel = bool(self._follow_cancel_requested or self.estop_active)

            result_future = goal_handle.get_result_async()
            result_future.add_done_callback(lambda fut, _seq=goal_seq: self._on_follow_result(fut, _seq))

            if auto_cancel:
                cancel_future = goal_handle.cancel_goal_async()
                if bool(getattr(self, "_pending_loop_restart", False)):
                    cancel_future.add_done_callback(
                        lambda fut, _seq=goal_seq: self._on_cancel_done(fut, _seq)
                    )

    def _on_follow_result(self, future, goal_seq: int):
            with self._nav_goal_lock:
                if goal_seq != int(self._path_goal_seq):
                    return
                # Always clear inflight first
                self._follow_goal_inflight = False
                self._active_goal_handle = None
                self._path_goal_handle = None
                self._follow_cancel_requested = False

            try:
                res = future.result()
            except Exception as e:
                self.get_logger().error(f"FollowPath result exception: {e}")
                self._set_follow_path_failure_state(
                    f'FollowPath result exception: {e}',
                    stop_loop=bool(self._loop_enabled and not self._loop_stop_requested),
                )
                return

            status = getattr(res, 'status', None)
            succeeded = (status == GoalStatus.STATUS_SUCCEEDED)
            result = getattr(res, 'result', None)
            result_message = ''
            if result is not None:
                result_message = str(getattr(result, 'message', '')).strip()
            self.get_logger().info(f"FollowPath result status = {status}")

            if self._loop_enabled and not self._loop_stop_requested:
                if getattr(self, 'estop_active', False):
                    self._set_follow_path_failure_state('Loop stopped: E-STOP active', stop_loop=True)
                    return
                if succeeded:
                    self._loop_retry_fail_streak = 0
                    # Advance pointer BEFORE dispatching the next segment.
                    self._loop_current_index = int(self._loop_target_index)
                    self._start_next_loop_segment(restart=False, retry_same_target=False)
                    return

                failure_message = result_message or (
                    f'Loop stopped: segment {self._loop_current_index + 1}->{self._loop_target_index + 1} failed'
                )
                self._set_follow_path_failure_state(failure_message, stop_loop=True)
                return

            # Normal (non-loop) path completion
            if result is not None:
                success = bool(getattr(result, 'success', succeeded))
                message = result_message
            else:
                success = succeeded
                message = ''
            if not message:
                message = 'Path following complete' if success else f'Path following failed (status={status})'
            self.path_state['active'] = False
            self.path_state['success'] = success
            self.path_state['message'] = message

    def _request_loop_restart(self):
            # set flags so loop will restart AFTER cancel completes
            self._pending_loop_restart = True

            with self._nav_goal_lock:
                gh = getattr(self, "_active_goal_handle", None)
                goal_seq = int(self._path_goal_seq)
                inflight = bool(getattr(self, "_follow_goal_inflight", False))
                if gh is None and inflight:
                    self._follow_cancel_requested = True
                    return

            if gh is not None and inflight and self._goal_handle_can_cancel(gh):
                cancel_future = gh.cancel_goal_async()
                cancel_future.add_done_callback(lambda fut, _seq=goal_seq: self._on_cancel_done(fut, _seq))
            else:
                # no inflight: restart immediately
                self._start_next_loop_segment(restart=True, retry_same_target=False)

    def _on_cancel_done(self, future, goal_seq: int):
            with self._nav_goal_lock:
                if goal_seq != int(self._path_goal_seq):
                    return
                # cancel finished, allow new dispatch
                self._follow_goal_inflight = False
                self._active_goal_handle = None
                self._path_goal_handle = None
                self._follow_cancel_requested = False

            if getattr(self, "_pending_loop_restart", False):
                self._pending_loop_restart = False
                self._start_next_loop_segment(restart=True, retry_same_target=False)

    @staticmethod
    def _clamp(v: int, lo: int, hi: int) -> int:
        return max(lo, min(v, hi))

    @staticmethod
    def _wrap_closed_next(current_idx: int, total: int) -> int:
        return (current_idx + 1) % total

    @staticmethod
    def _choose_pingpong_next(current_idx: int, total: int, direction: int) -> tuple:
        # Returns (next_idx, new_direction)
        new_dir = int(direction) if int(direction) != 0 else 1
        if current_idx <= 0:
            new_dir = 1
        elif current_idx >= (total - 1):
            new_dir = -1
        cand = current_idx + new_dir
        if cand < 0 or cand >= total:
            new_dir = -1 if new_dir >= 0 else 1
            cand = current_idx + new_dir
        return int(cand), int(new_dir)

    """Thin ROS client bridge for Flask routes."""

    VALID_MODES = {'manual', 'zones', 'sequence', 'path'}
    MODE_ALIASES = {'auto': 'zones', 'autonomous': 'zones', 'nav': 'zones'}
    PROFILE_SCHEMA_VERSION = 1
    DEFAULT_MAPPINGS: Dict[str, Dict[str, str]] = {
        'battery': {
            'topic': '/battery_state',
            'field': 'percentage',
            'type': 'sensor_msgs/msg/BatteryState',
        },
        'speed': {
            'topic': '/odometry/filtered',
            'field': 'twist.twist.linear.x',
            'type': 'nav_msgs/msg/Odometry',
        },
        'localization': {
            'topic': '/amcl_pose',
            'field': 'pose.pose',
            'type': 'geometry_msgs/msg/PoseWithCovarianceStamped',
        },
        'robot_pose': {
            'topic': '/tf',
            'field': 'map->base_link',
            'type': 'tf2_msgs/msg/TFMessage',
        },
        'map': {
            'topic': '/map',
            'field': 'OccupancyGrid',
            'type': 'nav_msgs/msg/OccupancyGrid',
        },
        'cmd_vel': {
            'topic': '/wheel_controller/cmd_vel_unstamped',
            'field': 'linear.x',
            'type': 'geometry_msgs/msg/Twist',
        },
        **default_action_mappings(),
    }
    DEFAULT_TOPICS: Dict[str, str] = catalog_default_topics()
    DEFAULT_SENSOR_FRAMES: Dict[str, str] = {
        'map': 'map',
        'odom': 'odom',
        'laser': 'base_scan',
        'camera': 'camera_link',
    }
    DEFAULT_UI_TOGGLES: Dict[str, bool] = {
        'show_zones': True,
        'show_paths': True,
        'follow_robot': False,
        'show_robot_halo': True,
        'show_robot_heading': True,
    }
    URDF_MAX_UPLOAD_BYTES = 2 * 1024 * 1024
    URDF_MAX_SOURCE_EDIT_BYTES = 2 * 1024 * 1024
    URDF_REQUIRED_SENSOR_FRAME_KEYS = ('laser', 'camera')
    URDF_OPTIONAL_SENSOR_FRAME_KEYS = ('odom', 'map')
    URDF_VISUAL_MAX_SHAPES = 800
    NAV2_COSTMAP_SCOPES = ('local_costmap', 'global_costmap')
    NAV2_FOOTPRINT_DEFAULT = [[0.40, 0.355], [0.40, -0.355], [-0.40, -0.355], [-0.40, 0.355]]
    BRINGUP_REPORT_SCHEMA_VERSION = 1
    DEPLOY_REGISTRY_SCHEMA_VERSION = 1
    DEPLOY_REQUIRED_FILES: Dict[str, str] = {
        'profile': 'profile.yaml',
        'nav2_params': 'nav2_params.yaml',
        'compiled_urdf': 'compiled.urdf',
    }
    ROBOT_EDITOR_SCHEMA_VERSION = 1
    ROBOT_EDITOR_MAX_LINKS = 120
    ROBOT_EDITOR_MAX_SHAPES = 800
    ROBOT_EDITOR_ALLOWED_SHAPES = {'box', 'cylinder', 'sphere'}
    ROBOT_EDITOR_ALLOWED_JOINT_TYPES = {'fixed', 'revolute', 'continuous', 'prismatic', 'floating', 'planar'}
    RELAUNCH_TARGETS: Dict[str, Dict[str, str]] = {
        'lidar_front': {
            'label': 'LiDAR Front',
            'workspace': 'next_ros2',
            'launch_command': 'ros2 launch sllidar_ros2 sllidar_t1_launch.py',
        },
        'lidar_rear': {
            'label': 'LiDAR Rear',
            'workspace': 'next_ros2',
            'launch_command': 'ros2 launch sllidar_ros2 sllidar_s2e_launch.py',
        },
        'imu': {
            'label': 'IMU Localization',
            'workspace': 'next_EKF',
            'launch_command': 'ros2 launch ugv_localization localization.launch.py',
        },
        'shelf_detection': {
            'label': 'Shelf Detection',
            'workspace': 'testBuild',
            'launch_command': 'ros2 launch next2_shelf lidar_ros2_launch.py',
        },
    }

    @staticmethod
    def _sanitize_topics(raw: Any) -> Dict[str, str]:
        return sanitize_topic_overrides(raw)

    @classmethod
    def _merge_topics_with_defaults(cls, raw: Any) -> Dict[str, str]:
        return merge_topic_overrides(raw)

    @classmethod
    def _topic_overrides_for_storage(cls, raw: Any) -> Dict[str, str]:
        return extract_topic_overrides(raw)

    @staticmethod
    def _sanitize_mappings(raw: Any) -> Dict[str, Dict[str, str]]:
        if not isinstance(raw, dict):
            return {}

        cleaned: Dict[str, Dict[str, str]] = {}
        for key, value in raw.items():
            name = str(key or '').strip()
            if not name or not isinstance(value, dict):
                continue

            topic = str(value.get('topic', '') or '').strip()
            field = str(value.get('field', '') or '').strip()
            msg_type = str(value.get('type', '') or '').strip()

            entry = {'topic': topic, 'field': field}
            if msg_type:
                entry['type'] = msg_type
            cleaned[name] = entry
        return cleaned

    @staticmethod
    def _sanitize_sensor_frames(raw: Any) -> Dict[str, str]:
        if not isinstance(raw, dict):
            return {}
        cleaned: Dict[str, str] = {}
        for key, value in raw.items():
            name = str(key or '').strip()
            frame = str(value or '').strip()
            if name and frame:
                cleaned[name] = frame
        return cleaned

    @staticmethod
    def _sanitize_ui_toggles(raw: Any) -> Dict[str, bool]:
        if not isinstance(raw, dict):
            return {}
        cleaned: Dict[str, bool] = {}
        for key, value in raw.items():
            name = str(key or '').strip()
            if not name:
                continue
            if isinstance(value, bool):
                cleaned[name] = value
            elif isinstance(value, (int, float)):
                cleaned[name] = bool(value)
            else:
                text = str(value or '').strip().lower()
                cleaned[name] = text in {'1', 'true', 'yes', 'on'}
        return cleaned

    @staticmethod
    def _sanitize_urdf_artifact(raw: Any) -> Dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        validation_raw = source.get('validation', {})
        validation: Dict[str, Any] = {}
        if isinstance(validation_raw, dict):
            validation = {
                'valid': bool(validation_raw.get('valid', False)),
                'parse_ok': bool(validation_raw.get('parse_ok', False)),
                'tree_connected': bool(validation_raw.get('tree_connected', False)),
                'base_link_exists': bool(validation_raw.get('base_link_exists', False)),
                'required_frames_ok': bool(validation_raw.get('required_frames_ok', False)),
                'message': str(validation_raw.get('message', '') or '').strip(),
                'required_frames': [str(v).strip() for v in validation_raw.get('required_frames', []) if str(v).strip()],
                'missing_required_frames': [str(v).strip() for v in validation_raw.get('missing_required_frames', []) if str(v).strip()],
                'optional_missing_frames': [str(v).strip() for v in validation_raw.get('optional_missing_frames', []) if str(v).strip()],
                'unknown_joint_links': [str(v).strip() for v in validation_raw.get('unknown_joint_links', []) if str(v).strip()],
                'link_count': int(validation_raw.get('link_count', 0) or 0),
                'joint_count': int(validation_raw.get('joint_count', 0) or 0),
            }

        runtime_raw = source.get('runtime', {})
        runtime: Dict[str, Any] = {}
        if isinstance(runtime_raw, dict):
            runtime = {
                'ok': bool(runtime_raw.get('ok', False)),
                'message': str(runtime_raw.get('message', '') or '').strip(),
                'node_name': str(runtime_raw.get('node_name', '') or '').strip(),
                'namespace': str(runtime_raw.get('namespace', '') or '').strip(),
                'required_tf_frames': [str(v).strip() for v in runtime_raw.get('required_tf_frames', []) if str(v).strip()],
                'observed_tf_frames': [str(v).strip() for v in runtime_raw.get('observed_tf_frames', []) if str(v).strip()],
                'missing_tf_frames': [str(v).strip() for v in runtime_raw.get('missing_tf_frames', []) if str(v).strip()],
                'pid': int(runtime_raw.get('pid', 0) or 0),
            }

        return {
            'source_filename': str(source.get('source_filename', '') or '').strip(),
            'source_type': str(source.get('source_type', '') or '').strip(),
            'source_path': str(source.get('source_path', '') or '').strip(),
            'compiled_urdf_path': str(source.get('compiled_urdf_path', '') or '').strip(),
            'validation_report_path': str(source.get('validation_report_path', '') or '').strip(),
            'uploaded_at': int(source.get('uploaded_at', 0) or 0),
            'validation': validation,
            'runtime': runtime,
        }

    @staticmethod
    def _sanitize_profile_release(raw: Any) -> Dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        production = bool(source.get('production', False))
        production_marked_at = 0
        try:
            production_marked_at = max(0, int(source.get('production_marked_at', 0) or 0))
        except Exception:
            production_marked_at = 0
        if not production:
            production_marked_at = 0
        return {
            'production': production,
            'production_marked_at': production_marked_at,
            'production_note': str(source.get('production_note', '') or '').strip(),
        }

    @staticmethod
    def _sanitize_profile_bringup(raw: Any) -> Dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        numeric_defaults = {
            'last_run_at': 0,
            'last_total_steps': 0,
            'last_passed_steps': 0,
            'last_failed_steps': 0,
        }
        cleaned_counts: Dict[str, int] = {}
        for key, default_value in numeric_defaults.items():
            try:
                cleaned_counts[key] = max(0, int(source.get(key, default_value) or default_value))
            except Exception:
                cleaned_counts[key] = default_value

        return {
            'last_run_at': cleaned_counts['last_run_at'],
            'last_pass': bool(source.get('last_pass', False)),
            'last_total_steps': cleaned_counts['last_total_steps'],
            'last_passed_steps': cleaned_counts['last_passed_steps'],
            'last_failed_steps': cleaned_counts['last_failed_steps'],
            'last_summary': str(source.get('last_summary', '') or '').strip(),
            'last_report_file': str(source.get('last_report_file', '') or '').strip(),
            'last_log_file': str(source.get('last_log_file', '') or '').strip(),
            'last_run_message': str(source.get('last_run_message', '') or '').strip(),
        }

    @staticmethod
    def _safe_float(
        value: Any,
        default: float = 0.0,
        *,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = float(default)
        if min_value is not None:
            parsed = max(float(min_value), parsed)
        if max_value is not None:
            parsed = min(float(max_value), parsed)
        return float(parsed)


    @classmethod
    def _triplet_from_any(
        cls,
        raw: Any,
        *,
        default: Tuple[float, float, float] = (0.0, 0.0, 0.0),
    ) -> List[float]:
        values: List[float] = []

        if isinstance(raw, dict):
            values = [
                cls._safe_float(raw.get('x', default[0]), default[0]),
                cls._safe_float(raw.get('y', default[1]), default[1]),
                cls._safe_float(raw.get('z', default[2]), default[2]),
            ]
            return values

        if isinstance(raw, (list, tuple)):
            values = [cls._safe_float(raw[idx] if idx < len(raw) else default[idx], default[idx]) for idx in range(3)]
            return values

        text = str(raw or '').replace(',', ' ')
        parts = [part for part in text.split() if part]
        for idx in range(3):
            token = parts[idx] if idx < len(parts) else default[idx]
            values.append(cls._safe_float(token, default[idx]))
        return values

    @classmethod
    def _default_robot_builder(cls, *, base_link: str = 'base_link') -> Dict[str, Any]:
        base_name = cls._normalize_frame_name(base_link) or 'base_link'
        return {
            'schema_version': cls.ROBOT_EDITOR_SCHEMA_VERSION,
            'source': 'draft',
            'base_link': base_name,
            'links': [
                {
                    'name': base_name,
                    'parent': '',
                    'joint_type': 'fixed',
                    'joint_xyz': [0.0, 0.0, 0.0],
                    'joint_rpy': [0.0, 0.0, 0.0],
                },
            ],
            'shapes': [
                {
                    'id': 'shape_1',
                    'name': 'base_collision',
                    'type': 'box',
                    'link': base_name,
                    'x': 0.0,
                    'y': 0.0,
                    'z': 0.0,
            # ...existing code...
                    'yaw': 0.0,
                    'width': 0.60,
                    'length': 0.45,
                    'height': 0.30,
                    'radius': 0.20,
                    'color': '#60a5fa',
                },
            ],
            'updated_at': 0,
        }

    @classmethod
    def _sanitize_robot_builder(cls, raw: Any, *, fallback_base_link: str = 'base_link') -> Dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        fallback = cls._normalize_frame_name(fallback_base_link) or 'base_link'

        schema_version = cls.ROBOT_EDITOR_SCHEMA_VERSION
        try:
            schema_version = max(1, int(source.get('schema_version', cls.ROBOT_EDITOR_SCHEMA_VERSION) or cls.ROBOT_EDITOR_SCHEMA_VERSION))
        except Exception:
            schema_version = cls.ROBOT_EDITOR_SCHEMA_VERSION

        base_link = cls._normalize_frame_name(source.get('base_link', fallback)) or fallback
        source_label = str(source.get('source', 'draft') or 'draft').strip() or 'draft'

        links_raw = source.get('links', [])
        if not isinstance(links_raw, list):
            links_raw = []
        links: List[Dict[str, Any]] = []
        used_link_names: Set[str] = set()

        for idx, raw_link in enumerate(links_raw):
            if not isinstance(raw_link, dict):
                continue

            raw_name = cls._normalize_frame_name(raw_link.get('name', '')) or f'link_{idx + 1}'
            candidate = raw_name
            suffix = 2
            while candidate in used_link_names:
                candidate = f'{raw_name}_{suffix}'
                suffix += 1
            link_name = candidate
            used_link_names.add(link_name)

            parent = cls._normalize_frame_name(raw_link.get('parent', ''))
            if parent == link_name:
                parent = ''

            joint_type = str(raw_link.get('joint_type', 'fixed') or 'fixed').strip().lower()
            if joint_type not in cls.ROBOT_EDITOR_ALLOWED_JOINT_TYPES:
                joint_type = 'fixed'

            joint_xyz = cls._triplet_from_any(raw_link.get('joint_xyz', [0.0, 0.0, 0.0]), default=(0.0, 0.0, 0.0))
            joint_rpy = cls._triplet_from_any(raw_link.get('joint_rpy', [0.0, 0.0, 0.0]), default=(0.0, 0.0, 0.0))

            links.append(
                {
                    'name': link_name,
                    'parent': parent,
                    'joint_type': joint_type,
                    'joint_xyz': [round(joint_xyz[0], 6), round(joint_xyz[1], 6), round(joint_xyz[2], 6)],
                    'joint_rpy': [round(joint_rpy[0], 6), round(joint_rpy[1], 6), round(joint_rpy[2], 6)],
                }
            )

            if len(links) >= int(cls.ROBOT_EDITOR_MAX_LINKS):
                break

        link_names = {str(link.get('name', '') or '') for link in links if str(link.get('name', '') or '').strip()}
        if base_link not in link_names:
            links.insert(
                0,
                {
                    'name': base_link,
                    'parent': '',
                    'joint_type': 'fixed',
                    'joint_xyz': [0.0, 0.0, 0.0],
                    'joint_rpy': [0.0, 0.0, 0.0],
                },
            )
            link_names.add(base_link)

        for link in links:
            name = str(link.get('name', '') or '').strip()
            parent = cls._normalize_frame_name(link.get('parent', ''))
            if not name or name == base_link:
                link['parent'] = ''
                continue
            if not parent or parent == name or parent not in link_names:
                link['parent'] = base_link
            else:
                link['parent'] = parent

        shapes_raw = source.get('shapes', [])
        if not isinstance(shapes_raw, list):
            shapes_raw = []
        shapes: List[Dict[str, Any]] = []
        used_shape_ids: Set[str] = set()

        for idx, raw_shape in enumerate(shapes_raw):
            if not isinstance(raw_shape, dict):
                continue

            shape_type = str(raw_shape.get('type', 'box') or 'box').strip().lower()
            if shape_type not in cls.ROBOT_EDITOR_ALLOWED_SHAPES:
                shape_type = 'box'

            raw_id = str(raw_shape.get('id', '') or '').strip() or f'shape_{idx + 1}'
            shape_id = raw_id
            suffix = 2
            while shape_id in used_shape_ids:
                shape_id = f'{raw_id}_{suffix}'
                suffix += 1
            used_shape_ids.add(shape_id)

            link = cls._normalize_frame_name(raw_shape.get('link', '')) or base_link
            if link not in link_names:
                link = base_link

            width = cls._safe_float(raw_shape.get('width', 0.40), 0.40, min_value=0.01, max_value=100.0)
            length = cls._safe_float(raw_shape.get('length', 0.30), 0.30, min_value=0.01, max_value=100.0)
            height = cls._safe_float(raw_shape.get('height', 0.25), 0.25, min_value=0.01, max_value=100.0)
            radius = cls._safe_float(raw_shape.get('radius', 0.15), 0.15, min_value=0.005, max_value=100.0)

            if shape_type == 'sphere':
                width = 2.0 * radius
                length = 2.0 * radius
            elif shape_type == 'cylinder':
                width = 2.0 * radius
                length = 2.0 * radius
            else:
                radius = max(0.005, min(width, length) * 0.5)

            shapes.append(
                {
                    'id': shape_id,
                    'name': str(raw_shape.get('name', shape_id) or shape_id).strip() or shape_id,
                    'type': shape_type,
                    'link': link,
                    'x': round(cls._safe_float(raw_shape.get('x', 0.0), 0.0, min_value=-1000.0, max_value=1000.0), 6),
                    'y': round(cls._safe_float(raw_shape.get('y', 0.0), 0.0, min_value=-1000.0, max_value=1000.0), 6),
                    'z': round(cls._safe_float(raw_shape.get('z', 0.0), 0.0, min_value=-1000.0, max_value=1000.0), 6),
                    'yaw': round(cls._safe_float(raw_shape.get('yaw', 0.0), 0.0, min_value=-1.0e6, max_value=1.0e6), 6),
                    'width': round(width, 6),
                    'length': round(length, 6),
                    'height': round(height, 6),
                    'radius': round(radius, 6),
                    'color': str(raw_shape.get('color', '#60a5fa') or '#60a5fa').strip() or '#60a5fa',
                }
            )

            if len(shapes) >= int(cls.ROBOT_EDITOR_MAX_SHAPES):
                break

        if not shapes:
            default_model = cls._default_robot_builder(base_link=base_link)
            shapes = default_model['shapes']

        updated_at = 0
        try:
            updated_at = max(0, int(source.get('updated_at', 0) or 0))
        except Exception:
            updated_at = 0

        return {
            'schema_version': schema_version,
            'source': source_label,
            'base_link': base_link,
            'links': links,
            'shapes': shapes,
            'updated_at': updated_at,
        }

    @classmethod
    def _merge_mappings_with_defaults(cls, loaded: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        merged: Dict[str, Dict[str, str]] = {}
        for key, default_value in cls.DEFAULT_MAPPINGS.items():
            entry = dict(default_value)
            if key in loaded:
                entry.update(loaded[key])
            merged[key] = entry

        for key, value in loaded.items():
            if key not in merged:
                merged[key] = dict(value)
        return merge_action_mappings(merged)

    def _get_or_declare_parameter(self, name: str, default_value: Any) -> Any:
        if self.has_parameter(name):
            return self.get_parameter(name).value
        return self.declare_parameter(name, default_value).value

    @staticmethod
    def _safe_profile_slug(robot_id: str) -> str:
        cleaned = re.sub(r'[^a-zA-Z0-9_-]+', '_', str(robot_id or '').strip()).strip('_')
        return cleaned.lower() or 'robot_profile'

    @classmethod
    def _sanitize_profile_payload(cls, raw: Any, fallback_robot_id: str = '') -> Dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        robot_id = str(source.get('robot_id', fallback_robot_id) or '').strip()
        namespace = str(source.get('namespace', '/') or '').strip() or '/'
        base_frame = str(source.get('base_frame', 'base_link') or '').strip() or 'base_link'
        urdf_source = str(source.get('urdf_source', '') or '').strip()
        nav2_params_file = str(source.get('nav2_params_file', '') or '').strip()

        sensor_frames = dict(cls.DEFAULT_SENSOR_FRAMES)
        sensor_frames.update(cls._sanitize_sensor_frames(source.get('sensor_frames', {})))

        ui_toggles = dict(cls.DEFAULT_UI_TOGGLES)
        ui_toggles.update(cls._sanitize_ui_toggles(source.get('ui_toggles', {})))

        map_association: Dict[str, str] = {}
        map_raw = source.get('map_association', {})
        if isinstance(map_raw, dict):
            map_association['active_map'] = str(map_raw.get('active_map', '') or '').strip()
        else:
            map_association['active_map'] = ''
        if not map_association['active_map']:
            map_association['active_map'] = str(source.get('map_yaml_path', '') or '').strip()

        profile_version = 1
        try:
            profile_version = max(1, int(source.get('profile_version', 1) or 1))
        except Exception:
            profile_version = 1

        schema_version = cls.PROFILE_SCHEMA_VERSION
        try:
            schema_version = max(1, int(source.get('schema_version', cls.PROFILE_SCHEMA_VERSION) or cls.PROFILE_SCHEMA_VERSION))
        except Exception:
            schema_version = cls.PROFILE_SCHEMA_VERSION

        mappings = cls._merge_mappings_with_defaults(cls._sanitize_mappings(source.get('mappings', {})))
        topics = cls._merge_topics_with_defaults(source.get('topics', {}))
        urdf_artifact = cls._sanitize_urdf_artifact(source.get('urdf_artifact', {}))
        robot_builder = cls._sanitize_robot_builder(source.get('robot_builder', {}), fallback_base_link=base_frame)
        release = cls._sanitize_profile_release(source.get('release', {}))
        bringup = cls._sanitize_profile_bringup(source.get('bringup', {}))

        return {
            'schema_version': schema_version,
            'profile_version': profile_version,
            'robot_id': robot_id,
            'namespace': namespace,
            'base_frame': base_frame,
            'sensor_frames': sensor_frames,
            'urdf_source': urdf_source,
            'nav2_params_file': nav2_params_file,
            'map_association': map_association,
            'ui_toggles': ui_toggles,
            'mappings': mappings,
            'topics': topics,
            'urdf_artifact': urdf_artifact,
            'robot_builder': robot_builder,
            'release': release,
            'bringup': bringup,
        }

    @classmethod
    def _profile_for_storage(cls, profile: Dict[str, Any]) -> Dict[str, Any]:
        stored = copy.deepcopy(profile if isinstance(profile, dict) else {})
        stored['topics'] = cls._topic_overrides_for_storage(stored.get('topics', {}))
        return stored

    def _load_profile_registry_state(self) -> Dict[str, Any]:
        state: Dict[str, Any] = {}
        if not getattr(self, 'robot_profile_registry_file', ''):
            active_from_db = self._load_active_profile_id_from_db()
            if active_from_db:
                return {
                    'schema_version': self.PROFILE_SCHEMA_VERSION,
                    'active_robot_id': active_from_db,
                    'updated_at': int(time.time()),
                }
            return state
        if not os.path.exists(self.robot_profile_registry_file):
            active_from_db = self._load_active_profile_id_from_db()
            if active_from_db:
                return {
                    'schema_version': self.PROFILE_SCHEMA_VERSION,
                    'active_robot_id': active_from_db,
                    'updated_at': int(time.time()),
                }
            return state
        try:
            with open(self.robot_profile_registry_file, 'r', encoding='utf-8') as f:
                loaded = yaml.safe_load(f) or {}
            if isinstance(loaded, dict):
                state = loaded
        except Exception as exc:
            self.get_logger().warn(f'Failed to load robot profile registry state: {exc}')
        if not str(state.get('active_robot_id', '') or '').strip():
            active_from_db = self._load_active_profile_id_from_db()
            if active_from_db:
                state['active_robot_id'] = active_from_db
        return state

    def _save_profile_registry_state(self, active_robot_id: str):
        if not getattr(self, 'robot_profile_registry_file', ''):
            return
        payload = {
            'schema_version': self.PROFILE_SCHEMA_VERSION,
            'active_robot_id': str(active_robot_id or '').strip(),
            'updated_at': int(time.time()),
        }
        try:
            registry_dir = os.path.dirname(self.robot_profile_registry_file)
            if registry_dir:
                os.makedirs(registry_dir, exist_ok=True)
            with open(self.robot_profile_registry_file, 'w', encoding='utf-8') as f:
                yaml.safe_dump(payload, f, sort_keys=False)
        except Exception as exc:
            self.get_logger().warn(f'Failed to save robot profile registry state: {exc}')

        self._save_active_profile_id_to_db(payload.get('active_robot_id', ''))

    def _initialize_profile_db_storage(self):
        self.db_manager = None
        if _DatabaseManager is None:
            return
        try:
            configured_db_path = str(
                self._get_or_declare_parameter('db_path', os.path.expanduser('~/DB/robot_data.db')) or ''
            ).strip()
            db_path = os.path.expanduser(configured_db_path or '~/DB/robot_data.db')
            self.db_manager = _DatabaseManager(db_path=db_path)
        except Exception as exc:
            self.db_manager = None
            self.get_logger().warn(f'Failed to initialize DatabaseManager for profile persistence: {exc}')

    def _load_robot_profiles_from_db(self) -> Dict[str, Dict[str, Any]]:
        manager = getattr(self, 'db_manager', None)
        if manager is None:
            return {}
        try:
            raw_profiles = manager.get_robot_profiles()
            if not isinstance(raw_profiles, dict):
                return {}
            profiles: Dict[str, Dict[str, Any]] = {}
            for robot_id, payload in raw_profiles.items():
                profile = self._sanitize_profile_payload(payload, fallback_robot_id=str(robot_id or '').strip())
                rid = str(profile.get('robot_id', '') or '').strip()
                if rid:
                    profiles[rid] = profile
            return profiles
        except Exception as exc:
            self.get_logger().warn(f'Failed loading robot profiles from database: {exc}')
            return {}

    def _save_robot_profile_to_db(self, profile: Dict[str, Any]):
        manager = getattr(self, 'db_manager', None)
        if manager is None:
            return
        try:
            sanitized = self._sanitize_profile_payload(profile, fallback_robot_id=str(profile.get('robot_id', '') or ''))
            robot_id = str(sanitized.get('robot_id', '') or '').strip()
            if not robot_id:
                return
            manager.save_robot_profile(robot_id, self._profile_for_storage(sanitized))
        except Exception as exc:
            self.get_logger().warn(f'Failed saving robot profile to database: {exc}')

    def _save_all_profiles_to_db(self, profiles: Dict[str, Dict[str, Any]]):
        manager = getattr(self, 'db_manager', None)
        if manager is None or not isinstance(profiles, dict):
            return
        try:
            sanitized_profiles: Dict[str, Dict[str, Any]] = {}
            for robot_id, payload in profiles.items():
                profile = self._sanitize_profile_payload(payload, fallback_robot_id=str(robot_id or '').strip())
                rid = str(profile.get('robot_id', '') or '').strip()
                if rid:
                    sanitized_profiles[rid] = self._profile_for_storage(profile)
            if sanitized_profiles:
                manager.save_robot_profiles(sanitized_profiles)
        except Exception as exc:
            self.get_logger().warn(f'Failed saving robot profiles to database: {exc}')

    def _load_active_profile_id_from_db(self) -> str:
        manager = getattr(self, 'db_manager', None)
        if manager is None:
            return ''
        try:
            return str(manager.get_active_robot_profile_id() or '').strip()
        except Exception as exc:
            self.get_logger().warn(f'Failed loading active robot profile id from database: {exc}')
            return ''

    def _save_active_profile_id_to_db(self, active_robot_id: str):
        manager = getattr(self, 'db_manager', None)
        if manager is None:
            return
        try:
            manager.save_active_robot_profile_id(str(active_robot_id or '').strip())
        except Exception as exc:
            self.get_logger().warn(f'Failed saving active robot profile id to database: {exc}')

    def _profile_file_path(self, robot_id: str) -> str:
        filename = f'{self._safe_profile_slug(robot_id)}.yaml'
        return os.path.join(self.robot_profiles_dir, filename)

    def _list_profile_paths(self) -> List[str]:
        if not getattr(self, 'robot_profiles_dir', ''):
            return []
        if not os.path.isdir(self.robot_profiles_dir):
            return []
        supported = {'.yaml', '.yml', '.json'}
        paths: List[str] = []
        registry_filename = os.path.basename(str(getattr(self, 'robot_profile_registry_file', '') or ''))
        for filename in sorted(os.listdir(self.robot_profiles_dir)):
            if filename.startswith('.'):
                continue
            if registry_filename and filename == registry_filename:
                continue
            base, ext = os.path.splitext(filename)
            if not base or ext.lower() not in supported:
                continue
            path = os.path.join(self.robot_profiles_dir, filename)
            # Broken symlinks are common in devel/install overlays when source
            # config paths are moved; ignore them so bootstrap can recover.
            if not os.path.exists(path):
                if os.path.islink(path):
                    try:
                        target = os.readlink(path)
                    except OSError:
                        target = '<unresolved>'
                    self.get_logger().warn(
                        f'Ignoring broken robot profile symlink "{path}" -> "{target}"'
                    )
                continue
            paths.append(path)
        return paths

    def _load_profile_payload(self, path: str) -> Dict[str, Any]:
        loaded: Dict[str, Any] = {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                if str(path).lower().endswith('.json'):
                    parsed = json.load(f)
                else:
                    parsed = yaml.safe_load(f) or {}
            if isinstance(parsed, dict):
                loaded = parsed
        except Exception as exc:
            self.get_logger().warn(f'Failed to load robot profile "{path}": {exc}')
        return loaded

    def _write_profile_payload(self, path: str, payload: Dict[str, Any]):
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        # Replace dangling symlink entry with a real file so writes succeed.
        if os.path.islink(path) and not os.path.exists(path):
            os.unlink(path)
        with open(path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(payload, f, sort_keys=False)

    def _ensure_robot_profiles_storage(self, *, recover: bool = True) -> str:
        configured = str(getattr(self, 'robot_profiles_dir', '') or '').strip()
        preferred = os.path.abspath(
            os.path.expanduser(configured or os.path.join(self.ui_root, 'config', 'robot_profiles'))
        )
        try:
            os.makedirs(preferred, exist_ok=True)
            self.robot_profiles_dir = preferred
            self.robot_profile_registry_file = os.path.join(preferred, 'registry.yaml')
            return preferred
        except Exception as exc:
            if not recover:
                raise
            fallback = os.path.abspath(
                os.path.expanduser(
                    os.path.join(os.path.expanduser('~'), '.next_ros2ws', 'robot_profiles')
                )
            )
            os.makedirs(fallback, exist_ok=True)
            if preferred != fallback:
                self.get_logger().warn(
                    f'Robot profiles dir "{preferred}" unavailable ({exc}); using fallback "{fallback}"'
                )
            self.robot_profiles_dir = fallback
            self.robot_profile_registry_file = os.path.join(fallback, 'registry.yaml')
            return fallback

    def _load_robot_profiles(self) -> Dict[str, Dict[str, Any]]:
        profiles: Dict[str, Dict[str, Any]] = {}
        profile_files: Dict[str, str] = {}
        for path in self._list_profile_paths():
            fallback_robot_id = os.path.splitext(os.path.basename(path))[0]
            payload = self._load_profile_payload(path)
            profile = self._sanitize_profile_payload(payload, fallback_robot_id=fallback_robot_id)
            robot_id = str(profile.get('robot_id', '') or '').strip()
            if not robot_id:
                continue
            storage_payload = self._profile_for_storage(profile)
            raw_topics = self._sanitize_topics(payload.get('topics', {}))
            stored_topics = storage_payload.get('topics', {})
            if raw_topics != stored_topics:
                try:
                    self._write_profile_payload(path, storage_payload)
                except Exception as exc:
                    self.get_logger().warn(f'Failed to normalize robot profile "{path}": {exc}')
            profiles[robot_id] = profile
            profile_files[robot_id] = path

        if not profiles:
            db_profiles = self._load_robot_profiles_from_db()
            if db_profiles:
                for robot_id, profile in db_profiles.items():
                    profiles[robot_id] = profile

        if profiles:
            self._save_all_profiles_to_db(profiles)

        self.robot_profile_files = profile_files
        return profiles

    def _resolve_profile_map_path(self, active_map: str) -> str:
        configured = str(active_map or '').strip()
        if not configured:
            return ''
        expanded = os.path.expanduser(configured)
        if os.path.isabs(expanded):
            return expanded
        return os.path.join(self.maps_dir, expanded)

    def _profile_map_reference(self, path: str) -> str:
        resolved = str(path or '').strip()
        if not resolved:
            return ''
        absolute = os.path.abspath(os.path.expanduser(resolved))
        maps_root = os.path.abspath(self.maps_dir)
        prefix = f'{maps_root}{os.sep}'
        if absolute == maps_root:
            return '.'
        if absolute.startswith(prefix):
            return os.path.relpath(absolute, maps_root)
        return resolved

    def _build_profile_runtime_payload(
        self,
        profile: Dict[str, Any],
        *,
        map_result: Optional[Dict[str, Any]] = None,
        settings_sync: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        active_map = str(profile.get('map_association', {}).get('active_map', '') or '').strip()
        map_yaml_path = self._resolve_profile_map_path(active_map) if active_map else ''
        payload: Dict[str, Any] = {
            'active_robot_id': str(profile.get('robot_id', '') or '').strip(),
            'profile': copy.deepcopy(profile),
            'mappings': copy.deepcopy(profile.get('mappings', {})),
            'topics': copy.deepcopy(self._merge_topics_with_defaults(profile.get('topics', {}))),
            'frames': {
                'namespace': str(profile.get('namespace', '/') or '/'),
                'base_frame': str(profile.get('base_frame', 'base_link') or 'base_link'),
                'sensor_frames': copy.deepcopy(profile.get('sensor_frames', {})),
            },
            'ui_toggles': copy.deepcopy(profile.get('ui_toggles', {})),
            'map_association': {
                'active_map': active_map,
                'map_yaml_path': map_yaml_path,
            },
            'profiles_dir': self.robot_profiles_dir,
        }
        if map_result is not None:
            payload['map_result'] = map_result
        if settings_sync is not None:
            payload['settings_sync'] = settings_sync
        return payload

    def _apply_profile_config_to_memory(self, profile: Dict[str, Any]):
        mappings = self._merge_mappings_with_defaults(self._sanitize_mappings(profile.get('mappings', {})))
        topics = self._merge_topics_with_defaults(profile.get('topics', {}))

        runtime_profile = self._sanitize_profile_payload(profile, fallback_robot_id=str(profile.get('robot_id', '') or ''))
        runtime_profile['mappings'] = mappings
        runtime_profile['topics'] = topics

        self.cached_settings_mappings = copy.deepcopy(mappings)
        self.topic_config = copy.deepcopy(topics)
        self.active_robot_profile = runtime_profile
        self.active_robot_profile_id = str(runtime_profile.get('robot_id', '') or '').strip()

    def _save_profile_to_disk(
        self,
        profile: Dict[str, Any],
        *,
        allow_overwrite: bool,
        increment_version: bool = False,
    ) -> Dict[str, Any]:
        self._ensure_robot_profiles_storage(recover=True)
        normalized = self._sanitize_profile_payload(profile, fallback_robot_id=str(profile.get('robot_id', '') or ''))
        robot_id = str(normalized.get('robot_id', '') or '').strip()
        if not robot_id:
            raise ValueError('robot_id is required')

        existing_path = self.robot_profile_files.get(robot_id, '')
        if existing_path and os.path.exists(existing_path):
            target_path = existing_path
            if not allow_overwrite:
                raise ValueError(f'Robot profile "{robot_id}" already exists')
            if increment_version:
                try:
                    normalized['profile_version'] = max(1, int(normalized.get('profile_version', 1) or 1)) + 1
                except Exception:
                    normalized['profile_version'] = 2
        else:
            target_path = self._profile_file_path(robot_id)
            if os.path.exists(target_path) and not allow_overwrite:
                raise ValueError(f'Robot profile "{robot_id}" already exists')

        try:
            self._write_profile_payload(target_path, self._profile_for_storage(normalized))
            self.robot_profile_files[robot_id] = target_path
        except FileNotFoundError:
            # Recover from broken install/share symlink paths by switching to fallback storage.
            self._ensure_robot_profiles_storage(recover=True)
            recovered_target = self._profile_file_path(robot_id)
            self._write_profile_payload(recovered_target, self._profile_for_storage(normalized))
            self.robot_profile_files[robot_id] = recovered_target
        self._save_robot_profile_to_db(normalized)
        return normalized

    def _ensure_profile_registry_bootstrap(self):
        if self._list_profile_paths():
            return

        nav2_default = os.path.join(self.ui_root, 'config', 'nav2_params.yaml')
        nav2_params_file = nav2_default if os.path.exists(nav2_default) else ''
        active_map_yaml = self._resolve_active_map_yaml() or ''
        default_profile = {
            'schema_version': self.PROFILE_SCHEMA_VERSION,
            'profile_version': 1,
            'robot_id': 'UGV-01',
            'namespace': '/',
            'base_frame': 'base_link',
            'sensor_frames': dict(self.DEFAULT_SENSOR_FRAMES),
            'urdf_source': self._default_description_urdf_source(),
            'nav2_params_file': nav2_params_file,
            'map_association': {
                'active_map': self._profile_map_reference(active_map_yaml),
            },
            'ui_toggles': dict(self.DEFAULT_UI_TOGGLES),
            'mappings': copy.deepcopy(getattr(self, 'cached_settings_mappings', self._merge_mappings_with_defaults({}))),
            'topics': copy.deepcopy(getattr(self, 'topic_config', self._merge_topics_with_defaults({}))),
        }
        normalized = self._sanitize_profile_payload(default_profile, fallback_robot_id='UGV-01')
        self._write_profile_payload(self._profile_file_path('UGV-01'), self._profile_for_storage(normalized))
        self._save_robot_profile_to_db(normalized)

    def _initialize_profile_registry(self):
        configured_dir = str(
            self._get_or_declare_parameter(
                'robot_profiles_dir',
                os.path.join(self.ui_root, 'config', 'robot_profiles'),
            )
            or ''
        ).strip()
        self.robot_profiles_dir = os.path.abspath(
            os.path.expanduser(configured_dir or os.path.join(self.ui_root, 'config', 'robot_profiles'))
        )
        self._ensure_robot_profiles_storage(recover=True)

        self._ensure_profile_registry_bootstrap()
        profiles = self._load_robot_profiles()
        if not profiles:
            self.active_robot_profile_id = ''
            self.active_robot_profile = None
            return

        state = self._load_profile_registry_state()
        active_robot_id = str(state.get('active_robot_id', '') or '').strip()
        if active_robot_id not in profiles:
            active_robot_id = sorted(profiles.keys())[0]

        active_profile = profiles.get(active_robot_id, {})
        if active_profile:
            self._apply_profile_config_to_memory(active_profile)
            self._save_profile_registry_state(active_robot_id)

    def _sync_active_profile_from_mappings(self, mappings: Dict[str, Dict[str, str]], topics: Dict[str, str]):
        active_robot_id = str(getattr(self, 'active_robot_profile_id', '') or '').strip()
        if not active_robot_id:
            return
        profiles = self._load_robot_profiles()
        profile = profiles.get(active_robot_id)
        if not profile:
            return

        profile['mappings'] = self._merge_mappings_with_defaults(self._sanitize_mappings(mappings))
        profile['topics'] = self._merge_topics_with_defaults(topics)
        profile['profile_version'] = max(1, int(profile.get('profile_version', 1) or 1)) + 1

        try:
            saved = self._save_profile_to_disk(profile, allow_overwrite=True, increment_version=False)
            self._apply_profile_config_to_memory(saved)
            self._save_profile_registry_state(active_robot_id)
        except Exception as exc:
            self.get_logger().warn(f'Failed syncing active robot profile: {exc}')

    def _artifact_relative_path(self, absolute_path: str) -> str:
        raw = str(absolute_path or '').strip()
        if not raw:
            return ''
        base = os.path.abspath(self.robot_profiles_dir)
        target = os.path.abspath(os.path.expanduser(raw))
        prefix = f'{base}{os.sep}'
        if target == base:
            return '.'
        if target.startswith(prefix):
            return os.path.relpath(target, base)
        return target

    def _artifact_absolute_path(self, artifact_path: str) -> str:
        raw = str(artifact_path or '').strip()
        if not raw:
            return ''
        expanded = os.path.abspath(os.path.expanduser(raw))
        if os.path.isabs(raw) and os.path.exists(expanded):
            return expanded
        return os.path.abspath(os.path.join(self.robot_profiles_dir, raw))

    @staticmethod
    def _sanitize_deploy_record(raw: Any) -> Dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}

        def _read_snapshot_id(key: str) -> str:
            return str(source.get(key, '') or '').strip()

        updated_at = 0
        try:
            updated_at = max(0, int(source.get('updated_at', 0) or 0))
        except Exception:
            updated_at = 0

        return {
            'current_snapshot_id': _read_snapshot_id('current_snapshot_id'),
            'last_known_good_snapshot_id': _read_snapshot_id('last_known_good_snapshot_id'),
            'previous_snapshot_id': _read_snapshot_id('previous_snapshot_id'),
            'updated_at': updated_at,
        }

    def _initialize_config_deploy_registry(self):
        configured_dir = str(
            self._get_or_declare_parameter(
                'robot_config_deploy_dir',
                os.path.join(self.ui_root, 'config', 'robot_runtime_config'),
            )
            or ''
        ).strip()

        self.robot_config_deploy_dir = os.path.abspath(
            os.path.expanduser(configured_dir or os.path.join(self.ui_root, 'config', 'robot_runtime_config'))
        )
        self.robot_config_snapshots_dir = os.path.join(self.robot_config_deploy_dir, 'snapshots')
        self.robot_config_active_dir = os.path.join(self.robot_config_deploy_dir, 'active')
        self.robot_config_registry_file = os.path.join(self.robot_config_deploy_dir, 'deploy_registry.yaml')

        os.makedirs(self.robot_config_deploy_dir, exist_ok=True)
        os.makedirs(self.robot_config_snapshots_dir, exist_ok=True)
        os.makedirs(self.robot_config_active_dir, exist_ok=True)

    def _load_config_deploy_registry(self) -> Dict[str, Any]:
        registry: Dict[str, Any] = {
            'schema_version': self.DEPLOY_REGISTRY_SCHEMA_VERSION,
            'updated_at': 0,
            'robots': {},
        }

        path = str(getattr(self, 'robot_config_registry_file', '') or '').strip()
        if not path or not os.path.exists(path):
            return registry

        try:
            with open(path, 'r', encoding='utf-8') as f:
                loaded = yaml.safe_load(f) or {}
        except Exception as exc:
            self.get_logger().warn(f'Failed to load deploy registry: {exc}')
            return registry

        if not isinstance(loaded, dict):
            return registry

        try:
            registry['schema_version'] = max(
                1,
                int(loaded.get('schema_version', self.DEPLOY_REGISTRY_SCHEMA_VERSION) or self.DEPLOY_REGISTRY_SCHEMA_VERSION),
            )
        except Exception:
            registry['schema_version'] = self.DEPLOY_REGISTRY_SCHEMA_VERSION

        try:
            registry['updated_at'] = max(0, int(loaded.get('updated_at', 0) or 0))
        except Exception:
            registry['updated_at'] = 0

        robots_raw = loaded.get('robots', {})
        robots: Dict[str, Dict[str, Any]] = {}
        if isinstance(robots_raw, dict):
            for robot_id, raw in robots_raw.items():
                name = str(robot_id or '').strip()
                if not name:
                    continue
                robots[name] = self._sanitize_deploy_record(raw)
        registry['robots'] = robots
        return registry

    def _save_config_deploy_registry(self, registry: Dict[str, Any]):
        path = str(getattr(self, 'robot_config_registry_file', '') or '').strip()
        if not path:
            return

        payload: Dict[str, Any] = {
            'schema_version': self.DEPLOY_REGISTRY_SCHEMA_VERSION,
            'updated_at': int(time.time()),
            'robots': {},
        }

        robots_raw = registry.get('robots', {})
        if isinstance(robots_raw, dict):
            robots: Dict[str, Any] = {}
            for robot_id, raw in robots_raw.items():
                name = str(robot_id or '').strip()
                if not name:
                    continue
                robots[name] = self._sanitize_deploy_record(raw)
            payload['robots'] = robots

        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(payload, f, sort_keys=False)

    def _deploy_robot_snapshots_dir(self, robot_id: str) -> str:
        return os.path.join(self.robot_config_snapshots_dir, self._safe_profile_slug(robot_id))

    def _deploy_snapshot_dir(self, robot_id: str, snapshot_id: str) -> str:
        return os.path.join(self._deploy_robot_snapshots_dir(robot_id), str(snapshot_id or '').strip())

    def _deploy_active_dir(self, robot_id: str) -> str:
        return os.path.join(self.robot_config_active_dir, self._safe_profile_slug(robot_id))

    def _next_deploy_snapshot_id(self, robot_id: str, profile_version: int) -> str:
        base = time.strftime('%Y%m%d_%H%M%S')
        version_part = f'v{max(1, int(profile_version or 1))}'
        candidate = f'{base}_{version_part}'
        root = self._deploy_robot_snapshots_dir(robot_id)
        os.makedirs(root, exist_ok=True)
        snapshot_dir = os.path.join(root, candidate)
        if not os.path.exists(snapshot_dir):
            return candidate

        suffix = 2
        while True:
            alt = f'{candidate}_{suffix}'
            if not os.path.exists(os.path.join(root, alt)):
                return alt
            suffix += 1

    @staticmethod
    def _sha256_file(path: str) -> str:
        digest = hashlib.sha256()
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _sha256_text(content: str) -> str:
        return hashlib.sha256(str(content or '').encode('utf-8')).hexdigest()

    def _load_deploy_manifest(self, robot_id: str, snapshot_id: str) -> Dict[str, Any]:
        manifest_path = os.path.join(self._deploy_snapshot_dir(robot_id, snapshot_id), 'manifest.yaml')
        if not os.path.exists(manifest_path):
            return {}
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                loaded = yaml.safe_load(f) or {}
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {}

    @classmethod
    def _manifest_expected_hashes(cls, manifest: Dict[str, Any]) -> Dict[str, str]:
        expected: Dict[str, str] = {}
        files = manifest.get('files', {}) if isinstance(manifest.get('files', {}), dict) else {}
        for key in cls.DEPLOY_REQUIRED_FILES:
            entry = files.get(key, {}) if isinstance(files.get(key, {}), dict) else {}
            digest = str(entry.get('sha256', '') or '').strip().lower()
            if digest:
                expected[key] = digest
        return expected

    def _collect_hashes_for_dir(self, directory: str) -> Dict[str, str]:
        hashes: Dict[str, str] = {}
        root = str(directory or '').strip()
        if not root or not os.path.isdir(root):
            return hashes

        for key, filename in self.DEPLOY_REQUIRED_FILES.items():
            path = os.path.join(root, filename)
            if not os.path.exists(path):
                continue
            try:
                hashes[key] = self._sha256_file(path)
            except Exception:
                continue
        return hashes

    @staticmethod
    def _hashes_match(actual: Dict[str, str], expected: Dict[str, str]) -> bool:
        if not expected:
            return False
        for key, exp in expected.items():
            got = str(actual.get(key, '') or '').strip().lower()
            if got != str(exp or '').strip().lower():
                return False
        return True

    def _collect_recent_deploy_snapshots(self, robot_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        root = self._deploy_robot_snapshots_dir(robot_id)
        if not os.path.isdir(root):
            return []

        entries: List[Dict[str, Any]] = []
        for name in os.listdir(root):
            snapshot_id = str(name or '').strip()
            if not snapshot_id:
                continue
            snapshot_dir = os.path.join(root, snapshot_id)
            if not os.path.isdir(snapshot_dir):
                continue

            manifest = self._load_deploy_manifest(robot_id, snapshot_id)
            if not manifest:
                continue

            created_at = 0
            try:
                created_at = max(0, int(manifest.get('created_at', 0) or 0))
            except Exception:
                created_at = 0

            profile_version = 0
            try:
                profile_version = max(0, int(manifest.get('profile_version', 0) or 0))
            except Exception:
                profile_version = 0

            entries.append(
                {
                    'snapshot_id': snapshot_id,
                    'created_at': created_at,
                    'profile_version': profile_version,
                    'manifest_file': os.path.join(snapshot_dir, 'manifest.yaml'),
                    'hashes': self._manifest_expected_hashes(manifest),
                }
            )

        entries.sort(key=lambda item: (int(item.get('created_at', 0) or 0), str(item.get('snapshot_id', ''))), reverse=True)
        return entries[: max(1, int(limit))]

    def _activate_deploy_snapshot(self, robot_id: str, snapshot_dir: str) -> str:
        source_dir = str(snapshot_dir or '').strip()
        if not source_dir or not os.path.isdir(source_dir):
            raise ValueError('Snapshot directory does not exist')

        active_dir = self._deploy_active_dir(robot_id)
        parent_dir = os.path.dirname(active_dir)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        temp_dir = tempfile.mkdtemp(prefix=f'.active_{self._safe_profile_slug(robot_id)}_', dir=parent_dir or None)
        try:
            for filename in list(self.DEPLOY_REQUIRED_FILES.values()) + ['manifest.yaml']:
                source_path = os.path.join(source_dir, filename)
                if not os.path.exists(source_path):
                    raise ValueError(f'Snapshot missing required file: {filename}')
                shutil.copy2(source_path, os.path.join(temp_dir, filename))

            if os.path.isdir(active_dir):
                shutil.rmtree(active_dir)
            os.rename(temp_dir, active_dir)
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise

        return active_dir

    @staticmethod
    def _sanitize_upload_filename(filename: str, default_name: str = 'uploaded_model') -> str:
        base = os.path.basename(str(filename or '').strip())
        cleaned = re.sub(r'[^a-zA-Z0-9._-]+', '_', base).strip('._')
        return cleaned or default_name

    def _decode_uploaded_urdf_text(self, content: bytes) -> str:
        if not isinstance(content, (bytes, bytearray)):
            raise ValueError('Uploaded content must be bytes')
        size = len(content)
        if size <= 0:
            raise ValueError('Uploaded file is empty')
        if size > int(self.URDF_MAX_UPLOAD_BYTES):
            raise ValueError(f'Uploaded file exceeds {self.URDF_MAX_UPLOAD_BYTES} bytes')
        if b'\x00' in content:
            raise ValueError('Binary files are not accepted')
        try:
            decoded = bytes(content).decode('utf-8')
        except UnicodeDecodeError as exc:
            raise ValueError(f'Upload must be UTF-8 text: {exc}') from exc
        if not decoded.strip():
            raise ValueError('Uploaded file has no XML content')
        return decoded

    def _ui_relative_path(self, absolute_path: str) -> str:
        raw = str(absolute_path or '').strip()
        if not raw:
            return ''
        abs_path = os.path.abspath(os.path.expanduser(raw))
        base = os.path.abspath(self.ui_root)
        prefix = f'{base}{os.sep}'
        if abs_path == base:
            return '.'
        if abs_path.startswith(prefix):
            return os.path.relpath(abs_path, base)
        return abs_path

    def _list_description_urdf_sources(self) -> List[str]:
        root = os.path.abspath(os.path.expanduser(str(getattr(self, 'description_dir', '') or '')))
        if not root or not os.path.isdir(root):
            return []

        allowed_ext = {'.urdf', '.xacro', '.xml'}
        sources: List[str] = []
        for dirpath, _, filenames in os.walk(root):
            for filename in filenames:
                ext = os.path.splitext(str(filename or '').strip())[1].lower()
                if ext not in allowed_ext:
                    continue
                absolute = os.path.join(dirpath, filename)
                rel_path = self._ui_relative_path(absolute)
                if rel_path:
                    sources.append(rel_path)

        deduped = sorted(set([str(path or '').strip() for path in sources if str(path or '').strip()]))
        return deduped

    def _default_description_urdf_source(self) -> str:
        preferred = os.path.join(self.description_dir, 'robot.urdf.xacro')
        if os.path.exists(preferred):
            return self._ui_relative_path(preferred)

        available = self._list_description_urdf_sources()
        if available:
            return str(available[0] or '').strip()
        return ''

    def _resolve_package_uri_path(self, source_path: str) -> str:
        uri = str(source_path or '').strip()
        prefix = 'package://'
        if not uri.startswith(prefix):
            return ''

        body = uri[len(prefix):].strip().lstrip('/')
        if not body:
            return ''

        package_name, _, remainder = body.partition('/')
        package_name = str(package_name or '').strip()
        remainder = str(remainder or '').strip()
        if not package_name:
            return ''

        candidates: List[str] = []
        try:
            package_share = get_package_share_directory(package_name)
            candidates.append(os.path.join(package_share, remainder) if remainder else package_share)
        except PackageNotFoundError:
            pass
        except Exception:
            pass

        # Prefer live workspace files for active bringup package (with legacy alias).
        if remainder:
            active_bringup = str(getattr(self, 'bringup_package', '') or '').strip()
            if package_name in {active_bringup, _DEFAULT_BRINGUP_PACKAGE}:
                candidates.append(os.path.join(self.ui_root, remainder))

        seen = set()
        for candidate in candidates:
            absolute = os.path.abspath(os.path.expanduser(str(candidate or '').strip()))
            if not absolute or absolute in seen:
                continue
            seen.add(absolute)
            if os.path.exists(absolute):
                return absolute
        return ''

    def _resolve_urdf_source_path(self, source_path: str, *, robot_id: str = '') -> str:
        configured = str(source_path or '').strip()
        if not configured:
            configured = self._default_description_urdf_source()
        if not configured:
            return ''

        if configured.startswith('package://'):
            package_resolved = self._resolve_package_uri_path(configured)
            if package_resolved:
                return package_resolved

        candidates: List[str] = []
        expanded = os.path.expanduser(configured)
        if os.path.isabs(expanded):
            candidates.append(expanded)
        else:
            candidates.append(os.path.join(self.ui_root, expanded))
            candidates.append(os.path.join(self.description_dir, expanded))
            candidates.append(os.path.join(self.robot_profiles_dir, expanded))
            profile_path = self.robot_profile_files.get(str(robot_id or '').strip(), '')
            if profile_path:
                candidates.append(os.path.join(os.path.dirname(profile_path), expanded))

        seen = set()
        for candidate in candidates:
            absolute = os.path.abspath(os.path.expanduser(str(candidate or '').strip()))
            if not absolute or absolute in seen:
                continue
            seen.add(absolute)
            if os.path.exists(absolute):
                return absolute
        return ''

    def _effective_profile_urdf_source(self, profile: Dict[str, Any], *, robot_id: str = '') -> Tuple[str, str]:
        configured = str(profile.get('urdf_source', '') if isinstance(profile, dict) else '').strip()
        if not configured:
            configured = self._default_description_urdf_source()
        resolved = self._resolve_urdf_source_path(configured, robot_id=robot_id)
        return configured, resolved

    def _is_description_source_path(self, absolute_path: str) -> bool:
        target = os.path.abspath(os.path.expanduser(str(absolute_path or '').strip()))
        root = os.path.abspath(os.path.expanduser(str(getattr(self, 'description_dir', '') or '')))
        if not target or not root:
            return False
        return target == root or target.startswith(f'{root}{os.sep}')

    def _compile_urdf_or_xacro(self, filename: str, raw_text: str) -> Dict[str, Any]:
        source_filename = self._sanitize_upload_filename(filename, default_name='uploaded_model')
        ext = os.path.splitext(source_filename)[1].lower()
        source_type = 'xacro' if ext == '.xacro' else 'urdf'
        lower_text = str(raw_text or '').lower()
        if 'xmlns:xacro=' in lower_text or '<xacro:' in lower_text:
            source_type = 'xacro'
        if source_type == 'urdf' and ext not in {'.urdf', '.xml'}:
            source_type = 'xacro' if ext == '.xacro' else source_type

        if source_type != 'xacro':
            return {
                'ok': True,
                'source_type': 'urdf',
                'source_filename': source_filename,
                'compiled_urdf': str(raw_text or ''),
                'message': 'URDF accepted',
            }

        if '<xacro:include' in lower_text:
            return {
                'ok': False,
                'source_type': 'xacro',
                'source_filename': source_filename,
                'message': 'xacro include statements are blocked for uploaded files',
            }

        tmp_dir = tempfile.mkdtemp(prefix='urdf_upload_')
        if ext != '.xacro':
            source_filename = f'{os.path.splitext(source_filename)[0] or "uploaded_model"}.xacro'
        tmp_path = os.path.join(tmp_dir, source_filename)
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                f.write(raw_text)
            compiled_doc = xacro.process_file(tmp_path)
            compiled_urdf = compiled_doc.toprettyxml(indent='  ')
        except Exception as exc:
            return {
                'ok': False,
                'source_type': 'xacro',
                'source_filename': source_filename,
                'message': f'Xacro compilation failed: {exc}',
            }
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            try:
                os.rmdir(tmp_dir)
            except Exception:
                pass

        return {
            'ok': True,
            'source_type': 'xacro',
            'source_filename': source_filename,
            'compiled_urdf': compiled_urdf,
            'message': 'Xacro compiled successfully',
        }

    def _validate_compiled_urdf(
        self,
        compiled_urdf: str,
        *,
        base_frame: str,
        sensor_frames: Dict[str, str],
    ) -> Dict[str, Any]:
        validation: Dict[str, Any] = {
            'valid': False,
            'parse_ok': False,
            'tree_connected': False,
            'base_link_exists': False,
            'required_frames_ok': False,
            'message': '',
            'required_frames': [],
            'missing_required_frames': [],
            'optional_missing_frames': [],
            'unknown_joint_links': [],
            'roots': [],
            'link_count': 0,
            'joint_count': 0,
        }

        try:
            root = ET.fromstring(str(compiled_urdf or '').strip())
        except Exception as exc:
            validation['message'] = f'URDF parse failed: {exc}'
            return validation

        if root.tag != 'robot':
            validation['message'] = f'Unexpected root tag "{root.tag}" (expected "robot")'
            return validation

        links: List[str] = []
        duplicates: List[str] = []
        seen_links = set()
        for link_elem in root.findall('.//link'):
            name = str(link_elem.attrib.get('name', '') or '').strip()
            if not name:
                continue
            links.append(name)
            if name in seen_links:
                duplicates.append(name)
            seen_links.add(name)

        link_set = set(links)
        validation['link_count'] = len(link_set)

        joints: List[Dict[str, str]] = []
        for joint_elem in root.findall('.//joint'):
            parent_elem = joint_elem.find('parent')
            child_elem = joint_elem.find('child')
            parent = str(parent_elem.attrib.get('link', '') if parent_elem is not None else '').strip()
            child = str(child_elem.attrib.get('link', '') if child_elem is not None else '').strip()
            if parent and child:
                joints.append({'parent': parent, 'child': child})
        validation['joint_count'] = len(joints)

        unknown_joint_links: List[str] = []
        adjacency: Dict[str, set] = {name: set() for name in link_set}
        children = set()
        for joint in joints:
            parent = joint['parent']
            child = joint['child']
            children.add(child)
            if parent not in link_set:
                unknown_joint_links.append(parent)
            if child not in link_set:
                unknown_joint_links.append(child)
            if parent in adjacency and child in adjacency:
                adjacency[parent].add(child)
                adjacency[child].add(parent)
        validation['unknown_joint_links'] = sorted(set(unknown_joint_links))

        roots = sorted([name for name in link_set if name not in children])
        validation['roots'] = roots

        base_link = str(base_frame or '').strip()
        required_frames: List[str] = []
        if base_link:
            required_frames.append(base_link)
        for key in self.URDF_REQUIRED_SENSOR_FRAME_KEYS:
            frame_name = str(sensor_frames.get(key, '') or '').strip()
            if frame_name:
                required_frames.append(frame_name)
        required_frames = sorted(set(required_frames))
        missing_required_frames = sorted([name for name in required_frames if name not in link_set])

        optional_missing_frames: List[str] = []
        for key in self.URDF_OPTIONAL_SENSOR_FRAME_KEYS:
            frame_name = str(sensor_frames.get(key, '') or '').strip()
            if frame_name and frame_name not in link_set:
                optional_missing_frames.append(frame_name)

        validation['required_frames'] = required_frames
        validation['missing_required_frames'] = missing_required_frames
        validation['optional_missing_frames'] = sorted(set(optional_missing_frames))

        connected = False
        if link_set:
            seed = base_link if base_link in link_set else next(iter(link_set))
            stack = [seed]
            visited = set()
            while stack:
                cur = stack.pop()
                if cur in visited:
                    continue
                visited.add(cur)
                stack.extend([name for name in adjacency.get(cur, set()) if name not in visited])
            connected = len(visited) == len(link_set)

        validation['parse_ok'] = True
        validation['tree_connected'] = bool(connected)
        validation['base_link_exists'] = bool(base_link and base_link in link_set)
        validation['required_frames_ok'] = len(missing_required_frames) == 0

        valid = (
            validation['parse_ok']
            and validation['tree_connected']
            and validation['base_link_exists']
            and validation['required_frames_ok']
            and len(validation['unknown_joint_links']) == 0
            and len(duplicates) == 0
            and validation['link_count'] > 0
        )
        validation['valid'] = bool(valid)

        notes: List[str] = []
        if duplicates:
            notes.append(f'duplicate link names: {", ".join(sorted(set(duplicates)))}')
        if validation['unknown_joint_links']:
            notes.append(f'joint references unknown links: {", ".join(validation["unknown_joint_links"])}')
        if not validation['tree_connected']:
            notes.append('link tree is disconnected')
        if not validation['base_link_exists']:
            notes.append(f'base link "{base_link}" not found')
        if missing_required_frames:
            notes.append(f'missing required frames: {", ".join(missing_required_frames)}')
        if validation['optional_missing_frames']:
            notes.append(f'optional frames absent from URDF: {", ".join(validation["optional_missing_frames"])}')

        validation['message'] = 'URDF valid' if valid else '; '.join(notes) or 'URDF validation failed'
        return validation

    def _managed_rsp_node_name(self) -> str:
        return 'robot_state_publisher_profile'

    def _stop_managed_robot_state_publisher(self):
        proc = getattr(self, 'managed_rsp_process', None)
        if proc is None:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except Exception:
                    proc.kill()
        except Exception:
            pass
        finally:
            self.managed_rsp_process = None
            self.managed_rsp_profile_id = ''
            self.managed_rsp_param_file = ''

    def _collect_tf_frames(self, timeout_sec: float = 2.5) -> List[str]:
        frames = set()
        lock = threading.Lock()

        def _tf_callback(msg: TFMessage):
            with lock:
                for transform in msg.transforms:
                    parent = str(transform.header.frame_id or '').strip().lstrip('/')
                    child = str(transform.child_frame_id or '').strip().lstrip('/')
                    if parent:
                        frames.add(parent)
                    if child:
                        frames.add(child)

        static_qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=20,
        )
        sub_tf = self.create_subscription(TFMessage, '/tf', _tf_callback, 20)
        sub_tf_static = self.create_subscription(TFMessage, '/tf_static', _tf_callback, static_qos)

        end_time = time.time() + max(0.2, float(timeout_sec))
        while time.time() < end_time:
            time.sleep(0.05)

        try:
            self.destroy_subscription(sub_tf)
        except Exception:
            pass
        try:
            self.destroy_subscription(sub_tf_static)
        except Exception:
            pass

        return sorted(frames)

    @staticmethod
    def _normalize_frame_name(name: Any) -> str:
        return str(name or '').strip().lstrip('/')

    @staticmethod
    def _parse_float_triplet(raw: Any, default: Tuple[float, float, float] = (0.0, 0.0, 0.0)) -> Tuple[float, float, float]:
        text = str(raw or '').replace(',', ' ')
        parts = [part for part in text.split() if part]
        values: List[float] = []
        for idx in range(3):
            if idx < len(parts):
                try:
                    values.append(float(parts[idx]))
                    continue
                except Exception:
                    pass
            values.append(float(default[idx]))
        return values[0], values[1], values[2]

    @staticmethod
    def _transform_identity() -> List[List[float]]:
        return [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0],
        ]

    @staticmethod
    def _rotation_from_rpy(roll: float, pitch: float, yaw: float) -> List[List[float]]:
        cr = math.cos(roll)
        sr = math.sin(roll)
        cp = math.cos(pitch)
        sp = math.sin(pitch)
        cy = math.cos(yaw)
        sy = math.sin(yaw)
        return [
            [cy * cp, (cy * sp * sr) - (sy * cr), (cy * sp * cr) + (sy * sr)],
            [sy * cp, (sy * sp * sr) + (cy * cr), (sy * sp * cr) - (cy * sr)],
            [-sp, cp * sr, cp * cr],
        ]

    @staticmethod
    def _rotation_from_quaternion(x: float, y: float, z: float, w: float) -> List[List[float]]:
        xx = x * x
        yy = y * y
        zz = z * z
        xy = x * y
        xz = x * z
        yz = y * z
        wx = w * x
        wy = w * y
        wz = w * z
        return [
            [1.0 - (2.0 * (yy + zz)), 2.0 * (xy - wz), 2.0 * (xz + wy)],
            [2.0 * (xy + wz), 1.0 - (2.0 * (xx + zz)), 2.0 * (yz - wx)],
            [2.0 * (xz - wy), 2.0 * (yz + wx), 1.0 - (2.0 * (xx + yy))],
        ]

    @classmethod
    def _transform_from_xyz_rpy(cls, x: float, y: float, z: float, roll: float, pitch: float, yaw: float) -> List[List[float]]:
        rot = cls._rotation_from_rpy(roll, pitch, yaw)
        return [
            [rot[0][0], rot[0][1], rot[0][2], float(x)],
            [rot[1][0], rot[1][1], rot[1][2], float(y)],
            [rot[2][0], rot[2][1], rot[2][2], float(z)],
            [0.0, 0.0, 0.0, 1.0],
        ]

    @classmethod
    def _transform_from_xyz_quaternion(cls, x: float, y: float, z: float, qx: float, qy: float, qz: float, qw: float) -> List[List[float]]:
        rot = cls._rotation_from_quaternion(qx, qy, qz, qw)
        return [
            [rot[0][0], rot[0][1], rot[0][2], float(x)],
            [rot[1][0], rot[1][1], rot[1][2], float(y)],
            [rot[2][0], rot[2][1], rot[2][2], float(z)],
            [0.0, 0.0, 0.0, 1.0],
        ]

    @staticmethod
    def _transform_multiply(a: List[List[float]], b: List[List[float]]) -> List[List[float]]:
        result = [[0.0] * 4 for _ in range(4)]
        for row in range(4):
            for col in range(4):
                result[row][col] = (
                    a[row][0] * b[0][col]
                    + a[row][1] * b[1][col]
                    + a[row][2] * b[2][col]
                    + a[row][3] * b[3][col]
                )
        return result

    @staticmethod
    def _transform_inverse(transform: List[List[float]]) -> List[List[float]]:
        rot = [
            [transform[0][0], transform[0][1], transform[0][2]],
            [transform[1][0], transform[1][1], transform[1][2]],
            [transform[2][0], transform[2][1], transform[2][2]],
        ]
        trans = [transform[0][3], transform[1][3], transform[2][3]]
        rot_t = [
            [rot[0][0], rot[1][0], rot[2][0]],
            [rot[0][1], rot[1][1], rot[2][1]],
            [rot[0][2], rot[1][2], rot[2][2]],
        ]
        inv_trans = [
            -(rot_t[0][0] * trans[0] + rot_t[0][1] * trans[1] + rot_t[0][2] * trans[2]),
            -(rot_t[1][0] * trans[0] + rot_t[1][1] * trans[1] + rot_t[1][2] * trans[2]),
            -(rot_t[2][0] * trans[0] + rot_t[2][1] * trans[1] + rot_t[2][2] * trans[2]),
        ]
        return [
            [rot_t[0][0], rot_t[0][1], rot_t[0][2], inv_trans[0]],
            [rot_t[1][0], rot_t[1][1], rot_t[1][2], inv_trans[1]],
            [rot_t[2][0], rot_t[2][1], rot_t[2][2], inv_trans[2]],
            [0.0, 0.0, 0.0, 1.0],
        ]

    @staticmethod
    def _transform_xy_yaw(transform: List[List[float]]) -> Tuple[float, float, float]:
        x = float(transform[0][3])
        y = float(transform[1][3])
        yaw = math.atan2(float(transform[1][0]), float(transform[0][0]))
        return x, y, yaw

    @classmethod
    def _resolve_relative_transforms(
        cls,
        reference_frame: str,
        pair_transforms: Dict[Tuple[str, str], List[List[float]]],
    ) -> Dict[str, List[List[float]]]:
        ref = cls._normalize_frame_name(reference_frame)
        if not ref or not isinstance(pair_transforms, dict) or not pair_transforms:
            return {}

        adjacency: Dict[str, List[Tuple[str, List[List[float]]]]] = {}
        for (parent, child), tf_matrix in pair_transforms.items():
            p = cls._normalize_frame_name(parent)
            c = cls._normalize_frame_name(child)
            if not p or not c:
                continue
            adjacency.setdefault(p, []).append((c, tf_matrix))
            adjacency.setdefault(c, []).append((p, cls._transform_inverse(tf_matrix)))

        if ref not in adjacency:
            return {}

        visited: Dict[str, List[List[float]]] = {ref: cls._transform_identity()}
        queue = [ref]
        idx = 0
        while idx < len(queue):
            cur = queue[idx]
            idx += 1
            cur_tf = visited[cur]
            for nxt, edge_tf in adjacency.get(cur, []):
                if nxt in visited:
                    continue
                visited[nxt] = cls._transform_multiply(cur_tf, edge_tf)
                queue.append(nxt)

        return visited

    def _collect_tf_tree(self, timeout_sec: float = 1.2) -> Dict[str, Any]:
        lock = threading.Lock()
        frame_names: Set[str] = set()
        edge_stats: Dict[Tuple[str, str], Dict[str, Any]] = {}
        pair_transforms: Dict[Tuple[str, str], List[List[float]]] = {}

        def _consume_tf(msg: TFMessage, *, is_static: bool):
            with lock:
                for transform in msg.transforms:
                    parent = self._normalize_frame_name(transform.header.frame_id)
                    child = self._normalize_frame_name(transform.child_frame_id)
                    if not parent or not child:
                        continue

                    frame_names.add(parent)
                    frame_names.add(child)

                    pair = (parent, child)
                    pair_transforms[pair] = self._transform_from_xyz_quaternion(
                        float(transform.transform.translation.x),
                        float(transform.transform.translation.y),
                        float(transform.transform.translation.z),
                        float(transform.transform.rotation.x),
                        float(transform.transform.rotation.y),
                        float(transform.transform.rotation.z),
                        float(transform.transform.rotation.w),
                    )

                    entry = edge_stats.get(pair)
                    if not entry:
                        entry = {
                            'parent': parent,
                            'child': child,
                            'static_seen': False,
                            'dynamic_seen': False,
                            'samples': 0,
                        }

                    if is_static:
                        entry['static_seen'] = True
                    else:
                        entry['dynamic_seen'] = True
                    entry['samples'] = int(entry.get('samples', 0) or 0) + 1
                    edge_stats[pair] = entry

        static_qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=20,
        )
        sub_tf = self.create_subscription(TFMessage, '/tf', lambda msg: _consume_tf(msg, is_static=False), 20)
        sub_tf_static = self.create_subscription(TFMessage, '/tf_static', lambda msg: _consume_tf(msg, is_static=True), static_qos)

        end_time = time.time() + max(0.2, float(timeout_sec))
        while time.time() < end_time:
            time.sleep(0.05)

        try:
            self.destroy_subscription(sub_tf)
        except Exception:
            pass
        try:
            self.destroy_subscription(sub_tf_static)
        except Exception:
            pass

        edges: List[Dict[str, Any]] = []
        for pair in sorted(edge_stats.keys(), key=lambda item: (item[0], item[1])):
            entry = edge_stats[pair]
            static_seen = bool(entry.get('static_seen'))
            dynamic_seen = bool(entry.get('dynamic_seen'))
            edges.append(
                {
                    'parent': str(entry.get('parent', '')),
                    'child': str(entry.get('child', '')),
                    'static_seen': static_seen,
                    'dynamic_seen': dynamic_seen,
                    'static': bool(static_seen and not dynamic_seen),
                    'dynamic': bool(dynamic_seen),
                    'samples': int(entry.get('samples', 0) or 0),
                }
            )

        return {
            'edges': edges,
            'transforms': pair_transforms,
            'frames': sorted(frame_names),
        }

    @staticmethod
    def _collect_profile_key_frames(profile: Dict[str, Any], base_frame: str) -> List[str]:
        frames: List[str] = []
        seen: Set[str] = set()

        def _append(name: Any):
            frame = RosBridge._normalize_frame_name(name)
            if not frame or frame in seen:
                return
            seen.add(frame)
            frames.append(frame)

        _append(base_frame)
        _append('base_footprint')

        sensor_frames = profile.get('sensor_frames', {}) if isinstance(profile.get('sensor_frames', {}), dict) else {}
        for key in ('laser', 'camera', 'odom', 'map'):
            _append(sensor_frames.get(key, ''))

        return frames

    def _load_compiled_urdf_artifact(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        artifact = profile.get('urdf_artifact', {}) if isinstance(profile.get('urdf_artifact', {}), dict) else {}
        compiled_urdf_path = str(artifact.get('compiled_urdf_path', '') or '').strip()
        source_type = str(artifact.get('source_type', '') or '').strip().lower()
        source_path = str(artifact.get('source_path', '') or '').strip()

        robot_id = str(profile.get('robot_id', '') or '').strip()
        urdf_source, resolved_source = self._effective_profile_urdf_source(profile, robot_id=robot_id)
        source_error = ''
        if urdf_source and resolved_source:
            ext = os.path.splitext(resolved_source)[1].lower()
            try:
                if ext == '.xacro':
                    compiled_doc = xacro.process_file(resolved_source)
                    compiled_urdf = compiled_doc.toprettyxml(indent='  ')
                    source_kind = 'xacro'
                else:
                    with open(resolved_source, 'r', encoding='utf-8') as f:
                        compiled_urdf = str(f.read() or '')
                    source_kind = 'urdf'
            except Exception as exc:
                source_error = f'Failed loading URDF source "{urdf_source}": {exc}'
            else:
                if compiled_urdf.strip():
                    return self._ok(
                        True,
                        f'Loaded URDF source from "{urdf_source}"',
                        compiled_urdf=compiled_urdf,
                        compiled_urdf_path='',
                        source_type=source_kind,
                        source_path=urdf_source,
                    )
                source_error = f'URDF source "{urdf_source}" resolved but content is empty'
        elif urdf_source and not resolved_source:
            source_error = f'urdf_source "{urdf_source}" is not readable'

        if compiled_urdf_path:
            resolved = self._artifact_absolute_path(compiled_urdf_path)
            if resolved and os.path.exists(resolved):
                try:
                    with open(resolved, 'r', encoding='utf-8') as f:
                        compiled = str(f.read() or '')
                except Exception as exc:
                    return self._ok(False, f'Failed reading compiled URDF artifact: {exc}')
                if not compiled.strip():
                    return self._ok(False, 'Compiled URDF artifact is empty')
                message = 'Loaded compiled URDF artifact'
                if source_error:
                    message = f'{message} (source warning: {source_error})'
                return self._ok(
                    True,
                    message,
                    compiled_urdf=compiled,
                    compiled_urdf_path=compiled_urdf_path,
                    source_type=source_type or 'urdf',
                    source_path=source_path,
                )

        if source_error:
            return self._ok(False, f'No compiled URDF artifact available. {source_error}')
        return self._ok(False, 'No compiled URDF artifact found and no URDF source is configured')

    def _parse_urdf_visualization_model(self, compiled_urdf: str, *, base_frame: str) -> Dict[str, Any]:
        try:
            root = ET.fromstring(str(compiled_urdf or '').strip())
        except Exception as exc:
            return self._ok(False, f'URDF parse failed: {exc}')

        if root.tag != 'robot':
            return self._ok(False, f'Unexpected root tag "{root.tag}" (expected "robot")')

        link_collisions: Dict[str, List[Dict[str, Any]]] = {}
        link_names: Set[str] = set()
        for link_elem in root.findall('./link'):
            link_name = self._normalize_frame_name(link_elem.attrib.get('name', ''))
            if not link_name:
                continue
            link_names.add(link_name)
            link_collisions.setdefault(link_name, [])

            for idx, collision_elem in enumerate(link_elem.findall('./collision')):
                collision_name = self._normalize_frame_name(collision_elem.attrib.get('name', f'collision_{idx + 1}'))
                origin_elem = collision_elem.find('origin')
                xyz = self._parse_float_triplet(origin_elem.attrib.get('xyz', '0 0 0') if origin_elem is not None else '0 0 0')
                rpy = self._parse_float_triplet(origin_elem.attrib.get('rpy', '0 0 0') if origin_elem is not None else '0 0 0')
                origin_tf = self._transform_from_xyz_rpy(xyz[0], xyz[1], xyz[2], rpy[0], rpy[1], rpy[2])

                geometry_elem = collision_elem.find('geometry')
                if geometry_elem is None:
                    continue

                shape_type = ''
                size_x = 0.0
                size_y = 0.0

                box_elem = geometry_elem.find('box')
                cyl_elem = geometry_elem.find('cylinder')
                sphere_elem = geometry_elem.find('sphere')
                mesh_elem = geometry_elem.find('mesh')

                if box_elem is not None:
                    box_size = self._parse_float_triplet(box_elem.attrib.get('size', '0 0 0'))
                    size_x = abs(float(box_size[0]))
                    size_y = abs(float(box_size[1]))
                    shape_type = 'box'
                elif cyl_elem is not None:
                    try:
                        radius = abs(float(cyl_elem.attrib.get('radius', '0') or 0.0))
                    except Exception:
                        radius = 0.0
                    diameter = 2.0 * radius
                    size_x = diameter
                    size_y = diameter
                    shape_type = 'cylinder'
                elif sphere_elem is not None:
                    try:
                        radius = abs(float(sphere_elem.attrib.get('radius', '0') or 0.0))
                    except Exception:
                        radius = 0.0
                    diameter = 2.0 * radius
                    size_x = diameter
                    size_y = diameter
                    shape_type = 'sphere'
                elif mesh_elem is not None:
                    mesh_scale = self._parse_float_triplet(mesh_elem.attrib.get('scale', '1 1 1'), default=(1.0, 1.0, 1.0))
                    # Mesh bounds are not available from raw URDF, so use conservative placeholders scaled from mesh scale.
                    size_x = max(0.10, abs(float(mesh_scale[0])) * 0.35)
                    size_y = max(0.10, abs(float(mesh_scale[1])) * 0.35)
                    shape_type = 'mesh_bbox'

                if size_x <= 0.0 or size_y <= 0.0:
                    continue

                link_collisions[link_name].append(
                    {
                        'name': collision_name,
                        'origin_transform': origin_tf,
                        'geometry_type': shape_type,
                        'size_x': size_x,
                        'size_y': size_y,
                    }
                )

        internal_joints: List[Dict[str, Any]] = []
        urdf_edges: List[Dict[str, Any]] = []
        for joint_elem in root.findall('./joint'):
            parent_elem = joint_elem.find('parent')
            child_elem = joint_elem.find('child')
            parent = self._normalize_frame_name(parent_elem.attrib.get('link', '') if parent_elem is not None else '')
            child = self._normalize_frame_name(child_elem.attrib.get('link', '') if child_elem is not None else '')
            if not parent or not child:
                continue

            origin_elem = joint_elem.find('origin')
            xyz = self._parse_float_triplet(origin_elem.attrib.get('xyz', '0 0 0') if origin_elem is not None else '0 0 0')
            rpy = self._parse_float_triplet(origin_elem.attrib.get('rpy', '0 0 0') if origin_elem is not None else '0 0 0')
            joint_tf = self._transform_from_xyz_rpy(xyz[0], xyz[1], xyz[2], rpy[0], rpy[1], rpy[2])

            joint_type = str(joint_elem.attrib.get('type', 'fixed') or 'fixed').strip().lower()
            is_static = joint_type in {'fixed'}

            internal_joints.append(
                {
                    'parent': parent,
                    'child': child,
                    'transform': joint_tf,
                    'joint_type': joint_type,
                    'static': is_static,
                }
            )
            urdf_edges.append(
                {
                    'parent': parent,
                    'child': child,
                    'joint_type': joint_type,
                    'static': bool(is_static),
                    'dynamic': bool(not is_static),
                    'source': 'urdf',
                }
            )

            link_names.add(parent)
            link_names.add(child)

        if not link_names:
            return self._ok(False, 'URDF contains no links')

        adjacency: Dict[str, List[Tuple[str, List[List[float]]]]] = {name: [] for name in link_names}
        for joint in internal_joints:
            parent = str(joint.get('parent', ''))
            child = str(joint.get('child', ''))
            tf_matrix = joint.get('transform')
            if not parent or not child or not isinstance(tf_matrix, list):
                continue
            adjacency.setdefault(parent, []).append((child, tf_matrix))
            adjacency.setdefault(child, []).append((parent, self._transform_inverse(tf_matrix)))

        requested_base = self._normalize_frame_name(base_frame)
        reference_frame = requested_base
        warnings: List[str] = []
        if reference_frame not in link_names:
            fallback = sorted(link_names)[0]
            if reference_frame:
                warnings.append(f'Base frame "{reference_frame}" not found in URDF; using "{fallback}" as reference.')
            reference_frame = fallback

        relative_transforms: Dict[str, List[List[float]]] = {reference_frame: self._transform_identity()}
        queue = [reference_frame]
        idx = 0
        while idx < len(queue):
            cur = queue[idx]
            idx += 1
            cur_tf = relative_transforms[cur]
            for child, edge_tf in adjacency.get(cur, []):
                if child in relative_transforms:
                    continue
                relative_transforms[child] = self._transform_multiply(cur_tf, edge_tf)
                queue.append(child)

        bounds_min_x = float('inf')
        bounds_min_y = float('inf')
        bounds_max_x = float('-inf')
        bounds_max_y = float('-inf')
        shapes: List[Dict[str, Any]] = []
        total_shapes = 0
        truncated = False

        for link_name in sorted(link_collisions.keys()):
            if link_name not in relative_transforms:
                continue
            link_tf = relative_transforms[link_name]
            for collision in link_collisions.get(link_name, []):
                total_shapes += 1
                if len(shapes) >= int(self.URDF_VISUAL_MAX_SHAPES):
                    truncated = True
                    continue

                shape_tf = self._transform_multiply(link_tf, collision['origin_transform'])
                center_x, center_y, yaw = self._transform_xy_yaw(shape_tf)
                size_x = float(collision.get('size_x', 0.0) or 0.0)
                size_y = float(collision.get('size_y', 0.0) or 0.0)
                if size_x <= 0.0 or size_y <= 0.0:
                    continue

                half_x = size_x * 0.5
                half_y = size_y * 0.5
                corners_local = [(-half_x, -half_y), (half_x, -half_y), (half_x, half_y), (-half_x, half_y)]
                cos_yaw = math.cos(yaw)
                sin_yaw = math.sin(yaw)
                points: List[Dict[str, float]] = []
                for local_x, local_y in corners_local:
                    world_x = center_x + (local_x * cos_yaw) - (local_y * sin_yaw)
                    world_y = center_y + (local_x * sin_yaw) + (local_y * cos_yaw)
                    bounds_min_x = min(bounds_min_x, world_x)
                    bounds_max_x = max(bounds_max_x, world_x)
                    bounds_min_y = min(bounds_min_y, world_y)
                    bounds_max_y = max(bounds_max_y, world_y)
                    points.append({'x': round(world_x, 4), 'y': round(world_y, 4)})

                shapes.append(
                    {
                        'link': link_name,
                        'name': str(collision.get('name', '') or ''),
                        'geometry_type': str(collision.get('geometry_type', '') or ''),
                        'center': {'x': round(center_x, 4), 'y': round(center_y, 4)},
                        'size': {'x': round(size_x, 4), 'y': round(size_y, 4)},
                        'yaw': round(yaw, 4),
                        'points': points,
                    }
                )

        if not shapes:
            bounds_min_x = -0.5
            bounds_max_x = 0.5
            bounds_min_y = -0.5
            bounds_max_y = 0.5

        bounds = {
            'min_x': round(bounds_min_x, 4),
            'max_x': round(bounds_max_x, 4),
            'min_y': round(bounds_min_y, 4),
            'max_y': round(bounds_max_y, 4),
            'width': round(max(0.01, bounds_max_x - bounds_min_x), 4),
            'height': round(max(0.01, bounds_max_y - bounds_min_y), 4),
        }

        message = 'Parsed URDF geometry'
        if warnings:
            message = f'{message}. {" ".join(warnings)}'

        return self._ok(
            True,
            message,
            requested_base_frame=requested_base,
            reference_frame=reference_frame,
            frame_names=sorted(link_names),
            relative_transforms=relative_transforms,
            urdf_edges=urdf_edges,
            top_view={
                'frame': reference_frame,
                'shapes': shapes,
                'bounds': bounds,
                'truncated': bool(truncated),
                'total_shapes': int(total_shapes),
                'rendered_shapes': len(shapes),
            },
            warnings=warnings,
        )

    @staticmethod
    def _combine_frame_tree(urdf_edges: Any, tf_edges: Any) -> Dict[str, Any]:
        merged: Dict[Tuple[str, str], Dict[str, Any]] = {}

        if isinstance(urdf_edges, list):
            for edge in urdf_edges:
                if not isinstance(edge, dict):
                    continue
                parent = RosBridge._normalize_frame_name(edge.get('parent', ''))
                child = RosBridge._normalize_frame_name(edge.get('child', ''))
                if not parent or not child:
                    continue
                merged[(parent, child)] = {
                    'parent': parent,
                    'child': child,
                    'has_urdf': True,
                    'urdf_static': bool(edge.get('static', False)),
                    'tf_static_seen': False,
                    'tf_dynamic_seen': False,
                }

        if isinstance(tf_edges, list):
            for edge in tf_edges:
                if not isinstance(edge, dict):
                    continue
                parent = RosBridge._normalize_frame_name(edge.get('parent', ''))
                child = RosBridge._normalize_frame_name(edge.get('child', ''))
                if not parent or not child:
                    continue
                key = (parent, child)
                entry = merged.get(
                    key,
                    {
                        'parent': parent,
                        'child': child,
                        'has_urdf': False,
                        'urdf_static': False,
                        'tf_static_seen': False,
                        'tf_dynamic_seen': False,
                    },
                )
                entry['tf_static_seen'] = bool(entry.get('tf_static_seen', False) or edge.get('static_seen', False) or edge.get('static', False))
                entry['tf_dynamic_seen'] = bool(entry.get('tf_dynamic_seen', False) or edge.get('dynamic_seen', False) or edge.get('dynamic', False))
                merged[key] = entry

        edge_list: List[Dict[str, Any]] = []
        frame_names: Set[str] = set()
        parents_by_frame: Dict[str, Set[str]] = {}
        children_by_frame: Dict[str, Set[str]] = {}
        dynamic_frames: Set[str] = set()
        static_frames: Set[str] = set()
        sources_by_frame: Dict[str, Set[str]] = {}

        for key in sorted(merged.keys(), key=lambda item: (item[0], item[1])):
            entry = merged[key]
            parent = str(entry.get('parent', ''))
            child = str(entry.get('child', ''))
            if not parent or not child:
                continue

            has_urdf = bool(entry.get('has_urdf', False))
            urdf_static = bool(entry.get('urdf_static', False))
            tf_static_seen = bool(entry.get('tf_static_seen', False))
            tf_dynamic_seen = bool(entry.get('tf_dynamic_seen', False))

            if tf_dynamic_seen:
                dynamic = True
                static = False
            elif tf_static_seen:
                dynamic = False
                static = True
            elif has_urdf:
                dynamic = not urdf_static
                static = urdf_static
            else:
                dynamic = False
                static = True

            if has_urdf and (tf_static_seen or tf_dynamic_seen):
                source = 'urdf+tf'
            elif has_urdf:
                source = 'urdf'
            elif tf_static_seen and not tf_dynamic_seen:
                source = 'tf_static'
            else:
                source = 'tf'

            edge_list.append(
                {
                    'parent': parent,
                    'child': child,
                    'static': bool(static),
                    'dynamic': bool(dynamic),
                    'source': source,
                }
            )

            frame_names.add(parent)
            frame_names.add(child)
            parents_by_frame.setdefault(child, set()).add(parent)
            children_by_frame.setdefault(parent, set()).add(child)
            sources_by_frame.setdefault(parent, set()).add(source)
            sources_by_frame.setdefault(child, set()).add(source)
            if dynamic:
                dynamic_frames.add(parent)
                dynamic_frames.add(child)
            if static:
                static_frames.add(parent)
                static_frames.add(child)

        frames: List[Dict[str, Any]] = []
        for frame in sorted(frame_names):
            parents = sorted(parents_by_frame.get(frame, set()))
            children = sorted(children_by_frame.get(frame, set()))
            is_dynamic = frame in dynamic_frames
            is_static = (frame in static_frames) and not is_dynamic
            frames.append(
                {
                    'name': frame,
                    'parent': parents[0] if len(parents) == 1 else '',
                    'parents': parents,
                    'children': children,
                    'child_count': len(children),
                    'static': bool(is_static),
                    'dynamic': bool(is_dynamic),
                    'sources': sorted(sources_by_frame.get(frame, set())),
                }
            )

        return {
            'edges': edge_list,
            'frames': frames,
            'counts': {
                'edges': len(edge_list),
                'frames': len(frames),
            },
        }

    def get_profile_urdf_visualization(self, robot_id: str = '', include_tf: bool = True):
        target_robot_id = str(robot_id or '').strip()
        if not target_robot_id:
            target_robot_id = str(self.active_robot_profile_id or '').strip()
        if not target_robot_id:
            return self._ok(False, 'No robot profile selected')

        profiles = self._load_robot_profiles()
        profile = profiles.get(target_robot_id)
        if not profile:
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        base_frame = self._normalize_frame_name(profile.get('base_frame', 'base_link')) or 'base_link'
        loaded = self._load_compiled_urdf_artifact(profile)
        if not bool(loaded.get('ok')):
            return self._ok(
                False,
                str(loaded.get('message', 'Unable to load URDF artifact') or 'Unable to load URDF artifact'),
                robot_id=target_robot_id,
                base_frame=base_frame,
                profile_version=int(profile.get('profile_version', 1) or 1),
                top_view={'frame': base_frame, 'shapes': [], 'bounds': {}, 'truncated': False, 'total_shapes': 0, 'rendered_shapes': 0},
                frame_markers=[],
                frame_tree={'edges': [], 'frames': [], 'counts': {'edges': 0, 'frames': 0}},
                source={},
                validation={},
            )

        parsed = self._parse_urdf_visualization_model(str(loaded.get('compiled_urdf', '') or ''), base_frame=base_frame)
        if not bool(parsed.get('ok')):
            return self._ok(
                False,
                str(parsed.get('message', 'Failed to parse URDF') or 'Failed to parse URDF'),
                robot_id=target_robot_id,
                base_frame=base_frame,
                profile_version=int(profile.get('profile_version', 1) or 1),
                top_view={'frame': base_frame, 'shapes': [], 'bounds': {}, 'truncated': False, 'total_shapes': 0, 'rendered_shapes': 0},
                frame_markers=[],
                frame_tree={'edges': [], 'frames': [], 'counts': {'edges': 0, 'frames': 0}},
                source={
                    'source_type': str(loaded.get('source_type', '') or ''),
                    'source_path': str(loaded.get('source_path', '') or ''),
                    'compiled_urdf_path': str(loaded.get('compiled_urdf_path', '') or ''),
                },
                validation={},
            )

        reference_frame = self._normalize_frame_name(parsed.get('reference_frame', base_frame)) or base_frame
        urdf_transforms = parsed.get('relative_transforms', {}) if isinstance(parsed.get('relative_transforms', {}), dict) else {}

        tf_tree = {'edges': [], 'transforms': {}, 'frames': []}
        if include_tf:
            tf_tree = self._collect_tf_tree(timeout_sec=1.2)
        tf_relative = self._resolve_relative_transforms(
            reference_frame,
            tf_tree.get('transforms', {}) if isinstance(tf_tree.get('transforms', {}), dict) else {},
        )

        frame_tree = self._combine_frame_tree(parsed.get('urdf_edges', []), tf_tree.get('edges', []))
        frame_modes: Dict[str, Dict[str, bool]] = {}
        for frame in frame_tree.get('frames', []):
            if not isinstance(frame, dict):
                continue
            name = self._normalize_frame_name(frame.get('name', ''))
            if not name:
                continue
            frame_modes[name] = {
                'static': bool(frame.get('static', False)),
                'dynamic': bool(frame.get('dynamic', False)),
            }

        key_frames = self._collect_profile_key_frames(profile, base_frame=reference_frame)
        frame_markers: List[Dict[str, Any]] = []
        tf_available_frames: List[str] = []
        tf_missing_frames: List[str] = []
        alignment_warnings: List[str] = []

        for frame_name in key_frames:
            tf_pose_raw = tf_relative.get(frame_name)
            urdf_pose_raw = urdf_transforms.get(frame_name)

            tf_pose: Dict[str, Any] = {'available': False}
            urdf_pose: Dict[str, Any] = {'available': False}

            if isinstance(tf_pose_raw, list):
                tfx, tfy, tfyaw = self._transform_xy_yaw(tf_pose_raw)
                tf_pose = {
                    'available': True,
                    'x': round(tfx, 4),
                    'y': round(tfy, 4),
                    'yaw': round(tfyaw, 4),
                }
                tf_available_frames.append(frame_name)
            elif include_tf:
                tf_missing_frames.append(frame_name)

            if isinstance(urdf_pose_raw, list):
                urdfx, urdfy, urdfyaw = self._transform_xy_yaw(urdf_pose_raw)
                urdf_pose = {
                    'available': True,
                    'x': round(urdfx, 4),
                    'y': round(urdfy, 4),
                    'yaw': round(urdfyaw, 4),
                }

            marker_source = 'missing'
            marker_x = 0.0
            marker_y = 0.0
            marker_yaw = 0.0
            if tf_pose.get('available'):
                marker_source = 'tf'
                marker_x = float(tf_pose.get('x', 0.0))
                marker_y = float(tf_pose.get('y', 0.0))
                marker_yaw = float(tf_pose.get('yaw', 0.0))
            elif urdf_pose.get('available'):
                marker_source = 'urdf'
                marker_x = float(urdf_pose.get('x', 0.0))
                marker_y = float(urdf_pose.get('y', 0.0))
                marker_yaw = float(urdf_pose.get('yaw', 0.0))

            delta_payload: Dict[str, Any] = {'available': False}
            if tf_pose.get('available') and urdf_pose.get('available'):
                dx = float(tf_pose.get('x', 0.0)) - float(urdf_pose.get('x', 0.0))
                dy = float(tf_pose.get('y', 0.0)) - float(urdf_pose.get('y', 0.0))
                distance = math.hypot(dx, dy)
                yaw_delta_raw = float(tf_pose.get('yaw', 0.0)) - float(urdf_pose.get('yaw', 0.0))
                yaw_delta = math.atan2(math.sin(yaw_delta_raw), math.cos(yaw_delta_raw))
                delta_payload = {
                    'available': True,
                    'dx': round(dx, 4),
                    'dy': round(dy, 4),
                    'distance': round(distance, 4),
                    'dyaw': round(yaw_delta, 4),
                }
                if distance > 0.35:
                    alignment_warnings.append(f'Frame "{frame_name}" offset is {distance:.3f} m between TF and URDF')

            mode = frame_modes.get(frame_name, {})
            frame_markers.append(
                {
                    'name': frame_name,
                    'x': round(marker_x, 4),
                    'y': round(marker_y, 4),
                    'yaw': round(marker_yaw, 4),
                    'source': marker_source,
                    'static': bool(mode.get('static', False)),
                    'dynamic': bool(mode.get('dynamic', False)),
                    'tf': tf_pose,
                    'urdf': urdf_pose,
                    'delta': delta_payload,
                }
            )

        frame_markers.sort(key=lambda item: item.get('name', ''))
        tf_available_frames = sorted(set(tf_available_frames))
        tf_missing_frames = sorted(set(tf_missing_frames))

        validation = {
            'requested_base_frame': base_frame,
            'reference_frame': reference_frame,
            'base_frame_in_urdf': bool(base_frame in set(parsed.get('frame_names', []))),
            'tf_enabled': bool(include_tf),
            'tf_observed_frames': list(tf_tree.get('frames', [])),
            'tf_available_frames': tf_available_frames,
            'tf_missing_frames': tf_missing_frames,
            'alignment_warnings': alignment_warnings,
            'warnings': list(parsed.get('warnings', [])),
            'key_frames': key_frames,
        }

        message = 'URDF visualization ready'
        if alignment_warnings:
            message = f'{message}. Alignment warnings detected for {len(alignment_warnings)} frame(s).'

        return self._ok(
            True,
            message,
            robot_id=target_robot_id,
            profile_version=int(profile.get('profile_version', 1) or 1),
            base_frame=base_frame,
            reference_frame=reference_frame,
            top_view=parsed.get('top_view', {}),
            frame_markers=frame_markers,
            frame_tree=frame_tree,
            source={
                'source_type': str(loaded.get('source_type', '') or ''),
                'source_path': str(loaded.get('source_path', '') or ''),
                'compiled_urdf_path': str(loaded.get('compiled_urdf_path', '') or ''),
            },
            validation=validation,
        )

    def _start_managed_robot_state_publisher(
        self,
        *,
        profile: Dict[str, Any],
        compiled_urdf_path: str,
    ) -> Dict[str, Any]:
        resolved_urdf = self._artifact_absolute_path(compiled_urdf_path)
        if not resolved_urdf or not os.path.exists(resolved_urdf):
            return self._ok(False, f'Compiled URDF artifact not found: {compiled_urdf_path}')

        try:
            with open(resolved_urdf, 'r', encoding='utf-8') as f:
                compiled_urdf = str(f.read() or '')
        except Exception as exc:
            return self._ok(False, f'Failed to read compiled URDF artifact: {exc}')

        if not compiled_urdf.strip():
            return self._ok(False, 'Compiled URDF artifact is empty')

        self._stop_managed_robot_state_publisher()

        node_name = self._managed_rsp_node_name()
        namespace = str(profile.get('namespace', '/') or '/').strip() or '/'
        use_sim_time = False
        try:
            if self.has_parameter('use_sim_time'):
                use_sim_time = bool(self.get_parameter('use_sim_time').value)
        except Exception:
            use_sim_time = False

        params_payload = {
            node_name: {
                'ros__parameters': {
                    'use_sim_time': bool(use_sim_time),
                    'robot_description': compiled_urdf,
                }
            }
        }

        params_file = os.path.join(os.path.dirname(resolved_urdf), 'robot_state_publisher.params.yaml')
        try:
            with open(params_file, 'w', encoding='utf-8') as f:
                yaml.safe_dump(params_payload, f, sort_keys=False)
        except Exception as exc:
            return self._ok(False, f'Failed writing robot_state_publisher params: {exc}')

        cmd = [
            'ros2',
            'run',
            'robot_state_publisher',
            'robot_state_publisher',
            '--ros-args',
            '-r',
            f'__node:={node_name}',
        ]
        if namespace not in {'', '/'}:
            cmd.extend(['-r', f'__ns:={namespace}'])
        cmd.extend(['--params-file', params_file])

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as exc:
            return self._ok(False, f'Failed to launch robot_state_publisher: {exc}')

        time.sleep(1.0)
        if proc.poll() is not None:
            return self._ok(False, 'robot_state_publisher exited immediately')

        self.managed_rsp_process = proc
        self.managed_rsp_profile_id = str(profile.get('robot_id', '') or '').strip()
        self.managed_rsp_param_file = params_file

        required_frames = [str(profile.get('base_frame', 'base_link') or 'base_link').strip()]
        sensor_frames = profile.get('sensor_frames', {}) if isinstance(profile.get('sensor_frames', {}), dict) else {}
        for key in self.URDF_REQUIRED_SENSOR_FRAME_KEYS:
            frame_name = str(sensor_frames.get(key, '') or '').strip()
            if frame_name:
                required_frames.append(frame_name)
        required_frames = sorted(set([name for name in required_frames if name]))

        observed_frames = self._collect_tf_frames(timeout_sec=2.5)
        observed_set = set(observed_frames)
        missing_tf_frames = sorted([name for name in required_frames if name not in observed_set])
        tf_ok = len(missing_tf_frames) == 0

        message = (
            f'robot_state_publisher running (pid {proc.pid}); TF frames detected'
            if tf_ok
            else f'robot_state_publisher running (pid {proc.pid}) but missing TF frames: {", ".join(missing_tf_frames)}'
        )

        return self._ok(
            tf_ok,
            message,
            node_name=node_name,
            namespace=namespace,
            pid=int(proc.pid),
            required_tf_frames=required_frames,
            observed_tf_frames=observed_frames,
            missing_tf_frames=missing_tf_frames,
            tf_frames_ok=tf_ok,
        )

    def upload_profile_urdf(
        self,
        *,
        robot_id: str,
        filename: str,
        content: bytes,
        apply_runtime: bool = True,
    ):
        profile_id = str(robot_id or '').strip()
        if not profile_id:
            return self._ok(False, 'robot_id is required')

        profiles = self._load_robot_profiles()
        profile = profiles.get(profile_id)
        if not profile:
            return self._ok(False, f'Robot profile "{profile_id}" not found')

        try:
            raw_text = self._decode_uploaded_urdf_text(content)
        except ValueError as exc:
            return self._ok(False, str(exc))

        compiled = self._compile_urdf_or_xacro(filename, raw_text)
        if not bool(compiled.get('ok')):
            return self._ok(False, str(compiled.get('message', 'URDF/Xacro compilation failed')))

        validation = self._validate_compiled_urdf(
            str(compiled.get('compiled_urdf', '') or ''),
            base_frame=str(profile.get('base_frame', 'base_link') or 'base_link'),
            sensor_frames=profile.get('sensor_frames', {}) if isinstance(profile.get('sensor_frames', {}), dict) else {},
        )
        if not bool(validation.get('valid')):
            return self._ok(False, str(validation.get('message', 'URDF validation failed')), validation=validation)

        current_version = int(profile.get('profile_version', 1) or 1)
        next_version = max(1, current_version + 1)
        artifact_dir = os.path.join(self.robot_profiles_dir, 'artifacts', self._safe_profile_slug(profile_id), f'v{next_version}')
        os.makedirs(artifact_dir, exist_ok=True)

        source_filename = self._sanitize_upload_filename(str(compiled.get('source_filename', filename) or filename), default_name='uploaded_model')
        source_ext = os.path.splitext(source_filename)[1].lower()
        source_type = str(compiled.get('source_type', 'urdf') or 'urdf').strip().lower()
        if source_type == 'xacro' and source_ext != '.xacro':
            source_filename = f'{os.path.splitext(source_filename)[0]}.xacro'
        if source_type == 'urdf' and source_ext not in {'.urdf', '.xml'}:
            source_filename = f'{os.path.splitext(source_filename)[0]}.urdf'

        source_path = os.path.join(artifact_dir, source_filename)
        compiled_path = os.path.join(artifact_dir, 'compiled.urdf')
        validation_path = os.path.join(artifact_dir, 'validation.yaml')

        try:
            with open(source_path, 'w', encoding='utf-8') as f:
                f.write(raw_text)
            with open(compiled_path, 'w', encoding='utf-8') as f:
                f.write(str(compiled.get('compiled_urdf', '') or ''))
            with open(validation_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(validation, f, sort_keys=False)
        except Exception as exc:
            return self._ok(False, f'Failed to store URDF artifacts: {exc}')

        profile['profile_version'] = next_version
        profile['urdf_source'] = self._artifact_relative_path(source_path)
        profile['urdf_artifact'] = {
            'source_filename': source_filename,
            'source_type': source_type,
            'source_path': self._artifact_relative_path(source_path),
            'compiled_urdf_path': self._artifact_relative_path(compiled_path),
            'validation_report_path': self._artifact_relative_path(validation_path),
            'uploaded_at': int(time.time()),
            'validation': validation,
            'runtime': {},
        }

        try:
            saved_profile = self._save_profile_to_disk(profile, allow_overwrite=True, increment_version=False)
        except Exception as exc:
            return self._ok(False, f'Failed to save profile with URDF artifact: {exc}')

        runtime_result: Optional[Dict[str, Any]] = None
        if apply_runtime:
            runtime_result = self._start_managed_robot_state_publisher(
                profile=saved_profile,
                compiled_urdf_path=saved_profile.get('urdf_artifact', {}).get('compiled_urdf_path', ''),
            )
            saved_profile['urdf_artifact']['runtime'] = {
                'ok': bool(runtime_result.get('ok')),
                'message': str(runtime_result.get('message', '') or ''),
                'node_name': str(runtime_result.get('node_name', '') or ''),
                'namespace': str(runtime_result.get('namespace', '') or ''),
                'required_tf_frames': list(runtime_result.get('required_tf_frames', [])),
                'observed_tf_frames': list(runtime_result.get('observed_tf_frames', [])),
                'missing_tf_frames': list(runtime_result.get('missing_tf_frames', [])),
                'pid': int(runtime_result.get('pid', 0) or 0),
            }
            try:
                saved_profile = self._save_profile_to_disk(saved_profile, allow_overwrite=True, increment_version=False)
            except Exception as exc:
                self.get_logger().warn(f'Failed to persist URDF runtime status: {exc}')

        if str(self.active_robot_profile_id or '').strip() == profile_id:
            self._apply_profile_config_to_memory(saved_profile)
            self._save_profile_registry_state(profile_id)

        msg = f'URDF artifact stored for profile "{profile_id}" at version {next_version}'
        if runtime_result and not bool(runtime_result.get('ok')):
            msg = f'{msg}. Runtime warning: {runtime_result.get("message", "")}'.strip()
        elif runtime_result:
            msg = f'{msg}. Runtime TF verified.'

        return self._ok(
            True,
            msg,
            robot_id=profile_id,
            profile_version=next_version,
            validation=validation,
            runtime=runtime_result or {},
            profile=saved_profile,
        )

    def get_profile_urdf_source_editor(self, robot_id: str = ''):
        target_robot_id = str(robot_id or '').strip()
        if not target_robot_id:
            target_robot_id = str(self.active_robot_profile_id or '').strip()
        if not target_robot_id:
            return self._ok(False, 'No robot profile selected')

        profiles = self._load_robot_profiles()
        profile = profiles.get(target_robot_id)
        if not profile:
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        available_sources = self._list_description_urdf_sources()
        source_path, resolved_path = self._effective_profile_urdf_source(profile, robot_id=target_robot_id)
        if resolved_path and not self._is_description_source_path(resolved_path):
            preferred_source = self._default_description_urdf_source()
            preferred_resolved = self._resolve_urdf_source_path(preferred_source, robot_id=target_robot_id)
            if preferred_source and preferred_resolved:
                source_path = preferred_source
                resolved_path = preferred_resolved

        if not source_path:
            return self._ok(
                False,
                'No URDF/Xacro source available in description directory',
                robot_id=target_robot_id,
                source_path='',
                resolved_source_path='',
                source_text='',
                source_type='',
                available_sources=available_sources,
                description_dir=self.description_dir,
            )

        if not resolved_path or not os.path.exists(resolved_path):
            return self._ok(
                False,
                f'URDF source "{source_path}" is not readable',
                robot_id=target_robot_id,
                source_path=source_path,
                resolved_source_path='',
                source_text='',
                source_type='',
                available_sources=available_sources,
                description_dir=self.description_dir,
            )

        try:
            with open(resolved_path, 'rb') as f:
                raw = bytes(f.read() or b'')
        except Exception as exc:
            return self._ok(
                False,
                f'Failed reading URDF source "{source_path}": {exc}',
                robot_id=target_robot_id,
                source_path=source_path,
                resolved_source_path=resolved_path,
                source_text='',
                source_type='',
                available_sources=available_sources,
                description_dir=self.description_dir,
            )

        if len(raw) > int(self.URDF_MAX_SOURCE_EDIT_BYTES):
            return self._ok(
                False,
                f'URDF source exceeds {int(self.URDF_MAX_SOURCE_EDIT_BYTES)} bytes',
                robot_id=target_robot_id,
                source_path=source_path,
                resolved_source_path=resolved_path,
                source_text='',
                source_type='',
                available_sources=available_sources,
                description_dir=self.description_dir,
            )

        if b'\x00' in raw:
            return self._ok(
                False,
                f'URDF source "{source_path}" appears to be binary',
                robot_id=target_robot_id,
                source_path=source_path,
                resolved_source_path=resolved_path,
                source_text='',
                source_type='',
                available_sources=available_sources,
                description_dir=self.description_dir,
            )

        try:
            source_text = raw.decode('utf-8')
        except UnicodeDecodeError as exc:
            return self._ok(
                False,
                f'URDF source "{source_path}" must be UTF-8 text: {exc}',
                robot_id=target_robot_id,
                source_path=source_path,
                resolved_source_path=resolved_path,
                source_text='',
                source_type='',
                available_sources=available_sources,
                description_dir=self.description_dir,
            )

        source_ext = os.path.splitext(str(resolved_path or '').strip())[1].lower()
        source_type = 'xacro' if source_ext == '.xacro' else 'urdf'
        if '<xacro:' in source_text.lower() or 'xmlns:xacro=' in source_text.lower():
            source_type = 'xacro'

        return self._ok(
            True,
            f'Loaded URDF source "{source_path}"',
            robot_id=target_robot_id,
            source_path=source_path,
            resolved_source_path=resolved_path,
            source_text=source_text,
            source_type=source_type,
            available_sources=available_sources,
            description_dir=self.description_dir,
        )

    def save_profile_urdf_source_editor(
        self,
        *,
        robot_id: str,
        source_path: str,
        source_text: str,
        apply_runtime: bool = True,
    ):
        profile_id = str(robot_id or '').strip()
        if not profile_id:
            return self._ok(False, 'robot_id is required')

        profiles = self._load_robot_profiles()
        profile = profiles.get(profile_id)
        if not profile:
            return self._ok(False, f'Robot profile "{profile_id}" not found')

        raw_text = str(source_text or '')
        if not raw_text.strip():
            return self._ok(False, 'URDF/Xacro source content is empty')
        if '\x00' in raw_text:
            return self._ok(False, 'Binary content is not accepted')
        encoded = raw_text.encode('utf-8')
        if len(encoded) > int(self.URDF_MAX_SOURCE_EDIT_BYTES):
            return self._ok(False, f'URDF source exceeds {int(self.URDF_MAX_SOURCE_EDIT_BYTES)} bytes')

        requested_source = str(source_path or '').strip()
        if not requested_source:
            requested_source = str(profile.get('urdf_source', '') or '').strip()
        if not requested_source:
            requested_source = self._default_description_urdf_source()
        if not requested_source:
            return self._ok(False, 'No target URDF/Xacro file selected')

        current_resolved = self._resolve_urdf_source_path(requested_source, robot_id=profile_id)
        if current_resolved and not self._is_description_source_path(current_resolved):
            fallback_source = self._default_description_urdf_source()
            if fallback_source:
                requested_source = fallback_source

        expanded = os.path.expanduser(requested_source)
        if os.path.isabs(expanded):
            resolved_source = os.path.abspath(expanded)
        else:
            candidate_ui = os.path.abspath(os.path.join(self.ui_root, expanded))
            candidate_desc = os.path.abspath(os.path.join(self.description_dir, expanded))
            resolved_source = candidate_ui if self._is_description_source_path(candidate_ui) else candidate_desc

        if not self._is_description_source_path(resolved_source):
            return self._ok(False, 'Only files inside the description directory can be edited from UI')

        source_ext = os.path.splitext(str(resolved_source or '').strip())[1].lower()
        if source_ext not in {'.urdf', '.xacro', '.xml'}:
            return self._ok(False, 'Only .urdf, .xacro, and .xml files are supported')

        source_dir = os.path.dirname(resolved_source)
        if source_dir:
            os.makedirs(source_dir, exist_ok=True)

        try:
            with open(resolved_source, 'w', encoding='utf-8') as f:
                f.write(raw_text)
        except Exception as exc:
            return self._ok(False, f'Failed writing source file: {exc}')

        source_type = 'xacro' if source_ext == '.xacro' else 'urdf'
        if source_type != 'xacro':
            lower = raw_text.lower()
            if '<xacro:' in lower or 'xmlns:xacro=' in lower:
                source_type = 'xacro'

        compiled_urdf = ''
        if source_type == 'xacro':
            try:
                compiled_doc = xacro.process_file(resolved_source)
                compiled_urdf = compiled_doc.toprettyxml(indent='  ')
            except Exception as exc:
                validation = {
                    'valid': False,
                    'parse_ok': False,
                    'tree_connected': False,
                    'base_link_exists': False,
                    'required_frames_ok': False,
                    'message': f'Xacro compilation failed: {exc}',
                }
                profile['urdf_source'] = self._ui_relative_path(resolved_source)
                profile['urdf_artifact'] = {
                    'source_filename': os.path.basename(resolved_source),
                    'source_type': source_type,
                    'source_path': profile['urdf_source'],
                    'compiled_urdf_path': '',
                    'validation_report_path': '',
                    'uploaded_at': int(time.time()),
                    'validation': validation,
                    'runtime': {},
                }
                try:
                    saved_profile = self._save_profile_to_disk(profile, allow_overwrite=True, increment_version=False)
                except Exception as save_exc:
                    return self._ok(False, f'Failed saving profile after compile error: {save_exc}', validation=validation)
                if str(self.active_robot_profile_id or '').strip() == profile_id:
                    self._apply_profile_config_to_memory(saved_profile)
                    self._save_profile_registry_state(profile_id)
                return self._ok(
                    False,
                    validation['message'],
                    robot_id=profile_id,
                    validation=validation,
                    profile=saved_profile,
                )
        else:
            compiled_urdf = raw_text

        validation = self._validate_compiled_urdf(
            compiled_urdf,
            base_frame=str(profile.get('base_frame', 'base_link') or 'base_link'),
            sensor_frames=profile.get('sensor_frames', {}) if isinstance(profile.get('sensor_frames', {}), dict) else {},
        )
        if not bool(validation.get('valid')):
            profile['urdf_source'] = self._ui_relative_path(resolved_source)
            profile['urdf_artifact'] = {
                'source_filename': os.path.basename(resolved_source),
                'source_type': source_type,
                'source_path': profile['urdf_source'],
                'compiled_urdf_path': '',
                'validation_report_path': '',
                'uploaded_at': int(time.time()),
                'validation': validation,
                'runtime': {},
            }
            try:
                saved_profile = self._save_profile_to_disk(profile, allow_overwrite=True, increment_version=False)
            except Exception as save_exc:
                return self._ok(False, f'Failed saving profile validation status: {save_exc}', validation=validation)
            if str(self.active_robot_profile_id or '').strip() == profile_id:
                self._apply_profile_config_to_memory(saved_profile)
                self._save_profile_registry_state(profile_id)
            return self._ok(
                False,
                str(validation.get('message', 'URDF validation failed') or 'URDF validation failed'),
                robot_id=profile_id,
                validation=validation,
                profile=saved_profile,
            )

        current_version = int(profile.get('profile_version', 1) or 1)
        next_version = max(1, current_version + 1)
        artifact_dir = os.path.join(self.robot_profiles_dir, 'artifacts', self._safe_profile_slug(profile_id), f'v{next_version}')
        os.makedirs(artifact_dir, exist_ok=True)

        source_filename = self._sanitize_upload_filename(os.path.basename(resolved_source), default_name='robot_model')
        source_copy_path = os.path.join(artifact_dir, source_filename)
        compiled_path = os.path.join(artifact_dir, 'compiled.urdf')
        validation_path = os.path.join(artifact_dir, 'validation.yaml')

        try:
            with open(source_copy_path, 'w', encoding='utf-8') as f:
                f.write(raw_text)
            with open(compiled_path, 'w', encoding='utf-8') as f:
                f.write(compiled_urdf)
            with open(validation_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(validation, f, sort_keys=False)
        except Exception as exc:
            return self._ok(False, f'Failed to store URDF artifacts: {exc}')

        source_ref = self._ui_relative_path(resolved_source)
        profile['profile_version'] = next_version
        profile['urdf_source'] = source_ref
        profile['urdf_artifact'] = {
            'source_filename': source_filename,
            'source_type': source_type,
            'source_path': source_ref,
            'compiled_urdf_path': self._artifact_relative_path(compiled_path),
            'validation_report_path': self._artifact_relative_path(validation_path),
            'uploaded_at': int(time.time()),
            'validation': validation,
            'runtime': {},
        }

        try:
            saved_profile = self._save_profile_to_disk(profile, allow_overwrite=True, increment_version=False)
        except Exception as exc:
            return self._ok(False, f'Failed to save profile with URDF artifact: {exc}')

        runtime_result: Optional[Dict[str, Any]] = None
        if apply_runtime:
            runtime_result = self._start_managed_robot_state_publisher(
                profile=saved_profile,
                compiled_urdf_path=saved_profile.get('urdf_artifact', {}).get('compiled_urdf_path', ''),
            )
            saved_profile['urdf_artifact']['runtime'] = {
                'ok': bool(runtime_result.get('ok')),
                'message': str(runtime_result.get('message', '') or ''),
                'node_name': str(runtime_result.get('node_name', '') or ''),
                'namespace': str(runtime_result.get('namespace', '') or ''),
                'required_tf_frames': list(runtime_result.get('required_tf_frames', [])),
                'observed_tf_frames': list(runtime_result.get('observed_tf_frames', [])),
                'missing_tf_frames': list(runtime_result.get('missing_tf_frames', [])),
                'pid': int(runtime_result.get('pid', 0) or 0),
            }
            try:
                saved_profile = self._save_profile_to_disk(saved_profile, allow_overwrite=True, increment_version=False)
            except Exception as exc:
                self.get_logger().warn(f'Failed to persist URDF runtime status: {exc}')

        if str(self.active_robot_profile_id or '').strip() == profile_id:
            self._apply_profile_config_to_memory(saved_profile)
            self._save_profile_registry_state(profile_id)

        msg = f'URDF source "{source_ref}" saved and validated at profile version {next_version}'
        if runtime_result and not bool(runtime_result.get('ok')):
            msg = f'{msg}. Runtime warning: {runtime_result.get("message", "")}'.strip()
        elif runtime_result:
            msg = f'{msg}. Runtime TF verified.'

        return self._ok(
            True,
            msg,
            robot_id=profile_id,
            source_path=source_ref,
            profile_version=next_version,
            validation=validation,
            runtime=runtime_result or {},
            profile=saved_profile,
        )

    @classmethod
    def _profile_fields_from_profile(cls, profile: Dict[str, Any]) -> Dict[str, Any]:
        source = profile if isinstance(profile, dict) else {}
        sensor_frames = source.get('sensor_frames', {}) if isinstance(source.get('sensor_frames', {}), dict) else {}
        ui_toggles = source.get('ui_toggles', {}) if isinstance(source.get('ui_toggles', {}), dict) else {}
        map_association = source.get('map_association', {}) if isinstance(source.get('map_association', {}), dict) else {}
        return {
            'robot_id': str(source.get('robot_id', '') or '').strip(),
            'namespace': str(source.get('namespace', '/') or '/').strip() or '/',
            'base_frame': str(source.get('base_frame', 'base_link') or 'base_link').strip() or 'base_link',
            'urdf_source': str(source.get('urdf_source', '') or '').strip(),
            'nav2_params_file': str(source.get('nav2_params_file', '') or '').strip(),
            'map_association': {
                'active_map': str(map_association.get('active_map', '') or '').strip(),
            },
            'sensor_frames': {
                'map': str(sensor_frames.get('map', 'map') or 'map').strip() or 'map',
                'odom': str(sensor_frames.get('odom', 'odom') or 'odom').strip() or 'odom',
                'laser': str(sensor_frames.get('laser', 'base_scan') or 'base_scan').strip() or 'base_scan',
                'camera': str(sensor_frames.get('camera', 'camera_link') or 'camera_link').strip() or 'camera_link',
            },
            'ui_toggles': {
                'show_zones': bool(ui_toggles.get('show_zones', True)),
                'show_paths': bool(ui_toggles.get('show_paths', True)),
                'follow_robot': bool(ui_toggles.get('follow_robot', False)),
                'show_robot_halo': bool(ui_toggles.get('show_robot_halo', True)),
                'show_robot_heading': bool(ui_toggles.get('show_robot_heading', True)),
            },
        }

    @classmethod
    def _apply_profile_field_overrides(cls, profile: Dict[str, Any], overrides: Any) -> Dict[str, Any]:
        source_profile = profile if isinstance(profile, dict) else {}
        updated = copy.deepcopy(source_profile)
        payload = overrides if isinstance(overrides, dict) else {}

        for key in ('robot_id', 'namespace', 'base_frame', 'urdf_source', 'nav2_params_file'):
            if key in payload:
                updated[key] = payload.get(key, '')

        map_association = payload.get('map_association')
        if isinstance(map_association, dict):
            updated['map_association'] = {'active_map': str(map_association.get('active_map', '') or '').strip()}

        sensor_frames = payload.get('sensor_frames')
        if isinstance(sensor_frames, dict):
            merged_sensor_frames = dict(updated.get('sensor_frames', {})) if isinstance(updated.get('sensor_frames', {}), dict) else {}
            merged_sensor_frames.update(sensor_frames)
            updated['sensor_frames'] = merged_sensor_frames

        ui_toggles = payload.get('ui_toggles')
        if isinstance(ui_toggles, dict):
            merged_toggles = dict(updated.get('ui_toggles', {})) if isinstance(updated.get('ui_toggles', {}), dict) else {}
            merged_toggles.update(ui_toggles)
            updated['ui_toggles'] = merged_toggles

        fallback_robot_id = str(source_profile.get('robot_id', '') or '').strip()
        return cls._sanitize_profile_payload(updated, fallback_robot_id=fallback_robot_id)

    def _import_robot_builder_from_urdf(self, compiled_urdf: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        try:
            root = ET.fromstring(str(compiled_urdf or '').strip())
        except Exception as exc:
            return self._ok(False, f'URDF parse failed: {exc}')
        if root.tag != 'robot':
            return self._ok(False, f'Unexpected root tag "{root.tag}" (expected "robot")')

        links_in_order: List[str] = []
        link_name_set: Set[str] = set()
        for link_elem in root.findall('./link'):
            link_name = self._normalize_frame_name(link_elem.attrib.get('name', ''))
            if not link_name or link_name in link_name_set:
                continue
            links_in_order.append(link_name)
            link_name_set.add(link_name)

        if not links_in_order:
            return self._ok(False, 'URDF contains no links')

        requested_base = self._normalize_frame_name(profile.get('base_frame', 'base_link')) or 'base_link'
        base_link = requested_base if requested_base in link_name_set else links_in_order[0]

        joints_by_child: Dict[str, Dict[str, Any]] = {}
        for joint_elem in root.findall('./joint'):
            parent_elem = joint_elem.find('parent')
            child_elem = joint_elem.find('child')
            parent = self._normalize_frame_name(parent_elem.attrib.get('link', '') if parent_elem is not None else '')
            child = self._normalize_frame_name(child_elem.attrib.get('link', '') if child_elem is not None else '')
            if not parent or not child or child in joints_by_child:
                continue

            origin_elem = joint_elem.find('origin')
            xyz = self._parse_float_triplet(origin_elem.attrib.get('xyz', '0 0 0') if origin_elem is not None else '0 0 0')
            rpy = self._parse_float_triplet(origin_elem.attrib.get('rpy', '0 0 0') if origin_elem is not None else '0 0 0')
            joint_type = str(joint_elem.attrib.get('type', 'fixed') or 'fixed').strip().lower()
            if joint_type not in self.ROBOT_EDITOR_ALLOWED_JOINT_TYPES:
                joint_type = 'fixed'

            joints_by_child[child] = {
                'parent': parent,
                'joint_type': joint_type,
                'joint_xyz': [float(xyz[0]), float(xyz[1]), float(xyz[2])],
                'joint_rpy': [float(rpy[0]), float(rpy[1]), float(rpy[2])],
            }

        links: List[Dict[str, Any]] = [
            {
                'name': base_link,
                'parent': '',
                'joint_type': 'fixed',
                'joint_xyz': [0.0, 0.0, 0.0],
                'joint_rpy': [0.0, 0.0, 0.0],
            }
        ]
        for link_name in links_in_order:
            if link_name == base_link:
                continue
            joint = joints_by_child.get(link_name, {})
            parent = self._normalize_frame_name(joint.get('parent', '')) or base_link
            if parent not in link_name_set or parent == link_name:
                parent = base_link
            links.append(
                {
                    'name': link_name,
                    'parent': parent,
                    'joint_type': str(joint.get('joint_type', 'fixed') or 'fixed'),
                    'joint_xyz': self._triplet_from_any(joint.get('joint_xyz', [0.0, 0.0, 0.0]), default=(0.0, 0.0, 0.0)),
                    'joint_rpy': self._triplet_from_any(joint.get('joint_rpy', [0.0, 0.0, 0.0]), default=(0.0, 0.0, 0.0)),
                }
            )
            if len(links) >= int(self.ROBOT_EDITOR_MAX_LINKS):
                break

        shapes: List[Dict[str, Any]] = []
        used_ids: Set[str] = set()
        for link_elem in root.findall('./link'):
            link_name = self._normalize_frame_name(link_elem.attrib.get('name', ''))
            if not link_name:
                continue

            for idx, collision_elem in enumerate(link_elem.findall('./collision')):
                origin_elem = collision_elem.find('origin')
                xyz = self._parse_float_triplet(origin_elem.attrib.get('xyz', '0 0 0') if origin_elem is not None else '0 0 0')
                rpy = self._parse_float_triplet(origin_elem.attrib.get('rpy', '0 0 0') if origin_elem is not None else '0 0 0')

                geometry_elem = collision_elem.find('geometry')
                if geometry_elem is None:
                    continue

                shape_type = ''
                width = 0.0
                length = 0.0
                height = 0.0
                radius = 0.0

                box_elem = geometry_elem.find('box')
                cyl_elem = geometry_elem.find('cylinder')
                sphere_elem = geometry_elem.find('sphere')
                if box_elem is not None:
                    size = self._parse_float_triplet(box_elem.attrib.get('size', '0.1 0.1 0.1'), default=(0.1, 0.1, 0.1))
                    width = abs(float(size[0]))
                    length = abs(float(size[1]))
                    height = max(0.01, abs(float(size[2])))
                    radius = max(0.005, min(width, length) * 0.5)
                    shape_type = 'box'
                elif cyl_elem is not None:
                    radius = max(0.005, abs(self._safe_float(cyl_elem.attrib.get('radius', 0.1), 0.1)))
                    height = max(0.01, abs(self._safe_float(cyl_elem.attrib.get('length', 0.2), 0.2)))
                    width = 2.0 * radius
                    length = 2.0 * radius
                    shape_type = 'cylinder'
                elif sphere_elem is not None:
                    radius = max(0.005, abs(self._safe_float(sphere_elem.attrib.get('radius', 0.1), 0.1)))
                    height = 2.0 * radius
                    width = 2.0 * radius
                    length = 2.0 * radius
                    shape_type = 'sphere'
                else:
                    continue

                base_id = str(collision_elem.attrib.get('name', '') or f'{link_name}_shape_{idx + 1}').strip()
                base_id = re.sub(r'[^a-zA-Z0-9_-]+', '_', base_id).strip('_') or f'{link_name}_shape_{idx + 1}'
                shape_id = base_id
                suffix = 2
                while shape_id in used_ids:
                    shape_id = f'{base_id}_{suffix}'
                    suffix += 1
                used_ids.add(shape_id)

                shapes.append(
                    {
                        'id': shape_id,
                        'name': shape_id,
                        'type': shape_type,
                        'link': link_name,
                        'x': float(xyz[0]),
                        'y': float(xyz[1]),
                        'z': float(xyz[2]),
                        'yaw': float(rpy[2]),
                        'width': float(width),
                        'length': float(length),
                        'height': float(height),
                        'radius': float(radius),
                        'color': '#60a5fa',
                    }
                )
                if len(shapes) >= int(self.ROBOT_EDITOR_MAX_SHAPES):
                    break
            if len(shapes) >= int(self.ROBOT_EDITOR_MAX_SHAPES):
                break

        builder = self._sanitize_robot_builder(
            {
                'schema_version': self.ROBOT_EDITOR_SCHEMA_VERSION,
                'source': 'urdf_import',
                'base_link': base_link,
                'links': links,
                'shapes': shapes,
                'updated_at': int(time.time()),
            },
            fallback_base_link=base_link,
        )
        return self._ok(
            True,
            'Loaded robot editor model from URDF',
            robot_builder=builder,
            base_link=base_link,
            link_count=len(builder.get('links', [])),
            shape_count=len(builder.get('shapes', [])),
        )

    def _render_robot_builder_urdf(self, robot_id: str, robot_builder: Dict[str, Any]) -> Dict[str, Any]:
        model = self._sanitize_robot_builder(robot_builder, fallback_base_link='base_link')
        robot_name = re.sub(r'[^a-zA-Z0-9_]+', '_', str(robot_id or '').strip()).strip('_') or 'robot_model'
        base_link = self._normalize_frame_name(model.get('base_link', 'base_link')) or 'base_link'
        links = model.get('links', []) if isinstance(model.get('links', []), list) else []
        shapes = model.get('shapes', []) if isinstance(model.get('shapes', []), list) else []

        root = ET.Element('robot', {'name': robot_name})

        shapes_by_link: Dict[str, List[Dict[str, Any]]] = {}
        for shape in shapes:
            if not isinstance(shape, dict):
                continue
            link_name = self._normalize_frame_name(shape.get('link', '')) or base_link
            shapes_by_link.setdefault(link_name, []).append(shape)

        ordered_links: List[Dict[str, Any]] = []
        base_entry = None
        for link in links:
            if not isinstance(link, dict):
                continue
            name = self._normalize_frame_name(link.get('name', ''))
            if not name:
                continue
            if name == base_link and base_entry is None:
                base_entry = link
            else:
                ordered_links.append(link)
        if base_entry is None:
            base_entry = {
                'name': base_link,
                'parent': '',
                'joint_type': 'fixed',
                'joint_xyz': [0.0, 0.0, 0.0],
                'joint_rpy': [0.0, 0.0, 0.0],
            }
        ordered_links.insert(0, base_entry)

        emitted_links: Set[str] = set()
        for link in ordered_links:
            link_name = self._normalize_frame_name(link.get('name', ''))
            if not link_name or link_name in emitted_links:
                continue
            emitted_links.add(link_name)
            link_elem = ET.SubElement(root, 'link', {'name': link_name})

            local_shapes = shapes_by_link.get(link_name, [])
            for shape_idx, shape in enumerate(local_shapes):
                shape_name = str(shape.get('name', '') or shape.get('id', '') or f'{link_name}_shape_{shape_idx + 1}').strip() or f'{link_name}_shape_{shape_idx + 1}'
                shape_name = re.sub(r'[^a-zA-Z0-9_-]+', '_', shape_name).strip('_') or f'{link_name}_shape_{shape_idx + 1}'
                collision_elem = ET.SubElement(link_elem, 'collision', {'name': shape_name})
                xyz = [
                    self._safe_float(shape.get('x', 0.0), 0.0, min_value=-1000.0, max_value=1000.0),
                    self._safe_float(shape.get('y', 0.0), 0.0, min_value=-1000.0, max_value=1000.0),
                    self._safe_float(shape.get('z', 0.0), 0.0, min_value=-1000.0, max_value=1000.0),
                ]
                yaw = self._safe_float(shape.get('yaw', 0.0), 0.0, min_value=-1.0e6, max_value=1.0e6)
                ET.SubElement(
                    collision_elem,
                    'origin',
                    {
                        'xyz': f'{xyz[0]:.6f} {xyz[1]:.6f} {xyz[2]:.6f}',
                        'rpy': f'0.000000 0.000000 {yaw:.6f}',
                    },
                )
                geometry_elem = ET.SubElement(collision_elem, 'geometry')

                shape_type = str(shape.get('type', 'box') or 'box').strip().lower()
                width = self._safe_float(shape.get('width', 0.40), 0.40, min_value=0.01, max_value=100.0)
                length = self._safe_float(shape.get('length', 0.30), 0.30, min_value=0.01, max_value=100.0)
                height = self._safe_float(shape.get('height', 0.25), 0.25, min_value=0.01, max_value=100.0)
                radius = self._safe_float(shape.get('radius', 0.15), 0.15, min_value=0.005, max_value=100.0)
                if shape_type == 'sphere':
                    ET.SubElement(geometry_elem, 'sphere', {'radius': f'{radius:.6f}'})
                elif shape_type == 'cylinder':
                    ET.SubElement(geometry_elem, 'cylinder', {'radius': f'{radius:.6f}', 'length': f'{height:.6f}'})
                else:
                    ET.SubElement(geometry_elem, 'box', {'size': f'{width:.6f} {length:.6f} {height:.6f}'})

        emitted_joint_names: Set[str] = set()
        for link in ordered_links:
            child = self._normalize_frame_name(link.get('name', ''))
            if not child or child == base_link:
                continue

            parent = self._normalize_frame_name(link.get('parent', '')) or base_link
            if parent not in emitted_links or parent == child:
                parent = base_link

            joint_type = str(link.get('joint_type', 'fixed') or 'fixed').strip().lower()
            if joint_type not in self.ROBOT_EDITOR_ALLOWED_JOINT_TYPES:
                joint_type = 'fixed'

            base_joint_name = re.sub(r'[^a-zA-Z0-9_-]+', '_', f'{parent}_to_{child}').strip('_') or f'joint_{child}'
            joint_name = base_joint_name
            suffix = 2
            while joint_name in emitted_joint_names:
                joint_name = f'{base_joint_name}_{suffix}'
                suffix += 1
            emitted_joint_names.add(joint_name)

            joint_elem = ET.SubElement(root, 'joint', {'name': joint_name, 'type': joint_type})
            ET.SubElement(joint_elem, 'parent', {'link': parent})
            ET.SubElement(joint_elem, 'child', {'link': child})

            joint_xyz = self._triplet_from_any(link.get('joint_xyz', [0.0, 0.0, 0.0]), default=(0.0, 0.0, 0.0))
            joint_rpy = self._triplet_from_any(link.get('joint_rpy', [0.0, 0.0, 0.0]), default=(0.0, 0.0, 0.0))
            ET.SubElement(
                joint_elem,
                'origin',
                {
                    'xyz': f'{joint_xyz[0]:.6f} {joint_xyz[1]:.6f} {joint_xyz[2]:.6f}',
                    'rpy': f'{joint_rpy[0]:.6f} {joint_rpy[1]:.6f} {joint_rpy[2]:.6f}',
                },
            )

        if hasattr(ET, 'indent'):
            try:
                ET.indent(root, space='  ')
            except Exception:
                pass
        compiled_urdf = ET.tostring(root, encoding='unicode')
        if not compiled_urdf.strip():
            return self._ok(False, 'Generated URDF is empty')

        return self._ok(
            True,
            'Generated URDF from robot editor model',
            compiled_urdf=compiled_urdf,
            robot_builder=model,
        )

    def get_profile_robot_editor(self, robot_id: str = '', reload_from_urdf: bool = False):
        target_robot_id = str(robot_id or '').strip()
        profiles = self._load_robot_profiles()
        if not profiles:
            return self._ok(False, 'No robot profiles available')

        if not target_robot_id:
            target_robot_id = str(self.active_robot_profile_id or '').strip()
        if not target_robot_id or target_robot_id not in profiles:
            target_robot_id = sorted(profiles.keys())[0]

        profile = profiles.get(target_robot_id, {})
        profile = self._sanitize_profile_payload(profile, fallback_robot_id=target_robot_id)
        builder = self._sanitize_robot_builder(
            profile.get('robot_builder', {}),
            fallback_base_link=str(profile.get('base_frame', 'base_link') or 'base_link'),
        )

        imported_from_urdf = False
        import_message = ''
        source_payload: Dict[str, Any] = {}
        artifact = profile.get('urdf_artifact', {}) if isinstance(profile.get('urdf_artifact', {}), dict) else {}
        has_profile_urdf_source = bool(str(profile.get('urdf_source', '') or '').strip()) or bool(str(artifact.get('compiled_urdf_path', '') or '').strip())
        builder_source = str(builder.get('source', '') or '').strip().lower()
        should_autoload_from_urdf = has_profile_urdf_source and builder_source not in {'robot_editor', 'urdf_import'}
        if bool(reload_from_urdf) or len(builder.get('shapes', [])) == 0 or should_autoload_from_urdf:
            loaded = self._load_compiled_urdf_artifact(profile)
            if bool(loaded.get('ok')):
                imported = self._import_robot_builder_from_urdf(str(loaded.get('compiled_urdf', '') or ''), profile=profile)
                if bool(imported.get('ok')):
                    builder = self._sanitize_robot_builder(
                        imported.get('robot_builder', {}),
                        fallback_base_link=str(profile.get('base_frame', 'base_link') or 'base_link'),
                    )
                    imported_from_urdf = True
                    import_message = str(imported.get('message', '') or '')
                else:
                    import_message = str(imported.get('message', '') or 'Unable to import URDF into robot editor')
            else:
                import_message = str(loaded.get('message', '') or 'No readable URDF source found')

            source_payload = {
                'source_type': str(loaded.get('source_type', '') or ''),
                'source_path': str(loaded.get('source_path', '') or ''),
                'compiled_urdf_path': str(loaded.get('compiled_urdf_path', '') or ''),
            }

        return self._ok(
            True,
            f'Loaded robot editor for profile "{target_robot_id}"',
            robot_id=target_robot_id,
            profile_version=int(profile.get('profile_version', 1) or 1),
            profile_fields=self._profile_fields_from_profile(profile),
            robot_builder=builder,
            imported_from_urdf=bool(imported_from_urdf),
            import_message=import_message,
            source=source_payload,
            profile=copy.deepcopy(profile),
        )

    def save_profile_robot_editor(
        self,
        *,
        robot_id: str,
        profile_fields: Any,
        robot_builder: Any,
        compile_urdf: bool = True,
        apply_runtime: bool = False,
    ):
        requested_robot_id = str(robot_id or '').strip()
        profiles = self._load_robot_profiles()
        if not profiles:
            return self._ok(False, 'No robot profiles available')

        if not requested_robot_id:
            requested_robot_id = str(self.active_robot_profile_id or '').strip()
        if not requested_robot_id or requested_robot_id not in profiles:
            requested_robot_id = sorted(profiles.keys())[0]

        original_profile = profiles.get(requested_robot_id, {})
        if not isinstance(original_profile, dict):
            return self._ok(False, f'Robot profile "{requested_robot_id}" not found')

        updated_profile = self._apply_profile_field_overrides(original_profile, profile_fields)
        target_robot_id = str(updated_profile.get('robot_id', requested_robot_id) or requested_robot_id).strip()
        if not target_robot_id:
            target_robot_id = requested_robot_id
            updated_profile['robot_id'] = target_robot_id

        if target_robot_id != requested_robot_id and target_robot_id in profiles:
            return self._ok(False, f'Cannot rename to "{target_robot_id}" because that profile already exists')

        updated_builder = self._sanitize_robot_builder(
            robot_builder,
            fallback_base_link=str(updated_profile.get('base_frame', 'base_link') or 'base_link'),
        )
        updated_builder['source'] = 'robot_editor'
        updated_builder['base_link'] = self._normalize_frame_name(updated_builder.get('base_link', updated_profile.get('base_frame', 'base_link'))) or 'base_link'
        updated_builder['updated_at'] = int(time.time())
        updated_profile['base_frame'] = str(updated_builder.get('base_link', updated_profile.get('base_frame', 'base_link')) or 'base_link')
        updated_profile['robot_builder'] = updated_builder
        updated_profile['robot_id'] = target_robot_id

        compile_requested = bool(compile_urdf)
        compile_ok = False
        compile_message = ''
        validation: Dict[str, Any] = {}
        generated_urdf_text = ''
        runtime_result: Optional[Dict[str, Any]] = None

        if compile_requested:
            rendered = self._render_robot_builder_urdf(target_robot_id, updated_builder)
            if not bool(rendered.get('ok')):
                compile_message = str(rendered.get('message', 'URDF generation failed') or 'URDF generation failed')
            else:
                generated_urdf_text = str(rendered.get('compiled_urdf', '') or '')
                validation = self._validate_compiled_urdf(
                    generated_urdf_text,
                    base_frame=str(updated_profile.get('base_frame', 'base_link') or 'base_link'),
                    sensor_frames=updated_profile.get('sensor_frames', {}) if isinstance(updated_profile.get('sensor_frames', {}), dict) else {},
                )
                compile_ok = bool(validation.get('valid', False))
                if compile_ok:
                    current_version = max(1, int(updated_profile.get('profile_version', 1) or 1))
                    next_version = max(1, current_version + 1)
                    artifact_dir = os.path.join(
                        self.robot_profiles_dir,
                        'artifacts',
                        self._safe_profile_slug(target_robot_id),
                        f'v{next_version}',
                    )
                    os.makedirs(artifact_dir, exist_ok=True)

                    source_path = os.path.join(artifact_dir, 'robot_editor_generated.urdf')
                    compiled_path = os.path.join(artifact_dir, 'compiled.urdf')
                    validation_path = os.path.join(artifact_dir, 'validation.yaml')
                    try:
                        with open(source_path, 'w', encoding='utf-8') as f:
                            f.write(generated_urdf_text)
                        with open(compiled_path, 'w', encoding='utf-8') as f:
                            f.write(generated_urdf_text)
                        with open(validation_path, 'w', encoding='utf-8') as f:
                            yaml.safe_dump(validation, f, sort_keys=False)
                    except Exception as exc:
                        compile_ok = False
                        compile_message = f'Generated URDF could not be written to artifacts: {exc}'

                    if compile_ok:
                        updated_profile['profile_version'] = next_version
                        updated_profile['urdf_source'] = self._artifact_relative_path(source_path)
                        updated_profile['urdf_artifact'] = {
                            'source_filename': os.path.basename(source_path),
                            'source_type': 'urdf',
                            'source_path': self._artifact_relative_path(source_path),
                            'compiled_urdf_path': self._artifact_relative_path(compiled_path),
                            'validation_report_path': self._artifact_relative_path(validation_path),
                            'uploaded_at': int(time.time()),
                            'validation': validation,
                            'runtime': {},
                        }
                        compile_message = 'URDF artifact generated from robot editor'
                else:
                    compile_message = str(validation.get('message', 'Generated URDF failed validation') or 'Generated URDF failed validation')

        save_increment = not bool(compile_requested and compile_ok)
        try:
            saved_profile = self._save_profile_to_disk(updated_profile, allow_overwrite=True, increment_version=save_increment)
        except Exception as exc:
            return self._ok(False, f'Failed saving robot editor profile: {exc}')

        saved_robot_id = str(saved_profile.get('robot_id', requested_robot_id) or requested_robot_id).strip()
        if requested_robot_id != saved_robot_id:
            old_profile_path = str(self.robot_profile_files.get(requested_robot_id, '') or '').strip()
            if not old_profile_path:
                old_profile_path = self._profile_file_path(requested_robot_id)
            new_profile_path = self.robot_profile_files.get(saved_robot_id, '')
            if old_profile_path and old_profile_path != new_profile_path and os.path.exists(old_profile_path):
                try:
                    os.remove(old_profile_path)
                except Exception as exc:
                    self.get_logger().warn(f'Failed removing old profile file during rename: {exc}')
            self.robot_profile_files.pop(requested_robot_id, None)

        if compile_requested and compile_ok and apply_runtime:
            runtime_result = self._start_managed_robot_state_publisher(
                profile=saved_profile,
                compiled_urdf_path=saved_profile.get('urdf_artifact', {}).get('compiled_urdf_path', ''),
            )
            saved_profile['urdf_artifact']['runtime'] = {
                'ok': bool(runtime_result.get('ok')),
                'message': str(runtime_result.get('message', '') or ''),
                'node_name': str(runtime_result.get('node_name', '') or ''),
                'namespace': str(runtime_result.get('namespace', '') or ''),
                'required_tf_frames': list(runtime_result.get('required_tf_frames', [])),
                'observed_tf_frames': list(runtime_result.get('observed_tf_frames', [])),
                'missing_tf_frames': list(runtime_result.get('missing_tf_frames', [])),
                'pid': int(runtime_result.get('pid', 0) or 0),
            }
            try:
                saved_profile = self._save_profile_to_disk(saved_profile, allow_overwrite=True, increment_version=False)
            except Exception as exc:
                self.get_logger().warn(f'Failed persisting robot editor runtime status: {exc}')

        if str(self.active_robot_profile_id or '').strip() in {requested_robot_id, saved_robot_id}:
            self._apply_profile_config_to_memory(saved_profile)
            self._save_profile_registry_state(saved_robot_id)

        profiles_by_id = self._load_robot_profiles()
        if saved_robot_id in profiles_by_id:
            saved_profile = profiles_by_id[saved_robot_id]

        if compile_requested:
            if compile_ok:
                message = f'Robot editor saved for "{saved_robot_id}". {compile_message}'
            else:
                detail = compile_message or 'Generated URDF did not pass validation'
                message = f'Robot editor draft saved for "{saved_robot_id}". URDF not updated: {detail}'
        else:
            message = f'Robot editor draft saved for "{saved_robot_id}"'

        return self._ok(
            True,
            message,
            robot_id=saved_robot_id,
            active_robot_id=saved_robot_id,
            profile_version=int(saved_profile.get('profile_version', 1) or 1),
            profile=copy.deepcopy(saved_profile),
            profiles_by_id=profiles_by_id,
            profile_fields=self._profile_fields_from_profile(saved_profile),
            robot_builder=copy.deepcopy(saved_profile.get('robot_builder', {})),
            compile_requested=bool(compile_requested),
            compile_ok=bool(compile_ok),
            compile_message=compile_message,
            validation=validation,
            runtime=runtime_result or {},
        )

    def _load_settings_payload_from_disk(self) -> Dict[str, Any]:
        configured = str(self._get_or_declare_parameter('settings_file', os.path.expanduser('~/ui_mappings.yaml')) or '').strip()
        self.settings_file = os.path.expanduser(configured or '~/ui_mappings.yaml')

        raw_payload: Dict[str, Any] = {}
        if os.path.exists(self.settings_file):
            try:
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    loaded = yaml.safe_load(f) or {}
                if isinstance(loaded, dict):
                    raw_payload = loaded
            except Exception as exc:
                self.get_logger().warn(f'Failed to load settings file: {exc}')

        if 'mappings' in raw_payload or 'topics' in raw_payload:
            mappings_raw = raw_payload.get('mappings', {})
            topics_raw = raw_payload.get('topics', {})
        else:
            mappings_raw = raw_payload
            topics_raw = {}

        mappings = self._merge_mappings_with_defaults(self._sanitize_mappings(mappings_raw))
        topics = self._merge_topics_with_defaults(topics_raw)
        return {'mappings': mappings, 'topics': topics}

    def _load_topic_config_from_disk(self) -> Dict[str, str]:
        payload = self._load_settings_payload_from_disk()
        self.cached_settings_mappings = payload.get('mappings', self._merge_mappings_with_defaults({}))
        return self._merge_topics_with_defaults(payload.get('topics', {}))

    def _topic(self, key: str) -> str:
        configured = str(self.topic_config.get(key, '') or '').strip()
        if configured:
            return configured
        return str(self.DEFAULT_TOPICS.get(key, '') or '').strip()

    def _map_frame(self) -> str:
        profile = self.active_robot_profile if isinstance(self.active_robot_profile, dict) else {}
        sensor_frames = profile.get('sensor_frames', {}) if isinstance(profile, dict) else {}
        if isinstance(sensor_frames, dict):
            configured = str(sensor_frames.get('map', '') or '').strip()
            if configured:
                return configured
        return 'map'

    def __init__(self):
        super().__init__('next_ros2ws_web_bridge')

        self.cb_group = ReentrantCallbackGroup()
        self.ui_root = _resolve_ui_root()
        self.bringup_package, _bringup_share = _resolve_bringup_package_share(
            search_roots=[self.ui_root, os.getcwd()]
        )
        self.maps_dir = os.path.join(self.ui_root, 'maps')
        self.description_dir = os.path.join(self.ui_root, 'description')
        self.downloads_dir = os.path.join(os.path.expanduser('~'), 'Downloads', 'DownloadedMaps')
        self.robot_profiles_dir = ''
        self.robot_profile_registry_file = ''
        self.robot_profile_files = {}
        self.robot_config_deploy_dir = ''
        self.robot_config_snapshots_dir = ''
        self.robot_config_active_dir = ''
        self.robot_config_registry_file = ''
        self.active_robot_profile_id = ''
        self.active_robot_profile = None
        self.managed_rsp_process = None
        self.managed_rsp_profile_id = ''
        self.managed_rsp_param_file = ''
        self.db_manager = None
        self._initialize_profile_db_storage()
        self.topic_config = self._load_topic_config_from_disk()
        self._initialize_profile_registry()
        self._initialize_config_deploy_registry()
        self.path_metadata_file = os.path.join(self.ui_root, 'config', 'path_metadata.yaml')
        self._path_metadata_lock = threading.Lock()
        self.path_metadata = self._load_path_metadata()

        # Service clients (core API only)
        self.save_zone_client = self.create_client(
            SaveZone,
            self._topic('service_save_zone'),
            callback_group=self.cb_group,
        )
        self.delete_zone_client = self.create_client(
            DeleteZone,
            self._topic('service_delete_zone'),
            callback_group=self.cb_group,
        )
        self.update_zone_params_client = self.create_client(
            UpdateZoneParams,
            self._topic('service_update_zone_params'),
            callback_group=self.cb_group,
        )
        self.reorder_zones_client = self.create_client(
            ReorderZones,
            self._topic('service_reorder_zones'),
            callback_group=self.cb_group,
        )
        self.get_zones_client = self.create_client(
            GetZones,
            self._topic('service_get_zones'),
            callback_group=self.cb_group,
        )

        self.save_path_client = self.create_client(
            SavePath,
            self._topic('service_save_path'),
            callback_group=self.cb_group,
        )
        self.delete_path_client = self.create_client(
            DeletePath,
            self._topic('service_delete_path'),
            callback_group=self.cb_group,
        )
        self.get_paths_client = self.create_client(
            GetPaths,
            self._topic('service_get_paths'),
            callback_group=self.cb_group,
        )

        self.save_layout_client = self.create_client(
            SaveLayout,
            self._topic('service_save_layout'),
            callback_group=self.cb_group,
        )
        self.load_layout_client = self.create_client(
            LoadLayout,
            self._topic('service_load_layout'),
            callback_group=self.cb_group,
        )
        self.delete_layout_client = self.create_client(
            DeleteLayout,
            self._topic('service_delete_layout'),
            callback_group=self.cb_group,
        )
        self.get_layouts_client = self.create_client(
            GetLayouts,
            self._topic('service_get_layouts'),
            callback_group=self.cb_group,
        )

        self.set_control_mode_client = self.create_client(
            SetControlMode,
            self._topic('service_set_control_mode'),
            callback_group=self.cb_group,
        )

        self.emergency_stop_client = self.create_client(
            SetBool,
            self._topic('service_safety_emergency_stop'),
            callback_group=self.cb_group,
        )
        self.safety_override_client = self.create_client(
            SetBool,
            self._topic('service_safety_override'),
            callback_group=self.cb_group,
        )
        self.safety_status_client = self.create_client(
            Trigger,
            self._topic('service_safety_status'),
            callback_group=self.cb_group,
        )
        self.safety_clear_state_client = self.create_client(
            Trigger,
            self._topic('service_safety_clear_state'),
            callback_group=self.cb_group,
        )

        self.set_stack_mode_client = self.create_client(
            SetStackMode,
            self._topic('service_stack_set_mode'),
            callback_group=self.cb_group,
        )
        self.stack_status_client = self.create_client(
            Trigger,
            self._topic('service_stack_status'),
            callback_group=self.cb_group,
        )
        self.stack_shutdown_client = self.create_client(
            Trigger,
            self._topic('service_stack_shutdown'),
            callback_group=self.cb_group,
        )

        self.upload_map_client = self.create_client(
            UploadMap,
            self._topic('service_map_upload'),
            callback_group=self.cb_group,
        )
        self.set_active_map_client = self.create_client(
            SetActiveMap,
            self._topic('service_map_set_active'),
            callback_group=self.cb_group,
        )
        self.get_active_map_client = self.create_client(
            GetActiveMap,
            self._topic('service_map_get_active'),
            callback_group=self.cb_group,
        )
        self.slam_save_map_client = self.create_client(
            SlamSaveMap,
            self._topic('service_slam_save_map'),
            callback_group=self.cb_group,
        )
        self.slam_serialize_client = self.create_client(
            SlamSerializePoseGraph,
            self._topic('service_slam_serialize'),
            callback_group=self.cb_group,
        )
        self.slam_deserialize_client = self.create_client(
            SlamDeserializePoseGraph,
            self._topic('service_slam_deserialize'),
            callback_group=self.cb_group,
        )
        self.map_server_load_map_client = self.create_client(
            LoadMap,
            self._topic('service_map_server_load_map'),
            callback_group=self.cb_group,
        )
        self.clear_global_costmap_client = self.create_client(
            ClearEntireCostmap,
            self._topic('service_clear_global_costmap'),
            callback_group=self.cb_group,
        )
        self.clear_local_costmap_client = self.create_client(
            ClearEntireCostmap,
            self._topic('service_clear_local_costmap'),
            callback_group=self.cb_group,
        )
        self.auto_reloc_client = self.create_client(
            Trigger,
            self._topic('service_auto_relocate'),
            callback_group=self.cb_group,
        )
        self.shelf_commit_client = self.create_client(
            Trigger,
            '/shelf/commit',
            callback_group=self.cb_group,
        )
        self.shelf_enable_client = self.create_client(
            SetBool,
            '/shelf/set_enabled',
            callback_group=self.cb_group,
        )
        self.shelf_simple_start_client = self.create_client(
            Trigger,
            '/shelf_simple/start',
            callback_group=self.cb_group,
        )

        self.get_map_layers_client = self.create_client(
            GetMapLayers,
            self._topic('service_map_layers_get'),
            callback_group=self.cb_group,
        )
        self.add_map_layer_client = self.create_client(
            AddMapLayerObject,
            self._topic('service_map_layers_add'),
            callback_group=self.cb_group,
        )
        self.delete_map_layer_client = self.create_client(
            DeleteMapLayerObject,
            self._topic('service_map_layers_delete'),
            callback_group=self.cb_group,
        )
        self.clear_map_layer_client = self.create_client(
            ClearMapLayer,
            self._topic('service_map_layers_clear'),
            callback_group=self.cb_group,
        )

        self.start_sequence_client = self.create_client(
            StartSequence,
            self._topic('service_mission_start_sequence'),
            callback_group=self.cb_group,
        )
        self.stop_sequence_client = self.create_client(
            StopSequence,
            self._topic('service_mission_stop_sequence'),
            callback_group=self.cb_group,
        )
        self.get_sequence_status_client = self.create_client(
            GetSequenceStatus,
            self._topic('service_mission_sequence_status'),
            callback_group=self.cb_group,
        )
        self.get_mission_status_client = self.create_client(
            GetMissionStatus,
            self._topic('service_mission_status'),
            callback_group=self.cb_group,
        )
        self.resume_mission_client = self.create_client(
            ResumeMission,
            self._topic('service_mission_resume'),
            callback_group=self.cb_group,
        )
        self.clear_mission_state_client = self.create_client(
            ClearMissionState,
            self._topic('service_mission_clear'),
            callback_group=self.cb_group,
        )

        self.editor_preview_client = self.create_client(
            GetEditorMapPreview,
            self._topic('service_editor_preview'),
            callback_group=self.cb_group,
        )
        self.editor_overwrite_client = self.create_client(
            OverwriteEditorMap,
            self._topic('service_editor_overwrite'),
            callback_group=self.cb_group,
        )
        self.editor_save_current_client = self.create_client(
            SaveCurrentMap,
            self._topic('service_editor_save_current'),
            callback_group=self.cb_group,
        )
        self.editor_reload_client = self.create_client(
            ReloadEditorMap,
            self._topic('service_editor_reload'),
            callback_group=self.cb_group,
        )
        self.editor_export_client = self.create_client(
            ExportEditorMap,
            self._topic('service_editor_export'),
            callback_group=self.cb_group,
        )

        self.get_ui_mappings_client = self.create_client(
            GetUiMappings,
            self._topic('service_settings_get_mappings'),
            callback_group=self.cb_group,
        )
        self.save_ui_mappings_client = self.create_client(
            SaveUiMappings,
            self._topic('service_settings_save_mappings'),
            callback_group=self.cb_group,
        )

        # Publishers owned by the web bridge for user commands.
        self.goal_pub = self.create_publisher(PoseStamped, self._topic('publisher_goal_pose'), 10)
        self.initial_pose_pub = self.create_publisher(PoseWithCovarianceStamped, self._topic('publisher_initial_pose'), 10)
        self.cmd_vel_manual_pub = self.create_publisher(Twist, self._topic('publisher_cmd_vel_manual'), 10)
        self.shelf_tick_pub = self.create_publisher(BoolMsg, '/tick', 10)
        self.shelf_detected = False
        self.shelf_detected_updated_at = 0.0
        self.shelf_status = self._default_shelf_status_payload()
        self.shelf_status_updated_at = 0.0

        # Action clients.
        self.go_to_zone_action_client = ActionClient(
            self,
            GoToZoneAction,
            self._topic('action_go_to_zone'),
            callback_group=self.cb_group,
        )
        self.follow_path_action_client = ActionClient(
            self,
            FollowPathAction,
            self._topic('action_follow_path'),
            callback_group=self.cb_group,
        )

        # State subscriptions for UI status.
        map_qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )
        self.create_subscription(OccupancyGrid, self._topic('subscription_map'), self._map_callback, map_qos)
        self.create_subscription(NavPath, self._topic('subscription_plan'), self._plan_callback, 10)
        self.create_subscription(NavPath, self._topic('subscription_global_plan'), self._plan_callback, 10)
        self.create_subscription(String, self._topic('subscription_control_mode'), self._control_mode_callback, 10)
        self.create_subscription(BoolMsg, self._topic('subscription_set_estop'), self._estop_callback, 10)
        self.create_subscription(BoolMsg, '/shelf_detected', self._shelf_detected_callback, 10)
        self.create_subscription(String, '/shelf/status_json', self._shelf_status_callback, 10)
        self.create_subscription(
            PoseWithCovarianceStamped,
            self._topic('subscription_amcl_pose'),
            self._amcl_pose_callback,
            10,
        )
        slam_pose_topic = str(self._topic('subscription_slam_pose') or '').strip()
        pose_fallback_topic = str(self._topic('subscription_pose_fallback') or '').strip()
        if slam_pose_topic:
            self.create_subscription(
                PoseWithCovarianceStamped,
                slam_pose_topic,
                self._slam_pose_callback,
                10,
            )
        if pose_fallback_topic and pose_fallback_topic != slam_pose_topic:
            self.create_subscription(
                PoseWithCovarianceStamped,
                pose_fallback_topic,
                self._pose_fallback_callback,
                10,
            )
        self.create_subscription(Odometry, self._topic('subscription_odom_filtered'), self._odom_filtered_callback, 10)
        self.create_subscription(Odometry, self._topic('subscription_odom'), self._odom_callback, 10)
        self.enable_scan_overlay_processing = bool(
            self.declare_parameter('enable_scan_overlay_processing', False).value
        )
        self.scan_overlay_min_update_sec = max(
            0.0,
            float(self.declare_parameter('scan_overlay_min_update_sec', 0.22).value),
        )
        self.scan_overlay_primary_topic = self._topic('subscription_scan_overlay_primary')
        self.scan_overlay_fallback_topic = (
            self._topic('subscription_scan_overlay_fallback')
            or self._topic('subscription_scan_combined')
        )
        self.scan_overlay_fallback_source = (
            str(self.scan_overlay_fallback_topic or 'scan_fallback').strip().lstrip('/') or 'scan_fallback'
        )
        if (
            self.scan_overlay_primary_topic
            and self.scan_overlay_primary_topic != self.scan_overlay_fallback_topic
        ):
            if self._scan_overlay_topic_uses_pointcloud(self.scan_overlay_primary_topic):
                self.create_subscription(
                    PointCloud2,
                    self.scan_overlay_primary_topic,
                    self._scan_overlay_primary_cloud_callback,
                    10,
                )
            else:
                self.create_subscription(
                    LaserScan,
                    self.scan_overlay_primary_topic,
                    self._scan_overlay_primary_callback,
                    10,
                )
        if self.scan_overlay_fallback_topic:
            self.create_subscription(
                LaserScan,
                self.scan_overlay_fallback_topic,
                self._scan_combined_callback,
                10,
            )

        self.current_mode = 'unknown'
        self.estop_active = False
        self.robot_pose = None
        self.robot_pose_source = None
        self.pose_prefer_tf = bool(self.declare_parameter('pose_prefer_tf', True).value)
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        self._last_pose_tf_miss_log_sec = 0.0
        self.pose_source_fallback_timeout_sec = max(
            0.1,
            float(self.declare_parameter('pose_source_fallback_timeout_sec', 1.5).value),
        )
        self.pose_cache_hold_sec = max(
            0.0,
            float(self.declare_parameter('pose_cache_hold_sec', 3.0).value),
        )
        self.manual_pose_lock_sec = max(
            0.0,
            float(self.declare_parameter('manual_pose_lock_sec', 8.0).value),
        )
        self.manual_pose_publish_count = max(
            1,
            int(self.declare_parameter('manual_pose_publish_count', 5).value),
        )
        self.manual_pose_publish_interval_sec = max(
            0.0,
            float(self.declare_parameter('manual_pose_publish_interval_sec', 0.08).value),
        )
        self.manual_pose_lock_until = 0.0
        self.localization_confidence = 0.0
        self.localization_pose_seen = False
        self.covariance_confidence = 0.0
        self.scan_map_match_confidence = 0.0
        self.scan_map_match_known_ratio = 0.0
        self.scan_map_match_hit_ratio = 0.0
        self.scan_map_match_mean_dist_m = 0.0
        self.scan_map_match_mean_likelihood = 0.0
        self._scan_map_match_ema = None
        self.last_pose_time = None
        self.last_requested_pose = None
        self.last_requested_pose_source = None
        self.last_requested_pose_time = None

        self.latest_map_msg = None
        self.latest_plan_path = []
        self.latest_plan_path_time = None
        self.scan_front_points = []
        self.scan_rear_points = []
        self.scan_overlay_origin = None
        self.scan_overlay_source = ''
        self._last_scan_origin_pose = None
        self._last_primary_overlay_update_monotonic = 0.0
        self._last_scan_overlay_update_monotonic = 0.0

        # UI scan denoising controls.
        self.ui_scan_stride = max(1, int(self.declare_parameter('ui_scan_stride', 2).value))
        self.ui_scan_outlier_jump_m = max(0.05, float(self.declare_parameter('ui_scan_outlier_jump_m', 0.35).value))
        self.ui_scan_max_points = max(120, int(self.declare_parameter('ui_scan_max_points', 900).value))
        self.ui_scan_min_range_m = max(0.0, float(self.declare_parameter('ui_scan_min_range_m', 0.08).value))
        self.ui_scan_median_window = max(1, int(self.declare_parameter('ui_scan_median_window', 5).value))
        self.scan_overlay_primary_hold_sec = max(
            0.0,
            float(self.declare_parameter('scan_overlay_primary_hold_sec', 0.60).value),
        )

        # NDT-style scan-to-map consistency model (for localization confidence).
        self._ndt_cell_size_m = max(0.10, float(self.declare_parameter('ndt_cell_size_m', 0.28).value))
        self._ndt_occ_threshold = int(self.declare_parameter('ndt_occ_threshold', 60).value)
        self._ndt_min_points_per_cell = max(3, int(self.declare_parameter('ndt_min_points_per_cell', 5).value))
        self._ndt_sigma_floor_m = max(0.03, float(self.declare_parameter('ndt_sigma_floor_m', 0.10).value))
        self._ndt_hit_mahalanobis_sq = max(0.5, float(self.declare_parameter('ndt_hit_mahalanobis_sq', 2.5).value))
        self._ndt_mahalanobis_clip_sq = max(
            self._ndt_hit_mahalanobis_sq,
            float(self.declare_parameter('ndt_mahalanobis_clip_sq', 9.0).value),
        )
        self._ndt_neighborhood_cells = max(0, int(self.declare_parameter('ndt_neighborhood_cells', 1).value))
        self._ndt_scan_sample_cap = max(80, int(self.declare_parameter('ndt_scan_sample_cap', 260).value))
        self._ndt_models = {}
        self._ndt_map_key = None
        self._ndt_model_count = 0
        self._ndt_occupied_points = 0
        self._ndt_origin_x = 0.0
        self._ndt_origin_y = 0.0
        self._ndt_map_resolution = 0.0
        self._ndt_map_width = 0
        self._ndt_map_height = 0
        self._ndt_map_data = None

        self.path_state = {
            'active': False,
            'accepted': False,
            'loop_mode': False,
            'loop_type': 'none',
            'current_index': 0,
            'total_waypoints': 0,
            'message': '',
            'success': False,
        }

        self._nav_goal_lock = threading.RLock()
        self._goto_goal_handle = None
        self._goto_goal_seq = 0
        self._goto_send_inflight = False
        self._goto_cancel_requested = False
        self._path_goal_handle = None
        self._path_goal_seq = 0
        self._follow_cancel_requested = False
        self._loop_path_points = []
        self._loop_display_waypoint_count = 0
        self._loop_stop_requested = False
        self._loop_restart_inflight = False
        self._loop_enabled = False
        self._loop_type = 'none'
        self._loop_current_index = 0
        self._loop_target_index = 1
        self._loop_direction = 1
        self._loop_retry_fail_streak = 0

        self._estop_enforce_lock = threading.Lock()
        self._estop_enforce_pending = False
        self._estop_enforce_inflight = False
        self._estop_enforce_timer = self.create_timer(
            0.05,
            self._process_estop_stop_all,
            callback_group=self.cb_group,
        )
        self._estop_enforce_timer.cancel()

        # ROS service-call dispatch model:
        # - Request threads enqueue service calls onto a bounded worker pool.
        # - Backpressure is applied when queue capacity is exhausted.
        # - Calls to the same rclpy client are serialized with per-client locks.
        self.service_call_worker_threads = max(
            2,
            int(self.declare_parameter('service_call_worker_threads', 8).value),
        )
        self.service_call_max_pending = max(
            self.service_call_worker_threads,
            int(self.declare_parameter('service_call_max_pending', 64).value),
        )
        self._service_call_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.service_call_worker_threads,
            thread_name_prefix='rosbridge_svc',
        )
        self._service_call_slots = threading.BoundedSemaphore(self.service_call_max_pending)
        self._service_call_client_locks: Dict[int, threading.Lock] = {}
        self._service_call_client_locks_guard = threading.Lock()
        self._service_calls_inflight = 0
        self._service_calls_inflight_guard = threading.Lock()
        self._service_overload_last_log_sec = 0.0
        self._service_overload_log_interval_sec = 2.0

        self.get_logger().info('ROS bridge ready: web routes use core ROS APIs only.')
        self.get_logger().info(f'Settings file: {self.settings_file}')
        self.get_logger().info(f'Robot profiles dir: {self.robot_profiles_dir}')
        self.get_logger().info(f'Robot deploy dir: {self.robot_config_deploy_dir}')
        self.get_logger().info(f'Active robot profile: {self.active_robot_profile_id or "none"}')
        self.get_logger().info(
            f'ROS service workers={self.service_call_worker_threads}, '
            f'max_pending={self.service_call_max_pending}'
        )

    def destroy_node(self):
        try:
            self._stop_managed_robot_state_publisher()
        except Exception:
            pass
        try:
            self._service_call_executor.shutdown(wait=False)
        except Exception:
            pass
        return super().destroy_node()

    # ---------- generic helpers ----------

    @staticmethod
    def _ok(ok: bool, message: str, **extra: Any) -> Dict[str, Any]:
        payload = {'ok': bool(ok), 'message': str(message)}
        payload.update(extra)
        return payload

    @staticmethod
    def _normalize_mode(mode: str) -> str:
        raw = (mode or '').strip().lower()
        return RosBridge.MODE_ALIASES.get(raw, raw)

    def _relaunch_workspace_candidates(self, workspace_name: str) -> List[str]:
        raw_name = str(workspace_name or '').strip()
        if not raw_name:
            return []

        token = re.sub(r'[^A-Z0-9]+', '_', raw_name.upper()).strip('_')
        env_candidates = [
            os.getenv(f'NEXT_{token}_ROOT', ''),
            os.getenv(f'{token}_ROOT', ''),
        ]
        parent_dir = os.path.abspath(os.path.dirname(self.ui_root))
        file_candidates = [
            os.path.join(os.path.expanduser('~'), raw_name),
            os.path.join(parent_dir, raw_name),
            os.path.join(self.ui_root, raw_name),
        ]

        candidates: List[str] = []
        seen: Set[str] = set()
        for candidate in [*env_candidates, *file_candidates]:
            absolute = os.path.abspath(os.path.expanduser(str(candidate or '').strip()))
            if not absolute or absolute in seen:
                continue
            seen.add(absolute)
            candidates.append(absolute)
        return candidates

    def _resolve_relaunch_workspace(self, workspace_name: str) -> str:
        for candidate in self._relaunch_workspace_candidates(workspace_name):
            if os.path.isdir(candidate):
                return candidate
        return ''

    @staticmethod
    def _resolve_workspace_setup_script(workspace_dir: str) -> str:
        root = os.path.abspath(os.path.expanduser(str(workspace_dir or '').strip()))
        if not root:
            return ''
        candidates = [
            os.path.join(root, 'install', 'setup.bash'),
            os.path.join(root, 'install', 'local_setup.bash'),
            os.path.join(root, 'devel', 'setup.bash'),
        ]
        for candidate in candidates:
            if os.path.isfile(candidate):
                return candidate
        return ''

    @staticmethod
    def _terminal_command_variants(terminal: str, *, title: str, command: str) -> List[List[str]]:
        quoted = shlex.quote(command)
        if terminal == 'gnome-terminal':
            return [[terminal, '--title', title, '--', 'bash', '-lc', command]]
        if terminal == 'mate-terminal':
            return [[terminal, '--title', title, '--', 'bash', '-lc', command]]
        if terminal == 'konsole':
            return [[terminal, '-p', f'tabtitle={title}', '-e', 'bash', '-lc', command]]
        if terminal == 'xfce4-terminal':
            return [[terminal, '--title', title, '--command', f'bash -lc {quoted}']]
        if terminal == 'lxterminal':
            return [[terminal, '--title', title, '-e', f'bash -lc {quoted}']]
        if terminal == 'xterm':
            return [[terminal, '-T', title, '-e', 'bash', '-lc', command]]
        if terminal == 'x-terminal-emulator':
            return [[terminal, '-T', title, '-e', 'bash', '-lc', command]]
        return []

    def _launch_command_in_new_terminal(self, *, title: str, command: str, cwd: str) -> Dict[str, Any]:
        has_display = bool(os.getenv('DISPLAY') or os.getenv('WAYLAND_DISPLAY'))
        if not has_display:
            return self._ok(
                False,
                'No graphical display detected. Cannot open a new terminal window.',
            )

        terminal_order = [
            'x-terminal-emulator',
            'gnome-terminal',
            'konsole',
            'xfce4-terminal',
            'mate-terminal',
            'lxterminal',
            'xterm',
        ]
        attempts: List[str] = []

        for terminal in terminal_order:
            terminal_bin = shutil.which(terminal)
            if not terminal_bin:
                continue
            variants = self._terminal_command_variants(terminal, title=title, command=command)
            for invocation in variants:
                try:
                    proc = subprocess.Popen(
                        invocation,
                        cwd=cwd or None,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True,
                    )
                    time.sleep(0.15)
                    rc = proc.poll()
                    if rc is None or int(rc) == 0:
                        return self._ok(
                            True,
                            f'Opened new terminal using {terminal}',
                            terminal=terminal,
                            pid=int(proc.pid),
                        )
                    attempts.append(f'{terminal}: exited early with code {rc}')
                except Exception as exc:
                    attempts.append(f'{terminal}: {exc}')

        if not attempts:
            return self._ok(
                False,
                'No supported terminal emulator found (tried x-terminal-emulator, gnome-terminal, konsole, xfce4-terminal, mate-terminal, lxterminal, xterm).',
            )

        return self._ok(
            False,
            'Failed to open terminal emulator',
            attempts=attempts,
        )

    def launch_relaunch_target(self, target: str):
        aliases = {
            'front': 'lidar_front',
            'lidar_front': 'lidar_front',
            'lidar-front': 'lidar_front',
            'rear': 'lidar_rear',
            'lidar_rear': 'lidar_rear',
            'lidar-rear': 'lidar_rear',
            'imu': 'imu',
            'imu_localization': 'imu',
            'imu-localization': 'imu',
            'shelf': 'shelf_detection',
            'shelf_detection': 'shelf_detection',
            'shelf-detection': 'shelf_detection',
            'shelf_detector': 'shelf_detection',
            'next2_shelf': 'shelf_detection',
        }
        normalized_target = aliases.get(str(target or '').strip().lower(), '')
        if not normalized_target:
            return self._ok(
                False,
                'Unknown relaunch target',
                available_targets=sorted(list(self.RELAUNCH_TARGETS.keys())),
            )

        spec = self.RELAUNCH_TARGETS.get(normalized_target, {})
        label = str(spec.get('label', normalized_target))
        workspace_name = str(spec.get('workspace', '') or '').strip()
        launch_command = str(spec.get('launch_command', '') or '').strip()
        if not workspace_name or not launch_command:
            return self._ok(False, f'Relaunch target "{normalized_target}" is not configured')

        workspace_dir = self._resolve_relaunch_workspace(workspace_name)
        if not workspace_dir:
            return self._ok(
                False,
                f'Workspace "{workspace_name}" not found',
                workspace_candidates=self._relaunch_workspace_candidates(workspace_name),
            )

        setup_script = self._resolve_workspace_setup_script(workspace_dir)
        if not setup_script:
            return self._ok(
                False,
                f'No setup script found in workspace "{workspace_dir}"',
                expected=[
                    os.path.join(workspace_dir, 'install', 'setup.bash'),
                    os.path.join(workspace_dir, 'install', 'local_setup.bash'),
                    os.path.join(workspace_dir, 'devel', 'setup.bash'),
                ],
            )

        shell_command = (
            f'cd {shlex.quote(workspace_dir)}'
            f' && source {shlex.quote(setup_script)}'
            f' && {launch_command}; rc=$?; echo; '
            f'echo "[relaunch] {label} exited with code ${rc}"; exec bash'
        )
        launch_result = self._launch_command_in_new_terminal(
            title=f'Relaunch: {label}',
            command=shell_command,
            cwd=workspace_dir,
        )
        if not bool(launch_result.get('ok')):
            return launch_result

        return self._ok(
            True,
            f'Relaunch started for {label}',
            target=normalized_target,
            workspace=workspace_dir,
            setup_script=setup_script,
            launch_command=launch_command,
            terminal=str(launch_result.get('terminal', '')),
            terminal_pid=launch_result.get('pid'),
        )

    def _wait_future(self, future, timeout_sec: float):
        end_time = time.time() + max(0.0, float(timeout_sec))
        while time.time() < end_time:
            if future.done():
                return True
            time.sleep(0.01)
        return False

    def _service_client_lock(self, client) -> threading.Lock:
        client_key = id(client)
        with self._service_call_client_locks_guard:
            lock = self._service_call_client_locks.get(client_key)
            if lock is None:
                lock = threading.Lock()
                self._service_call_client_locks[client_key] = lock
        return lock

    def _call_service_sync(self, client, request, *, wait_timeout: float, response_timeout: float):
        client_lock = self._service_client_lock(client)
        with client_lock:
            if not client.wait_for_service(timeout_sec=wait_timeout):
                return None, 'service_not_available'
            ros_future = client.call_async(request)

        if not self._wait_future(ros_future, response_timeout):
            return None, 'timeout'
        try:
            return ros_future.result(), None
        except Exception as exc:
            return None, f'exception: {exc}'

    def _call_service(self, client, request, *, wait_timeout: float = 1.0, response_timeout: float = 4.0):
        if not self._service_call_slots.acquire(blocking=False):
            now_sec = time.time()
            should_log = (now_sec - self._service_overload_last_log_sec) >= self._service_overload_log_interval_sec
            if should_log:
                with self._service_calls_inflight_guard:
                    inflight = int(self._service_calls_inflight)
                self.get_logger().warn(
                    f'Service dispatch overloaded: inflight={inflight}, '
                    f'max_pending={self.service_call_max_pending}'
                )
                self._service_overload_last_log_sec = now_sec
            return None, 'timeout'

        with self._service_calls_inflight_guard:
            self._service_calls_inflight += 1

        def _runner():
            try:
                return self._call_service_sync(
                    client,
                    request,
                    wait_timeout=wait_timeout,
                    response_timeout=response_timeout,
                )
            finally:
                with self._service_calls_inflight_guard:
                    self._service_calls_inflight = max(0, self._service_calls_inflight - 1)
                self._service_call_slots.release()

        try:
            task = self._service_call_executor.submit(_runner)
        except Exception as exc:
            with self._service_calls_inflight_guard:
                self._service_calls_inflight = max(0, self._service_calls_inflight - 1)
            self._service_call_slots.release()
            return None, f'exception: {exc}'

        join_timeout = max(0.05, float(wait_timeout) + float(response_timeout) + 0.25)
        try:
            return task.result(timeout=join_timeout)
        except concurrent.futures.TimeoutError:
            return None, 'timeout'
        except Exception as exc:
            return None, f'exception: {exc}'

    def _call_setbool(self, client, enabled: bool, *, unavailable_msg: str, timeout_msg: str):
        req = SetBool.Request()
        req.data = bool(enabled)
        response, err = self._call_service(client, req, wait_timeout=1.0, response_timeout=3.0)
        if err == 'service_not_available':
            return self._ok(False, unavailable_msg)
        if err == 'timeout':
            return self._ok(False, timeout_msg)
        if response is None:
            return self._ok(False, 'Service call failed')
        return self._ok(bool(response.success), str(response.message), success=bool(response.success))

    def _scan_overlay_topic_uses_pointcloud(self, topic: Optional[str]) -> bool:
        name = str(topic or '').strip()
        if not name:
            return False

        union_cloud_topic = str(self._topic('subscription_scan_union_cloud') or '').strip()
        if union_cloud_topic and name == union_cloud_topic:
            return True

        lowered = name.lower()
        return (
            lowered.endswith('_cloud')
            or lowered.endswith('/cloud')
            or 'point_cloud' in lowered
        )

    # ---------- callbacks ----------

    def _map_callback(self, msg: OccupancyGrid):
        self.latest_map_msg = msg
        if self.enable_scan_overlay_processing:
            self._rebuild_ndt_map_model(msg)

    def _rebuild_ndt_map_model(self, map_msg: OccupancyGrid):
        width = int(map_msg.info.width)
        height = int(map_msg.info.height)
        resolution = float(map_msg.info.resolution)
        if width <= 0 or height <= 0 or resolution <= 0.0:
            self._ndt_models = {}
            self._ndt_map_data = None
            self._ndt_model_count = 0
            self._ndt_occupied_points = 0
            return

        origin_x = float(map_msg.info.origin.position.x)
        origin_y = float(map_msg.info.origin.position.y)
        data = map_msg.data
        if not data:
            self._ndt_models = {}
            self._ndt_map_data = None
            self._ndt_model_count = 0
            self._ndt_occupied_points = 0
            return

        map_load = map_msg.info.map_load_time
        map_key = (
            width,
            height,
            round(resolution, 6),
            round(origin_x, 4),
            round(origin_y, 4),
            int(map_load.sec),
            int(map_load.nanosec),
            len(data),
        )
        if map_key == self._ndt_map_key and self._ndt_models:
            return

        cell_size = float(self._ndt_cell_size_m)
        occ_threshold = int(self._ndt_occ_threshold)
        min_pts = int(self._ndt_min_points_per_cell)
        sigma2_floor = float(self._ndt_sigma_floor_m) ** 2

        accum = {}
        occupied_points = 0

        for my in range(height):
            row = my * width
            wy = origin_y + (float(my) + 0.5) * resolution
            for mx in range(width):
                if int(data[row + mx]) < occ_threshold:
                    continue
                wx = origin_x + (float(mx) + 0.5) * resolution
                cx = int((wx - origin_x) / cell_size)
                cy = int((wy - origin_y) / cell_size)
                key = (cx, cy)
                rec = accum.get(key)
                if rec is None:
                    rec = [0, 0.0, 0.0, 0.0, 0.0, 0.0]
                    accum[key] = rec
                rec[0] += 1
                rec[1] += wx
                rec[2] += wy
                rec[3] += wx * wx
                rec[4] += wy * wy
                rec[5] += wx * wy
                occupied_points += 1

        models = {}
        for key, rec in accum.items():
            n = int(rec[0])
            if n < min_pts:
                continue

            inv_n = 1.0 / float(n)
            mx = rec[1] * inv_n
            my = rec[2] * inv_n
            var_x = max(sigma2_floor, (rec[3] * inv_n) - (mx * mx))
            var_y = max(sigma2_floor, (rec[4] * inv_n) - (my * my))
            cov_xy = (rec[5] * inv_n) - (mx * my)

            det = (var_x * var_y) - (cov_xy * cov_xy)
            if det <= (sigma2_floor * sigma2_floor * 0.25):
                cov_xy = 0.0
                det = max(sigma2_floor * sigma2_floor, var_x * var_y)
            if det <= 1e-12:
                continue

            inv00 = var_y / det
            inv11 = var_x / det
            inv01 = -cov_xy / det
            models[key] = (mx, my, inv00, inv01, inv11)

        self._ndt_models = models
        self._ndt_map_key = map_key
        self._ndt_model_count = len(models)
        self._ndt_occupied_points = int(occupied_points)
        self._ndt_origin_x = origin_x
        self._ndt_origin_y = origin_y
        self._ndt_map_resolution = resolution
        self._ndt_map_width = width
        self._ndt_map_height = height
        self._ndt_map_data = data

        self.get_logger().info(
            f'NDT model rebuilt: {self._ndt_model_count} cells from {self._ndt_occupied_points} occupied points '
            f'(cell={self._ndt_cell_size_m:.2f}m)'
        )

    def _plan_callback(self, msg: NavPath):
        self.latest_plan_path = [
            {
                'x': float(pose.pose.position.x),
                'y': float(pose.pose.position.y),
            }
            for pose in msg.poses
        ]
        self.latest_plan_path_time = time.time()

    def _control_mode_callback(self, msg: String):
        self.current_mode = (msg.data or '').strip().lower() or 'unknown'

    def _estop_callback(self, msg: BoolMsg):
        previous_state = bool(getattr(self, 'estop_active', False))
        self.estop_active = bool(msg.data)

        # On E-STOP rising edge, force-stop all active navigation tasks.
        if self.estop_active and (not previous_state):
            self._schedule_estop_stop_all()

    def _schedule_estop_stop_all(self):
        with self._estop_enforce_lock:
            if self._estop_enforce_pending or self._estop_enforce_inflight:
                return
            self._estop_enforce_pending = True

        try:
            self._estop_enforce_timer.reset()
        except Exception:
            self._process_estop_stop_all()

    def _process_estop_stop_all(self):
        with self._estop_enforce_lock:
            if (not self._estop_enforce_pending) or self._estop_enforce_inflight:
                try:
                    self._estop_enforce_timer.cancel()
                except Exception:
                    pass
                return
            self._estop_enforce_pending = False
            self._estop_enforce_inflight = True

        try:
            try:
                self._estop_enforce_timer.cancel()
            except Exception:
                pass
            self._enforce_estop_stop_all()
        finally:
            with self._estop_enforce_lock:
                self._estop_enforce_inflight = False
                rerun = bool(self.estop_active and self._estop_enforce_pending)

            if rerun:
                try:
                    self._estop_enforce_timer.reset()
                except Exception:
                    self._process_estop_stop_all()

    @staticmethod
    def _default_shelf_status_payload() -> Dict[str, Any]:
        return {
            'shelf_detected': False,
            'detector_enabled': False,
            'candidate_valid': False,
            'candidate_fresh': False,
            'candidate_consistent': False,
            'candidate_age_sec': -1.0,
            'hotspot_count': 0,
            'hotspot_points': [],
            'solver_ok': False,
            'last_reason': 'status_unavailable',
            'center_pose': None,
            'candidate_front_width_m': -1.0,
            'candidate_back_width_m': -1.0,
            'preferred_front_width_m': -1.0,
            'scan_frame_id': '',
            'committed_target_valid': False,
            'committed_target_age_sec': -1.0,
            'committed_target_pose': None,
            'max_intensity': 0.0,
            'frame_mismatch_warning': '',
            'trigger_topic': '/tick',
            'detected_topic': '/shelf_detected',
            'status_topic': '/shelf/status_json',
        }

    def _shelf_detected_callback(self, msg: BoolMsg):
        self.shelf_detected = bool(msg.data)
        self.shelf_detected_updated_at = time.time()
        if isinstance(self.shelf_status, dict):
            self.shelf_status['shelf_detected'] = bool(msg.data)
            self.shelf_status['candidate_valid'] = bool(msg.data)

    def _shelf_status_callback(self, msg: String):
        try:
            parsed = json.loads(str(msg.data or '{}'))
        except Exception as exc:
            self.get_logger().warn(f'Failed to parse shelf status JSON: {exc}')
            return

        if not isinstance(parsed, dict):
            self.get_logger().warn('Shelf status payload is not a JSON object.')
            return

        payload = self._default_shelf_status_payload()
        payload.update(parsed)
        payload['shelf_detected'] = bool(payload.get('candidate_valid', payload.get('shelf_detected', False)))
        payload['detector_enabled'] = bool(payload.get('detector_enabled', False))
        payload['candidate_valid'] = bool(payload.get('candidate_valid', False))
        payload['candidate_fresh'] = bool(payload.get('candidate_fresh', False))
        payload['candidate_consistent'] = bool(payload.get('candidate_consistent', False))
        payload['solver_ok'] = bool(payload.get('solver_ok', False))
        payload['committed_target_valid'] = bool(payload.get('committed_target_valid', False))

        self.shelf_status = payload
        self.shelf_status_updated_at = time.time()
        self.shelf_detected = bool(payload.get('shelf_detected', False))
        self.shelf_detected_updated_at = self.shelf_status_updated_at

    def _enforce_estop_stop_all(self):
        try:
            details = {
                'goto': self.cancel_go_to_zone(reason='GoToZone canceled by E-STOP'),
                'path': self.stop_path(),
                # Keep mission state for resume; do not clear it.
                'sequence': self.stop_sequence(),
            }
            self.get_logger().warn(
                'E-STOP active: enforced stop of navigation tasks '
                f"(goto_ok={details['goto'].get('ok')}, "
                f"path_ok={details['path'].get('ok')}, "
                f"sequence_ok={details['sequence'].get('ok')})"
            )
        except Exception as exc:
            self.get_logger().warn(f'Failed E-STOP enforcement stop-all: {exc}')

    @staticmethod
    def _yaw_from_quaternion(q) -> float:
        siny_cosp = 2.0 * (q.w * q.z + q.x * q.y)
        cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
        return float(math.atan2(siny_cosp, cosy_cosp))

    @staticmethod
    def _covariance_to_confidence(covariance) -> float:
        try:
            pos_var = float(covariance[0] + covariance[7])
        except Exception:
            pos_var = 0.0
        cov_confidence = 100.0 * math.exp(-2.2 * max(0.0, pos_var))
        return max(0.0, min(100.0, cov_confidence))

    @staticmethod
    def _pose_source_priority(source: Optional[str]) -> int:
        priorities = {
            'amcl': 40,
            'slam_toolbox': 35,
            'pose': 30,
            'manual_initialpose': 25,
            'auto_relocate': 25,
            'odometry_filtered': 20,
            'odom': 10,
        }
        return int(priorities.get(str(source or '').strip().lower(), 0))

    def _should_accept_pose_source(self, source: str) -> bool:
        source = str(source or '').strip().lower() or 'unknown'
        now = time.time()
        current_source = str(self.robot_pose_source or '').strip().lower() or None

        # After a manual initial pose, hold against low-priority odom/fallback jumps
        # until localization source (AMCL/SLAM) catches up.
        if self.manual_pose_lock_until > now:
            if source not in {'manual_initialpose', 'auto_relocate', 'amcl', 'slam_toolbox'}:
                return False

        if current_source is None:
            return True

        low_priority_sources = {'pose', 'odometry_filtered', 'odom'}
        high_priority_sources = {'manual_initialpose', 'auto_relocate', 'amcl', 'slam_toolbox'}

        # Do not let odom/fallback sources steal pose once localization/manual pose is active.
        if current_source in high_priority_sources and source in low_priority_sources:
            return False

        incoming_priority = self._pose_source_priority(source)
        current_priority = self._pose_source_priority(current_source)
        if incoming_priority >= current_priority:
            return True

        # Fallback aging is allowed only among low-priority sources.
        if current_source in low_priority_sources and source in low_priority_sources:
            if self.last_pose_time is None:
                return True
            age = max(0.0, now - self.last_pose_time)
            return age >= float(self.pose_source_fallback_timeout_sec)

        return False

    def _apply_robot_pose(self, x: float, y: float, theta: float, source: str, force: bool = False) -> bool:
        source = str(source or '').strip().lower() or 'unknown'
        if (not force) and (not self._should_accept_pose_source(source)):
            return False

        self.robot_pose = {
            'x': float(x),
            'y': float(y),
            'theta': float(theta),
        }
        self.robot_pose_source = source
        self.last_pose_time = time.time()
        return True

    def _record_requested_pose(self, x: float, y: float, theta: float, source: str):
        self.last_requested_pose = {
            'x': float(x),
            'y': float(y),
            'theta': float(theta),
        }
        self.last_requested_pose_source = str(source or '').strip().lower() or 'unknown'
        self.last_requested_pose_time = time.time()

    @staticmethod
    def _extract_pose_from_status_text(message: str) -> Optional[Tuple[float, float, float]]:
        text = str(message or '').strip()
        if not text:
            return None
        match = re.search(
            r'x\s*=\s*([-+]?\d+(?:\.\d+)?)\s*,\s*y\s*=\s*([-+]?\d+(?:\.\d+)?)\s*,\s*yaw\s*=\s*([-+]?\d+(?:\.\d+)?)',
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        try:
            x = float(match.group(1))
            y = float(match.group(2))
            yaw = float(match.group(3))
        except Exception:
            return None
        if not (math.isfinite(x) and math.isfinite(y) and math.isfinite(yaw)):
            return None
        return x, y, yaw

    def _preferred_base_frames(self, frame_hint: Optional[str] = None) -> List[str]:
        candidates: List[str] = []
        hint = self._normalize_frame_name(frame_hint)
        if hint:
            candidates.append(hint)

        profile = self.active_robot_profile if isinstance(self.active_robot_profile, dict) else {}
        configured_base = self._normalize_frame_name(profile.get('base_frame', ''))
        if configured_base and configured_base not in candidates:
            candidates.append(configured_base)

        for fallback in ('base_link', 'base_footprint'):
            if fallback not in candidates:
                candidates.append(fallback)
        return candidates

    def _lookup_pose_from_tf(
        self,
        frame_hint: Optional[str] = None,
        stamp_msg=None,
        *,
        allow_base_fallback: bool = True,
        allow_latest_fallback: bool = True,
    ) -> Optional[Tuple[float, float, float]]:
        if not getattr(self, 'tf_buffer', None):
            return None

        map_frame = self._normalize_frame_name(self._map_frame()) or 'map'
        candidates: List[str] = []
        hint = self._normalize_frame_name(frame_hint)
        if hint:
            candidates.append(hint)
        if allow_base_fallback:
            for fallback in self._preferred_base_frames(frame_hint):
                if fallback not in candidates:
                    candidates.append(fallback)

        if not candidates:
            return None

        query_times = []
        if stamp_msg is not None:
            try:
                query_times.append(RosTime.from_msg(stamp_msg))
            except Exception:
                pass
        if allow_latest_fallback or not query_times:
            query_times.append(RosTime())

        for query_time in query_times:
            for base_frame in candidates:
                try:
                    transform = self.tf_buffer.lookup_transform(
                        map_frame,
                        base_frame,
                        query_time,
                        timeout=RosDuration(seconds=0.15),
                    )
                except TransformException:
                    continue
                except Exception:
                    continue

                try:
                    tx = float(transform.transform.translation.x)
                    ty = float(transform.transform.translation.y)
                    yaw = self._yaw_from_quaternion(transform.transform.rotation)
                except Exception:
                    continue

                if math.isfinite(tx) and math.isfinite(ty) and math.isfinite(yaw):
                    return tx, ty, yaw

        return None

    def _log_pose_tf_miss(self, source: str, frame_hint: Optional[str] = None):
        now = time.time()
        if (now - float(getattr(self, '_last_pose_tf_miss_log_sec', 0.0))) < 2.0:
            return
        self._last_pose_tf_miss_log_sec = now
        map_frame = self._normalize_frame_name(self._map_frame()) or 'map'
        hint = self._normalize_frame_name(frame_hint) or '<unknown>'
        self.get_logger().warn(
            f'Pose update skipped for source="{source}" (missing TF {map_frame} -> {hint})'
        )

    def _update_pose_from_covariance_msg(self, msg: PoseWithCovarianceStamped, source: str):
        pose = msg.pose.pose
        theta = self._yaw_from_quaternion(pose.orientation)
        x = float(pose.position.x)
        y = float(pose.position.y)

        source_frame = self._normalize_frame_name(getattr(msg.header, 'frame_id', ''))
        map_frame = self._normalize_frame_name(self._map_frame()) or 'map'
        if self.pose_prefer_tf and source_frame and source_frame != map_frame:
            tf_pose = self._lookup_pose_from_tf()
            if tf_pose is None:
                self._log_pose_tf_miss(source, source_frame)
                return
            x, y, theta = tf_pose

        if not self._apply_robot_pose(x, y, theta, source):
            return

        source_key = str(source or '').strip().lower()
        if source_key in {'amcl', 'slam_toolbox'}:
            self.localization_pose_seen = True
            if self.manual_pose_lock_until > 0.0:
                self.get_logger().info(
                    f'Pose accepted from {source_key}: x={x:.3f}, '
                    f'y={y:.3f}, yaw={theta:.3f}'
                )
            self.manual_pose_lock_until = 0.0

        self.covariance_confidence = self._covariance_to_confidence(msg.pose.covariance)
        self._recompute_localization_confidence()

    def _update_pose_from_odometry_msg(self, msg: Odometry, source: str):
        pose = msg.pose.pose
        theta = self._yaw_from_quaternion(pose.orientation)
        x = float(pose.position.x)
        y = float(pose.position.y)

        if self.pose_prefer_tf:
            child_frame = self._normalize_frame_name(getattr(msg, 'child_frame_id', ''))
            tf_pose = self._lookup_pose_from_tf(child_frame)
            if tf_pose is None:
                self._log_pose_tf_miss(source, child_frame)
                return
            x, y, theta = tf_pose

        if not self._apply_robot_pose(x, y, theta, source):
            return

        self.covariance_confidence = self._covariance_to_confidence(msg.pose.covariance)
        self._recompute_localization_confidence()

    def _amcl_pose_callback(self, msg: PoseWithCovarianceStamped):
        self._update_pose_from_covariance_msg(msg, 'amcl')

    def _slam_pose_callback(self, msg: PoseWithCovarianceStamped):
        self._update_pose_from_covariance_msg(msg, 'slam_toolbox')

    def _pose_fallback_callback(self, msg: PoseWithCovarianceStamped):
        self._update_pose_from_covariance_msg(msg, 'pose')

    def _odom_filtered_callback(self, msg: Odometry):
        self._update_pose_from_odometry_msg(msg, 'odometry_filtered')

    def _odom_callback(self, msg: Odometry):
        self._update_pose_from_odometry_msg(msg, 'odom')

    def _scan_overlay_primary_recent(self) -> bool:
        if self._last_primary_overlay_update_monotonic <= 0.0:
            return False
        age_sec = time.monotonic() - float(self._last_primary_overlay_update_monotonic)
        return age_sec <= float(self.scan_overlay_primary_hold_sec)

    def _update_scan_overlay_points(self, points: List[Dict[str, float]], source: str) -> bool:
        now_mono = time.monotonic()
        if self.scan_overlay_min_update_sec > 0.0 and self._last_scan_overlay_update_monotonic > 0.0:
            age_sec = now_mono - float(self._last_scan_overlay_update_monotonic)
            if age_sec < float(self.scan_overlay_min_update_sec):
                return False

        if not points:
            return False
        self.scan_front_points = points
        self.scan_rear_points = []
        self.scan_overlay_origin = (
            dict(self._last_scan_origin_pose)
            if isinstance(self._last_scan_origin_pose, dict)
            else None
        )
        self.scan_overlay_source = str(source or '').strip()
        if self.enable_scan_overlay_processing:
            self._update_scan_map_match(points)
        self._last_scan_overlay_update_monotonic = now_mono
        return True

    def _update_scan_overlay(self, msg: LaserScan, source: str) -> bool:
        # Convert the scan frame to map using TF at the scan timestamp so the
        # web overlay stays aligned during rotation the same way RViz does.
        return self._update_scan_overlay_points(self._scan_to_map_points(msg), source)

    def _update_pointcloud_overlay(self, msg: PointCloud2, source: str) -> bool:
        return self._update_scan_overlay_points(self._pointcloud_to_map_points(msg), source)

    def _scan_overlay_primary_callback(self, msg: LaserScan):
        if self._update_scan_overlay(msg, 'scan_slam'):
            self._last_primary_overlay_update_monotonic = time.monotonic()

    def _scan_overlay_primary_cloud_callback(self, msg: PointCloud2):
        if self._update_pointcloud_overlay(msg, 'scan_combined_cloud'):
            self._last_primary_overlay_update_monotonic = time.monotonic()

    def _scan_combined_callback(self, msg: LaserScan):
        if self._scan_overlay_primary_recent():
            return
        self._update_scan_overlay(msg, self.scan_overlay_fallback_source)

    def _recompute_localization_confidence(self):
        cov_conf = max(0.0, min(100.0, float(self.covariance_confidence)))

        if self._scan_map_match_ema is None:
            self.localization_confidence = cov_conf
            return

        map_conf_raw = max(0.0, min(100.0, float(self._scan_map_match_ema) * 100.0))
        known_ratio = max(0.0, min(1.0, float(self.scan_map_match_known_ratio)))
        hit_ratio = max(0.0, min(1.0, float(self.scan_map_match_hit_ratio)))
        mean_dist = max(0.0, float(self.scan_map_match_mean_dist_m))

        # Reliability gate for NDT likelihood confidence.
        reliability = 0.55 + 0.45 * ((0.60 * hit_ratio) + (0.40 * known_ratio))
        if mean_dist > 0.30:
            reliability *= max(0.35, 1.0 - ((mean_dist - 0.30) / 0.70))

        map_conf = max(0.0, min(100.0, map_conf_raw * reliability))

        # Make scan-to-map consistency the primary term while still anchoring to AMCL covariance.
        map_weight = 0.55 if known_ratio >= 0.45 else 0.45
        combined = (1.0 - map_weight) * cov_conf + map_weight * map_conf

        # If NDT alignment is clearly good, prevent confidence from staying artificially low.
        if map_conf >= 80.0 and hit_ratio >= 0.55 and known_ratio >= 0.45:
            combined = max(combined, min(95.0, map_conf * 0.92))

        # Guardrails against obvious wrong-global-pose states.
        if (hit_ratio < 0.12) or (known_ratio < 0.20):
            combined = min(combined, 50.0)
        if mean_dist > 0.45:
            combined = min(combined, 45.0)
        if cov_conf >= 85.0 and map_conf <= 30.0:
            combined = min(combined, 60.0)

        self.localization_confidence = max(0.0, min(100.0, combined))

    def _estimate_scan_map_consistency(self, points: List[Dict[str, float]]):
        if not points or not self._ndt_models or self._ndt_map_data is None:
            return None

        width = int(self._ndt_map_width)
        height = int(self._ndt_map_height)
        resolution = float(self._ndt_map_resolution)
        if width <= 0 or height <= 0 or resolution <= 0.0:
            return None

        origin_x = float(self._ndt_origin_x)
        origin_y = float(self._ndt_origin_y)
        data = self._ndt_map_data

        stride = max(1, len(points) // int(self._ndt_scan_sample_cap))
        occ_threshold = int(self._ndt_occ_threshold)
        neighborhood = int(self._ndt_neighborhood_cells)
        hit_mahal_sq = float(self._ndt_hit_mahalanobis_sq)
        clip_mahal_sq = float(self._ndt_mahalanobis_clip_sq)
        cell_size = float(self._ndt_cell_size_m)

        sampled = 0
        known = 0
        matched = 0
        hits = 0
        like_sum = 0.0
        mahal_sum = 0.0

        for pt in points[::stride]:
            try:
                x = float(pt.get('x', 0.0))
                y = float(pt.get('y', 0.0))
            except Exception:
                continue

            mx = int((x - origin_x) / resolution)
            my = int((y - origin_y) / resolution)
            if mx < 0 or my < 0 or mx >= width or my >= height:
                continue

            sampled += 1
            idx = my * width + mx
            cell = int(data[idx])
            if cell < 0:
                continue

            known += 1

            cx = int((x - origin_x) / cell_size)
            cy = int((y - origin_y) / cell_size)

            best_d2 = None
            for dy in range(-neighborhood, neighborhood + 1):
                for dx in range(-neighborhood, neighborhood + 1):
                    model = self._ndt_models.get((cx + dx, cy + dy))
                    if model is None:
                        continue

                    mean_x, mean_y, inv00, inv01, inv11 = model
                    ux = x - mean_x
                    uy = y - mean_y
                    d2 = (ux * ((inv00 * ux) + (inv01 * uy))) + (uy * ((inv01 * ux) + (inv11 * uy)))
                    if d2 < 0.0:
                        d2 = 0.0

                    # Light penalty for matches taken from neighboring NDT cells.
                    if dx != 0 or dy != 0:
                        d2 += 0.15 * float((dx * dx) + (dy * dy))

                    if (best_d2 is None) or (d2 < best_d2):
                        best_d2 = d2

            if best_d2 is None:
                # No nearby NDT Gaussian model around this endpoint.
                if cell >= occ_threshold:
                    best_d2 = 0.0
                else:
                    continue

            matched += 1
            clipped_d2 = min(float(best_d2), clip_mahal_sq)
            like = math.exp(-0.5 * clipped_d2)
            like_sum += like
            mahal_sum += clipped_d2
            if clipped_d2 <= hit_mahal_sq:
                hits += 1

        if sampled < 16 or known < 10 or matched < 8:
            return None

        known_ratio = float(known) / float(sampled)
        match_ratio = float(matched) / float(max(known, 1))
        hit_ratio = float(hits) / float(max(matched, 1))
        mean_like = float(like_sum) / float(max(matched, 1))
        mean_mahal = float(mahal_sum) / float(max(matched, 1))

        sigma_ref = max(float(self._ndt_sigma_floor_m), float(self._ndt_cell_size_m) * 0.35)
        mean_dist = math.sqrt(max(0.0, mean_mahal)) * sigma_ref

        score = (
            mean_like
            * (0.55 + 0.45 * hit_ratio)
            * (0.55 + 0.45 * match_ratio)
            * (0.65 + 0.35 * known_ratio)
        )
        if match_ratio < 0.25:
            score *= max(0.2, match_ratio / 0.25)

        score = max(0.0, min(1.0, float(score)))
        return score, known_ratio, hit_ratio, mean_dist, mean_like

    def _update_scan_map_match(self, points: List[Dict[str, float]]):
        estimated = self._estimate_scan_map_consistency(points)
        if estimated is None:
            return

        score, known_ratio, hit_ratio, mean_dist, mean_like = estimated

        if self._scan_map_match_ema is None:
            self._scan_map_match_ema = score
        else:
            # Responsive smoothing: allow faster rise when alignment improves.
            prev = float(self._scan_map_match_ema)
            if score >= prev:
                alpha = 0.30 if (score - prev) > 0.12 else 0.22
            else:
                alpha = 0.16
            self._scan_map_match_ema = ((1.0 - alpha) * prev) + (alpha * score)

        self.scan_map_match_confidence = max(0.0, min(100.0, float(self._scan_map_match_ema) * 100.0))
        self.scan_map_match_known_ratio = max(0.0, min(1.0, float(known_ratio)))
        self.scan_map_match_hit_ratio = max(0.0, min(1.0, float(hit_ratio)))
        self.scan_map_match_mean_dist_m = max(0.0, float(mean_dist))
        self.scan_map_match_mean_likelihood = max(0.0, min(1.0, float(mean_like)))
        self._recompute_localization_confidence()

    def _scan_to_map_points(self, msg: LaserScan) -> List[Dict[str, float]]:
        source_frame = self._normalize_frame_name(getattr(msg.header, 'frame_id', ''))
        allow_latest_fallback = source_frame in {'base_footprint', 'base_link'}
        tf_pose = self._lookup_pose_from_tf(
            source_frame,
            getattr(msg.header, 'stamp', None),
            allow_base_fallback=False,
            allow_latest_fallback=allow_latest_fallback,
        ) if source_frame else None

        if tf_pose is None:
            self._last_scan_origin_pose = None
            return []

        px, py, theta = tf_pose
        self._last_scan_origin_pose = {
            'x': float(px),
            'y': float(py),
            'theta': float(theta),
        }

        c = math.cos(theta)
        s = math.sin(theta)

        ranges = [float(r) for r in msg.ranges]
        count = len(ranges)
        if count == 0:
            return []

        stride = max(1, int(self.ui_scan_stride))
        max_points = max(1, int(self.ui_scan_max_points))
        # Prevent angular truncation: if a dense scan would exceed max_points,
        # increase stride up front so we still cover the full sweep.
        if count > max_points:
            stride = max(stride, int(math.ceil(float(count) / float(max_points))))
        jump = float(self.ui_scan_outlier_jump_m)
        valid_min = max(float(msg.range_min), float(self.ui_scan_min_range_m))
        valid_max = float(msg.range_max)
        median_window = max(1, int(self.ui_scan_median_window))
        median_half = median_window // 2
        median_cache = {}

        def filtered_range(index: int):
            if index < 0 or index >= count:
                return float('nan')
            if index in median_cache:
                return median_cache[index]

            raw = float(ranges[index])
            if median_window <= 1:
                value = raw if (math.isfinite(raw) and (valid_min < raw < valid_max)) else float('nan')
                median_cache[index] = value
                return value

            start = max(0, index - median_half)
            end = min(count - 1, index + median_half)
            values = []
            for j in range(start, end + 1):
                value = float(ranges[j])
                if math.isfinite(value) and (valid_min < value < valid_max):
                    values.append(value)

            if not values:
                median_cache[index] = float('nan')
                return median_cache[index]

            values.sort()
            median_cache[index] = float(values[len(values) // 2])
            return median_cache[index]

        points = []
        for i in range(0, count, stride):
            rng = filtered_range(i)
            if not math.isfinite(rng):
                continue

            # Reject isolated spikes that differ sharply from both immediate neighbors.
            if 0 < i < (count - 1):
                prev = filtered_range(i - 1)
                nxt = filtered_range(i + 1)
                prev_ok = math.isfinite(prev)
                next_ok = math.isfinite(nxt)
                if prev_ok and next_ok:
                    if abs(rng - prev) > jump and abs(rng - nxt) > jump:
                        continue

            angle = msg.angle_min + float(i) * float(msg.angle_increment)
            bx = float(rng * math.cos(angle))
            by = float(rng * math.sin(angle))
            mx = px + c * bx - s * by
            my = py + s * bx + c * by
            points.append({'x': mx, 'y': my, 'range': float(rng)})

        if len(points) > max_points:
            sample_step = max(1, int(math.ceil(float(len(points)) / float(max_points))))
            points = points[::sample_step][:max_points]

        return points

    def _pointcloud_to_map_points(self, msg: PointCloud2) -> List[Dict[str, float]]:
        source_frame = self._normalize_frame_name(getattr(msg.header, 'frame_id', ''))
        allow_latest_fallback = source_frame in {'base_footprint', 'base_link'}
        tf_pose = self._lookup_pose_from_tf(
            source_frame,
            getattr(msg.header, 'stamp', None),
            allow_base_fallback=False,
            allow_latest_fallback=allow_latest_fallback,
        ) if source_frame else None

        if tf_pose is None:
            self._last_scan_origin_pose = None
            return []

        px, py, theta = tf_pose
        self._last_scan_origin_pose = {
            'x': float(px),
            'y': float(py),
            'theta': float(theta),
        }

        c = math.cos(theta)
        s = math.sin(theta)
        stride = max(1, int(self.ui_scan_stride))
        max_points = max(1, int(self.ui_scan_max_points))
        min_range = float(self.ui_scan_min_range_m)

        field_names = [str(field.name) for field in getattr(msg, 'fields', [])]
        has_intensity = 'intensity' in field_names
        requested_fields = ('x', 'y', 'intensity') if has_intensity else ('x', 'y')

        points: List[Dict[str, float]] = []
        for idx, entry in enumerate(point_cloud2.read_points(msg, field_names=requested_fields, skip_nans=True)):
            if (idx % stride) != 0:
                continue
            try:
                lx = float(entry[0])
                ly = float(entry[1])
            except Exception:
                continue
            if not (math.isfinite(lx) and math.isfinite(ly)):
                continue

            range_m = math.hypot(lx, ly)
            if range_m <= min_range:
                continue

            intensity = float(entry[2]) if has_intensity and len(entry) > 2 else 0.0
            wx = px + (c * lx) - (s * ly)
            wy = py + (s * lx) + (c * ly)
            points.append({
                'x': float(wx),
                'y': float(wy),
                'range': float(range_m),
                'intensity': float(intensity),
            })

        if len(points) > max_points:
            sample_step = max(1, int(math.ceil(float(len(points)) / float(max_points))))
            points = points[::sample_step][:max_points]

        return points

    # ---------- conversion helpers ----------

    def _zone_msg_to_dict(self, zone_msg) -> Dict[str, Any]:
        return {
            'position': {
                'x': float(zone_msg.position.x),
                'y': float(zone_msg.position.y),
                'z': float(zone_msg.position.z),
            },
            'orientation': {
                'x': float(zone_msg.orientation.x),
                'y': float(zone_msg.orientation.y),
                'z': float(zone_msg.orientation.z),
                'w': float(zone_msg.orientation.w),
            },
            'frame_id': str(zone_msg.frame_id or 'map'),
            'type': str(zone_msg.type or 'normal'),
            'speed': float(zone_msg.speed),
            'action': str(zone_msg.action or ''),
            'charge_duration': float(zone_msg.charge_duration),
        }

    @staticmethod
    def _path_msg_to_dict(path_msg) -> List[Dict[str, float]]:
        return [
            {
                'x': float(point.x),
                'y': float(point.y),
            }
            for point in path_msg.points
        ]

    def _layout_msg_to_dict(self, layout_msg) -> Dict[str, Any]:
        return {
            'description': str(layout_msg.description or ''),
            'map_path': str(layout_msg.map_path or ''),
            'zones': {
                str(zone_msg.name): self._zone_msg_to_dict(zone_msg)
                for zone_msg in layout_msg.zones
                if str(zone_msg.name).strip()
            },
            'paths': {
                str(path_msg.name): self._path_msg_to_dict(path_msg)
                for path_msg in layout_msg.paths
                if str(path_msg.name).strip()
            },
        }

    # ---------- map helpers ----------

    def _occupancy_to_png_payload(self, map_msg: OccupancyGrid):
        if Image is None:
            return None

        width = int(map_msg.info.width)
        height = int(map_msg.info.height)
        if width <= 0 or height <= 0:
            return None

        pixels = []
        for value in map_msg.data:
            if value < 0:
                px = 205
            elif value == 0:
                px = 255
            elif value >= 100:
                px = 0
            else:
                px = int(255.0 * (100.0 - float(value)) / 100.0)
            pixels.append(px)

        image = Image.new('L', (width, height))
        image.putdata(pixels)
        image = image.transpose(Image.FLIP_TOP_BOTTOM)

        out = io.BytesIO()
        image.convert('RGB').save(out, format='PNG')
        encoded = base64.b64encode(out.getvalue()).decode('ascii')

        return {
            'image': encoded,
            'width': width,
            'height': height,
            'resolution': float(map_msg.info.resolution),
            'origin': [
                float(map_msg.info.origin.position.x),
                float(map_msg.info.origin.position.y),
                float(self._yaw_from_quaternion(map_msg.info.origin.orientation)),
            ],
        }

    def _resolve_active_map_yaml(self) -> Optional[str]:
        default_map = os.path.join(self.maps_dir, 'smr_map.yaml')
        config_path = os.path.join(self.ui_root, 'config', 'active_map_config.yaml')

        candidate = default_map
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as cfg_file:
                    config = yaml.safe_load(cfg_file) or {}
                configured = str(config.get('active_map', '') or '').strip()
                if configured:
                    if os.path.isabs(configured):
                        candidate = configured
                    else:
                        candidate = os.path.join(self.maps_dir, configured)
        except Exception as exc:
            self.get_logger().warn(f'Failed reading active_map_config.yaml: {exc}')

        candidate = _canonicalize_map_yaml_path(candidate, self.ui_root)
        if os.path.exists(candidate):
            return candidate
        default_map = _canonicalize_map_yaml_path(default_map, self.ui_root)
        if os.path.exists(default_map):
            return default_map
        return None

    def _static_map_to_png_payload(self) -> Optional[Dict[str, Any]]:
        if Image is None:
            return None

        yaml_path = self._resolve_active_map_yaml()
        if not yaml_path:
            return None

        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                map_yaml = yaml.safe_load(f) or {}
        except Exception as exc:
            self.get_logger().warn(f'Failed to read map yaml "{yaml_path}": {exc}')
            return None

        image_ref = str(map_yaml.get('image', '') or '').strip()
        if not image_ref:
            return None

        if os.path.isabs(image_ref):
            image_path = image_ref
        else:
            image_path = os.path.join(os.path.dirname(yaml_path), image_ref)

        if not os.path.exists(image_path):
            return None

        try:
            image = Image.open(image_path)
            if map_yaml.get('negate', 0) == 1:
                image = Image.eval(image, lambda x: 255 - x)
            image = image.convert('RGB')

            out = io.BytesIO()
            image.save(out, format='PNG')
            encoded = base64.b64encode(out.getvalue()).decode('ascii')

            origin = map_yaml.get('origin', [0.0, 0.0, 0.0])
            if not isinstance(origin, list):
                origin = [0.0, 0.0, 0.0]
            while len(origin) < 3:
                origin.append(0.0)

            return {
                'image': encoded,
                'width': int(image.width),
                'height': int(image.height),
                'resolution': float(map_yaml.get('resolution', 0.05)),
                'origin': [float(origin[0]), float(origin[1]), float(origin[2])],
                'source': 'static',
            }
        except Exception as exc:
            self.get_logger().warn(f'Failed to render static map image "{image_path}": {exc}')
            return None

    def get_map_png(self):
        if self.latest_map_msg is not None:
            payload = self._occupancy_to_png_payload(self.latest_map_msg)
            if payload is not None:
                payload['ok'] = True
                payload['source'] = 'live'
                return payload

        payload = self._static_map_to_png_payload()
        if payload is not None:
            payload['ok'] = True
            return payload

        return self._ok(False, 'No live map data received and static map fallback is unavailable')

    def get_live_map(self):
        msg = self.latest_map_msg
        if msg is None:
            return {'ok': False, 'error': 'No live map data received yet'}

        return {
            'ok': True,
            'map': {
                'stamp': {
                    'sec': msg.header.stamp.sec,
                    'nanosec': msg.header.stamp.nanosec,
                },
                'info': {
                    'width': msg.info.width,
                    'height': msg.info.height,
                    'resolution': msg.info.resolution,
                    'origin': {
                        'position': {
                            'x': msg.info.origin.position.x,
                            'y': msg.info.origin.position.y,
                            'z': msg.info.origin.position.z,
                        },
                        'orientation': {
                            'x': msg.info.origin.orientation.x,
                            'y': msg.info.origin.orientation.y,
                            'z': msg.info.origin.orientation.z,
                            'w': msg.info.origin.orientation.w,
                        },
                    },
                },
                'data': list(msg.data),
            },
        }

    def get_live_path(self):
        return {
            'ok': True,
            'path': list(self.latest_plan_path),
        }

    @staticmethod
    def _decode_layers_json(raw: str) -> Dict[str, List[Dict[str, Any]]]:
        base = {
            'no_go_zones': [],
            'restricted': [],
            'slow_zones': [],
        }
        try:
            parsed = json.loads(str(raw or '{}'))
        except Exception:
            parsed = {}

        if not isinstance(parsed, dict):
            return base

        for key in base:
            value = parsed.get(key, [])
            if isinstance(value, list):
                base[key] = [item for item in value if isinstance(item, dict)]
        return base

    def get_filter_layers(self):
        req = GetMapLayers.Request()
        response, err = self._call_service(self.get_map_layers_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'MapLayerManager get service not available', **self._decode_layers_json('{}'))
        if err == 'timeout':
            return self._ok(False, 'MapLayerManager get service timed out', **self._decode_layers_json('{}'))
        if response is None:
            return self._ok(False, 'MapLayerManager get request failed', **self._decode_layers_json('{}'))

        layers = self._decode_layers_json(response.layers_json)
        return self._ok(bool(response.success), str(response.message), **layers)

    def add_filter_object(self, layer: str, obj: Dict[str, Any]):
        req = AddMapLayerObject.Request()
        req.layer = str(layer or '')
        req.object_json = json.dumps(obj if isinstance(obj, dict) else {})

        response, err = self._call_service(self.add_map_layer_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            result = self._ok(False, 'MapLayerManager add service not available', **self._decode_layers_json('{}'))
            result['result'] = result['message']
            return result
        if err == 'timeout':
            result = self._ok(False, 'MapLayerManager add service timed out', **self._decode_layers_json('{}'))
            result['result'] = result['message']
            return result
        if response is None:
            result = self._ok(False, 'MapLayerManager add request failed', **self._decode_layers_json('{}'))
            result['result'] = result['message']
            return result

        layers = self._decode_layers_json(response.layers_json)
        result = self._ok(bool(response.ok), str(response.message), object_id=str(response.object_id or ''), **layers)
        result['result'] = result['message']
        return result

    def delete_filter_object(self, layer: str, object_id: str):
        req = DeleteMapLayerObject.Request()
        req.layer = str(layer or '')
        req.object_id = str(object_id or '')

        response, err = self._call_service(self.delete_map_layer_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'MapLayerManager delete service not available', **self._decode_layers_json('{}'))
        if err == 'timeout':
            return self._ok(False, 'MapLayerManager delete service timed out', **self._decode_layers_json('{}'))
        if response is None:
            return self._ok(False, 'MapLayerManager delete request failed', **self._decode_layers_json('{}'))

        layers = self._decode_layers_json(response.layers_json)
        return self._ok(bool(response.ok), str(response.message), **layers)

    def clear_filter_layer(self, layer: str):
        req = ClearMapLayer.Request()
        req.layer = str(layer or 'all')

        response, err = self._call_service(self.clear_map_layer_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'MapLayerManager clear service not available', **self._decode_layers_json('{}'))
        if err == 'timeout':
            return self._ok(False, 'MapLayerManager clear service timed out', **self._decode_layers_json('{}'))
        if response is None:
            return self._ok(False, 'MapLayerManager clear request failed', **self._decode_layers_json('{}'))

        layers = self._decode_layers_json(response.layers_json)
        return self._ok(bool(response.ok), str(response.message), **layers)

    # ---------- zones ----------

    def get_zones(self):
        req = GetZones.Request()
        response, err = self._call_service(self.get_zones_client, req, wait_timeout=1.0, response_timeout=5.0)
        if err == 'service_not_available':
            return self._ok(False, 'GetZones service not available', zones={})
        if err == 'timeout':
            return self._ok(False, 'GetZones timed out', zones={})
        if response is None or not bool(response.success):
            msg = response.message if response is not None else 'GetZones failed'
            return self._ok(False, str(msg), zones={})

        zones = {}
        for zone_msg in response.zones:
            name = str(zone_msg.name).strip()
            if not name:
                continue
            zones[name] = self._zone_msg_to_dict(zone_msg)
        return self._ok(True, f'Retrieved {len(zones)} zones', zones=zones)

    def save_zone(
        self,
        name: str,
        x: float,
        y: float,
        theta: float,
        zone_type: str = 'normal',
        speed: float = 0.5,
        action: str = '',
        charge_duration: float = 0.0,
    ):
        zone_name = (name or '').strip()
        if not zone_name:
            return self._ok(False, 'Zone name is required')

        pose_msg = PoseStamped()
        pose_msg.header.frame_id = self._map_frame()
        pose_msg.header.stamp = self.get_clock().now().to_msg()
        pose_msg.pose.position.x = float(x)
        pose_msg.pose.position.y = float(y)
        pose_msg.pose.position.z = 0.0
        pose_msg.pose.orientation.x = 0.0
        pose_msg.pose.orientation.y = 0.0
        pose_msg.pose.orientation.z = math.sin(float(theta) / 2.0)
        pose_msg.pose.orientation.w = math.cos(float(theta) / 2.0)
        self.goal_pub.publish(pose_msg)
        time.sleep(0.05)

        req = SaveZone.Request()
        req.name = zone_name
        response, err = self._call_service(self.save_zone_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'Save zone service not available')
        if err == 'timeout':
            return self._ok(False, 'Save zone timed out')
        if response is None:
            return self._ok(False, 'Save zone failed')
        if not bool(response.ok):
            return self._ok(False, str(response.message))

        zone_type = str(zone_type or 'normal')
        needs_metadata_update = (
            zone_type != 'normal'
            or abs(float(speed) - 0.5) > 1e-6
            or bool(action)
            or float(charge_duration) > 0.0
        )
        if not needs_metadata_update:
            return self._ok(True, str(response.message))

        meta = self.update_zone_params(zone_name, zone_type, float(speed), str(action or ''), float(charge_duration))
        if meta.get('ok'):
            return self._ok(True, str(response.message))

        return self._ok(
            True,
            f'{response.message}. Metadata update warning: {meta.get("message", "unknown")}',
        )

    def delete_zone(self, name: str):
        req = DeleteZone.Request()
        req.name = str(name or '')
        response, err = self._call_service(self.delete_zone_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'Delete zone service not available')
        if err == 'timeout':
            return self._ok(False, 'Delete zone timed out')
        if response is None:
            return self._ok(False, 'Delete zone failed')
        return self._ok(bool(response.ok), str(response.message))

    def reorder_zones(self, zone_names: List[str]):
        req = ReorderZones.Request()
        req.zone_names = [str(name) for name in zone_names if str(name).strip()]
        response, err = self._call_service(self.reorder_zones_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'Reorder zones service not available')
        if err == 'timeout':
            return self._ok(False, 'Reorder zones timed out')
        if response is None:
            return self._ok(False, 'Reorder zones failed')
        return self._ok(bool(response.ok), str(response.message))

    def update_zone_params(self, name: str, zone_type: str, speed: float, action: str, charge_duration: float):
        req = UpdateZoneParams.Request()
        req.name = str(name or '')
        req.type = str(zone_type or 'normal')
        req.speed = float(speed)
        req.action = str(action or '')
        req.charge_duration = float(charge_duration)

        response, err = self._call_service(
            self.update_zone_params_client,
            req,
            wait_timeout=1.0,
            response_timeout=4.0,
        )
        if err == 'service_not_available':
            return self._ok(False, 'Update zone params service not available')
        if err == 'timeout':
            return self._ok(False, 'Update zone params timed out')
        if response is None:
            return self._ok(False, 'Update zone params failed')
        return self._ok(bool(response.ok), str(response.message))

    # ---------- go-to-zone action ----------

    def _go_to_zone_feedback(self, feedback_msg):
        # Retained for observability; route responses stay thin.
        feedback = feedback_msg.feedback
        self.get_logger().debug(
            f'GoToZone feedback: {feedback.status} ({feedback.progress * 100.0:.1f}%)'
        )

    def _go_to_zone_result_done(self, future, goal_seq: int):
        with self._nav_goal_lock:
            if goal_seq != int(self._goto_goal_seq):
                return
            self._goto_goal_handle = None
            self._goto_send_inflight = False
            self._goto_cancel_requested = False
        try:
            wrapped = future.result()
        except Exception as exc:
            self.get_logger().warn(f'GoToZone result failed: {exc}')
            return

        result = wrapped.result
        if bool(result.success):
            self.get_logger().info(f'GoToZone succeeded: {result.message}')
        else:
            self.get_logger().warn(f'GoToZone failed: {result.message}')

    def go_to_zone(self, name: str):
        if self.estop_active:
            return self._ok(False, 'E-STOP active. Reset before navigating.')

        zone_name = str(name or '').strip()
        if not zone_name:
            return self._ok(False, 'Zone name is required')

        # Navigation actions require NAV stack to be running.
        stack = self.get_stack_status()
        if bool(stack.get('ok')) and (not bool(stack.get('nav_running'))):
            return self._ok(
                False,
                f'NAV stack not running (mode={stack.get("mode", "unknown")}, nav_running={stack.get("nav_running")})',
            )

        if not self.go_to_zone_action_client.wait_for_server(timeout_sec=2.0):
            return self._ok(False, 'GoToZone action server not available')

        goal_msg = GoToZoneAction.Goal()
        goal_msg.name = zone_name

        with self._nav_goal_lock:
            self._goto_goal_seq += 1
            goal_seq = int(self._goto_goal_seq)
            self._goto_goal_handle = None
            self._goto_send_inflight = True
            self._goto_cancel_requested = False

        send_future = self.go_to_zone_action_client.send_goal_async(
            goal_msg,
            feedback_callback=self._go_to_zone_feedback,
        )
        if not self._wait_future(send_future, 3.0):
            with self._nav_goal_lock:
                if goal_seq == int(self._goto_goal_seq):
                    self._goto_send_inflight = False
                    self._goto_cancel_requested = False
            return self._ok(False, 'Timeout sending GoToZone goal')

        try:
            goal_handle = send_future.result()
        except Exception as exc:
            with self._nav_goal_lock:
                if goal_seq == int(self._goto_goal_seq):
                    self._goto_send_inflight = False
                    self._goto_cancel_requested = False
            return self._ok(False, f'Failed to send GoToZone goal: {exc}')

        if not goal_handle.accepted:
            with self._nav_goal_lock:
                if goal_seq == int(self._goto_goal_seq):
                    self._goto_send_inflight = False
                    self._goto_cancel_requested = False
            return self._ok(False, f'GoToZone goal rejected for "{zone_name}"')

        auto_cancel = False
        with self._nav_goal_lock:
            if goal_seq != int(self._goto_goal_seq):
                auto_cancel = True
            else:
                self._goto_goal_handle = goal_handle
                self._goto_send_inflight = False
                auto_cancel = bool(self._goto_cancel_requested or self.estop_active)

        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(lambda fut, _seq=goal_seq: self._go_to_zone_result_done(fut, _seq))

        if auto_cancel:
            cancel_ok, cancel_msg = self._cancel_client_goal(
                goal_handle,
                timeout_sec=2.0,
                label='GoToZone goal',
            )
            if not cancel_ok:
                self.get_logger().warn(cancel_msg)
            return self._ok(False, 'GoToZone dispatch raced with cancel/E-STOP; cancel requested')

        return self._ok(True, f'Navigation to zone "{zone_name}" started')

    def cancel_go_to_zone(self, reason: str = 'GoToZone canceled'):
        with self._nav_goal_lock:
            goal_handle = getattr(self, '_goto_goal_handle', None)
            send_inflight = bool(getattr(self, '_goto_send_inflight', False))
            if goal_handle is None and send_inflight:
                self._goto_cancel_requested = True
                return self._ok(True, 'GoToZone cancel queued while dispatch is in flight')

        if goal_handle is None:
            return self._ok(True, 'No active GoToZone goal')

        ok, message = self._cancel_client_goal(
            goal_handle,
            timeout_sec=2.0,
            label='GoToZone goal',
        )
        if not ok:
            return self._ok(False, message)

        with self._nav_goal_lock:
            if goal_handle is self._goto_goal_handle:
                self._goto_goal_handle = None
                self._goto_send_inflight = False
                self._goto_cancel_requested = False

        return self._ok(True, str(reason or 'GoToZone canceled'))

    # ---------- path action / CRUD ----------

    @staticmethod
    def _parse_path_points(path_points: Any):
        parsed: List[Dict[str, float]] = []
        if not isinstance(path_points, list):
            return parsed

        for point in path_points:
            x = None
            y = None
            zone_name = ''
            shelf_check = False

            if isinstance(point, dict):
                x = point.get('x')
                y = point.get('y')
                zone_name = str(
                    point.get('zone_name', point.get('zoneName', ''))
                ).strip()
                shelf_check = RosBridge._as_bool(
                    point.get('shelf_check', point.get('shelfCheck', False))
                )
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                x = point[0]
                y = point[1]
            else:
                continue

            try:
                entry: Dict[str, Any] = {'x': float(x), 'y': float(y)}
            except (TypeError, ValueError):
                continue

            if zone_name:
                entry['zone_name'] = zone_name
            if shelf_check:
                entry['shelf_check'] = True

            parsed.append(entry)
        return parsed

    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {'1', 'true', 'yes', 'on'}:
                return True
            if normalized in {'0', 'false', 'no', 'off', ''}:
                return False
        return bool(default)

    @classmethod
    def _normalize_loop_type(cls, loop_type: Any, *, loop_mode: Any = None) -> str:
        if isinstance(loop_type, (bool, int, float)) and bool(loop_type):
            return 'ping_pong'
        raw = str(loop_type or '').strip().lower()
        if raw in {'closed', 'closed_loop', 'closed-loop', 'loop_closed'}:
            return 'closed'
        if raw in {'ping_pong', 'pingpong', 'ping-pong', 'bounce', 'loop'}:
            return 'ping_pong'
        if cls._as_bool(loop_mode, default=False):
            return 'ping_pong'
        return 'none'

    @staticmethod
    def _is_path_explicitly_closed(points: List[Dict[str, float]], tolerance_m: float = 0.10) -> bool:
        if len(points) < 3:
            return False
        first = points[0]
        last = points[-1]
        try:
            fx = float(first.get('x', 0.0))
            fy = float(first.get('y', 0.0))
            lx = float(last.get('x', 0.0))
            ly = float(last.get('y', 0.0))
        except Exception:
            return False
        return math.hypot(lx - fx, ly - fy) <= max(0.0, float(tolerance_m))

    @staticmethod
    def _prepare_loop_execution_points(points: List[Dict[str, float]]) -> List[Dict[str, float]]:
        """Normalize loop anchors.

        If path is explicitly closed (last ~= first), drop terminal duplicate so runtime
        can handle the closing edge deterministically.
        """
        if len(points) < 2:
            return list(points)

        normalized = [dict(point) for point in points]
        if RosBridge._is_path_explicitly_closed(normalized):
            normalized = normalized[:-1]
        return normalized

    @staticmethod
    def _sanitize_curved_segments(
        raw_segments: Any,
        *,
        point_count: int,
        closed_loop: bool = False,
    ) -> List[int]:
        count = max(0, int(point_count))
        if count < 2:
            return []
        segment_count = count if bool(closed_loop) else (count - 1)
        max_segment_index = segment_count - 1
        if max_segment_index < 0:
            return []
        if not isinstance(raw_segments, (list, tuple, set)):
            return []

        cleaned: Set[int] = set()
        for raw_index in raw_segments:
            try:
                idx = int(raw_index)
            except Exception:
                continue
            if idx < 0 or idx > max_segment_index:
                continue
            cleaned.add(idx)
        return sorted(cleaned)

    @staticmethod
    def _sanitize_curved_segment_controls(
        raw_controls: Any,
        *,
        point_count: int,
        closed_loop: bool = False,
        allowed_segments: Optional[Set[int]] = None,
    ) -> Dict[str, List[Dict[str, float]]]:
        count = max(0, int(point_count))
        if count < 2:
            return {}

        segment_count = count if bool(closed_loop) else (count - 1)
        max_segment_index = segment_count - 1
        if max_segment_index < 0:
            return {}

        if not isinstance(raw_controls, dict):
            return {}

        if allowed_segments is None:
            allowed = set(range(0, max_segment_index + 1))
        else:
            allowed = set()
            for raw_idx in allowed_segments:
                try:
                    allowed.add(int(raw_idx))
                except Exception:
                    continue

        cleaned: Dict[str, List[Dict[str, float]]] = {}
        for raw_key, raw_value in raw_controls.items():
            try:
                idx = int(raw_key)
            except Exception:
                continue
            if idx < 0 or idx > max_segment_index or idx not in allowed:
                continue

            raw_list = raw_value if isinstance(raw_value, list) else [raw_value]
            valid_list: List[Dict[str, float]] = []
            for entry in raw_list:
                if not isinstance(entry, dict):
                    continue
                try:
                    x = float(entry.get('x'))
                    y = float(entry.get('y'))
                except Exception:
                    continue
                if not (math.isfinite(x) and math.isfinite(y)):
                    continue
                valid_list.append({'x': round(x, 6), 'y': round(y, 6)})

            if valid_list:
                cleaned[str(idx)] = valid_list

        return cleaned

    @staticmethod
    def _has_non_default_path_settings(settings: Dict[str, Any]) -> bool:
        loop_type = str(settings.get('loop_type', 'none') or 'none')
        curved_segments = settings.get('curved_segments', [])
        curved_controls = settings.get('curved_segment_controls', {})
        return loop_type != 'none' or bool(curved_segments) or bool(curved_controls)

    def _normalize_path_settings(
        self,
        settings_raw: Any,
        *,
        point_count: int,
        existing_settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        existing = existing_settings if isinstance(existing_settings, dict) else {}
        incoming = settings_raw if isinstance(settings_raw, dict) else {}

        loop_type_raw = incoming.get('loop_type', existing.get('loop_type', 'none'))
        loop_mode_raw = incoming.get('loop_mode', existing.get('loop_mode', False))
        loop_type = self._normalize_loop_type(loop_type_raw, loop_mode=loop_mode_raw)

        curved_raw = incoming.get('curved_segments', incoming.get('curve_segments'))
        if curved_raw is None:
            curved_raw = existing.get('curved_segments', [])
        curved_segments = self._sanitize_curved_segments(
            curved_raw,
            point_count=point_count,
            closed_loop=(loop_type == 'closed'),
        )
        curved_segment_set = set(curved_segments)

        controls_raw = incoming.get('curved_segment_controls', incoming.get('curve_controls'))
        if controls_raw is None:
            controls_raw = existing.get('curved_segment_controls', existing.get('curve_controls'))
        curved_segment_controls = self._sanitize_curved_segment_controls(
            controls_raw,
            point_count=point_count,
            closed_loop=(loop_type == 'closed'),
            allowed_segments=curved_segment_set,
        )

        return {
            'loop_type': loop_type,
            'loop_mode': bool(loop_type != 'none'),
            'curved_segments': curved_segments,
            'curved_segment_controls': curved_segment_controls,
            'has_curves': bool(curved_segments),
        }

    @staticmethod
    def _next_loop_target(current_idx: int, total: int, loop_type: str, direction: int) -> Tuple[int, int]:
        if total <= 1:
            return current_idx, 1

        if loop_type == 'closed':
            return (current_idx + 1) % total, 1

        # Ping-pong: ...A->B->C->B->A->B...
        d = 1 if int(direction) >= 0 else -1
        if current_idx <= 0:
            d = 1
        elif current_idx >= total - 1:
            d = -1
        return current_idx + d, d

    def _start_next_loop_segment(self, *, restart: bool, retry_same_target: bool = False):
        """Send the FULL remaining path slice to Nav2 so RPP sees all upcoming turns.

        Instead of one 2-point segment at a time, we send every waypoint from
        the current position all the way to the turnaround point in a single
        nav_msgs/Path goal.  RPP then plans smooth velocities through every
        intermediate turn without restarting.  When it reaches the end the
        callback advances the index to the endpoint and we dispatch the return
        slice.

        Ping-pong: forward = send points[current..last], backward = send
                   points[current..0] (reversed).
        Closed:    send points[current..last] + points[0..current-1] (full loop).
        """
        if (not self._loop_enabled) or self._loop_stop_requested:
            return self._ok(False, "Loop stop requested")

        points = list(self._loop_path_points)
        total = len(points)
        if total < 2:
            return self._ok(False, "Loop requires at least 2 waypoints")

        if getattr(self, "_follow_goal_inflight", False):
            return self._ok(False, "Follow goal already in-flight")

        current_idx = max(0, min(int(getattr(self, "_loop_current_index", 0)), total - 1))
        self._loop_current_index = current_idx
        direction = int(getattr(self, "_loop_direction", 1))

        if retry_same_target:
            target_idx = int(getattr(self, "_loop_target_index", current_idx))
            if target_idx < 0 or target_idx >= total or target_idx == current_idx:
                target_idx = (total - 1) if direction >= 0 else 0
            direction = 1 if target_idx >= current_idx else -1
        else:
            if self._loop_type == 'closed':
                # Full loop: advance to the point just before wrapping back to start
                target_idx = (current_idx - 1 + total) % total
                direction = 1
                self._loop_direction = 1
            else:
                # Ping-pong: full sweep to the far end in current direction
                if direction >= 0:
                    if current_idx >= total - 1:
                        direction = -1   # already at end, reverse
                    target_idx = 0 if direction < 0 else (total - 1)
                    self._loop_direction = -1 if target_idx == total - 1 else 1
                else:
                    if current_idx <= 0:
                        direction = 1    # already at start, forward
                    target_idx = (total - 1) if direction > 0 else 0
                    self._loop_direction = 1 if target_idx == 0 else -1

        self._loop_target_index = target_idx

        # Build the full waypoint slice from current_idx to target_idx
        if self._loop_type == 'closed':
            if current_idx <= target_idx:
                indices = list(range(current_idx, target_idx + 1))
            else:
                indices = list(range(current_idx, total)) + list(range(0, target_idx + 1))
        else:
            if target_idx >= current_idx:
                indices = list(range(current_idx, target_idx + 1))
            else:
                indices = list(range(current_idx, target_idx - 1, -1))

        segment_points = [dict(points[i]) for i in indices]

        loop_label = "Closed" if self._loop_type == "closed" else "Ping-pong"
        state_message = (
            f"{loop_label} {current_idx + 1}\u2192{target_idx + 1} "
            f"({len(segment_points)} wpts{' retry' if retry_same_target else ''})"
        )

        start_result = self._start_follow_path_goal(
            segment_points,
            loop_type=self._loop_type,
            restart=bool(restart),
            display_total=self._loop_display_waypoint_count if self._loop_display_waypoint_count > 0 else total,
            state_current_index=current_idx,
            state_message=state_message,
        )

        if not start_result.get("ok"):
            self._follow_goal_inflight = False
            return start_result

        self.path_state["active"] = True
        self.path_state["loop_mode"] = True
        self.path_state["loop_type"] = self._loop_type
        self.path_state["current_index"] = int(current_idx)
        self.path_state["total_waypoints"] = int(
            self._loop_display_waypoint_count if self._loop_display_waypoint_count > 0 else total
        )
        self.path_state["message"] = state_message

        return start_result


    def _load_path_metadata(self) -> Dict[str, Dict[str, Any]]:
        metadata: Dict[str, Dict[str, Any]] = {}
        path = str(getattr(self, 'path_metadata_file', '') or '').strip()
        if not path or not os.path.exists(path):
            return metadata

        try:
            with open(path, 'r', encoding='utf-8') as f:
                raw = yaml.safe_load(f) or {}
        except Exception as exc:
            self.get_logger().warn(f'Failed to load path metadata file "{path}": {exc}')
            return metadata

        if not isinstance(raw, dict):
            return metadata

        for name, payload in raw.items():
            path_name = str(name or '').strip()
            if not path_name:
                continue

            points_raw = payload
            settings_raw: Any = {}
            if isinstance(payload, dict):
                points_raw = payload.get('points', [])
                settings_raw = payload.get('settings', {})

            parsed_points = self._parse_path_points(points_raw)
            settings = self._normalize_path_settings(
                settings_raw,
                point_count=len(parsed_points),
            )
            if not parsed_points and not self._has_non_default_path_settings(settings):
                continue
            metadata[path_name] = {
                'points': parsed_points,
                'settings': settings,
            }
        return metadata

    def _save_path_metadata(self):
        path = str(getattr(self, 'path_metadata_file', '') or '').strip()
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(self.path_metadata, f, sort_keys=False)
        except Exception as exc:
            self.get_logger().warn(f'Failed to save path metadata file "{path}": {exc}')

    def _set_path_metadata(self, name: str, points: List[Dict[str, float]], settings: Optional[Dict[str, Any]] = None):
        path_name = str(name or '').strip()
        if not path_name:
            return

        cleaned: List[Dict[str, float]] = []
        has_zone_reference = False
        has_shelf_check = False
        for point in points:
            try:
                x = float(point.get('x'))
                y = float(point.get('y'))
            except Exception:
                continue
            if not (math.isfinite(x) and math.isfinite(y)):
                continue
            entry: Dict[str, float] = {'x': round(x, 6), 'y': round(y, 6)}

            zone_name = str(
                point.get('zone_name', point.get('zoneName', ''))
            ).strip()
            if zone_name:
                entry['zone_name'] = zone_name
                has_zone_reference = True
            if self._as_bool(point.get('shelf_check', point.get('shelfCheck', False))):
                entry['shelf_check'] = True
                has_shelf_check = True
            cleaned.append(entry)

        with self._path_metadata_lock:
            existing_entry = self.path_metadata.get(path_name, {})
            existing_settings = existing_entry.get('settings', {}) if isinstance(existing_entry, dict) else {}
            normalized_settings = self._normalize_path_settings(
                settings,
                point_count=len(cleaned),
                existing_settings=existing_settings,
            )
            keep_points_metadata = has_zone_reference or has_shelf_check
            keep_metadata = keep_points_metadata or self._has_non_default_path_settings(normalized_settings)
            if keep_metadata:
                self.path_metadata[path_name] = {
                    'points': cleaned if keep_points_metadata else [],
                    'settings': normalized_settings,
                }
            else:
                self.path_metadata.pop(path_name, None)
            self._save_path_metadata()

    def _remove_path_metadata(self, name: str):
        path_name = str(name or '').strip()
        if not path_name:
            return
        with self._path_metadata_lock:
            self.path_metadata.pop(path_name, None)
            self._save_path_metadata()

    def _apply_path_metadata(self, name: str, points_xy: List[Dict[str, float]]) -> List[Dict[str, float]]:
        path_name = str(name or '').strip()
        if not path_name:
            return points_xy

        with self._path_metadata_lock:
            metadata_entry = self.path_metadata.get(path_name, {})
            metadata_points = list(metadata_entry.get('points', [])) if isinstance(metadata_entry, dict) else []

        if not metadata_points:
            return points_xy

        enriched: List[Dict[str, float]] = []
        for idx, point in enumerate(points_xy):
            entry = {'x': float(point.get('x', 0.0)), 'y': float(point.get('y', 0.0))}
            if idx < len(metadata_points):
                md = metadata_points[idx]

                try:
                    mx = float(md.get('x', entry['x']))
                    my = float(md.get('y', entry['y']))
                    is_near = math.hypot(mx - entry['x'], my - entry['y']) <= 0.75
                except Exception:
                    is_near = False

                if is_near:
                    zone_name = str(md.get('zone_name', '') or '').strip()
                    if zone_name:
                        entry['zoneName'] = zone_name
                    if self._as_bool(md.get('shelf_check', False)):
                        entry['shelfCheck'] = True
            enriched.append(entry)
        return enriched

    @staticmethod
    def _bezier_point_xy(control_nodes: List[Dict[str, float]], t: float) -> Optional[Dict[str, float]]:
        if not isinstance(control_nodes, list) or len(control_nodes) < 2:
            return None

        clamped_t = max(0.0, min(1.0, float(t)))
        work: List[Dict[str, float]] = []
        for node in control_nodes:
            try:
                work.append({
                    'x': float(node.get('x', 0.0)),
                    'y': float(node.get('y', 0.0)),
                })
            except Exception:
                return None

        for k in range(len(work) - 1, 0, -1):
            for i in range(k):
                work[i] = {
                    'x': ((1.0 - clamped_t) * work[i]['x']) + (clamped_t * work[i + 1]['x']),
                    'y': ((1.0 - clamped_t) * work[i]['y']) + (clamped_t * work[i + 1]['y']),
                }
        return work[0]

    def _build_path_metric_points(
        self,
        points: Any,
        *,
        settings: Optional[Dict[str, Any]] = None,
        curve_sample_count: int = 24,
    ) -> List[Dict[str, float]]:
        anchors = self._parse_path_points(points)
        if len(anchors) < 2:
            return anchors

        normalized_settings = self._normalize_path_settings(
            settings or {},
            point_count=len(anchors),
        )
        loop_type = str(normalized_settings.get('loop_type', 'none') or 'none')
        closed_loop = loop_type == 'closed'
        curved_segments = set(normalized_settings.get('curved_segments', []))
        curved_controls = normalized_settings.get('curved_segment_controls', {})
        segment_count = len(anchors) if closed_loop else (len(anchors) - 1)

        sampled: List[Dict[str, float]] = [{
            'x': float(anchors[0]['x']),
            'y': float(anchors[0]['y']),
        }]
        sample_count = max(2, int(curve_sample_count or 24))

        for segment_idx in range(segment_count):
            start = anchors[segment_idx]
            end = anchors[0] if (closed_loop and segment_idx == len(anchors) - 1) else anchors[segment_idx + 1]

            if segment_idx not in curved_segments:
                sampled.append({
                    'x': float(end['x']),
                    'y': float(end['y']),
                })
                continue

            controls_raw = curved_controls.get(str(segment_idx), [])
            controls = []
            if isinstance(controls_raw, list):
                for entry in controls_raw:
                    try:
                        controls.append({
                            'x': float(entry.get('x', 0.0)),
                            'y': float(entry.get('y', 0.0)),
                        })
                    except Exception:
                        continue

            if not controls:
                controls = [{
                    'x': (float(start['x']) + float(end['x'])) / 2.0,
                    'y': (float(start['y']) + float(end['y'])) / 2.0,
                }]

            nodes = [start, *controls, end]
            for sample_idx in range(1, sample_count + 1):
                point = self._bezier_point_xy(nodes, sample_idx / sample_count)
                if point is None:
                    continue
                sampled.append(point)

        return sampled

    def _compute_path_length_m(self, points: Any, *, settings: Optional[Dict[str, Any]] = None) -> float:
        sampled_points = self._build_path_metric_points(points, settings=settings)
        if len(sampled_points) < 2:
            return 0.0

        total = 0.0
        for idx in range(1, len(sampled_points)):
            prev = sampled_points[idx - 1]
            cur = sampled_points[idx]
            total += math.hypot(
                float(cur.get('x', 0.0)) - float(prev.get('x', 0.0)),
                float(cur.get('y', 0.0)) - float(prev.get('y', 0.0)),
            )
        return float(total)

    def _get_path_settings(self, name: str, point_count: int) -> Dict[str, Any]:
        path_name = str(name or '').strip()
        if not path_name:
            return self._normalize_path_settings({}, point_count=point_count)

        with self._path_metadata_lock:
            metadata_entry = self.path_metadata.get(path_name, {})
            settings_raw = metadata_entry.get('settings', {}) if isinstance(metadata_entry, dict) else {}

        return self._normalize_path_settings(settings_raw, point_count=point_count)




    def _follow_path_feedback(self, feedback_msg):
        feedback = feedback_msg.feedback
        if bool(self._loop_enabled) and len(self._loop_path_points) >= 2:
            local_index = int(feedback.current_index)
            if local_index <= 0:
                mapped_index = int(self._loop_current_index)
            else:
                mapped_index = int(self._loop_target_index)
            total = int(self.path_state.get('total_waypoints', 0) or 0)
            if total <= 0:
                total = int(self._loop_display_waypoint_count or len(self._loop_path_points))
            self.path_state['current_index'] = max(0, mapped_index)
            self.path_state['total_waypoints'] = max(0, total)
            self.path_state['message'] = str(feedback.status or f'Loop segment {self._loop_current_index + 1}->{self._loop_target_index + 1}')
            return

        self.path_state['current_index'] = int(feedback.current_index)
        self.path_state['total_waypoints'] = int(feedback.total)
        self.path_state['message'] = str(feedback.status or '')
    
    def _build_follow_path_goal(self, points: Any):
        goal = FollowPathAction.Goal()
        for point in points:
            x = float(point.get('x', 0.0))
            y = float(point.get('y', 0.0))
            pose = PoseStamped()
            pose.header.frame_id = self._map_frame()
            pose.header.stamp = self.get_clock().now().to_msg()
            pose.pose.position.x = x
            pose.pose.position.y = y
            pose.pose.position.z = 0.0
            # Path waypoints are geometry only; POIs own heading semantics.
            pose.pose.orientation.w = 1.0
            goal.waypoints.append(pose)
        goal.zone_names = [
            str(point.get('zone_name', point.get('zoneName', '')) or '')
            for point in points
        ]
        goal.shelf_checks = [
            bool(self._as_bool(point.get('shelf_check', point.get('shelfCheck', False))))
            for point in points
        ]
        return goal
    
    def _start_follow_path_goal(
        self,
        points: Any,
        *,
        loop_type: str = 'none',
        restart: bool = False,
        display_total: Optional[int] = None,
        state_current_index: int = 0,
        state_message: Optional[str] = None,
    ):
        if restart and self._loop_stop_requested:
            return self._ok(False, 'Loop stop requested')

        wait_timeout = 5.0 if restart else 2.0
        if not self.follow_path_action_client.wait_for_server(timeout_sec=wait_timeout):
            return self._ok(False, 'FollowPath action server not available')
    
        goal = self._build_follow_path_goal(points)
        ok = self._send_followpath_goal(goal)
        if not ok:
            return self._ok(False, 'FollowPath goal already in-flight')
        total_waypoints = int(display_total) if display_total is not None else len(points)
        if total_waypoints < 0:
            total_waypoints = 0
        default_message = 'Loop restart: navigating to waypoint 1' if restart else 'Path following started'
        final_message = str(state_message) if state_message is not None else default_message
        normalized_loop_type = self._normalize_loop_type(loop_type, loop_mode=False)
        self.path_state = {
            'active': True,
            'accepted': True,
            'loop_mode': bool(normalized_loop_type != 'none'),
            'loop_type': normalized_loop_type,
            'current_index': max(0, int(state_current_index)),
            'total_waypoints': total_waypoints,
            'message': final_message,
            'success': False,
        }
        return self._ok(True, final_message)

    # _follow_path_result_done and _restart_follow_path_loop have been removed.
    # All loop progression is now driven by _on_follow_result (single callback chain).

    
    def follow_path(self, path_points: Any, loop_type: Any = 'none', loop_mode: Optional[bool] = None):
        if self.estop_active:
            return self._ok(False, 'E-STOP active. Reset before path following.')

        # Path tracking requires NAV stack running (controller/planner active).
        stack = self.get_stack_status()
        if not bool(stack.get('ok')):
            return self._ok(False, f"Cannot start path: {stack.get('message', 'stack status unavailable')}")

        nav_running = bool(stack.get('nav_running'))
        stack_mode = str(stack.get('mode', 'unknown'))
        if not nav_running:
            return self._ok(False, f'NAV stack not running (mode={stack_mode}, nav_running={nav_running})')

        points = self._parse_path_points(path_points)
        if len(points) < 2:
            return self._ok(False, 'Path must have at least 2 points')

        requested_loop_type = self._normalize_loop_type(loop_type, loop_mode=loop_mode)

        # Preempt any active goal using the unified goal handle.
        with self._nav_goal_lock:
            active_goal = getattr(self, '_active_goal_handle', None)
            goal_seq = int(getattr(self, '_path_goal_seq', 0))
            follow_inflight = bool(getattr(self, '_follow_goal_inflight', False))
        if active_goal is not None or follow_inflight:
            self.get_logger().warn('Preempting active FollowPath goal for new path request')
            if active_goal is None:
                with self._nav_goal_lock:
                    self._follow_cancel_requested = True
                return self._ok(False, 'FollowPath dispatch is still in flight; cancel queued, retry shortly')
            cancel_ok, cancel_msg = self._cancel_client_goal(
                active_goal,
                timeout_sec=1.5,
                label='FollowPath goal',
            )
            if not cancel_ok:
                self.get_logger().warn(cancel_msg)
                return self._ok(False, cancel_msg)
            with self._nav_goal_lock:
                if goal_seq == int(getattr(self, '_path_goal_seq', 0)):
                    self._follow_goal_inflight = False
                    self._active_goal_handle = None
                    self._path_goal_handle = None
                    self._follow_cancel_requested = False

        self._loop_enabled = requested_loop_type != 'none'
        self._loop_type = requested_loop_type if self._loop_enabled else 'none'
        self._loop_stop_requested = False
        self._loop_restart_inflight = False
        self._loop_current_index = 0
        self._loop_target_index = 1
        self._loop_direction = 1
        self._loop_retry_fail_streak = 0
        self._loop_display_waypoint_count = len(points) if self._loop_enabled else 0

        execution_points = list(points)
        if self._loop_enabled:
            execution_points = self._prepare_loop_execution_points(points)
            if len(execution_points) < 2:
                self._loop_enabled = False
                self._loop_type = 'none'
                self._loop_path_points = []
                self._loop_display_waypoint_count = 0
                return self._ok(False, 'Loop path must have at least 2 distinct waypoints')
            self._loop_path_points = list(execution_points)
            self._loop_current_index = 0
            self._loop_target_index = 1
            self._loop_direction = 1
        else:
            self._loop_path_points = []

        if self._loop_enabled:
            start_result = self._start_next_loop_segment(restart=False, retry_same_target=False)
        else:
            start_result = self._start_follow_path_goal(
                execution_points,
                loop_type='none',
                restart=False,
                display_total=len(execution_points),
            )
        if not start_result.get('ok'):
            self._loop_enabled = False
            self._loop_type = 'none'
            self._loop_path_points = []
            self._loop_display_waypoint_count = 0
            self._loop_current_index = 0
            self._loop_target_index = 1
            self._loop_direction = 1
            return start_result

        if self._loop_enabled:
            if self._loop_type == 'closed':
                self.path_state['message'] = 'Path loop active (closed): last point reconnects to first'
            else:
                self.path_state['message'] = 'Path loop active (ping-pong): runs until Stop Path'
            return self._ok(True, self.path_state['message'])

        return start_result
    
    def stop_path(self):
        self._loop_enabled = False
        self._loop_type = 'none'
        self._loop_stop_requested = True
        self._loop_restart_inflight = False
        self._loop_path_points = []
        self._loop_display_waypoint_count = 0
        self._loop_current_index = 0
        self._loop_target_index = 1
        self._loop_direction = 1
        self._loop_retry_fail_streak = 0
        self._pending_loop_restart = False

        # Use unified goal handle from the new state machine.
        with self._nav_goal_lock:
            goal_handle = getattr(self, '_active_goal_handle', None)
            goal_seq = int(getattr(self, '_path_goal_seq', 0))
            follow_inflight = bool(getattr(self, '_follow_goal_inflight', False))
            if goal_handle is None and follow_inflight:
                self._follow_cancel_requested = True

        self.path_state['active'] = False
        self.path_state['loop_mode'] = False
        self.path_state['loop_type'] = 'none'
        self.path_state['success'] = False
    
        if goal_handle is None and follow_inflight:
            self.path_state['message'] = 'Path stop queued while dispatch is in flight'
            return self._ok(True, 'Path stop queued while dispatch is in flight')

        if goal_handle is None:
            self.path_state['message'] = 'Path following stopped'
            return self._ok(True, 'Path following stopped')

        ok, message = self._cancel_client_goal(
            goal_handle,
            timeout_sec=2.0,
            label='FollowPath goal',
        )
        if not ok:
            return self._ok(False, message)

        with self._nav_goal_lock:
            if goal_seq == int(getattr(self, '_path_goal_seq', 0)) and goal_handle is self._active_goal_handle:
                self._active_goal_handle = None
                self._path_goal_handle = None
                self._follow_goal_inflight = False
                self._follow_cancel_requested = False
    
        self.path_state['message'] = 'Path following stopped'
        self.path_state['success'] = False
        return self._ok(True, 'Path following stopped')

    
    def get_path_status(self):
        active = bool(self.path_state.get('active'))
        reported_loop_type = self._normalize_loop_type(
            self.path_state.get('loop_type', 'none'),
            loop_mode=self.path_state.get('loop_mode', False),
        )
        runtime_loop_enabled = (
            bool(self._loop_enabled)
            and (not self._loop_stop_requested)
            and len(self._loop_path_points) >= 2
        )
        runtime_loop_type = self._loop_type if runtime_loop_enabled else 'none'
        loop_type = runtime_loop_type if active and runtime_loop_type != 'none' else reported_loop_type

        return {
            'active': active,
            'loop_mode': bool(loop_type != 'none'),
            'loop_type': loop_type,
            'current_index': int(self.path_state.get('current_index', 0)),
            'total_waypoints': int(self.path_state.get('total_waypoints', 0)),
            'message': str(self.path_state.get('message', '')),
            'success': bool(self.path_state.get('success', False)),
        }
    
    def save_path(self, name: str, path_points: Any, settings: Optional[Dict[str, Any]] = None):
        path_name = str(name or '').strip()
        if not path_name:
            return self._ok(False, 'Path name is required')
    
        points = self._parse_path_points(path_points)
        if len(points) < 2:
            return self._ok(False, 'Path must have at least 2 points')
    
        req = SavePath.Request()
        req.name = path_name
        req.x_coords = [float(point.get('x', 0.0)) for point in points]
        req.y_coords = [float(point.get('y', 0.0)) for point in points]
    
        response, err = self._call_service(self.save_path_client, req, wait_timeout=1.0, response_timeout=6.0)
        if err == 'service_not_available':
            return self._ok(False, 'SavePath service not available')
        if err == 'timeout':
            return self._ok(False, 'SavePath timed out')
        if response is None:
            return self._ok(False, 'SavePath failed')
        ok = bool(response.ok)
        if ok:
            self._set_path_metadata(path_name, points, settings=settings)
        return self._ok(ok, str(response.message))
    
    def delete_path(self, name: str):
        req = DeletePath.Request()
        req.name = str(name or '')
    
        response, err = self._call_service(self.delete_path_client, req, wait_timeout=1.0, response_timeout=5.0)
        if err == 'service_not_available':
            return self._ok(False, 'DeletePath service not available')
        if err == 'timeout':
            return self._ok(False, 'DeletePath timed out')
        if response is None:
            return self._ok(False, 'DeletePath failed')
        ok = bool(response.ok)
        if ok:
            self._remove_path_metadata(req.name)
        return self._ok(ok, str(response.message))
    
    def get_paths(self):
        req = GetPaths.Request()
        response, err = self._call_service(self.get_paths_client, req, wait_timeout=1.0, response_timeout=6.0)
        if err == 'service_not_available':
            return self._ok(False, 'GetPaths service not available', paths={}, path_settings={}, path_metrics={})
        if err == 'timeout':
            return self._ok(False, 'GetPaths timed out', paths={}, path_settings={}, path_metrics={})
        if response is None or not bool(response.success):
            msg = response.message if response is not None else 'GetPaths failed'
            return self._ok(False, str(msg), paths={}, path_settings={}, path_metrics={})
    
        paths = {}
        path_settings: Dict[str, Dict[str, Any]] = {}
        path_metrics: Dict[str, Dict[str, Any]] = {}
        for path_msg in response.paths:
            name = str(path_msg.name).strip()
            if not name:
                continue
            points_xy = self._path_msg_to_dict(path_msg)
            enriched_points = self._apply_path_metadata(name, points_xy)
            paths[name] = enriched_points
            settings = self._get_path_settings(name, point_count=len(enriched_points))
            path_settings[name] = settings
            path_metrics[name] = {
                'length_m': round(self._compute_path_length_m(enriched_points, settings=settings), 3),
            }
        return self._ok(
            True,
            f'Retrieved {len(paths)} paths',
            paths=paths,
            path_settings=path_settings,
            path_metrics=path_metrics,
        )
    
    def load_path(self, name: str):
        paths_result = self.get_paths()
        if not paths_result.get('ok'):
            return self._ok(False, paths_result.get('message', 'Failed to load paths'))
    
        name = str(name or '')
        all_paths = paths_result.get('paths', {})
        if name not in all_paths:
            return self._ok(False, f'Path "{name}" not found')
        all_settings = paths_result.get('path_settings', {})
        return self._ok(
            True,
            f'Path "{name}" loaded',
            path=all_paths[name],
            settings=all_settings.get(name, self._normalize_path_settings({}, point_count=len(all_paths[name]))),
        )
    
    # ---------- layouts ----------


    def get_layouts(self):
        req = GetLayouts.Request()
        response, err = self._call_service(self.get_layouts_client, req, wait_timeout=1.0, response_timeout=6.0)
        if err == 'service_not_available':
            return self._ok(False, 'GetLayouts service not available', current=None, layouts={})
        if err == 'timeout':
            return self._ok(False, 'GetLayouts timed out', current=None, layouts={})
        if response is None or not bool(response.success):
            msg = response.message if response is not None else 'GetLayouts failed'
            return self._ok(False, str(msg), current=None, layouts={})

        layouts = {}
        for layout_msg in response.layouts:
            name = str(layout_msg.name).strip()
            if not name:
                continue
            data = self._layout_msg_to_dict(layout_msg)
            layouts[name] = {
                'description': data.get('description', ''),
                'zones_count': len(data.get('zones', {})),
                'paths_count': len(data.get('paths', {})),
                'map_path': data.get('map_path', ''),
            }

        return self._ok(True, f'Retrieved {len(layouts)} layouts', current=None, layouts=layouts)

    def save_layout(self, name: str, description: str = ''):
        req = SaveLayout.Request()
        req.name = str(name or '')
        req.description = str(description or '')

        response, err = self._call_service(self.save_layout_client, req, wait_timeout=1.0, response_timeout=6.0)
        if err == 'service_not_available':
            return self._ok(False, 'SaveLayout service not available')
        if err == 'timeout':
            return self._ok(False, 'SaveLayout timed out')
        if response is None:
            return self._ok(False, 'SaveLayout failed')
        return self._ok(bool(response.ok), str(response.message))

    def load_layout(self, name: str):
        req = LoadLayout.Request()
        req.name = str(name or '')

        response, err = self._call_service(self.load_layout_client, req, wait_timeout=1.0, response_timeout=6.0)
        if err == 'service_not_available':
            return self._ok(False, 'LoadLayout service not available')
        if err == 'timeout':
            return self._ok(False, 'LoadLayout timed out')
        if response is None:
            return self._ok(False, 'LoadLayout failed')
        return self._ok(bool(response.ok), str(response.message))

    def delete_layout(self, name: str):
        req = DeleteLayout.Request()
        req.name = str(name or '')

        response, err = self._call_service(self.delete_layout_client, req, wait_timeout=1.0, response_timeout=6.0)
        if err == 'service_not_available':
            return self._ok(False, 'DeleteLayout service not available')
        if err == 'timeout':
            return self._ok(False, 'DeleteLayout timed out')
        if response is None:
            return self._ok(False, 'DeleteLayout failed')
        return self._ok(bool(response.ok), str(response.message))


    # ---------- mission / sequence ----------

    def start_sequence(self, zones: Any):
        clean_zones = []
        if isinstance(zones, list):
            prev_name = ''
            for item in zones:
                name = str(item or '').strip()
                if not name:
                    continue
                # Preserve operator-defined order and allow revisits (A->B->A).
                # Only collapse immediate duplicate clicks (A->A).
                if name == prev_name:
                    continue
                clean_zones.append(name)
                prev_name = name

        if not clean_zones:
            return self._ok(
                False,
                'No zones provided for sequence',
                active=False,
                current_index=0,
                total_zones=0,
                current_zone='',
            )

        req = StartSequence.Request()
        req.zones = list(clean_zones)
        response, err = self._call_service(self.start_sequence_client, req, wait_timeout=1.0, response_timeout=5.0)
        if err == 'service_not_available':
            return self._ok(
                False,
                'MissionManager start_sequence service not available',
                active=False,
                current_index=0,
                total_zones=len(clean_zones),
                current_zone='',
            )
        if err == 'timeout':
            return self._ok(
                False,
                'MissionManager start_sequence timed out',
                active=False,
                current_index=0,
                total_zones=len(clean_zones),
                current_zone='',
            )
        if response is None:
            return self._ok(
                False,
                'Start sequence failed',
                active=False,
                current_index=0,
                total_zones=len(clean_zones),
                current_zone='',
            )

        return self._ok(
            bool(response.ok),
            str(response.message),
            active=bool(response.ok),
            current_index=0,
            total_zones=len(clean_zones),
            current_zone=(clean_zones[0] if clean_zones else ''),
        )

    def stop_sequence(self):
        req = StopSequence.Request()
        response, err = self._call_service(self.stop_sequence_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(
                False,
                'MissionManager stop_sequence service not available',
                active=False,
                current_index=0,
                total_zones=0,
                current_zone='',
            )
        if err == 'timeout':
            return self._ok(
                False,
                'MissionManager stop_sequence timed out',
                active=False,
                current_index=0,
                total_zones=0,
                current_zone='',
            )
        if response is None:
            return self._ok(
                False,
                'Stop sequence failed',
                active=False,
                current_index=0,
                total_zones=0,
                current_zone='',
            )

        return self._ok(
            bool(response.ok),
            str(response.message),
            active=False,
            current_index=0,
            total_zones=0,
            current_zone='',
        )

    def get_sequence_status(self):
        req = GetSequenceStatus.Request()
        response, err = self._call_service(self.get_sequence_status_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(
                False,
                'MissionManager sequence_status service not available',
                active=False,
                current_index=0,
                total_zones=0,
                current_zone='',
            )
        if err == 'timeout':
            return self._ok(
                False,
                'MissionManager sequence_status timed out',
                active=False,
                current_index=0,
                total_zones=0,
                current_zone='',
            )
        if response is None:
            return self._ok(
                False,
                'Sequence status failed',
                active=False,
                current_index=0,
                total_zones=0,
                current_zone='',
            )

        return {
            'ok': bool(response.ok),
            'active': bool(response.active),
            'current_index': int(response.current_index),
            'total_zones': int(response.total_zones),
            'current_zone': str(response.current_zone),
            'message': str(response.message),
        }

    def get_mission_status(self):
        req = GetMissionStatus.Request()
        response, err = self._call_service(self.get_mission_status_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(
                False,
                'MissionManager status service not available',
                active=False,
                paused=False,
                state='IDLE',
                type='',
                data=[],
                progress=0,
                interrupted_by='',
            )
        if err == 'timeout':
            return self._ok(
                False,
                'MissionManager status timed out',
                active=False,
                paused=False,
                state='IDLE',
                type='',
                data=[],
                progress=0,
                interrupted_by='',
            )
        if response is None:
            return self._ok(
                False,
                'Mission status failed',
                active=False,
                paused=False,
                state='IDLE',
                type='',
                data=[],
                progress=0,
                interrupted_by='',
            )

        return {
            'ok': bool(response.ok),
            'active': bool(response.active),
            'paused': bool(getattr(response, 'paused', False)),
            'state': str(getattr(response, 'state', 'IDLE')),
            'type': str(response.type),
            'data': [str(item) for item in response.data],
            'progress': int(response.progress),
            'interrupted_by': str(response.interrupted_by),
            'message': str(response.message),
        }

    def resume_mission(self):
        req = ResumeMission.Request()
        response, err = self._call_service(self.resume_mission_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'MissionManager resume service not available', type='')
        if err == 'timeout':
            return self._ok(False, 'MissionManager resume timed out', type='')
        if response is None:
            return self._ok(False, 'Mission resume failed', type='')
        return self._ok(bool(response.ok), str(response.message), type=str(response.type))

    def clear_mission_state(self):
        req = ClearMissionState.Request()
        response, err = self._call_service(self.clear_mission_state_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'MissionManager clear service not available')
        if err == 'timeout':
            return self._ok(False, 'MissionManager clear timed out')
        if response is None:
            return self._ok(False, 'Mission clear failed')
        return self._ok(bool(response.ok), str(response.message))

    # ---------- editor map ----------

    def get_editor_map_preview(self):
        req = GetEditorMapPreview.Request()
        response, err = self._call_service(self.editor_preview_client, req, wait_timeout=1.0, response_timeout=5.0)
        if err == 'service_not_available':
            return self._ok(False, 'MapEditorManager preview service not available')
        if err == 'timeout':
            return self._ok(False, 'MapEditorManager preview timed out')
        if response is None:
            return self._ok(False, 'Map editor preview failed')

        map_yaml_path = _canonicalize_map_yaml_path(str(response.map_yaml_path), self.ui_root)
        return self._ok(
            bool(response.ok),
            str(response.message),
            image=str(response.image),
            width=int(response.width),
            height=int(response.height),
            resolution=float(response.resolution),
            origin=[float(v) for v in response.origin],
            map_yaml_path=map_yaml_path,
        )

    def overwrite_editor_map(self, image_data: str, origin: Any):
        req = OverwriteEditorMap.Request()
        req.image = str(image_data or '')
        req.origin = []
        if isinstance(origin, list):
            for value in origin[:3]:
                try:
                    req.origin.append(float(value))
                except (TypeError, ValueError):
                    continue

        response, err = self._call_service(self.editor_overwrite_client, req, wait_timeout=1.0, response_timeout=5.0)
        if err == 'service_not_available':
            return self._ok(False, 'MapEditorManager overwrite service not available')
        if err == 'timeout':
            return self._ok(False, 'MapEditorManager overwrite timed out')
        if response is None:
            return self._ok(False, 'Map editor overwrite failed')
        return self._ok(bool(response.ok), str(response.message))

    def save_current_editor_map(self):
        req = SaveCurrentMap.Request()
        response, err = self._call_service(self.editor_save_current_client, req, wait_timeout=1.0, response_timeout=8.0)
        if err == 'service_not_available':
            return self._ok(False, 'MapEditorManager save_current service not available')
        if err == 'timeout':
            return self._ok(False, 'MapEditorManager save_current timed out')
        if response is None:
            return self._ok(False, 'Save current map failed')

        map_yaml_path = _canonicalize_map_yaml_path(str(response.map_yaml_path), self.ui_root)
        return self._ok(
            bool(response.ok),
            str(response.message),
            map_yaml_path=map_yaml_path,
            map_image_path=str(response.map_image_path),
        )

    def reload_editor_map(self):
        req = ReloadEditorMap.Request()
        response, err = self._call_service(self.editor_reload_client, req, wait_timeout=1.0, response_timeout=6.0)
        if err == 'service_not_available':
            return self._ok(False, 'MapEditorManager reload service not available')
        if err == 'timeout':
            return self._ok(False, 'MapEditorManager reload timed out')
        if response is None:
            return self._ok(False, 'Reload editor map failed')

        map_yaml_path = _canonicalize_map_yaml_path(str(response.map_yaml_path), self.ui_root)
        return self._ok(
            bool(response.ok),
            str(response.message),
            image=str(response.image),
            width=int(response.width),
            height=int(response.height),
            resolution=float(response.resolution),
            origin=[float(v) for v in response.origin],
            map_yaml_path=map_yaml_path,
        )

    def export_editor_map(self, map_name: str):
        req = ExportEditorMap.Request()
        req.map_name = str(map_name or '').strip()
        response, err = self._call_service(self.editor_export_client, req, wait_timeout=1.0, response_timeout=8.0)
        if err == 'service_not_available':
            return self._ok(False, 'MapEditorManager export service not available')
        if err == 'timeout':
            return self._ok(False, 'MapEditorManager export timed out')
        if response is None:
            return self._ok(False, 'Export edited map failed')

        map_yaml_path = _canonicalize_map_yaml_path(str(response.map_yaml_path), self.ui_root)
        return self._ok(
            bool(response.ok),
            str(response.message),
            map_yaml_path=map_yaml_path,
            map_image_path=str(response.map_image_path),
        )

    # ---------- footprint / nav2 config ----------

    @staticmethod
    def _normalize_namespace(namespace: Any) -> str:
        ns = str(namespace or '/').strip() or '/'
        if not ns.startswith('/'):
            ns = f'/{ns}'
        if ns != '/':
            ns = ns.rstrip('/')
        return ns or '/'

    @staticmethod
    def _join_namespace(namespace: str, suffix: str) -> str:
        base = RosBridge._normalize_namespace(namespace)
        tail = str(suffix or '').strip()
        if not tail:
            return base
        if not tail.startswith('/'):
            tail = f'/{tail}'
        if base == '/':
            return tail
        return f'{base}{tail}'

    def _resolve_profile_nav2_params_path(self, profile: Dict[str, Any], robot_id: str = '') -> str:
        configured = str(profile.get('nav2_params_file', '') or '').strip()
        candidates: List[str] = []

        if configured:
            expanded = os.path.expanduser(configured)
            if os.path.isabs(expanded):
                candidates.append(expanded)
            else:
                candidates.append(os.path.join(self.ui_root, expanded))
                candidates.append(os.path.join(self.robot_profiles_dir, expanded))
                profile_path = self.robot_profile_files.get(str(robot_id or '').strip(), '')
                if profile_path:
                    candidates.append(os.path.join(os.path.dirname(profile_path), expanded))
        else:
            candidates.append(os.path.join(self.ui_root, 'config', 'nav2_params.yaml'))

        deduped: List[str] = []
        seen = set()
        for path in candidates:
            abs_path = os.path.abspath(os.path.expanduser(str(path or '').strip()))
            if not abs_path or abs_path in seen:
                continue
            seen.add(abs_path)
            deduped.append(abs_path)

        for path in deduped:
            if os.path.exists(path):
                return path

        return deduped[0] if deduped else ''

    @staticmethod
    def _costmap_ros_params_section(payload: Dict[str, Any], scope: str, create: bool = False) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}

        scope_data = payload.get(scope)
        if not isinstance(scope_data, dict):
            if not create:
                return {}
            scope_data = {}
            payload[scope] = scope_data

        node_data = scope_data.get(scope)
        if not isinstance(node_data, dict):
            if not create:
                return {}
            node_data = {}
            scope_data[scope] = node_data

        ros_params = node_data.get('ros__parameters')
        if not isinstance(ros_params, dict):
            if not create:
                return {}
            ros_params = {}
            node_data['ros__parameters'] = ros_params

        return ros_params

    @staticmethod
    def _coerce_float(value: Any, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = float(default)
        if not math.isfinite(parsed):
            return float(default)
        return float(parsed)

    @staticmethod
    def _parse_footprint_points(raw: Any) -> List[List[float]]:
        source: Any = raw
        if isinstance(raw, str):
            text = str(raw or '').strip()
            if not text:
                source = []
            else:
                try:
                    source = yaml.safe_load(text)
                except Exception:
                    source = []

        points: List[List[float]] = []
        if isinstance(source, list):
            for item in source:
                x_val = None
                y_val = None
                if isinstance(item, dict):
                    x_val = item.get('x')
                    y_val = item.get('y')
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    x_val = item[0]
                    y_val = item[1]
                if x_val is None or y_val is None:
                    continue
                try:
                    x = float(x_val)
                    y = float(y_val)
                except Exception:
                    continue
                if not math.isfinite(x) or not math.isfinite(y):
                    continue
                points.append([round(x, 4), round(y, 4)])

        if len(points) > 1:
            first = points[0]
            last = points[-1]
            if abs(first[0] - last[0]) < 1e-4 and abs(first[1] - last[1]) < 1e-4:
                points = points[:-1]

        if len(points) < 3:
            return []
        return points[:40]

    @classmethod
    def _format_footprint_param(cls, points: Any) -> str:
        parsed = cls._parse_footprint_points(points)
        chunks = [f'[{round(pt[0], 4)}, {round(pt[1], 4)}]' for pt in parsed]
        return f'[{", ".join(chunks)}]'

    @classmethod
    def _footprint_default_for_radius(cls, radius: float) -> List[List[float]]:
        r = max(0.08, float(radius))
        half_x = round(max(0.12, r * 1.12), 4)
        half_y = round(max(0.10, r * 0.84), 4)
        return [
            [half_x, half_y],
            [half_x, -half_y],
            [-half_x, -half_y],
            [-half_x, half_y],
        ]

    @classmethod
    def _estimate_radius_from_footprint(cls, points: Any, default_radius: float = 0.30) -> float:
        parsed = cls._parse_footprint_points(points)
        if len(parsed) < 3:
            return float(default_radius)
        radius = 0.0
        for x, y in parsed:
            radius = max(radius, math.hypot(float(x), float(y)))
        if radius <= 0.0:
            return float(default_radius)
        return float(radius)

    @classmethod
    def _normalize_footprint_config(
        cls,
        mode: Any,
        robot_radius: Any,
        footprint: Any,
        footprint_padding: Any,
    ) -> Dict[str, Any]:
        normalized_mode = str(mode or 'circle').strip().lower()
        if normalized_mode not in {'circle', 'polygon'}:
            normalized_mode = 'circle'

        parsed_radius = cls._coerce_float(robot_radius, 0.30)
        parsed_radius = max(0.05, min(4.0, parsed_radius))

        parsed_padding = cls._coerce_float(footprint_padding, 0.01)
        parsed_padding = max(0.0, min(1.0, parsed_padding))

        parsed_footprint = cls._parse_footprint_points(footprint)
        if len(parsed_footprint) < 3:
            parsed_footprint = cls._footprint_default_for_radius(parsed_radius)

        if normalized_mode == 'polygon':
            parsed_radius = cls._estimate_radius_from_footprint(parsed_footprint, default_radius=parsed_radius)

        return {
            'mode': normalized_mode,
            'robot_radius': round(parsed_radius, 4),
            'footprint_padding': round(parsed_padding, 4),
            'footprint': parsed_footprint,
            'footprint_text': cls._format_footprint_param(parsed_footprint),
        }

    @classmethod
    def _footprint_scope_from_params(cls, ros_params: Any) -> Dict[str, Any]:
        params = ros_params if isinstance(ros_params, dict) else {}
        robot_radius_raw = params.get('robot_radius', None)
        robot_radius = None
        if robot_radius_raw is not None:
            try:
                robot_radius = float(robot_radius_raw)
            except Exception:
                robot_radius = None

        footprint_raw = params.get('footprint', '')
        footprint_pts = cls._parse_footprint_points(footprint_raw)

        padding = cls._coerce_float(params.get('footprint_padding', 0.0), 0.0)
        mode = 'unknown'
        if len(footprint_pts) >= 3:
            mode = 'polygon'
        elif robot_radius is not None:
            mode = 'circle'

        return {
            'mode': mode,
            'robot_radius': (round(float(robot_radius), 4) if robot_radius is not None else None),
            'footprint_padding': round(float(padding), 4),
            'footprint': footprint_pts,
            'footprint_text': cls._format_footprint_param(footprint_pts),
            'footprint_raw': footprint_raw,
        }

    def _resolve_costmap_node_names(self, profile: Dict[str, Any]) -> Dict[str, str]:
        namespace = self._normalize_namespace(profile.get('namespace', '/'))
        return {
            'local_costmap': self._join_namespace(namespace, '/local_costmap/local_costmap'),
            'global_costmap': self._join_namespace(namespace, '/global_costmap/global_costmap'),
        }

    @staticmethod
    def _parameter_value_to_python(param_value: ParameterValue) -> Any:
        if not isinstance(param_value, ParameterValue):
            return None

        ptype = int(param_value.type)
        if ptype == int(ParameterType.PARAMETER_BOOL):
            return bool(param_value.bool_value)
        if ptype == int(ParameterType.PARAMETER_INTEGER):
            return int(param_value.integer_value)
        if ptype == int(ParameterType.PARAMETER_DOUBLE):
            return float(param_value.double_value)
        if ptype == int(ParameterType.PARAMETER_STRING):
            return str(param_value.string_value)
        if ptype == int(ParameterType.PARAMETER_BOOL_ARRAY):
            return [bool(v) for v in param_value.bool_array_value]
        if ptype == int(ParameterType.PARAMETER_INTEGER_ARRAY):
            return [int(v) for v in param_value.integer_array_value]
        if ptype == int(ParameterType.PARAMETER_DOUBLE_ARRAY):
            return [float(v) for v in param_value.double_array_value]
        if ptype == int(ParameterType.PARAMETER_STRING_ARRAY):
            return [str(v) for v in param_value.string_array_value]
        return None

    @staticmethod
    def _python_to_parameter_value(value: Any) -> ParameterValue:
        if isinstance(value, bool):
            return ParameterValue(type=ParameterType.PARAMETER_BOOL, bool_value=bool(value))
        if isinstance(value, int) and not isinstance(value, bool):
            return ParameterValue(type=ParameterType.PARAMETER_INTEGER, integer_value=int(value))
        if isinstance(value, float):
            return ParameterValue(type=ParameterType.PARAMETER_DOUBLE, double_value=float(value))
        if isinstance(value, str):
            return ParameterValue(type=ParameterType.PARAMETER_STRING, string_value=str(value))
        if isinstance(value, list):
            if all(isinstance(v, (int, float)) for v in value):
                return ParameterValue(type=ParameterType.PARAMETER_DOUBLE_ARRAY, double_array_value=[float(v) for v in value])
            if all(isinstance(v, str) for v in value):
                return ParameterValue(type=ParameterType.PARAMETER_STRING_ARRAY, string_array_value=[str(v) for v in value])
        return ParameterValue(type=ParameterType.PARAMETER_NOT_SET)

    def _get_runtime_node_parameters(self, node_name: str, names: List[str]) -> Dict[str, Any]:
        clean_node = str(node_name or '').strip()
        clean_names = [str(name or '').strip() for name in names if str(name or '').strip()]
        if not clean_node or not clean_names:
            return self._ok(False, 'Node name and parameter names are required', values={})

        service_name = f'{clean_node}/get_parameters'
        client = self.create_client(GetParameters, service_name, callback_group=self.cb_group)
        try:
            req = GetParameters.Request()
            req.names = list(clean_names)
            response, err = self._call_service(client, req, wait_timeout=0.6, response_timeout=2.5)
        finally:
            try:
                self.destroy_client(client)
            except Exception:
                pass

        if err == 'service_not_available':
            return self._ok(False, f'{service_name} unavailable', values={})
        if err == 'timeout':
            return self._ok(False, f'{service_name} timed out', values={})
        if response is None:
            return self._ok(False, f'{service_name} failed', values={})

        values: Dict[str, Any] = {}
        response_values = list(response.values) if hasattr(response, 'values') else []
        for idx, name in enumerate(clean_names):
            msg_value = response_values[idx] if idx < len(response_values) else None
            values[name] = self._parameter_value_to_python(msg_value) if msg_value is not None else None

        return self._ok(True, f'Read {len(values)} parameters from {clean_node}', values=values)

    def _set_runtime_node_parameters(self, node_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        clean_node = str(node_name or '').strip()
        if not clean_node or not isinstance(params, dict) or not params:
            return self._ok(False, 'Node name and parameter updates are required')

        service_name = f'{clean_node}/set_parameters'
        client = self.create_client(SetParameters, service_name, callback_group=self.cb_group)
        try:
            req = SetParameters.Request()
            for name, value in params.items():
                param_name = str(name or '').strip()
                if not param_name:
                    continue
                req.parameters.append(
                    Parameter(
                        name=param_name,
                        value=self._python_to_parameter_value(value),
                    )
                )
            if not req.parameters:
                return self._ok(False, 'No valid parameter updates provided')
            response, err = self._call_service(client, req, wait_timeout=0.6, response_timeout=3.5)
        finally:
            try:
                self.destroy_client(client)
            except Exception:
                pass

        if err == 'service_not_available':
            return self._ok(False, f'{service_name} unavailable')
        if err == 'timeout':
            return self._ok(False, f'{service_name} timed out')
        if response is None:
            return self._ok(False, f'{service_name} failed')

        failures: List[str] = []
        results = list(response.results) if hasattr(response, 'results') else []
        for idx, result in enumerate(results):
            if bool(result.successful):
                continue
            key = req.parameters[idx].name if idx < len(req.parameters) else f'param_{idx}'
            reason = str(result.reason or 'unknown')
            failures.append(f'{key}: {reason}')

        if failures:
            return self._ok(False, f'Failed to set {clean_node} params: {"; ".join(failures)}')

        return self._ok(True, f'Applied {len(req.parameters)} params to {clean_node}')

    def _runtime_footprint_snapshot(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        node_names = self._resolve_costmap_node_names(profile)
        scopes: Dict[str, Any] = {}
        for scope in self.NAV2_COSTMAP_SCOPES:
            node_name = node_names.get(scope, '')
            get_result = self._get_runtime_node_parameters(node_name, ['robot_radius', 'footprint', 'footprint_padding'])
            if not bool(get_result.get('ok')):
                scopes[scope] = {
                    'available': False,
                    'node': node_name,
                    'error': str(get_result.get('message', 'parameter read failed') or 'parameter read failed'),
                    'mode': 'unknown',
                    'robot_radius': None,
                    'footprint_padding': None,
                    'footprint': [],
                    'footprint_text': '',
                }
                continue

            values = get_result.get('values', {}) if isinstance(get_result.get('values', {}), dict) else {}
            parsed = self._footprint_scope_from_params(values)
            parsed.update(
                {
                    'available': True,
                    'node': node_name,
                    'error': '',
                    'raw': values,
                }
            )
            scopes[scope] = parsed

        return {'scopes': scopes}

    @staticmethod
    def _footprint_points_match(expected: Any, actual: Any, tol: float = 0.02) -> bool:
        left = RosBridge._parse_footprint_points(expected)
        right = RosBridge._parse_footprint_points(actual)
        if len(left) != len(right):
            return False
        for idx in range(len(left)):
            if abs(float(left[idx][0]) - float(right[idx][0])) > tol:
                return False
            if abs(float(left[idx][1]) - float(right[idx][1])) > tol:
                return False
        return True

    @classmethod
    def _footprint_runtime_verification(cls, config: Dict[str, Any], runtime_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        scopes = runtime_snapshot.get('scopes', {}) if isinstance(runtime_snapshot.get('scopes', {}), dict) else {}
        mode = str(config.get('mode', 'circle') or 'circle').strip().lower()
        expected_radius = cls._coerce_float(config.get('robot_radius', 0.30), 0.30)
        expected_padding = cls._coerce_float(config.get('footprint_padding', 0.0), 0.0)
        expected_points = cls._parse_footprint_points(config.get('footprint', []))

        per_scope: Dict[str, Any] = {}
        all_ok = True
        for scope in cls.NAV2_COSTMAP_SCOPES:
            scope_data = scopes.get(scope, {}) if isinstance(scopes.get(scope, {}), dict) else {}
            if not bool(scope_data.get('available', False)):
                per_scope[scope] = {'ok': False, 'reason': str(scope_data.get('error', 'scope unavailable') or 'scope unavailable')}
                all_ok = False
                continue

            observed_mode = str(scope_data.get('mode', 'unknown') or 'unknown')
            observed_radius = scope_data.get('robot_radius', None)
            observed_padding = scope_data.get('footprint_padding', None)
            observed_points = cls._parse_footprint_points(scope_data.get('footprint', []))

            if mode == 'polygon':
                points_ok = cls._footprint_points_match(expected_points, observed_points, tol=0.03)
                padding_ok = observed_padding is not None and abs(float(observed_padding) - float(expected_padding)) <= 0.03
                ok = points_ok and padding_ok and observed_mode == 'polygon'
                reason = 'ok' if ok else 'runtime polygon mismatch'
            else:
                radius_ok = observed_radius is not None and abs(float(observed_radius) - float(expected_radius)) <= 0.03
                padding_ok = observed_padding is not None and abs(float(observed_padding) - float(expected_padding)) <= 0.03
                mode_ok = observed_mode in {'circle', 'unknown'}
                ok = radius_ok and padding_ok and mode_ok
                reason = 'ok' if ok else 'runtime circle mismatch'

            per_scope[scope] = {
                'ok': bool(ok),
                'reason': reason,
                'observed_mode': observed_mode,
            }
            all_ok = all_ok and bool(ok)

        return {'ok': bool(all_ok), 'scopes': per_scope}

    def get_profile_nav2_footprint(self, robot_id: str = '', include_runtime: bool = True):
        target_robot_id = str(robot_id or '').strip()
        profiles = self._load_robot_profiles()
        if not profiles:
            return self._ok(False, 'No robot profiles available')

        if not target_robot_id:
            target_robot_id = str(self.active_robot_profile_id or '').strip()
        if not target_robot_id or target_robot_id not in profiles:
            target_robot_id = sorted(profiles.keys())[0]

        profile = profiles.get(target_robot_id)
        if not isinstance(profile, dict):
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        nav2_params_path = self._resolve_profile_nav2_params_path(profile, target_robot_id)
        if not nav2_params_path:
            return self._ok(False, f'Profile "{target_robot_id}" has no nav2 params file configured')

        if not os.path.exists(nav2_params_path):
            return self._ok(False, f'Nav2 params file does not exist: {nav2_params_path}')

        try:
            with open(nav2_params_path, 'r', encoding='utf-8') as f:
                nav2_payload = yaml.safe_load(f) or {}
        except Exception as exc:
            return self._ok(False, f'Failed reading Nav2 params file: {exc}')

        if not isinstance(nav2_payload, dict):
            return self._ok(False, 'Nav2 params file is not a YAML dictionary')

        yaml_scopes: Dict[str, Any] = {}
        for scope in self.NAV2_COSTMAP_SCOPES:
            ros_params = self._costmap_ros_params_section(nav2_payload, scope, create=False)
            yaml_scopes[scope] = self._footprint_scope_from_params(ros_params)

        primary_scope = yaml_scopes.get('local_costmap', {})
        if str(primary_scope.get('mode', 'unknown')) == 'unknown':
            primary_scope = yaml_scopes.get('global_costmap', {})

        config = self._normalize_footprint_config(
            primary_scope.get('mode', 'circle'),
            primary_scope.get('robot_radius', 0.30),
            primary_scope.get('footprint', self.NAV2_FOOTPRINT_DEFAULT),
            primary_scope.get('footprint_padding', 0.0),
        )

        runtime_snapshot = {'scopes': {}}
        verification = {'ok': False, 'scopes': {}}
        if include_runtime:
            runtime_snapshot = self._runtime_footprint_snapshot(profile)
            verification = self._footprint_runtime_verification(config, runtime_snapshot)

        return self._ok(
            True,
            f'Loaded footprint config for profile "{target_robot_id}"',
            robot_id=target_robot_id,
            profile_version=int(profile.get('profile_version', 1) or 1),
            nav2_params_file=nav2_params_path,
            config=config,
            yaml_scopes=yaml_scopes,
            runtime_readback=runtime_snapshot,
            verification=verification,
        )

    def set_profile_nav2_footprint(
        self,
        *,
        robot_id: str,
        mode: Any,
        robot_radius: Any,
        footprint: Any,
        footprint_padding: Any,
        apply_runtime: bool = True,
    ):
        target_robot_id = str(robot_id or '').strip()
        if not target_robot_id:
            return self._ok(False, 'robot_id is required')

        profiles = self._load_robot_profiles()
        profile = profiles.get(target_robot_id)
        if not isinstance(profile, dict):
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        nav2_params_path = self._resolve_profile_nav2_params_path(profile, target_robot_id)
        if not nav2_params_path:
            return self._ok(False, f'Profile "{target_robot_id}" has no nav2 params file configured')

        if not os.path.exists(nav2_params_path):
            return self._ok(False, f'Nav2 params file does not exist: {nav2_params_path}')

        normalized = self._normalize_footprint_config(mode, robot_radius, footprint, footprint_padding)
        footprint_text = str(normalized.get('footprint_text', '[]') or '[]')

        try:
            with open(nav2_params_path, 'r', encoding='utf-8') as f:
                nav2_payload = yaml.safe_load(f) or {}
        except Exception as exc:
            return self._ok(False, f'Failed reading Nav2 params file: {exc}')
        if not isinstance(nav2_payload, dict):
            nav2_payload = {}

        for scope in self.NAV2_COSTMAP_SCOPES:
            ros_params = self._costmap_ros_params_section(nav2_payload, scope, create=True)
            ros_params['footprint_padding'] = float(normalized['footprint_padding'])
            if normalized['mode'] == 'polygon':
                ros_params['footprint'] = footprint_text
                ros_params.pop('robot_radius', None)
            else:
                ros_params['robot_radius'] = float(normalized['robot_radius'])
                ros_params['footprint'] = '[]'

        try:
            with open(nav2_params_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(nav2_payload, f, sort_keys=False)
        except Exception as exc:
            return self._ok(False, f'Failed writing Nav2 params file: {exc}')

        runtime_apply: Dict[str, Any] = {'applied': False, 'scopes': {}}
        if apply_runtime:
            node_names = self._resolve_costmap_node_names(profile)
            apply_ok = True
            for scope in self.NAV2_COSTMAP_SCOPES:
                node_name = node_names.get(scope, '')
                updates: Dict[str, Any] = {
                    'footprint_padding': float(normalized['footprint_padding']),
                }
                if normalized['mode'] == 'polygon':
                    updates['footprint'] = footprint_text
                else:
                    updates['robot_radius'] = float(normalized['robot_radius'])
                    updates['footprint'] = '[]'
                set_result = self._set_runtime_node_parameters(node_name, updates)
                runtime_apply['scopes'][scope] = set_result
                apply_ok = apply_ok and bool(set_result.get('ok'))

            runtime_apply['applied'] = bool(apply_ok)
            if apply_ok:
                self._clear_nav2_costmaps()

        runtime_snapshot = self._runtime_footprint_snapshot(profile) if apply_runtime else {'scopes': {}}
        verification = self._footprint_runtime_verification(normalized, runtime_snapshot) if apply_runtime else {'ok': False, 'scopes': {}}

        yaml_scopes: Dict[str, Any] = {}
        for scope in self.NAV2_COSTMAP_SCOPES:
            ros_params = self._costmap_ros_params_section(nav2_payload, scope, create=False)
            yaml_scopes[scope] = self._footprint_scope_from_params(ros_params)

        message = f'Updated footprint in {os.path.basename(nav2_params_path)}'
        if apply_runtime:
            if bool(verification.get('ok')):
                message = f'{message}. Runtime readback matches.'
            else:
                message = f'{message}. Runtime apply/readback has warnings.'

        return self._ok(
            True,
            message,
            robot_id=target_robot_id,
            nav2_params_file=nav2_params_path,
            config=normalized,
            yaml_scopes=yaml_scopes,
            runtime_apply=runtime_apply,
            runtime_readback=runtime_snapshot,
            verification=verification,
        )

    def _resolve_nav2_template_params_path(self) -> str:
        configured = str(
            self._get_or_declare_parameter(
                'nav2_template_params_file',
                os.path.join(self.ui_root, 'config', 'nav2_params.yaml'),
            )
            or ''
        ).strip()
        if not configured:
            configured = os.path.join(self.ui_root, 'config', 'nav2_params.yaml')
        expanded = os.path.abspath(os.path.expanduser(configured))
        if os.path.isabs(configured):
            return expanded
        return os.path.abspath(os.path.join(self.ui_root, configured))

    @staticmethod
    def _flatten_tree_paths(value: Any, prefix: str = '') -> Dict[str, Any]:
        flat: Dict[str, Any] = {}
        if isinstance(value, dict):
            if prefix and not value:
                flat[prefix] = {}
            for key in sorted(value.keys(), key=lambda item: str(item)):
                name = str(key)
                child_prefix = f'{prefix}/{name}' if prefix else name
                flat.update(RosBridge._flatten_tree_paths(value.get(key), child_prefix))
            return flat

        if not prefix:
            return flat

        if isinstance(value, (list, dict)):
            flat[prefix] = copy.deepcopy(value)
        else:
            flat[prefix] = value
        return flat

    @staticmethod
    def _tree_path_get(root: Dict[str, Any], path: str, default: Any = None) -> Any:
        if not isinstance(root, dict):
            return default
        segments = [segment for segment in str(path or '').split('/') if segment]
        if not segments:
            return default
        current: Any = root
        for segment in segments:
            if not isinstance(current, dict) or segment not in current:
                return default
            current = current.get(segment)
        return copy.deepcopy(current)

    @staticmethod
    def _tree_path_set(root: Dict[str, Any], path: str, value: Any, create: bool = True) -> bool:
        if not isinstance(root, dict):
            return False
        segments = [segment for segment in str(path or '').split('/') if segment]
        if not segments:
            return False

        current: Any = root
        for segment in segments[:-1]:
            if not isinstance(current, dict):
                return False
            child = current.get(segment)
            if isinstance(child, dict):
                current = child
                continue
            if not create:
                return False
            child = {}
            current[segment] = child
            current = child

        if not isinstance(current, dict):
            return False
        current[segments[-1]] = copy.deepcopy(value)
        return True

    @staticmethod
    def _canonical_json(value: Any) -> str:
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=False)
        except Exception:
            return repr(value)

    @classmethod
    def _build_tree_diff(cls, current_payload: Dict[str, Any], baseline_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        current_flat = cls._flatten_tree_paths(current_payload)
        baseline_flat = cls._flatten_tree_paths(baseline_payload)
        all_paths = sorted(set(current_flat.keys()) | set(baseline_flat.keys()))

        diff: List[Dict[str, Any]] = []
        for path in all_paths:
            in_current = path in current_flat
            in_baseline = path in baseline_flat

            if in_current and not in_baseline:
                diff.append(
                    {
                        'path': path,
                        'kind': 'added',
                        'current': copy.deepcopy(current_flat.get(path)),
                        'baseline': None,
                    }
                )
                continue

            if in_baseline and not in_current:
                diff.append(
                    {
                        'path': path,
                        'kind': 'removed',
                        'current': None,
                        'baseline': copy.deepcopy(baseline_flat.get(path)),
                    }
                )
                continue

            current_val = current_flat.get(path)
            baseline_val = baseline_flat.get(path)
            if cls._canonical_json(current_val) != cls._canonical_json(baseline_val):
                diff.append(
                    {
                        'path': path,
                        'kind': 'changed',
                        'current': copy.deepcopy(current_val),
                        'baseline': copy.deepcopy(baseline_val),
                    }
                )

        return diff

    @staticmethod
    def _flatten_tree_entries(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        flat = RosBridge._flatten_tree_paths(payload)
        for path in sorted(flat.keys()):
            value = flat.get(path)
            entries.append(
                {
                    'path': path,
                    'value': copy.deepcopy(value),
                    'value_json': RosBridge._canonical_json(value),
                    'type': type(value).__name__,
                }
            )
        return entries

    @staticmethod
    def _nav2_safe_live_param_bindings() -> Dict[str, Tuple[str, str]]:
        return {
            'local_costmap/local_costmap/ros__parameters/inflation_layer/inflation_radius': ('local_costmap', 'inflation_layer.inflation_radius'),
            'global_costmap/global_costmap/ros__parameters/inflation_layer/inflation_radius': ('global_costmap', 'inflation_layer.inflation_radius'),
            'controller_server/ros__parameters/FollowPath/max_vel_x': ('controller_server', 'FollowPath.max_vel_x'),
            'controller_server/ros__parameters/FollowPath/min_vel_x': ('controller_server', 'FollowPath.min_vel_x'),
            'controller_server/ros__parameters/FollowPath/max_vel_theta': ('controller_server', 'FollowPath.max_vel_theta'),
            'controller_server/ros__parameters/FollowPath/min_speed_xy': ('controller_server', 'FollowPath.min_speed_xy'),
            'controller_server/ros__parameters/FollowPath/max_speed_xy': ('controller_server', 'FollowPath.max_speed_xy'),
            'controller_server/ros__parameters/FollowPath/desired_linear_vel': ('controller_server', 'FollowPath.desired_linear_vel'),
            'controller_server/ros__parameters/FollowPath/acc_lim_x': ('controller_server', 'FollowPath.acc_lim_x'),
            'controller_server/ros__parameters/FollowPath/acc_lim_theta': ('controller_server', 'FollowPath.acc_lim_theta'),
            'controller_server/ros__parameters/FollowPath/decel_lim_x': ('controller_server', 'FollowPath.decel_lim_x'),
            'controller_server/ros__parameters/FollowPath/decel_lim_theta': ('controller_server', 'FollowPath.decel_lim_theta'),
            'controller_server/ros__parameters/FollowPath/max_v': ('controller_server', 'FollowPath.max_v'),
            'controller_server/ros__parameters/FollowPath/max_w': ('controller_server', 'FollowPath.max_w'),
            'controller_server/ros__parameters/FollowPath/min_tracking_speed': ('controller_server', 'FollowPath.min_tracking_speed'),
            'controller_server/ros__parameters/FollowPath/acc_lim_v': ('controller_server', 'FollowPath.acc_lim_v'),
            'controller_server/ros__parameters/FollowPath/acc_lim_w': ('controller_server', 'FollowPath.acc_lim_w'),
            'controller_server/ros__parameters/FollowPath/rpp_desired_linear_vel': ('controller_server', 'FollowPath.rpp_desired_linear_vel'),
            'amcl/ros__parameters/scan_topic': ('amcl', 'scan_topic'),
            'local_costmap/local_costmap/ros__parameters/voxel_layer/scan/topic': ('local_costmap', 'voxel_layer.scan.topic'),
            'global_costmap/global_costmap/ros__parameters/obstacle_layer/scan/topic': ('global_costmap', 'obstacle_layer.scan.topic'),
            'amcl/ros__parameters/base_frame_id': ('amcl', 'base_frame_id'),
            'amcl/ros__parameters/odom_frame_id': ('amcl', 'odom_frame_id'),
            'amcl/ros__parameters/global_frame_id': ('amcl', 'global_frame_id'),
            'local_costmap/local_costmap/ros__parameters/robot_base_frame': ('local_costmap', 'robot_base_frame'),
            'local_costmap/local_costmap/ros__parameters/global_frame': ('local_costmap', 'global_frame'),
            'global_costmap/global_costmap/ros__parameters/robot_base_frame': ('global_costmap', 'robot_base_frame'),
            'global_costmap/global_costmap/ros__parameters/global_frame': ('global_costmap', 'global_frame'),
            'bt_navigator/ros__parameters/robot_base_frame': ('bt_navigator', 'robot_base_frame'),
            'bt_navigator/ros__parameters/global_frame': ('bt_navigator', 'global_frame'),
        }

    def _resolve_nav2_runtime_nodes(self, profile: Dict[str, Any]) -> Dict[str, str]:
        namespace = self._normalize_namespace(profile.get('namespace', '/'))
        nodes = self._resolve_costmap_node_names(profile)
        nodes.update(
            {
                'controller_server': self._join_namespace(namespace, '/controller_server'),
                'amcl': self._join_namespace(namespace, '/amcl'),
                'bt_navigator': self._join_namespace(namespace, '/bt_navigator'),
            }
        )
        return nodes

    def _extract_nav2_curated_subset(self, nav2_payload: Dict[str, Any], footprint_config: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
        def _get(path: str, default: Any = None) -> Any:
            return self._tree_path_get(nav2_payload, path, default)

        map_frame = str(
            _get('amcl/ros__parameters/global_frame_id', '')
            or _get('bt_navigator/ros__parameters/global_frame', '')
            or _get('global_costmap/global_costmap/ros__parameters/global_frame', '')
            or profile.get('sensor_frames', {}).get('map', 'map')
            or 'map'
        )
        odom_frame = str(
            _get('amcl/ros__parameters/odom_frame_id', '')
            or _get('local_costmap/local_costmap/ros__parameters/global_frame', '')
            or profile.get('sensor_frames', {}).get('odom', 'odom')
            or 'odom'
        )
        base_frame = str(
            _get('amcl/ros__parameters/base_frame_id', '')
            or _get('bt_navigator/ros__parameters/robot_base_frame', '')
            or profile.get('base_frame', 'base_link')
            or 'base_link'
        )
        scan_topic = str(
            _get('amcl/ros__parameters/scan_topic', '')
            or _get('local_costmap/local_costmap/ros__parameters/voxel_layer/scan/topic', '')
            or _get('global_costmap/global_costmap/ros__parameters/obstacle_layer/scan/topic', '')
            or '/scan'
        )

        curated = {
            'footprint': copy.deepcopy(footprint_config),
            'inflation': {
                'local_radius': self._coerce_float(
                    _get('local_costmap/local_costmap/ros__parameters/inflation_layer/inflation_radius', 1.0),
                    1.0,
                ),
                'global_radius': self._coerce_float(
                    _get('global_costmap/global_costmap/ros__parameters/inflation_layer/inflation_radius', 1.0),
                    1.0,
                ),
            },
            'controller_limits': {
                'max_vel_x': self._coerce_float(
                    _get(
                        'controller_server/ros__parameters/FollowPath/max_vel_x',
                        _get(
                            'controller_server/ros__parameters/FollowPath/max_v',
                            _get('controller_server/ros__parameters/FollowPath/desired_linear_vel', 0.55),
                        ),
                    ),
                    0.55,
                ),
                'min_vel_x': self._coerce_float(_get('controller_server/ros__parameters/FollowPath/min_vel_x', 0.0), 0.0),
                'max_vel_theta': self._coerce_float(
                    _get(
                        'controller_server/ros__parameters/FollowPath/max_vel_theta',
                        _get('controller_server/ros__parameters/FollowPath/max_w', 1.0),
                    ),
                    1.0,
                ),
                'min_speed_xy': self._coerce_float(
                    _get(
                        'controller_server/ros__parameters/FollowPath/min_speed_xy',
                        _get('controller_server/ros__parameters/FollowPath/min_tracking_speed', 0.0),
                    ),
                    0.0,
                ),
                'max_speed_xy': self._coerce_float(
                    _get(
                        'controller_server/ros__parameters/FollowPath/max_speed_xy',
                        _get(
                            'controller_server/ros__parameters/FollowPath/max_v',
                            _get('controller_server/ros__parameters/FollowPath/desired_linear_vel', 0.55),
                        ),
                    ),
                    0.55,
                ),
            },
            'acceleration_limits': {
                'acc_lim_x': self._coerce_float(
                    _get(
                        'controller_server/ros__parameters/FollowPath/acc_lim_x',
                        _get('controller_server/ros__parameters/FollowPath/acc_lim_v', 2.0),
                    ),
                    2.0,
                ),
                'acc_lim_theta': self._coerce_float(
                    _get(
                        'controller_server/ros__parameters/FollowPath/acc_lim_theta',
                        _get('controller_server/ros__parameters/FollowPath/acc_lim_w', 3.0),
                    ),
                    3.0,
                ),
                'decel_lim_x': self._coerce_float(
                    _get(
                        'controller_server/ros__parameters/FollowPath/decel_lim_x',
                        -abs(self._coerce_float(_get('controller_server/ros__parameters/FollowPath/acc_lim_v', 2.0), 2.0)),
                    ),
                    -2.0,
                ),
                'decel_lim_theta': self._coerce_float(
                    _get(
                        'controller_server/ros__parameters/FollowPath/decel_lim_theta',
                        -abs(self._coerce_float(_get('controller_server/ros__parameters/FollowPath/acc_lim_w', 3.0), 3.0)),
                    ),
                    -3.0,
                ),
            },
            'sensor_topics': {
                'scan_topic': scan_topic,
                'amcl_scan_topic': str(_get('amcl/ros__parameters/scan_topic', scan_topic)),
                'local_scan_topic': str(_get('local_costmap/local_costmap/ros__parameters/voxel_layer/scan/topic', scan_topic)),
                'global_scan_topic': str(_get('global_costmap/global_costmap/ros__parameters/obstacle_layer/scan/topic', scan_topic)),
            },
            'frames': {
                'map_frame': map_frame,
                'odom_frame': odom_frame,
                'base_frame': base_frame,
            },
        }
        return curated

    def _curated_subset_to_path_updates(self, curated: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        updates: Dict[str, Any] = {}
        footprint_payload: Dict[str, Any] = {}
        source = curated if isinstance(curated, dict) else {}

        footprint = source.get('footprint', {}) if isinstance(source.get('footprint', {}), dict) else {}
        if footprint:
            footprint_payload = {
                'mode': str(footprint.get('mode', 'circle') or 'circle').strip().lower(),
                'robot_radius': self._coerce_float(footprint.get('robot_radius', 0.30), 0.30),
                'footprint': footprint.get('footprint', []),
                'footprint_padding': self._coerce_float(footprint.get('footprint_padding', 0.0), 0.0),
            }

        inflation = source.get('inflation', {}) if isinstance(source.get('inflation', {}), dict) else {}
        if 'local_radius' in inflation:
            updates['local_costmap/local_costmap/ros__parameters/inflation_layer/inflation_radius'] = self._coerce_float(inflation.get('local_radius', 1.0), 1.0)
        if 'global_radius' in inflation:
            updates['global_costmap/global_costmap/ros__parameters/inflation_layer/inflation_radius'] = self._coerce_float(inflation.get('global_radius', 1.0), 1.0)

        controller = source.get('controller_limits', {}) if isinstance(source.get('controller_limits', {}), dict) else {}
        for key in ('max_vel_x', 'min_vel_x', 'max_vel_theta', 'min_speed_xy', 'max_speed_xy'):
            if key in controller:
                value = self._coerce_float(controller.get(key, 0.0), 0.0)
                updates[f'controller_server/ros__parameters/FollowPath/{key}'] = value
                if key in ('max_vel_x', 'max_speed_xy'):
                    updates['controller_server/ros__parameters/FollowPath/max_v'] = value
                    updates['controller_server/ros__parameters/FollowPath/rpp_desired_linear_vel'] = value
                    updates['controller_server/ros__parameters/FollowPath/desired_linear_vel'] = value
                elif key == 'max_vel_theta':
                    updates['controller_server/ros__parameters/FollowPath/max_w'] = value
                elif key == 'min_speed_xy':
                    updates['controller_server/ros__parameters/FollowPath/min_tracking_speed'] = value

        accel = source.get('acceleration_limits', {}) if isinstance(source.get('acceleration_limits', {}), dict) else {}
        for key in ('acc_lim_x', 'acc_lim_theta', 'decel_lim_x', 'decel_lim_theta'):
            if key in accel:
                value = self._coerce_float(accel.get(key, 0.0), 0.0)
                updates[f'controller_server/ros__parameters/FollowPath/{key}'] = value
                if key == 'acc_lim_x':
                    updates['controller_server/ros__parameters/FollowPath/acc_lim_v'] = value
                elif key == 'acc_lim_theta':
                    updates['controller_server/ros__parameters/FollowPath/acc_lim_w'] = value

        topics = source.get('sensor_topics', {}) if isinstance(source.get('sensor_topics', {}), dict) else {}
        scan_topic = str(topics.get('scan_topic', '') or '').strip()
        if scan_topic:
            updates['amcl/ros__parameters/scan_topic'] = scan_topic
            updates['local_costmap/local_costmap/ros__parameters/voxel_layer/scan/topic'] = scan_topic
            updates['global_costmap/global_costmap/ros__parameters/obstacle_layer/scan/topic'] = scan_topic
        else:
            if 'amcl_scan_topic' in topics:
                updates['amcl/ros__parameters/scan_topic'] = str(topics.get('amcl_scan_topic', '') or '').strip()
            if 'local_scan_topic' in topics:
                updates['local_costmap/local_costmap/ros__parameters/voxel_layer/scan/topic'] = str(topics.get('local_scan_topic', '') or '').strip()
            if 'global_scan_topic' in topics:
                updates['global_costmap/global_costmap/ros__parameters/obstacle_layer/scan/topic'] = str(topics.get('global_scan_topic', '') or '').strip()

        frames = source.get('frames', {}) if isinstance(source.get('frames', {}), dict) else {}
        map_frame = str(frames.get('map_frame', '') or '').strip()
        odom_frame = str(frames.get('odom_frame', '') or '').strip()
        base_frame = str(frames.get('base_frame', '') or '').strip()
        if map_frame:
            updates['amcl/ros__parameters/global_frame_id'] = map_frame
            updates['bt_navigator/ros__parameters/global_frame'] = map_frame
            updates['global_costmap/global_costmap/ros__parameters/global_frame'] = map_frame
        if odom_frame:
            updates['amcl/ros__parameters/odom_frame_id'] = odom_frame
            updates['local_costmap/local_costmap/ros__parameters/global_frame'] = odom_frame
        if base_frame:
            updates['amcl/ros__parameters/base_frame_id'] = base_frame
            updates['bt_navigator/ros__parameters/robot_base_frame'] = base_frame
            updates['local_costmap/local_costmap/ros__parameters/robot_base_frame'] = base_frame
            updates['global_costmap/global_costmap/ros__parameters/robot_base_frame'] = base_frame

        return updates, footprint_payload

    def get_profile_nav2_parameter_editor(self, robot_id: str = '', include_runtime: bool = True):
        target_robot_id = str(robot_id or '').strip()
        profiles = self._load_robot_profiles()
        if not profiles:
            return self._ok(False, 'No robot profiles available')

        if not target_robot_id:
            target_robot_id = str(self.active_robot_profile_id or '').strip()
        if not target_robot_id or target_robot_id not in profiles:
            target_robot_id = sorted(profiles.keys())[0]

        profile = profiles.get(target_robot_id)
        if not isinstance(profile, dict):
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        nav2_params_path = self._resolve_profile_nav2_params_path(profile, target_robot_id)
        if not nav2_params_path:
            return self._ok(False, f'Profile "{target_robot_id}" has no nav2 params file configured')
        if not os.path.exists(nav2_params_path):
            return self._ok(False, f'Nav2 params file does not exist: {nav2_params_path}')

        try:
            with open(nav2_params_path, 'r', encoding='utf-8') as f:
                nav2_payload = yaml.safe_load(f) or {}
        except Exception as exc:
            return self._ok(False, f'Failed reading Nav2 params file: {exc}')
        if not isinstance(nav2_payload, dict):
            nav2_payload = {}

        template_path = self._resolve_nav2_template_params_path()
        template_payload: Dict[str, Any] = {}
        if template_path and os.path.exists(template_path):
            try:
                with open(template_path, 'r', encoding='utf-8') as f:
                    loaded = yaml.safe_load(f) or {}
                if isinstance(loaded, dict):
                    template_payload = loaded
            except Exception as exc:
                self.get_logger().warn(f'Failed loading Nav2 template params "{template_path}": {exc}')

        footprint_result = self.get_profile_nav2_footprint(robot_id=target_robot_id, include_runtime=include_runtime)
        if bool(footprint_result.get('ok')):
            footprint_config = footprint_result.get('config', {})
            footprint_runtime = footprint_result.get('runtime_readback', {})
            footprint_verification = footprint_result.get('verification', {})
        else:
            fallback = self._normalize_footprint_config('circle', 0.30, self.NAV2_FOOTPRINT_DEFAULT, 0.0)
            footprint_config = fallback
            footprint_runtime = {'scopes': {}}
            footprint_verification = {'ok': False, 'scopes': {}}

        curated = self._extract_nav2_curated_subset(nav2_payload, footprint_config, profile)
        diff = self._build_tree_diff(nav2_payload, template_payload)
        full_tree = self._flatten_tree_entries(nav2_payload)
        template_tree = self._flatten_tree_entries(template_payload)

        diagnostics = self.nav2_diagnostics() if include_runtime else {}
        stack = self.get_stack_status() if include_runtime else {}
        validation = {
            'stack': stack,
            'diagnostics': diagnostics,
            'nav_active': bool(stack.get('ok')) and bool(stack.get('nav_running')),
            'costmap_update_inputs_ok': bool(diagnostics.get('topics', {}).get('has_map')) and bool(diagnostics.get('topics', {}).get('has_scan_input')),
            'footprint_runtime_ok': bool(footprint_verification.get('ok', False)),
        }

        return self._ok(
            True,
            f'Loaded Nav2 parameter editor for profile "{target_robot_id}"',
            robot_id=target_robot_id,
            profile_version=int(profile.get('profile_version', 1) or 1),
            nav2_params_file=nav2_params_path,
            template_params_file=template_path,
            curated=curated,
            full_tree=full_tree,
            template_tree=template_tree,
            diff=diff,
            safe_live_paths=sorted(self._nav2_safe_live_param_bindings().keys()),
            footprint_runtime=footprint_runtime,
            footprint_verification=footprint_verification,
            validation=validation,
        )

    def apply_profile_nav2_parameter_editor(
        self,
        *,
        robot_id: str,
        apply_mode: str,
        curated_updates: Any,
        advanced_updates: Any,
        restart_nav: bool = False,
    ):
        target_robot_id = str(robot_id or '').strip()
        if not target_robot_id:
            return self._ok(False, 'robot_id is required')

        mode = str(apply_mode or 'offline').strip().lower()
        if mode not in {'offline', 'online', 'both'}:
            mode = 'offline'

        profiles = self._load_robot_profiles()
        profile = profiles.get(target_robot_id)
        if not isinstance(profile, dict):
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        nav2_params_path = self._resolve_profile_nav2_params_path(profile, target_robot_id)
        if not nav2_params_path:
            return self._ok(False, f'Profile "{target_robot_id}" has no nav2 params file configured')
        if not os.path.exists(nav2_params_path):
            return self._ok(False, f'Nav2 params file does not exist: {nav2_params_path}')

        try:
            with open(nav2_params_path, 'r', encoding='utf-8') as f:
                nav2_payload = yaml.safe_load(f) or {}
        except Exception as exc:
            return self._ok(False, f'Failed reading Nav2 params file: {exc}')
        if not isinstance(nav2_payload, dict):
            nav2_payload = {}

        path_updates: Dict[str, Any] = {}
        curated_path_updates, curated_footprint = self._curated_subset_to_path_updates(curated_updates)
        path_updates.update(curated_path_updates)

        advanced_raw = advanced_updates
        if isinstance(advanced_raw, dict):
            for raw_path, value in advanced_raw.items():
                path = str(raw_path or '').strip().strip('/')
                if not path:
                    continue
                path_updates[path] = copy.deepcopy(value)
        elif isinstance(advanced_raw, list):
            for item in advanced_raw:
                if not isinstance(item, dict):
                    continue
                path = str(item.get('path', '') or '').strip().strip('/')
                if not path:
                    continue
                path_updates[path] = copy.deepcopy(item.get('value'))

        applied_yaml_paths: List[str] = []
        skipped_yaml_paths: List[str] = []
        for path, value in path_updates.items():
            if self._tree_path_set(nav2_payload, path, value, create=True):
                applied_yaml_paths.append(path)
            else:
                skipped_yaml_paths.append(path)

        try:
            with open(nav2_params_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(nav2_payload, f, sort_keys=False)
        except Exception as exc:
            return self._ok(False, f'Failed writing Nav2 params file: {exc}')

        online_enabled = mode in {'online', 'both'}
        runtime_results: Dict[str, Any] = {'enabled': online_enabled, 'applied': {}, 'skipped': []}
        safe_bindings = self._nav2_safe_live_param_bindings()
        runtime_nodes = self._resolve_nav2_runtime_nodes(profile)

        footprint_runtime_result: Dict[str, Any] = {}
        if curated_footprint:
            footprint_set = self.set_profile_nav2_footprint(
                robot_id=target_robot_id,
                mode=curated_footprint.get('mode', 'circle'),
                robot_radius=curated_footprint.get('robot_radius', 0.30),
                footprint=curated_footprint.get('footprint', []),
                footprint_padding=curated_footprint.get('footprint_padding', 0.0),
                apply_runtime=online_enabled,
            )
            footprint_runtime_result = footprint_set
            if not bool(footprint_set.get('ok')):
                return self._ok(False, f'Failed applying footprint settings: {footprint_set.get("message", "")}')

        if online_enabled:
            grouped_updates: Dict[str, Dict[str, Any]] = {}
            for path, value in path_updates.items():
                binding = safe_bindings.get(path)
                if not binding:
                    runtime_results['skipped'].append({'path': path, 'reason': 'not_live_safe'})
                    continue

                alias, param_name = binding
                node_name = runtime_nodes.get(alias, '')
                if not node_name:
                    runtime_results['skipped'].append({'path': path, 'reason': f'node_alias_unresolved:{alias}'})
                    continue

                if path.endswith('/footprint'):
                    parsed_points = self._parse_footprint_points(value)
                    grouped_updates.setdefault(node_name, {})[param_name] = self._format_footprint_param(parsed_points)
                else:
                    grouped_updates.setdefault(node_name, {})[param_name] = value

            for node_name in sorted(grouped_updates.keys()):
                set_result = self._set_runtime_node_parameters(node_name, grouped_updates.get(node_name, {}))
                runtime_results['applied'][node_name] = set_result

            needs_costmap_refresh = bool(curated_footprint) or any('inflation_radius' in path for path in path_updates.keys())
            if needs_costmap_refresh:
                clear_ok, clear_msg = self._clear_nav2_costmaps()
                runtime_results['costmap_clear'] = {'ok': bool(clear_ok), 'message': clear_msg}

        restart_result: Dict[str, Any] = {'requested': bool(restart_nav), 'performed': False}
        if mode in {'offline', 'both'} and bool(restart_nav):
            stack_before = self.get_stack_status()
            stop_result = self.set_stack_mode('stop')
            nav_result = self.set_stack_mode('nav')
            restart_result = {
                'requested': True,
                'performed': True,
                'stack_before': stack_before,
                'stop': stop_result,
                'nav': nav_result,
                'ok': bool(stop_result.get('ok')) and bool(nav_result.get('ok')),
            }

        editor_snapshot = self.get_profile_nav2_parameter_editor(robot_id=target_robot_id, include_runtime=True)
        if not bool(editor_snapshot.get('ok')):
            return self._ok(False, f'Params saved but validation reload failed: {editor_snapshot.get("message", "")}')

        message = f'Applied Nav2 parameter changes ({len(applied_yaml_paths)} YAML updates)'
        if skipped_yaml_paths:
            message = f'{message}. Skipped {len(skipped_yaml_paths)} YAML paths.'
        if online_enabled:
            message = f'{message}. Online apply attempted for live-safe params.'
        if mode in {'offline', 'both'}:
            message = f'{message}. Offline apply complete; restart required for non-live-safe params.'

        return self._ok(
            True,
            message,
            robot_id=target_robot_id,
            apply_mode=mode,
            nav2_params_file=nav2_params_path,
            yaml_updates=sorted(applied_yaml_paths),
            yaml_skipped=sorted(skipped_yaml_paths),
            runtime=runtime_results,
            footprint_result=footprint_runtime_result,
            restart=restart_result,
            editor=editor_snapshot,
            validation=editor_snapshot.get('validation', {}),
        )

    # ---------- bringup wizard ----------

    @staticmethod
    def _bringup_log(log_lines: List[str], message: str):
        timestamp = time.strftime('%H:%M:%S', time.localtime())
        log_lines.append(f'[{timestamp}] {str(message or "").strip()}')

    def _bringup_report_dir(self, robot_id: str) -> str:
        return os.path.join(self.robot_profiles_dir, 'bringup_reports', self._safe_profile_slug(robot_id))

    def _save_bringup_artifacts(self, robot_id: str, report_payload: Dict[str, Any], log_lines: List[str]) -> Dict[str, Any]:
        report_dir = self._bringup_report_dir(robot_id)
        os.makedirs(report_dir, exist_ok=True)

        run_ts = int(report_payload.get('run_at_epoch', int(time.time())) or int(time.time()))
        stamp = time.strftime('%Y%m%d_%H%M%S', time.localtime(run_ts))
        report_path = os.path.join(report_dir, f'{stamp}.yaml')
        log_path = os.path.join(report_dir, f'{stamp}.log')

        artifacts = {
            'report_file': self._artifact_relative_path(report_path),
            'log_file': self._artifact_relative_path(log_path),
        }
        report_payload['artifacts'] = dict(artifacts)

        with open(report_path, 'w', encoding='utf-8') as report_file:
            yaml.safe_dump(report_payload, report_file, sort_keys=False)
        with open(log_path, 'w', encoding='utf-8') as log_file:
            for line in log_lines:
                log_file.write(f'{line}\n')

        return artifacts

    def _bringup_tf_step(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        sensor_frames = profile.get('sensor_frames', {}) if isinstance(profile.get('sensor_frames', {}), dict) else {}
        map_frame = self._normalize_frame_name(sensor_frames.get('map', 'map')) or 'map'
        odom_frame = self._normalize_frame_name(sensor_frames.get('odom', 'odom')) or 'odom'
        base_frame = self._normalize_frame_name(profile.get('base_frame', 'base_link')) or 'base_link'

        sensors: List[str] = []
        for key in ('laser', 'camera'):
            frame = self._normalize_frame_name(sensor_frames.get(key, ''))
            if frame:
                sensors.append(frame)
        sensors = sorted(set(sensors))

        tf_tree = self._collect_tf_tree(timeout_sec=1.3)
        observed_frames = set(tf_tree.get('frames', [])) if isinstance(tf_tree.get('frames', []), list) else set()
        pair_transforms = tf_tree.get('transforms', {}) if isinstance(tf_tree.get('transforms', {}), dict) else {}

        map_relative = self._resolve_relative_transforms(map_frame, pair_transforms)
        odom_relative = self._resolve_relative_transforms(odom_frame, pair_transforms)
        base_relative = self._resolve_relative_transforms(base_frame, pair_transforms)

        has_map = map_frame in observed_frames
        has_odom = odom_frame in observed_frames
        has_base = base_frame in observed_frames
        map_to_odom = bool(map_frame == odom_frame or odom_frame in map_relative)
        odom_to_base = bool(odom_frame == base_frame or base_frame in odom_relative)

        sensor_results: List[Dict[str, Any]] = []
        for frame in sensors:
            connected = bool(frame == base_frame or frame in base_relative)
            sensor_results.append({'frame': frame, 'connected': connected})
        sensors_ok = all(entry.get('connected', False) for entry in sensor_results) if sensor_results else True

        ok = bool(has_map and has_odom and has_base and map_to_odom and odom_to_base and sensors_ok)
        issues: List[str] = []
        if not has_map:
            issues.append(f'missing frame "{map_frame}"')
        if not has_odom:
            issues.append(f'missing frame "{odom_frame}"')
        if not has_base:
            issues.append(f'missing frame "{base_frame}"')
        if not map_to_odom:
            issues.append(f'no transform map->odom ({map_frame}->{odom_frame})')
        if not odom_to_base:
            issues.append(f'no transform odom->base ({odom_frame}->{base_frame})')
        for entry in sensor_results:
            if not entry.get('connected', False):
                issues.append(f'no transform base->sensor ({base_frame}->{entry.get("frame", "")})')

        message = (
            f'TF connected for {map_frame}->{odom_frame}->{base_frame} and {len(sensor_results)} sensor frame(s)'
            if ok
            else '; '.join(issues) or 'TF connectivity check failed'
        )

        return {
            'id': 'tf_connectivity',
            'name': 'TF connected',
            'ok': ok,
            'message': message,
            'details': {
                'map_frame': map_frame,
                'odom_frame': odom_frame,
                'base_frame': base_frame,
                'observed_frames': sorted(observed_frames),
                'sensor_checks': sensor_results,
            },
        }

    def _bringup_urdf_step(self, robot_id: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        artifact = profile.get('urdf_artifact', {}) if isinstance(profile.get('urdf_artifact', {}), dict) else {}
        validation = artifact.get('validation', {}) if isinstance(artifact.get('validation', {}), dict) else {}
        compiled_ref = str(artifact.get('compiled_urdf_path', '') or '').strip()
        compiled_abs = self._artifact_absolute_path(compiled_ref) if compiled_ref else ''
        compiled_exists = bool(compiled_abs and os.path.exists(compiled_abs))
        compiled_size = 0
        if compiled_exists:
            try:
                compiled_size = int(os.path.getsize(compiled_abs))
            except Exception:
                compiled_size = 0

        viz = self.get_profile_urdf_visualization(robot_id=robot_id, include_tf=False)
        rendered_shapes = int(viz.get('top_view', {}).get('rendered_shapes', 0) or 0) if isinstance(viz.get('top_view', {}), dict) else 0
        frame_count = int(viz.get('frame_tree', {}).get('counts', {}).get('frames', 0) or 0) if isinstance(viz.get('frame_tree', {}), dict) else 0

        validation_ok = bool(validation.get('valid', False))
        viz_ok = bool(viz.get('ok', False))
        geometry_ok = bool(rendered_shapes > 0 or frame_count > 0)
        ok = bool(validation_ok and compiled_exists and viz_ok and geometry_ok)

        issues: List[str] = []
        if not validation_ok:
            issues.append(str(validation.get('message', 'URDF validation not passed') or 'URDF validation not passed'))
        if not compiled_exists:
            issues.append('compiled URDF artifact missing')
        if not viz_ok:
            issues.append(str(viz.get('message', 'URDF visualization failed') or 'URDF visualization failed'))
        if not geometry_ok:
            issues.append('URDF contains no visible geometry/frame data')

        message = (
            f'URDF loaded and visible ({rendered_shapes} shape(s), {frame_count} frame(s))'
            if ok
            else '; '.join(issues) or 'URDF loaded check failed'
        )

        return {
            'id': 'urdf_loaded',
            'name': 'Robot model visible',
            'ok': ok,
            'message': message,
            'details': {
                'validation_ok': validation_ok,
                'compiled_urdf_path': compiled_ref,
                'compiled_urdf_exists': compiled_exists,
                'compiled_urdf_size_bytes': compiled_size,
                'rendered_shapes': rendered_shapes,
                'frame_count': frame_count,
            },
        }

    def _bringup_localization_step(self, initial_pose: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        seed_requested = isinstance(initial_pose, dict)
        seed_result: Dict[str, Any] = {}
        seed_payload: Dict[str, float] = {}

        if seed_requested:
            try:
                seed_payload = {
                    'x': float(initial_pose.get('x', 0.0)),
                    'y': float(initial_pose.get('y', 0.0)),
                    'theta': float(initial_pose.get('theta', 0.0)),
                }
            except Exception:
                return {
                    'id': 'localization_pose_estimate',
                    'name': 'Localization set',
                    'ok': False,
                    'message': 'Invalid initial pose values',
                    'details': {'initial_pose': initial_pose},
                }

            seed_result = self.set_initial_pose(seed_payload['x'], seed_payload['y'], seed_payload['theta'])
            if not bool(seed_result.get('ok', False)):
                return {
                    'id': 'localization_pose_estimate',
                    'name': 'Localization set',
                    'ok': False,
                    'message': f'2D pose estimate failed: {seed_result.get("message", "unknown error")}',
                    'details': {'initial_pose': seed_payload, 'seed_result': seed_result},
                }

        accepted_sources = {'manual_initialpose', 'amcl', 'slam_toolbox'}
        snapshot = self.get_robot_pose()
        end_time = time.time() + 2.0
        while time.time() < end_time:
            snapshot = self.get_robot_pose()
            source = str(snapshot.get('pose_source', '') or '').strip().lower()
            if bool(snapshot.get('has_pose')) and source in accepted_sources:
                break
            time.sleep(0.1)

        has_pose = bool(snapshot.get('has_pose'))
        source = str(snapshot.get('pose_source', '') or '').strip().lower()
        confidence = float(snapshot.get('confidence', 0.0) or 0.0)
        source_ok = source in accepted_sources
        ok = bool(has_pose and source_ok)

        message = (
            f'Localization source "{source}" with confidence {confidence:.1f}%'
            if ok
            else f'Localization not ready (has_pose={has_pose}, pose_source="{source}")'
        )

        return {
            'id': 'localization_pose_estimate',
            'name': 'Localization set',
            'ok': ok,
            'message': message,
            'details': {
                'initial_pose_requested': seed_requested,
                'initial_pose': seed_payload,
                'seed_result': seed_result,
                'pose_snapshot': snapshot,
            },
        }

    def _bringup_costmap_step(self, robot_id: str) -> Dict[str, Any]:
        diagnostics = self.nav2_diagnostics()
        stack = diagnostics.get('stack', {}) if isinstance(diagnostics.get('stack', {}), dict) else {}
        topics = diagnostics.get('topics', {}) if isinstance(diagnostics.get('topics', {}), dict) else {}

        footprint_result = self.get_profile_nav2_footprint(robot_id=robot_id, include_runtime=True)
        verification = (
            footprint_result.get('verification', {})
            if isinstance(footprint_result.get('verification', {}), dict)
            else {}
        )

        nav_running = bool(stack.get('ok')) and bool(stack.get('nav_running'))
        has_map = bool(topics.get('has_map'))
        has_scan = bool(topics.get('has_scan_input'))
        footprint_ok = bool(verification.get('ok', False))
        ok = bool(nav_running and has_map and has_scan and footprint_ok)

        issues: List[str] = []
        if not nav_running:
            issues.append('NAV stack is not active')
        if not has_map:
            issues.append('map topic missing')
        if not has_scan:
            issues.append('scan input topic missing')
        if not footprint_ok:
            issues.append('footprint runtime verification failed')
        if not bool(footprint_result.get('ok', False)):
            issues.append(str(footprint_result.get('message', 'footprint check failed')))

        message = (
            'Costmaps are updating and footprint runtime matches Nav2'
            if ok
            else '; '.join(issues) or 'Costmap/footprint validation failed'
        )

        return {
            'id': 'costmap_footprint',
            'name': 'Costmaps update + footprint',
            'ok': ok,
            'message': message,
            'details': {
                'stack': stack,
                'topics': topics,
                'footprint_result': footprint_result,
            },
        }

    def _bringup_goal_cancel_step(self) -> Dict[str, Any]:
        if self.estop_active:
            return {
                'id': 'goal_cancel_stop',
                'name': 'Goal send/cancel/stop',
                'ok': False,
                'message': 'E-STOP active. Reset E-STOP before running goal cancel test.',
                'details': {'estop_active': True},
            }

        pose_payload = self.get_robot_pose()
        pose = pose_payload.get('pose') if isinstance(pose_payload.get('pose'), dict) else None
        if not isinstance(pose, dict):
            return {
                'id': 'goal_cancel_stop',
                'name': 'Goal send/cancel/stop',
                'ok': False,
                'message': 'Robot pose unavailable; cannot generate short goal',
                'details': {'pose_snapshot': pose_payload},
            }

        x = float(pose.get('x', 0.0) or 0.0)
        y = float(pose.get('y', 0.0) or 0.0)
        theta = float(pose.get('theta', 0.0) or 0.0)
        dx = math.cos(theta) * 0.25
        dy = math.sin(theta) * 0.25
        short_path = [
            {'x': x + dx, 'y': y + dy},
            {'x': x + (2.0 * dx), 'y': y + (2.0 * dy)},
        ]

        follow_result = self.follow_path(short_path, loop_mode=False)
        if not bool(follow_result.get('ok', False)):
            return {
                'id': 'goal_cancel_stop',
                'name': 'Goal send/cancel/stop',
                'ok': False,
                'message': f'Failed to start short goal: {follow_result.get("message", "unknown error")}',
                'details': {'short_path': short_path, 'follow_result': follow_result},
            }

        time.sleep(0.55)
        cancel_result = self.stop_path()
        time.sleep(0.2)
        path_status = self.get_path_status()
        stopped = bool(cancel_result.get('ok', False)) and (not bool(path_status.get('active', False)))
        ok = bool(stopped)

        message = (
            'Short goal accepted and cancel/stop behavior verified'
            if ok
            else 'Short goal cancel/stop verification failed'
        )

        return {
            'id': 'goal_cancel_stop',
            'name': 'Goal send/cancel/stop',
            'ok': ok,
            'message': message,
            'details': {
                'short_path': short_path,
                'follow_result': follow_result,
                'cancel_result': cancel_result,
                'path_status': path_status,
            },
        }

    def run_profile_bringup_smoke_wizard(
        self,
        *,
        robot_id: str,
        initial_pose: Optional[Dict[str, Any]] = None,
        run_goal_cancel: bool = True,
    ):
        target_robot_id = str(robot_id or '').strip()
        if not target_robot_id:
            return self._ok(False, 'robot_id is required')

        profiles = self._load_robot_profiles()
        profile = profiles.get(target_robot_id)
        if not isinstance(profile, dict):
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        run_ts = int(time.time())
        run_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(run_ts))
        log_lines: List[str] = []
        steps: List[Dict[str, Any]] = []

        self._bringup_log(log_lines, f'Bringup wizard started for profile "{target_robot_id}"')

        tf_step = self._bringup_tf_step(profile)
        steps.append(tf_step)
        self._bringup_log(log_lines, f'Step TF connected: {"PASS" if tf_step["ok"] else "FAIL"} - {tf_step["message"]}')

        urdf_step = self._bringup_urdf_step(target_robot_id, profile)
        steps.append(urdf_step)
        self._bringup_log(log_lines, f'Step Robot model visible: {"PASS" if urdf_step["ok"] else "FAIL"} - {urdf_step["message"]}')

        localization_step = self._bringup_localization_step(initial_pose)
        steps.append(localization_step)
        self._bringup_log(log_lines, f'Step Localization set: {"PASS" if localization_step["ok"] else "FAIL"} - {localization_step["message"]}')

        costmap_step = self._bringup_costmap_step(target_robot_id)
        steps.append(costmap_step)
        self._bringup_log(log_lines, f'Step Costmaps + footprint: {"PASS" if costmap_step["ok"] else "FAIL"} - {costmap_step["message"]}')

        if bool(run_goal_cancel):
            goal_step = self._bringup_goal_cancel_step()
        else:
            goal_step = {
                'id': 'goal_cancel_stop',
                'name': 'Goal send/cancel/stop',
                'ok': False,
                'message': 'Goal cancel test skipped by request',
                'details': {'skipped': True},
            }
        steps.append(goal_step)
        self._bringup_log(log_lines, f'Step Goal send/cancel/stop: {"PASS" if goal_step["ok"] else "FAIL"} - {goal_step["message"]}')

        total_steps = len(steps)
        passed_steps = sum(1 for step in steps if bool(step.get('ok', False)))
        failed_steps = max(0, total_steps - passed_steps)
        all_pass = failed_steps == 0

        final_message = (
            f'Bringup wizard PASS ({passed_steps}/{total_steps})'
            if all_pass
            else f'Bringup wizard FAIL ({passed_steps}/{total_steps})'
        )
        self._bringup_log(log_lines, final_message)

        report_payload: Dict[str, Any] = {
            'schema_version': self.BRINGUP_REPORT_SCHEMA_VERSION,
            'robot_id': target_robot_id,
            'profile_version': int(profile.get('profile_version', 1) or 1),
            'run_at_epoch': run_ts,
            'run_at_iso': run_iso,
            'all_pass': all_pass,
            'summary': {
                'total_steps': total_steps,
                'passed_steps': passed_steps,
                'failed_steps': failed_steps,
            },
            'steps': steps,
            'logs': list(log_lines),
        }

        artifacts: Dict[str, str] = {}
        artifact_error = ''
        try:
            artifacts = self._save_bringup_artifacts(target_robot_id, report_payload, log_lines)
        except Exception as exc:
            artifact_error = str(exc)

        report_payload['artifacts'] = dict(artifacts)

        profile['bringup'] = {
            'last_run_at': run_ts,
            'last_pass': bool(all_pass),
            'last_total_steps': int(total_steps),
            'last_passed_steps': int(passed_steps),
            'last_failed_steps': int(failed_steps),
            'last_summary': final_message,
            'last_report_file': str(artifacts.get('report_file', '') or ''),
            'last_log_file': str(artifacts.get('log_file', '') or ''),
            'last_run_message': final_message,
        }

        save_warning = ''
        try:
            saved_profile = self._save_profile_to_disk(profile, allow_overwrite=True, increment_version=False)
            if str(self.active_robot_profile_id or '').strip() == target_robot_id:
                self._apply_profile_config_to_memory(saved_profile)
                self._save_profile_registry_state(target_robot_id)
                profile = saved_profile
        except Exception as exc:
            save_warning = str(exc)

        if artifact_error:
            final_message = f'{final_message}. Artifact save warning: {artifact_error}'
        if save_warning:
            final_message = f'{final_message}. Profile save warning: {save_warning}'

        return self._ok(
            True,
            final_message,
            robot_id=target_robot_id,
            pass_report=bool(all_pass),
            report=report_payload,
            bringup=copy.deepcopy(profile.get('bringup', {})),
            release=copy.deepcopy(profile.get('release', {})),
            production_allowed=bool(all_pass),
        )

    def get_profile_bringup_report(self, robot_id: str = ''):
        target_robot_id = str(robot_id or '').strip()
        profiles = self._load_robot_profiles()
        if not profiles:
            return self._ok(False, 'No robot profiles available')

        if not target_robot_id:
            target_robot_id = str(self.active_robot_profile_id or '').strip()
        if not target_robot_id or target_robot_id not in profiles:
            target_robot_id = sorted(profiles.keys())[0]

        profile = profiles.get(target_robot_id, {})
        bringup = self._sanitize_profile_bringup(profile.get('bringup', {}))
        release = self._sanitize_profile_release(profile.get('release', {}))

        report_payload: Dict[str, Any] = {}
        has_report = False
        report_file = str(bringup.get('last_report_file', '') or '').strip()
        report_abs = self._artifact_absolute_path(report_file) if report_file else ''
        if report_abs and os.path.exists(report_abs):
            try:
                with open(report_abs, 'r', encoding='utf-8') as report_handle:
                    loaded = yaml.safe_load(report_handle) or {}
                if isinstance(loaded, dict):
                    report_payload = loaded
                    has_report = True
            except Exception as exc:
                return self._ok(
                    False,
                    f'Failed reading bringup report: {exc}',
                    robot_id=target_robot_id,
                    bringup=bringup,
                    release=release,
                )

        message = (
            f'Loaded bringup report for "{target_robot_id}"'
            if has_report
            else f'No bringup report recorded for "{target_robot_id}"'
        )
        return self._ok(
            True,
            message,
            robot_id=target_robot_id,
            profile_version=int(profile.get('profile_version', 1) or 1),
            bringup=bringup,
            release=release,
            has_report=bool(has_report),
            report=report_payload,
            production_allowed=bool(bringup.get('last_pass', False)),
        )

    def set_profile_production_status(self, robot_id: str, production: bool, note: str = ''):
        target_robot_id = str(robot_id or '').strip()
        if not target_robot_id:
            return self._ok(False, 'robot_id is required')

        profiles = self._load_robot_profiles()
        profile = profiles.get(target_robot_id)
        if not isinstance(profile, dict):
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        desired = bool(production)
        bringup = self._sanitize_profile_bringup(profile.get('bringup', {}))
        release = self._sanitize_profile_release(profile.get('release', {}))

        if desired and (not bool(bringup.get('last_pass', False))):
            return self._ok(
                False,
                'Profile cannot be marked production until the bringup wizard reports PASS',
                robot_id=target_robot_id,
                bringup=bringup,
                release=release,
                production_allowed=False,
            )

        release['production'] = desired
        release['production_marked_at'] = int(time.time()) if desired else 0
        release['production_note'] = str(note or '').strip()
        profile['release'] = release

        try:
            saved_profile = self._save_profile_to_disk(profile, allow_overwrite=True, increment_version=True)
        except Exception as exc:
            return self._ok(False, f'Failed updating production status: {exc}')

        if str(self.active_robot_profile_id or '').strip() == target_robot_id:
            self._apply_profile_config_to_memory(saved_profile)
            self._save_profile_registry_state(target_robot_id)

        message = (
            f'Profile "{target_robot_id}" marked as production'
            if desired
            else f'Profile "{target_robot_id}" removed from production'
        )
        return self._ok(
            True,
            message,
            robot_id=target_robot_id,
            profile_version=int(saved_profile.get('profile_version', 1) or 1),
            bringup=self._sanitize_profile_bringup(saved_profile.get('bringup', {})),
            release=self._sanitize_profile_release(saved_profile.get('release', {})),
            profile=copy.deepcopy(saved_profile),
            production_allowed=bool(bringup.get('last_pass', False)),
        )

    def push_profile_config_to_robot(self, robot_id: str):
        requested_robot_id = str(robot_id or '').strip()
        profiles = self._load_robot_profiles()
        if not profiles:
            return self._ok(False, 'No robot profiles available')

        target_robot_id = requested_robot_id or str(self.active_robot_profile_id or '').strip()
        if not target_robot_id or target_robot_id not in profiles:
            target_robot_id = sorted(profiles.keys())[0]

        profile = profiles.get(target_robot_id)
        if not isinstance(profile, dict):
            return self._ok(False, f'Robot profile "{target_robot_id}" not found')

        profile_file = str(self.robot_profile_files.get(target_robot_id, '') or '').strip()
        if not profile_file or not os.path.exists(profile_file):
            candidate_profile_file = self._profile_file_path(target_robot_id)
            if os.path.exists(candidate_profile_file):
                profile_file = candidate_profile_file
        if not profile_file or not os.path.exists(profile_file):
            return self._ok(False, f'Profile file not found for "{target_robot_id}"')

        nav2_params_path = self._resolve_profile_nav2_params_path(profile, target_robot_id)
        if not nav2_params_path:
            return self._ok(False, f'Profile "{target_robot_id}" has no nav2 params file configured')
        if not os.path.exists(nav2_params_path):
            return self._ok(False, f'Nav2 params file does not exist: {nav2_params_path}')

        urdf_artifact = profile.get('urdf_artifact', {}) if isinstance(profile.get('urdf_artifact', {}), dict) else {}
        urdf_validation = urdf_artifact.get('validation', {}) if isinstance(urdf_artifact.get('validation', {}), dict) else {}
        if not bool(urdf_validation.get('valid', False)):
            return self._ok(False, 'URDF valid badge is required before deploy (upload + validate URDF/Xacro first)')

        urdf_loaded = self._load_compiled_urdf_artifact(profile)
        if not bool(urdf_loaded.get('ok', False)):
            return self._ok(False, str(urdf_loaded.get('message', 'Failed loading compiled URDF artifact')))
        compiled_urdf = str(urdf_loaded.get('compiled_urdf', '') or '')
        if not compiled_urdf.strip():
            return self._ok(False, 'Compiled URDF artifact is empty')

        profile_version = max(1, int(profile.get('profile_version', 1) or 1))
        snapshot_id = self._next_deploy_snapshot_id(target_robot_id, profile_version)
        snapshot_dir = self._deploy_snapshot_dir(target_robot_id, snapshot_id)
        os.makedirs(snapshot_dir, exist_ok=False)

        profile_out_path = os.path.join(snapshot_dir, self.DEPLOY_REQUIRED_FILES['profile'])
        nav2_out_path = os.path.join(snapshot_dir, self.DEPLOY_REQUIRED_FILES['nav2_params'])
        urdf_out_path = os.path.join(snapshot_dir, self.DEPLOY_REQUIRED_FILES['compiled_urdf'])

        shutil.copy2(profile_file, profile_out_path)
        shutil.copy2(nav2_params_path, nav2_out_path)
        with open(urdf_out_path, 'w', encoding='utf-8') as urdf_out:
            urdf_out.write(compiled_urdf)

        expected_hashes = {
            'profile': self._sha256_file(profile_out_path),
            'nav2_params': self._sha256_file(nav2_out_path),
            'compiled_urdf': self._sha256_file(urdf_out_path),
        }

        manifest = {
            'schema_version': self.DEPLOY_REGISTRY_SCHEMA_VERSION,
            'snapshot_id': snapshot_id,
            'robot_id': target_robot_id,
            'created_at': int(time.time()),
            'profile_version': profile_version,
            'active_robot_profile_id': str(self.active_robot_profile_id or ''),
            'sources': {
                'profile_file': os.path.abspath(profile_file),
                'nav2_params_file': os.path.abspath(nav2_params_path),
                'compiled_urdf_source_path': str(urdf_loaded.get('compiled_urdf_path', '') or ''),
            },
            'files': {
                'profile': {
                    'filename': self.DEPLOY_REQUIRED_FILES['profile'],
                    'sha256': expected_hashes['profile'],
                },
                'nav2_params': {
                    'filename': self.DEPLOY_REQUIRED_FILES['nav2_params'],
                    'sha256': expected_hashes['nav2_params'],
                },
                'compiled_urdf': {
                    'filename': self.DEPLOY_REQUIRED_FILES['compiled_urdf'],
                    'sha256': expected_hashes['compiled_urdf'],
                },
            },
        }
        manifest_path = os.path.join(snapshot_dir, 'manifest.yaml')
        with open(manifest_path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(manifest, f, sort_keys=False)

        active_dir = self._activate_deploy_snapshot(target_robot_id, snapshot_dir)
        active_hashes = self._collect_hashes_for_dir(active_dir)
        hash_match = self._hashes_match(active_hashes, expected_hashes)
        if not hash_match:
            return self._ok(
                False,
                'Deploy hash verification failed after push',
                robot_id=target_robot_id,
                snapshot_id=snapshot_id,
                expected_hashes=expected_hashes,
                active_hashes=active_hashes,
            )

        registry = self._load_config_deploy_registry()
        robots = registry.get('robots', {}) if isinstance(registry.get('robots', {}), dict) else {}
        record = self._sanitize_deploy_record(robots.get(target_robot_id, {}))
        previous_current = str(record.get('current_snapshot_id', '') or '').strip()

        record['previous_snapshot_id'] = previous_current
        if previous_current and previous_current != snapshot_id:
            record['last_known_good_snapshot_id'] = previous_current
        if not str(record.get('last_known_good_snapshot_id', '') or '').strip():
            record['last_known_good_snapshot_id'] = snapshot_id
        record['current_snapshot_id'] = snapshot_id
        record['updated_at'] = int(time.time())
        robots[target_robot_id] = record
        registry['robots'] = robots
        self._save_config_deploy_registry(registry)

        return self._ok(
            True,
            f'Pushed profile "{target_robot_id}" config to robot deploy folder',
            robot_id=target_robot_id,
            snapshot_id=snapshot_id,
            profile_version=profile_version,
            deploy_root=self.robot_config_deploy_dir,
            active_dir=active_dir,
            snapshot_dir=snapshot_dir,
            manifest_file=manifest_path,
            expected_hashes=expected_hashes,
            active_hashes=active_hashes,
            hash_match=hash_match,
            current_snapshot_id=record.get('current_snapshot_id', ''),
            last_known_good_snapshot_id=record.get('last_known_good_snapshot_id', ''),
            previous_snapshot_id=record.get('previous_snapshot_id', ''),
            recent_snapshots=self._collect_recent_deploy_snapshots(target_robot_id, limit=10),
        )

    def rollback_profile_config_on_robot(self, robot_id: str, snapshot_id: str = ''):
        requested_robot_id = str(robot_id or '').strip()
        if not requested_robot_id:
            requested_robot_id = str(self.active_robot_profile_id or '').strip()
        if not requested_robot_id:
            return self._ok(False, 'robot_id is required')

        registry = self._load_config_deploy_registry()
        robots = registry.get('robots', {}) if isinstance(registry.get('robots', {}), dict) else {}
        record = self._sanitize_deploy_record(robots.get(requested_robot_id, {}))

        target_snapshot_id = str(snapshot_id or '').strip()
        if not target_snapshot_id:
            target_snapshot_id = str(record.get('last_known_good_snapshot_id', '') or '').strip()
        if not target_snapshot_id:
            return self._ok(False, f'No last-known-good snapshot recorded for "{requested_robot_id}"')

        snapshot_dir = self._deploy_snapshot_dir(requested_robot_id, target_snapshot_id)
        if not os.path.isdir(snapshot_dir):
            return self._ok(False, f'Rollback snapshot does not exist: {target_snapshot_id}')

        manifest = self._load_deploy_manifest(requested_robot_id, target_snapshot_id)
        if not manifest:
            return self._ok(False, f'Rollback snapshot manifest is missing or invalid: {target_snapshot_id}')

        expected_hashes = self._manifest_expected_hashes(manifest)
        missing_hash_keys = [key for key in self.DEPLOY_REQUIRED_FILES if key not in expected_hashes]
        if missing_hash_keys:
            return self._ok(False, f'Rollback snapshot manifest missing hashes: {", ".join(missing_hash_keys)}')

        active_dir = self._activate_deploy_snapshot(requested_robot_id, snapshot_dir)
        active_hashes = self._collect_hashes_for_dir(active_dir)
        hash_match = self._hashes_match(active_hashes, expected_hashes)
        if not hash_match:
            return self._ok(
                False,
                f'Rollback hash verification failed for "{target_snapshot_id}"',
                robot_id=requested_robot_id,
                snapshot_id=target_snapshot_id,
                expected_hashes=expected_hashes,
                active_hashes=active_hashes,
            )

        previous_current = str(record.get('current_snapshot_id', '') or '').strip()
        record['previous_snapshot_id'] = previous_current
        record['current_snapshot_id'] = target_snapshot_id
        record['last_known_good_snapshot_id'] = target_snapshot_id
        record['updated_at'] = int(time.time())
        robots[requested_robot_id] = record
        registry['robots'] = robots
        self._save_config_deploy_registry(registry)

        profile_version = 0
        try:
            profile_version = max(0, int(manifest.get('profile_version', 0) or 0))
        except Exception:
            profile_version = 0

        return self._ok(
            True,
            f'Rolled back "{requested_robot_id}" to snapshot "{target_snapshot_id}"',
            robot_id=requested_robot_id,
            snapshot_id=target_snapshot_id,
            profile_version=profile_version,
            deploy_root=self.robot_config_deploy_dir,
            active_dir=active_dir,
            snapshot_dir=snapshot_dir,
            expected_hashes=expected_hashes,
            active_hashes=active_hashes,
            hash_match=hash_match,
            current_snapshot_id=record.get('current_snapshot_id', ''),
            last_known_good_snapshot_id=record.get('last_known_good_snapshot_id', ''),
            previous_snapshot_id=record.get('previous_snapshot_id', ''),
            recent_snapshots=self._collect_recent_deploy_snapshots(requested_robot_id, limit=10),
        )

    def get_profile_config_deploy_status(self, robot_id: str = ''):
        target_robot_id = str(robot_id or '').strip()
        if not target_robot_id:
            target_robot_id = str(self.active_robot_profile_id or '').strip()

        registry = self._load_config_deploy_registry()
        robots = registry.get('robots', {}) if isinstance(registry.get('robots', {}), dict) else {}
        record = self._sanitize_deploy_record(robots.get(target_robot_id, {}))

        current_snapshot_id = str(record.get('current_snapshot_id', '') or '').strip()
        last_known_good_snapshot_id = str(record.get('last_known_good_snapshot_id', '') or '').strip()
        previous_snapshot_id = str(record.get('previous_snapshot_id', '') or '').strip()

        active_dir = self._deploy_active_dir(target_robot_id) if target_robot_id else ''
        active_hashes = self._collect_hashes_for_dir(active_dir) if target_robot_id else {}
        expected_hashes: Dict[str, str] = {}
        profile_version = 0
        manifest_file = ''
        snapshot_dir = ''

        if target_robot_id and current_snapshot_id:
            snapshot_dir = self._deploy_snapshot_dir(target_robot_id, current_snapshot_id)
            manifest_file = os.path.join(snapshot_dir, 'manifest.yaml')
            manifest = self._load_deploy_manifest(target_robot_id, current_snapshot_id)
            expected_hashes = self._manifest_expected_hashes(manifest)
            try:
                profile_version = max(0, int(manifest.get('profile_version', 0) or 0))
            except Exception:
                profile_version = 0

        hash_match = self._hashes_match(active_hashes, expected_hashes) if expected_hashes else False

        message = (
            f'Deploy status for "{target_robot_id}"'
            if target_robot_id
            else 'No robot profile selected for deploy status'
        )
        return self._ok(
            True,
            message,
            robot_id=target_robot_id,
            deploy_root=self.robot_config_deploy_dir,
            active_dir=active_dir,
            current_snapshot_id=current_snapshot_id,
            last_known_good_snapshot_id=last_known_good_snapshot_id,
            previous_snapshot_id=previous_snapshot_id,
            profile_version=profile_version,
            snapshot_dir=snapshot_dir,
            manifest_file=manifest_file,
            expected_hashes=expected_hashes,
            active_hashes=active_hashes,
            hash_match=hash_match,
            recent_snapshots=self._collect_recent_deploy_snapshots(target_robot_id, limit=10) if target_robot_id else [],
        )

    # ---------- settings ----------

    def list_robot_profiles(self):
        profiles = self._load_robot_profiles()
        if not profiles:
            self._ensure_profile_registry_bootstrap()
            profiles = self._load_robot_profiles()

        if not profiles:
            return self._ok(False, 'No robot profiles available', active_robot_id='', profiles=[], profiles_by_id={})

        active_robot_id = str(self.active_robot_profile_id or '').strip()
        if active_robot_id not in profiles:
            state = self._load_profile_registry_state()
            active_robot_id = str(state.get('active_robot_id', '') or '').strip()
        if active_robot_id not in profiles:
            active_robot_id = sorted(profiles.keys())[0]

        active_profile = profiles.get(active_robot_id, {})
        if active_profile:
            self._apply_profile_config_to_memory(active_profile)
            self._save_profile_registry_state(active_robot_id)

        summaries = []
        for robot_id in sorted(profiles.keys()):
            profile = profiles[robot_id]
            release = self._sanitize_profile_release(profile.get('release', {}))
            bringup = self._sanitize_profile_bringup(profile.get('bringup', {}))
            summaries.append(
                {
                    'robot_id': robot_id,
                    'namespace': str(profile.get('namespace', '/') or '/'),
                    'base_frame': str(profile.get('base_frame', 'base_link') or 'base_link'),
                    'map_association': str(profile.get('map_association', {}).get('active_map', '') or ''),
                    'urdf_source': str(profile.get('urdf_source', '') or ''),
                    'nav2_params_file': str(profile.get('nav2_params_file', '') or ''),
                    'profile_version': int(profile.get('profile_version', 1) or 1),
                    'schema_version': int(profile.get('schema_version', self.PROFILE_SCHEMA_VERSION) or self.PROFILE_SCHEMA_VERSION),
                    'urdf_valid': bool(profile.get('urdf_artifact', {}).get('validation', {}).get('valid', False)),
                    'urdf_source_type': str(profile.get('urdf_artifact', {}).get('source_type', '') or ''),
                    'production': bool(release.get('production', False)),
                    'production_marked_at': int(release.get('production_marked_at', 0) or 0),
                    'bringup_last_pass': bool(bringup.get('last_pass', False)),
                    'bringup_last_run_at': int(bringup.get('last_run_at', 0) or 0),
                    'bringup_last_report_file': str(bringup.get('last_report_file', '') or ''),
                }
            )

        return self._ok(
            True,
            f'Loaded {len(summaries)} robot profiles',
            active_robot_id=active_robot_id,
            profiles=summaries,
            profiles_by_id=profiles,
            profiles_dir=self.robot_profiles_dir,
        )

    def _merge_profile_overrides(self, base_profile: Dict[str, Any], overrides: Any) -> Dict[str, Any]:
        merged = copy.deepcopy(base_profile if isinstance(base_profile, dict) else {})
        if not isinstance(overrides, dict):
            return merged

        for key in ('robot_id', 'namespace', 'base_frame', 'urdf_source', 'nav2_params_file'):
            if key in overrides:
                merged[key] = str(overrides.get(key) or '').strip()

        if isinstance(overrides.get('sensor_frames'), dict):
            sensor_frames = self._sanitize_sensor_frames(merged.get('sensor_frames', {}))
            sensor_frames.update(self._sanitize_sensor_frames(overrides.get('sensor_frames', {})))
            merged['sensor_frames'] = sensor_frames

        if isinstance(overrides.get('ui_toggles'), dict):
            ui_toggles = self._sanitize_ui_toggles(merged.get('ui_toggles', {}))
            ui_toggles.update(self._sanitize_ui_toggles(overrides.get('ui_toggles', {})))
            merged['ui_toggles'] = ui_toggles

        map_override = overrides.get('map_association')
        if isinstance(map_override, dict):
            active_map = str(map_override.get('active_map', '') or '').strip()
            merged['map_association'] = {'active_map': self._profile_map_reference(active_map)}

        if isinstance(overrides.get('topics'), dict):
            topics = self._merge_topics_with_defaults(merged.get('topics', {}))
            topics.update(self._sanitize_topics(overrides.get('topics', {})))
            merged['topics'] = self._merge_topics_with_defaults(topics)

        if isinstance(overrides.get('mappings'), dict):
            current = self._sanitize_mappings(merged.get('mappings', {}))
            updates = self._sanitize_mappings(overrides.get('mappings', {}))
            for key, value in updates.items():
                entry = dict(current.get(key, {}))
                entry.update(value)
                current[key] = entry
            merged['mappings'] = self._merge_mappings_with_defaults(current)

        return merged

    def select_robot_profile(self, robot_id: str, apply_map: bool = True):
        chosen = str(robot_id or '').strip()
        if not chosen:
            return self._ok(False, 'robot_id is required')

        profiles = self._load_robot_profiles()
        profile = profiles.get(chosen)
        if not profile:
            return self._ok(False, f'Robot profile "{chosen}" not found')

        self._apply_profile_config_to_memory(profile)
        self._save_profile_registry_state(chosen)

        settings_sync = self.save_ui_mappings(
            {
                'mappings': self.cached_settings_mappings,
                'topics': self.topic_config,
            },
            confirmed=True,
            sync_active_profile=False,
        )

        runtime_result: Optional[Dict[str, Any]] = None
        urdf_artifact = profile.get('urdf_artifact', {}) if isinstance(profile.get('urdf_artifact', {}), dict) else {}
        compiled_urdf_path = str(urdf_artifact.get('compiled_urdf_path', '') or '').strip()
        validation_ok = bool(urdf_artifact.get('validation', {}).get('valid', False)) if isinstance(urdf_artifact.get('validation', {}), dict) else False
        if compiled_urdf_path and validation_ok:
            runtime_result = self._start_managed_robot_state_publisher(
                profile=profile,
                compiled_urdf_path=compiled_urdf_path,
            )
            profile['urdf_artifact']['runtime'] = {
                'ok': bool(runtime_result.get('ok')),
                'message': str(runtime_result.get('message', '') or ''),
                'node_name': str(runtime_result.get('node_name', '') or ''),
                'namespace': str(runtime_result.get('namespace', '') or ''),
                'required_tf_frames': list(runtime_result.get('required_tf_frames', [])),
                'observed_tf_frames': list(runtime_result.get('observed_tf_frames', [])),
                'missing_tf_frames': list(runtime_result.get('missing_tf_frames', [])),
                'pid': int(runtime_result.get('pid', 0) or 0),
            }
            try:
                saved_profile = self._save_profile_to_disk(profile, allow_overwrite=True, increment_version=False)
                self._apply_profile_config_to_memory(saved_profile)
                profile = saved_profile
            except Exception as exc:
                self.get_logger().warn(f'Failed to persist robot_state_publisher runtime metadata: {exc}')
        else:
            self._stop_managed_robot_state_publisher()

        map_result: Optional[Dict[str, Any]] = None
        active_map = str(profile.get('map_association', {}).get('active_map', '') or '').strip()
        if apply_map and active_map:
            resolved_map = self._resolve_profile_map_path(active_map)
            map_result = self.set_active_map(resolved_map)
        elif apply_map:
            map_result = self._ok(True, 'Profile has no active map association')

        notes: List[str] = []
        if settings_sync and not bool(settings_sync.get('ok')):
            notes.append(f'Settings sync warning: {settings_sync.get("message", "unknown error")}')
        elif settings_sync and bool(settings_sync.get('requires_restart')):
            notes.append('Endpoint topic rebinding requires restarting backend nodes.')
        if runtime_result and not bool(runtime_result.get('ok')):
            notes.append(f'robot_state_publisher warning: {runtime_result.get("message", "runtime check failed")}')
        if map_result and not bool(map_result.get('ok')):
            notes.append(f'Map association warning: {map_result.get("message", "unknown error")}')

        runtime_profile = self.active_robot_profile if isinstance(self.active_robot_profile, dict) else profile
        payload = self._build_profile_runtime_payload(
            runtime_profile,
            map_result=map_result,
            settings_sync=settings_sync,
        )
        payload['urdf_runtime'] = runtime_result or {}

        message = f'Applied robot profile "{chosen}"'
        if notes:
            message = f'{message}. {" ".join(notes)}'

        return self._ok(True, message, **payload)

    def clone_robot_profile(self, source_robot_id: str, overrides: Any, activate: bool = True):
        source_id = str(source_robot_id or '').strip()
        if not source_id:
            return self._ok(False, 'source_robot_id is required')

        profiles = self._load_robot_profiles()
        source_profile = profiles.get(source_id)
        if not source_profile:
            return self._ok(False, f'Source robot profile "{source_id}" not found')

        merged = self._merge_profile_overrides(source_profile, overrides)
        candidate = self._sanitize_profile_payload(merged, fallback_robot_id='')
        target_robot_id = str(candidate.get('robot_id', '') or '').strip()
        if not target_robot_id:
            return self._ok(False, 'robot_id is required in overrides')
        if target_robot_id == source_id:
            return self._ok(False, 'Clone requires a new robot_id')
        if target_robot_id in profiles:
            return self._ok(False, f'Robot profile "{target_robot_id}" already exists')

        candidate['profile_version'] = 1
        candidate['urdf_artifact'] = {}
        candidate['release'] = self._sanitize_profile_release({})
        candidate['bringup'] = self._sanitize_profile_bringup({})
        active_map = str(candidate.get('map_association', {}).get('active_map', '') or '').strip()
        if active_map:
            candidate['map_association'] = {'active_map': self._profile_map_reference(active_map)}

        try:
            saved_profile = self._save_profile_to_disk(candidate, allow_overwrite=False)
        except Exception as exc:
            return self._ok(False, f'Failed to clone robot profile: {exc}')

        if activate:
            applied = self.select_robot_profile(target_robot_id, apply_map=True)
            if bool(applied.get('ok')):
                applied['message'] = f'Cloned "{source_id}" to "{target_robot_id}" and applied'
            return applied

        return self._ok(
            True,
            f'Cloned "{source_id}" to "{target_robot_id}"',
            source_robot_id=source_id,
            target_robot_id=target_robot_id,
            profile=saved_profile,
            profiles_dir=self.robot_profiles_dir,
        )

    def get_ui_mappings(self):
        req = GetUiMappings.Request()
        response, err = self._call_service(self.get_ui_mappings_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(
                False,
                'SettingsManager get_mappings service not available',
                mappings=dict(self.cached_settings_mappings),
                topics=dict(self.topic_config),
                settings_file=self.settings_file,
                active_robot_profile_id=str(self.active_robot_profile_id or ''),
                active_robot_profile=copy.deepcopy(self.active_robot_profile) if isinstance(self.active_robot_profile, dict) else {},
            )
        if err == 'timeout':
            return self._ok(
                False,
                'SettingsManager get_mappings timed out',
                mappings=dict(self.cached_settings_mappings),
                topics=dict(self.topic_config),
                settings_file=self.settings_file,
                active_robot_profile_id=str(self.active_robot_profile_id or ''),
                active_robot_profile=copy.deepcopy(self.active_robot_profile) if isinstance(self.active_robot_profile, dict) else {},
            )
        if response is None:
            return self._ok(
                False,
                'Get settings mappings failed',
                mappings=dict(self.cached_settings_mappings),
                topics=dict(self.topic_config),
                settings_file=self.settings_file,
                active_robot_profile_id=str(self.active_robot_profile_id or ''),
                active_robot_profile=copy.deepcopy(self.active_robot_profile) if isinstance(self.active_robot_profile, dict) else {},
            )

        parsed: Dict[str, Any] = {}
        try:
            loaded = json.loads(str(response.mappings_json or '{}'))
            if isinstance(loaded, dict):
                parsed = loaded
        except Exception:
            parsed = {}

        if 'mappings' in parsed or 'topics' in parsed:
            mappings_raw = parsed.get('mappings', {})
            topics_raw = parsed.get('topics', {})
        else:
            mappings_raw = parsed
            topics_raw = {}

        mappings = self._merge_mappings_with_defaults(self._sanitize_mappings(mappings_raw))
        topics = self._merge_topics_with_defaults(topics_raw)

        active_profile = self.active_robot_profile if isinstance(self.active_robot_profile, dict) else {}
        if active_profile:
            mappings = self._merge_mappings_with_defaults(self._sanitize_mappings(active_profile.get('mappings', {})))
            topics = self._merge_topics_with_defaults(active_profile.get('topics', {}))

        self.cached_settings_mappings = dict(mappings)
        self.topic_config = dict(topics)

        return self._ok(
            bool(response.ok),
            str(response.message),
            mappings=mappings,
            topics=topics,
            settings_file=self.settings_file,
            active_robot_profile_id=str(self.active_robot_profile_id or ''),
            active_robot_profile=copy.deepcopy(self.active_robot_profile) if isinstance(self.active_robot_profile, dict) else {},
        )

    def save_ui_mappings(self, mappings: Any, confirmed: bool = True, *, sync_active_profile: bool = True):
        payload = mappings if isinstance(mappings, dict) else {}
        if 'mappings' in payload or 'topics' in payload:
            mappings_payload = payload.get('mappings', {})
            topics_payload = payload.get('topics', {})
        else:
            mappings_payload = payload
            topics_payload = {}

        sanitized_mappings = self._sanitize_mappings(mappings_payload)
        sanitized_topics = self._sanitize_topics(topics_payload)
        merged_mappings = self._merge_mappings_with_defaults(sanitized_mappings)
        merged_topics = self._merge_topics_with_defaults(sanitized_topics)

        req = SaveUiMappings.Request()
        req.mappings_json = json.dumps(
            {
                'mappings': sanitized_mappings,
                'topics': sanitized_topics,
            }
        )
        req.confirmed = bool(confirmed)

        response, err = self._call_service(self.save_ui_mappings_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'SettingsManager save_mappings service not available', require_confirmation=False)
        if err == 'timeout':
            return self._ok(False, 'SettingsManager save_mappings timed out', require_confirmation=False)
        if response is None:
            return self._ok(False, 'Save settings mappings failed', require_confirmation=False)

        if bool(response.ok):
            self.cached_settings_mappings = merged_mappings
            self.topic_config = merged_topics
            if sync_active_profile:
                self._sync_active_profile_from_mappings(merged_mappings, merged_topics)

        return self._ok(
            bool(response.ok),
            str(response.message),
            require_confirmation=bool(response.require_confirmation),
            requires_restart=True,
        )
    # ---------- safety ----------

    def set_estop(self, enabled: bool):
        return self._call_setbool(
            self.emergency_stop_client,
            enabled,
            unavailable_msg='Emergency stop service not available',
            timeout_msg='Timeout waiting for emergency stop service',
        )

    def set_safety_override(self, enabled: bool):
        return self._call_setbool(
            self.safety_override_client,
            enabled,
            unavailable_msg='Safety override service not available',
            timeout_msg='Timeout waiting for safety override service',
        )

    def get_safety_status(self):
        req = Trigger.Request()
        response, err = self._call_service(self.safety_status_client, req, wait_timeout=1.0, response_timeout=3.0)
        if err == 'service_not_available':
            return self._ok(False, 'Safety status service not available')
        if err == 'timeout':
            return self._ok(False, 'Safety status timed out')
        if response is None:
            return self._ok(False, 'Safety status failed')

        msg = str(response.message)
        lower = msg.lower()
        low_conf = 'low confidence stop: active' in lower
        obstacle = 'obstacle stop: active' in lower
        override = 'manual override: enabled' in lower
        estop_text = 'e-stop: active' in lower
        estop = bool(self.estop_active) or estop_text
        stop_reason = ''
        for raw_line in msg.splitlines():
            line = str(raw_line or '').strip()
            if line.lower().startswith('stop reason:'):
                stop_reason = line.split(':', 1)[1].strip()
                break

        normalized_reason = str(stop_reason or '').strip()
        normalized_reason_lower = normalized_reason.lower()
        reason_ignored = normalized_reason_lower in {
            '',
            'none',
            'cleared',
            'manual_clear_state',
            'distance_safety_disabled',
        }
        if estop:
            display_reason = 'E-STOP active'
        elif low_conf:
            display_reason = 'Low localization confidence'
        elif obstacle:
            display_reason = (
                f'Obstacle: {normalized_reason}'
                if not reason_ignored
                else 'Obstacle detected'
            )
        else:
            display_reason = ''

        return {
            'ok': bool(response.success),
            'message': msg,
            'active': bool(low_conf or obstacle or estop),
            'low_confidence_stop': bool(low_conf),
            'obstacle_stop': bool(obstacle),
            'manual_override': bool(override),
            'estop_active': bool(estop),
            'stop_reason': normalized_reason,
            'display_reason': display_reason,
        }


    def clear_safety_state(self):
        req = Trigger.Request()
        response, err = self._call_service(self.safety_clear_state_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'Safety clear_state service not available')
        if err == 'timeout':
            return self._ok(False, 'Safety clear_state timed out')
        if response is None:
            return self._ok(False, 'Safety clear_state failed')

        if bool(response.success):
            self.estop_active = False
        return self._ok(bool(response.success), str(response.message))

    def clear_system_state(self):
        # Operator recovery action: clear safety latches and force stack STOP.
        details = {}

        # Stop active mission/path activities first.
        details['path'] = self.stop_path()
        details['sequence'] = self.stop_sequence()
        details['mission'] = self.clear_mission_state()

        # Clear safety state using canonical safety controller endpoint.
        safety_clear = self.clear_safety_state()
        if safety_clear.get('ok'):
            details['safety'] = safety_clear
        else:
            # Fallback for older deployments without safety/clear_state.
            # Keep override OFF to avoid latching bypass unexpectedly.
            safety_override = self.set_safety_override(False)
            estop_reset = self.set_estop(False)
            fallback_ok = bool(safety_override.get('ok')) and bool(estop_reset.get('ok'))
            msg = (
                'Safety clear_state unavailable; fallback applied '
                f'(override_disabled={safety_override.get("ok")}, estop_reset={estop_reset.get("ok")})'
            )
            details['safety'] = self._ok(fallback_ok, msg, override=safety_override, estop=estop_reset)
            if fallback_ok:
                self.estop_active = False

        # Force stack STOP regardless of current mode.
        details['stack'] = self.set_stack_mode('stop')

        # Treat safety clear as the primary success criterion for E-STOP reset flows.
        # Stack STOP is still attempted and reported, but should not re-latch UI E-STOP on partial failures.
        ok = bool(details['safety'].get('ok'))

        summary_parts = []
        if details['stack'].get('ok'):
            summary_parts.append('stack=STOP')
        else:
            summary_parts.append(f"stack_error={details['stack'].get('message', 'unknown')}")

        if details['safety'].get('ok'):
            summary_parts.append('safety=cleared')
        else:
            summary_parts.append(f"safety_error={details['safety'].get('message', 'unknown')}")

        summary_parts.append('path=stopped' if details['path'].get('ok') else 'path_stop_failed')
        summary_parts.append('sequence=stopped' if details['sequence'].get('ok') else 'sequence_stop_failed')

        return self._ok(ok, '; '.join(summary_parts), details=details)

    # ---------- mode ----------

    def set_control_mode(self, mode: str):
        normalized = self._normalize_mode(mode)
        if normalized not in self.VALID_MODES:
            return self._ok(False, f'Invalid mode. Use: {", ".join(sorted(self.VALID_MODES))}')

        req = SetControlMode.Request()
        req.mode = normalized
        response, err = self._call_service(
            self.set_control_mode_client,
            req,
            wait_timeout=1.0,
            response_timeout=3.0,
        )
        if err == 'service_not_available':
            return self._ok(False, 'SetControlMode service not available')
        if err == 'timeout':
            return self._ok(False, 'Timeout waiting for SetControlMode response')
        if response is None:
            return self._ok(False, 'SetControlMode call failed')

        if bool(response.ok):
            self.current_mode = normalized

        return self._ok(bool(response.ok), str(response.message), mode=normalized)

    # ---------- velocity / pose ----------

    def publish_manual_velocity(self, linear: float, angular: float):
        if self.estop_active:
            return self._ok(False, 'E-STOP is active - manual control disabled')

        msg = Twist()
        msg.linear.x = float(linear)
        msg.angular.z = float(angular)
        self.cmd_vel_manual_pub.publish(msg)
        
        return self._ok(
            True,
            'Manual velocity published',
            linear=float(linear),
            angular=float(angular),
        )

    def set_initial_pose(self, x: float, y: float, theta: float):
        x = float(x)
        y = float(y)
        theta = float(theta)

        msg = PoseWithCovarianceStamped()
        msg.header.frame_id = self._map_frame()
        # Use zero timestamp so AMCL resolves pose against latest available TF
        # and avoids "extrapolation into future/past" failures when clocks drift.
        msg.header.stamp.sec = 0
        msg.header.stamp.nanosec = 0
        msg.pose.pose.position.x = x
        msg.pose.pose.position.y = y
        msg.pose.pose.position.z = 0.0
        msg.pose.pose.orientation.x = 0.0
        msg.pose.pose.orientation.y = 0.0
        msg.pose.pose.orientation.z = math.sin(theta / 2.0)
        msg.pose.pose.orientation.w = math.cos(theta / 2.0)
        msg.pose.covariance = [
            0.25, 0.0, 0.0, 0.0, 0.0, 0.0,
            0.0, 0.25, 0.0, 0.0, 0.0, 0.0,
            0.0, 0.0, 99999.0, 0.0, 0.0, 0.0,
            0.0, 0.0, 0.0, 99999.0, 0.0, 0.0,
            0.0, 0.0, 0.0, 0.0, 99999.0, 0.0,
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0685,
        ]

        publish_count = max(1, int(self.manual_pose_publish_count))
        publish_interval = max(0.0, float(self.manual_pose_publish_interval_sec))

        self.get_logger().info(
            f'Initial pose request: x={x:.3f}, y={y:.3f}, yaw={theta:.3f} rad '
            f'(publishes={publish_count}, lock={self.manual_pose_lock_sec:.1f}s)'
        )

        for idx in range(publish_count):
            self.initial_pose_pub.publish(msg)
            if (idx + 1) < publish_count and publish_interval > 0.0:
                time.sleep(publish_interval)

        self.manual_pose_lock_until = time.time() + max(0.0, float(self.manual_pose_lock_sec))

        self._record_requested_pose(x, y, theta, 'manual_initialpose')
        # Reflect immediately in UI while AMCL/SLAM converges on the published pose.
        self._apply_robot_pose(x, y, theta, 'manual_initialpose', force=True)

        return self._ok(
            True,
            f'Robot pose set to ({x:.2f}, {y:.2f}); holding manual pose for {self.manual_pose_lock_sec:.1f}s',
        )

    def get_robot_pose(self):
        pose_age = None
        if self.last_pose_time is not None:
            pose_age = max(0.0, time.time() - self.last_pose_time)
        requested_pose_age = None
        if self.last_requested_pose_time is not None:
            requested_pose_age = max(0.0, time.time() - self.last_requested_pose_time)

        live_pose = None
        live_pose_source = None
        tf_pose = self._lookup_pose_from_tf()
        if tf_pose is not None:
            live_pose = {
                'x': float(tf_pose[0]),
                'y': float(tf_pose[1]),
                'theta': float(tf_pose[2]),
            }
            live_pose_source = self.robot_pose_source or 'tf'
            pose_age = 0.0
        elif (
            isinstance(self.robot_pose, dict)
            and self.last_pose_time is not None
            and pose_age is not None
            and pose_age <= float(self.pose_cache_hold_sec)
        ):
            live_pose = {
                'x': float(self.robot_pose.get('x', 0.0)),
                'y': float(self.robot_pose.get('y', 0.0)),
                'theta': float(self.robot_pose.get('theta', 0.0)),
            }
            source = str(self.robot_pose_source or 'cached').strip()
            live_pose_source = f'{source}_cached' if source else 'cached'

        return {
            'pose': live_pose,
            'requested_pose': self.last_requested_pose,
            'requested_pose_source': self.last_requested_pose_source,
            'requested_pose_age_sec': requested_pose_age,
            'pose_frame': self._map_frame(),
            'confidence': float(self.localization_confidence),
            'covariance_confidence': float(self.covariance_confidence),
            'scan_map_match_confidence': float(self.scan_map_match_confidence),
            'scan_map_known_ratio': float(self.scan_map_match_known_ratio),
            'scan_map_hit_ratio': float(self.scan_map_match_hit_ratio),
            'scan_map_mean_dist_m': float(self.scan_map_match_mean_dist_m),
            'scan_map_mean_likelihood': float(self.scan_map_match_mean_likelihood),
            'safety_stop': bool(self.estop_active),
            'override_active': False,
            'control_mode': str(self.current_mode or 'unknown'),
            'has_pose': live_pose is not None,
            'pose_source': live_pose_source,
            'pose_age_sec': pose_age,
        }

    # ---------- stack ----------

    def set_stack_mode(self, mode: str):
        req = SetStackMode.Request()
        req.mode = str(mode or '').strip().lower()

        response, err = self._call_service(self.set_stack_mode_client, req, wait_timeout=4.0, response_timeout=60.0)
        if err == 'service_not_available':
            return self._ok(False, 'StackManager service not available')
        if err == 'timeout':
            return self._ok(False, 'Timed out waiting for stack switch')
        if response is None:
            return self._ok(False, 'Stack mode request failed')

        return self._ok(bool(response.ok), str(response.message), mode=req.mode)

    def get_stack_status(self):
        req = Trigger.Request()
        response, err = self._call_service(self.stack_status_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'StackManager status service not available')
        if err == 'timeout':
            return self._ok(False, 'StackManager status timed out')
        if response is None:
            return self._ok(False, 'Stack status request failed')

        if not bool(response.success):
            return self._ok(False, str(response.message))

        payload = {}
        try:
            payload = json.loads(str(response.message) or '{}')
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        payload.setdefault('mode', 'unknown')
        payload.setdefault('nav_running', False)
        payload.setdefault('slam_running', False)
        payload.setdefault('nav_ready', False)
        payload.setdefault('nav_lifecycle_state', 'inactive')
        payload.setdefault('slam_ready', False)
        payload.setdefault('owner_pid', 0)
        payload.setdefault('startup_mode', '')

        return self._ok(True, 'Stack status retrieved', **payload)

    def shutdown_stack(self):
        req = Trigger.Request()
        response, err = self._call_service(self.stack_shutdown_client, req, wait_timeout=1.0, response_timeout=30.0)
        if err == 'service_not_available':
            return self._ok(False, 'StackManager shutdown service not available')
        if err == 'timeout':
            return self._ok(False, 'StackManager shutdown timed out')
        if response is None:
            return self._ok(False, 'Stack shutdown request failed')
        return self._ok(bool(response.success), str(response.message))

    # ---------- map manager ----------

    @staticmethod
    def _load_map_result_name(code: int) -> str:
        names = {
            int(LoadMap.Response.RESULT_SUCCESS): 'success',
            int(LoadMap.Response.RESULT_MAP_DOES_NOT_EXIST): 'map_does_not_exist',
            int(LoadMap.Response.RESULT_INVALID_MAP_DATA): 'invalid_map_data',
            int(LoadMap.Response.RESULT_INVALID_MAP_METADATA): 'invalid_map_metadata',
            int(LoadMap.Response.RESULT_UNDEFINED_FAILURE): 'undefined_failure',
        }
        return names.get(int(code), f'code_{int(code)}')

    def _clear_nav2_costmaps(self):
        """Best-effort clear to flush stale obstacle/static caches after map swap."""
        notes = []
        cleared = 0
        clear_req = ClearEntireCostmap.Request()

        for label, client in (
            ('global', self.clear_global_costmap_client),
            ('local', self.clear_local_costmap_client),
        ):
            _resp, err = self._call_service(client, clear_req, wait_timeout=0.8, response_timeout=3.0)
            if err:
                notes.append(f'{label}_clear={err}')
                continue
            cleared += 1

        if not notes:
            return True, f'Cleared {cleared}/2 costmaps'
        return (cleared > 0), f'Cleared {cleared}/2 costmaps ({", ".join(notes)})'

    def _reload_nav2_map(self, map_yaml_path: str):
        """Reload running map_server without requiring full Nav2 restart."""
        map_path = str(map_yaml_path or '').strip()
        if not map_path:
            return False, 'empty map path'

        req = LoadMap.Request()
        req.map_url = map_path

        response, err = self._call_service(
            self.map_server_load_map_client,
            req,
            wait_timeout=1.2,
            response_timeout=8.0,
        )
        if err == 'service_not_available':
            return False, 'map_server/load_map unavailable (NAV likely not running)'
        if err == 'timeout':
            return False, 'timeout waiting for map_server/load_map'
        if response is None:
            return False, 'map_server/load_map failed'

        code = int(response.result)
        if code != int(LoadMap.Response.RESULT_SUCCESS):
            return False, f'load_map failed: {self._load_map_result_name(code)}'

        _cleared_ok, clear_msg = self._clear_nav2_costmaps()
        return True, f'load_map applied from {map_path}; {clear_msg}'

    def upload_map(self, map_name: str, yaml_content: str, pgm_b64: str):
        req = UploadMap.Request()
        req.map_name = str(map_name or '')
        req.yaml_content = str(yaml_content or '')
        req.pgm_base64 = str(pgm_b64 or '')

        response, err = self._call_service(self.upload_map_client, req, wait_timeout=2.0, response_timeout=12.0)
        if err == 'service_not_available':
            return self._ok(False, 'Map upload service not available')
        if err == 'timeout':
            return self._ok(False, 'Map upload timed out')
        if response is None:
            return self._ok(False, 'Map upload failed')

        return self._ok(
            bool(response.success),
            str(response.message),
            success=bool(response.success),
            map_path=str(response.map_path),
        )

    def set_active_map(self, map_yaml_path: str):
        req = SetActiveMap.Request()
        req.map_yaml_path = str(map_yaml_path or '')

        response, err = self._call_service(self.set_active_map_client, req, wait_timeout=2.0, response_timeout=10.0)
        if err == 'service_not_available':
            return self._ok(False, 'Set active map service not available')
        if err == 'timeout':
            return self._ok(False, 'Set active map timed out')
        if response is None:
            return self._ok(False, 'Set active map failed')

        ok = bool(response.success)
        msg = str(response.message)
        resolved_map_path = _canonicalize_map_yaml_path(str(map_yaml_path or '').strip(), self.ui_root)
        live_reload = False
        live_message = ''

        # Keep map activation successful even if live reload cannot run (e.g. NAV stopped).
        if ok:
            active = self.get_active_map()
            if bool(active.get('ok')) and active.get('map_yaml_path'):
                resolved_map_path = _canonicalize_map_yaml_path(str(active.get('map_yaml_path')), self.ui_root)

            live_reload, live_message = self._reload_nav2_map(resolved_map_path)
            if live_reload:
                msg = msg.replace('⚠️ Restart Nav2 to apply changes.', '').strip()
                msg = f'{msg} Live Nav2 map reload applied.'
            else:
                msg = f'{msg} Live Nav2 map reload skipped: {live_message}'

        return self._ok(
            ok,
            msg,
            success=ok,
            map_yaml_path=resolved_map_path,
            live_reload=live_reload,
            live_message=live_message,
        )

    def get_active_map(self):
        req = GetActiveMap.Request()
        response, err = self._call_service(self.get_active_map_client, req, wait_timeout=1.0, response_timeout=4.0)
        if err == 'service_not_available':
            return self._ok(False, 'Get active map service not available')
        if err == 'timeout':
            return self._ok(False, 'Get active map timed out')
        if response is None:
            return self._ok(False, 'Get active map failed')

        map_yaml_path = _canonicalize_map_yaml_path(str(response.map_yaml_path), self.ui_root)
        return self._ok(
            bool(response.success),
            str(response.message),
            success=bool(response.success),
            map_yaml_path=map_yaml_path,
            map_path=map_yaml_path,
        )

    def list_maps(self):
        maps = []
        seen = set()

        for base_dir in (self.maps_dir, self.downloads_dir):
            if not os.path.isdir(base_dir):
                continue
            for filename in os.listdir(base_dir):
                if not filename.endswith('.yaml'):
                    continue
                full = os.path.join(base_dir, filename)
                stem = os.path.splitext(filename)[0]
                pgm = os.path.join(base_dir, f'{stem}.pgm')
                key = os.path.abspath(full)
                if key in seen:
                    continue
                seen.add(key)
                maps.append(
                    {
                        'name': stem,
                        'yaml_file': filename,
                        'yaml_path': full,
                        'pgm_file': os.path.basename(pgm) if os.path.exists(pgm) else None,
                        'location': 'downloads' if 'DownloadedMaps' in full else 'workspace',
                    }
                )

        maps.sort(key=lambda item: item['name'])
        return self._ok(True, f'Found {len(maps)} maps', success=True, maps=maps)

    def reload_active_map(self):
        active = self.get_active_map()
        if not bool(active.get('ok')):
            return active

        map_yaml_path = str(active.get('map_yaml_path') or '').strip()
        if not map_yaml_path:
            return self._ok(False, 'No active map path available')

        live_reload, live_message = self._reload_nav2_map(map_yaml_path)
        if live_reload:
            return self._ok(
                True,
                f'Active map reloaded in Nav2: {os.path.basename(map_yaml_path)}',
                map_yaml_path=map_yaml_path,
                live_reload=True,
                live_message=live_message,
            )

        return self._ok(
            False,
            f'Failed to reload active map in Nav2: {live_message}',
            map_yaml_path=map_yaml_path,
            live_reload=False,
            live_message=live_message,
        )

    def save_slam_map(self, map_name: str, set_active: bool = False):
        clean_name = (map_name or '').strip()
        if not clean_name:
            clean_name = time.strftime('slam_map_%Y%m%d_%H%M%S')

        if '/' in clean_name or '\\' in clean_name:
            return self._ok(False, 'Invalid map name')

        map_base = os.path.join(self.maps_dir, clean_name)
        req = SlamSaveMap.Request()
        req.name.data = map_base

        response, err = self._call_service(self.slam_save_map_client, req, wait_timeout=2.0, response_timeout=12.0)
        if err == 'service_not_available':
            return self._ok(False, 'slam_toolbox save_map service not available')
        if err == 'timeout':
            return self._ok(False, 'Timeout waiting for slam_toolbox save_map')
        if response is None:
            return self._ok(False, 'slam_toolbox save_map failed')

        if response.result != SlamSaveMap.Response.RESULT_SUCCESS:
            return self._ok(False, f'SLAM map save failed (code {response.result})')

        map_yaml_path = f'{map_base}.yaml'
        payload = {
            'ok': True,
            'message': f'Map saved: {clean_name}',
            'map_name': clean_name,
            'map_yaml_path': map_yaml_path,
            'map_pgm_path': f'{map_base}.pgm',
            'set_active': bool(set_active),
        }

        dotted_result = ensure_dotted_truth_map(
            map_yaml_path,
            workspace_root=self.ui_root,
            logger=self.get_logger(),
        )
        if not bool(dotted_result.get('ok')):
            return self._ok(
                False,
                str(dotted_result.get('message', 'Failed to prepare dotted truth map')),
                map_name=clean_name,
                map_yaml_path=map_yaml_path,
                map_pgm_path=f'{map_base}.pgm',
                set_active=bool(set_active),
            )

        payload['map_pgm_path'] = str(dotted_result.get('dotted_image_path') or payload['map_pgm_path'])

        if set_active:
            active_result = self.set_active_map(map_yaml_path)
            if active_result.get('ok'):
                payload['message'] = f"{payload['message']}. {active_result.get('message', '')}".strip()
            else:
                payload['message'] = f"{payload['message']}. Failed to set active map: {active_result.get('message', '')}".strip()

        return payload

    def serialize_pose_graph(self, filename: str):
        """Serialize the current slam_toolbox pose graph to disk.

        filename: path without extension — slam_toolbox appends .posegraph automatically.
        If empty, defaults to maps_dir/slam_pg_<timestamp>.
        """
        clean = (filename or '').strip()
        if not clean:
            clean = os.path.join(self.maps_dir, time.strftime('slam_pg_%Y%m%d_%H%M%S'))
        # Strip .posegraph extension if caller provided it — service adds it
        if clean.endswith('.posegraph'):
            clean = clean[:-len('.posegraph')]
        req = SlamSerializePoseGraph.Request()
        req.filename = clean
        response, err = self._call_service(
            self.slam_serialize_client, req, wait_timeout=2.0, response_timeout=15.0)
        if err == 'service_not_available':
            return self._ok(False, 'slam_toolbox serialize_map service not available (is SLAM running?)')
        if err == 'timeout':
            return self._ok(False, 'Timeout waiting for slam_toolbox serialize_map')
        if response is None:
            return self._ok(False, 'slam_toolbox serialize_map failed')
        return self._ok(True, f'Pose graph saved to {clean}.posegraph',
                        posegraph_path=f'{clean}.posegraph')

    def deserialize_pose_graph(self, filename: str):
        """Deserialize an existing slam_toolbox pose graph from disk.

        filename: path to the .posegraph file (with or without extension).
        """
        clean = (filename or '').strip()
        if not clean:
            return self._ok(False, 'filename is required for deserialize')
        # Ensure .posegraph extension is stripped — service may add it itself
        if clean.endswith('.posegraph'):
            clean = clean[:-len('.posegraph')]
        req = SlamDeserializePoseGraph.Request()
        req.filename = clean
        req.match_type = SlamDeserializePoseGraph.Request.START_AT_FIRST_NODE
        response, err = self._call_service(
            self.slam_deserialize_client, req, wait_timeout=2.0, response_timeout=20.0)
        if err == 'service_not_available':
            return self._ok(False, 'slam_toolbox deserialize_map service not available (is SLAM running?)')
        if err == 'timeout':
            return self._ok(False, 'Timeout waiting for slam_toolbox deserialize_map')
        if response is None:
            return self._ok(False, 'slam_toolbox deserialize_map failed')
        return self._ok(True, f'Pose graph loaded from {clean}.posegraph',
                        posegraph_path=f'{clean}.posegraph')

    def auto_relocate(self):
        req = Trigger.Request()
        response, err = self._call_service(self.auto_reloc_client, req, wait_timeout=1.0, response_timeout=12.0)
        if err == 'service_not_available':
            return self._ok(False, 'Auto relocalize service not available')
        if err == 'timeout':
            return self._ok(False, 'Auto relocalize timed out')
        if response is None:
            return self._ok(False, 'Auto relocalize failed')
        success = bool(response.success)
        message = str(response.message)
        pose = self._extract_pose_from_status_text(message) if success else None
        payload: Dict[str, Any] = {}
        if pose is not None:
            x, y, yaw = pose
            self._record_requested_pose(x, y, yaw, 'auto_relocate')
            self._apply_robot_pose(x, y, yaw, 'auto_relocate', force=True)
            self.manual_pose_lock_until = time.time() + max(0.0, float(self.manual_pose_lock_sec))
            payload.update({
                'x': x,
                'y': y,
                'yaw': yaw,
                'pose_reflected': True,
            })
        return self._ok(success, message, **payload)

    def trigger_shelf_detection(self):
        status_payload = dict(self._default_shelf_status_payload())
        if isinstance(self.shelf_status, dict):
            status_payload.update(self.shelf_status)
        status_payload['shelf_detected'] = bool(status_payload.get('candidate_valid', False))

        if not bool(status_payload.get('candidate_valid', False)):
            reason = str(status_payload.get('last_reason') or 'stale_candidate')
            return self._ok(
                False,
                f'Shelf trigger rejected: {reason}',
                **status_payload,
            )

        req = Trigger.Request()
        response = None
        err = None
        try:
            response, err = self._call_service(
                self.shelf_commit_client,
                req,
                wait_timeout=0.8,
                response_timeout=2.5,
            )
        except Exception:
            response = None
            err = 'service_call_failed'

        if err == 'service_not_available':
            try:
                self.shelf_tick_pub.publish(BoolMsg(data=True))
                return self._ok(
                    True,
                    'Shelf trigger published on /tick for the latest fresh candidate',
                    commit_transport='tick_fallback',
                    **status_payload,
                )
            except Exception as exc:
                return self._ok(False, f'Failed to publish shelf detection trigger: {exc}', **status_payload)

        if err == 'timeout':
            return self._ok(False, 'Shelf commit service timed out', **status_payload)
        if err == 'service_call_failed':
            return self._ok(False, 'Shelf commit service call failed', **status_payload)
        if response is None:
            return self._ok(False, 'Shelf commit failed', **status_payload)

        success = bool(response.success)
        message = str(response.message or ('Shelf commit accepted' if success else 'Shelf commit failed'))

        # After successful commit, trigger the simple shelf inserter
        if success:
            try:
                self._call_service(
                    self.shelf_simple_start_client,
                    Trigger.Request(),
                    wait_timeout=0.8,
                    response_timeout=2.5,
                )
            except Exception:
                pass  # inserter may not be running; commit still succeeded

        return self._ok(success, message, commit_transport='service', **status_payload)

    def set_shelf_detector_enabled(self, enabled: bool):
        result = self._call_setbool(
            self.shelf_enable_client,
            enabled,
            unavailable_msg='Shelf detector enable service unavailable',
            timeout_msg='Shelf detector enable request timed out',
        )
        if isinstance(self.shelf_status, dict):
            self.shelf_status['detector_enabled'] = bool(enabled) if result.get('ok') else bool(
                self.shelf_status.get('detector_enabled', False)
            )
            if result.get('ok'):
                self.shelf_status['candidate_valid'] = False
                self.shelf_status['candidate_fresh'] = False
                self.shelf_status['candidate_consistent'] = False
                self.shelf_status['shelf_detected'] = False
                self.shelf_status['hotspot_count'] = 0
                self.shelf_status['hotspot_points'] = []
                self.shelf_status['solver_ok'] = False
                self.shelf_status['center_pose'] = None
                self.shelf_status['candidate_front_width_m'] = -1.0
                self.shelf_status['candidate_back_width_m'] = -1.0
                self.shelf_status['preferred_front_width_m'] = -1.0
                self.shelf_status['max_intensity'] = 0.0
                self.shelf_status['last_reason'] = 'waiting_for_scan' if enabled else 'detector_disabled'
                self.shelf_status_updated_at = time.time()
        return result

    def get_shelf_detection_status(self):
        last_age_sec = -1.0
        if float(self.shelf_status_updated_at) > 0.0:
            last_age_sec = max(0.0, time.time() - float(self.shelf_status_updated_at))
        elif float(self.shelf_detected_updated_at) > 0.0:
            last_age_sec = max(0.0, time.time() - float(self.shelf_detected_updated_at))

        status_payload = dict(self._default_shelf_status_payload())
        if isinstance(self.shelf_status, dict):
            status_payload.update(self.shelf_status)
        status_payload['shelf_detected'] = bool(status_payload.get('candidate_valid', False))

        return self._ok(
            True,
            'Shelf detection status',
            last_update_age_sec=float(last_age_sec),
            **status_payload,
        )

    # ---------- diagnostics ----------

    def nav2_diagnostics(self):
        topics = set(name for name, _types in self.get_topic_names_and_types())
        stack = self.get_stack_status()
        pose_snapshot = self.get_robot_pose()
        scan_front_topic = self._topic('diagnostic_scan_front')
        scan_rear_topic = self._topic('diagnostic_scan_rear')
        scan_union_cloud_topic = self._topic('subscription_scan_union_cloud') or self._topic('subscription_scan_overlay_primary')
        scan_combined_topic = self._topic('subscription_scan_combined')
        map_topic = self._topic('subscription_map')
        amcl_topic = self._topic('subscription_amcl_pose')
        slam_topic = self._topic('subscription_slam_pose')
        pose_fallback_topic = self._topic('subscription_pose_fallback')
        odom_filtered_topic = self._topic('subscription_odom_filtered')
        odom_topic = self._topic('subscription_odom')
        has_scan = bool(scan_front_topic and scan_front_topic in topics)
        has_scan2 = bool(scan_rear_topic and scan_rear_topic in topics)
        has_scan_union_cloud = bool(scan_union_cloud_topic and scan_union_cloud_topic in topics)
        has_scan_combined = bool(scan_combined_topic and scan_combined_topic in topics)
        has_scan_input = bool(has_scan or has_scan2 or has_scan_union_cloud or has_scan_combined)

        return {
            'current_state': {
                'localization_confidence': f'{self.localization_confidence:.1f}%',
                'safety_stop_active': bool(self.estop_active),
                'estop_active': bool(self.estop_active),
                'path_following': bool(self.path_state.get('active')),
                'has_lidar_data': bool(self.scan_front_points),
                'has_position': bool(pose_snapshot.get('has_pose')),
            },
            'topics': {
                'has_scan': has_scan,
                'has_scan2': has_scan2,
                'has_scan_union_cloud': has_scan_union_cloud,
                'has_scan_combined': bool(has_scan_combined or has_scan_union_cloud),
                'has_scan_input': has_scan_input,
                'has_map': bool(map_topic and map_topic in topics),
                'has_amcl_pose': bool(amcl_topic and amcl_topic in topics),
                'has_slam_pose': bool(slam_topic and slam_topic in topics),
                'has_pose_fallback': bool(pose_fallback_topic and pose_fallback_topic in topics),
                'has_odom_filtered': bool(odom_filtered_topic and odom_filtered_topic in topics),
                'has_odom': bool(odom_topic and odom_topic in topics),
                'has_tf': '/tf' in topics,
                'has_tf_static': '/tf_static' in topics,
            },
            'stack': stack,
        }

    def heartbeat_snapshot(self):
        # Heartbeat ownership should live in core diagnostics manager.
        return {}

    def obstacle_snapshot(self):
        points = self.scan_front_points
        if not points:
            return self._ok(False, 'No lidar data available')

        ranges = [p['range'] for p in points]
        min_dist = min(ranges) if ranges else None
        bins = {
            'critical': sum(1 for d in ranges if d < 0.4),
            'warning': sum(1 for d in ranges if 0.4 <= d < 0.8),
            'safe': sum(1 for d in ranges if 0.8 <= d < 2.0),
            'clear': sum(1 for d in ranges if d >= 2.0),
        }

        blocked = bins['critical'] > 10 or (min_dist is not None and min_dist < 0.4)

        return {
            'obstacle_count': bins,
            'total_points': len(ranges),
            'min_distance_m': None if min_dist is None else round(float(min_dist), 3),
            'safety_radius_m': 0.4,
            'assessment': 'BLOCKED' if blocked else 'CLEAR',
        }
