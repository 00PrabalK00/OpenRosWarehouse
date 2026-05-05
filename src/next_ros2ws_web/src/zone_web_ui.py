#!/usr/bin/env python3

import base64
import contextlib
import functools
import hashlib
import io
import json
import logging
import math
import os
import re
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import cv2
import rclpy
from ament_index_python.packages import get_package_share_directory
from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory, session
from flask_cors import CORS
from rclpy.executors import MultiThreadedExecutor
import yaml

from next_ros2ws_web.ros_bridge import RosBridge

try:
    from next_ros2ws_web import next_ops
except ImportError:
    import next_ops

try:
    from next_ros2ws_web.dotted_map import (
        DEFAULT_MAP_RESOLUTION_M_PER_PX as _DEFAULT_DOTTED_MAP_RESOLUTION,
        SUPPORTED_PRESETS as _DOTTED_PRESETS,
        generate_dotted_map as _generate_dotted_map,
    )
except ImportError:
    try:
        from dotted_map import (
            DEFAULT_MAP_RESOLUTION_M_PER_PX as _DEFAULT_DOTTED_MAP_RESOLUTION,
            SUPPORTED_PRESETS as _DOTTED_PRESETS,
            generate_dotted_map as _generate_dotted_map,
        )
    except ImportError:
        _generate_dotted_map = None
        _DOTTED_PRESETS = ('balanced', 'balanced_plus', 'quality', 'speed')
        _DEFAULT_DOTTED_MAP_RESOLUTION = 0.05

try:
    from next_ros2ws_core.action_registry import list_actions_payload
except Exception:
    def list_actions_payload():
        return []


pkg_share_dir = get_package_share_directory('next_ros2ws_web')
web_dir = os.path.join(pkg_share_dir, 'web')
DEV_AUTH_SESSION_KEY = 'next_dev_ui_unlocked'
app = Flask(
    __name__,
    template_folder=os.path.join(web_dir, 'templates'),
    static_folder=os.path.join(web_dir, 'static'),
)
app.secret_key = os.getenv('NEXT_WEB_UI_SESSION_SECRET', 'next_ros2ws_web_dev_session_secret')
# SECURITY: set NEXT_WEB_UI_SESSION_SECRET env var to a strong random secret in production.
# Example: export NEXT_WEB_UI_SESSION_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")

# SECURITY: restrict CORS to localhost only. Remote clients must go through an authenticated
# reverse proxy. Blanket CORS(app) would allow any origin to make credentialed requests.
CORS(app, origins=['http://localhost:5000', 'http://127.0.0.1:5000'], supports_credentials=True)

logging.getLogger('werkzeug').setLevel(logging.ERROR)

ros_node = None

_DOTTED_RENDER_VERSION = 'v9-dotted-map-restore'
_DOTTED_CACHE_MAX_ENTRIES = 12
_DOTTED_RENDER_CACHE: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
_DOTTED_RENDER_CACHE_LOCK = threading.Lock()
_LIVE_DOTTED_OCCUPIED_THRESHOLD = 15
_LIVE_DOTTED_UNKNOWN_SHADE = 230
_DEFAULT_CAMERA_DEVICE = '/dev/v4l/by-id/usb-046d_C270_HD_WEBCAM_F6E57210-video-index0'
_FALLBACK_CAMERA_DEVICE = '/dev/video0'
_CAMERA_WIDTH = int(os.getenv('NEXT_WEB_CAMERA_WIDTH', '640') or '640')
_CAMERA_HEIGHT = int(os.getenv('NEXT_WEB_CAMERA_HEIGHT', '360') or '360')
_CAMERA_FPS = max(1, int(os.getenv('NEXT_WEB_CAMERA_FPS', '10') or '10'))
_CAMERA_JPEG_QUALITY = max(40, min(90, int(os.getenv('NEXT_WEB_CAMERA_JPEG_QUALITY', '70') or '70')))
_CAMERA_CAPTURE_LOCK = threading.Lock()
_CAMERA_SHARED_CAPTURE = None
_CAMERA_SHARED_SOURCE = ''
_CAMERA_BLACK_FRAME_THRESHOLD = 3.0
_CAMERA_WARMUP_FRAMES = max(6, int(os.getenv('NEXT_WEB_CAMERA_WARMUP_FRAMES', '12') or '12'))


def _node():
    if ros_node is None:
        return None, (jsonify({'ok': False, 'message': 'ROS node not initialized'}), 500)
    return ros_node, None


def _camera_source_candidates() -> List[str]:
    configured = str(os.getenv('NEXT_WEB_CAMERA_DEVICE', '') or '').strip()
    candidates = [configured, _DEFAULT_CAMERA_DEVICE, _FALLBACK_CAMERA_DEVICE]
    return [path for path in candidates if path]


def _resolve_camera_source() -> Optional[str]:
    for path in _camera_source_candidates():
        if os.path.exists(path):
            return path
    return None


def _camera_stream_response(message: str, status: int = 503):
    return jsonify({'ok': False, 'message': str(message)}), status


def _camera_open_candidates(source_path: str) -> List[Any]:
    candidates: List[Any] = []
    for candidate in (source_path, os.path.realpath(source_path)):
        if candidate and candidate not in candidates:
            candidates.append(candidate)
        match = re.search(r'/dev/video(\d+)$', str(candidate))
        if match:
            index = int(match.group(1))
            if index not in candidates:
                candidates.append(index)
    return candidates


def _open_camera_capture(source_path: str):
    for candidate in _camera_open_candidates(source_path):
        for backend in (cv2.CAP_V4L2, cv2.CAP_ANY):
            capture = cv2.VideoCapture(candidate, backend)
            if capture.isOpened():
                return capture
            capture.release()
    return None


def _release_camera_capture() -> None:
    global _CAMERA_SHARED_CAPTURE, _CAMERA_SHARED_SOURCE
    if _CAMERA_SHARED_CAPTURE is not None:
        with contextlib.suppress(Exception):
            _CAMERA_SHARED_CAPTURE.release()
    _CAMERA_SHARED_CAPTURE = None
    _CAMERA_SHARED_SOURCE = ''


def _get_camera_capture(source_path: str):
    global _CAMERA_SHARED_CAPTURE, _CAMERA_SHARED_SOURCE
    if _CAMERA_SHARED_CAPTURE is not None and _CAMERA_SHARED_SOURCE == source_path:
        return _CAMERA_SHARED_CAPTURE

    _release_camera_capture()
    capture = _open_camera_capture(source_path)
    if capture is None:
        return None

    capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    capture.set(cv2.CAP_PROP_FRAME_WIDTH, _CAMERA_WIDTH)
    capture.set(cv2.CAP_PROP_FRAME_HEIGHT, _CAMERA_HEIGHT)
    capture.set(cv2.CAP_PROP_FPS, _CAMERA_FPS)
    _CAMERA_SHARED_CAPTURE = capture
    _CAMERA_SHARED_SOURCE = source_path
    return capture


def _capture_camera_frame(source_path: str) -> bytes:
    capture = _get_camera_capture(source_path)
    if capture is None:
        raise RuntimeError(f'Unable to open camera source: {source_path}')

    frame = None
    frame_mean = 0.0
    for _ in range(_CAMERA_WARMUP_FRAMES):
        ok, candidate = capture.read()
        if not ok or candidate is None:
            time.sleep(0.05)
            continue
        frame = candidate
        frame_mean = float(candidate.mean())
        if frame_mean > _CAMERA_BLACK_FRAME_THRESHOLD:
            break
        time.sleep(0.05)

    if frame is None:
        _release_camera_capture()
        raise RuntimeError(f'Unable to read camera frame from: {source_path}')

    if frame_mean <= _CAMERA_BLACK_FRAME_THRESHOLD:
        _release_camera_capture()
        raise RuntimeError('Camera is returning black startup frames; try again.')

    encoded_ok, encoded = cv2.imencode(
        '.jpg',
        frame,
        [int(cv2.IMWRITE_JPEG_QUALITY), _CAMERA_JPEG_QUALITY],
    )
    if not encoded_ok:
        raise RuntimeError('Unable to encode camera frame')
    return encoded.tobytes()


def _json_body() -> Dict[str, Any]:
    data = request.get_json(force=True, silent=True)
    return data if isinstance(data, dict) else {}


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


def _status(result: Dict[str, Any], *, ok_code: int = 200, fail_code: int = 500):
    return jsonify(result), ok_code if bool(result.get('ok')) else fail_code


def _conflict(message: str):
    return jsonify({'ok': False, 'conflict': True, 'message': str(message)}), 409


def _normalize_dotted_preset(raw: Any) -> str:
    preset = str(raw or 'balanced').strip().lower()
    if preset not in set(_DOTTED_PRESETS):
        return 'balanced'
    return preset


def _yaw_from_quaternion(payload: Any) -> float:
    if not isinstance(payload, dict):
        return 0.0
    x = float(payload.get('x', 0.0) or 0.0)
    y = float(payload.get('y', 0.0) or 0.0)
    z = float(payload.get('z', 0.0) or 0.0)
    w = float(payload.get('w', 1.0) or 1.0)
    siny_cosp = 2.0 * (w * z + x * y)
    cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
    return math.atan2(siny_cosp, cosy_cosp)


def _dotted_cache_get(cache_key: Tuple[str, str, str]) -> Optional[Dict[str, Any]]:
    with _DOTTED_RENDER_CACHE_LOCK:
        cached = _DOTTED_RENDER_CACHE.get(cache_key)
        return dict(cached) if isinstance(cached, dict) else None


def _dotted_cache_put(cache_key: Tuple[str, str, str], payload: Dict[str, Any]) -> None:
    with _DOTTED_RENDER_CACHE_LOCK:
        _DOTTED_RENDER_CACHE[cache_key] = dict(payload)
        while len(_DOTTED_RENDER_CACHE) > _DOTTED_CACHE_MAX_ENTRIES:
            oldest_key = next(iter(_DOTTED_RENDER_CACHE))
            if oldest_key == cache_key:
                break
            _DOTTED_RENDER_CACHE.pop(oldest_key, None)


def _base64_png_to_pil(encoded_png: str):
    from PIL import Image

    raw = base64.b64decode(str(encoded_png or '').encode('ascii'))
    return Image.open(io.BytesIO(raw)).convert('RGB')


def _live_map_to_pil(map_payload: Dict[str, Any]):
    from PIL import Image

    info = map_payload.get('info', {}) if isinstance(map_payload, dict) else {}
    width = int(info.get('width', 0) or 0)
    height = int(info.get('height', 0) or 0)
    data = map_payload.get('data', []) if isinstance(map_payload, dict) else []
    if width <= 0 or height <= 0 or len(data) < width * height:
        raise ValueError('Live map payload is incomplete')

    pixels = []
    for value in data[: width * height]:
        if value < 0:
            px = _LIVE_DOTTED_UNKNOWN_SHADE
        elif value >= _LIVE_DOTTED_OCCUPIED_THRESHOLD:
            # Treat moderate-confidence occupied cells as solid walls for the
            # dotted renderer. The previous grayscale mapping was throwing away
            # too much live SLAM structure and leaving only tiny fragments.
            px = 0
        elif value == 0:
            px = 255
        else:
            px = 245
        pixels.append(px)

    image = Image.new('L', (width, height))
    image.putdata(pixels)
    return image.transpose(Image.FLIP_TOP_BOTTOM).convert('RGB')


def _render_dotted_payload(
    *,
    pil_image,
    resolution: Any,
    origin: Any,
    source: str,
    preset: str,
    cache_token: str,
    stamp: Optional[Dict[str, Any]] = None,
    extra_lines: Optional[List] = None,
) -> Dict[str, Any]:
    if _generate_dotted_map is None:
        raise RuntimeError('Dotted map renderer is unavailable')

    resolved_preset = _normalize_dotted_preset(preset)
    try:
        resolved_resolution = float(resolution)
    except Exception:
        resolved_resolution = float(_DEFAULT_DOTTED_MAP_RESOLUTION)
    if resolved_resolution <= 0.0:
        resolved_resolution = float(_DEFAULT_DOTTED_MAP_RESOLUTION)

    # Extra lines bypass cache (overlay is mutable)
    cache_key = (_DOTTED_RENDER_VERSION, resolved_preset, str(cache_token))
    if not extra_lines:
        cached = _dotted_cache_get(cache_key)
        if cached is not None:
            return cached

    dotted_kwargs: Dict[str, Any] = dict(
        map_resolution_m_per_px=resolved_resolution,
        preset=resolved_preset,
    )
    if extra_lines:
        dotted_kwargs['extra_lines'] = extra_lines

    dotted_img = _generate_dotted_map(pil_image, **dotted_kwargs)
    out = io.BytesIO()
    dotted_img.save(out, format='PNG')
    payload: Dict[str, Any] = {
        'ok': True,
        'image': base64.b64encode(out.getvalue()).decode('ascii'),
        'width': int(dotted_img.width),
        'height': int(dotted_img.height),
        'resolution': float(resolved_resolution),
        'origin': list(origin) if isinstance(origin, (list, tuple)) else [0.0, 0.0, 0.0],
        'source': str(source or 'dotted'),
        'preset': resolved_preset,
        'dotted': True,
    }
    if isinstance(stamp, dict):
        payload['stamp'] = {
            'sec': int(stamp.get('sec', 0) or 0),
            'nanosec': int(stamp.get('nanosec', 0) or 0),
        }

    if not extra_lines:
        _dotted_cache_put(cache_key, payload)
    return dict(payload)


def _access_config_file() -> str:
    static_dir = app.static_folder or os.path.join(web_dir, 'static')
    resources_dir = os.path.join(static_dir, 'resources')

    env_override = os.getenv('NEXT_OPERATOR_ACCESS_FILE', '').strip()
    if env_override:
        return os.path.abspath(os.path.expanduser(env_override))

    local_override = os.path.join(resources_dir, 'operator_access.local.yaml')
    if os.path.isfile(local_override):
        return local_override

    return os.path.join(resources_dir, 'operator_access.yaml')


def _load_dev_panel_password() -> str:
    env_password = os.getenv('NEXT_DEV_UI_PASSWORD', '').strip()
    if env_password:
        return env_password

    default_password = ''
    config_file = _access_config_file()
    if not os.path.isfile(config_file):
        return default_password

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            payload = yaml.safe_load(f) or {}
    except Exception:
        return default_password

    if not isinstance(payload, dict):
        return default_password

    dev_ui = payload.get('dev_ui', {})
    if isinstance(dev_ui, dict):
        dev_password = str(dev_ui.get('password', '') or '').strip()
        if dev_password:
            return dev_password

    # Backward compatibility with older config key.
    operator_ui = payload.get('operator_ui', {})
    if isinstance(operator_ui, dict):
        legacy_password = str(operator_ui.get('password', '') or '').strip()
        if legacy_password:
            return legacy_password

    root_password = str(payload.get('password', '') or '').strip()
    if root_password:
        return root_password

    return default_password


def _dev_auth_required() -> bool:
    return bool(_load_dev_panel_password())


def _dev_unlocked() -> bool:
    return bool(session.get(DEV_AUTH_SESSION_KEY, False))


# ---------------------------------------------------------------------------
# Motion-critical API auth guard
# ---------------------------------------------------------------------------
# All POST/DELETE endpoints that command robot motion, change safety state,
# or alter navigation mode require authentication. Callers must either have
# an active /dev browser session, or supply the panel password via the
# X-API-Token request header (for non-browser automation scripts).
_MOTION_PATHS: frozenset = frozenset({
    '/api/manual/velocity',
    '/api/estop/activate',
    '/api/estop/reset',
    '/api/mode/manual',
    '/api/mode/auto',
    '/api/zones/goto',
    '/api/path_mode/goto',
    '/api/path/follow',
    '/api/path/stop',
    '/api/sequence/start',
    '/api/sequence/stop',
    '/api/safety/force_resume',
    '/api/safety/override',
    '/api/robot/set_pose',
    '/api/stack/mode',
    '/api/stack/shutdown',
    '/api/auto_relocate',
    '/api/mission/resume',
    '/api/mission/clear',
})


@app.before_request
def _check_motion_auth():
    """Reject unauthenticated callers on all motion-critical endpoints."""
    if request.path not in _MOTION_PATHS:
        return None
    # Auth not configured → open (dev/test environment without password file).
    if not _dev_auth_required():
        return None
    # Valid browser session → allow.
    if _dev_unlocked():
        return None
    # Non-browser clients may use X-API-Token header with the panel password.
    api_token = request.headers.get('X-API-Token', '').strip()
    if api_token and api_token == _load_dev_panel_password():
        return None
    return jsonify({'ok': False, 'message': 'Unauthorized: session or X-API-Token required'}), 401



@app.route('/')
def root():
    return redirect('/dev')


@app.route('/dev')
def index():
    if not _dev_auth_required() or _dev_unlocked():
        return render_template('index.html', user_page=False)
    return render_template('dev_lock.html', error='')


@app.route('/dev/camera')
def dev_camera():
    if _dev_auth_required() and not _dev_unlocked():
        return "You don't have enough permissions to access this page.", 403

    source_path = _resolve_camera_source()
    return render_template(
        'camera.html',
        source_device=source_path or 'unavailable',
        frame_url='/api/camera/frame',
        available=bool(source_path),
        width=_CAMERA_WIDTH,
        height=_CAMERA_HEIGHT,
        fps=_CAMERA_FPS,
    )


@app.route('/dev/unlock', methods=['POST'])
def unlock_dev():
    if not _dev_auth_required():
        session[DEV_AUTH_SESSION_KEY] = True
        return redirect('/dev')

    username = str(request.form.get('username', '') or '').strip()
    password = str(request.form.get('password', '') or '').strip()
    
    success, role = next_ops.verify_login(username, password)
    if success:
        session[DEV_AUTH_SESSION_KEY] = True
        session['next_user_name'] = username
        session['next_user_role'] = role
        return redirect('/dev')

    return render_template('dev_lock.html', error='Invalid username or password')


@app.route('/dev/lock', methods=['POST'])
def lock_dev():
    session.pop(DEV_AUTH_SESSION_KEY, None)
    return redirect('/dev')


@app.route('/user')
def operator_ui():
    return render_template('index.html', user_page=True)


@app.route('/editor')
def editor():
    if _dev_auth_required() and not _dev_unlocked():
        return "You don't have enough permissions to access this page.", 403
    return render_template('map_editor.html')


@app.route('/device-config')
def device_config():
    if _dev_auth_required() and not _dev_unlocked():
        return "You don't have enough permissions to access this page.", 403
    return render_template('device_config.html')


@app.route('/api/map')
def get_map():
    node, err = _node()
    if err:
        return err

    result = node.get_map_png()
    if not result.get('ok'):
        return jsonify(result), 503

    dotted = request.args.get('dotted', 'false').lower() in ('1', 'true', 'yes')
    if dotted and _generate_dotted_map is not None:
        try:
            pil_img = _base64_png_to_pil(str(result.get('image', '') or ''))
            preset = _normalize_dotted_preset(request.args.get('preset', 'balanced'))
            cache_hash = hashlib.sha1(str(result.get('image', '') or '').encode('ascii')).hexdigest()
            # Load user-drawn overlay segments if present
            extra_lines: List = []
            overlay_path = _dotted_overlay_path(node)
            if overlay_path and os.path.isfile(overlay_path):
                try:
                    with open(overlay_path, 'r', encoding='utf-8') as _f:
                        _ov = json.load(_f)
                    extra_lines = _ov.get('segments', []) if isinstance(_ov, dict) else []
                except Exception:
                    extra_lines = []
            dotted_payload = _render_dotted_payload(
                pil_image=pil_img,
                resolution=result.get('resolution', _DEFAULT_DOTTED_MAP_RESOLUTION),
                origin=result.get('origin', [0.0, 0.0, 0.0]),
                source=f"{str(result.get('source', 'map'))}_dotted",
                preset=preset,
                cache_token=f"static:{cache_hash}:{preset}",
                extra_lines=extra_lines or None,
            )
            return jsonify(dotted_payload), 200
        except Exception as exc:
            app.logger.warning('Dotted map render failed for /api/map: %s', exc)
            pass  # Fall through and return original map on any error

    return jsonify(result), 200


@app.route('/api/camera/frame')
def get_camera_frame():
    if _dev_auth_required() and not _dev_unlocked():
        return _camera_stream_response('Unauthorized', 403)

    source_path = _resolve_camera_source()
    if not source_path:
        return _camera_stream_response(
            'No camera device found. Set NEXT_WEB_CAMERA_DEVICE or connect a webcam.',
            503,
        )

    lock_acquired = False
    try:
        lock_acquired = _CAMERA_CAPTURE_LOCK.acquire(timeout=1.0)
        if not lock_acquired:
            return _camera_stream_response('Camera is busy. Try again in a moment.', 503)
        jpg_bytes = _capture_camera_frame(source_path)
        return Response(
            jpg_bytes,
            mimetype='image/jpeg',
            headers={
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0',
            },
        )
    except RuntimeError as exc:
        return _camera_stream_response(str(exc), 503)
    finally:
        if lock_acquired:
            with contextlib.suppress(Exception):
                _CAMERA_CAPTURE_LOCK.release()


@app.route('/api/map/live_rendered')
def get_live_rendered_map():
    node, err = _node()
    if err:
        return err

    result = node.get_live_map()
    if not result.get('ok'):
        return jsonify(result), 503

    map_payload = result.get('map')
    if not isinstance(map_payload, dict):
        return jsonify({'ok': False, 'message': 'Live map payload missing'}), 503

    try:
        preset = _normalize_dotted_preset(request.args.get('preset', 'balanced'))
        info = map_payload.get('info', {}) if isinstance(map_payload, dict) else {}
        origin_payload = info.get('origin', {}) if isinstance(info, dict) else {}
        position = origin_payload.get('position', {}) if isinstance(origin_payload, dict) else {}
        orientation = origin_payload.get('orientation', {}) if isinstance(origin_payload, dict) else {}
        resolution = float(info.get('resolution', _DEFAULT_DOTTED_MAP_RESOLUTION) or _DEFAULT_DOTTED_MAP_RESOLUTION)
        origin = [
            float(position.get('x', 0.0) or 0.0),
            float(position.get('y', 0.0) or 0.0),
            float(_yaw_from_quaternion(orientation)),
        ]
        stamp = map_payload.get('stamp', {}) if isinstance(map_payload, dict) else {}
        stamp_key = f"{int(stamp.get('sec', 0) or 0)}.{int(stamp.get('nanosec', 0) or 0)}"
        pil_img = _live_map_to_pil(map_payload)
        dotted_payload = _render_dotted_payload(
            pil_image=pil_img,
            resolution=resolution,
            origin=origin,
            source='live_dotted',
            preset=preset,
            cache_token=f"live:{stamp_key}:{preset}:{int(info.get('width', 0) or 0)}x{int(info.get('height', 0) or 0)}",
            stamp=stamp if isinstance(stamp, dict) else None,
        )
        return jsonify(dotted_payload), 200
    except Exception as exc:
        app.logger.warning('Dotted live map render failed: %s', exc)
        return jsonify({'ok': False, 'message': f'Failed to render dotted live map: {exc}'}), 503


@app.route('/api/map/live')
def get_live_map():
    node, err = _node()
    if err:
        return err
    return jsonify(node.get_live_map())


@app.route('/api/zones')
def get_zones():
    node, err = _node()
    if err:
        return err

    result = node.get_zones()
    return jsonify({'zones': result.get('zones', {})}), (200 if result.get('ok') else 503)


@app.route('/api/robot/pose')
def get_robot_pose():
    node, err = _node()
    if err:
        return err
    return jsonify(node.get_robot_pose())


@app.route('/api/path')
def get_path():
    node, err = _node()
    if err:
        return err

    result = node.get_live_path()
    return jsonify({'path': result.get('path', [])})


@app.route('/api/scan')
def get_scan():
    node, err = _node()
    if err:
        return err

    combined = list(node.scan_front_points) + list(node.scan_rear_points)
    return jsonify({
        'scan': combined,
        'origin': node.scan_overlay_origin,
        'source': str(node.scan_overlay_source or ''),
    })


@app.route('/api/zones/save', methods=['POST'])
def save_zone():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    name = data.get('name')
    x = data.get('x')
    y = data.get('y')
    theta = data.get('theta', 0.0)
    overwrite = _as_bool(data.get('overwrite', False), default=False)

    if not name or x is None or y is None:
        return jsonify({'ok': False, 'message': 'Missing required fields'}), 400

    zone_name = str(name).strip()
    if not overwrite:
        existing = node.get_zones()
        zone_map = existing.get('zones', {}) if isinstance(existing, dict) else {}
        if isinstance(zone_map, dict) and zone_name in zone_map:
            return _conflict(f'Zone "{zone_name}" already exists. Confirm overwrite to replace it.')

    result = node.save_zone(
        name=zone_name,
        x=float(x),
        y=float(y),
        theta=float(theta),
        zone_type=data.get('type', 'normal'),
        speed=float(data.get('speed', 0.5)),
        action=str(data.get('action', '')),
        charge_duration=float(data.get('charge_duration', 0.0) or 0.0),
        point_type=str(data.get('point_type', 'generic')),
        template_id=str(data.get('template_id', data.get('shelf_template_id', ''))),
        recognize=_as_bool(data.get('recognize', data.get('shelf_detection_on', False)), default=False),
        action_point_notes=str(data.get('action_point_notes', '')),
        pre_point=data.get('pre_point') or None,
    )
    return _status(result, fail_code=503)


@app.route('/api/zones/goto', methods=['POST'])
def goto_zone():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    name = data.get('name')
    if not name:
        return jsonify({'ok': False, 'message': 'Missing zone name'}), 400

    result = node.go_to_zone(str(name))
    return _status(result, fail_code=503)


@app.route('/api/zones/delete', methods=['POST'])
def delete_zone():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    name = data.get('name')
    if not name:
        return jsonify({'ok': False, 'message': 'Missing zone name'}), 400

    result = node.delete_zone(str(name))
    return _status(result, fail_code=503)


@app.route('/api/zones/reorder', methods=['POST'])
def reorder_zones():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    zones_dict = data.get('zones')
    if not isinstance(zones_dict, dict):
        return jsonify({'ok': False, 'message': 'Missing zones data'}), 400

    result = node.reorder_zones(list(zones_dict.keys()))
    return _status(result, fail_code=503)


@app.route('/api/zones/update', methods=['POST'])
def update_zone():
    # Position update maps to the same core save-zone API.
    return save_zone()


@app.route('/api/zones/update-params', methods=['POST'])
def update_zone_params():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    name = data.get('name')
    if not name:
        return jsonify({'ok': False, 'message': 'Zone name is required'}), 400

    result = node.update_zone_params(
        name=str(name),
        zone_type=str(data.get('type', 'normal')),
        speed=float(data.get('speed', 0.5)),
        action=str(data.get('action', '')),
        charge_duration=float(data.get('charge_duration', 0.0) or 0.0),
        point_type=str(data.get('point_type', 'generic')),
        template_id=str(data.get('template_id', data.get('shelf_template_id', ''))),
        recognize=_as_bool(data.get('recognize', data.get('shelf_detection_on', False)), default=False),
        action_point_notes=str(data.get('action_point_notes', '')),
        pre_point=data.get('pre_point') or None,
    )
    return _status(result, fail_code=503)


@app.route('/api/recognition/templates', methods=['GET'])
def get_recognition_templates():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'model:read'):
        return jsonify(next_ops.permission_error(role, 'model:read')), 403

    node, err = _node()
    if err:
        return err

    result = node.list_recognition_templates()
    return _status(result, fail_code=503)


@app.route('/api/recognition/templates/save', methods=['POST'])
def save_recognition_template():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'model:write'):
        return jsonify(next_ops.permission_error(role, 'model:write')), 403

    node, err = _node()
    if err:
        return err

    data = _json_body()
    result = node.save_recognition_template(data.get('template', data))
    return _status(result, fail_code=400)


@app.route('/api/recognition/templates/validate', methods=['POST'])
def validate_recognition_template():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'validation:run'):
        return jsonify(next_ops.permission_error(role, 'validation:run')), 403

    node, err = _node()
    if err:
        return err

    data = _json_body()
    result = node.validate_recognition_template(data.get('template', data))
    return _status(result, fail_code=400)


@app.route('/api/recognition/templates/publish', methods=['POST'])
def publish_recognition_template():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'model:write'):
        return jsonify(next_ops.permission_error(role, 'model:write')), 403

    node, err = _node()
    if err:
        return err

    data = _json_body()
    result = node.publish_recognition_template(data.get('template', data))
    return _status(result, fail_code=400)


@app.route('/api/recognition/templates/duplicate', methods=['POST'])
def duplicate_recognition_template():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'model:write'):
        return jsonify(next_ops.permission_error(role, 'model:write')), 403

    node, err = _node()
    if err:
        return err

    data = _json_body()
    template_id = str(data.get('template_id', '') or '').strip()
    if not template_id:
        return jsonify({'ok': False, 'message': 'template_id is required'}), 400

    result = node.duplicate_recognition_template(template_id)
    return _status(result, fail_code=404)


@app.route('/api/recognition/templates/delete', methods=['POST'])
def delete_recognition_template():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'model:write'):
        return jsonify(next_ops.permission_error(role, 'model:write')), 403

    node, err = _node()
    if err:
        return err

    data = _json_body()
    template_id = str(data.get('template_id', '') or '').strip()
    if not template_id:
        return jsonify({'ok': False, 'message': 'template_id is required'}), 400

    result = node.delete_recognition_template(template_id)
    return _status(result, fail_code=400)


@app.route('/api/recognition/templates/export/<template_id>', methods=['GET'])
def export_recognition_template(template_id):
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'export:run'):
        return jsonify(next_ops.permission_error(role, 'export:run')), 403

    node, err = _node()
    if err:
        return err

    result = node.export_recognition_template(template_id)
    return _status(result, fail_code=404)


@app.route('/api/recognition/templates/pull_all', methods=['POST'])
def pull_all_recognition_templates():
    """Pull all recognition files from the robot to the local store."""
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'import:run'):
        return jsonify(next_ops.permission_error(role, 'import:run')), 403

    node, err = _node()
    if err:
        return err

    if hasattr(node, 'pull_all_recognition_templates'):
        result = node.pull_all_recognition_templates()
    elif hasattr(node, 'sync_recognition_templates'):
        result = node.sync_recognition_templates(direction='pull')
    else:
        # Fallback: list templates (acts as a refresh from whatever source is available)
        result = node.list_recognition_templates()
        if isinstance(result, dict):
            result = dict(result)
            result.setdefault('message', 'Pull triggered (list refreshed)')
    return _status(result, fail_code=503)


@app.route('/api/recognition/templates/push_all', methods=['POST'])
def push_all_recognition_templates():
    """Push all recognition files from local store to the robot."""
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'export:run'):
        return jsonify(next_ops.permission_error(role, 'export:run')), 403

    node, err = _node()
    if err:
        return err

    if hasattr(node, 'push_all_recognition_templates'):
        result = node.push_all_recognition_templates()
    elif hasattr(node, 'sync_recognition_templates'):
        result = node.sync_recognition_templates(direction='push')
    else:
        # Fallback: attempt to publish each known template
        list_result = node.list_recognition_templates()
        templates = []
        if isinstance(list_result, dict):
            templates = list_result.get('templates', list_result.get('items', []))
        elif isinstance(list_result, list):
            templates = list_result
        pushed = 0
        errors = []
        for tmpl in templates:
            try:
                r = node.publish_recognition_template(tmpl)
                if isinstance(r, dict) and r.get('ok'):
                    pushed += 1
                else:
                    errors.append(str(tmpl.get('name', tmpl.get('id', '?'))))
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
        if errors:
            result = {'ok': False, 'message': 'Pushed ' + str(pushed) + ', failed: ' + ', '.join(errors)}
        else:
            result = {'ok': True, 'message': 'Pushed ' + str(pushed) + ' template(s)'}
    return _status(result, fail_code=503)


@app.route('/api/path/follow', methods=['POST'])
def follow_path():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    path_points = data.get('path', [])
    loop_type = data.get('loop_type', 'none')
    loop_mode = _as_bool(data.get('loop_mode', False), default=False)
    settings = data.get('settings', {})

    result = node.follow_path(path_points, loop_type=loop_type, loop_mode=loop_mode, settings=settings)
    code = 200 if result.get('ok') else 503
    return jsonify(result), code


@app.route('/api/path_mode/plan', methods=['POST'])
def plan_path_mode_route():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    destination = (
        data.get('destination_poi')
        or data.get('destination')
        or data.get('zone_name')
        or data.get('name')
        or ''
    )
    selected_path = data.get('selected_path') or data.get('path_name') or ''
    current_poi = data.get('current_poi') or data.get('start_poi') or ''

    result = node.plan_path_mode_route(
        str(destination),
        selected_path=str(selected_path or ''),
        current_poi=str(current_poi or ''),
    )
    return _status(result, fail_code=404)


@app.route('/api/path_mode/goto', methods=['POST'])
def goto_path_mode_route():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    destination = (
        data.get('destination_poi')
        or data.get('destination')
        or data.get('zone_name')
        or data.get('name')
        or ''
    )
    selected_path = data.get('selected_path') or data.get('path_name') or ''
    current_poi = data.get('current_poi') or data.get('start_poi') or ''

    result = node.follow_path_mode_route(
        str(destination),
        selected_path=str(selected_path or ''),
        current_poi=str(current_poi or ''),
    )
    return _status(result, fail_code=503)


@app.route('/api/path/stop', methods=['POST'])
def stop_path():
    node, err = _node()
    if err:
        return err

    result = node.stop_path()
    return _status(result, fail_code=503)


@app.route('/api/path/status')
def path_status():
    node, err = _node()
    if err:
        return err
    return jsonify(node.get_path_status())


@app.route('/api/path/save', methods=['POST'])
def save_path():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    name = data.get('name')
    path_points = data.get('path', [])
    settings = data.get('settings', {})
    overwrite = _as_bool(data.get('overwrite', False), default=False)

    if not name:
        return jsonify({'ok': False, 'message': 'Path name is required'}), 400

    path_name = str(name).strip()
    if not overwrite:
        existing = node.get_paths()
        path_map = existing.get('paths', {}) if isinstance(existing, dict) else {}
        if isinstance(path_map, dict) and path_name in path_map:
            return _conflict(f'Path "{path_name}" already exists. Confirm overwrite to replace it.')

    if not isinstance(settings, dict):
        settings = {}
    result = node.save_path(path_name, path_points, settings=settings)
    return _status(result, fail_code=503)


@app.route('/api/paths')
def get_paths():
    node, err = _node()
    if err:
        return err

    result = node.get_paths()
    return jsonify({
        'paths': result.get('paths', {}),
        'path_settings': result.get('path_settings', {}),
        'path_metrics': result.get('path_metrics', {}),
    }), (200 if result.get('ok') else 503)


@app.route('/api/path/load/<path_name>')
def load_path(path_name):
    node, err = _node()
    if err:
        return err

    result = node.load_path(path_name)
    return _status(result, fail_code=404)


@app.route('/api/path/delete/<path_name>', methods=['DELETE'])
def delete_path(path_name):
    node, err = _node()
    if err:
        return err

    result = node.delete_path(path_name)
    return _status(result, fail_code=503)



@app.route('/api/mission/status')
def get_mission_status():
    node, err = _node()
    if err:
        return err

    result = node.get_mission_status()
    return jsonify(result), (200 if result.get('ok') else 503)


@app.route('/api/mission/resume', methods=['POST'])
def resume_mission():
    node, err = _node()
    if err:
        return err

    result = node.resume_mission()
    return _status(result, fail_code=503)


@app.route('/api/mission/clear', methods=['POST'])
def clear_mission():
    node, err = _node()
    if err:
        return err

    result = node.clear_mission_state()
    return _status(result, fail_code=503)
@app.route('/api/layouts')
def get_layouts():
    node, err = _node()
    if err:
        return err

    result = node.get_layouts()
    return jsonify({'current': result.get('current'), 'layouts': result.get('layouts', {})}), (
        200 if result.get('ok') else 503
    )


@app.route('/api/layout/save', methods=['POST'])
def save_layout():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    name = data.get('name')
    overwrite = _as_bool(data.get('overwrite', False), default=False)
    if not name:
        return jsonify({'ok': False, 'message': 'Layout name is required'}), 400

    layout_name = str(name).strip()
    if not overwrite:
        existing = node.get_layouts()
        layout_map = existing.get('layouts', {}) if isinstance(existing, dict) else {}
        if isinstance(layout_map, dict) and layout_name in layout_map:
            return _conflict(f'Layout "{layout_name}" already exists. Confirm overwrite to replace it.')

    result = node.save_layout(layout_name, str(data.get('description', '')))
    return _status(result, fail_code=503)


@app.route('/api/layout/load/<layout_name>', methods=['POST'])
def load_layout(layout_name):
    node, err = _node()
    if err:
        return err

    result = node.load_layout(layout_name)
    return _status(result, fail_code=503)


@app.route('/api/layout/delete/<layout_name>', methods=['DELETE'])
def delete_layout(layout_name):
    node, err = _node()
    if err:
        return err

    result = node.delete_layout(layout_name)
    return _status(result, fail_code=503)


@app.route('/api/safety/force_resume', methods=['POST'])
def force_resume():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'test_mode:run'):
        return jsonify(next_ops.permission_error(role, 'test_mode:run')), 403

    node, err = _node()
    if err:
        return err

    # Force-resume should clear latches and explicitly keep override OFF.
    clear_result = node.clear_safety_state()
    override_off_result = node.set_safety_override(False)

    if clear_result.get('ok'):
        ok = bool(override_off_result.get('ok'))
        result = {
            'ok': ok,
            'message': (
                'Safety cleared and override disabled'
                if ok else f"Safety cleared, but failed to disable override: {override_off_result.get('message', 'unknown error')}"
            ),
            'clear': clear_result,
            'override': override_off_result,
        }
        return _status(result, fail_code=503)

    # Fallback for deployments without safety/clear_state: reset E-STOP + ensure override OFF.
    estop_result = node.set_estop(False)
    ok = bool(estop_result.get('ok')) and bool(override_off_result.get('ok'))
    result = {
        'ok': ok,
        'message': (
            'Fallback resume applied: estop reset + override disabled'
            if ok else (
                'Force resume failed: '
                f"clear_state={clear_result.get('message', 'n/a')}; "
                f"estop_reset={estop_result.get('message', 'n/a')}; "
                f"override_disable={override_off_result.get('message', 'n/a')}"
            )
        ),
        'clear': clear_result,
        'estop': estop_result,
        'override': override_off_result,
    }
    return _status(result, fail_code=503)


@app.route('/api/safety/override', methods=['POST'])
def set_safety_override():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'test_mode:run'):
        return jsonify(next_ops.permission_error(role, 'test_mode:run')), 403

    node, err = _node()
    if err:
        return err

    data = _json_body()
    enable = bool(data.get('enable', True))
    result = node.set_safety_override(enable)
    return _status(result, fail_code=503)


@app.route('/api/safety/status', methods=['GET'])
def get_safety_status():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'network:read'):
        return jsonify(next_ops.permission_error(role, 'network:read')), 403

    node, err = _node()
    if err:
        return err

    result = node.get_safety_status()
    # Keep backward behavior: status endpoint returns 200 even on degraded state.
    return jsonify(result), 200



@app.route('/api/sequence/start', methods=['POST'])
def start_sequence():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    zones = data.get('zones', [])
    if not isinstance(zones, list):
        return jsonify({'ok': False, 'message': 'zones must be a list'}), 400

    result = node.start_sequence(zones)
    return _status(result, fail_code=503)


@app.route('/api/sequence/stop', methods=['POST'])
def stop_sequence():
    node, err = _node()
    if err:
        return err

    result = node.stop_sequence()
    return _status(result, fail_code=503)


@app.route('/api/sequence/status')
def sequence_status():
    node, err = _node()
    if err:
        return err

    result = node.get_sequence_status()
    return jsonify(result), (200 if result.get('ok') else 503)
@app.route('/api/manual/velocity', methods=['POST'])
def manual_velocity():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    linear = float(data.get('linear', 0.0) or 0.0)
    angular = float(data.get('angular', 0.0) or 0.0)
    result = node.publish_manual_velocity(linear, angular)
    return _status(result, fail_code=503)


@app.route('/api/estop/activate', methods=['POST'])
def estop_activate():
    node, err = _node()
    if err:
        return err

    result = node.set_estop(True)
    return _status(result, fail_code=503)


@app.route('/api/estop/reset', methods=['POST'])
def estop_reset():
    node, err = _node()
    if err:
        return err

    # E-STOP reset must be safety-only. Do not alter stack mode or mission/path state.
    clear_result = node.clear_safety_state()
    override_off_result = node.set_safety_override(False)

    if clear_result.get('ok'):
        ok = bool(override_off_result.get('ok'))
        result = {
            'ok': ok,
            'message': (
                'E-STOP cleared and safety override disabled'
                if ok else f"E-STOP cleared, but failed to disable override: {override_off_result.get('message', 'unknown error')}"
            ),
            'clear': clear_result,
            'override': override_off_result,
        }
        return _status(result, fail_code=503)

    # Fallback for deployments without safety/clear_state support.
    estop_result = node.set_estop(False)
    ok = bool(estop_result.get('ok')) and bool(override_off_result.get('ok'))
    result = {
        'ok': ok,
        'message': (
            'Fallback E-STOP reset applied: estop reset + override disabled'
            if ok else (
                'E-STOP reset failed: '
                f"clear_state={clear_result.get('message', 'n/a')}; "
                f"estop_reset={estop_result.get('message', 'n/a')}; "
                f"override_disable={override_off_result.get('message', 'n/a')}"
            )
        ),
        'clear': clear_result,
        'estop': estop_result,
        'override': override_off_result,
    }
    return _status(result, fail_code=503)


@app.route('/api/mode/manual', methods=['POST'])
def set_manual_mode():
    node, err = _node()
    if err:
        return err

    result = node.set_control_mode('manual')
    if result.get('ok'):
        result.setdefault('message', 'Switched to manual control mode')
    return _status(result, fail_code=503)


@app.route('/api/mode/auto', methods=['POST'])
def set_auto_mode():
    node, err = _node()
    if err:
        return err

    result = node.set_control_mode('zones')
    if result.get('ok'):
        result.setdefault('message', 'Switched to autonomous navigation mode')
    return _status(result, fail_code=503)


@app.route('/api/robot/set_pose', methods=['POST'])
def set_initial_pose():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    x = data.get('x')
    y = data.get('y')
    theta = data.get('theta', 0.0)

    if x is None or y is None:
        return jsonify({'ok': False, 'message': 'Missing x or y coordinates'}), 400

    result = node.set_initial_pose(float(x), float(y), float(theta or 0.0))
    return _status(result, fail_code=503)


@app.route('/api/diagnostics/nav2', methods=['GET'])
def nav2_diagnostics():
    node, err = _node()
    if err:
        return err

    return jsonify(node.nav2_diagnostics())


@app.route('/api/diagnostics/heartbeats', methods=['GET'])
def get_heartbeats():
    node, err = _node()
    if err:
        return err

    return jsonify(node.heartbeat_snapshot())


@app.route('/api/diagnostics/obstacles', methods=['GET'])
def check_obstacles():
    node, err = _node()
    if err:
        return err

    result = node.obstacle_snapshot()
    if result.get('ok') is False:
        return jsonify(result), 503
    return jsonify(result)


@app.route('/api/editor/map/preview')
def get_editor_map_preview():
    node, err = _node()
    if err:
        return err

    result = node.get_editor_map_preview()
    if not result.get('ok'):
        return jsonify(result), 503

    dotted = request.args.get('dotted', 'false').lower() in ('1', 'true', 'yes')
    if dotted and _generate_dotted_map is not None:
        try:
            pil_img = _base64_png_to_pil(str(result.get('image', '') or ''))
            preset = _normalize_dotted_preset(request.args.get('preset', 'balanced'))
            cache_hash = hashlib.sha1(str(result.get('image', '') or '').encode('ascii')).hexdigest()
            extra_lines: List = []
            overlay_path = _dotted_overlay_path(node)
            if overlay_path and os.path.isfile(overlay_path):
                try:
                    with open(overlay_path, 'r', encoding='utf-8') as _f:
                        _ov = json.load(_f)
                    extra_lines = _ov.get('segments', []) if isinstance(_ov, dict) else []
                except Exception:
                    extra_lines = []
            dotted_payload = _render_dotted_payload(
                pil_image=pil_img,
                resolution=result.get('resolution', _DEFAULT_DOTTED_MAP_RESOLUTION),
                origin=result.get('origin', [0.0, 0.0, 0.0]),
                source='editor_preview_dotted',
                preset=preset,
                cache_token=f"editor:{cache_hash}:{preset}",
                extra_lines=extra_lines or None,
            )
            return jsonify(dotted_payload), 200
        except Exception as exc:
            app.logger.warning('Dotted map render failed for /api/editor/map/preview: %s', exc)

    return jsonify(result), 200


@app.route('/api/editor/layers', methods=['GET'])
def get_editor_layers():
    node, err = _node()
    if err:
        return err

    result = node.get_filter_layers()
    return _status(result, fail_code=503)


@app.route('/api/filters', methods=['GET'])
def get_filters():
    node, err = _node()
    if err:
        return err

    result = node.get_filter_layers()
    return _status(result, fail_code=503)


@app.route('/api/filters/add', methods=['POST'])
def add_filter():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    layer = str(data.get('layer', '') or '').strip()
    obj = data.get('obj')
    if not layer or not isinstance(obj, dict):
        return jsonify({'ok': False, 'message': 'layer and obj are required'}), 400

    result = node.add_filter_object(layer, obj)
    return _status(result, fail_code=503)


@app.route('/api/filters/delete', methods=['POST'])
def delete_filter():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    layer = str(data.get('layer', '') or '').strip()
    object_id = str(data.get('id', '') or '').strip()
    if not layer or not object_id:
        return jsonify({'ok': False, 'message': 'layer and id are required'}), 400

    result = node.delete_filter_object(layer, object_id)
    return _status(result, fail_code=503)


@app.route('/api/filters/clear', methods=['POST'])
def clear_filters():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    layer = str(data.get('layer', 'all') or 'all').strip() or 'all'
    result = node.clear_filter_layer(layer)
    return _status(result, fail_code=503)


@app.route('/api/editor/layer/add', methods=['POST'])
def add_editor_layer_object():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    layer = str(data.get('layer', '') or '').strip()
    obj = data.get('obj')
    if not layer or not isinstance(obj, dict):
        return jsonify({'ok': False, 'message': 'layer and obj are required'}), 400

    result = node.add_filter_object(layer, obj)
    return _status(result, fail_code=503)


@app.route('/api/editor/layer/clear', methods=['POST'])
def clear_editor_layer():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    layer = str(data.get('layer', 'all') or 'all').strip() or 'all'
    result = node.clear_filter_layer(layer)
    return _status(result, fail_code=503)


@app.route('/api/editor/map/export', methods=['POST'])
def export_editor_map():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    map_name = str(data.get('map_name') or data.get('name') or '').strip()
    result = node.export_editor_map(map_name)
    return _status(result, fail_code=503)


@app.route('/api/editor/map/overwrite', methods=['POST'])
def overwrite_map_with_edits():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    image = data.get('image')
    origin = data.get('origin', [])
    if not image:
        return jsonify({'ok': False, 'message': 'image is required'}), 400

    result = node.overwrite_editor_map(str(image), origin if isinstance(origin, list) else [])
    return _status(result, fail_code=503)


@app.route('/api/editor/map/save_current', methods=['POST'])
def save_to_current_map():
    node, err = _node()
    if err:
        return err

    result = node.save_current_editor_map()
    return _status(result, fail_code=503)


@app.route('/api/editor/map/reload', methods=['POST'])
def reload_current_map():
    node, err = _node()
    if err:
        return err

    result = node.reload_editor_map()
    return _status(result, fail_code=503)
@app.route('/api/map/upload', methods=['POST'])
def upload_map():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    map_name = str(data.get('map_name', '')).strip()
    yaml_content = str(data.get('yaml_content', ''))
    pgm_base64 = str(data.get('pgm_base64', ''))

    if not map_name or not yaml_content or not pgm_base64:
        return jsonify({'ok': False, 'message': 'map_name, yaml_content and pgm_base64 are required'}), 400

    result = node.upload_map(map_name, yaml_content, pgm_base64)
    status = 200 if result.get('ok') else 500
    return jsonify(result), status


def _dotted_overlay_path(node) -> Optional[str]:
    """Return path to dotted overlay JSON for the active map, or None if unavailable."""
    try:
        active = node.get_active_map()
        yaml_path = active.get('map_yaml_path') or active.get('map_path')
        if not yaml_path:
            return None
        stem = os.path.splitext(yaml_path)[0]
        return stem + '_dotted_overlay.json'
    except Exception:
        return None


@app.route('/api/map/dotted_overlay', methods=['GET'])
def get_dotted_overlay():
    node, err = _node()
    if err:
        return err

    overlay_path = _dotted_overlay_path(node)
    if overlay_path is None:
        return jsonify({'ok': True, 'segments': []}), 200

    if not os.path.isfile(overlay_path):
        return jsonify({'ok': True, 'segments': []}), 200

    try:
        with open(overlay_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        segments = data.get('segments', []) if isinstance(data, dict) else []
        return jsonify({'ok': True, 'segments': segments}), 200
    except Exception as exc:
        app.logger.warning('Failed to read dotted overlay: %s', exc)
        return jsonify({'ok': False, 'message': str(exc)}), 500


@app.route('/api/map/dotted_overlay', methods=['POST'])
def save_dotted_overlay():
    node, err = _node()
    if err:
        return err

    overlay_path = _dotted_overlay_path(node)
    if overlay_path is None:
        return jsonify({'ok': False, 'message': 'No active map'}), 400

    body = _json_body()
    segments = body.get('segments', [])
    if not isinstance(segments, list):
        return jsonify({'ok': False, 'message': 'segments must be a list'}), 400

    try:
        with open(overlay_path, 'w', encoding='utf-8') as f:
            json.dump({'segments': segments}, f)
        return jsonify({'ok': True, 'count': len(segments)}), 200
    except Exception as exc:
        app.logger.warning('Failed to write dotted overlay: %s', exc)
        return jsonify({'ok': False, 'message': str(exc)}), 500


@app.route('/api/map/set_active', methods=['POST'])
def set_active_map():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    map_yaml_path = data.get('map_yaml_path') or data.get('map_path')
    if not map_yaml_path:
        return jsonify({'ok': False, 'message': 'map_yaml_path is required'}), 400

    result = node.set_active_map(str(map_yaml_path))
    status = 200 if result.get('ok') else 500
    return jsonify(result), status


@app.route('/api/map/get_active', methods=['GET'])
def get_active_map():
    node, err = _node()
    if err:
        return err

    result = node.get_active_map()
    status = 200 if result.get('ok') else 500
    return jsonify(result), status


@app.route('/api/map/list', methods=['GET'])
def list_maps():
    node, err = _node()
    if err:
        return err

    result = node.list_maps()
    return jsonify(result), 200


@app.route('/api/map/reload', methods=['POST'])
def reload_active_map():
    node, err = _node()
    if err:
        return err

    result = node.reload_active_map()
    status = 200 if result.get('ok') else 500
    return jsonify(result), status


@app.route('/api/map/download/<filename>')
def download_map_file(filename):
    node, err = _node()
    if err:
        return err

    for base in (node.maps_dir, node.downloads_dir):
        file_path = os.path.join(base, filename)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return send_from_directory(base, filename, as_attachment=True)

    return jsonify({'ok': False, 'message': 'File not found'}), 404


@app.route('/api/stack/mode', methods=['POST'])
def set_stack_mode():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    mode = str(data.get('mode', '')).strip().lower()
    if mode not in {'nav', 'slam', 'stop'}:
        return jsonify({'ok': False, 'message': 'Invalid mode'}), 400

    result = node.set_stack_mode(mode)
    code = 200 if result.get('ok') else 503
    return jsonify(result), code


@app.route('/api/stack/status', methods=['GET'])
def get_stack_status():
    node, err = _node()
    if err:
        return err

    result = node.get_stack_status()
    code = 200 if result.get('ok') else 503
    return jsonify(result), code


@app.route('/api/stack/shutdown', methods=['POST'])
def shutdown_stack():
    node, err = _node()
    if err:
        return err

    result = node.shutdown_stack()
    code = 200 if result.get('ok') else 503
    return jsonify(result), code


@app.route('/api/slam/save_map', methods=['POST'])
def save_slam_map():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    map_name = str(data.get('map_name', '') or '').strip()
    set_active = bool(data.get('set_active', False))

    result = node.save_slam_map(map_name, set_active=set_active)
    code = 200 if result.get('ok') else 500
    return jsonify(result), code


@app.route('/api/slam/serialize', methods=['POST'])
def slam_serialize():
    node, err = _node()
    if err:
        return err
    data = _json_body()
    # Accept 'file' (from stitch popup) or 'filename'
    filename = str(data.get('filename') or data.get('file') or '').strip()
    result = node.serialize_pose_graph(filename)
    return jsonify(result), (200 if result.get('ok') else 500)


@app.route('/api/slam/deserialize', methods=['POST'])
def slam_deserialize():
    node, err = _node()
    if err:
        return err
    data = _json_body()
    # Accept 'file' (from stitch popup) or 'filename'
    filename = str(data.get('filename') or data.get('file') or '').strip()
    result = node.deserialize_pose_graph(filename)
    return jsonify(result), (200 if result.get('ok') else 500)


@app.route('/api/stack/health', methods=['GET'])
def stack_health():
    node, err = _node()
    if err:
        return err

    stack = node.get_stack_status()
    diagnostics = node.nav2_diagnostics()
    return jsonify(
        {
            'stack': stack,
            'diagnostics': diagnostics,
            'ok': bool(stack.get('ok')),
        }
    )


@app.route('/api/auto_relocate', methods=['POST'])
def auto_relocate():
    node, err = _node()
    if err:
        return err

    result = node.auto_relocate()
    code = 200 if result.get('ok') else 503
    return jsonify(result), code


@app.route('/api/shelf/trigger', methods=['POST'])
def trigger_shelf_detection():
    node, err = _node()
    if err:
        return err

    result = node.trigger_shelf_detection()
    code = 200 if result.get('ok') else 503
    return jsonify(result), code


@app.route('/api/shelf/enable', methods=['POST'])
def set_shelf_detector_enabled():
    node, err = _node()
    if err:
        return err

    payload = request.get_json(silent=True) or {}
    enabled = bool(payload.get('enabled', False))
    result = node.set_shelf_detector_enabled(enabled)
    code = 200 if result.get('ok') else 503
    return jsonify(result), code


@app.route('/api/shelf/status', methods=['GET'])
def get_shelf_detection_status():
    node, err = _node()
    if err:
        return err

    result = node.get_shelf_detection_status()
    code = 200 if result.get('ok') else 503
    return jsonify(result), code


@app.route('/api/settings/topics', methods=['GET'])
def get_available_topics():
    node, err = _node()
    if err:
        return err

    topics = []
    for topic_name, type_list in node.get_topic_names_and_types():
        if type_list:
            topics.append({'topic': topic_name, 'type': type_list[0]})

    topics.sort(key=lambda x: x['topic'])
    return jsonify({'ok': True, 'topics': topics})


@app.route('/api/settings/robot_profiles', methods=['GET'])
def list_robot_profiles():
    node, err = _node()
    if err:
        return err

    result = node.list_robot_profiles()
    return _status(result, fail_code=503)


@app.route('/api/settings/robot_profiles/select', methods=['POST'])
def select_robot_profile():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    apply_map = bool(data.get('apply_map', True))
    result = node.select_robot_profile(robot_id, apply_map=apply_map)
    return _status(result, fail_code=503)


@app.route('/api/settings/robot_profiles/clone', methods=['POST'])
def clone_robot_profile():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    source_robot_id = str(data.get('source_robot_id', '') or '').strip()
    if not source_robot_id:
        return jsonify({'ok': False, 'message': 'source_robot_id is required'}), 400

    overrides = data.get('overrides', {})
    if not isinstance(overrides, dict):
        overrides = {}

    activate = bool(data.get('activate', True))
    result = node.clone_robot_profile(source_robot_id, overrides, activate=activate)
    return _status(result, fail_code=503)


@app.route('/api/settings/robot_profiles/create', methods=['POST'])
def create_robot_profile():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    overrides = data.get('overrides', {})
    if not isinstance(overrides, dict):
        overrides = {}

    if not overrides:
        candidate_keys = (
            'robot_id',
            'display_name',
            'base_robot_id',
            'namespace',
            'base_frame',
            'urdf_source',
            'nav2_params_file',
            'map_association',
            'sensor_frames',
            'ui_toggles',
            'topics',
            'mappings',
        )
        overrides = {
            key: data.get(key)
            for key in candidate_keys
            if key in data
        }

    activate = bool(data.get('activate', True))
    result = node.create_robot_profile(overrides=overrides, activate=activate)
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/delete', methods=['POST'])
def delete_robot_profile():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    result = node.delete_robot_profile(robot_id)
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/robot_editor', methods=['GET'])
def get_robot_profile_robot_editor():
    node, err = _node()
    if err:
        return err

    role = next_ops.role_from_request(request, session)

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    reload_from_urdf_raw = str(request.args.get('reload_from_urdf', 'false') or 'false').strip().lower()
    reload_from_urdf = reload_from_urdf_raw in {'1', 'true', 'yes', 'on'}
    
    import_active_raw = str(request.args.get('import_active', 'false') or 'false').strip().lower()
    import_active = import_active_raw in {'1', 'true', 'yes', 'on'}
    if (reload_from_urdf or import_active) and not next_ops.has_permission(role, 'import:run'):
        return jsonify(next_ops.permission_error(role, 'import:run')), 403
    
    result = node.get_profile_robot_editor(robot_id=robot_id, reload_from_urdf=reload_from_urdf, import_active=import_active)
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/robot_editor/save', methods=['POST'])
def save_robot_profile_robot_editor():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    profile_fields = data.get('profile_fields', {})
    if not isinstance(profile_fields, dict):
        profile_fields = {}

    robot_builder = data.get('robot_builder', {})
    if not isinstance(robot_builder, dict):
        robot_builder = {}

    safety_overrides = data.get('safety_overrides', None)
    if safety_overrides is not None and not isinstance(safety_overrides, dict):
        safety_overrides = {}

    compile_urdf = bool(data.get('compile_urdf', True))
    apply_runtime = bool(data.get('apply_runtime', False))
    result = node.save_profile_robot_editor(
        robot_id=robot_id,
        profile_fields=profile_fields,
        robot_builder=robot_builder,
        safety_overrides=safety_overrides,
        compile_urdf=compile_urdf,
        apply_runtime=apply_runtime,
    )
    if result.get('ok'):
        next_ops.record_event(
            severity='info',
            source='ui',
            message=f"Saved profile configuration: {robot_id}",
            obj=robot_id,
            reason='User editor save',
        )
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/deployment', methods=['GET'])
def get_robot_profile_deployment_status():
    node, err = _node()
    if err:
        return err

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    result = node.get_profile_config_deploy_status(robot_id=robot_id)
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/snapshot', methods=['GET'])
def get_robot_profile_snapshot():
    node, err = _node()
    if err:
        return err

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    snapshot_id = str(request.args.get('snapshot_id', '') or '').strip()
    if not robot_id or not snapshot_id:
        return jsonify({'ok': False, 'message': 'robot_id and snapshot_id are required'}), 400

    result = node.get_profile_config_snapshot_details(robot_id=robot_id, snapshot_id=snapshot_id)
    return _status(result, fail_code=404)


@app.route('/api/settings/robot_profiles/generated_bundle', methods=['GET'])
def get_generated_bundle():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'generated:read'):
        return jsonify(next_ops.permission_error(role, 'generated:read')), 403
    
    node, err = _node()
    if err:
        return err

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    result = node.get_profile_robot_editor(robot_id=robot_id)
    if not result.get('ok'):
        return jsonify(result), 400
    
    profile = result.get('profile', {})
    robot_builder = result.get('robot_builder', {})
    return jsonify(next_ops.generated_bundle(profile, robot_builder))

@app.route('/api/settings/robot_profiles/deployment/push', methods=['POST'])
def push_robot_profile_deployment():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'deploy:push'):
        return jsonify(next_ops.permission_error(role, 'deploy:push')), 403

    editor_res = node.get_profile_robot_editor(robot_id=robot_id)
    if editor_res.get('ok'):
        validation = next_ops.validate_robot_model(
            editor_res.get('profile', {}),
            editor_res.get('robot_builder', {})
        )
        if not validation.get('ready_to_deploy'):
            return jsonify({
                'ok': False,
                'message': 'Validation errors block deployment push',
                'validation': validation
            }), 400

    result = node.push_profile_config_to_robot(robot_id=robot_id)
    if result.get('ok'):
        next_ops.record_event(
            severity='info',
            source='ui',
            message=f"Deployed profile configuration to robot: {robot_id}",
            obj=robot_id,
            reason='User requested deployment push',
            required_action='Verify robot behavior after configuration update'
        )
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/deployment/rollback', methods=['POST'])
def rollback_robot_profile_deployment():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'deploy:rollback'):
        return jsonify(next_ops.permission_error(role, 'deploy:rollback')), 403

    snapshot_id = str(data.get('snapshot_id', '') or '').strip()
    result = node.rollback_profile_config_on_robot(robot_id=robot_id, snapshot_id=snapshot_id)
    if result.get('ok'):
        next_ops.record_event(
            severity='warning',
            source='ui',
            message=f"Rolled back profile configuration for {robot_id} to snapshot {snapshot_id}",
            obj=robot_id,
            reason='User requested deployment rollback',
            action_required='Verify stability on previous configuration'
        )
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/urdf/upload', methods=['POST'])
def upload_robot_profile_urdf():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'import:run'):
        return jsonify(next_ops.permission_error(role, 'import:run')), 403

    node, err = _node()
    if err:
        return err

    robot_id = str(request.form.get('robot_id', '') or '').strip()
    if not robot_id:
        data = _json_body()
        robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    apply_runtime_raw = str(request.form.get('apply_runtime', 'true') or 'true').strip().lower()
    apply_runtime = apply_runtime_raw in {'1', 'true', 'yes', 'on'}

    upload_file = request.files.get('file')
    if upload_file is None:
        return jsonify({'ok': False, 'message': 'file is required'}), 400

    filename = str(upload_file.filename or '').strip()
    if not filename:
        return jsonify({'ok': False, 'message': 'uploaded filename is required'}), 400

    content = upload_file.read()

    # Handle YAML/JSON profile bundle imports
    fn_lower = filename.lower()
    if fn_lower.endswith(('.yaml', '.yml', '.json')):
        try:
            import yaml
            if fn_lower.endswith('.json'):
                data = json.loads(content.decode('utf-8'))
            else:
                data = yaml.safe_load(content.decode('utf-8'))
            
            if not isinstance(data, dict):
                return jsonify({'ok': False, 'message': 'Invalid profile format'}), 400
                
            # If it's a full bundle, extract components
            profile_fields = data.get('profile', data.get('profile_fields', {}))
            robot_builder = data.get('robot_builder', data.get('model', {}))
            
            if not profile_fields and not robot_builder:
                # Assume the whole file is the profile/model mixed or just fields
                profile_fields = data
            
            result = node.save_profile_robot_editor(
                robot_id=robot_id,
                profile_fields=profile_fields,
                robot_builder=robot_builder,
                compile_urdf=True,
                apply_runtime=apply_runtime
            )
            return _status(result, fail_code=400)
        except Exception as exc:
            return jsonify({'ok': False, 'message': f'Failed to parse profile: {exc}'}), 400

    result = node.upload_profile_urdf(
        robot_id=robot_id,
        filename=filename,
        content=content,
        apply_runtime=apply_runtime,
    )
    code = 200 if result.get('ok') else 400
    return jsonify(result), code


@app.route('/api/settings/robot_profiles/urdf/visualization', methods=['GET'])
def get_robot_profile_urdf_visualization():
    node, err = _node()
    if err:
        return err

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    include_tf_raw = str(request.args.get('include_tf', 'true') or 'true').strip().lower()
    include_tf = include_tf_raw in {'1', 'true', 'yes', 'on'}

    result = node.get_profile_urdf_visualization(robot_id=robot_id, include_tf=include_tf)
    return _status(result, fail_code=200)


@app.route('/api/settings/robot_profiles/urdf/source', methods=['GET'])
def get_robot_profile_urdf_source():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'import:run'):
        return jsonify(next_ops.permission_error(role, 'import:run')), 403

    node, err = _node()
    if err:
        return err

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    result = node.get_profile_urdf_source_editor(robot_id=robot_id)
    return _status(result, fail_code=200)


@app.route('/api/settings/robot_profiles/urdf/source', methods=['POST'])
def save_robot_profile_urdf_source():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'import:run'):
        return jsonify(next_ops.permission_error(role, 'import:run')), 403

    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    source_path = str(data.get('source_path', '') or '').strip()
    source_text = str(data.get('source_text', '') or '')
    apply_runtime = bool(data.get('apply_runtime', True))

    result = node.save_profile_urdf_source_editor(
        robot_id=robot_id,
        source_path=source_path,
        source_text=source_text,
        apply_runtime=apply_runtime,
    )
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/footprint', methods=['GET'])
def get_robot_profile_footprint():
    node, err = _node()
    if err:
        return err

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    include_runtime_raw = str(request.args.get('include_runtime', 'true') or 'true').strip().lower()
    include_runtime = include_runtime_raw in {'1', 'true', 'yes', 'on'}

    result = node.get_profile_nav2_footprint(robot_id=robot_id, include_runtime=include_runtime)
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/footprint', methods=['POST'])
def set_robot_profile_footprint():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    mode = data.get('mode', 'circle')
    robot_radius = data.get('robot_radius', 0.30)
    footprint = data.get('footprint', [])
    footprint_padding = data.get('footprint_padding', 0.0)
    apply_runtime = bool(data.get('apply_runtime', True))

    result = node.set_profile_nav2_footprint(
        robot_id=robot_id,
        mode=mode,
        robot_radius=robot_radius,
        footprint=footprint,
        footprint_padding=footprint_padding,
        apply_runtime=apply_runtime,
    )
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/nav2_parameters', methods=['GET'])
def get_robot_profile_nav2_parameters():
    node, err = _node()
    if err:
        return err
    role = next_ops.role_from_request(request, session)

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    include_runtime_raw = str(request.args.get('include_runtime', 'true') or 'true').strip().lower()
    include_runtime = include_runtime_raw in {'1', 'true', 'yes', 'on'}

    result = node.get_profile_nav2_parameter_editor(robot_id=robot_id, include_runtime=include_runtime)
    if isinstance(result, dict):
        result['advanced_available'] = bool(next_ops.has_permission(role, 'import:run'))
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/nav2_parameters/apply', methods=['POST'])
def apply_robot_profile_nav2_parameters():
    node, err = _node()
    if err:
        return err
    role = next_ops.role_from_request(request, session)

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    apply_mode = str(data.get('apply_mode', 'offline') or 'offline').strip().lower()
    curated_updates = data.get('curated_updates', {})
    advanced_updates = data.get('advanced_updates', {})
    restart_nav = bool(data.get('restart_nav', False))
    has_advanced_updates = bool(advanced_updates) and advanced_updates not in ({}, [])
    if has_advanced_updates and not next_ops.has_permission(role, 'import:run'):
        return jsonify(next_ops.permission_error(role, 'import:run')), 403

    result = node.apply_profile_nav2_parameter_editor(
        robot_id=robot_id,
        apply_mode=apply_mode,
        curated_updates=curated_updates,
        advanced_updates=advanced_updates,
        restart_nav=restart_nav,
    )
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/bringup_wizard', methods=['GET'])
def get_robot_profile_bringup_wizard():
    node, err = _node()
    if err:
        return err

    robot_id = str(request.args.get('robot_id', '') or '').strip()
    result = node.get_profile_bringup_report(robot_id=robot_id)
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/bringup_wizard/run', methods=['POST'])
def run_robot_profile_bringup_wizard():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    run_goal_cancel = bool(data.get('run_goal_cancel', True))
    use_initial_pose = bool(data.get('use_initial_pose', False))
    initial_pose_raw = data.get('initial_pose', {})
    initial_pose = initial_pose_raw if (use_initial_pose and isinstance(initial_pose_raw, dict)) else None

    result = node.run_profile_bringup_smoke_wizard(
        robot_id=robot_id,
        initial_pose=initial_pose,
        run_goal_cancel=run_goal_cancel,
    )
    return _status(result, fail_code=400)


@app.route('/api/settings/robot_profiles/production', methods=['POST'])
def set_robot_profile_production():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    robot_id = str(data.get('robot_id', '') or '').strip()
    if not robot_id:
        return jsonify({'ok': False, 'message': 'robot_id is required'}), 400

    production = bool(data.get('production', False))
    note = str(data.get('note', '') or '').strip()
    result = node.set_profile_production_status(robot_id=robot_id, production=production, note=note)
    return _status(result, fail_code=400)


@app.route('/api/device_config/restart_component', methods=['POST'])
def restart_device_config_component():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    component = str(data.get('component', '') or '').strip()
    if not component:
        return jsonify({'ok': False, 'message': 'component is required'}), 400

    enabled = _as_bool(data.get('enabled', True), True)
    result = node.restart_device_config_component(component=component, enabled=enabled)
    return _status(result, fail_code=400)


@app.route('/api/settings/update_firmware', methods=['POST'])
def run_firmware_update():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'test_mode:run'):
        return jsonify(next_ops.permission_error(role, 'test_mode:run')), 403

    command = str(os.getenv('NEXT_FIRMWARE_UPDATE_COMMAND', '') or '').strip()
    if not command:
        return jsonify({
            'ok': False,
            'message': 'Firmware update command is not configured. Set NEXT_FIRMWARE_UPDATE_COMMAND on the host.',
        }), 400

    try:
        subprocess.Popen(
            ['/bin/bash', '-lc', command],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        next_ops.record_event(
            severity='warning',
            source='settings',
            message='Firmware update command launched',
            reason='User requested firmware update',
            details={'command': command},
        )
        return jsonify({'ok': True, 'message': 'Firmware update command launched'})
    except Exception as exc:
        return jsonify({'ok': False, 'message': f'Failed to launch firmware update: {exc}'}), 500



@app.route('/api/settings/mappings', methods=['GET'])
def get_ui_mappings():
    node, err = _node()
    if err:
        return err

    result = node.get_ui_mappings()
    return _status(result, fail_code=503)


@app.route('/api/settings/mappings', methods=['POST'])
def save_ui_mappings():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    mappings = data.get('mappings', {})
    topics = data.get('topics', {})
    confirmed = bool(data.get('confirmed', False))

    if isinstance(mappings, dict) and ('mappings' in mappings or 'topics' in mappings) and ('topics' not in data):
        payload = mappings
    else:
        payload = {
            'mappings': mappings if isinstance(mappings, dict) else {},
            'topics': topics if isinstance(topics, dict) else {},
        }

    result = node.save_ui_mappings(payload, confirmed=confirmed)
    return _status(result, fail_code=503)


@app.route('/api/actions', methods=['GET'])
def get_available_actions():
    return jsonify({'ok': True, 'actions': list_actions_payload()})


@app.route('/api/settings/validate', methods=['POST'])
def validate_topic_mapping():
    node, err = _node()
    if err:
        return err

    data = _json_body()
    topic = data.get('topic')
    if not topic:
        return jsonify({'ok': False, 'status': 'INVALID', 'message': 'Topic required'}), 400

    topics = {name for name, _types in node.get_topic_names_and_types()}
    if topic not in topics:
        return jsonify({'ok': True, 'status': 'NO_TOPIC', 'message': f'Topic {topic} not found'})

    return jsonify({'ok': True, 'status': 'OK', 'message': 'Topic available'})


@app.route('/api/settings/preview', methods=['GET'])
def get_settings_preview():
    node, err = _node()
    if err:
        return err

    pose_snapshot = node.get_robot_pose()
    pose = pose_snapshot.get('pose') if isinstance(pose_snapshot, dict) else None
    preview = {
        'battery': '—',
        'speed': '—',
        'localization': f'{node.localization_confidence:.0f}%',
        'pose': 'OK' if pose else 'NO DATA',
    }
    return jsonify({'ok': True, 'preview': preview})


@app.route('/api/next/permissions', methods=['GET'])
def next_get_permissions():
    role = next_ops.role_from_request(request, session)
    return jsonify(next_ops.permission_payload(role))


@app.route('/api/next/network_health', methods=['GET'])
def next_network_health():
    host = request.args.get('host', 'localhost')
    return jsonify(next_ops.network_health(host))


@app.route('/api/next/model/validate', methods=['POST'])
def next_validate_model():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'validation:run'):
        return jsonify(next_ops.permission_error(role, 'validation:run')), 403

    data = _json_body()
    profile = data.get('profile', {})
    robot_builder = data.get('robot_builder', {})
    validation = next_ops.validate_robot_model(profile, robot_builder)
    return jsonify(validation)


@app.route('/api/next/events', methods=['GET', 'POST'])
def next_events():
    role = next_ops.role_from_request(request, session)
    if request.method == 'POST':
        if not next_ops.has_permission(role, 'events:write'):
            return jsonify(next_ops.permission_error(role, 'events:write')), 403
        data = _json_body()
        record = next_ops.record_event(
            severity=data.get('severity', 'info'),
            source=data.get('source', 'ui'),
            message=data.get('message', ''),
            obj=data.get('object', ''),
            field=data.get('field', ''),
            reason=data.get('reason', ''),
            action_required=data.get('required_action', ''),
        )
        return jsonify({"ok": True, "event": record})
    else:
        if not next_ops.has_permission(role, 'events:read'):
            return jsonify(next_ops.permission_error(role, 'events:read')), 403
        limit = request.args.get('limit', 100, type=int)
        severity = request.args.get('severity', '')
        return jsonify(next_ops.list_events(limit=limit, severity=severity))


@app.route('/api/settings/shortcuts', methods=['GET', 'POST', 'DELETE'])
def next_shortcuts():
    if request.method == 'GET':
        return jsonify(next_ops.load_shortcuts())
    elif request.method == 'POST':
        data = _json_body()
        if 'shortcuts' not in data:
            return jsonify({'ok': False, 'message': 'Missing "shortcuts" key'}), 400
        return jsonify(next_ops.save_shortcuts(data['shortcuts']))
    elif request.method == 'DELETE':
        return jsonify(next_ops.save_shortcuts(next_ops.DEFAULT_SHORTCUTS.copy()))


@app.route('/api/next/login', methods=['POST'])
def next_login():
    data = _json_body()
    username = str(data.get('username', '')).strip()
    password = str(data.get('password', '')).strip()
    
    success, role = next_ops.verify_login(username, password)
    if success:
        session[DEV_AUTH_SESSION_KEY] = True
        session['next_user_name'] = username
        session['next_user_role'] = role
        return jsonify({'ok': True, 'username': username, 'role': role})
    
    return jsonify({'ok': False, 'message': 'Invalid username or password'}), 401


@app.route('/api/next/logout', methods=['POST'])
def next_logout():
    session.pop(DEV_AUTH_SESSION_KEY, None)
    session.pop('next_user_name', None)
    session.pop('next_user_role', None)
    return jsonify({'ok': True})


@app.route('/api/next/users', methods=['GET', 'POST', 'DELETE'])
def next_users():
    role = next_ops.role_from_request(request, session)
    if role != 'admin':
        return jsonify(next_ops.permission_error(role, 'admin:users')), 403

    if request.method == 'GET':
        users = next_ops.get_users()
        # Strip password hashes before returning
        safe_users = {u: d['role'] for u, d in users.items()}
        return jsonify({'ok': True, 'users': safe_users})
    elif request.method == 'POST':
        data = _json_body()
        username = str(data.get('username', '')).strip()
        user_role = str(data.get('role', 'viewer')).strip()
        password = data.get('password') # Optional update
        if not username:
            return jsonify({'ok': False, 'message': 'username required'}), 400
        return jsonify(next_ops.set_user_role(username, user_role, password=password))
    elif request.method == 'DELETE':
        data = _json_body()
        username = str(data.get('username', '')).strip()
        if not username:
            return jsonify({'ok': False, 'message': 'username required'}), 400
        return jsonify(next_ops.delete_user(username))


@app.route('/api/next/test_mode/indicators', methods=['POST'])
def next_test_indicators():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'test_mode:run'):
        return jsonify(next_ops.permission_error(role, 'test_mode:run')), 403

    data = _json_body()
    pattern = str(data.get('pattern', 'blink')).strip()
    color = str(data.get('color', '#ffffff')).strip()
    
    # In a real system, this would call a ROS service to set LED patterns.
    # For now, we'll log the event.
    next_ops.record_event(
        severity='info',
        source='test_mode',
        message=f"Hardware test: Indicators set to pattern='{pattern}', color='{color}'",
        reason='User hardware verification',
    )
    return jsonify({'ok': True, 'message': f'Indicator test "{pattern}" started'})


@app.route('/api/next/test_mode/motors', methods=['POST'])
def next_test_motors():
    role = next_ops.role_from_request(request, session)
    if not next_ops.has_permission(role, 'test_mode:run'):
        return jsonify(next_ops.permission_error(role, 'test_mode:run')), 403

    data = _json_body()
    action = str(data.get('action', 'nudge')).strip()
    
    next_ops.record_event(
        severity='warning',
        source='test_mode',
        message=f"Hardware test: Motors action='{action}' triggered",
        reason='User hardware verification',
    )
    return jsonify({'ok': True, 'message': f'Motor test "{action}" triggered'})


def run_flask():
    # Run threaded to avoid endpoint head-of-line blocking under high-frequency UI polling.
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False, threaded=True)


def main(args=None):
    global ros_node

    rclpy.init(args=args)
    executor = MultiThreadedExecutor(num_threads=4)

    ros_node = RosBridge()
    executor.add_node(ros_node)

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    ros_node.get_logger().info('Web UI available at: http://localhost:5000')

    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            ros_node.destroy_node()
        except Exception:
            pass
        try:
            rclpy.shutdown()
        except Exception:
            pass


if __name__ == '__main__':
    main()
