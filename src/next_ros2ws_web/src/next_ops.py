"""Next-style operations shared by the web UI."""
from __future__ import annotations

import hashlib
import html
import json
import os
import re
import socket
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence


ROLE_PERMISSIONS: Dict[str, Sequence[str]] = {
    "viewer": (
        "model:read",
        "validation:read",
        "events:read",
        "network:read",
    ),
    "editor": (
        "model:read",
        "model:write",
        "validation:read",
        "validation:run",
        "events:read",
        "network:read",
    ),
    "engineer": (
        "model:read",
        "model:write",
        "validation:read",
        "validation:run",
        "generated:read",
        "import:run",
        "export:run",
        "events:read",
        "events:write",
        "network:read",
    ),
    "admin": (
        "model:read",
        "model:write",
        "validation:read",
        "validation:run",
        "generated:read",
        "import:run",
        "export:run",
        "deploy:push",
        "deploy:rollback",
        "events:read",
        "events:write",
        "network:read",
        "test_mode:run",
    ),
    "service": (
        "model:read",
        "model:write",
        "validation:read",
        "validation:run",
        "generated:read",
        "import:run",
        "export:run",
        "schema:migrate",
        "events:read",
        "events:write",
        "network:read",
        "test_mode:run",
    ),
}


ROS_TOPIC_RE = re.compile(r"^/?[A-Za-z_][A-Za-z0-9_]*(/[A-Za-z_][A-Za-z0-9_]*)*$")
ROS_FRAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
IP_OR_HOST_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")


def normalized_role(raw: Any) -> str:
    """Return a supported role name."""
    role = str(raw or "").strip().lower()
    return role if role in ROLE_PERMISSIONS else "viewer"


def _get_db() -> Any:
    from next_ros2ws_core.db_manager import DatabaseManager
    # Assume the default db path
    return DatabaseManager()

def get_users() -> Dict[str, Dict[str, str]]:
    """Return a dictionary of username -> {role, password_hash}."""
    try:
        db = _get_db()
        users = db.get_users()
        if not users:
            # Create default admin user if none exist
            default_hash = hashlib.sha256("admin".encode()).hexdigest()
            db.set_user("admin", "admin", default_hash)
            return {"admin": {"role": "admin", "password_hash": default_hash}}
        return users
    except Exception as e:
        print(f"Error getting users from DB: {e}")
        return {"admin": {"role": "admin", "password_hash": hashlib.sha256("admin".encode()).hexdigest()}}


def save_users(users: Dict[str, Any]) -> Dict[str, Any]:
    """Deprecated: use set_user/delete_user instead."""
    return {"ok": True, "users": users}


def set_user_role(username: str, role: str, password: Optional[str] = None) -> Dict[str, Any]:
    """Create or update a user's role and optionally password."""
    if not username:
        return {"ok": False, "message": "Username is required"}
    try:
        db = _get_db()
        users = db.get_users()
        current_hash = hashlib.sha256(username.encode()).hexdigest()
        if username in users:
            current_hash = users[username]["password_hash"]
        
        if password:
            current_hash = hashlib.sha256(password.encode()).hexdigest()
            
        db.set_user(username, normalized_role(role), current_hash)
        return {"ok": True, "users": db.get_users()}
    except Exception as e:
        return {"ok": False, "message": str(e)}


def delete_user(username: str) -> Dict[str, Any]:
    """Delete a user."""
    try:
        db = _get_db()
        users = db.get_users()
        if username in users:
            if users[username]["role"] == "admin" and len([u for u, d in users.items() if d["role"] == "admin"]) <= 1:
                return {"ok": False, "message": "Cannot delete the last admin user."}
            db.delete_user(username)
            return {"ok": True, "users": db.get_users()}
        return {"ok": True, "users": users}
    except Exception as e:
        return {"ok": False, "message": str(e)}


def verify_login(username: str, password: str) -> Tuple[bool, Optional[str]]:
    """Verify credentials and return (success, role)."""
    users = get_users()
    if username in users:
        stored_hash = users[username].get("password_hash")
        if stored_hash == hashlib.sha256(password.encode()).hexdigest():
            return True, users[username]["role"]
    return False, None


def role_from_request(req: Any, session: Optional[Dict[str, Any]] = None) -> str:
    """Resolve the effective UI role from request, session, or environment."""
    username = ""
    header_role = ""
    try:
        username = str(req.headers.get("X-NEXT-USER", "") or "").strip()
        header_role = str(req.headers.get("X-NEXT-ROLE", "") or "").strip()
    except Exception:
        pass

    if username:
        users = get_users()
        if username in users:
            return users[username]["role"]

    session_role = ""
    if isinstance(session, dict):
        session_role = str(session.get("next_user_role", "") or "").strip()

    env_role = os.getenv("NEXT_WEB_UI_ROLE", "admin")
    return normalized_role(header_role or session_role or env_role)


def permission_payload(role: str) -> Dict[str, Any]:
    """Build the permission response for a role."""
    clean_role = normalized_role(role)
    permissions = sorted(set(ROLE_PERMISSIONS.get(clean_role, ())))
    return {
        "ok": True,
        "role": clean_role,
        "permissions": permissions,
        "can_read_model": "model:read" in permissions,
        "can_edit_model": "model:write" in permissions,
        "can_validate": "validation:run" in permissions,
        "can_export": "export:run" in permissions,
        "can_import": "import:run" in permissions,
        "can_push": "deploy:push" in permissions,
        "can_rollback": "deploy:rollback" in permissions,
        "can_test_hardware": "test_mode:run" in permissions,
        "can_view_generated": "generated:read" in permissions,
        "can_edit_advanced_config": "import:run" in permissions,
    }


def has_permission(role: str, permission: str) -> bool:
    """Return whether role can perform permission."""
    return permission in set(ROLE_PERMISSIONS.get(normalized_role(role), ()))


def permission_error(role: str, permission: str) -> Dict[str, Any]:
    """Return a structured permission failure payload."""
    return {
        "ok": False,
        "code": "permission_denied",
        "role": normalized_role(role),
        "permission": str(permission),
        "message": (
            f'Role "{normalized_role(role)}" is not allowed to perform '
            f'"{permission}". Ask an admin or switch to a permitted role.'
        ),
    }


def event_store_path() -> Path:
    """Return the local structured event log path."""
    root = Path(os.getenv("NEXT_NEXT_STATE_DIR", "~/.next_ros2ws/next")).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root / "events.jsonl"


def record_event(
    *,
    severity: str,
    source: str,
    message: str,
    action_required: str = "",
    robot_id: str = "",
    obj: str = "",
    field: str = "",
    reason: str = "",
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Append a structured operational event and return it."""
    event = {
        "id": hashlib.sha1(f"{time.time_ns()}:{source}:{message}".encode()).hexdigest()[:16],
        "timestamp": int(time.time()),
        "severity": str(severity or "info").strip().lower() or "info",
        "source": str(source or "web").strip() or "web",
        "robot_id": str(robot_id or "").strip(),
        "object": str(obj or "").strip(),
        "field": str(field or "").strip(),
        "message": str(message or "").strip(),
        "reason": str(reason or "").strip(),
        "action_required": str(action_required or "").strip(),
        "details": details if isinstance(details, dict) else {},
    }
    path = event_store_path()
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event, sort_keys=True) + "\n")
    return event


def list_events(limit: int = 100, severity: str = "") -> Dict[str, Any]:
    """List recent structured operational events."""
    path = event_store_path()
    requested = max(1, min(int(limit or 100), 1000))
    severity_filter = str(severity or "").strip().lower()
    events: List[Dict[str, Any]] = []
    if path.exists():
        with path.open("r", encoding="utf-8") as stream:
            for line in stream:
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                if severity_filter and str(payload.get("severity", "")).lower() != severity_filter:
                    continue
                events.append(payload)
    return {"ok": True, "events": events[-requested:], "count": min(len(events), requested)}


def shortcuts_store_path() -> Path:
    """Return the file path for persisting user shortcut preferences."""
    root = Path(os.getenv("NEXT_NEXT_STATE_DIR", "~/.next_ros2ws/next")).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root / "shortcuts.json"


DEFAULT_SHORTCUTS = {
    "save": "ctrl+s",
    "validate": "ctrl+enter",
    "search": "ctrl+f",
    "cancel": "escape",
    "zoom_in": "ctrl+=",
    "zoom_out": "ctrl+-",
    "pan": "space",
    "map_view": "v",
    "map_zones": "z",
    "map_paths": "p",
    "estop": "e",
}


def load_shortcuts() -> Dict[str, Any]:
    """Load persistent shortcut preferences."""
    path = shortcuts_store_path()
    if not path.exists():
        return {"ok": True, "shortcuts": DEFAULT_SHORTCUTS.copy()}
    try:
        with path.open("r", encoding="utf-8") as stream:
            data = json.load(stream)
            shortcuts = {**DEFAULT_SHORTCUTS, **(data if isinstance(data, dict) else {})}
            return {"ok": True, "shortcuts": shortcuts}
    except Exception as exc:
        return {"ok": False, "message": str(exc), "shortcuts": DEFAULT_SHORTCUTS.copy()}


def save_shortcuts(shortcuts: Dict[str, Any]) -> Dict[str, Any]:
    """Save persistent shortcut preferences."""
    path = shortcuts_store_path()
    try:
        with path.open("w", encoding="utf-8") as stream:
            json.dump(shortcuts, stream, indent=2)
        return {"ok": True, "shortcuts": shortcuts}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _issue(
    severity: str,
    code: str,
    message: str,
    *,
    obj: str = "",
    field: str = "",
    why: str = "",
    action: str = "",
) -> Dict[str, str]:
    return {
        "severity": severity,
        "code": code,
        "object": obj,
        "field": field,
        "message": message,
        "why_it_matters": why,
        "action_required": action,
    }


def _shape_kind(shape: Dict[str, Any]) -> str:
    semantic = _text(shape.get("semantic_type")).lower()
    if semantic:
        return semantic

    text = " ".join(
        [
            _text(shape.get("type")),
            _text(shape.get("name")),
            _text(shape.get("id")),
            _text(shape.get("role")),
            _text(shape.get("usage")),
        ]
    ).lower()
    if any(token in text for token in ("laser", "lidar", "scan")):
        return "laser"
    if any(token in text for token in ("camera", "depth", "rgb")):
        return "camera"
    if "battery" in text or "bms" in text:
        return "battery"
    if "charger" in text or "dock" in text:
        return "charger"
    if "collision" in text or "footprint" in text:
        return "collision"
    if any(token in text for token in ("safety", "protect", "slow", "stop")):
        return "safety"
    if "wheel" in text:
        return "wheel"
    return _text(shape.get("type")) or "device"


def _enabled(shape: Dict[str, Any]) -> bool:
    return bool(shape.get("enabled", True))


def _valid_topic(topic: str) -> bool:
    return not topic or bool(ROS_TOPIC_RE.match(topic))


def _valid_frame(frame: str) -> bool:
    return not frame or bool(ROS_FRAME_RE.match(frame))


def validate_robot_model(profile: Dict[str, Any], robot_builder: Dict[str, Any]) -> Dict[str, Any]:
    """Run strict field, object, and system validation for a robot model."""
    profile = profile if isinstance(profile, dict) else {}
    model = robot_builder if isinstance(robot_builder, dict) else {}
    links = model.get("links", []) if isinstance(model.get("links", []), list) else []
    shapes = model.get("shapes", []) if isinstance(model.get("shapes", []), list) else []
    base_frame = _text(model.get("base_link") or profile.get("base_frame"))
    link_names = [_text(link.get("name")) for link in links if isinstance(link, dict)]
    link_set = {name for name in link_names if name}
    child_frames: Dict[str, str] = {}
    command_topics: Dict[str, str] = {}
    errors: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []
    suggestions: List[Dict[str, str]] = []

    robot_id = _text(profile.get("robot_id"))
    if not robot_id:
        errors.append(
            _issue(
                "critical",
                "robot_id_required",
                "Robot profile has no robot_id.",
                field="robot_id",
                why="Deployment snapshots and generated files need a stable robot identity.",
                action="Set a unique robot model identifier before saving or deploying.",
            )
        )

    if not base_frame:
        errors.append(
            _issue(
                "critical",
                "base_frame_required",
                "Robot model has no base frame.",
                field="base_frame",
                why="URDF, TF, Nav2 and safety footprints need one authoritative base frame.",
                action="Set base_frame/base_link to the physical robot base frame.",
            )
        )
    elif not _valid_frame(base_frame):
        errors.append(
            _issue(
                "critical",
                "base_frame_invalid",
                f'Base frame "{base_frame}" is not a valid ROS frame name.',
                field="base_frame",
                why="Invalid frame names break TF lookup and generated URDF.",
                action=(
                    "Use letters, numbers and underscores, starting with a letter "
                    "or underscore."
                ),
            )
        )
    elif base_frame not in link_set:
        warnings.append(
            _issue(
                "warning",
                "base_frame_missing_link",
                f'Base frame "{base_frame}" is not listed as a model link.',
                obj=base_frame,
                field="links",
                why="Generated TF may not contain the expected root frame.",
                action="Add the base frame to the transform/link list.",
            )
        )

    if not shapes:
        errors.append(
            _issue(
                "critical",
                "no_geometry",
                "Robot model has no chassis, device, collision or safety geometry.",
                field="robot_builder.shapes",
                why="The canvas, URDF, Nav2 footprint and safety zones cannot be generated.",
                action="Add at least a chassis shape and collision footprint.",
            )
        )

    seen_shape_ids: set[str] = set()
    enabled_sensor_count = 0
    has_chassis = False
    has_collision = False
    has_battery = False
    has_drive = False

    for shape in shapes:
        if not isinstance(shape, dict):
            continue
        shape_id = _text(shape.get("id") or shape.get("name") or "shape")
        kind = _shape_kind(shape)
        enabled = _enabled(shape)
        link = _text(shape.get("link") or base_frame)
        topic = _text(shape.get("topic") or shape.get("scan_topic") or shape.get("image_topic"))
        driver = _text(shape.get("driver") or shape.get("driver_plugin"))
        child_frame = _text(shape.get("child_frame") or shape.get("frame_id"))

        if shape_id in seen_shape_ids:
            errors.append(
                _issue(
                    "critical",
                    "duplicate_object_id",
                    f'Duplicate model object id "{shape_id}".',
                    obj=shape_id,
                    field="id",
                    why=(
                        "Object references, validation and deployment diffs require "
                        "stable unique ids."
                    ),
                    action=(
                        "Rename or duplicate the object through the model editor so "
                        "a unique id is assigned."
                    ),
                )
            )
        seen_shape_ids.add(shape_id)

        if not link or link not in link_set:
            errors.append(
                _issue(
                    "critical",
                    "missing_parent_frame",
                    f'{shape_id}: parent frame "{link}" is not present in the transform tree.',
                    obj=shape_id,
                    field="link",
                    why="The object cannot be located in robot coordinates.",
                    action="Select an existing parent frame or add the missing frame.",
                )
            )

        if child_frame and not _valid_frame(child_frame):
            errors.append(
                _issue(
                    "critical",
                    "invalid_child_frame",
                    f'{shape_id}: child frame "{child_frame}" is invalid.',
                    obj=shape_id,
                    field="child_frame",
                    why="Invalid frame names break TF and URDF output.",
                    action="Use a valid ROS frame name.",
                )
            )

        if enabled and child_frame:
            previous = child_frames.get(child_frame)
            if previous and previous != shape_id:
                errors.append(
                    _issue(
                        "critical",
                        "duplicate_child_frame",
                        f'{shape_id}: child frame "{child_frame}" is already used by {previous}.',
                        obj=shape_id,
                        field="child_frame",
                        why="Duplicate frames make the TF tree ambiguous.",
                        action="Assign a unique child frame to each enabled device.",
                    )
                )
            child_frames[child_frame] = shape_id

        if enabled and topic and not _valid_topic(topic):
            errors.append(
                _issue(
                    "critical",
                    "invalid_topic",
                    f'{shape_id}: topic "{topic}" is not a valid ROS topic name.',
                    obj=shape_id,
                    field="topic",
                    why="Invalid topic names prevent driver and diagnostics binding.",
                    action="Use a valid absolute or relative ROS topic name.",
                )
            )

        if enabled and topic and ("cmd" in topic or "command" in topic):
            previous = command_topics.get(topic)
            if previous and previous != shape_id:
                errors.append(
                    _issue(
                        "critical",
                        "conflicting_command_topic",
                        f'{shape_id}: command topic "{topic}" conflicts with {previous}.',
                        obj=shape_id,
                        field="topic",
                        why=(
                            "Two enabled devices publishing commands to the same "
                            "target can create unsafe motion."
                        ),
                        action="Separate the command topics or disable one device.",
                    )
                )
            command_topics[topic] = shape_id

        width = _float(shape.get("width") or shape.get("radius"))
        length = _float(shape.get("length") or shape.get("radius"))
        height = _float(shape.get("height"))
        radius = _float(shape.get("radius"))
        if width <= 0 or length <= 0 or height < 0:
            errors.append(
                _issue(
                    "critical",
                    "invalid_dimensions",
                    f"{shape_id}: dimensions must be greater than zero.",
                    obj=shape_id,
                    field="width/length/height",
                    why="Invalid geometry cannot be exported to URDF, footprint or safety zones.",
                    action="Enter positive metric dimensions for the object.",
                )
            )

        if kind in {"laser", "camera"}:
            enabled_sensor_count += 1 if enabled else 0
            if enabled and not topic:
                errors.append(
                    _issue(
                        "critical",
                        "sensor_topic_required",
                        f"{shape_id}: enabled {kind} has no runtime topic.",
                        obj=shape_id,
                        field="topic",
                        why=(
                            "Navigation, safety and diagnostics cannot observe this "
                            "enabled sensor."
                        ),
                        action="Set scan_topic/image_topic/topic or disable the sensor.",
                    )
                )
            if enabled and not driver:
                warnings.append(
                    _issue(
                        "warning",
                        "sensor_driver_missing",
                        f"{shape_id}: enabled {kind} has no driver binding.",
                        obj=shape_id,
                        field="driver",
                        why="Deployment cannot generate complete driver parameters.",
                        action="Select a driver plugin or script for this device.",
                    )
                )
            if kind == "laser" and enabled:
                range_m = _float(shape.get("range_m"))
                if range_m <= 0 and radius <= 0:
                    warnings.append(
                        _issue(
                            "warning",
                            "laser_range_missing",
                            f"{shape_id}: laser range is not configured.",
                            obj=shape_id,
                            field="range_m",
                            why="Safety and obstacle layers need realistic detection range.",
                            action="Set min/max/range values from the sensor datasheet.",
                        )
                    )
                elif range_m > 40.0:
                    warnings.append(
                        _issue(
                            "warning",
                            "laser_range_extreme",
                            f"{shape_id}: laser range {range_m}m is unusually high.",
                            obj=shape_id,
                            field="range_m",
                            why="Extremely long range settings may increase computational load or reduce resolution.",
                            action="Verify the effective reliable range in the operating environment.",
                        )
                    )
                
                warnings.append(
                    _issue(
                        "warning",
                        "laser_interference_risk",
                        f"{shape_id}: ensure environment supports laser detection.",
                        obj=shape_id,
                        field="advanced_metadata",
                        why="Highly reflective, specular, or black objects, strong ambient light, and cross-laser interference can reduce reliability.",
                        action="Document operational limits for reflectivity and sunlight in metadata and configure safety filters.",
                    )
                )

        if kind == "battery":
            has_battery = has_battery or enabled
            if enabled and not driver:
                warnings.append(
                    _issue(
                        "warning",
                        "battery_driver_missing",
                        f"{shape_id}: enabled battery has no BMS driver.",
                        obj=shape_id,
                        field="driver",
                        why=(
                            "Runtime battery state and low-battery safety behavior "
                            "may be unavailable."
                        ),
                        action="Bind the battery to a BMS driver or diagnostics source.",
                    )
                )

        charger_has_interface = _text(shape.get("ip_address")) or _text(shape.get("port"))
        if kind == "charger" and enabled and not charger_has_interface:
            warnings.append(
                _issue(
                    "warning",
                    "charger_interface_missing",
                    f"{shape_id}: charger has no communication interface configured.",
                    obj=shape_id,
                    field="ip_address/port",
                    why="Charging and docking workflows need a charger handshake path.",
                    action="Set charger IP, serial port, IO handshake or disable the charger.",
                )
            )

        has_chassis = has_chassis or kind == "chassis" or "base" in shape_id.lower()
        has_collision = has_collision or kind == "collision"
        has_drive = has_drive or kind in {"wheel", "controller"} or "wheel" in shape_id.lower()

    if not has_chassis:
        errors.append(
            _issue(
                "critical",
                "chassis_required",
                "Model has no primary chassis object.",
                field="robot_builder.shapes",
                why="Chassis drives URDF, footprint, safety margins and canvas body display.",
                action="Add or label one chassis/body geometry object.",
            )
        )

    if not has_collision:
        warnings.append(
            _issue(
                "warning",
                "collision_shape_missing",
                "No explicit collision footprint object found.",
                field="collision",
                why="Nav2 and safety controller will fall back to chassis geometry.",
                action=(
                    "Add a collision or footprint object that covers the chassis "
                    "and protrusions."
                ),
            )
        )

    if not has_drive:
        warnings.append(
            _issue(
                "warning",
                "drive_system_missing",
                "No drive system object is configured.",
                field="devices",
                why="Motion validation cannot confirm wheel/controller parameters.",
                action="Add wheel, controller or drive-system objects.",
            )
        )

    if not has_battery:
        warnings.append(
            _issue(
                "warning",
                "battery_missing",
                "No enabled battery object is configured.",
                field="devices.battery",
                why="Low-battery behavior and diagnostics cannot be generated from the model.",
                action=(
                    "Add an enabled battery or document the external power source "
                    "in advanced metadata."
                ),
            )
        )

    if enabled_sensor_count == 0:
        warnings.append(
            _issue(
                "warning",
                "no_enabled_sensors",
                "No enabled navigation or perception sensors are configured.",
                field="devices",
                why="Navigation, obstacle detection and safety zones need sensor sources.",
                action="Enable at least one laser/camera/distance sensor if present on the robot.",
            )
        )

    suggestions.append(
        _issue(
            "suggestion",
            "laser_environment_guidance",
            (
                "Check laser limits against black, specular, reflective and "
                "strong-light environments."
            ),
            field="laser",
            why=(
                "The Next reference warns these conditions reduce LiDAR "
                "detection reliability."
            ),
            action="Record operating environment constraints in the model metadata.",
        )
    )

    ready = not errors
    return {
        "ok": True,
        "ready_to_deploy": ready,
        "errors": errors,
        "warnings": warnings,
        "suggestions": suggestions,
        "summary": {
            "critical_errors": len(errors),
            "warnings": len(warnings),
            "suggestions": len(suggestions),
            "enabled_sensors": enabled_sensor_count,
            "shape_count": len(shapes),
            "link_count": len(links),
        },
    }


def _sorted_shapes(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    shapes = model.get("shapes", []) if isinstance(model.get("shapes", []), list) else []
    return sorted(
        [shape for shape in shapes if isinstance(shape, dict)],
        key=lambda item: (_text(item.get("id") or item.get("name")), _text(item.get("link"))),
    )


def _sorted_links(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    links = model.get("links", []) if isinstance(model.get("links", []), list) else []
    return sorted(
        [link for link in links if isinstance(link, dict)],
        key=lambda item: _text(item.get("name")),
    )


def generate_static_transforms(model: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate deterministic static transform records."""
    transforms: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for link in _sorted_links(model):
        child = _text(link.get("name"))
        parent = _text(link.get("parent"))
        if not child or not parent:
            continue
        seen.add(child)
        transforms.append(
            {
                "parent_frame": parent,
                "child_frame": child,
                "xyz": list(link.get("joint_xyz", [0.0, 0.0, 0.0]))[:3],
                "rpy": list(link.get("joint_rpy", [0.0, 0.0, 0.0]))[:3],
            }
        )
    for shape in _sorted_shapes(model):
        child = _text(shape.get("child_frame") or shape.get("frame_id"))
        parent = _text(shape.get("link") or model.get("base_link") or "base_link")
        if not child or not parent or child in seen:
            continue
        seen.add(child)
        transforms.append(
            {
                "parent_frame": parent,
                "child_frame": child,
                "xyz": [
                    _float(shape.get("x")),
                    _float(shape.get("y")),
                    _float(shape.get("z")),
                ],
                "rpy": [
                    _float(shape.get("roll")),
                    _float(shape.get("pitch")),
                    _float(shape.get("yaw")),
                ],
            }
        )
    return transforms


def _xml(value: Any) -> str:
    return html.escape(_text(value), quote=True)


def _num(value: Any, default: float = 0.0) -> str:
    return f"{_float(value, default):.6g}"


def _shape_geometry_xml(shape: Dict[str, Any]) -> str:
    shape_type = _text(shape.get("type")).lower()
    radius = max(0.001, _float(shape.get("radius"), 0.05))
    length = max(0.001, _float(shape.get("length") or radius * 2.0, 0.1))
    width = max(0.001, _float(shape.get("width") or radius * 2.0, 0.1))
    height = max(0.001, _float(shape.get("height"), 0.05))
    if shape_type in {"cylinder", "laser", "wheel"}:
        return f'<cylinder radius="{_num(radius)}" length="{_num(height)}"/>'
    if shape_type == "sphere":
        return f'<sphere radius="{_num(radius)}"/>'
    return f'<box size="{_num(length)} {_num(width)} {_num(height)}"/>'


def _shape_origin_xml(shape: Dict[str, Any]) -> str:
    return (
        f'<origin xyz="{_num(shape.get("x"))} {_num(shape.get("y"))} '
        f'{_num(shape.get("z"))}" rpy="{_num(shape.get("roll"))} '
        f'{_num(shape.get("pitch"))} {_num(shape.get("yaw"))}"/>'
    )


def _urdf_body(model: Dict[str, Any]) -> List[str]:
    base = _text(model.get("base_link")) or "base_link"
    link_names = {_text(link.get("name")) for link in _sorted_links(model)}
    link_names.add(base)
    visuals: Dict[str, List[Dict[str, Any]]] = {}
    for shape in _sorted_shapes(model):
        if not _enabled(shape):
            continue
        link_name = _text(
            shape.get("child_frame") or shape.get("frame_id") or shape.get("link") or base
        )
        link_names.add(link_name)
        visuals.setdefault(link_name, []).append(shape)
    lines: List[str] = []
    for name in sorted(name for name in link_names if name):
        shape_blocks = visuals.get(name, [])
        if not shape_blocks:
            lines.append(f'  <link name="{_xml(name)}"/>')
            continue
        lines.append(f'  <link name="{_xml(name)}">')
        for shape in shape_blocks:
            shape_id = _text(shape.get("id") or shape.get("name") or "object")
            geometry = _shape_geometry_xml(shape)
            has_child_frame = bool(_text(shape.get("child_frame") or shape.get("frame_id")))
            origin = (
                '<origin xyz="0 0 0" rpy="0 0 0"/>'
                if has_child_frame else _shape_origin_xml(shape)
            )
            lines.extend(
                [
                    f'    <visual name="{_xml(shape_id)}_visual">',
                    f"      {origin}",
                    f"      <geometry>{geometry}</geometry>",
                    "    </visual>",
                    f'    <collision name="{_xml(shape_id)}_collision">',
                    f"      {origin}",
                    f"      <geometry>{geometry}</geometry>",
                    "    </collision>",
                ]
            )
        lines.append("  </link>")
    for transform in generate_static_transforms(model):
        parent = _text(transform.get("parent_frame"))
        child = _text(transform.get("child_frame"))
        if not parent or not child:
            continue
        xyz = transform.get("xyz", [0, 0, 0])
        rpy = transform.get("rpy", [0, 0, 0])
        lines.extend(
            [
                f'  <joint name="{_xml(parent)}_to_{_xml(child)}" type="fixed">',
                f'    <parent link="{_xml(parent)}"/>',
                f'    <child link="{_xml(child)}"/>',
                (
                    f'    <origin xyz="{_num(xyz[0] if len(xyz) > 0 else 0)} '
                    f'{_num(xyz[1] if len(xyz) > 1 else 0)} '
                    f'{_num(xyz[2] if len(xyz) > 2 else 0)}" rpy="'
                    f'{_num(rpy[0] if len(rpy) > 0 else 0)} '
                    f'{_num(rpy[1] if len(rpy) > 1 else 0)} '
                    f'{_num(rpy[2] if len(rpy) > 2 else 0)}"/>'
                ),
                "  </joint>",
            ]
        )
    for link_name, shape_blocks in sorted(visuals.items()):
        material = _text(shape_blocks[0].get("color")) or "#60a5fa"
        lines.extend(
            [
                f'  <gazebo reference="{_xml(link_name)}">',
                f'    <material>{_xml(material)}</material>',
                "  </gazebo>",
            ]
        )
    return lines


def generate_urdf(model: Dict[str, Any], profile: Dict[str, Any]) -> str:
    """Generate a deterministic URDF from the robot model."""
    robot_name = _text(profile.get("robot_id") or model.get("robot_id") or "robot_model")
    lines = [f'<robot name="{_xml(robot_name)}">']
    lines.extend(_urdf_body(model if isinstance(model, dict) else {}))
    lines.append("</robot>")
    return "\n".join(lines) + "\n"


def generate_xacro(model: Dict[str, Any], profile: Dict[str, Any]) -> str:
    """Generate a deterministic xacro variant of the robot model."""
    robot_name = _text(profile.get("robot_id") or model.get("robot_id") or "robot_model")
    lines = [f'<robot name="{_xml(robot_name)}" xmlns:xacro="http://ros.org/wiki/xacro">']
    lines.append('  <xacro:property name="generated_by" value="next_ros2ws"/>')
    lines.extend(_urdf_body(model if isinstance(model, dict) else {}))
    lines.append("</robot>")
    return "\n".join(lines) + "\n"


def _json_file(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def generate_ros2_param_yaml(name: str, payload: Dict[str, Any]) -> str:
    """Generate deterministic ROS 2 parameter YAML."""
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    lines = [
        f"{name}:",
        "  ros__parameters:",
    ]
    for line in encoded.splitlines():
        lines.append(f"    # {line}")
    lines.append("    generated_config_json: |")
    for line in encoded.splitlines():
        lines.append(f"      {line}")
    return "\n".join(lines) + "\n"


def generate_driver_parameters(model: Dict[str, Any]) -> Dict[str, Any]:
    """Generate deterministic driver parameter records from model objects."""
    drivers: Dict[str, Any] = {}
    for shape in _sorted_shapes(model):
        if not _enabled(shape):
            continue
        shape_id = _text(shape.get("id") or shape.get("name"))
        driver = _text(shape.get("driver") or shape.get("driver_plugin"))
        topic = _text(shape.get("topic") or shape.get("scan_topic") or shape.get("image_topic"))
        if not shape_id or (not driver and not topic):
            continue
        drivers[shape_id] = {
            "type": _shape_kind(shape),
            "driver": driver,
            "topic": topic,
            "frame_id": _text(
                shape.get("frame_id") or shape.get("child_frame") or shape.get("link")
            ),
            "protocol": _text(shape.get("protocol")),
            "port": _text(shape.get("port")),
            "ip_address": _text(shape.get("ip_address")),
            "baud_rate": int(_float(shape.get("baud_rate"), 0)),
            "timeout_s": _float(shape.get("timeout_s"), 0.0),
            "diagnostic_source": _text(shape.get("diagnostic_source")),
        }
    return drivers


def generate_nav2_outputs(model: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    """Generate Nav2 geometry suggestions from model geometry."""
    shapes = _sorted_shapes(model)
    collision = next((s for s in shapes if _shape_kind(s) == "collision"), None)
    chassis = next(
        (
            s for s in shapes
            if _shape_kind(s) == "chassis" or "base" in _text(s.get("id")).lower()
        ),
        None,
    )
    source = collision or chassis or (shapes[0] if shapes else {})
    width = max(0.01, _float(source.get("width"), 0.6))
    length = max(0.01, _float(source.get("length"), 0.45))
    half_l = length / 2.0
    half_w = width / 2.0
    footprint = [
        [round(half_l, 4), round(half_w, 4)],
        [round(half_l, 4), round(-half_w, 4)],
        [round(-half_l, 4), round(-half_w, 4)],
        [round(-half_l, 4), round(half_w, 4)],
    ]
    sensors = [
        {
            "name": _text(s.get("id") or s.get("name")),
            "type": _shape_kind(s),
            "topic": _text(s.get("topic") or s.get("scan_topic") or s.get("image_topic")),
            "frame": _text(s.get("frame_id") or s.get("child_frame") or s.get("link")),
        }
        for s in shapes
        if _enabled(s) and _shape_kind(s) in {"laser", "camera"}
    ]
    return {
        "base_frame": _text(
            model.get("base_link") or profile.get("base_frame") or "base_link"
        ),
        "odom_frame": _text(
            profile.get("sensor_frames", {}).get("odom")
            if isinstance(profile.get("sensor_frames"), dict)
            else ""
        ) or "odom",
        "footprint": footprint,
        "footprint_padding": 0.02,
        "inflation_radius_suggestion": round(max(width, length) * 0.35, 3),
        "obstacle_sources": sensors,
        "docking_approach_zone": {
            "front_clearance_m": round(half_l + 0.25, 3),
            "side_clearance_m": round(half_w + 0.10, 3),
        },
    }


def generate_safety_outputs(model: Dict[str, Any]) -> Dict[str, Any]:
    """Generate safety controller zone suggestions."""
    zones: List[Dict[str, Any]] = []
    for shape in _sorted_shapes(model):
        kind = _shape_kind(shape)
        if kind not in {"safety", "collision", "laser"}:
            continue
        shape_id = _text(shape.get("id") or shape.get("name"))
        zones.append(
            {
                "name": shape_id,
                "source_object": shape_id,
                "source_sensor": _text(shape.get("link")),
                "shape": _text(shape.get("type")) or "box",
                "enabled": _enabled(shape),
                "stop_behavior": _text(shape.get("safety_behavior")) or "stop_on_intrusion",
                "recovery_behavior": (
                    _text(shape.get("recovery_behavior")) or "manual_or_clear_path"
                ),
                "range_m": _float(shape.get("range_m") or shape.get("radius"), 0.0),
            }
        )
    return {"zones": zones, "docking_override_policy": "expand_zone_do_not_disable"}


def generate_diagnostics_outputs(model: Dict[str, Any]) -> Dict[str, Any]:
    """Generate ROS 2 diagnostics aggregator config suggestions."""
    analyzers: Dict[str, Any] = {}
    for shape in _sorted_shapes(model):
        if not _enabled(shape):
            continue
        kind = _shape_kind(shape)
        if kind not in {"laser", "camera", "battery"}:
            continue
        shape_id = _text(shape.get("id") or shape.get("name"))
        analyzers[shape_id] = {
            "type": "diagnostic_aggregator/GenericAnalyzer",
            "path": kind,
            "timeout": 5.0,
            "find_and_remove_prefix": kind,
            "num_items": 1,
            "expected_names": [shape_id],
        }
    return {"pub_rate": 1.0, "analyzers": analyzers}


def generated_bundle(profile: Dict[str, Any], model: Dict[str, Any]) -> Dict[str, Any]:
    """Build a deterministic generated configuration bundle preview."""
    validation = validate_robot_model(profile, model)
    safe_profile = profile if isinstance(profile, dict) else {}
    safe_model = model if isinstance(model, dict) else {}
    static_transforms = generate_static_transforms(safe_model)
    driver_parameters = generate_driver_parameters(safe_model)
    nav2 = generate_nav2_outputs(safe_model, safe_profile)
    safety_controller = generate_safety_outputs(safe_model)
    diagnostics = generate_diagnostics_outputs(safe_model)
    urdf = generate_urdf(safe_model, safe_profile)
    xacro = generate_xacro(safe_model, safe_profile)
    bundle = {
        "profile": safe_profile,
        "robot_builder": safe_model,
        "static_transforms": static_transforms,
        "driver_parameters": driver_parameters,
        "nav2": nav2,
        "safety_controller": safety_controller,
        "diagnostics": diagnostics,
        "urdf": urdf,
        "xacro": xacro,
        "validation": validation,
    }
    encoded = json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode("utf-8")
    checksum = hashlib.sha256(encoded).hexdigest()
    files = {
        "profile.json": _json_file(safe_profile),
        "robot_builder.json": _json_file(safe_model),
        "robot_model.urdf": urdf,
        "robot_model.xacro": xacro,
        "static_transforms.json": _json_file(static_transforms),
        "driver_parameters.yaml": generate_ros2_param_yaml(
            "driver_parameters", driver_parameters
        ),
        "nav2_geometry.yaml": generate_ros2_param_yaml("nav2_geometry", nav2),
        "safety_controller.yaml": generate_ros2_param_yaml(
            "safety_controller", safety_controller
        ),
        "diagnostics.yaml": generate_ros2_param_yaml("diagnostics", diagnostics),
        "validation_report.json": _json_file(validation),
    }
    manifest = {
        "schema_version": 1,
        "created_at": int(time.time()),
        "robot_id": _text(safe_profile.get("robot_id")),
        "ready_to_deploy": bool(validation.get("ready_to_deploy")),
        "checksum": checksum,
        "generated_files": sorted([*files, "deployment_manifest.json"]),
        "file_checksums": {
            name: hashlib.sha256(content.encode("utf-8")).hexdigest()
            for name, content in sorted(files.items())
        },
        "rollback_required": True,
    }
    bundle["deployment_manifest"] = manifest
    files["deployment_manifest.json"] = _json_file(manifest)
    return {"ok": True, "bundle": bundle, "manifest": manifest, "files": files}


def network_health(
    host: str,
    ports: Optional[Iterable[int]] = None,
    timeout_s: float = 0.35,
) -> Dict[str, Any]:
    """Probe robot network health without requiring robot-specific APIs."""
    target = _text(host)
    if not target:
        return {"ok": False, "message": "host is required"}
    if not IP_OR_HOST_RE.match(target):
        return {"ok": False, "message": "host contains unsupported characters"}
    checked_ports = list(ports or (19200, 19204, 19205, 19206, 19207, 19208, 19210))
    results: List[Dict[str, Any]] = []
    reachable = 0
    started = time.monotonic()
    for port in checked_ports:
        port_started = time.monotonic()
        ok = False
        error = ""
        try:
            with socket.create_connection((target, int(port)), timeout=float(timeout_s)):
                ok = True
        except Exception as exc:
            error = str(exc)
        latency_ms = round((time.monotonic() - port_started) * 1000.0, 2)
        reachable += 1 if ok else 0
        results.append(
            {
                "port": int(port),
                "reachable": ok,
                "latency_ms": latency_ms,
                "error": error,
            }
        )
    total_ms = round((time.monotonic() - started) * 1000.0, 2)
    return {
        "ok": True,
        "host": target,
        "ports": results,
        "reachable_ports": reachable,
        "checked_ports": len(checked_ports),
        "average_probe_ms": round(total_ms / max(1, len(checked_ports)), 2),
        "link_state": "reachable" if reachable else "unreachable",
        "message": (
            f"{reachable}/{len(checked_ports)} Next-style service ports reachable"
            if reachable
            else "No configured robot service ports were reachable"
        ),
    }
