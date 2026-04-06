#!/usr/bin/env python3

from __future__ import annotations

import json

from dataclasses import dataclass, asdict
from threading import RLock
from typing import Any, Dict, List

ACTION_MAPPING_PREFIX = 'action_node_'


@dataclass(frozen=True)
class ActionDefinition:
    id: str
    label: str
    topic: str
    type: str
    field: str
    description: str = ''


# Core defaults that ship with the platform.
_DEFAULT_ACTIONS: Dict[str, ActionDefinition] = {
    'lift_up': ActionDefinition(
        id='lift_up',
        label='Lift Up',
        topic='/lift/cmd_rpm',
        type='std_msgs/msg/Int32',
        field='{"data": -1500}',
        description='Commands lift motor upward.',
    ),
    'lift_down': ActionDefinition(
        id='lift_down',
        label='Lift Down',
        topic='/lift/cmd_rpm',
        type='std_msgs/msg/Int32',
        field='{"data": 1500}',
        description='Commands lift motor downward.',
    ),
}

# Optional extension registry for runtime additions.
_EXTENSION_ACTIONS: Dict[str, ActionDefinition] = {}
_EXTENSION_LOCK = RLock()


def normalize_action_id(raw: Any) -> str:
    text = str(raw or '').strip().lower()
    if not text:
        return ''
    out = []
    last_underscore = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            last_underscore = False
            continue
        if not last_underscore:
            out.append('_')
            last_underscore = True
    normalized = ''.join(out).strip('_')
    return normalized


def register_action(definition: Dict[str, Any]) -> ActionDefinition:
    action_id = normalize_action_id(definition.get('id') or definition.get('key'))
    if not action_id:
        raise ValueError('Action id is required')

    label = str(definition.get('label') or action_id.replace('_', ' ').title()).strip()
    topic = str(definition.get('topic') or '').strip()
    msg_type = str(definition.get('type') or 'std_msgs/msg/Int32').strip()
    field = str(definition.get('field') or '').strip()
    description = str(definition.get('description') or '').strip()

    action = ActionDefinition(
        id=action_id,
        label=label,
        topic=topic,
        type=msg_type,
        field=field,
        description=description,
    )
    with _EXTENSION_LOCK:
        _EXTENSION_ACTIONS[action_id] = action
    return action


def unregister_action(action_id: str) -> bool:
    normalized = normalize_action_id(action_id)
    if not normalized:
        return False
    with _EXTENSION_LOCK:
        return _EXTENSION_ACTIONS.pop(normalized, None) is not None


def list_actions() -> List[ActionDefinition]:
    with _EXTENSION_LOCK:
        merged = dict(_DEFAULT_ACTIONS)
        merged.update(_EXTENSION_ACTIONS)
    return [merged[key] for key in sorted(merged.keys())]


def list_actions_payload() -> List[Dict[str, str]]:
    return [asdict(action) for action in list_actions()]


def default_action_mappings() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for action in list_actions():
        out[f'{ACTION_MAPPING_PREFIX}{action.id}'] = {
            'topic': action.topic,
            'field': action.field,
            'type': action.type,
        }
    return out


def _coerce_legacy_lift_field(mapping_key: str, field_raw: str, fallback_field: str) -> str:
    key = str(mapping_key or '').strip()
    raw = str(field_raw or '').strip()
    fallback = str(fallback_field or '').strip()
    if key not in {f'{ACTION_MAPPING_PREFIX}lift_up', f'{ACTION_MAPPING_PREFIX}lift_down'}:
        return raw
    if not raw:
        return fallback

    try:
        payload = json.loads(raw)
    except Exception:
        return raw
    if not isinstance(payload, dict):
        return raw

    try:
        value = int(float(payload.get('data')))
    except Exception:
        return raw
    if abs(value) != 300:
        return raw

    payload['data'] = -1500 if key.endswith('lift_up') else 1500
    return json.dumps(payload)


def merge_action_mappings(mappings: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}
    source = mappings if isinstance(mappings, dict) else {}

    for key, value in source.items():
        if not isinstance(value, dict):
            continue
        merged[str(key)] = dict(value)

    for key, defaults in default_action_mappings().items():
        current = merged.get(key, {})
        if not isinstance(current, dict):
            current = {}
        topic = str(current.get('topic') or defaults.get('topic') or '').strip()
        field = str(current.get('field') or defaults.get('field') or '').strip()
        msg_type = str(current.get('type') or defaults.get('type') or '').strip()
        field = _coerce_legacy_lift_field(key, field, str(defaults.get('field') or '').strip())
        merged[key] = {
            'topic': topic,
            'field': field,
            'type': msg_type,
        }

    return merged
