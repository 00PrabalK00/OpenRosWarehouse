from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict

import yaml


DOTTED_TRUTH_VERSION = 'grid-quantized-dots-v1'
DOTTED_TRUTH_META_KEY = 'next_dotted_truth'
DEFAULT_VENV_DIRNAME = '.dotted_map_venv'


def _log(logger: Any, level: str, message: str) -> None:
    if logger is None:
        return
    handler = getattr(logger, level, None)
    if callable(handler):
        handler(message)


def _resolve_image_path(map_yaml_path: str, image_ref: str) -> str:
    image_ref = str(image_ref or '').strip()
    if os.path.isabs(image_ref):
        return os.path.abspath(image_ref)
    return os.path.abspath(os.path.join(os.path.dirname(map_yaml_path), image_ref))


def _image_ref_for_yaml(map_yaml_path: str, image_path: str) -> str:
    yaml_dir = os.path.abspath(os.path.dirname(map_yaml_path))
    image_abs = os.path.abspath(image_path)
    try:
        rel = os.path.relpath(image_abs, yaml_dir)
        if not rel.startswith('..'):
            return rel
    except Exception:
        pass
    return image_abs


def _write_yaml(path: str, data: Dict[str, Any]) -> None:
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as handle:
        yaml.safe_dump(data, handle, default_flow_style=False)
    os.replace(tmp, path)


def _default_source_from_dotted_image(current_image_path: str) -> str:
    path = Path(current_image_path)
    stem = path.stem
    suffix = path.suffix or '.pgm'
    if stem.endswith('_dotted_truth'):
        candidate = path.with_name(f'{stem[:-13]}{suffix}')
        if candidate.exists():
            return str(candidate)
    return str(path)


def build_dotted_truth_plan(map_yaml_path: str, map_yaml: Dict[str, Any]) -> Dict[str, str]:
    image_ref = str(map_yaml.get('image', '') or '').strip()
    if not image_ref:
        raise ValueError('Map yaml missing image field')

    current_image_path = _resolve_image_path(map_yaml_path, image_ref)
    meta = map_yaml.get(DOTTED_TRUTH_META_KEY)
    meta = meta if isinstance(meta, dict) else {}

    source_ref = str(meta.get('source_image', '') or '').strip()
    if source_ref:
        source_image_path = _resolve_image_path(map_yaml_path, source_ref)
    else:
        source_image_path = _default_source_from_dotted_image(current_image_path)

    dotted_ref = str(meta.get('image', '') or '').strip()
    if dotted_ref:
        dotted_image_path = _resolve_image_path(map_yaml_path, dotted_ref)
    elif current_image_path != source_image_path:
        dotted_image_path = current_image_path
    else:
        source_path = Path(source_image_path)
        if source_path.stem.endswith('_dotted_truth'):
            dotted_image_path = str(source_path)
        else:
            dotted_image_path = str(source_path.with_name(f'{source_path.stem}_dotted_truth.pgm'))

    return {
        'source_image_path': os.path.abspath(source_image_path),
        'source_image_ref': _image_ref_for_yaml(map_yaml_path, source_image_path),
        'dotted_image_path': os.path.abspath(dotted_image_path),
        'dotted_image_ref': _image_ref_for_yaml(map_yaml_path, dotted_image_path),
    }


def _candidate_python_paths(workspace_root: str = ''):
    env_python = str(os.getenv('NEXT_DOTTED_MAP_PYTHON', '') or '').strip()
    if env_python:
        yield env_python

    env_venv = str(os.getenv('NEXT_DOTTED_MAP_VENV', '') or '').strip()
    if env_venv:
        yield os.path.join(env_venv, 'bin', 'python3')
        yield os.path.join(env_venv, 'bin', 'python')

    if workspace_root:
        yield os.path.join(workspace_root, DEFAULT_VENV_DIRNAME, 'bin', 'python3')
        yield os.path.join(workspace_root, DEFAULT_VENV_DIRNAME, 'bin', 'python')

    yield sys.executable


def resolve_dotted_truth_python(workspace_root: str = '') -> str:
    for candidate in _candidate_python_paths(workspace_root):
        normalized = os.path.abspath(os.path.expanduser(str(candidate)))
        if os.path.isfile(normalized) and os.access(normalized, os.X_OK):
            return normalized
    return sys.executable


def resolve_dotted_truth_worker_path() -> str:
    return str(Path(__file__).with_name('map_dotted_truth_worker.py'))


def run_dotted_truth_worker(
    *,
    input_path: str,
    output_path: str,
    workspace_root: str = '',
    logger: Any = None,
    timeout_sec: float = 180.0,
) -> Dict[str, Any]:
    python_bin = resolve_dotted_truth_python(workspace_root)
    worker_script = resolve_dotted_truth_worker_path()
    cmd = [
        python_bin,
        worker_script,
        '--input',
        os.path.abspath(input_path),
        '--output',
        os.path.abspath(output_path),
    ]

    if os.path.abspath(python_bin) == os.path.abspath(sys.executable):
        _log(
            logger,
            'warn',
            'Dotted truth map worker is using the current interpreter. '
            'Set NEXT_DOTTED_MAP_VENV or NEXT_DOTTED_MAP_PYTHON to force a separate venv.',
        )

    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout_sec)),
            check=False,
        )
    except Exception as exc:
        return {
            'ok': False,
            'message': f'Failed launching dotted truth worker: {exc}',
            'python': python_bin,
        }

    stderr = str(completed.stderr or '').strip()
    stdout = str(completed.stdout or '').strip()
    if completed.returncode != 0:
        detail = stderr or stdout or f'exit code {completed.returncode}'
        return {
            'ok': False,
            'message': f'Dotted truth worker failed: {detail}',
            'python': python_bin,
            'returncode': int(completed.returncode),
        }

    return {
        'ok': True,
        'message': 'Dotted truth map generated',
        'python': python_bin,
        'stdout': stdout,
        'stderr': stderr,
    }


def ensure_dotted_truth_map(
    map_yaml_path: str,
    *,
    workspace_root: str = '',
    logger: Any = None,
) -> Dict[str, Any]:
    yaml_path = os.path.abspath(os.path.expanduser(str(map_yaml_path or '').strip()))
    if not yaml_path:
        return {'ok': False, 'message': 'Map yaml path is required'}
    if not os.path.exists(yaml_path):
        return {'ok': False, 'message': f'Map yaml not found: {yaml_path}'}

    try:
        with open(yaml_path, 'r', encoding='utf-8') as handle:
            map_yaml = yaml.safe_load(handle) or {}
    except Exception as exc:
        return {'ok': False, 'message': f'Failed reading map yaml: {exc}'}

    if not isinstance(map_yaml, dict):
        map_yaml = {}

    try:
        plan = build_dotted_truth_plan(yaml_path, map_yaml)
    except Exception as exc:
        return {'ok': False, 'message': str(exc)}

    source_image_path = plan['source_image_path']
    dotted_image_path = plan['dotted_image_path']

    if not os.path.exists(source_image_path):
        return {
            'ok': False,
            'message': f'Source image not found for dotted truth generation: {source_image_path}',
            'map_yaml_path': yaml_path,
        }

    meta = map_yaml.get(DOTTED_TRUTH_META_KEY)
    meta = dict(meta) if isinstance(meta, dict) else {}

    image_matches = os.path.abspath(_resolve_image_path(yaml_path, str(map_yaml.get('image', '') or ''))) == os.path.abspath(dotted_image_path)
    source_matches = str(meta.get('source_image', '') or '').strip() == plan['source_image_ref']
    dotted_matches = str(meta.get('image', '') or '').strip() == plan['dotted_image_ref']
    version_matches = str(meta.get('generator', '') or '').strip() == DOTTED_TRUTH_VERSION

    dotted_exists = os.path.exists(dotted_image_path)
    dotted_is_fresh = False
    if dotted_exists:
        try:
            dotted_is_fresh = os.path.getmtime(dotted_image_path) >= os.path.getmtime(source_image_path)
        except Exception:
            dotted_is_fresh = False

    generator_ran = False
    if not (dotted_exists and dotted_is_fresh and image_matches and source_matches and dotted_matches and version_matches):
        os.makedirs(os.path.dirname(dotted_image_path) or '.', exist_ok=True)
        result = run_dotted_truth_worker(
            input_path=source_image_path,
            output_path=dotted_image_path,
            workspace_root=workspace_root,
            logger=logger,
        )
        if not bool(result.get('ok')):
            result['map_yaml_path'] = yaml_path
            result['source_image_path'] = source_image_path
            result['dotted_image_path'] = dotted_image_path
            return result
        generator_ran = True

    updated = False
    if str(map_yaml.get('image', '') or '').strip() != plan['dotted_image_ref']:
        map_yaml['image'] = plan['dotted_image_ref']
        updated = True

    desired_meta = {
        'enabled': True,
        'generator': DOTTED_TRUTH_VERSION,
        'source_image': plan['source_image_ref'],
        'image': plan['dotted_image_ref'],
    }
    if map_yaml.get(DOTTED_TRUTH_META_KEY) != desired_meta:
        map_yaml[DOTTED_TRUTH_META_KEY] = desired_meta
        updated = True

    if updated:
        try:
            _write_yaml(yaml_path, map_yaml)
        except Exception as exc:
            return {'ok': False, 'message': f'Failed writing dotted truth yaml metadata: {exc}'}

    return {
        'ok': True,
        'message': 'Dotted truth map ready',
        'map_yaml_path': yaml_path,
        'source_image_path': source_image_path,
        'dotted_image_path': dotted_image_path,
        'image_updated': updated,
        'generator_ran': generator_ran,
    }
