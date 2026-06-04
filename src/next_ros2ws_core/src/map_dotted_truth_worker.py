#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from PIL import Image

def find_dotted_map():
    """Attempt to locate and import dotted_map using multiple strategies."""
    
    # 1. Try standard installed package first
    try:
        from next_ros2ws_web.dotted_map import generate_dotted_map
        return generate_dotted_map, "installed package (next_ros2ws_web.dotted_map)"
    except ImportError:
        pass

    # 2. Try environment variable for workspace root
    workspace_root = os.getenv('NEXT_WORKSPACE_ROOT', '')
    if workspace_root:
        candidate = os.path.join(workspace_root, 'src', 'next_ros2ws_web', 'src')
        if os.path.isdir(candidate):
            if candidate not in sys.path:
                sys.path.insert(0, candidate)
            try:
                from dotted_map import generate_dotted_map
                return generate_dotted_map, f"workspace_root env: {candidate}"
            except ImportError:
                pass

    # 3. Try relative paths from script location (source tree discovery)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        # In source tree: src/next_ros2ws_core/src/ -> src/next_ros2ws_web/src/
        os.path.join(script_dir, '..', '..', 'next_ros2ws_web', 'src'),
        # In install tree (if symlinked): 
        os.path.join(script_dir, '..', '..', '..', '..', '..', 'src', 'next_ros2ws_web', 'src'),
    ]
    
    for cand in candidates:
        cand = os.path.abspath(cand)
        if os.path.isdir(cand):
            if cand not in sys.path:
                sys.path.insert(0, cand)
            try:
                from dotted_map import generate_dotted_map
                return generate_dotted_map, f"relative path: {cand}"
            except ImportError:
                pass

    # 4. Final attempt: maybe it's already in sys.path
    try:
        from dotted_map import generate_dotted_map
        return generate_dotted_map, "already in sys.path"
    except ImportError:
        pass

    return None, "not found"

generate_dotted_map, discovery_method = find_dotted_map()

if not generate_dotted_map:
    print(f"CRITICAL: Could not find dotted_map module. (Method: {discovery_method})")
    print(f"DEBUG: sys.path = {sys.path}")
    print(f"DEBUG: __file__ = {__file__}")
    sys.exit(1)

# print(f"DEBUG: Loaded dotted_map via {discovery_method}")


def convert_map(input_path: Path, output_path: Path) -> None:
    raw_image = Image.open(input_path)
    
    # We use the new generate_dotted_map.
    # In practice, map_resolution_m_per_px could be parsed from the yaml, 
    # but the defaults in generate_dotted_map are robust enough for most cases.
    dotted_image = generate_dotted_map(raw_image, preset="balanced")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dotted_image.save(output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description='Dotted Map Generator Worker')
    parser.add_argument('--input', required=True, type=str, help='Path to input PGM image')
    parser.add_argument('--output', required=True, type=str, help='Path to output PGM image')
    args = parser.parse_args()

    convert_map(Path(args.input), Path(args.output))


if __name__ == '__main__':
    main()
