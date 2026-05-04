#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from PIL import Image

# Add next_ros2ws_web/src to sys.path so we can import the new dotted_map.py
web_src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'next_ros2ws_web', 'src'))
if web_src_dir not in sys.path:
    sys.path.insert(0, web_src_dir)

try:
    from dotted_map import generate_dotted_map
except ImportError as e:
    print(f"Error importing new dotted_map logic: {e}")
    sys.exit(1)


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
