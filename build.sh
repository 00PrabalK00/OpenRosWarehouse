#!/bin/bash
# Quick build script for the Next UI workspace

cd "$(dirname "$0")"
colcon build --symlink-install "$@"
