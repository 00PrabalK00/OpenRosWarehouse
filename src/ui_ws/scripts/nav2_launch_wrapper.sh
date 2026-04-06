#!/bin/bash
# Wrapper for nav2_bringup that reads active map from config

set -u

# Resolve workspace root dynamically
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Script lives in scripts/ — go up one level to the workspace root.
UI_ROOT="${NEXT_UI_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"

# Resolve package share once (used for robust path fallback)
SHARE_DIR="$(python3 - <<'PY'
try:
    from ament_index_python.packages import get_package_share_directory
    print(get_package_share_directory('ugv_bringup'))
except Exception:
    print('')
PY
)"

# Get active map from config
CONFIG_FILE="$UI_ROOT/config/active_map_config.yaml"
if [ ! -f "$CONFIG_FILE" ] && [ -n "$SHARE_DIR" ] && [ -f "$SHARE_DIR/config/active_map_config.yaml" ]; then
    CONFIG_FILE="$SHARE_DIR/config/active_map_config.yaml"
fi

CONFIG_ROOT="$(cd "$(dirname "$CONFIG_FILE")" && pwd)"
MAPS_DIR="$CONFIG_ROOT/maps"
SHARE_MAPS_DIR=""
if [ -n "$SHARE_DIR" ]; then
    SHARE_MAPS_DIR="$SHARE_DIR/maps"
fi

DEFAULT_MAP="$MAPS_DIR/smr_map.yaml"
if [ ! -f "$DEFAULT_MAP" ] && [ -n "$SHARE_MAPS_DIR" ] && [ -f "$SHARE_MAPS_DIR/smr_map.yaml" ]; then
    DEFAULT_MAP="$SHARE_MAPS_DIR/smr_map.yaml"
fi

PARAMS_FILE="$UI_ROOT/config/nav2_params.yaml"
if [ ! -f "$PARAMS_FILE" ] && [ -n "$SHARE_DIR" ] && [ -f "$SHARE_DIR/config/nav2_params.yaml" ]; then
    PARAMS_FILE="$SHARE_DIR/config/nav2_params.yaml"
fi

# Read active map from config, fallback to default
if [ -f "$CONFIG_FILE" ]; then
    ACTIVE_MAP=$(grep "active_map:" "$CONFIG_FILE" | awk '{print $2}' | tr -d "'\"")
    if [ -z "$ACTIVE_MAP" ]; then
        ACTIVE_MAP="$DEFAULT_MAP"
    fi
else
    ACTIVE_MAP="$DEFAULT_MAP"
fi

# Normalize relative map paths robustly across src/ and install/ layouts.
if [[ "$ACTIVE_MAP" != /* ]]; then
    CANDIDATES=("$MAPS_DIR/$ACTIVE_MAP")
    if [ -n "$SHARE_MAPS_DIR" ]; then
        CANDIDATES+=("$SHARE_MAPS_DIR/$ACTIVE_MAP")
    fi
    CANDIDATES+=("$UI_ROOT/maps/$ACTIVE_MAP")

    RESOLVED=""
    for candidate in "${CANDIDATES[@]}"; do
        if [ -f "$candidate" ]; then
            RESOLVED="$candidate"
            break
        fi
    done

    if [ -n "$RESOLVED" ]; then
        ACTIVE_MAP="$RESOLVED"
    else
        # Keep first candidate for a deterministic error message below.
        ACTIVE_MAP="${CANDIDATES[0]}"
    fi
fi

if [ ! -f "$ACTIVE_MAP" ] && [ -f "$DEFAULT_MAP" ]; then
    echo "[nav2 wrapper] WARNING: Active map not found: $ACTIVE_MAP"
    echo "[nav2 wrapper] Falling back to default map: $DEFAULT_MAP"
    ACTIVE_MAP="$DEFAULT_MAP"
fi

if [ ! -f "$ACTIVE_MAP" ]; then
    echo "[nav2 wrapper] ERROR: Map file does not exist: $ACTIVE_MAP"
    echo "[nav2 wrapper] active_map_config: $CONFIG_FILE"
    exit 1
fi

echo "Using map: $ACTIVE_MAP"

echo "Using params: $PARAMS_FILE"
if [ ! -f "$PARAMS_FILE" ]; then
    echo "[nav2 wrapper] ERROR: Params file does not exist: $PARAMS_FILE"
    exit 1
fi

# Build a temporary params file with absolute paths
TMP_PARAMS_FILE="$(mktemp /tmp/nav2_params_XXXX.yaml)"
UI_ROOT="$UI_ROOT" ACTIVE_MAP="$ACTIVE_MAP" PARAMS_FILE="$PARAMS_FILE" TMP_PARAMS_FILE="$TMP_PARAMS_FILE" python3 - <<'PY'
import os
import yaml

params_file = os.environ["PARAMS_FILE"]
ui_root = os.environ["UI_ROOT"]
active_map = os.environ["ACTIVE_MAP"]
tmp_file = os.environ["TMP_PARAMS_FILE"]

bt_to_pose = os.path.join(ui_root, "config", "navigate_to_pose_simple.xml")
bt_through = os.path.join(ui_root, "config", "navigate_through_poses_simple.xml")

with open(params_file, "r") as f:
    data = yaml.safe_load(f) or {}

if "bt_navigator" in data:
    params = data["bt_navigator"].get("ros__parameters", {})
    params["default_nav_to_pose_bt_xml"] = bt_to_pose
    params["default_nav_through_poses_bt_xml"] = bt_through
    data["bt_navigator"]["ros__parameters"] = params

if "map_server" in data:
    params = data["map_server"].get("ros__parameters", {})
    params["yaml_filename"] = active_map
    data["map_server"]["ros__parameters"] = params

with open(tmp_file, "w") as f:
    yaml.safe_dump(data, f, default_flow_style=False)
PY

PARAMS_FILE="$TMP_PARAMS_FILE"

cleanup_tmp() {
    rm -f "$TMP_PARAMS_FILE"
}
trap cleanup_tmp EXIT

# Default to real robot time unless overridden.
USE_SIM_TIME="${NEXT_USE_SIM_TIME:-false}"
SIM_TIME_ARG=()
if [[ "$*" != *"use_sim_time:="* ]]; then
    SIM_TIME_ARG=("use_sim_time:=$USE_SIM_TIME")
fi

# Velocity routing is handled by twist_mux in zone_nav_ui.launch.py:
# Nav2 publishes /cmd_vel (navigation input), twist_mux emits /cmd_vel_out,
# and that output is remapped to /cmd_vel_mux_out; SafetyController is the
# only publisher to /wheel_controller/cmd_vel_unstamped.

# Launch Nav2 with active map (don't override if user already provided map)
if [[ "$*" == *"map:="* ]]; then
    # User provided map argument, use their values
    ros2 launch nav2_bringup bringup_launch.py "${SIM_TIME_ARG[@]}" use_composition:=False autostart:=True params_file:=$PARAMS_FILE "$@"
else
    # Use active map from config
    ros2 launch nav2_bringup bringup_launch.py "${SIM_TIME_ARG[@]}" use_composition:=False autostart:=True map:=$ACTIVE_MAP params_file:=$PARAMS_FILE "$@"
fi
