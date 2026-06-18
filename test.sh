#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="/home/next/testBuild"
# Default to launch-only so callers do not pay for an extra full workspace
# build when they already built explicitly. Set NEXT_AUTO_BUILD=1 to opt in.
AUTO_BUILD="${NEXT_AUTO_BUILD:-0}"
BLOCKED_OVERLAY_ROOT="/home/next/next_UI/install"

# Start from a clean ROS overlay state so old workspaces like next_UI/my_bot
# cannot leak into this bringup and reintroduce stale launch/config behavior.
unset AMENT_PREFIX_PATH
unset COLCON_PREFIX_PATH
unset CMAKE_PREFIX_PATH
unset PYTHONPATH
unset ROS_PACKAGE_PATH
unset COLCON_CURRENT_PREFIX
unset NEXT_UI_ROOT
unset NEXT_BRINGUP_PACKAGE
unset NEXT_ROBOT_PACKAGE

strip_overlay_prefix() {
  local var_name="$1"
  local blocked_root="$2"
  local current_value="${!var_name-}"
  local filtered_parts=()
  local item=""
  local old_ifs="$IFS"
  IFS=':'
  for item in $current_value; do
    [[ -z "$item" ]] && continue
    case "$item" in
      "$blocked_root"|"$blocked_root"/*)
        continue
        ;;
    esac
    filtered_parts+=("$item")
  done
  IFS="$old_ifs"
  printf -v "$var_name" '%s' "$(IFS=:; echo "${filtered_parts[*]}")"
  export "$var_name"
}

strip_blocked_overlay() {
  local blocked_root="$1"
  strip_overlay_prefix AMENT_PREFIX_PATH "$blocked_root"
  strip_overlay_prefix COLCON_PREFIX_PATH "$blocked_root"
  strip_overlay_prefix CMAKE_PREFIX_PATH "$blocked_root"
  strip_overlay_prefix PYTHONPATH "$blocked_root"
  strip_overlay_prefix LD_LIBRARY_PATH "$blocked_root"
  strip_overlay_prefix PATH "$blocked_root"
}

set +u
source /opt/ros/humble/setup.bash
source /home/next/next_ros2/install/local_setup.bash
source /home/next/next_EKF/install/local_setup.bash
set -u

strip_blocked_overlay "$BLOCKED_OVERLAY_ROOT"

if [[ "$AUTO_BUILD" != "0" ]]; then
  echo "[test.sh] Building workspace before launch: $WORKSPACE_ROOT"
  (
    cd "$WORKSPACE_ROOT"
    colcon build --symlink-install
  )
fi

set +u
source "$WORKSPACE_ROOT/install/local_setup.bash"
set -u

strip_blocked_overlay "$BLOCKED_OVERLAY_ROOT"

# The PGV Matrix Tag stack (reader + tag map + localizer + diagnostics) is now
# part of the bringup launch graph (zone_nav_ui.launch.py, enable_pgv_localization
# defaults true). Do NOT also start a standalone reader here: two readers on the
# same RS-485 port interleave query/response frames, corrupting the data and
# teleporting localization. Override the port/tag map via the launch args.

# Start Node-RED (workflows UI on :1880) unless something already listens there.
# Backgrounded so the foreground ros2 launch below stays the main process.
NODE_RED_PORT="${NODE_RED_PORT:-1880}"
NODE_RED_LOG="${NODE_RED_LOG:-$WORKSPACE_ROOT/node-red.log}"
if command -v node-red >/dev/null 2>&1; then
  if ss -ltnH "sport = :$NODE_RED_PORT" 2>/dev/null | grep -q ":$NODE_RED_PORT"; then
    echo "[test.sh] Node-RED already running on :$NODE_RED_PORT; skipping launch."
  else
    echo "[test.sh] Launching Node-RED on :$NODE_RED_PORT (log: $NODE_RED_LOG)"
    nohup node-red -p "$NODE_RED_PORT" >"$NODE_RED_LOG" 2>&1 &
  fi
else
  echo "[test.sh] node-red not found on PATH; skipping Node-RED launch." >&2
fi

exec ros2 launch ugv_bringup zone_nav_ui.launch.py
