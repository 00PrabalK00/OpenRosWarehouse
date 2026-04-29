#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="/home/next/testBuild"
# Default to launch-only so callers do not pay for an extra full workspace
# build when they already built explicitly. Set NEXT_AUTO_BUILD=1 to opt in.
AUTO_BUILD="${NEXT_AUTO_BUILD:-0}"

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

set +u
source /opt/ros/humble/setup.bash
source /home/next/next_ros2/install/local_setup.bash
source /home/next/next_EKF/install/local_setup.bash
set -u

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

exec ros2 launch ugv_bringup zone_nav_ui.launch.py
