"""Standalone launch for the PGV Matrix Tag localization stack.

Brings up the driver, tag-map server, localizer and diagnostics together. The
localizer publishes /pgv/corrected_pose and /initialpose so the navigation
stack can use Matrix Tags as localization anchors; mission and route decisions
remain outside this launch.
This is intentionally a separate launch file - it is NOT part of the main
bringup graph. Run it on its own:

    ros2 launch next_ros2ws_pgv pgv_localization.launch.py

Override the device port, the tag map or the params on the command line:

    ros2 launch next_ros2ws_pgv pgv_localization.launch.py \
        port:=/dev/next/pgv \
        tag_map:=/abs/path/tag_map.yaml
"""

import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    pkg_share = get_package_share_directory("next_ros2ws_pgv")
    default_params = os.path.join(pkg_share, "config", "pgv_localization.yaml")
    default_tag_map = os.path.join(pkg_share, "config", "tag_map.yaml")

    params_arg = DeclareLaunchArgument("params_file", default_value=default_params)
    tag_map_arg = DeclareLaunchArgument("tag_map", default_value=default_tag_map)
    port_arg = DeclareLaunchArgument("port", default_value="/dev/next/pgv")
    mode_arg = DeclareLaunchArgument("localization_mode", default_value="amcl")
    route_arg = DeclareLaunchArgument("launch_route_follower", default_value="true")

    params_file = LaunchConfiguration("params_file")
    tag_map = LaunchConfiguration("tag_map")
    port = LaunchConfiguration("port")
    localization_mode = LaunchConfiguration("localization_mode")
    launch_route_follower = LaunchConfiguration("launch_route_follower")

    driver = Node(
        package="next_ros2ws_pgv",
        executable="pgv_reader",
        name="pgv_reader",
        output="screen",
        parameters=[params_file, {"port": port}],
    )

    tag_map_server = Node(
        package="next_ros2ws_pgv",
        executable="tag_map_server",
        name="tag_map_server",
        output="screen",
        parameters=[{"tag_map_path": tag_map}],
    )

    localizer = Node(
        package="next_ros2ws_pgv",
        executable="pgv_localizer",
        name="pgv_localizer",
        output="screen",
        parameters=[params_file, {
            "tag_map_path": tag_map,
            "localization_mode": localization_mode,
        }],
    )

    diagnostics = Node(
        package="next_ros2ws_pgv",
        executable="pgv_diagnostics",
        name="pgv_diagnostics",
        output="screen",
        parameters=[params_file, {"tag_map_path": tag_map}],
    )

    route_follower = Node(
        package="next_ros2ws_pgv",
        executable="route_follower",
        name="route_follower",
        output="screen",
        parameters=[params_file, {"tag_map_path": tag_map}],
        condition=IfCondition(launch_route_follower),
    )

    return LaunchDescription([
        params_arg,
        tag_map_arg,
        port_arg,
        mode_arg,
        route_arg,
        driver,
        tag_map_server,
        localizer,
        diagnostics,
        route_follower,
    ])
