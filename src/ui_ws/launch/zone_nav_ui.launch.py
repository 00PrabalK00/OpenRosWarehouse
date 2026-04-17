#!/usr/bin/env python3

import os
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory, PackageNotFoundError


def generate_launch_description():
    use_sim_time = LaunchConfiguration('use_sim_time')
    stack_startup_mode = LaunchConfiguration('stack_startup_mode')
    robot_namespace = LaunchConfiguration('robot_namespace')
    db_path = LaunchConfiguration('db_path')
    enable_db_viewer = LaunchConfiguration('enable_db_viewer')
    db_viewer_port = LaunchConfiguration('db_viewer_port')
    enable_auto_reloc = LaunchConfiguration('enable_auto_reloc')
    enable_shelf_detector = LaunchConfiguration('enable_shelf_detector')
    enable_ui_scan_overlay_processing = LaunchConfiguration('enable_ui_scan_overlay_processing')
    rosbridge_port = LaunchConfiguration('rosbridge_port')

    pkg_share = get_package_share_directory('ugv_bringup')
    robot_yaml = os.path.join(pkg_share, 'config', 'robot.yaml')
    twist_mux_yaml = os.path.join(pkg_share, 'config', 'twist_mux.yaml')
    auto_reloc_params = None
    try:
        tools_share = get_package_share_directory('next_ros2ws_tools')
        for candidate in (
            os.path.join(tools_share, 'config', 'relocalization_params.yaml'),
            os.path.join(tools_share, 'relocalization_params.yaml'),
        ):
            if os.path.exists(candidate):
                auto_reloc_params = candidate
                break
    except PackageNotFoundError:
        auto_reloc_params = None
    if auto_reloc_params is None:
        workspace_root = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(__file__)
                )
            )
        )
        for candidate in (
            os.path.join(
                workspace_root,
                'src',
                'next_ros2ws_tools',
                'src',
                'relocalization_params.yaml',
            ),
            os.path.join(
                workspace_root,
                'src',
                'next_ros2ws_tools',
                'config',
                'relocalization_params.yaml',
            ),
        ):
            if os.path.exists(candidate):
                auto_reloc_params = candidate
                break

    auto_reloc_param_sources = []
    if auto_reloc_params is not None:
        auto_reloc_param_sources.append(auto_reloc_params)

    launch_items = [
        DeclareLaunchArgument(
            'use_sim_time',
            default_value='false',
            description='Use simulation clock if true'
        ),
        DeclareLaunchArgument(
            'stack_startup_mode',
            default_value='stop',
            description='Auto-start stack mode at launch: nav | slam | stop'
        ),
        DeclareLaunchArgument(
            'robot_namespace',
            default_value='',
            description='Optional robot namespace prefix for core endpoints (empty keeps global names)'
        ),
        DeclareLaunchArgument(
            'db_path',
            default_value='~/DB/robot_data.db',
            description='SQLite database path shared by DB-backed services'
        ),
        DeclareLaunchArgument(
            'enable_db_viewer',
            default_value='false',
            description='Launch db_viewer web UI alongside core stack'
        ),
        DeclareLaunchArgument(
            'db_viewer_port',
            default_value='8000',
            description='HTTP port used by db_viewer'
        ),
        DeclareLaunchArgument(
            'enable_auto_reloc',
            default_value='true',
            description='Launch the auto relocalization helper'
        ),
        DeclareLaunchArgument(
            'enable_shelf_detector',
            default_value='true',
            description='Launch shelf / distance detection helper nodes'
        ),
        DeclareLaunchArgument(
            'enable_ui_scan_overlay_processing',
            default_value='true',
            description='Enable expensive scan-overlay + scan-to-map processing inside the web UI node'
        ),
        DeclareLaunchArgument(
            'rosbridge_port',
            default_value='9090',
            description='WebSocket port exposed by rosbridge for browser ROSLIB clients'
        ),

        # Core zone/state ownership
        # Zone manager — all tunable params in config/robot.yaml [zone_manager]
        Node(
            package='next_ros2ws_core',
            executable='zone_manager',
            name='zone_manager',
            output='screen',
            parameters=[robot_yaml, {
                'use_sim_time': use_sim_time,
                'robot_namespace': robot_namespace,
                'db_path': db_path,
            }]
        ),

        Node(
            package='next_ros2ws_core',
            executable='navigation_arbitrator',
            name='navigation_arbitrator',
            output='screen',
            parameters=[{
                'use_sim_time': use_sim_time,
                'robot_namespace': robot_namespace,
            }]
        ),

        Node(
            package='next_ros2ws_core',
            executable='control_mode_manager',
            name='control_mode_manager',
            output='screen',
            parameters=[{'use_sim_time': use_sim_time}]
        ),

        Node(
            package='next_ros2ws_core',
            executable='mission_manager',
            name='mission_manager',
            output='screen',
            parameters=[{'use_sim_time': use_sim_time, 'db_path': db_path}]
        ),

        # Safety — all tunable params in config/robot.yaml [safety_controller]
        Node(
            package='next_ros2ws_core',
            executable='safety_controller',
            name='safety_controller',
            output='screen',
            parameters=[robot_yaml, {'use_sim_time': use_sim_time}]
        ),

        Node(
            package='next_ros2ws_tools',
            executable='scan_merger',
            name='scan_merger',
            output='screen',
            parameters=[robot_yaml, {'use_sim_time': use_sim_time}]
        ),

        # Shelf detector — simple intensity-based reflector detection
        Node(
            package='next2_shelf_simple',
            executable='shelf_detector',
            name='shelf_detector',
            output='screen',
            parameters=[robot_yaml, {'use_sim_time': use_sim_time}],
            condition=IfCondition(enable_shelf_detector),
        ),

        Node(
            package='next_ros2ws_tools',
            executable='auto_reloc',
            name='auto_reloc',
            output='screen',
            parameters=auto_reloc_param_sources + [
                {
                    'use_sim_time': use_sim_time,
                    'base_frame': 'base_footprint',
                    # Use both physical lidars directly for relocalization.
                    'scan_topic': '/scan',
                    'scan2_topic': '/scan2',
                    # Global CSM behavior
                    'use_scan_bundle': True,
                    'bundle_scan_count': 6,
                    'bundle_window_sec': 1.2,
                    'bundle_motion_compensate_odom': True,
                    # Disable automatic relocalization on boot; trigger manually via /auto_relocate.
                    'auto_relocate_on_startup': False,
                    # Keep startup spin disabled by default with dual-lidar 360 coverage.
                    'startup_spin_before_match': False,
                }
            ],
            condition=IfCondition(enable_auto_reloc),
        ),

        # Map/state services
        Node(
            package='next_ros2ws_core',
            executable='map_manager',
            name='map_manager',
            output='screen',
            parameters=[{'use_sim_time': use_sim_time}]
        ),

        Node(
            package='next_ros2ws_core',
            executable='map_layer_manager',
            name='map_layer_manager',
            output='screen',
            parameters=[{'use_sim_time': use_sim_time, 'db_path': db_path}]
        ),

        Node(
            package='next_ros2ws_core',
            executable='map_editor_manager',
            name='map_editor_manager',
            output='screen',
            parameters=[{'use_sim_time': use_sim_time}]
        ),

        Node(
            package='next_ros2ws_core',
            executable='settings_manager',
            name='settings_manager',
            output='screen',
            parameters=[{'use_sim_time': use_sim_time, 'db_path': db_path}]
        ),

        # Stack manager — startup_mode default in config/robot.yaml [stack_manager]
        Node(
            package='next_ros2ws_core',
            executable='stack_manager',
            name='stack_manager',
            output='screen',
            parameters=[robot_yaml, {
                'use_sim_time': use_sim_time,
                'startup_mode': stack_startup_mode,
            }]
        ),

        # Keepout/speed filter masks for Nav2 costmap filters.
        Node(
            package='next_ros2ws_core',
            executable='keepout_zone_publisher',
            name='keepout_zone_publisher',
            output='screen',
            parameters=[robot_yaml, {'use_sim_time': use_sim_time, 'db_path': db_path}]
        ),

        # Web UI
        Node(
            package='rosbridge_server',
            executable='rosbridge_websocket',
            name='rosbridge_websocket',
            output='screen',
            respawn=True,
            respawn_delay=2.0,
            parameters=[{
                'use_sim_time': use_sim_time,
                'port': rosbridge_port,
                'address': '0.0.0.0',
            }]
        ),

        Node(
            package='next_ros2ws_web',
            executable='zone_web_ui',
            name='zone_web_ui',
            output='screen',
            respawn=True,
            respawn_delay=2.0,
            parameters=[{
                'use_sim_time': use_sim_time,
                'db_path': db_path,
                'enable_scan_overlay_processing': enable_ui_scan_overlay_processing,
            }]
        ),

        # SQLite database viewer (http://localhost:<db_viewer_port>)
        Node(
            package='next_ros2ws_tools',
            executable='db_viewer',
            name='db_viewer',
            output='screen',
            condition=IfCondition(enable_db_viewer),
            arguments=[
                '--db-path',
                db_path,
                '--port',
                db_viewer_port,
            ],
        ),
    ]

    # twist_mux is a hard production dependency: without it /cmd_vel_mux_out is
    # never published and safety_controller receives no velocity commands, leaving
    # the robot unable to move.  This call raises PackageNotFoundError at
    # generate_launch_description() time (before any nodes start) if the package
    # is missing, giving an unambiguous failure.
    # Install: sudo apt install ros-humble-twist-mux
    get_package_share_directory('twist_mux')

    launch_items.extend([
        Node(
            package='next_ros2ws_core',
            executable='mode_switcher',
            name='mode_switcher',
            output='screen',
            parameters=[{'use_sim_time': use_sim_time}]
        ),
        # Velocity priorities and lock topics from the dedicated twist_mux.yaml.
        # Using robot.yaml here caused a priority mismatch on lock entries that
        # blocked cmd_vel in autonomous mode ("Failed to make progress" loop).
        Node(
            package='twist_mux',
            executable='twist_mux',
            name='twist_mux',
            output='screen',
            parameters=[twist_mux_yaml, {'use_sim_time': use_sim_time}],
            remappings=[('/cmd_vel_out', '/cmd_vel_mux_out')]
        ),
    ])

    return LaunchDescription(launch_items)
