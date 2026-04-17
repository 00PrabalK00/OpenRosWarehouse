#!/usr/bin/env python3

import os
import yaml
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory, PackageNotFoundError


def _resolve_ui_root():
    env_root = os.getenv('NEXT_UI_ROOT')
    if env_root:
        return os.path.abspath(os.path.expanduser(env_root))
    try:
        return get_package_share_directory('ugv_bringup')
    except PackageNotFoundError:
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def _normalize_map_path(map_path, pkg_dir):
    if not map_path:
        return os.path.join(pkg_dir, 'maps', 'smr_map.yaml')
    if os.path.isabs(map_path):
        return map_path
    return os.path.join(pkg_dir, 'maps', map_path)


def generate_launch_description():
    pkg_dir = _resolve_ui_root()
    nav2_params_file = os.path.join(pkg_dir, 'config', 'nav2_params.yaml')
    
    # Read active map from Map Manager config (file lives under config/, not pkg root)
    active_map_config_file = os.path.join(pkg_dir, 'config', 'active_map_config.yaml')
    default_map = os.path.join(pkg_dir, 'maps', 'smr_map.yaml')
    
    try:
        if os.path.exists(active_map_config_file):
            with open(active_map_config_file, 'r') as f:
                config = yaml.safe_load(f) or {}
                map_file = _normalize_map_path(config.get('active_map', default_map), pkg_dir)
                print(f"[Navigation Launch] Using active map: {map_file}")
        else:
            map_file = _normalize_map_path(default_map, pkg_dir)
            print(f"[Navigation Launch] No active map config, using default: {map_file}")
    except Exception as e:
        print(f"[Navigation Launch] Error reading active map config: {e}, using default")
        map_file = _normalize_map_path(default_map, pkg_dir)
    
    use_sim_time = LaunchConfiguration('use_sim_time')
    autostart = LaunchConfiguration('autostart')
    bt_to_pose = os.path.join(pkg_dir, 'config', 'navigate_to_pose_simple.xml')
    bt_through = os.path.join(pkg_dir, 'config', 'navigate_through_poses_simple.xml')
    
    # Start bt_navigator LAST after others are active
    lifecycle_nodes = [
        'map_server',
        'planner_server',
        'controller_server',
        'local_costmap',
        'global_costmap',
        'behavior_server',
        'bt_navigator',
    ]
    remappings = [('/tf', 'tf'), ('/tf_static', 'tf_static')]
    
    return LaunchDescription([
        # Default false: real UGV does not use /clock from a simulator.
        # Override with use_sim_time:=true when running in Gazebo/Isaac.
        DeclareLaunchArgument('use_sim_time', default_value='false'),
        DeclareLaunchArgument('autostart', default_value='true'),
        
        Node(package='nav2_map_server', executable='map_server', name='map_server', output='screen',
             parameters=[nav2_params_file, {'yaml_filename': map_file}], remappings=remappings),
        
        Node(package='nav2_planner', executable='planner_server', name='planner_server', output='screen',
             parameters=[nav2_params_file], remappings=remappings),
        
        Node(package='nav2_controller', executable='controller_server', name='controller_server', output='screen',
             parameters=[nav2_params_file], remappings=remappings),

        # NOTE: In ROS 2 Humble the executable name is `nav2_costmap_2d` (not `costmap_2d`).
        Node(package='nav2_costmap_2d', executable='nav2_costmap_2d', name='local_costmap', output='screen',
             parameters=[nav2_params_file], remappings=remappings),

        Node(package='nav2_costmap_2d', executable='nav2_costmap_2d', name='global_costmap', output='screen',
             parameters=[nav2_params_file], remappings=remappings),
        
        Node(package='nav2_behaviors', executable='behavior_server', name='behavior_server', output='screen',
             parameters=[nav2_params_file], remappings=remappings),
        
        Node(package='nav2_bt_navigator', executable='bt_navigator', name='bt_navigator', output='screen',
             parameters=[nav2_params_file, {
                 'default_nav_to_pose_bt_xml': bt_to_pose,
                 'default_nav_through_poses_bt_xml': bt_through,
             }], remappings=remappings),
        
        Node(package='nav2_lifecycle_manager', executable='lifecycle_manager', name='lifecycle_manager_navigation',
             output='screen', parameters=[{'use_sim_time': use_sim_time, 'autostart': autostart, 
             'node_names': lifecycle_nodes, 'bond_timeout': 10.0, 'attempt_respawn_reconnection': True,
             'bond_respawn_max_duration': 10.0}])
    ])
