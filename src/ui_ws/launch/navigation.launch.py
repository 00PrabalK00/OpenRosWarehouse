#!/usr/bin/env python3

import os
import tempfile
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


def _truthy_env(name: str) -> bool:
    return str(os.getenv(name, '')).strip().lower() in {'1', 'true', 'yes', 'on'}


def _write_pgv_blank_map():
    """Create a large free-space map for PGV-only navigation.

    PGV mode localizes from absolute Matrix Tag coordinates, so it should not
    inherit occupancy-map walls/edges from the active AGV navigation map. Nav2
    still needs a finite map/costmap canvas, so provide a blank one.
    """
    resolution = float(os.getenv('NEXT_PGV_BLANK_MAP_RESOLUTION', '0.05'))
    width_m = float(os.getenv('NEXT_PGV_BLANK_MAP_WIDTH_M', '80.0'))
    height_m = float(os.getenv('NEXT_PGV_BLANK_MAP_HEIGHT_M', '80.0'))
    origin_x = float(os.getenv('NEXT_PGV_BLANK_MAP_ORIGIN_X', str(-width_m / 2.0)))
    origin_y = float(os.getenv('NEXT_PGV_BLANK_MAP_ORIGIN_Y', str(-height_m / 2.0)))

    width_px = max(1, int(round(width_m / resolution)))
    height_px = max(1, int(round(height_m / resolution)))
    base = os.getenv('NEXT_PGV_BLANK_MAP_BASENAME', '/tmp/next_pgv_blank_map')
    pgm_path = f'{base}.pgm'
    yaml_path = f'{base}.yaml'

    if not os.path.exists(pgm_path):
        with open(pgm_path, 'wb') as f:
            f.write(f'P5\n{width_px} {height_px}\n255\n'.encode('ascii'))
            f.write(bytes([254]) * width_px * height_px)

    map_doc = {
        'image': pgm_path,
        'mode': 'trinary',
        'resolution': resolution,
        'origin': [origin_x, origin_y, 0.0],
        'negate': 0,
        'occupied_thresh': 0.65,
        'free_thresh': 0.25,
    }
    with open(yaml_path, 'w') as f:
        yaml.safe_dump(map_doc, f, default_flow_style=False)

    return yaml_path, origin_x, origin_y, width_m, height_m


def _write_pgv_nav2_params(nav2_params_file, blank_map_file, origin_x, origin_y, width_m, height_m):
    with open(nav2_params_file, 'r') as f:
        data = yaml.safe_load(f) or {}

    odom_topic = os.getenv('NEXT_PGV_ODOM_TOPIC', '/wheel_controller/odom')

    map_params = data.setdefault('map_server', {}).setdefault('ros__parameters', {})
    map_params['yaml_filename'] = blank_map_file

    bt_params = data.setdefault('bt_navigator', {}).setdefault('ros__parameters', {})
    bt_params['odom_topic'] = odom_topic

    controller_params = data.setdefault('controller_server', {}).setdefault('ros__parameters', {})
    controller_params['odom_topic'] = odom_topic
    # PGV POI paths may begin with an in-place heading alignment from the last
    # tag-corrected pose. Do not let Nav2's generic progress checker abort just
    # because the robot is rotating before translating.
    progress_params = controller_params.setdefault('progress_checker', {})
    progress_params['required_movement_radius'] = 0.001
    progress_params['movement_time_allowance'] = 60.0
    follow_params = controller_params.setdefault('FollowPath', {})
    follow_params['abort_on_progress_stall'] = False

    global_params = (
        data.setdefault('global_costmap', {})
        .setdefault('global_costmap', {})
        .setdefault('ros__parameters', {})
    )
    global_params['origin_x'] = origin_x
    global_params['origin_y'] = origin_y
    global_params['width'] = int(round(width_m))
    global_params['height'] = int(round(height_m))
    global_params['track_unknown_space'] = False
    global_params['plugins'] = ['static_layer', 'inflation_layer', 'keepout_filter']

    local_params = (
        data.setdefault('local_costmap', {})
        .setdefault('local_costmap', {})
        .setdefault('ros__parameters', {})
    )
    # In PGV mode lidar remains enforced by safety_controller. Keep Nav2's
    # local canvas free of scan-derived "walls" so Matrix Tag navigation is not
    # coupled to occupancy/edge detection.
    local_params['plugins'] = ['inflation_layer', 'keepout_filter']

    fd, path = tempfile.mkstemp(prefix='pgv_nav2_params_', suffix='.yaml')
    os.close(fd)
    with open(path, 'w') as f:
        yaml.safe_dump(data, f, default_flow_style=False)
    return path


def generate_launch_description():
    pkg_dir = _resolve_ui_root()
    nav2_params_file = os.path.join(pkg_dir, 'config', 'nav2_params.yaml')
    pgv_nav_mode = _truthy_env('NEXT_PGV_NAV_MODE')
    
    if pgv_nav_mode:
        map_file, origin_x, origin_y, width_m, height_m = _write_pgv_blank_map()
        nav2_params_file = _write_pgv_nav2_params(
            nav2_params_file, map_file, origin_x, origin_y, width_m, height_m)
        print(
            f"[Navigation Launch] PGV mode: using blank canvas {map_file} "
            f"bounds=({origin_x:.2f},{origin_y:.2f}) {width_m:.1f}x{height_m:.1f}m"
        )
    else:
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
    map_arg = LaunchConfiguration('map')
    
    from launch.substitutions import PythonExpression
    # In PGV mode, we always use the blank map generated above.
    # Otherwise, use the 'map' launch argument if provided, falling back to the config-derived map_file.
    map_to_use = map_file if pgv_nav_mode else PythonExpression([
        "'", map_arg, "' if '", map_arg, "' != '' else '", map_file, "'"
    ])

    bt_to_pose = os.path.join(pkg_dir, 'config', 'navigate_to_pose_simple.xml')
    bt_through = os.path.join(pkg_dir, 'config', 'navigate_through_poses_simple.xml')
    
    # Start bt_navigator LAST after others are active
    lifecycle_nodes = [
        'map_server',
        'planner_server',
        'controller_server',
        'behavior_server',
        'bt_navigator',
    ]
    remappings = [('/tf', 'tf'), ('/tf_static', 'tf_static')]
    
    ld = LaunchDescription([
        # Default false: real UGV does not use /clock from a simulator.
        # Override with use_sim_time:=true when running in Gazebo/Isaac.
        DeclareLaunchArgument('map', default_value=''),
        DeclareLaunchArgument('use_sim_time', default_value='false'),
        DeclareLaunchArgument('autostart', default_value='true'),
        
        Node(package='nav2_map_server', executable='map_server', name='map_server', output='screen',
             parameters=[nav2_params_file, {'yaml_filename': map_to_use}], remappings=remappings),
        
        Node(package='nav2_planner', executable='planner_server', name='planner_server', output='screen',
             parameters=[nav2_params_file], remappings=remappings),
        
        Node(package='nav2_controller', executable='controller_server', name='controller_server', output='screen',
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

    # PGV localization bringup (if requested)
    if pgv_nav_mode:
        from launch.actions import IncludeLaunchDescription
        from launch.launch_description_sources import PythonLaunchDescriptionSource
        try:
            pgv_share = get_package_share_directory('next_ros2ws_pgv')
            pgv_args = {
                'port': os.getenv('NEXT_PGV_PORT', '/dev/next/pgv'),
                'localization_mode': 'pure_pgv',
                'launch_route_follower': 'true',
            }
            pgv_tag_map = str(os.getenv('NEXT_PGV_TAG_MAP', '') or '').strip()
            if pgv_tag_map:
                pgv_args['tag_map'] = pgv_tag_map
            ld.add_action(IncludeLaunchDescription(
                PythonLaunchDescriptionSource(
                    os.path.join(pgv_share, 'launch', 'pgv_localization.launch.py')
                ),
                launch_arguments=pgv_args.items(),
            ))
            print("[Navigation Launch] PGV mode: including next_ros2ws_pgv bringup.")
        except Exception as e:
            print(f"[Navigation Launch] ERROR: Could not find next_ros2ws_pgv: {e}")

    # Reflector localization bringup (if requested)
    if _truthy_env('NEXT_REFLECTOR_NAV_MODE'):
        from launch.actions import IncludeLaunchDescription
        from launch.launch_description_sources import PythonLaunchDescriptionSource
        try:
            reflector_share = get_package_share_directory('next_ros2ws_reflector')
            ld.add_action(IncludeLaunchDescription(
                PythonLaunchDescriptionSource(
                    os.path.join(reflector_share, 'launch', 'reflector_localization.launch.py')
                ),
                launch_arguments={
                    'use_sim_time': use_sim_time,
                    'odom_topic': '/odometry/filtered',
                    'reflector_map_file': map_to_use,
                }.items()
            ))
            print("[Navigation Launch] Reflector mode: including next_ros2ws_reflector bringup.")
        except Exception as e:
            print(f"[Navigation Launch] ERROR: Could not find next_ros2ws_reflector: {e}")

    return ld
