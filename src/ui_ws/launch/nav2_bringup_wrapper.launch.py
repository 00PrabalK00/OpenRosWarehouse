import os
import xml.etree.ElementTree as ET
import yaml
from launch import LaunchDescription
from launch.actions import IncludeLaunchDescription, DeclareLaunchArgument
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration
from ament_index_python.packages import get_package_share_directory, PackageNotFoundError
from nav2_common.launch import RewrittenYaml


def _package_name_from_xml(package_xml_path):
    try:
        root = ET.parse(package_xml_path).getroot()
    except Exception:
        return ''
    name_node = root.find('name')
    if name_node is None:
        return ''
    return str(name_node.text or '').strip()


def _candidate_bringup_packages():
    for env_name in ('NEXT_BRINGUP_PACKAGE', 'NEXT_ROBOT_PACKAGE'):
        value = str(os.getenv(env_name, '') or '').strip()
        if value:
            yield value

    file_dir = os.path.abspath(os.path.dirname(__file__))
    package_xml = os.path.abspath(os.path.join(file_dir, '..', 'package.xml'))
    parsed_name = _package_name_from_xml(package_xml)
    if parsed_name:
        yield parsed_name

    yield 'ugv_bringup'


def _resolve_ui_root():
    env_root = os.getenv('NEXT_UI_ROOT')
    if env_root:
        return os.path.abspath(os.path.expanduser(env_root))

    seen = set()
    for package_name in _candidate_bringup_packages():
        if not package_name or package_name in seen:
            continue
        seen.add(package_name)
        try:
            return get_package_share_directory(package_name)
        except PackageNotFoundError:
            continue

    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def _normalize_map_path(map_path, pkg_dir):
    if not map_path:
        return os.path.join(pkg_dir, 'maps', 'smr_map.yaml')
    if os.path.isabs(map_path):
        return map_path
    return os.path.join(pkg_dir, 'maps', map_path)


def generate_launch_description():
    pkg_dir = _resolve_ui_root()
    
    # Read active map from Map Manager config
    active_map_config_file = os.path.join(pkg_dir, 'config', 'active_map_config.yaml')
    default_map = os.path.join(pkg_dir, 'maps', 'smr_map.yaml')
    
    try:
        if os.path.exists(active_map_config_file):
            with open(active_map_config_file, 'r') as f:
                config = yaml.safe_load(f) or {}
                map_file = _normalize_map_path(config.get('active_map', default_map), pkg_dir)
                print(f"[Navigation Bringup Wrapper] Using active map: {map_file}")
        else:
            map_file = _normalize_map_path(default_map, pkg_dir)
            print(f"[Navigation Bringup Wrapper] No active map config, using default: {map_file}")
    except Exception as e:
        print(f"[Navigation Bringup Wrapper] Error reading active map config: {e}, using default")
        map_file = _normalize_map_path(default_map, pkg_dir)
    
    # Resolve nav2_bringup launch directory via ament index rather than hardcoding
    # the ROS distro install path (which breaks on Iron, non-/opt installs, Docker, etc.)
    nav2_bringup_dir = os.path.join(get_package_share_directory('nav2_bringup'), 'launch')
    
    # Declare launch arguments
    use_sim_time = LaunchConfiguration('use_sim_time')
    params_file = LaunchConfiguration('params_file')

    bt_to_pose = os.path.join(pkg_dir, 'config', 'navigate_to_pose_simple.xml')
    bt_through = os.path.join(pkg_dir, 'config', 'navigate_through_poses_simple.xml')
    configured_params = RewrittenYaml(
        source_file=params_file,
        root_key='',
        param_rewrites={
            'yaml_filename': map_file,
            'default_nav_to_pose_bt_xml': bt_to_pose,
            'default_nav_through_poses_bt_xml': bt_through,
        },
        convert_types=True
    )
    
    return LaunchDescription([
        DeclareLaunchArgument('use_sim_time', default_value='false',
                            description='Use simulation time (set true for Gazebo)'),
        DeclareLaunchArgument('params_file', 
                            default_value=os.path.join(pkg_dir, 'config', 'nav2_params.yaml'),
                            description='Full path to the ROS2 parameters file to use'),
        
        # Include nav2_bringup with the dynamically determined map
        IncludeLaunchDescription(
            PythonLaunchDescriptionSource(os.path.join(nav2_bringup_dir, 'bringup_launch.py')),
            launch_arguments={
                'map': map_file,
                'use_sim_time': use_sim_time,
                'params_file': configured_params,
                'use_composition': 'False',
                'use_respawn': 'False',
                'use_rviz': 'False'
            }.items()
        )
    ])
