import os
import xml.etree.ElementTree as ET

from ament_index_python.packages import get_package_share_directory, PackageNotFoundError

from launch import LaunchDescription
from launch.substitutions import LaunchConfiguration
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch_ros.actions import Node

import xacro


def _package_name_from_xml(package_xml_path):
    try:
        root = ET.parse(package_xml_path).getroot()
    except Exception:
        return ''
    name_node = root.find('name')
    if name_node is None:
        return ''
    return str(name_node.text or '').strip()


def _resolve_bringup_share():
    candidates = []
    for env_name in ('NEXT_BRINGUP_PACKAGE', 'NEXT_ROBOT_PACKAGE'):
        value = str(os.getenv(env_name, '') or '').strip()
        if value:
            candidates.append(value)

    file_dir = os.path.abspath(os.path.dirname(__file__))
    package_xml = os.path.abspath(os.path.join(file_dir, '..', 'package.xml'))
    parsed_name = _package_name_from_xml(package_xml)
    if parsed_name:
        candidates.append(parsed_name)

    candidates.append('ugv_bringup')

    seen = set()
    for package_name in candidates:
        if not package_name or package_name in seen:
            continue
        seen.add(package_name)
        try:
            return get_package_share_directory(package_name)
        except PackageNotFoundError:
            continue

    # Source-tree fallback for unsourced environments.
    return os.path.abspath(os.path.join(file_dir, '..'))


def launch_setup(context, *args, **kwargs):
    """Deferred setup: evaluated at launch time so LaunchConfiguration args are available."""
    sim_mode_str = LaunchConfiguration('sim_mode').perform(context)
    use_sim_time_str = LaunchConfiguration('use_sim_time').perform(context)

    # Process xacro with sim_mode so the correct control plugin is selected.
    pkg_path = _resolve_bringup_share()
    xacro_file = os.path.join(pkg_path, 'urdf', 'robot.urdf.xacro')
    robot_description_config = xacro.process_file(
        xacro_file,
        mappings={'sim_mode': sim_mode_str},
    )

    params = {
        'robot_description': robot_description_config.toxml(),
        'use_sim_time': use_sim_time_str.lower() == 'true',
    }

    return [
        Node(
            package='robot_state_publisher',
            executable='robot_state_publisher',
            output='screen',
            parameters=[params],
        ),
    ]


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument(
            'use_sim_time',
            default_value='false',
            description='Use sim time if true',
        ),
        DeclareLaunchArgument(
            'sim_mode',
            default_value='false',
            description='true = Gazebo diff-drive plugin; false = real hardware ros2_control',
        ),
        OpaqueFunction(function=launch_setup),
    ])
