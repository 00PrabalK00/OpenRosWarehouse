#!/usr/bin/env python3
import os
import xml.etree.ElementTree as ET

from ament_index_python.packages import get_package_share_directory, PackageNotFoundError
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


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


def generate_launch_description():
    use_sim_time = LaunchConfiguration('use_sim_time')
    pkg_share = _resolve_bringup_share()
    robot_yaml = os.path.join(pkg_share, 'config', 'robot.yaml')
    return LaunchDescription([
        DeclareLaunchArgument(
            'use_sim_time',
            default_value='false',
            description='Use simulation clock if true'
        ),
        Node(
            package='next_ros2ws_core',
            executable='safety_controller',
            name='safety_controller',
            output='screen',
            parameters=[robot_yaml, {'use_sim_time': use_sim_time}]
        ),
        
    ])
