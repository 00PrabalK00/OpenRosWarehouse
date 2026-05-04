#!/usr/bin/env python3

"""Compatibility alias for the navigation launch.

This file existed as an empty placeholder. Keeping a small wrapper here avoids
breakage for older scripts that still invoke `waypoints_follow.launch.py`.
"""

import os

from launch import LaunchDescription
from launch.actions import IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource

from ament_index_python.packages import get_package_share_directory


def generate_launch_description():
    pkg_share = get_package_share_directory('ugv_bringup')
    navigation_launch = os.path.join(pkg_share, 'launch', 'navigation.launch.py')
    return LaunchDescription([
        IncludeLaunchDescription(PythonLaunchDescriptionSource(navigation_launch)),
    ])
