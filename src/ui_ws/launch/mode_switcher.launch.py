#!/usr/bin/env python3
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    use_sim_time = LaunchConfiguration('use_sim_time')
    return LaunchDescription([
        DeclareLaunchArgument(
            'use_sim_time',
            default_value='false',
            description='Use simulation clock if true'
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
            executable='mode_switcher',
            name='mode_switcher',
            output='screen',
            parameters=[{'use_sim_time': use_sim_time}]
        )
    ])
