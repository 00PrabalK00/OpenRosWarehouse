from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription([
        Node(
            package='next2_shelf_simple',
            executable='shelf_inserter',
            name='shelf_inserter',
            output='screen',
            parameters=[{
                'cmd_vel_topic': '/cmd_vel_joy',
                'status_topic': '/shelf/status_json',
            }],
        ),
    ])
