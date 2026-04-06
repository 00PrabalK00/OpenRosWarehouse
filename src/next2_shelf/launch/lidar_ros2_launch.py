from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare

def generate_launch_description():
    params_file = LaunchConfiguration('params_file')

    return LaunchDescription([
        DeclareLaunchArgument(
            'params_file',
            default_value=PathJoinSubstitution(
                [FindPackageShare('next2_shelf'), 'config', 'lidar_params.yaml']
            ),
            description='Full path to the ROS 2 parameters file.',
        ),
        Node(
            package='next2_shelf',
            executable='shelf_detector',
            name='distance_calculator',
            output='screen',
            parameters=[params_file],
        ),
    ])
