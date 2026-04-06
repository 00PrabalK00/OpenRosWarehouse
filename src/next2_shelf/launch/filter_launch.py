from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    params_file = LaunchConfiguration('params_file')
    input_scan = LaunchConfiguration('input_scan')
    output_scan = LaunchConfiguration('output_scan')

    return LaunchDescription([
        DeclareLaunchArgument(
            'params_file',
            default_value=PathJoinSubstitution(
                [FindPackageShare('next2_shelf'), 'config', 'median_filter_example.yaml']
            ),
            description='Full path to the laser_filters chain configuration.',
        ),
        DeclareLaunchArgument(
            'input_scan',
            default_value='/lds1/scan_raw',
            description='Input LaserScan topic.',
        ),
        DeclareLaunchArgument(
            'output_scan',
            default_value='base_scan',
            description='Filtered LaserScan topic.',
        ),
        Node(
            package='laser_filters',
            executable='scan_to_scan_filter_chain',
            name='laser_filter',
            output='screen',
            parameters=[params_file],
            remappings=[
                ('/lds1/scan_raw', input_scan),
                ('base_scan', output_scan),
            ],
        ),
    ])
