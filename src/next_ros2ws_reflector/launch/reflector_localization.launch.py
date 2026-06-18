import os
from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node

def generate_launch_description():
    pkg_share = get_package_share_directory('next_ros2ws_reflector')
    
    # Parameters
    reflector_map_file = LaunchConfiguration('reflector_map_file', 
                                             default=os.path.join(pkg_share, 'config', 'reflector_map.yaml'))
    scan_topic = LaunchConfiguration('scan_topic', default='/scan')
    odom_topic = LaunchConfiguration('odom_topic', default='/wheel_controller/odom')
    use_sim_time = LaunchConfiguration('use_sim_time', default='false')

    # Reflector Localizer Node
    localizer_node = Node(
        package='next_ros2ws_reflector',
        executable='reflector_localizer',
        name='reflector_localizer',
        output='screen',
        parameters=[
            os.path.join(pkg_share, 'config', 'reflector_localizer.yaml'),
            {
                'reflector_map_file': reflector_map_file,
                'scan_topic': scan_topic,
                'odom_topic': odom_topic,
                'use_sim_time': use_sim_time,
            }
        ]
    )

    # Map-Odom Broadcaster Node
    broadcaster_node = Node(
        package='next_ros2ws_reflector',
        executable='map_odom_broadcaster',
        name='map_odom_broadcaster',
        output='screen',
        parameters=[
            {
                'map_frame': 'map',
                'odom_frame': 'odom',
                'base_frame': 'base_link',
                'odom_topic': odom_topic,
                'corrected_pose_topic': 'reflector_pose',
                'use_sim_time': use_sim_time,
            }
        ]
    )

    return LaunchDescription([
        DeclareLaunchArgument('reflector_map_file', default_value=os.path.join(pkg_share, 'config', 'reflector_map.yaml')),
        DeclareLaunchArgument('scan_topic', default_value='/scan'),
        DeclareLaunchArgument('odom_topic', default_value='/wheel_controller/odom'),
        DeclareLaunchArgument('use_sim_time', default_value='false'),
        localizer_node,
        broadcaster_node
    ])
