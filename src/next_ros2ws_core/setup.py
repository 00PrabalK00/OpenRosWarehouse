from setuptools import setup

package_name = 'next_ros2ws_core'

setup(
    name=package_name,
    version='1.0.0',
    packages=[package_name],
    package_dir={package_name: 'src'},
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Next Robotics',
    maintainer_email='prabalkhareofficial@gmail.com',
    description='Core ROS 2 nodes for the Next UGV platform.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'control_mode_manager = next_ros2ws_core.control_mode_manager:main',
            'keepout_zone_publisher = next_ros2ws_core.keepout_zone_publisher:main',
            'map_manager = next_ros2ws_core.map_manager:main',
            'map_layer_manager = next_ros2ws_core.map_layer_manager:main',
            'map_editor_manager = next_ros2ws_core.map_editor_manager:main',
            'mission_manager = next_ros2ws_core.mission_manager:main',
            'mode_switcher = next_ros2ws_core.mode_switcher:main',
            'navigation_arbitrator = next_ros2ws_core.navigation_arbitrator:main',
            'safety_controller = next_ros2ws_core.safety_controller:main',
            'settings_manager = next_ros2ws_core.settings_manager:main',
            'stack_manager = next_ros2ws_core.stack_manager:main',
            'zone_manager = next_ros2ws_core.zone_manager:main',
        ],
    },
)
