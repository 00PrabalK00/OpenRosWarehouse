from glob import glob
import os

from setuptools import setup

package_name = 'next_ros2ws_tools'

setup(
    name=package_name,
    version='0.0.0',
    packages=[package_name],
    package_dir={package_name: 'src'},
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        (
            'share/' + package_name,
            ['package.xml'],
        ),
        (
            os.path.join('share', package_name, 'config'),
            glob(os.path.join('config', '*.yaml')),
        ),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='aun',
    maintainer_email='aun@todo.todo',
    description='Tools/experiments for next_ros2ws (may use heavy deps like OpenCV/numpy).',
    license='TODO',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'auto_reloc     = next_ros2ws_tools.auto_reloc:main',
            'db_viewer      = next_ros2ws_tools.db_viewer:main',
            'odom_tf_bridge = next_ros2ws_tools.odom_tf_bridge:main',
            'scan_deskewer  = next_ros2ws_tools.scan_deskewer:main',
            'slam_scan_gate = next_ros2ws_tools.slam_scan_gate:main',
            'scan_merger    = next_ros2ws_tools.scan_merger:main',
            'tf_topic_relay = next_ros2ws_tools.tf_topic_relay:main',
        ],
    },
)
