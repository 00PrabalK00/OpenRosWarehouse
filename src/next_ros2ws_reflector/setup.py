from glob import glob
import os
from setuptools import setup

package_name = 'next_ros2ws_reflector'

setup(
    name=package_name,
    version='0.1.0',
    packages=[package_name],
    package_dir={package_name: 'src'},
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name + '/config', glob('config/*.yaml')),
        ('share/' + package_name + '/launch', glob('launch/*.launch.py')),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='next',
    maintainer_email='next@example.com',
    description='Reflector-based localization for AGV',
    license='Apache-2.0',
    entry_points={
        'console_scripts': [
            'reflector_localizer = next_ros2ws_reflector.reflector_localizer:main',
            'map_odom_broadcaster = next_ros2ws_reflector.map_odom_broadcaster:main',
        ],
    },
)
