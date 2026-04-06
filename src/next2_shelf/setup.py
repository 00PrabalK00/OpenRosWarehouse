import os
from glob import glob

from setuptools import setup

package_name = 'next2_shelf'

setup(
    name=package_name,
    version='0.1.0',
    packages=['scripts'],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.py')),
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
        (os.path.join('share', package_name, 'scripts'), ['scripts/lidar_ros2.py']),
    ],
    install_requires=['setuptools', 'numpy', 'scikit-learn'],
    zip_safe=True,
    maintainer='Your Name',
    maintainer_email='user@example.com',
    description='ROS 2 shelf detector ported from laser_leg_shelf_detect and merged with next2_shelf features.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'lidar_ros2 = scripts.lidar_ros2:main',
            'shelf_detector = scripts.lidar_ros2:main',
        ],
    },
)
