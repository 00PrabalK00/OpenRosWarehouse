import os
from glob import glob

from setuptools import setup

package_name = 'next2_shelf'

setup(
    name=package_name,
    version='0.1.0',
    packages=[package_name],
    package_dir={package_name: 'scripts'},
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.py')),
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
    ],
    install_requires=['setuptools', 'numpy', 'scikit-learn'],
    zip_safe=True,
    maintainer='Next Robotics',
    maintainer_email='prabalkhareofficial@gmail.com',
    description='ROS 2 shelf detector and docking state machine for the Next UGV platform.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'lidar_ros2 = next2_shelf.lidar_ros2:main',
            'shelf_detector = next2_shelf.lidar_ros2:main',
            'shelf_docking_sm = next2_shelf.shelf_docking_sm:main',
        ],
    },
)
