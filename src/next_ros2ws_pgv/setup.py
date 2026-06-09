from glob import glob

from setuptools import setup

package_name = 'next_ros2ws_pgv'

setup(
    name=package_name,
    version='1.0.0',
    packages=[package_name],
    package_dir={package_name: 'src'},
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name + '/config', glob('config/*.yaml')),
        ('share/' + package_name + '/launch', glob('launch/*.launch.py')),
    ],
    install_requires=['setuptools', 'pyserial'],
    zip_safe=True,
    maintainer='Next Robotics',
    maintainer_email='prabalkhareofficial@gmail.com',
    description='Standalone ROS 2 driver and oriented Matrix Tag localization '
                'stack for the R3138 PGV AGV code reader.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'pgv_reader = next_ros2ws_pgv.pgv_reader:main',
            'tag_map_server = next_ros2ws_pgv.tag_map_server:main',
            'pgv_localizer = next_ros2ws_pgv.pgv_localizer:main',
            'pgv_diagnostics = next_ros2ws_pgv.pgv_diagnostics:main',
            'pgv_calibrator = next_ros2ws_pgv.pgv_calibrator:main',
            'route_follower = next_ros2ws_pgv.route_follower:main',
            'pgv_sign_test = next_ros2ws_pgv.pgv_sign_test:main',
        ],
    },
)
