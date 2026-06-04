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
    ],
    install_requires=['setuptools', 'pyserial'],
    zip_safe=True,
    maintainer='Next Robotics',
    maintainer_email='prabalkhareofficial@gmail.com',
    description='Standalone ROS 2 driver for the Pepperl+Fuchs R3138 PGV AGV code reader.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'pgv_reader = next_ros2ws_pgv.pgv_reader:main',
        ],
    },
)
