from setuptools import setup

package_name = 'next2_shelf_simple'

setup(
    name=package_name,
    version='0.1.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name + '/launch', ['launch/shelf_inserter_launch.py']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Next Robotics',
    maintainer_email='prabalkhareofficial@gmail.com',
    description='Simple shelf insertion node.',
    license='Apache-2.0',
    entry_points={
        'console_scripts': [
            'shelf_inserter = next2_shelf_simple.shelf_inserter:main',
            'shelf_detector = next2_shelf_simple.shelf_detector:main',
        ],
    },
)
