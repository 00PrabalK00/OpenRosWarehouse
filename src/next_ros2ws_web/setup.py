from setuptools import setup
import os
from glob import glob

package_name = 'next_ros2ws_web'
template_files = [path for path in glob('web/templates/*') if os.path.isfile(path)]
static_files = [path for path in glob('web/static/*') if os.path.isfile(path)]
static_resource_files = [path for path in glob('web/static/resources/*') if os.path.isfile(path)]
setup(
    name=package_name,
    version='1.0.0',
    packages=[package_name],
    package_dir={package_name: 'src'},
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        # Install web assets
        (os.path.join('share', package_name, 'web', 'templates'), template_files),
        (os.path.join('share', package_name, 'web', 'static'), static_files),
        (os.path.join('share', package_name, 'web', 'static', 'resources'), static_resource_files),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Next Robotics',
    maintainer_email='prabalkhareofficial@gmail.com',
    description='Flask-based web UI for the Next UGV platform.',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'zone_web_ui = next_ros2ws_web.zone_web_ui:main',
        ],
    },
)
