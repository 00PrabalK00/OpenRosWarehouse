"""Launch for the PGV extrinsic calibration procedure.

Brings up the driver (so PGV readings flow) and the calibrator. Drive the
robot over a fixed floor code, then call the services:

    ros2 launch next_ros2ws_pgv pgv_calibration.launch.py odom_topic:=/odom

    ros2 service call /pgv_calibrator/start std_srvs/srv/Trigger {}
    # drive forward/back a few times, then spin in place one full turn
    ros2 service call /pgv_calibrator/finish std_srvs/srv/Trigger {}

The current URDF/TF base_link->pgv_link transform is used as the calibration
reference/initial guess. If TF is not available, base_to_pgv_* from
pgv_localization.yaml is used as the fallback reference. The estimated
base_to_pgv_x/y/yaw_deg plus the delta from that reference are written to
output_yaml, and a fit-quality plot is written to plot_path. Copy/apply the
result only after reviewing it.

Separate from the main bringup graph on purpose.
"""

import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    pkg_share = get_package_share_directory("next_ros2ws_pgv")
    default_params = os.path.join(pkg_share, "config", "pgv_localization.yaml")

    port_arg = DeclareLaunchArgument("port", default_value="/dev/next/pgv")
    odom_arg = DeclareLaunchArgument("odom_topic", default_value="/odom")
    out_arg = DeclareLaunchArgument(
        "output_yaml", default_value="/tmp/pgv_calibration.yaml"
    )
    plot_arg = DeclareLaunchArgument(
        "plot_path", default_value="/tmp/pgv_calibration_fit.png"
    )

    port = LaunchConfiguration("port")
    odom_topic = LaunchConfiguration("odom_topic")
    output_yaml = LaunchConfiguration("output_yaml")
    plot_path = LaunchConfiguration("plot_path")

    driver = Node(
        package="next_ros2ws_pgv",
        executable="pgv_reader",
        name="pgv_reader",
        output="screen",
        parameters=[default_params, {"port": port}],
    )

    calibrator = Node(
        package="next_ros2ws_pgv",
        executable="pgv_calibrator",
        name="pgv_calibrator",
        output="screen",
        parameters=[default_params, {
            "odom_topic": odom_topic,
            "output_yaml": output_yaml,
            "plot_path": plot_path,
        }],
    )

    return LaunchDescription([
        port_arg, odom_arg, out_arg, plot_arg,
        driver, calibrator,
    ])
