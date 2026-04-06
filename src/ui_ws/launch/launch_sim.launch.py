import os

from ament_index_python.packages import get_package_share_directory

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription, RegisterEventHandler
from launch.event_handlers import OnProcessStart
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration

from launch_ros.actions import Node


def generate_launch_description():
    package_name = 'ugv_bringup'
    pkg_share = get_package_share_directory(package_name)

    # -------- Launch arguments --------
    world = LaunchConfiguration('world')
    use_sim_time = LaunchConfiguration('use_sim_time')

    declare_world_arg = DeclareLaunchArgument(
        'world',
        default_value=os.path.join(pkg_share, 'worlds', 'small_warehouse.world'),
        description='Full path to Gazebo world file (.world or .sdf)'
    )

    declare_use_sim_time = DeclareLaunchArgument(
        'use_sim_time',
        default_value='true',
        description='Use simulation clock'
    )

    # -------- Robot state publisher / description --------
    # Pass sim_mode:=true so rsp.launch.py selects the Gazebo control plugin path
    # in robot.urdf.xacro instead of the real-hardware ros2_control path.
    rsp = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            os.path.join(pkg_share, 'launch', 'rsp.launch.py')
        ),
        launch_arguments={
            'use_sim_time': use_sim_time,
            'sim_mode': 'true',
        }.items()
    )

    # -------- Joystick + twist mux --------
    joystick = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            os.path.join(pkg_share, 'launch', 'joystick.launch.py')
        ),
        launch_arguments={'use_sim_time': use_sim_time}.items()
    )

    # Velocity priorities defined in config/robot.yaml [twist_mux]
    robot_yaml = os.path.join(pkg_share, 'config', 'robot.yaml')
    twist_mux_yaml = os.path.join(pkg_share, 'config', 'twist_mux.yaml')
    twist_mux = Node(
        package="twist_mux",
        executable="twist_mux",
        output="screen",
        parameters=[twist_mux_yaml, {'use_sim_time': use_sim_time}],
        # Route to /cmd_vel_mux_out so safety_controller stays in the command path.
        # safety_controller then writes to /diff_cont/cmd_vel_unstamped via its
        # cmd_vel_output_topic parameter set in the safety_controller.launch.py include.
        remappings=[('/cmd_vel_out', '/cmd_vel_mux_out')]
    )

    # -------- Gazebo --------
    gazebo = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            os.path.join(get_package_share_directory('gazebo_ros'), 'launch', 'gazebo.launch.py')
        ),
        launch_arguments={
            'world': world,
            'verbose': 'true'
        }.items()
    )

    # -------- Spawn robot into Gazebo --------
    spawn_entity = Node(
        package='gazebo_ros',
        executable='spawn_entity.py',
        output='screen',
        arguments=['-topic', 'robot_description', '-entity', 'ugv_bringup', '-x', '0.0',
                   '-y', '0.0',
                   '-z', '0.1']
    )

    # -------- Controllers (spawn AFTER spawn_entity starts) --------
    # This is much more reliable than fixed TimerAction delays.
    joint_broad_spawner = Node(
        package="controller_manager",
        executable="spawner",
        output="screen",
        arguments=["joint_broad", "--controller-manager", "/controller_manager"],
    )

    diff_drive_spawner = Node(
        package="controller_manager",
        executable="spawner",
        output="screen",
        arguments=["diff_cont", "--controller-manager", "/controller_manager"],
    )

    start_controllers_after_spawn = RegisterEventHandler(
        OnProcessStart(
            target_action=spawn_entity,
            on_start=[joint_broad_spawner, diff_drive_spawner]
        )
    )

    # -------- Safety Controller + Zone Publisher (Keepout & Speed Filters) --------
    # Safety params come from config/robot.yaml [safety_controller] so all launch
    # paths share one source of truth.
    # In sim, safety_controller output is remapped to the Gazebo diff_drive topic.
    # This keeps the full safety path: twist_mux -> /cmd_vel_mux_out -> safety_controller -> /diff_cont/cmd_vel_unstamped
    safety_controller = Node(
        package='next_ros2ws_core',
        executable='safety_controller',
        name='safety_controller',
        output='screen',
        parameters=[
            robot_yaml,
            {
                'use_sim_time': use_sim_time,
                # Sim uses diff_cont instead of wheel_controller hardware topic.
                'cmd_vel_output_topic': '/diff_cont/cmd_vel_unstamped',
            },
        ],
    )

    return LaunchDescription([
        declare_world_arg,
        declare_use_sim_time,

        rsp,
        joystick,
        twist_mux,
        safety_controller,

        gazebo,
        spawn_entity,

        start_controllers_after_spawn,
    ])
