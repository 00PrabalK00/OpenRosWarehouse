# next2_shelf

`next2_shelf` is now the ROS 2 port target for `laser_leg_shelf_detect-main`, merged with the ROI and debug marker behavior that already existed in this repo.

## What It Does

The node subscribes to `sensor_msgs/msg/LaserScan`, filters by intensity, clusters hits with DBSCAN, and detects shelves in two modes:

- `4-leg shelf`: validates front and back leg spacing against `shelf_width`
- `2-leg + offset shelf`: infers the hidden back legs with `offset_length`

It publishes:

- `/markers` and `/group_markers` as `visualization_msgs/msg/MarkerArray`
- `/shelf_roi_marker` as the ROI bounding box marker
- `/shelf_detected` as `std_msgs/msg/Bool`
- `/move_base_simple/goal` and `/goal_pose` as `geometry_msgs/msg/PoseStamped`
- TF frames for `shelf`, `centroid`, and the detected leg groups

It also supports:

- `/tick` trigger handling for goal publication
- optional Nav2 `NavigateToPose` action requests
- optional `next_msgs` jacking action requests after successful navigation

## Build

```bash
cd ~/ros2_ws
colcon build --packages-select next2_shelf
source install/setup.bash
```

## Launch

```bash
ros2 launch next2_shelf lidar_ros2_launch.py
```

ROS 1 compatibility presets are also included:

```bash
ros2 launch next2_shelf lidar_ros2_launch.py params_file:=`ros2 pkg prefix next2_shelf`/share/next2_shelf/config/lidar_best_compat.yaml
ros2 launch next2_shelf lidar_ros2_launch.py params_file:=`ros2 pkg prefix next2_shelf`/share/next2_shelf/config/shelf_simple_compat.yaml
```

The old laser filter chain config and launch were also ported:

```bash
ros2 launch next2_shelf filter_launch.py
```

## Main Parameters

- `scan_topic`: laser scan topic, default `/scan`
- `laser_frame`: TF frame used for markers and transforms
- `use_roi`: enables the `x_min/x_max/y_min/y_max` crop from the original repo
- `intensity_threshold`: intensity threshold for candidate points
- `eps` or `proximity_threshold`: DBSCAN radius
- `min_samples` or `min_sample`: DBSCAN minimum samples
- `shelf_width` and `shelf_width_tolerance`: 4-leg validation
- `dist_threshold`: switches between 4-leg and 2-leg detection logic
- `offset_length`: virtual back-leg offset for the 2-leg mode
- `enable_nav2_action`: enables Nav2 action requests if `nav2_msgs` is installed
- `enable_jacking_action`: enables `next_msgs` jacking requests if that action exists in the environment
- `clear_tf_on_lost`: republishes known frames at the origin when detection is lost, matching the old ROS 1 clear behavior more closely

## Notes

- The merged runtime code lives in `scripts/lidar_ros2.py`.
- Nav2 support is optional. If `nav2_msgs` is not installed, the node still publishes goal poses.
- Jacking support is optional. If `next_msgs` is not installed, the node still detects shelves and publishes goals.
