# next_nav2_controller

`next_nav2_controller` is a **Nav2 Controller Server plugin** (`nav2_core::Controller`), not a BT node and not a planner.

It consumes a global path from Nav2, converts it into explicit line segments, and outputs continuous `cmd_vel` commands using one of three runtime-selectable control modes:

- `rollout`: dense velocity rollout scoring (default legacy behavior)
- `rpp`: regulated pure-pursuit-style tracking with curvature and collision regulation
- `mpc`: finite-horizon random-shooting MPC over control sequences

## Plugin Class

- `next_nav2_controller::NextNav2Controller`

## Required Nav2 Configuration Pattern

```yaml
controller_server:
  ros__parameters:
    controller_plugins: ["FollowPath"]
    FollowPath:
      plugin: "next_nav2_controller::NextNav2Controller"
      control_mode: "rpp"  # rollout | rpp | mpc
      lookahead_distance: 0.55
      segment_finish_epsilon: 0.12
      max_v: 0.55
      max_w: 1.0
      acc_lim_v: 2.5
      acc_lim_w: 3.5
      horizon_time: 1.2
      num_samples: 84
      w_lateral: 8.0
      w_heading: 5.0
      w_progress: 6.0
      w_smooth: 0.8
      w_obstacle: 10.0
      final_approach_distance: 0.8
      blocked_timeout: 2.5

      # RPP mode params
      rpp_desired_linear_vel: 0.45
      rpp_use_collision_detection: true

      # MPC mode params
      mpc_steps: 10
      mpc_iterations: 2
      mpc_num_samples: 24
```

## Debug Topics (optional)

Enable with:

```yaml
FollowPath:
  debug_publishers: true
```

Topics:

- `<plugin_name>/projection_point`
- `<plugin_name>/target_point`
- `<plugin_name>/segment_index`
- `<plugin_name>/lateral_error`
