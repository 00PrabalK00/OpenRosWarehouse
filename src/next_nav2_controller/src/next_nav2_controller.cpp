#include "next_nav2_controller/next_nav2_controller.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>

#include "angles/angles.h"
#include "nav2_core/exceptions.hpp"
#include "nav2_costmap_2d/cost_values.hpp"
#include "nav2_costmap_2d/footprint.hpp"
#include "nav2_util/node_utils.hpp"
#include "nav2_util/robot_utils.hpp"
#include "pluginlib/class_list_macros.hpp"
#include "tf2_geometry_msgs/tf2_geometry_msgs.hpp"

namespace
{

constexpr double kTiny = 1e-6;
constexpr double kPi = 3.14159265358979323846;

}  // namespace

namespace next_nav2_controller
{

void NextNav2Controller::configure(
  const rclcpp_lifecycle::LifecycleNode::WeakPtr & parent,
  std::string name,
  std::shared_ptr<tf2_ros::Buffer> tf,
  std::shared_ptr<nav2_costmap_2d::Costmap2DROS> costmap_ros)
{
  node_ = parent.lock();
  if (!node_) {
    throw std::runtime_error("next_nav2_controller: failed to lock parent lifecycle node");
  }

  plugin_name_ = std::move(name);
  tf_ = std::move(tf);
  costmap_ros_ = std::move(costmap_ros);
  // Issue 30: explicit null-checks for critical injected dependencies
  if (!tf_) {
    throw nav2_core::PlannerException("next_nav2_controller: tf buffer is null");
  }
  if (!costmap_ros_) {
    throw std::runtime_error("next_nav2_controller: costmap_ros is null");
  }

  costmap_ = costmap_ros_->getCostmap();
  if (!costmap_) {
    throw std::runtime_error("next_nav2_controller: getCostmap() returned null");
  }
  collision_checker_.setCostmap(costmap_);

  global_frame_ = costmap_ros_->getGlobalFrameID();
  base_frame_ = costmap_ros_->getBaseFrameID();

  declareAndLoadParameters();

  if (debug_publishers_) {
    projection_pub_ = node_->create_publisher<geometry_msgs::msg::PointStamped>(
      plugin_name_ + "/projection_point", rclcpp::SystemDefaultsQoS());
    target_pub_ = node_->create_publisher<geometry_msgs::msg::PointStamped>(
      plugin_name_ + "/target_point", rclcpp::SystemDefaultsQoS());
    segment_index_pub_ = node_->create_publisher<std_msgs::msg::Int32>(
      plugin_name_ + "/segment_index", rclcpp::SystemDefaultsQoS());
    lateral_error_pub_ = node_->create_publisher<std_msgs::msg::Float64>(
      plugin_name_ + "/lateral_error", rclcpp::SystemDefaultsQoS());
    distance_remaining_pub_ = node_->create_publisher<std_msgs::msg::Float64>(
      plugin_name_ + "/distance_remaining", rclcpp::SystemDefaultsQoS());
    heading_error_pub_ = node_->create_publisher<std_msgs::msg::Float64>(
      plugin_name_ + "/heading_error", rclcpp::SystemDefaultsQoS());
    cmd_v_pub_ = node_->create_publisher<std_msgs::msg::Float64>(
      plugin_name_ + "/cmd_v", rclcpp::SystemDefaultsQoS());
    cmd_w_pub_ = node_->create_publisher<std_msgs::msg::Float64>(
      plugin_name_ + "/cmd_w", rclcpp::SystemDefaultsQoS());
    final_approach_pub_ = node_->create_publisher<std_msgs::msg::Float64>(
      plugin_name_ + "/final_approach", rclcpp::SystemDefaultsQoS());
    rotate_mode_pub_ = node_->create_publisher<std_msgs::msg::Float64>(
      plugin_name_ + "/rotate_mode", rclcpp::SystemDefaultsQoS());
  }

  blocked_ = false;
  raw_plan_.poses.clear();
  global_plan_.poses.clear();
  segments_.clear();
  current_segment_index_ = 0;
  t_along_segment_ = 0.0;
  stable_progress_m_ = 0.0;
  mpc_v_sequence_.clear();
  mpc_w_sequence_.clear();
  last_progress_checkpoint_m_ = 0.0;
  last_progress_checkpoint_stamp_ = node_->now();
  last_command_ = geometry_msgs::msg::Twist();
  last_command_stamp_ = node_->now();
  last_plan_refresh_stamp_ = rclcpp::Time();
  in_waypoint_pivot_ = false;
  pivot_target_heading_ = 0.0;
  pivot_consumed_waypoint_progress_ = -1.0;
  pivot_start_stamp_ = rclcpp::Time();
  pivot_start_yaw_ = 0.0;
  pivot_last_yaw_ = 0.0;
  pivot_last_yaw_progress_stamp_ = rclcpp::Time();
  in_no_arc_rotate_mode_ = false;
  in_rotate_to_heading_mode_ = false;
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;

  RCLCPP_INFO(
    node_->get_logger(),
    "%s configured as Nav2 Controller plugin (global_frame=%s, base_frame=%s, mode=%s)",
    plugin_name_.c_str(), global_frame_.c_str(), base_frame_.c_str(), control_mode_name_.c_str());
}

void NextNav2Controller::cleanup()
{
  std::lock_guard<std::mutex> lock(data_mutex_);
  projection_pub_.reset();
  target_pub_.reset();
  segment_index_pub_.reset();
  lateral_error_pub_.reset();
  distance_remaining_pub_.reset();
  heading_error_pub_.reset();
  cmd_v_pub_.reset();
  cmd_w_pub_.reset();
  final_approach_pub_.reset();
  rotate_mode_pub_.reset();

  global_plan_.poses.clear();
  raw_plan_.poses.clear();
  segments_.clear();

  blocked_ = false;
  in_waypoint_pivot_ = false;
  pivot_consumed_waypoint_progress_ = -1.0;
  pivot_start_stamp_ = rclcpp::Time();
  pivot_start_yaw_ = 0.0;
  pivot_last_yaw_ = 0.0;
  pivot_last_yaw_progress_stamp_ = rclcpp::Time();
  in_no_arc_rotate_mode_ = false;
  in_rotate_to_heading_mode_ = false;
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
  current_segment_index_ = 0;
  t_along_segment_ = 0.0;
  stable_progress_m_ = 0.0;
  mpc_v_sequence_.clear();
  mpc_w_sequence_.clear();
  last_plan_refresh_stamp_ = rclcpp::Time();
  last_command_ = geometry_msgs::msg::Twist();
  last_command_stamp_ = rclcpp::Time();
  last_progress_checkpoint_m_ = 0.0;
  last_progress_checkpoint_stamp_ = rclcpp::Time();
  blocked_since_ = rclcpp::Time();
  pivot_target_heading_ = 0.0;
}

void NextNav2Controller::activate()
{
  std::lock_guard<std::mutex> lock(data_mutex_);
  if (projection_pub_) {
    projection_pub_->on_activate();
  }
  if (target_pub_) {
    target_pub_->on_activate();
  }
  if (segment_index_pub_) {
    segment_index_pub_->on_activate();
  }
  if (lateral_error_pub_) {
    lateral_error_pub_->on_activate();
  }
  if (distance_remaining_pub_) {
    distance_remaining_pub_->on_activate();
  }
  if (heading_error_pub_) {
    heading_error_pub_->on_activate();
  }
  if (cmd_v_pub_) {
    cmd_v_pub_->on_activate();
  }
  if (cmd_w_pub_) {
    cmd_w_pub_->on_activate();
  }
  if (final_approach_pub_) {
    final_approach_pub_->on_activate();
  }
  if (rotate_mode_pub_) {
    rotate_mode_pub_->on_activate();
  }
}

void NextNav2Controller::deactivate()
{
  std::lock_guard<std::mutex> lock(data_mutex_);
  if (projection_pub_) {
    projection_pub_->on_deactivate();
  }
  if (target_pub_) {
    target_pub_->on_deactivate();
  }
  if (segment_index_pub_) {
    segment_index_pub_->on_deactivate();
  }
  if (lateral_error_pub_) {
    lateral_error_pub_->on_deactivate();
  }
  if (distance_remaining_pub_) {
    distance_remaining_pub_->on_deactivate();
  }
  if (heading_error_pub_) {
    heading_error_pub_->on_deactivate();
  }
  if (cmd_v_pub_) {
    cmd_v_pub_->on_deactivate();
  }
  if (cmd_w_pub_) {
    cmd_w_pub_->on_deactivate();
  }
  if (final_approach_pub_) {
    final_approach_pub_->on_deactivate();
  }
  if (rotate_mode_pub_) {
    rotate_mode_pub_->on_deactivate();
  }
}

void NextNav2Controller::setPlan(const nav_msgs::msg::Path & path)
{
  std::lock_guard<std::mutex> lock(data_mutex_);

  // Issue 31: validate incoming plan for NaN/Inf
  for (const auto & ps : path.poses) {
    const auto & p = ps.pose.position;
    if (!std::isfinite(p.x) || !std::isfinite(p.y) || !std::isfinite(p.z)) {
      throw nav2_core::PlannerException(
              "next_nav2_controller: plan contains non-finite position values");
    }
  }

  raw_plan_ = path;
  global_plan_ = transformPathToGlobalFrame(raw_plan_);
  rebuildSegments();
  buildVelocityProfile();

  if (global_plan_.poses.empty() || segments_.empty()) {
    throw nav2_core::PlannerException("next_nav2_controller: received plan without usable segments");
  }

  current_segment_index_ = 0;
  t_along_segment_ = 0.0;
  stable_progress_m_ = 0.0;
  last_progress_checkpoint_m_ = 0.0;
  last_progress_checkpoint_stamp_ = node_->now();
  blocked_ = false;
  in_waypoint_pivot_ = false;
  pivot_consumed_waypoint_progress_ = -1.0;
  pivot_start_stamp_ = rclcpp::Time();
  pivot_start_yaw_ = 0.0;
  pivot_last_yaw_ = 0.0;
  pivot_last_yaw_progress_stamp_ = rclcpp::Time();
  in_no_arc_rotate_mode_ = false;
  in_rotate_to_heading_mode_ = false;
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
  mpc_v_sequence_.clear();
  mpc_w_sequence_.clear();
  last_plan_refresh_stamp_ = node_->now();

  RCLCPP_DEBUG(
    node_->get_logger(),
    "%s setPlan(): %zu poses, %zu segments",
    plugin_name_.c_str(), global_plan_.poses.size(), segments_.size());
}

geometry_msgs::msg::TwistStamped NextNav2Controller::computeVelocityCommands(
  const geometry_msgs::msg::PoseStamped & pose,
  const geometry_msgs::msg::Twist & velocity,
  nav2_core::GoalChecker * goal_checker)
{
  std::lock_guard<std::mutex> lock(data_mutex_);

  if (segments_.empty() || global_plan_.poses.empty()) {
    throw nav2_core::PlannerException(
            "next_nav2_controller: computeVelocityCommands called without a plan");
  }

  auto now = node_->now();

  // Issue 31: validate incoming robot pose for NaN/Inf
  if (!std::isfinite(pose.pose.position.x) || !std::isfinite(pose.pose.position.y)) {
    throw nav2_core::PlannerException(
            "next_nav2_controller: robot pose contains non-finite coordinates");
  }

  geometry_msgs::msg::PoseStamped robot_pose = pose;
  if (robot_pose.header.frame_id != global_frame_) {
    geometry_msgs::msg::PoseStamped transformed;
    if (!nav2_util::transformPoseInTargetFrame(
        robot_pose, transformed, *tf_, global_frame_, transform_tolerance_))
    {
      throw nav2_core::PlannerException(
              "next_nav2_controller: failed to transform robot pose into controller frame " +
              global_frame_);
    }
    robot_pose = transformed;
  }

  // If plan arrives in a drifting frame (e.g. map) while controller tracks in
  // local costmap frame (e.g. odom), refresh transformed path periodically so
  // we do not chase stale transformed coordinates.
  const double refresh_period = std::max(0.0, plan_refresh_period_);
  const bool needs_refresh =
    !raw_plan_.poses.empty() &&
    !raw_plan_.header.frame_id.empty() &&
    raw_plan_.header.frame_id != global_frame_ &&
    (last_plan_refresh_stamp_.nanoseconds() <= 0 ||
    (now - last_plan_refresh_stamp_).seconds() >= refresh_period);
  if (needs_refresh) {
    try {
      global_plan_ = transformPathToGlobalFrame(raw_plan_);
      rebuildSegments();
      buildVelocityProfile();
      // After refresh, reproject robot onto the new segment set so that
      // current_segment_index_, t_along_segment_, and stable_progress_m_
      // are consistent with the updated transformed coordinates.
      if (!segments_.empty()) {
        // Issue 9: use full point-to-projection distance + heading agreement +
        // backtrack penalty for segment selection during refresh (not just
        // lateral_error which is unsafe for self-overlapping / parallel paths).
        const double robot_yaw_refresh = tf2::getYaw(robot_pose.pose.orientation);
        const geometry_msgs::msg::Point robot_control_point_refresh =
          computeControlPoint(
            robot_pose.pose.position.x,
            robot_pose.pose.position.y,
            robot_yaw_refresh);
        double best_dist = std::numeric_limits<double>::infinity();
        std::size_t best_idx = 0;
        for (std::size_t i = 0; i < segments_.size(); ++i) {
          const Projection p = projectPointOntoSegment(
            robot_control_point_refresh, segments_[i]);
          const double dx = robot_control_point_refresh.x - p.point.x;
          const double dy = robot_control_point_refresh.y - p.point.y;
          const double dist_sq = (dx * dx) + (dy * dy);
          // Heading agreement: prefer segments that match robot heading
          const double hdg_err = std::abs(angles::shortest_angular_distance(
              robot_yaw_refresh, segments_[i].heading));
          // Backtrack penalty: resist selecting segments behind known progress
          const double cand_progress = segments_[i].cumulative_start + p.t;
          const double backtrack_pen = 0.10 * std::pow(
            std::max(0.0, stable_progress_m_ - cand_progress), 2.0);
          // Hysteresis for current segment
          const double seg_hysteresis = 0.002 * std::abs(
            static_cast<double>(i) - static_cast<double>(current_segment_index_));
          const double score = dist_sq + (0.15 * hdg_err * hdg_err) +
            backtrack_pen + seg_hysteresis;
          if (score < best_dist) {
            best_dist = score;
            best_idx = i;
          }
        }
        // Only reanchor forward from the last known segment to preserve
        // monotonic progress (don't jump backwards).
        if (best_idx >= current_segment_index_) {
          current_segment_index_ = best_idx;
        }
        current_segment_index_ = std::min(current_segment_index_, segments_.size() - 1);
        const Projection reproj = projectPointOntoSegment(
          robot_control_point_refresh, segments_[current_segment_index_]);
        t_along_segment_ = reproj.t;
        const double reproj_progress =
          segments_[current_segment_index_].cumulative_start + reproj.t;
        // Allow stable_progress_m_ to advance to the reprojected position but
        // never go backward (monotonic guarantee).
        stable_progress_m_ = std::max(stable_progress_m_, reproj_progress);
        // Issue 10: rebase progress checkpoint so stall detection stays consistent
        // with the refreshed plan geometry.
        last_progress_checkpoint_m_ = std::max(last_progress_checkpoint_m_, reproj_progress);
        last_progress_checkpoint_stamp_ = now;
      } else {
        current_segment_index_ = 0;
      }
      last_plan_refresh_stamp_ = now;
    } catch (const std::exception & ex) {
      RCLCPP_WARN_THROTTLE(
        node_->get_logger(),
        *node_->get_clock(),
        2000,
        "%s: failed to refresh transformed plan: %s",
        plugin_name_.c_str(),
        ex.what());
    }
  }

  const auto & goal_pose = global_plan_.poses.back();
  const bool goal_reached =
    goal_checker != nullptr && goal_checker->isGoalReached(robot_pose.pose, goal_pose.pose, velocity);

  if (goal_reached) {
    blocked_ = false;
    auto stop = makeZeroCommand(now);
    last_command_ = stop.twist;
    last_command_stamp_ = now;
    return stop;
  }

  if (current_segment_index_ >= segments_.size()) {
    current_segment_index_ = segments_.size() - 1;
  }

  const double robot_yaw = tf2::getYaw(robot_pose.pose.orientation);
  const geometry_msgs::msg::Point robot_control_point =
    computeControlPoint(robot_pose.pose.position.x, robot_pose.pose.position.y, robot_yaw);

  Projection projection = projectPointOntoSegment(
    robot_control_point,
    segments_[current_segment_index_]);

  // Issue 8: reanchor FIRST so pre-pivot detection uses the corrected segment
  // index rather than a potentially stale one.
  reanchorToNearestSegment(robot_control_point, projection, robot_yaw, true);

  // ── Waypoint pivot: detect junction BEFORE segment index may advance ─────
  if (waypoint_pivot_enabled_ && !in_waypoint_pivot_
    && current_segment_index_ + 1 < segments_.size())
  {
    // Issue 20: junction consumed latch – do not re-arm pivot at a junction the
    // robot already pivoted at until it has departed far enough.
    const bool junction_still_consumed =
      (pivot_consumed_waypoint_progress_ >= 0.0) &&
      (stable_progress_m_ - pivot_consumed_waypoint_progress_ < pivot_departure_distance_);

    if (!junction_still_consumed) {
      const Segment & pre_seg = segments_[current_segment_index_];
      const double d_to_wp = std::hypot(
        robot_control_point.x - pre_seg.p1.x,
        robot_control_point.y - pre_seg.p1.y);
      const double next_hdg = segments_[current_segment_index_ + 1].heading;
      const double hdg_diff = std::abs(angles::shortest_angular_distance(pre_seg.heading, next_hdg));
      if (hdg_diff > waypoint_pivot_threshold_ && d_to_wp < waypoint_pivot_center_epsilon_) {
        in_waypoint_pivot_ = true;
        pivot_target_heading_ = next_hdg;
        pivot_consumed_waypoint_progress_ = -1.0;  // will be set on completion
        pivot_start_stamp_ = now;
        pivot_start_yaw_ = robot_yaw;
        pivot_last_yaw_ = robot_yaw;
        pivot_last_yaw_progress_stamp_ = now;
        RCLCPP_DEBUG(node_->get_logger(),
          "%s: waypoint pivot activated (Δθ=%.2f rad, dist=%.3f m)",
          plugin_name_.c_str(), hdg_diff, d_to_wp);
      }
    }
  }

  advanceSegmentIfReady(robot_control_point, projection);

  // ── Waypoint pivot: also detect AFTER segment advance ────────────────────
  if (waypoint_pivot_enabled_ && !in_waypoint_pivot_
    && current_segment_index_ > 0
    && current_segment_index_ < segments_.size())
  {
    const bool post_junction_consumed =
      (pivot_consumed_waypoint_progress_ >= 0.0) &&
      (stable_progress_m_ - pivot_consumed_waypoint_progress_ < pivot_departure_distance_);

    if (!post_junction_consumed) {
      const Segment & prev_seg = segments_[current_segment_index_ - 1];
      const Segment & cur_seg  = segments_[current_segment_index_];
      const double hdg_diff_post = std::abs(
        angles::shortest_angular_distance(prev_seg.heading, cur_seg.heading));
      const double d_to_junction = std::hypot(
        robot_control_point.x - prev_seg.p1.x,
        robot_control_point.y - prev_seg.p1.y);
      if (hdg_diff_post > waypoint_pivot_threshold_ &&
        d_to_junction < waypoint_pivot_center_epsilon_ * 2.0)
      {
        in_waypoint_pivot_ = true;
        pivot_target_heading_ = cur_seg.heading;
        pivot_consumed_waypoint_progress_ = -1.0;
        pivot_start_stamp_ = now;
        pivot_start_yaw_ = robot_yaw;
        pivot_last_yaw_ = robot_yaw;
        pivot_last_yaw_progress_stamp_ = now;
        RCLCPP_DEBUG(node_->get_logger(),
          "%s: waypoint pivot activated post-advance (Δθ=%.2f rad, dist=%.3f m)",
          plugin_name_.c_str(), hdg_diff_post, d_to_junction);
      }
    }
  }

  Segment & segment = segments_[current_segment_index_];

  double absolute_progress = segment.cumulative_start + projection.t;
  if (absolute_progress > stable_progress_m_) {
    stable_progress_m_ = absolute_progress;
  }

  // Issue 11: do not apply monotonic clamp when very close to a waypoint
  // junction – slight backward/lateral repositioning is intentional there.
  const bool near_pivot_junction = waypoint_pivot_enabled_ &&
    current_segment_index_ + 1 < segments_.size() &&
    std::hypot(
      robot_control_point.x - segment.p1.x,
      robot_control_point.y - segment.p1.y) < waypoint_pivot_center_epsilon_ * 2.0;

  if (!near_pivot_junction &&
    absolute_progress + progress_backtrack_tolerance_ < stable_progress_m_)
  {
    const double clamped_t = std::clamp(
      stable_progress_m_ - segment.cumulative_start,
      0.0,
      segment.length);
    projection.t = clamped_t;
    projection.point = pointOnSegment(segment, projection.t);
    absolute_progress = segment.cumulative_start + projection.t;
  }

  t_along_segment_ = projection.t;

  if (last_progress_checkpoint_stamp_.nanoseconds() <= 0) {
    last_progress_checkpoint_m_ = stable_progress_m_;
    last_progress_checkpoint_stamp_ = now;
  } else if (stable_progress_m_ >= (last_progress_checkpoint_m_ + progress_stall_epsilon_)) {
    last_progress_checkpoint_m_ = stable_progress_m_;
    last_progress_checkpoint_stamp_ = now;
  }

  const double goal_dx = goal_pose.pose.position.x - robot_pose.pose.position.x;
  const double goal_dy = goal_pose.pose.position.y - robot_pose.pose.position.y;
  const double goal_distance = std::hypot(goal_dx, goal_dy);
  const bool on_last_segment = (current_segment_index_ + 1) >= segments_.size();
  const bool final_approach = on_last_segment && goal_distance <= final_approach_distance_;
  const double total_path_length = segments_.back().cumulative_start + segments_.back().length;
  const double abs_lateral_error = std::abs(projection.lateral_error);

  if (strict_line_tracking_ && !final_approach) {
    if (!strict_line_path_captured_) {
      if (abs_lateral_error <= strict_line_capture_lateral_error_) {
        strict_line_path_captured_ = true;
      }
    } else if (abs_lateral_error >= strict_line_release_lateral_error_) {
      strict_line_path_captured_ = false;
    }
  }
  const bool strict_line_locked = strict_line_tracking_ && !final_approach && strict_line_path_captured_;

  double final_scale = 1.0;
  if (final_approach && final_approach_distance_ > kTiny) {
    final_scale = std::clamp(goal_distance / final_approach_distance_, 0.2, 1.0);
  }

  const double adaptive_lookahead = lookahead_distance_ +
    (lookahead_speed_gain_ * std::abs(velocity.linear.x));
  const double lookahead_nominal = std::clamp(
    adaptive_lookahead,
    lookahead_min_distance_,
    lookahead_max_distance_);
  const double lookahead = std::max(0.05, lookahead_nominal * final_scale);
  double runtime_max_v = std::max(min_tracking_speed_, max_v_ * final_scale);
  if (speed_profile_enabled_) {
    // Apply the precomputed per-segment velocity profile.  The backward pass in
    // buildVelocityProfile() already propagated braking constraints upstream, so
    // the current segment's limit naturally enforces anticipatory deceleration.
    runtime_max_v = std::min(runtime_max_v, segment.speed_limit);
  }
  double lateral_error_limit = std::numeric_limits<double>::infinity();
  if (strict_line_locked) {
    if (strict_line_adaptive_lateral_limit_) {
      // Issue 22: clamp the adaptive widening to a hard ceiling so "strict"
      // mode remains meaningfully strict even under accumulated drift.
      lateral_error_limit = std::clamp(
        abs_lateral_error + strict_line_max_lateral_growth_,
        strict_line_max_lateral_error_,
        strict_line_adaptive_max_limit_);
    } else {
      lateral_error_limit = strict_line_max_lateral_error_;
    }

    if (abs_lateral_error > strict_line_speed_slowdown_error_) {
      const double lateral_error_span = std::max(
        kTiny,
        lateral_error_limit - strict_line_speed_slowdown_error_);
      const double lateral_ratio = std::clamp(
        (abs_lateral_error - strict_line_speed_slowdown_error_) / lateral_error_span,
        0.0,
        1.0);
      const double speed_scale = 1.0 - (1.0 - strict_line_min_speed_scale_) * lateral_ratio;
      runtime_max_v = std::max(min_tracking_speed_, runtime_max_v * speed_scale);
    }
  }

  // ── Waypoint approach braking: gentle taper before a pivot junction ──────
  if (waypoint_pivot_enabled_ && !in_waypoint_pivot_ && !final_approach
    && current_segment_index_ + 1 < segments_.size())
  {
    const Segment & app_seg = segments_[current_segment_index_];
    const double d_app = std::hypot(
      robot_control_point.x - app_seg.p1.x,
      robot_control_point.y - app_seg.p1.y);
    const double next_hdg_app = segments_[current_segment_index_ + 1].heading;
    const double hdg_diff_app = std::abs(
      angles::shortest_angular_distance(app_seg.heading, next_hdg_app));
    if (hdg_diff_app > waypoint_pivot_threshold_ && d_app < waypoint_pivot_approach_distance_) {
      const double brake_ratio = std::clamp(d_app / waypoint_pivot_approach_distance_, 0.0, 1.0);
      // Issue 25: allow speed to decay below min_tracking_speed_ near the
      // exact junction; use pivot_entry_speed_ as the floor there.
      const double brake_floor = (d_app < waypoint_pivot_center_epsilon_)
        ? pivot_entry_speed_
        : min_tracking_speed_;
      runtime_max_v = std::min(runtime_max_v,
        std::max(brake_floor, runtime_max_v * brake_ratio));
    }
  }

  const double runtime_min_v = allow_reverse_ ? -runtime_max_v : 0.0;

  const double target_progress = std::clamp(absolute_progress + lookahead, 0.0, total_path_length);
  std::size_t target_segment_index = current_segment_index_;
  const geometry_msgs::msg::Point target_point = pointOnPath(target_progress, &target_segment_index);
  const Segment & target_segment = segments_[target_segment_index];

  const double segment_heading = target_segment.heading;
  const double goal_heading = tf2::getYaw(goal_pose.pose.orientation);
  const double heading_blend = final_approach ? (1.0 - final_scale) : 0.0;
  const double heading_reference = angles::normalize_angle(
    segment_heading + heading_blend * angles::shortest_angular_distance(segment_heading, goal_heading));
  // robot_yaw already declared above (before reanchorToNearestSegment call)
  const double target_heading_now = std::atan2(
    target_point.y - robot_control_point.y,
    target_point.x - robot_control_point.x);
  const double target_heading_error_now = std::abs(
    angles::shortest_angular_distance(robot_yaw, target_heading_now));
  const double heading_error_reference_signed =
    angles::shortest_angular_distance(robot_yaw, heading_reference);
  const double heading_error_reference_now = std::abs(heading_error_reference_signed);
  const double heading_error_for_rotation = final_approach
    ? heading_error_reference_now
    : target_heading_error_now;
  // Issue 21: hysteresis for rotate-in-place mode (non-RPP).
  // Enter when heading error exceeds rotate_to_heading_threshold_;
  // exit only when it drops below rotate_to_heading_exit_threshold_.
  if (!in_rotate_to_heading_mode_) {
    if (!final_approach && heading_error_for_rotation > rotate_to_heading_threshold_) {
      in_rotate_to_heading_mode_ = true;
    }
  } else {
    if (final_approach || heading_error_for_rotation < rotate_to_heading_exit_threshold_) {
      in_rotate_to_heading_mode_ = false;
    }
  }
  bool rotate_in_place_mode = in_rotate_to_heading_mode_;
  if (control_mode_ == ControlMode::Rpp) {
    // RPP has its own rotate-to-heading behavior; avoid double-gating linear velocity.
    rotate_in_place_mode = false;
    in_rotate_to_heading_mode_ = false;
  }

  const double heading_weight = final_approach
    ? (w_heading_ * (1.5 + (1.0 - final_scale)))
    : w_heading_;

  const double dt_cmd = std::max(0.02, (now - last_command_stamp_).seconds());

  const double v_window = std::max(0.05, std::abs(acc_lim_v_) * dt_cmd);
  const double w_window = std::max(0.05, std::abs(acc_lim_w_) * dt_cmd);

  const double v_feedback =
    std::abs(velocity.linear.x) > 0.01 ? velocity.linear.x : last_command_.linear.x;
  const double w_feedback =
    std::abs(velocity.angular.z) > 0.01 ? velocity.angular.z : last_command_.angular.z;

  double v_min = std::clamp(v_feedback - v_window, runtime_min_v, runtime_max_v);
  double v_max = std::clamp(v_feedback + v_window, runtime_min_v, runtime_max_v);
  if (v_max < v_min) {
    std::swap(v_min, v_max);
  }

  double w_min = std::clamp(w_feedback - w_window, -max_w_, max_w_);
  double w_max = std::clamp(w_feedback + w_window, -max_w_, max_w_);
  if (w_max < w_min) {
    std::swap(w_min, w_max);
  }

  if (rotate_in_place_mode) {
    const double rotate_limit = std::max(0.0, std::min(v_max, rotate_in_place_max_v_));
    v_min = allow_reverse_ ? -rotate_limit : 0.0;
    v_max = rotate_limit;
    w_min = -max_w_;
    w_max = max_w_;
  }

  // ── Waypoint pivot: exit when robot is aligned to the next segment ────────
  if (in_waypoint_pivot_) {
    const double pivot_err = std::abs(
      angles::shortest_angular_distance(robot_yaw, pivot_target_heading_));
    if (pivot_err < waypoint_pivot_done_threshold_) {
      in_waypoint_pivot_ = false;
      // Issue 20: record consumed junction progress so the pivot gate cannot
      // re-arm until the robot departs far enough.
      pivot_consumed_waypoint_progress_ = stable_progress_m_;
      RCLCPP_DEBUG(node_->get_logger(), "%s: waypoint pivot complete", plugin_name_.c_str());
    }
  }

  // ── Active waypoint pivot: stop and spin fast toward the next segment ─────
  if (waypoint_pivot_enabled_ && in_waypoint_pivot_) {
    // Issue 4: pivot watchdog – abort if pivot takes too long or yaw stops moving
    const double pivot_elapsed = (now - pivot_start_stamp_).seconds();
    if (pivot_start_stamp_.nanoseconds() > 0 && pivot_elapsed > pivot_timeout_) {
      in_waypoint_pivot_ = false;
      pivot_consumed_waypoint_progress_ = stable_progress_m_;
      RCLCPP_WARN(node_->get_logger(),
        "%s: pivot watchdog: total timeout exceeded (%.1f s); aborting pivot",
        plugin_name_.c_str(), pivot_elapsed);
      throw nav2_core::PlannerException(
              "next_nav2_controller: pivot timed out; requesting Nav2 recovery");
    }
    const double yaw_step = std::abs(
      angles::shortest_angular_distance(pivot_last_yaw_, robot_yaw));
    if (yaw_step >= pivot_progress_epsilon_) {
      pivot_last_yaw_ = robot_yaw;
      pivot_last_yaw_progress_stamp_ = now;
    } else if (pivot_last_yaw_progress_stamp_.nanoseconds() > 0) {
      const double no_progress_for = (now - pivot_last_yaw_progress_stamp_).seconds();
      if (no_progress_for > pivot_progress_timeout_) {
        in_waypoint_pivot_ = false;
        pivot_consumed_waypoint_progress_ = stable_progress_m_;
        RCLCPP_WARN(node_->get_logger(),
          "%s: pivot stalled (no yaw progress for %.1f s); aborting pivot",
          plugin_name_.c_str(), no_progress_for);
        throw nav2_core::PlannerException(
                "next_nav2_controller: pivot stalled; requesting Nav2 recovery");
      }
    }

    // Issue 2: check that the full angular sweep of the pivot is collision-free
    // before issuing any angular velocity command.
    {
      std::unique_lock<nav2_costmap_2d::Costmap2D::mutex_t> costmap_lock(*(costmap_->getMutex()));
      const auto footprint = costmap_ros_->getRobotFootprint();
      if (!isPivotCollisionFree(
          robot_yaw, pivot_target_heading_,
          robot_pose.pose.position.x, robot_pose.pose.position.y,
          footprint))
      {
        if (!blocked_) {
          blocked_ = true;
          blocked_since_ = now;
        }
        in_waypoint_pivot_ = false;
        pivot_consumed_waypoint_progress_ = stable_progress_m_;
        RCLCPP_WARN_THROTTLE(node_->get_logger(), *node_->get_clock(), 2000,
          "%s: pivot blocked by obstacle; aborting pivot",
          plugin_name_.c_str());
        auto stop = makeZeroCommand(now);
        last_command_ = stop.twist;
        last_command_stamp_ = now;
        return stop;
      }
    }

    const double pivot_err_signed =
      angles::shortest_angular_distance(robot_yaw, pivot_target_heading_);

    // Issue 3: apply angular acceleration window so pivot respects acc_lim_w_
    const double pivot_w_desired = std::copysign(
      std::min(waypoint_pivot_angular_vel_, max_w_), pivot_err_signed);
    const double pivot_w = std::clamp(pivot_w_desired, w_feedback - w_window, w_feedback + w_window);

    geometry_msgs::msg::TwistStamped pivot_cmd;
    pivot_cmd.header.stamp = now;
    pivot_cmd.header.frame_id = base_frame_;
    pivot_cmd.twist.linear.x = 0.0;
    pivot_cmd.twist.angular.z = pivot_w;
    blocked_ = false;
    last_command_ = pivot_cmd.twist;
    last_command_stamp_ = now;
    RCLCPP_DEBUG_THROTTLE(node_->get_logger(), *node_->get_clock(), 500,
      "%s: pivot in-place (err=%.3f rad, w=%.2f rad/s)",
      plugin_name_.c_str(), std::abs(pivot_err_signed), pivot_w);
    return pivot_cmd;
  }

  auto return_blocked_stop = [&](const char * reason) -> geometry_msgs::msg::TwistStamped {
      if (!blocked_) {
        blocked_ = true;
        blocked_since_ = now;
      }

      const double blocked_for = (now - blocked_since_).seconds();
      RCLCPP_WARN_THROTTLE(
        node_->get_logger(),
        *node_->get_clock(),
        2000,
        "%s: %s for %.2f s (timeout %.2f s)",
        plugin_name_.c_str(),
        reason,
        blocked_for,
        blocked_timeout_);

      if (blocked_for > blocked_timeout_) {
        if (abort_on_blocked_timeout_) {
          throw nav2_core::PlannerException(
                  "next_nav2_controller: " + std::string(reason) + "; requesting Nav2 replan/recovery");
        }
        RCLCPP_WARN_THROTTLE(
          node_->get_logger(),
          *node_->get_clock(),
          2000,
          "%s: blocked timeout exceeded (%.2f s) but abort is disabled; continuing line tracking",
          plugin_name_.c_str(),
          blocked_for);
      }

      auto stop = makeZeroCommand(now);
      last_command_ = stop.twist;
      last_command_stamp_ = now;
      return stop;
    };

  auto enforce_no_arc_motion = [&](double & v_cmd, double & w_cmd) {
      if (!disallow_arcing_) {
        return;
      }

      if (std::abs(v_cmd) <= kTiny || std::abs(w_cmd) <= kTiny) {
        return;
      }

      // Issue 21: hysteresis – update rotate/translate phase state before clamping
      const double heading_error_abs = std::abs(heading_error_for_rotation);
      if (!in_no_arc_rotate_mode_) {
        if (heading_error_abs > no_arc_heading_error_threshold_) {
          in_no_arc_rotate_mode_ = true;
        }
      } else {
        if (heading_error_abs < no_arc_heading_exit_threshold_) {
          in_no_arc_rotate_mode_ = false;
        }
      }

      if (in_no_arc_rotate_mode_) {
        v_cmd = 0.0;
      } else {
        w_cmd = 0.0;
      }
    };

  if (control_mode_ == ControlMode::Rpp) {
    // Use long lookahead across collinear segments so dense straight paths do
    // not collapse into a tiny carrot and induce left-right chatter. Near a
    // real turn, fall back to the active-segment carrot to avoid corner-cutting.
    double rpp_lookahead = lookahead;
    if (!rpp_use_velocity_scaled_lookahead_) {
      rpp_lookahead = std::max(
        0.05,
        std::clamp(lookahead_distance_, lookahead_min_distance_, lookahead_max_distance_) * final_scale);
    }
    const double rpp_target_t = std::min(segment.length, projection.t + rpp_lookahead);
    geometry_msgs::msg::Point rpp_target_point = pointOnSegment(segment, rpp_target_t);
    const double lookahead_turn_delta = std::abs(
      angles::shortest_angular_distance(segment.heading, target_segment.heading));
    const bool can_use_full_path_lookahead =
      (target_segment_index == current_segment_index_) ||
      (lookahead_turn_delta <= rpp_segment_lookahead_turn_threshold_);
    if (can_use_full_path_lookahead) {
      rpp_target_point = target_point;
    }

    const double dx = rpp_target_point.x - robot_control_point.x;
    const double dy = rpp_target_point.y - robot_control_point.y;
    const double cos_yaw = std::cos(robot_yaw);
    const double sin_yaw = std::sin(robot_yaw);
    const double x_robot = (cos_yaw * dx) + (sin_yaw * dy);
    const double y_robot = (-sin_yaw * dx) + (cos_yaw * dy);
    const double dist_sq = (x_robot * x_robot) + (y_robot * y_robot);
    const double curvature = dist_sq > kTiny ? (2.0 * y_robot / dist_sq) : 0.0;
    const double target_heading_now = std::atan2(dy, dx);
    const double target_heading_error_signed =
      angles::shortest_angular_distance(robot_yaw, target_heading_now);
    // If the carrot is behind the base and reverse is disallowed, rotate first
    // rather than driving "forward away" while distance can still decrease.
    const bool target_behind = (!allow_reverse_) && (x_robot < -0.02);

    double command_v = std::min(runtime_max_v, std::max(min_tracking_speed_, rpp_desired_linear_vel_ * final_scale));
    if (allow_reverse_ && x_robot < 0.0) {
      command_v = -command_v;
    } else if (!allow_reverse_) {
      command_v = std::max(0.0, command_v);
    }

    if (std::abs(curvature) > kTiny) {
      const double radius = std::abs(1.0 / curvature);
      if (radius < rpp_regulated_linear_scaling_min_radius_) {
        const double radius_scale = std::clamp(
          radius / std::max(kTiny, rpp_regulated_linear_scaling_min_radius_),
          0.0,
          1.0);
        const double min_scale = std::clamp(
          rpp_regulated_linear_scaling_min_speed_ / std::max(kTiny, std::abs(command_v)),
          0.0,
          1.0);
        command_v *= std::max(radius_scale, min_scale);
      }
    }

    if (!final_approach) {
      const double cruise_floor = std::max(min_tracking_speed_, min_cruise_speed_);
      if (std::abs(command_v) < cruise_floor) {
        if (allow_reverse_ && command_v < 0.0) {
          command_v = -cruise_floor;
        } else {
          command_v = cruise_floor;
        }
      }
    }
    if (target_behind) {
      command_v = 0.0;
    }

    double command_w = curvature * command_v;
    if (final_approach) {
      // During final approach the carrot collapses toward the goal point, so
      // curvature can go to ~0 while a meaningful goal-heading error remains.
      // Always feed back toward heading_reference here, even when strict-line
      // tracking is disabled, so terminal heading convergence does not depend
      // on a separate controller layer.
      command_w += strict_line_k_heading_ * heading_blend * heading_error_reference_signed;
    }
    if (!final_approach) {
      const double tracking_w_limit = std::max(0.20, 2.0 * std::abs(command_v));
      const double clamped_w_limit = std::min(max_w_, tracking_w_limit);
      command_w = std::clamp(command_w, -clamped_w_limit, clamped_w_limit);
    }

    auto enforce_diff_drive_limits = [&](double & v_cmd, double & w_cmd) {
        if (!enforce_diff_drive_kinematics_ || wheel_separation_ <= kTiny || max_wheel_linear_speed_ <= 0.0) {
          return;
        }

        const double wheel_limit = std::max(0.0, max_wheel_linear_speed_);
        v_cmd = std::clamp(v_cmd, -wheel_limit, wheel_limit);
        if (!allow_reverse_) {
          v_cmd = std::max(0.0, v_cmd);
        }

        const double half_track = 0.5 * wheel_separation_;
        if (half_track <= kTiny) {
          return;
        }

        const double remaining_wheel_speed = std::max(0.0, wheel_limit - std::abs(v_cmd));
        const double max_w_from_wheels = remaining_wheel_speed / half_track;
        w_cmd = std::clamp(w_cmd, -max_w_from_wheels, max_w_from_wheels);
      };

    enforce_diff_drive_limits(command_v, command_w);

    // Use the segment/reference heading as the primary rotation authority.
    // The instantaneous carrot angle can jitter left/right around the base when
    // the target point is slightly behind, which causes alternating spin
    // directions even though the desired travel heading is stable.
    const double rotate_heading_error = final_approach
      ? heading_error_reference_now
      : (target_behind
        ? std::max(std::abs(target_heading_error_signed), heading_error_reference_now)
        : heading_error_for_rotation);
    double rotate_heading_error_signed = heading_error_reference_signed;
    if (target_behind && std::abs(rotate_heading_error_signed) < 0.05) {
      rotate_heading_error_signed = target_heading_error_signed;
    }

    // Issue 21: hysteresis for RPP rotate-to-heading mode
    if (!in_rpp_rotate_mode_) {
      in_rpp_rotate_mode_ = rpp_use_rotate_to_heading_ &&
        (final_approach
          ? (rotate_heading_error > rpp_rotate_to_heading_min_angle_)
          : (target_behind || rotate_heading_error > rpp_rotate_to_heading_min_angle_));
    } else {
      in_rpp_rotate_mode_ = rpp_use_rotate_to_heading_ &&
        (final_approach
          ? (rotate_heading_error > rpp_rotate_to_heading_exit_angle_)
          : (target_behind || rotate_heading_error > rpp_rotate_to_heading_exit_angle_));
    }

    if (in_rpp_rotate_mode_)
    {
      const double rotate_limit = std::max(0.0, std::min(v_max, rotate_in_place_max_v_));
      // Clamp the rate-limiter window to the rotate limit so the final
      // v_min/v_max clamp below cannot re-introduce forward motion while
      // the robot is supposed to be spinning in place.  This mirrors the
      // identical fix already applied to the non-RPP rotate_in_place_mode path.
      v_min = allow_reverse_ ? -rotate_limit : 0.0;
      v_max = rotate_limit;
      w_min = -max_w_;
      w_max = max_w_;
      command_v = final_approach
        ? 0.0
        : (allow_reverse_ ? std::clamp(command_v, -rotate_limit, rotate_limit) : 0.0);
      command_w = std::copysign(
        std::max(0.05, rpp_rotate_to_heading_angular_vel_),
        rotate_heading_error_signed);
    }
    enforce_diff_drive_limits(command_v, command_w);

    // Issue 1: apply no-arc constraint BEFORE the collision sim so the sim
    // tests the actual command that will be issued, not a pre-clamp arc.
    enforce_no_arc_motion(command_v, command_w);
    enforce_diff_drive_limits(command_v, command_w);

    bool imminent_collision = false;
    double obstacle_integral = 0.0;
    int obstacle_samples = 0;

    {
      std::unique_lock<nav2_costmap_2d::Costmap2D::mutex_t> costmap_lock(*(costmap_->getMutex()));
      const auto footprint = costmap_ros_->getRobotFootprint();
      const double sim_dt = std::max(0.02, rpp_collision_check_resolution_);
      // When pure-rotation (v=0 due to disallow_arcing or rotate-to-heading), skip
      // forward projection — the robot does not translate so sweeping a multi-step
      // arc through the inflation layer produces false collision detections.
      const bool pure_rotation_sim = std::abs(command_v) <= kTiny;
      const int sim_steps = (rpp_use_collision_detection_ && !pure_rotation_sim)
        ? std::max(1, static_cast<int>(std::ceil(rpp_max_allowed_time_to_collision_ / sim_dt)))
        : 1;

      double sim_x = robot_pose.pose.position.x;
      double sim_y = robot_pose.pose.position.y;
      double sim_theta = robot_yaw;

      for (int i = 0; i < sim_steps; ++i) {
        if (!allow_unknown_space_ && footprintTouchesUnknown(sim_x, sim_y, sim_theta, footprint)) {
          imminent_collision = true;
          break;
        }

        const double cost = poseCost(sim_x, sim_y, sim_theta, footprint);
        if (cost < 0.0 || cost >= static_cast<double>(nav2_costmap_2d::LETHAL_OBSTACLE)) {
          imminent_collision = true;
          break;
        }

        const double normalized_cost = cost >= static_cast<double>(nav2_costmap_2d::INSCRIBED_INFLATED_OBSTACLE)
          ? 1.0
          : std::clamp(
          cost / static_cast<double>(nav2_costmap_2d::MAX_NON_OBSTACLE),
          0.0,
          1.0);
        obstacle_integral += normalized_cost;
        ++obstacle_samples;

        sim_x += command_v * std::cos(sim_theta) * sim_dt;
        sim_y += command_v * std::sin(sim_theta) * sim_dt;
        sim_theta = angles::normalize_angle(sim_theta + command_w * sim_dt);
      }
    }

    if (imminent_collision) {
      return return_blocked_stop("RPP collision forecast");
    }

    blocked_ = false;
    double obstacle_cost_avg = obstacle_samples > 0
      ? (obstacle_integral / static_cast<double>(obstacle_samples))
      : 0.0;

    if (
      rpp_use_obstacle_fallback_ && !final_approach &&
      obstacle_cost_avg >= rpp_obstacle_fallback_cost_threshold_)
    {
      CandidateResult best_fallback;
      best_fallback.score = std::numeric_limits<double>::infinity();

      const int v_samples = std::max(2, rpp_obstacle_fallback_v_samples_);
      const int w_samples = std::max(3, rpp_obstacle_fallback_w_samples_);
      const double v_span = std::max(0.06, runtime_max_v * 0.45);
      const double w_span = std::max(0.30, max_w_ * 0.65);
      const double fallback_v_min = std::clamp(
        command_v - v_span,
        runtime_min_v,
        runtime_max_v);
      const double fallback_v_max = std::clamp(
        command_v + v_span,
        runtime_min_v,
        runtime_max_v);
      const double fallback_w_min = std::clamp(command_w - w_span, -max_w_, max_w_);
      const double fallback_w_max = std::clamp(command_w + w_span, -max_w_, max_w_);

      std::unique_lock<nav2_costmap_2d::Costmap2D::mutex_t> fallback_lock(*(costmap_->getMutex()));

      // Canonical samples: ensure the search always covers the full command envelope
      // regardless of where the current command_v/w happens to be.
      struct FallbackSample { double v; double w; };
      const std::vector<FallbackSample> canonical_samples = {
        {0.0, 0.0},                                     // stop
        {0.0, max_w_},                                  // spin left
        {0.0, -max_w_},                                 // spin right
        {0.0, max_w_ * 0.5},                            // gentle left
        {0.0, -max_w_ * 0.5},                           // gentle right
        {min_tracking_speed_, 0.0},                     // slow straight
        {runtime_max_v * 0.3, max_w_ * 0.5},            // slow left arc
        {runtime_max_v * 0.3, -max_w_ * 0.5},           // slow right arc
        {allow_reverse_ ? -min_tracking_speed_ : 0.0, 0.0},   // slow reverse (if enabled)
        {allow_reverse_ ? -min_tracking_speed_ : 0.0, max_w_ * 0.4},  // reverse-left
        {allow_reverse_ ? -min_tracking_speed_ : 0.0, -max_w_ * 0.4}, // reverse-right
      };

      auto eval_fallback_candidate = [&](double sv, double sw) {
        if (!allow_reverse_) { sv = std::max(0.0, sv); }
        // Issue 1: only generate no-arc candidates when disallow_arcing_ is set,
        // so the scored command is always the command that will actually be executed.
        if (disallow_arcing_ && std::abs(sv) > kTiny && std::abs(sw) > kTiny) { return; }
        if (sv == 0.0 && !allow_reverse_) { sw = std::clamp(sw, -max_w_, max_w_); }
        if (!diffDriveKinematicsFeasible(sv, sw)) { return; }
        CandidateResult candidate = scoreCandidate(
          sv, sw, robot_pose, velocity, segment, rpp_target_point,
          heading_reference, final_approach, goal_reached, lateral_error_limit);
        if (!candidate.feasible) { return; }
        double total_score =
          candidate.score +
          (1.2 * std::pow(candidate.v - command_v, 2)) +
          (0.35 * std::pow(candidate.w - command_w, 2));
        if (!final_approach && std::abs(candidate.v) < min_tracking_speed_ && std::abs(candidate.w) > 0.25) {
          total_score += (w_progress_ * 0.5);
        }
        if (!best_fallback.feasible || total_score < best_fallback.score) {
          best_fallback = candidate;
          best_fallback.score = total_score;
        }
      };

      for (const auto & cs : canonical_samples) {
        eval_fallback_candidate(cs.v, cs.w);
      }

      for (int vi = 0; vi < v_samples; ++vi) {
        const double alpha_v = (v_samples == 1) ? 0.0 : static_cast<double>(vi) / static_cast<double>(v_samples - 1);
        double sample_v = fallback_v_min + (fallback_v_max - fallback_v_min) * alpha_v;
        if (!allow_reverse_) {
          sample_v = std::max(0.0, sample_v);
        }

        for (int wi = 0; wi < w_samples; ++wi) {
          const double alpha_w = (w_samples == 1) ? 0.0 : static_cast<double>(wi) / static_cast<double>(w_samples - 1);
          const double sample_w = fallback_w_min + (fallback_w_max - fallback_w_min) * alpha_w;

          if (!diffDriveKinematicsFeasible(sample_v, sample_w)) {
            continue;
          }

          // Issue 1: skip arc candidates when no-arc mode is active
          if (disallow_arcing_ && std::abs(sample_v) > kTiny && std::abs(sample_w) > kTiny) {
            continue;
          }

          CandidateResult candidate = scoreCandidate(
            sample_v,
            sample_w,
            robot_pose,
            velocity,
            segment,
            rpp_target_point,
            heading_reference,
            final_approach,
            goal_reached,
            lateral_error_limit);
          if (!candidate.feasible) {
            continue;
          }

          // Stay close to nominal RPP command while allowing obstacle-driven deflection.
          double total_score =
            candidate.score +
            (1.2 * std::pow(candidate.v - command_v, 2)) +
            (0.35 * std::pow(candidate.w - command_w, 2));
          if (
            !final_approach &&
            std::abs(candidate.v) < min_tracking_speed_ &&
            std::abs(candidate.w) > 0.25)
          {
            total_score += (w_progress_ * 0.5);
          }

          if (!best_fallback.feasible || total_score < best_fallback.score) {
            best_fallback = candidate;
            best_fallback.score = total_score;
          }
        }
      }
      fallback_lock.unlock();

      if (best_fallback.feasible) {
        command_v = best_fallback.v;
        command_w = best_fallback.w;
        obstacle_cost_avg = std::min(obstacle_cost_avg, best_fallback.obstacle_cost_avg);
      }
    }

    const double obstacle_speed_scale = std::clamp(
      1.0 - (obstacle_speed_reduction_gain_ * obstacle_cost_avg),
      min_obstacle_speed_scale_,
      1.0);
    runtime_max_v = std::max(min_tracking_speed_, runtime_max_v * obstacle_speed_scale);
    const double runtime_min_v_cmd = allow_reverse_ ? -runtime_max_v : 0.0;
    command_v = std::clamp(command_v, runtime_min_v_cmd, runtime_max_v);
    command_v = std::clamp(command_v, v_min, v_max);
    command_w = std::clamp(command_w, w_min, w_max);
    // Note: enforce_no_arc_motion was already applied BEFORE the collision sim
    // (Issue 1) so no post-hoc clamp needed here.

    geometry_msgs::msg::TwistStamped cmd;
    cmd.header.stamp = now;
    cmd.header.frame_id = base_frame_;
    cmd.twist.linear.x = command_v;
    cmd.twist.linear.y = 0.0;
    cmd.twist.linear.z = 0.0;
    cmd.twist.angular.x = 0.0;
    cmd.twist.angular.y = 0.0;
    cmd.twist.angular.z = command_w;

    // Do not fire progress stall during intentional pivot or rotate-to-heading:
    // the robot is deliberately stationary (or nearly so) while turning.
    const bool intentional_rotation_rpp =
      in_waypoint_pivot_ ||
      (in_rpp_rotate_mode_ && rpp_use_rotate_to_heading_);
    if (!intentional_rotation_rpp &&
      std::abs(cmd.twist.linear.x) > progress_stall_cmd_v_threshold_) {
      const double stalled_for = (now - last_progress_checkpoint_stamp_).seconds();
      if (stalled_for > progress_stall_timeout_) {
        if (abort_on_progress_stall_) {
          blocked_ = true;
          blocked_since_ = last_progress_checkpoint_stamp_;
          RCLCPP_WARN_THROTTLE(
            node_->get_logger(),
            *node_->get_clock(),
            2000,
            "%s: progress stalled for %.2f s (cmd_v=%.3f m/s) -> triggering recovery",
            plugin_name_.c_str(),
            stalled_for,
            cmd.twist.linear.x);
          throw nav2_core::PlannerException(
                  "next_nav2_controller: progress stalled; requesting Nav2 replan/recovery");
        }
        last_progress_checkpoint_stamp_ = now;
        last_progress_checkpoint_m_ = stable_progress_m_;
      }
    }

    last_command_ = cmd.twist;
    last_command_stamp_ = now;
    publishDebug(projection.point, rpp_target_point, projection.lateral_error,
      goal_distance, heading_error_reference_signed,
      cmd.twist.linear.x, cmd.twist.angular.z,
      final_approach, in_rpp_rotate_mode_, now);
    return cmd;
  }

  if (control_mode_ == ControlMode::Mpc) {
    const int mpc_steps = std::max(2, mpc_steps_);
    const int mpc_iterations = std::max(1, mpc_iterations_);
    const int mpc_samples = std::max(1, mpc_num_samples_);
    const double mpc_dt = horizon_time_ / static_cast<double>(mpc_steps);
    const double dv_limit = std::max(0.02, std::abs(acc_lim_v_) * mpc_dt);
    const double dw_limit = std::max(0.02, std::abs(acc_lim_w_) * mpc_dt);

    if (
      mpc_v_sequence_.size() != static_cast<std::size_t>(mpc_steps) ||
      mpc_w_sequence_.size() != static_cast<std::size_t>(mpc_steps))
    {
      mpc_v_sequence_.assign(mpc_steps, std::clamp(v_feedback, runtime_min_v, runtime_max_v));
      mpc_w_sequence_.assign(mpc_steps, std::clamp(w_feedback, -max_w_, max_w_));
    } else if (mpc_warm_start_ && mpc_steps > 1) {
      std::rotate(mpc_v_sequence_.begin(), mpc_v_sequence_.begin() + 1, mpc_v_sequence_.end());
      std::rotate(mpc_w_sequence_.begin(), mpc_w_sequence_.begin() + 1, mpc_w_sequence_.end());
      mpc_v_sequence_.back() = mpc_v_sequence_[mpc_steps - 2];
      mpc_w_sequence_.back() = mpc_w_sequence_[mpc_steps - 2];
    }

    std::unique_lock<nav2_costmap_2d::Costmap2D::mutex_t> costmap_lock(*(costmap_->getMutex()));
    const auto footprint = costmap_ros_->getRobotFootprint();

    auto evaluate_sequence = [&](const std::vector<double> & seq_v,
      const std::vector<double> & seq_w,
      CandidateResult & out) -> bool
      {
        out = CandidateResult();
        if (seq_v.size() != seq_w.size() || seq_v.empty()) {
          return false;
        }

        double x = robot_pose.pose.position.x;
        double y = robot_pose.pose.position.y;
        double theta = robot_yaw;
        double prev_v = v_feedback;
        double prev_w = w_feedback;
        double obstacle_integral = 0.0;
        double lateral_integral = 0.0;
        double heading_integral = 0.0;
        double target_distance_integral = 0.0;
        double angular_integral = 0.0;
        double smooth_integral = 0.0;
        const double total_path_length_mpc = segments_.back().cumulative_start + segments_.back().length;
        double mpc_sim_progress = segment.cumulative_start + projection.t;
        std::size_t mpc_sim_seg_idx = current_segment_index_;
        Projection end_projection = projection;

        for (std::size_t i = 0; i < seq_v.size(); ++i) {
          double v = std::clamp(seq_v[i], runtime_min_v, runtime_max_v);
          if (!allow_reverse_) {
            v = std::max(0.0, v);
          }
          const double w = std::clamp(seq_w[i], -max_w_, max_w_);

          // Issue 1: enforce no-arc constraint at each MPC step so the entire
          // scored sequence is collision-checked in its final executed form.
          if (disallow_arcing_ && std::abs(v) > kTiny && std::abs(w) > kTiny) {
            return false;
          }

          if (!diffDriveKinematicsFeasible(v, w)) {
            return false;
          }

          x += v * std::cos(theta) * mpc_dt;
          y += v * std::sin(theta) * mpc_dt;
          theta = angles::normalize_angle(theta + w * mpc_dt);

          const geometry_msgs::msg::Point simulated_control_point =
            computeControlPoint(x, y, theta);

          // Advance mpc_sim_seg_idx forward as the simulation progresses.
          while (mpc_sim_seg_idx + 1 < segments_.size()) {
            const Projection fwd = projectPointOntoSegment(
              simulated_control_point, segments_[mpc_sim_seg_idx]);
            if (segments_[mpc_sim_seg_idx].length - fwd.t > segment_finish_epsilon_) {
              break;
            }
            ++mpc_sim_seg_idx;
          }
          const Projection simulated_projection = projectPointOntoSegment(
            simulated_control_point, segments_[mpc_sim_seg_idx]);
          end_projection = simulated_projection;
          mpc_sim_progress = std::min(
            total_path_length_mpc,
            segments_[mpc_sim_seg_idx].cumulative_start + simulated_projection.t);

          if (
            strict_line_locked &&
            std::abs(simulated_projection.lateral_error) > lateral_error_limit)
          {
            return false;
          }

          if (!allow_unknown_space_ && footprintTouchesUnknown(x, y, theta, footprint)) {
            return false;
          }

          const double cost = poseCost(x, y, theta, footprint);
          if (cost < 0.0 || cost >= static_cast<double>(nav2_costmap_2d::LETHAL_OBSTACLE)) {
            return false;
          }

          const double normalized_cost = cost >= static_cast<double>(nav2_costmap_2d::INSCRIBED_INFLATED_OBSTACLE)
            ? 1.0
            : std::clamp(
            cost / static_cast<double>(nav2_costmap_2d::MAX_NON_OBSTACLE),
            0.0,
            1.0);
          obstacle_integral += normalized_cost;
          lateral_integral += std::abs(simulated_projection.lateral_error);
          heading_integral += std::abs(angles::shortest_angular_distance(theta, heading_reference));
          const double target_dx_step = target_point.x - simulated_control_point.x;
          const double target_dy_step = target_point.y - simulated_control_point.y;
          target_distance_integral += std::hypot(target_dx_step, target_dy_step);
          angular_integral += std::abs(w);
          smooth_integral += std::pow(v - prev_v, 2) + 0.4 * std::pow(w - prev_w, 2);
          prev_v = v;
          prev_w = w;
        }

        const double inv_steps = 1.0 / static_cast<double>(seq_v.size());
        const geometry_msgs::msg::Point end_control_point =
          computeControlPoint(x, y, theta);
        const double target_dx_end = target_point.x - end_control_point.x;
        const double target_dy_end = target_point.y - end_control_point.y;
        const double target_distance_end = std::hypot(target_dx_end, target_dy_end);
        const double target_heading_end = std::atan2(target_dy_end, target_dx_end);
        const double target_bearing_error_end = target_distance_end > 0.02
          ? std::abs(angles::shortest_angular_distance(theta, target_heading_end))
          : 0.0;
        const double heading_error_end =
          std::abs(angles::shortest_angular_distance(theta, heading_reference));
        const double terminal_lateral_error = std::abs(end_projection.lateral_error);
        const double progress_gain = std::max(0.0, mpc_sim_progress - stable_progress_m_);

        double score =
          (w_obstacle_ * (obstacle_integral * inv_steps)) +
          (w_lateral_ * std::pow(lateral_integral * inv_steps, 2)) +
          (w_heading_ * std::pow(heading_integral * inv_steps, 2)) +
          (w_target_ * std::pow(target_distance_integral * inv_steps, 2)) +
          (w_smooth_ * smooth_integral) +
          (w_angular_rate_ * angular_integral * inv_steps) -
          (w_progress_ * progress_gain) +
          (mpc_terminal_heading_weight_ * heading_error_end * heading_error_end) +
          (mpc_terminal_lateral_weight_ * terminal_lateral_error * terminal_lateral_error) +
          (mpc_terminal_target_weight_ * target_distance_end * target_distance_end);

        if (strict_line_locked) {
          score += strict_line_lateral_weight_scale_ * w_lateral_ *
            std::pow(lateral_integral * inv_steps, 2);
        }

        out.feasible = true;
        out.score = score;
        out.v = std::clamp(seq_v.front(), runtime_min_v, runtime_max_v);
        if (!allow_reverse_) {
          out.v = std::max(0.0, out.v);
        }
        out.w = std::clamp(seq_w.front(), -max_w_, max_w_);
        out.projected_progress = mpc_sim_progress;
        out.lateral_error_end = terminal_lateral_error;
        out.heading_error_end = heading_error_end;
        out.target_distance_end = target_distance_end;
        out.target_bearing_error_end = target_bearing_error_end;
        out.obstacle_cost_avg = obstacle_integral * inv_steps;
        return true;
      };

    CandidateResult best;
    best.score = std::numeric_limits<double>::infinity();
    std::vector<double> best_v = mpc_v_sequence_;
    std::vector<double> best_w = mpc_w_sequence_;
    evaluate_sequence(best_v, best_w, best);

    std::normal_distribution<double> noise_v(0.0, mpc_noise_v_);
    std::normal_distribution<double> noise_w(0.0, mpc_noise_w_);

    for (int iter = 0; iter < mpc_iterations; ++iter) {
      for (int sample = 0; sample < mpc_samples; ++sample) {
        std::vector<double> candidate_v = best_v;
        std::vector<double> candidate_w = best_w;
        double prev_v = v_feedback;
        double prev_w = w_feedback;

        for (int step = 0; step < mpc_steps; ++step) {
          candidate_v[step] += noise_v(mpc_rng_);
          candidate_w[step] += noise_w(mpc_rng_);
          candidate_v[step] = std::clamp(candidate_v[step], runtime_min_v, runtime_max_v);
          candidate_w[step] = std::clamp(candidate_w[step], -max_w_, max_w_);
          candidate_v[step] = std::clamp(candidate_v[step], prev_v - dv_limit, prev_v + dv_limit);
          candidate_w[step] = std::clamp(candidate_w[step], prev_w - dw_limit, prev_w + dw_limit);
          if (!allow_reverse_) {
            candidate_v[step] = std::max(0.0, candidate_v[step]);
          }
          prev_v = candidate_v[step];
          prev_w = candidate_w[step];
        }

        CandidateResult candidate_result;
        if (!evaluate_sequence(candidate_v, candidate_w, candidate_result)) {
          continue;
        }

        if (!best.feasible || candidate_result.score < best.score) {
          best = candidate_result;
          best_v = std::move(candidate_v);
          best_w = std::move(candidate_w);
        }
      }
    }

    costmap_lock.unlock();

    if (!best.feasible) {
      return return_blocked_stop("MPC trajectories blocked");
    }

    blocked_ = false;
    mpc_v_sequence_ = best_v;
    mpc_w_sequence_ = best_w;

    const double obstacle_speed_scale = std::clamp(
      1.0 - (obstacle_speed_reduction_gain_ * best.obstacle_cost_avg),
      min_obstacle_speed_scale_,
      1.0);
    runtime_max_v = std::max(min_tracking_speed_, runtime_max_v * obstacle_speed_scale);
    const double runtime_min_v_cmd = allow_reverse_ ? -runtime_max_v : 0.0;
    const bool cruise_floor_enabled =
      !final_approach &&
      !rotate_in_place_mode &&
      best.obstacle_cost_avg < 0.35 &&
      heading_error_for_rotation < std::max(0.25, rotate_to_heading_threshold_ * 0.6);
    double commanded_v = std::clamp(best.v, runtime_min_v_cmd, runtime_max_v);
    if (cruise_floor_enabled && runtime_max_v >= min_cruise_speed_) {
      if (allow_reverse_) {
        const double sign = (commanded_v < 0.0) ? -1.0 : 1.0;
        commanded_v = sign * std::max(std::abs(commanded_v), min_cruise_speed_);
      } else {
        commanded_v = std::max(commanded_v, min_cruise_speed_);
      }
    }
    commanded_v = std::clamp(commanded_v, v_min, v_max);
    double commanded_w = std::clamp(best.w, w_min, w_max);
    // Note: MPC evaluate_sequence already rejects arc steps when disallow_arcing_
    // (Issue 1), so no post-hoc enforce_no_arc_motion is needed here.

    geometry_msgs::msg::TwistStamped cmd;
    cmd.header.stamp = now;
    cmd.header.frame_id = base_frame_;
    cmd.twist.linear.x = commanded_v;
    cmd.twist.linear.y = 0.0;
    cmd.twist.linear.z = 0.0;
    cmd.twist.angular.x = 0.0;
    cmd.twist.angular.y = 0.0;
    cmd.twist.angular.z = commanded_w;

    // Do not fire progress stall during intentional pivot or rotate-to-heading.
    const bool intentional_rotation_mpc =
      in_waypoint_pivot_ || rotate_in_place_mode;
    if (!intentional_rotation_mpc &&
      std::abs(cmd.twist.linear.x) > progress_stall_cmd_v_threshold_) {
      const double stalled_for = (now - last_progress_checkpoint_stamp_).seconds();
      if (stalled_for > progress_stall_timeout_) {
        if (abort_on_progress_stall_) {
          blocked_ = true;
          blocked_since_ = last_progress_checkpoint_stamp_;
          RCLCPP_WARN_THROTTLE(
            node_->get_logger(),
            *node_->get_clock(),
            2000,
            "%s: progress stalled for %.2f s (cmd_v=%.3f m/s) -> triggering recovery",
            plugin_name_.c_str(),
            stalled_for,
            cmd.twist.linear.x);
          throw nav2_core::PlannerException(
                  "next_nav2_controller: progress stalled; requesting Nav2 replan/recovery");
        }
        last_progress_checkpoint_stamp_ = now;
        last_progress_checkpoint_m_ = stable_progress_m_;
      }
    }

    last_command_ = cmd.twist;
    last_command_stamp_ = now;
    publishDebug(projection.point, target_point, projection.lateral_error,
      goal_distance, heading_error_reference_signed,
      cmd.twist.linear.x, cmd.twist.angular.z,
      final_approach, rotate_in_place_mode, now);
    return cmd;
  }

  const int total_samples = std::max(6, num_samples_);
  const int default_v_samples = std::max(2, static_cast<int>(std::floor(std::sqrt(total_samples))));
  const int default_w_samples = std::max(
    3,
    static_cast<int>(std::ceil(static_cast<double>(total_samples) / default_v_samples)));
  const int v_samples = sample_grid_v_ > 1 ? sample_grid_v_ : default_v_samples;
  const int w_samples = sample_grid_w_ > 1 ? sample_grid_w_ : default_w_samples;

  CandidateResult best;
  best.score = std::numeric_limits<double>::infinity();

  std::unique_lock<nav2_costmap_2d::Costmap2D::mutex_t> costmap_lock(*(costmap_->getMutex()));

  bool used_geometric_candidate = false;
  if (strict_line_locked && strict_line_geometric_control_ &&
    !disallow_arcing_)  // Issue 1: skip geometric arc candidate when no-arc is active
  {
    const double lateral_abs = std::abs(projection.lateral_error);
    const double lateral_scale = lateral_error_limit > kTiny
      ? std::clamp(1.0 - (lateral_abs / lateral_error_limit), strict_line_min_speed_scale_, 1.0)
      : strict_line_min_speed_scale_;
    const double heading_scale = std::clamp(1.0 - (heading_error_reference_now / kPi), 0.0, 1.0);

    double geo_v = runtime_max_v * lateral_scale * heading_scale;
    if (heading_error_reference_now > strict_line_rotate_only_error_) {
      geo_v = 0.0;
    } else if (geo_v > 0.0 && geo_v < min_tracking_speed_) {
      geo_v = min_tracking_speed_;
    }
    geo_v = std::clamp(geo_v, runtime_min_v, runtime_max_v);
    geo_v = std::clamp(geo_v, v_min, v_max);

    double geo_w = (strict_line_k_heading_ * heading_error_reference_signed) -
      (strict_line_k_lateral_ * projection.lateral_error);
    geo_w = std::clamp(geo_w, -max_w_, max_w_);
    geo_w = std::clamp(geo_w, w_min, w_max);

    if (diffDriveKinematicsFeasible(geo_v, geo_w)) {
      CandidateResult candidate = scoreCandidate(
        geo_v,
        geo_w,
        robot_pose,
        velocity,
        segment,
        target_point,
        heading_reference,
        final_approach,
        goal_reached,
        lateral_error_limit);

      // Issue 27: only use the geometric shortcut when conditions are clearly
      // favourable. Near junctions, high-obstacle areas, or large heading errors
      // the full search must run so we don't skip the best candidate.
      const bool near_junction = (current_segment_index_ + 1 < segments_.size()) &&
        (std::hypot(robot_control_point.x - segment.p1.x,
                    robot_control_point.y - segment.p1.y) < waypoint_pivot_approach_distance_);
      const bool heading_good = heading_error_reference_now < (strict_line_rotate_only_error_ * 0.5);
      const bool lateral_good = lateral_abs < (strict_line_capture_lateral_error_ * 0.5);
      const double center_cost_quick = poseCost(
        robot_pose.pose.position.x, robot_pose.pose.position.y,
        robot_yaw, costmap_ros_->getRobotFootprint());
      const bool low_obstacle_quick =
        center_cost_quick >= 0.0 &&
        center_cost_quick < static_cast<double>(nav2_costmap_2d::MAX_NON_OBSTACLE) * 0.5;

      if (candidate.feasible && heading_good && lateral_good &&
        !near_junction && !final_approach && low_obstacle_quick)
      {
        best = candidate;
        best.score = candidate.score;
        used_geometric_candidate = true;
      }
    }
  }

  if (!used_geometric_candidate) {
    if (disallow_arcing_) {
      // Issue 1: generate-time no-arc constraint – only sample pure-translate
      // (v, 0) and pure-rotate (0, w) families so scoring and collision-checking
      // are always done on the exact command that will be executed.
      auto score_and_update = [&](double sv, double sw) {
        if (!diffDriveKinematicsFeasible(sv, sw)) { return; }
        CandidateResult candidate = scoreCandidate(
          sv, sw, robot_pose, velocity, segment, target_point,
          heading_reference, final_approach, goal_reached, lateral_error_limit);
        if (!candidate.feasible) { return; }
        const double heading_end_penalty = heading_weight * candidate.heading_error_end *
          candidate.heading_error_end;
        const double lateral_end_penalty = w_lateral_ * candidate.lateral_error_end *
          candidate.lateral_error_end;
        const double target_distance_penalty = w_target_ * candidate.target_distance_end *
          candidate.target_distance_end;
        const double target_bearing_penalty = w_bearing_ * candidate.target_bearing_error_end *
          candidate.target_bearing_error_end;
        const double progress_gain =
          std::max(0.0, candidate.projected_progress - stable_progress_m_);
        double total_score =
          candidate.score +
          heading_end_penalty + lateral_end_penalty +
          target_distance_penalty + target_bearing_penalty -
          (w_progress_ * progress_gain);
        if (!goal_reached && std::abs(sv) < min_tracking_speed_ && !final_approach &&
          !rotate_in_place_mode)
        {
          total_score += w_progress_ * 0.5;
        }
        if (!best.feasible || total_score < best.score) {
          best = candidate;
          best.score = total_score;
        }
      };

      // Translate-only: (v, 0)
      for (int vi = 0; vi < v_samples; ++vi) {
        const double alpha_v = (v_samples == 1) ? 0.5 :
          static_cast<double>(vi) / static_cast<double>(v_samples - 1);
        const double sv = v_min + (v_max - v_min) * alpha_v;
        score_and_update(sv, 0.0);
      }
      // Rotate-only: (0, w)
      for (int wi = 0; wi < w_samples; ++wi) {
        const double alpha_w = (w_samples == 1) ? 0.5 :
          static_cast<double>(wi) / static_cast<double>(w_samples - 1);
        const double sw = w_min + (w_max - w_min) * alpha_w;
        score_and_update(0.0, sw);
      }
    } else {
    for (int vi = 0; vi < v_samples; ++vi) {
      const double alpha_v = (v_samples == 1) ? 0.0 : static_cast<double>(vi) / (v_samples - 1);
      const double sample_v = v_min + (v_max - v_min) * alpha_v;

      for (int wi = 0; wi < w_samples; ++wi) {
        const double alpha_w = (w_samples == 1) ? 0.0 : static_cast<double>(wi) / (w_samples - 1);
        const double sample_w = w_min + (w_max - w_min) * alpha_w;

        if (!diffDriveKinematicsFeasible(sample_v, sample_w)) {
          continue;
        }

        CandidateResult candidate = scoreCandidate(
          sample_v,
          sample_w,
          robot_pose,
          velocity,
          segment,
          target_point,
          heading_reference,
          final_approach,
          goal_reached,
          lateral_error_limit);

        if (!candidate.feasible) {
          continue;
        }

        const double heading_end_penalty = heading_weight * candidate.heading_error_end *
          candidate.heading_error_end;
        const double lateral_end_penalty = w_lateral_ * candidate.lateral_error_end *
          candidate.lateral_error_end;
        const double target_distance_penalty = w_target_ * candidate.target_distance_end *
          candidate.target_distance_end;
        const double target_bearing_penalty = w_bearing_ * candidate.target_bearing_error_end *
          candidate.target_bearing_error_end;
        const double progress_gain = std::max(0.0, candidate.projected_progress - stable_progress_m_);
        double total_score =
          candidate.score +
          heading_end_penalty +
          lateral_end_penalty +
          target_distance_penalty +
          target_bearing_penalty -
          (w_progress_ * progress_gain);

        const double misalignment_ratio = rotate_to_heading_threshold_ > kTiny
          ? clamp01(heading_error_for_rotation / rotate_to_heading_threshold_)
          : 1.0;
        total_score += w_misaligned_forward_ * misalignment_ratio * sample_v * sample_v;

        if (
          !goal_reached && std::abs(sample_v) < min_tracking_speed_ && !final_approach &&
          !rotate_in_place_mode)
        {
          total_score += w_progress_ * 0.5;
        }

        if (!best.feasible || total_score < best.score) {
          best = candidate;
          best.score = total_score;
        }
      }
    }
    }  // end else (disallow_arcing_ == false)
  }  // end if (!used_geometric_candidate)

  costmap_lock.unlock();

  if (!best.feasible) {
    if (!blocked_) {
      blocked_ = true;
      blocked_since_ = now;
    }

    const double blocked_for = (now - blocked_since_).seconds();
    RCLCPP_WARN_THROTTLE(
      node_->get_logger(),
      *node_->get_clock(),
      2000,
      "%s: all rollouts blocked for %.2f s (timeout %.2f s)",
      plugin_name_.c_str(),
      blocked_for,
      blocked_timeout_);

    if (blocked_for > blocked_timeout_) {
      if (abort_on_blocked_timeout_) {
        throw nav2_core::PlannerException(
                "next_nav2_controller: blocked timeout exceeded; requesting Nav2 replan/recovery");
      }
      RCLCPP_WARN_THROTTLE(
        node_->get_logger(),
        *node_->get_clock(),
        2000,
        "%s: blocked timeout exceeded (%.2f s) but abort is disabled; continuing line tracking",
        plugin_name_.c_str(),
        blocked_for);
    }

    auto stop = makeZeroCommand(now);
    last_command_ = stop.twist;
    last_command_stamp_ = now;
    return stop;
  }

  blocked_ = false;

  const double obstacle_speed_scale = std::clamp(
    1.0 - (obstacle_speed_reduction_gain_ * best.obstacle_cost_avg),
    min_obstacle_speed_scale_,
    1.0);
  runtime_max_v = std::max(min_tracking_speed_, runtime_max_v * obstacle_speed_scale);
  const double runtime_min_v_cmd = allow_reverse_ ? -runtime_max_v : 0.0;
  const bool cruise_floor_enabled =
    !final_approach &&
    !rotate_in_place_mode &&
    best.obstacle_cost_avg < 0.35 &&
    heading_error_for_rotation < std::max(0.25, rotate_to_heading_threshold_ * 0.6);
  // Issue 23: near a waypoint junction, allow speed to drop to min_capture_speed_
  // instead of enforcing the full cruise floor. This enables precise centering.
  const bool near_waypoint_capture = waypoint_pivot_enabled_ &&
    current_segment_index_ + 1 < segments_.size() &&
    std::hypot(robot_control_point.x - segment.p1.x,
               robot_control_point.y - segment.p1.y) < waypoint_pivot_approach_distance_ * 0.6;
  const double effective_cruise_floor = near_waypoint_capture
    ? std::max(0.0, min_capture_speed_)
    : min_cruise_speed_;
  double commanded_v = best.v;
  if (cruise_floor_enabled && runtime_max_v >= effective_cruise_floor && effective_cruise_floor > 0.0) {
    if (allow_reverse_) {
      const double sign = (commanded_v < 0.0) ? -1.0 : 1.0;
      commanded_v = sign * std::max(std::abs(commanded_v), effective_cruise_floor);
    } else {
      commanded_v = std::max(commanded_v, effective_cruise_floor);
    }
  }

  double commanded_v_out = std::clamp(commanded_v, runtime_min_v_cmd, runtime_max_v);
  double commanded_w_out = std::clamp(best.w, -max_w_, max_w_);
  // Note: rollout generates only feasible (no-arc) candidates when disallow_arcing_
  // (Issue 1), so no post-hoc clamp needed here.

  geometry_msgs::msg::TwistStamped cmd;
  cmd.header.stamp = now;
  cmd.header.frame_id = base_frame_;
  cmd.twist.linear.x = commanded_v_out;
  cmd.twist.linear.y = 0.0;
  cmd.twist.linear.z = 0.0;
  cmd.twist.angular.x = 0.0;
  cmd.twist.angular.y = 0.0;
  cmd.twist.angular.z = commanded_w_out;

  // Do not fire progress stall during intentional pivot or rotate-to-heading.
  if (!in_waypoint_pivot_ &&
    std::abs(cmd.twist.linear.x) > progress_stall_cmd_v_threshold_) {
    const double stalled_for = (now - last_progress_checkpoint_stamp_).seconds();
    if (stalled_for > progress_stall_timeout_) {
      if (abort_on_progress_stall_) {
        blocked_ = true;
        blocked_since_ = last_progress_checkpoint_stamp_;
        RCLCPP_WARN_THROTTLE(
          node_->get_logger(),
          *node_->get_clock(),
          2000,
          "%s: progress stalled for %.2f s (cmd_v=%.3f m/s) -> triggering recovery",
          plugin_name_.c_str(),
          stalled_for,
          cmd.twist.linear.x);
        throw nav2_core::PlannerException(
                "next_nav2_controller: progress stalled; requesting Nav2 replan/recovery");
      }
      // Re-arm warning window when stall abort is disabled.
      last_progress_checkpoint_stamp_ = now;
      last_progress_checkpoint_m_ = stable_progress_m_;
    }
  }

  last_command_ = cmd.twist;
  last_command_stamp_ = now;

  publishDebug(projection.point, target_point, projection.lateral_error,
    goal_distance, heading_error_reference_signed,
    cmd.twist.linear.x, cmd.twist.angular.z,
    final_approach, rotate_in_place_mode, now);

  return cmd;
}

void NextNav2Controller::setSpeedLimit(const double & speed_limit, const bool & percentage)
{
  std::lock_guard<std::mutex> lock(data_mutex_);

  if (speed_limit <= 0.0) {
    max_v_ = base_max_v_;
    rotate_in_place_max_v_ = std::min(base_rotate_in_place_max_v_, max_v_);
    return;
  }

  if (percentage) {
    max_v_ = std::clamp(base_max_v_ * (speed_limit / 100.0), 0.0, base_max_v_);
  } else {
    max_v_ = std::clamp(speed_limit, 0.0, base_max_v_);
  }
  rotate_in_place_max_v_ = std::min(base_rotate_in_place_max_v_, max_v_);
}

// Issue 32: explicit cancel/reset so all pivot, progress, and MPC warm-start state is cleared
bool NextNav2Controller::cancel()
{
  std::lock_guard<std::mutex> lock(data_mutex_);
  in_waypoint_pivot_ = false;
  pivot_consumed_waypoint_progress_ = -1.0;
  pivot_start_stamp_ = rclcpp::Time();
  pivot_last_yaw_progress_stamp_ = rclcpp::Time();
  in_no_arc_rotate_mode_ = false;
  in_rotate_to_heading_mode_ = false;
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
  blocked_ = false;
  mpc_v_sequence_.clear();
  mpc_w_sequence_.clear();
  RCLCPP_DEBUG(node_->get_logger(), "%s: cancel() called", plugin_name_.c_str());
  return true;
}

void NextNav2Controller::reset()
{
  std::lock_guard<std::mutex> lock(data_mutex_);
  in_waypoint_pivot_ = false;
  pivot_consumed_waypoint_progress_ = -1.0;
  pivot_start_stamp_ = rclcpp::Time();
  pivot_last_yaw_ = 0.0;
  pivot_last_yaw_progress_stamp_ = rclcpp::Time();
  in_no_arc_rotate_mode_ = false;
  in_rotate_to_heading_mode_ = false;
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
  blocked_ = false;
  blocked_since_ = rclcpp::Time();
  stable_progress_m_ = 0.0;
  last_progress_checkpoint_m_ = 0.0;
  last_progress_checkpoint_stamp_ = rclcpp::Time();
  mpc_v_sequence_.clear();
  mpc_w_sequence_.clear();
  last_command_ = geometry_msgs::msg::Twist();
  last_command_stamp_ = rclcpp::Time();
  RCLCPP_DEBUG(node_->get_logger(), "%s: reset() called", plugin_name_.c_str());
}

void NextNav2Controller::declareAndLoadParameters()
{
  auto node = node_;

  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".lookahead_distance", rclcpp::ParameterValue(lookahead_distance_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".lookahead_speed_gain", rclcpp::ParameterValue(lookahead_speed_gain_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".lookahead_min_distance", rclcpp::ParameterValue(lookahead_min_distance_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".lookahead_max_distance", rclcpp::ParameterValue(lookahead_max_distance_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".segment_finish_epsilon", rclcpp::ParameterValue(segment_finish_epsilon_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".max_v", rclcpp::ParameterValue(max_v_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".max_w", rclcpp::ParameterValue(max_w_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".acc_lim_v", rclcpp::ParameterValue(acc_lim_v_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".acc_lim_w", rclcpp::ParameterValue(acc_lim_w_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".decel_lim_v", rclcpp::ParameterValue(decel_lim_v_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".horizon_time", rclcpp::ParameterValue(horizon_time_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".num_samples", rclcpp::ParameterValue(num_samples_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".sample_grid_v", rclcpp::ParameterValue(sample_grid_v_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".sample_grid_w", rclcpp::ParameterValue(sample_grid_w_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_lateral", rclcpp::ParameterValue(w_lateral_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_heading", rclcpp::ParameterValue(w_heading_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_progress", rclcpp::ParameterValue(w_progress_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_smooth", rclcpp::ParameterValue(w_smooth_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_obstacle", rclcpp::ParameterValue(w_obstacle_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_target", rclcpp::ParameterValue(w_target_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_bearing", rclcpp::ParameterValue(w_bearing_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_angular_rate", rclcpp::ParameterValue(w_angular_rate_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".w_misaligned_forward", rclcpp::ParameterValue(w_misaligned_forward_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".enforce_diff_drive_kinematics",
    rclcpp::ParameterValue(enforce_diff_drive_kinematics_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".allow_reverse", rclcpp::ParameterValue(allow_reverse_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".wheel_separation", rclcpp::ParameterValue(wheel_separation_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".max_wheel_linear_speed", rclcpp::ParameterValue(max_wheel_linear_speed_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".final_approach_distance", rclcpp::ParameterValue(final_approach_distance_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".blocked_timeout", rclcpp::ParameterValue(blocked_timeout_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rotate_to_heading_threshold", rclcpp::ParameterValue(rotate_to_heading_threshold_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rotate_in_place_max_v", rclcpp::ParameterValue(rotate_in_place_max_v_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".obstacle_speed_reduction_gain", rclcpp::ParameterValue(obstacle_speed_reduction_gain_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".min_obstacle_speed_scale", rclcpp::ParameterValue(min_obstacle_speed_scale_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".allow_unknown_space", rclcpp::ParameterValue(allow_unknown_space_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".unknown_check_resolution", rclcpp::ParameterValue(unknown_check_resolution_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".progress_stall_timeout", rclcpp::ParameterValue(progress_stall_timeout_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".progress_stall_epsilon", rclcpp::ParameterValue(progress_stall_epsilon_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".progress_stall_cmd_v_threshold", rclcpp::ParameterValue(progress_stall_cmd_v_threshold_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".abort_on_blocked_timeout", rclcpp::ParameterValue(abort_on_blocked_timeout_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".abort_on_progress_stall", rclcpp::ParameterValue(abort_on_progress_stall_));

  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rollout_dt", rclcpp::ParameterValue(rollout_dt_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".transform_tolerance", rclcpp::ParameterValue(transform_tolerance_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".plan_refresh_period", rclcpp::ParameterValue(plan_refresh_period_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".progress_backtrack_tolerance",
    rclcpp::ParameterValue(progress_backtrack_tolerance_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".min_tracking_speed", rclcpp::ParameterValue(min_tracking_speed_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".min_cruise_speed", rclcpp::ParameterValue(min_cruise_speed_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".strict_line_tracking", rclcpp::ParameterValue(strict_line_tracking_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_max_lateral_error",
    rclcpp::ParameterValue(strict_line_max_lateral_error_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_max_lateral_growth",
    rclcpp::ParameterValue(strict_line_max_lateral_growth_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_adaptive_lateral_limit",
    rclcpp::ParameterValue(strict_line_adaptive_lateral_limit_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_capture_lateral_error",
    rclcpp::ParameterValue(strict_line_capture_lateral_error_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_release_lateral_error",
    rclcpp::ParameterValue(strict_line_release_lateral_error_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_speed_slowdown_error",
    rclcpp::ParameterValue(strict_line_speed_slowdown_error_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_min_speed_scale",
    rclcpp::ParameterValue(strict_line_min_speed_scale_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_lateral_weight_scale",
    rclcpp::ParameterValue(strict_line_lateral_weight_scale_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_geometric_control",
    rclcpp::ParameterValue(strict_line_geometric_control_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_k_heading",
    rclcpp::ParameterValue(strict_line_k_heading_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_k_lateral",
    rclcpp::ParameterValue(strict_line_k_lateral_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_rotate_only_error",
    rclcpp::ParameterValue(strict_line_rotate_only_error_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".disallow_arcing", rclcpp::ParameterValue(disallow_arcing_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".no_arc_heading_error_threshold",
    rclcpp::ParameterValue(no_arc_heading_error_threshold_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".debug_publishers", rclcpp::ParameterValue(debug_publishers_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".control_mode", rclcpp::ParameterValue(control_mode_name_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_use_velocity_scaled_lookahead",
    rclcpp::ParameterValue(rpp_use_velocity_scaled_lookahead_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rpp_desired_linear_vel", rclcpp::ParameterValue(rpp_desired_linear_vel_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_regulated_linear_scaling_min_radius",
    rclcpp::ParameterValue(rpp_regulated_linear_scaling_min_radius_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_regulated_linear_scaling_min_speed",
    rclcpp::ParameterValue(rpp_regulated_linear_scaling_min_speed_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rpp_use_rotate_to_heading", rclcpp::ParameterValue(rpp_use_rotate_to_heading_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_rotate_to_heading_min_angle",
    rclcpp::ParameterValue(rpp_rotate_to_heading_min_angle_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_rotate_to_heading_angular_vel",
    rclcpp::ParameterValue(rpp_rotate_to_heading_angular_vel_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rpp_use_collision_detection", rclcpp::ParameterValue(rpp_use_collision_detection_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_max_allowed_time_to_collision",
    rclcpp::ParameterValue(rpp_max_allowed_time_to_collision_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_collision_check_resolution",
    rclcpp::ParameterValue(rpp_collision_check_resolution_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_segment_lookahead_turn_threshold",
    rclcpp::ParameterValue(rpp_segment_lookahead_turn_threshold_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rpp_use_obstacle_fallback", rclcpp::ParameterValue(rpp_use_obstacle_fallback_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_obstacle_fallback_cost_threshold",
    rclcpp::ParameterValue(rpp_obstacle_fallback_cost_threshold_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_obstacle_fallback_v_samples",
    rclcpp::ParameterValue(rpp_obstacle_fallback_v_samples_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".rpp_obstacle_fallback_w_samples",
    rclcpp::ParameterValue(rpp_obstacle_fallback_w_samples_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".mpc_steps", rclcpp::ParameterValue(mpc_steps_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".mpc_iterations", rclcpp::ParameterValue(mpc_iterations_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".mpc_num_samples", rclcpp::ParameterValue(mpc_num_samples_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".mpc_noise_v", rclcpp::ParameterValue(mpc_noise_v_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".mpc_noise_w", rclcpp::ParameterValue(mpc_noise_w_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".mpc_warm_start", rclcpp::ParameterValue(mpc_warm_start_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".mpc_terminal_heading_weight",
    rclcpp::ParameterValue(mpc_terminal_heading_weight_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".mpc_terminal_lateral_weight",
    rclcpp::ParameterValue(mpc_terminal_lateral_weight_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".mpc_terminal_target_weight",
    rclcpp::ParameterValue(mpc_terminal_target_weight_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".mpc_seed", rclcpp::ParameterValue(mpc_seed_));

  node->get_parameter(plugin_name_ + ".lookahead_distance", lookahead_distance_);
  node->get_parameter(plugin_name_ + ".lookahead_speed_gain", lookahead_speed_gain_);
  node->get_parameter(plugin_name_ + ".lookahead_min_distance", lookahead_min_distance_);
  node->get_parameter(plugin_name_ + ".lookahead_max_distance", lookahead_max_distance_);
  node->get_parameter(plugin_name_ + ".segment_finish_epsilon", segment_finish_epsilon_);
  node->get_parameter(plugin_name_ + ".max_v", max_v_);
  node->get_parameter(plugin_name_ + ".max_w", max_w_);
  node->get_parameter(plugin_name_ + ".acc_lim_v", acc_lim_v_);
  node->get_parameter(plugin_name_ + ".acc_lim_w", acc_lim_w_);
  node->get_parameter(plugin_name_ + ".decel_lim_v", decel_lim_v_);
  node->get_parameter(plugin_name_ + ".horizon_time", horizon_time_);
  node->get_parameter(plugin_name_ + ".num_samples", num_samples_);
  node->get_parameter(plugin_name_ + ".sample_grid_v", sample_grid_v_);
  node->get_parameter(plugin_name_ + ".sample_grid_w", sample_grid_w_);
  node->get_parameter(plugin_name_ + ".w_lateral", w_lateral_);
  node->get_parameter(plugin_name_ + ".w_heading", w_heading_);
  node->get_parameter(plugin_name_ + ".w_progress", w_progress_);
  node->get_parameter(plugin_name_ + ".w_smooth", w_smooth_);
  node->get_parameter(plugin_name_ + ".w_obstacle", w_obstacle_);
  node->get_parameter(plugin_name_ + ".w_target", w_target_);
  node->get_parameter(plugin_name_ + ".w_bearing", w_bearing_);
  node->get_parameter(plugin_name_ + ".w_angular_rate", w_angular_rate_);
  node->get_parameter(plugin_name_ + ".w_misaligned_forward", w_misaligned_forward_);
  node->get_parameter(plugin_name_ + ".enforce_diff_drive_kinematics", enforce_diff_drive_kinematics_);
  node->get_parameter(plugin_name_ + ".allow_reverse", allow_reverse_);
  node->get_parameter(plugin_name_ + ".wheel_separation", wheel_separation_);
  node->get_parameter(plugin_name_ + ".max_wheel_linear_speed", max_wheel_linear_speed_);
  node->get_parameter(plugin_name_ + ".final_approach_distance", final_approach_distance_);
  node->get_parameter(plugin_name_ + ".blocked_timeout", blocked_timeout_);
  node->get_parameter(plugin_name_ + ".rotate_to_heading_threshold", rotate_to_heading_threshold_);
  node->get_parameter(plugin_name_ + ".rotate_in_place_max_v", rotate_in_place_max_v_);
  node->get_parameter(plugin_name_ + ".obstacle_speed_reduction_gain", obstacle_speed_reduction_gain_);
  node->get_parameter(plugin_name_ + ".min_obstacle_speed_scale", min_obstacle_speed_scale_);
  node->get_parameter(plugin_name_ + ".allow_unknown_space", allow_unknown_space_);
  node->get_parameter(plugin_name_ + ".unknown_check_resolution", unknown_check_resolution_);
  node->get_parameter(plugin_name_ + ".progress_stall_timeout", progress_stall_timeout_);
  node->get_parameter(plugin_name_ + ".progress_stall_epsilon", progress_stall_epsilon_);
  node->get_parameter(plugin_name_ + ".progress_stall_cmd_v_threshold", progress_stall_cmd_v_threshold_);
  node->get_parameter(plugin_name_ + ".abort_on_blocked_timeout", abort_on_blocked_timeout_);
  node->get_parameter(plugin_name_ + ".abort_on_progress_stall", abort_on_progress_stall_);

  node->get_parameter(plugin_name_ + ".rollout_dt", rollout_dt_);
  node->get_parameter(plugin_name_ + ".transform_tolerance", transform_tolerance_);
  node->get_parameter(plugin_name_ + ".plan_refresh_period", plan_refresh_period_);
  node->get_parameter(plugin_name_ + ".progress_backtrack_tolerance", progress_backtrack_tolerance_);
  node->get_parameter(plugin_name_ + ".min_tracking_speed", min_tracking_speed_);
  node->get_parameter(plugin_name_ + ".min_cruise_speed", min_cruise_speed_);
  node->get_parameter(plugin_name_ + ".strict_line_tracking", strict_line_tracking_);
  node->get_parameter(plugin_name_ + ".strict_line_max_lateral_error", strict_line_max_lateral_error_);
  node->get_parameter(plugin_name_ + ".strict_line_max_lateral_growth", strict_line_max_lateral_growth_);
  node->get_parameter(
    plugin_name_ + ".strict_line_adaptive_lateral_limit",
    strict_line_adaptive_lateral_limit_);
  node->get_parameter(
    plugin_name_ + ".strict_line_capture_lateral_error",
    strict_line_capture_lateral_error_);
  node->get_parameter(
    plugin_name_ + ".strict_line_release_lateral_error",
    strict_line_release_lateral_error_);
  node->get_parameter(plugin_name_ + ".strict_line_speed_slowdown_error", strict_line_speed_slowdown_error_);
  node->get_parameter(plugin_name_ + ".strict_line_min_speed_scale", strict_line_min_speed_scale_);
  node->get_parameter(plugin_name_ + ".strict_line_lateral_weight_scale", strict_line_lateral_weight_scale_);
  node->get_parameter(plugin_name_ + ".strict_line_geometric_control", strict_line_geometric_control_);
  node->get_parameter(plugin_name_ + ".strict_line_k_heading", strict_line_k_heading_);
  node->get_parameter(plugin_name_ + ".strict_line_k_lateral", strict_line_k_lateral_);
  node->get_parameter(plugin_name_ + ".strict_line_rotate_only_error", strict_line_rotate_only_error_);
  node->get_parameter(plugin_name_ + ".disallow_arcing", disallow_arcing_);
  node->get_parameter(
    plugin_name_ + ".no_arc_heading_error_threshold",
    no_arc_heading_error_threshold_);
  node->get_parameter(plugin_name_ + ".debug_publishers", debug_publishers_);
  node->get_parameter(plugin_name_ + ".control_mode", control_mode_name_);
  node->get_parameter(
    plugin_name_ + ".rpp_use_velocity_scaled_lookahead",
    rpp_use_velocity_scaled_lookahead_);
  node->get_parameter(plugin_name_ + ".rpp_desired_linear_vel", rpp_desired_linear_vel_);
  node->get_parameter(
    plugin_name_ + ".rpp_regulated_linear_scaling_min_radius",
    rpp_regulated_linear_scaling_min_radius_);
  node->get_parameter(
    plugin_name_ + ".rpp_regulated_linear_scaling_min_speed",
    rpp_regulated_linear_scaling_min_speed_);
  node->get_parameter(plugin_name_ + ".rpp_use_rotate_to_heading", rpp_use_rotate_to_heading_);
  node->get_parameter(
    plugin_name_ + ".rpp_rotate_to_heading_min_angle",
    rpp_rotate_to_heading_min_angle_);
  node->get_parameter(
    plugin_name_ + ".rpp_rotate_to_heading_angular_vel",
    rpp_rotate_to_heading_angular_vel_);
  node->get_parameter(
    plugin_name_ + ".rpp_use_collision_detection",
    rpp_use_collision_detection_);
  node->get_parameter(
    plugin_name_ + ".rpp_max_allowed_time_to_collision",
    rpp_max_allowed_time_to_collision_);
  node->get_parameter(
    plugin_name_ + ".rpp_collision_check_resolution",
    rpp_collision_check_resolution_);
  node->get_parameter(
    plugin_name_ + ".rpp_segment_lookahead_turn_threshold",
    rpp_segment_lookahead_turn_threshold_);
  node->get_parameter(
    plugin_name_ + ".rpp_use_obstacle_fallback",
    rpp_use_obstacle_fallback_);
  node->get_parameter(
    plugin_name_ + ".rpp_obstacle_fallback_cost_threshold",
    rpp_obstacle_fallback_cost_threshold_);
  node->get_parameter(
    plugin_name_ + ".rpp_obstacle_fallback_v_samples",
    rpp_obstacle_fallback_v_samples_);
  node->get_parameter(
    plugin_name_ + ".rpp_obstacle_fallback_w_samples",
    rpp_obstacle_fallback_w_samples_);
  node->get_parameter(plugin_name_ + ".mpc_steps", mpc_steps_);
  node->get_parameter(plugin_name_ + ".mpc_iterations", mpc_iterations_);
  node->get_parameter(plugin_name_ + ".mpc_num_samples", mpc_num_samples_);
  node->get_parameter(plugin_name_ + ".mpc_noise_v", mpc_noise_v_);
  node->get_parameter(plugin_name_ + ".mpc_noise_w", mpc_noise_w_);
  node->get_parameter(plugin_name_ + ".mpc_warm_start", mpc_warm_start_);
  node->get_parameter(
    plugin_name_ + ".mpc_terminal_heading_weight",
    mpc_terminal_heading_weight_);
  node->get_parameter(
    plugin_name_ + ".mpc_terminal_lateral_weight",
    mpc_terminal_lateral_weight_);
  node->get_parameter(
    plugin_name_ + ".mpc_terminal_target_weight",
    mpc_terminal_target_weight_);
  node->get_parameter(plugin_name_ + ".mpc_seed", mpc_seed_);

  // ── Waypoint pivot parameters ────────────────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".waypoint_pivot_enabled",
    rclcpp::ParameterValue(waypoint_pivot_enabled_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".waypoint_pivot_threshold",
    rclcpp::ParameterValue(waypoint_pivot_threshold_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".waypoint_pivot_approach_distance",
    rclcpp::ParameterValue(waypoint_pivot_approach_distance_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".waypoint_pivot_center_epsilon",
    rclcpp::ParameterValue(waypoint_pivot_center_epsilon_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".waypoint_pivot_angular_vel",
    rclcpp::ParameterValue(waypoint_pivot_angular_vel_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".waypoint_pivot_done_threshold",
    rclcpp::ParameterValue(waypoint_pivot_done_threshold_));
  node->get_parameter(plugin_name_ + ".waypoint_pivot_enabled",           waypoint_pivot_enabled_);
  node->get_parameter(plugin_name_ + ".waypoint_pivot_threshold",         waypoint_pivot_threshold_);
  node->get_parameter(plugin_name_ + ".waypoint_pivot_approach_distance", waypoint_pivot_approach_distance_);
  node->get_parameter(plugin_name_ + ".waypoint_pivot_center_epsilon",    waypoint_pivot_center_epsilon_);
  node->get_parameter(plugin_name_ + ".waypoint_pivot_angular_vel",       waypoint_pivot_angular_vel_);
  node->get_parameter(plugin_name_ + ".waypoint_pivot_done_threshold",    waypoint_pivot_done_threshold_);

  // ── Velocity profile parameters ──────────────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".speed_profile_enabled",
    rclcpp::ParameterValue(speed_profile_enabled_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".speed_profile_max_lateral_accel",
    rclcpp::ParameterValue(speed_profile_max_lateral_accel_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".speed_profile_min_speed",
    rclcpp::ParameterValue(speed_profile_min_speed_));
  node->get_parameter(plugin_name_ + ".speed_profile_enabled",           speed_profile_enabled_);
  node->get_parameter(plugin_name_ + ".speed_profile_max_lateral_accel", speed_profile_max_lateral_accel_);
  node->get_parameter(plugin_name_ + ".speed_profile_min_speed",         speed_profile_min_speed_);

  if (control_mode_name_ == "rpp") {
    control_mode_ = ControlMode::Rpp;
  } else if (control_mode_name_ == "mpc") {
    control_mode_ = ControlMode::Mpc;
  } else if (control_mode_name_ == "rollout") {
    control_mode_ = ControlMode::Rollout;
  } else {
    RCLCPP_WARN(
      node_->get_logger(),
      "%s: unknown control_mode '%s', falling back to 'rollout'",
      plugin_name_.c_str(),
      control_mode_name_.c_str());
    control_mode_name_ = "rollout";
    control_mode_ = ControlMode::Rollout;
  }

  max_v_ = std::max(0.0, max_v_);
  base_max_v_ = max_v_;
  max_w_ = std::max(0.05, std::abs(max_w_));
  acc_lim_v_ = std::max(0.05, std::abs(acc_lim_v_));
  acc_lim_w_ = std::max(0.05, std::abs(acc_lim_w_));
  // decel_lim_v_ defaults to acc_lim_v_ if not explicitly configured
  if (decel_lim_v_ <= 0.0) {
    decel_lim_v_ = acc_lim_v_;
  }
  decel_lim_v_ = std::max(0.05, std::abs(decel_lim_v_));
  rotate_in_place_max_v_ = std::clamp(std::abs(rotate_in_place_max_v_), 0.0, max_v_);
  base_rotate_in_place_max_v_ = rotate_in_place_max_v_;
  horizon_time_ = std::max(0.2, horizon_time_);
  num_samples_ = std::max(6, num_samples_);
  sample_grid_v_ = std::max(2, sample_grid_v_);
  sample_grid_w_ = std::max(3, sample_grid_w_);
  lookahead_speed_gain_ = std::max(0.0, lookahead_speed_gain_);
  lookahead_min_distance_ = std::max(0.05, lookahead_min_distance_);
  lookahead_max_distance_ = std::max(lookahead_min_distance_, lookahead_max_distance_);
  w_target_ = std::max(0.0, w_target_);
  w_bearing_ = std::max(0.0, w_bearing_);
  w_angular_rate_ = std::max(0.0, w_angular_rate_);
  w_misaligned_forward_ = std::max(0.0, w_misaligned_forward_);
  wheel_separation_ = std::max(0.0, wheel_separation_);
  max_wheel_linear_speed_ = std::max(0.0, max_wheel_linear_speed_);
  rotate_to_heading_threshold_ = std::clamp(std::abs(rotate_to_heading_threshold_), 0.1, 3.14);
  // rotate_in_place_max_v_ and base_rotate_in_place_max_v_ already set above
  obstacle_speed_reduction_gain_ = std::clamp(obstacle_speed_reduction_gain_, 0.0, 1.0);
  min_obstacle_speed_scale_ = std::clamp(min_obstacle_speed_scale_, 0.05, 1.0);
  unknown_check_resolution_ = std::max(0.01, unknown_check_resolution_);
  progress_stall_timeout_ = std::max(0.2, progress_stall_timeout_);
  progress_stall_epsilon_ = std::max(0.001, progress_stall_epsilon_);
  progress_stall_cmd_v_threshold_ = std::max(0.001, progress_stall_cmd_v_threshold_);
  rollout_dt_ = std::max(0.02, rollout_dt_);
  plan_refresh_period_ = std::max(0.0, plan_refresh_period_);
  final_approach_distance_ = std::max(0.05, final_approach_distance_);
  blocked_timeout_ = std::max(0.2, blocked_timeout_);
  min_tracking_speed_ = std::max(0.0, min_tracking_speed_);
  min_cruise_speed_ = std::max(0.0, min_cruise_speed_);
  strict_line_max_lateral_error_ = std::max(0.01, strict_line_max_lateral_error_);
  strict_line_max_lateral_growth_ = std::max(0.0, strict_line_max_lateral_growth_);
  strict_line_capture_lateral_error_ = std::clamp(
    std::abs(strict_line_capture_lateral_error_),
    0.01,
    std::max(0.01, strict_line_max_lateral_error_));
  strict_line_release_lateral_error_ = std::max(
    strict_line_capture_lateral_error_,
    std::abs(strict_line_release_lateral_error_));
  strict_line_speed_slowdown_error_ = std::clamp(
    strict_line_speed_slowdown_error_,
    0.0,
    strict_line_max_lateral_error_);
  strict_line_min_speed_scale_ = std::clamp(strict_line_min_speed_scale_, 0.05, 1.0);
  strict_line_lateral_weight_scale_ = std::max(0.0, strict_line_lateral_weight_scale_);
  strict_line_k_heading_ = std::max(0.0, strict_line_k_heading_);
  strict_line_k_lateral_ = std::max(0.0, strict_line_k_lateral_);
  strict_line_rotate_only_error_ = std::clamp(
    std::abs(strict_line_rotate_only_error_),
    0.05,
    1.57);
  no_arc_heading_error_threshold_ = std::clamp(
    std::abs(no_arc_heading_error_threshold_),
    0.005,
    0.50);
  rpp_desired_linear_vel_ = std::max(0.0, rpp_desired_linear_vel_);
  rpp_regulated_linear_scaling_min_radius_ = std::max(0.05, rpp_regulated_linear_scaling_min_radius_);
  rpp_regulated_linear_scaling_min_speed_ = std::max(0.0, rpp_regulated_linear_scaling_min_speed_);
  rpp_rotate_to_heading_min_angle_ = std::clamp(
    std::abs(rpp_rotate_to_heading_min_angle_),
    0.05,
    1.57);
  rpp_rotate_to_heading_angular_vel_ = std::max(0.05, std::abs(rpp_rotate_to_heading_angular_vel_));
  rpp_max_allowed_time_to_collision_ = std::max(0.1, rpp_max_allowed_time_to_collision_);
  rpp_segment_lookahead_turn_threshold_ = std::clamp(
    std::abs(rpp_segment_lookahead_turn_threshold_),
    0.01,
    1.57);
  rpp_obstacle_fallback_cost_threshold_ = std::clamp(rpp_obstacle_fallback_cost_threshold_, 0.0, 1.0);
  rpp_obstacle_fallback_v_samples_ = std::max(2, rpp_obstacle_fallback_v_samples_);
  rpp_obstacle_fallback_w_samples_ = std::max(3, rpp_obstacle_fallback_w_samples_);
  rpp_collision_check_resolution_ = std::max(0.01, rpp_collision_check_resolution_);
  mpc_steps_ = std::max(2, mpc_steps_);
  mpc_iterations_ = std::max(1, mpc_iterations_);
  mpc_num_samples_ = std::max(1, mpc_num_samples_);
  mpc_noise_v_ = std::max(0.0, mpc_noise_v_);
  mpc_noise_w_ = std::max(0.0, mpc_noise_w_);
  mpc_terminal_heading_weight_ = std::max(0.0, mpc_terminal_heading_weight_);
  mpc_terminal_lateral_weight_ = std::max(0.0, mpc_terminal_lateral_weight_);
  mpc_terminal_target_weight_ = std::max(0.0, mpc_terminal_target_weight_);
  mpc_rng_.seed(static_cast<std::mt19937::result_type>(std::max(0, mpc_seed_)));
  mpc_v_sequence_.clear();
  mpc_w_sequence_.clear();

  if (!allow_reverse_) {
    min_cruise_speed_ = std::min(min_cruise_speed_, max_v_);
  }
  segment_finish_epsilon_ = std::max(0.01, segment_finish_epsilon_);

  // ── Waypoint pivot clamps ─────────────────────────────────────────────────
  waypoint_pivot_threshold_         = std::clamp(std::abs(waypoint_pivot_threshold_), 0.05, kPi);
  waypoint_pivot_approach_distance_ = std::max(0.05, waypoint_pivot_approach_distance_);
  waypoint_pivot_center_epsilon_    = std::clamp(
    waypoint_pivot_center_epsilon_,
    segment_finish_epsilon_,
    waypoint_pivot_approach_distance_);
  waypoint_pivot_angular_vel_       = std::max(0.10, std::abs(waypoint_pivot_angular_vel_));
  waypoint_pivot_done_threshold_    = std::clamp(std::abs(waypoint_pivot_done_threshold_), 0.02, 0.5);

  speed_profile_max_lateral_accel_ = std::max(0.1, speed_profile_max_lateral_accel_);
  speed_profile_min_speed_         = std::clamp(speed_profile_min_speed_, 0.0, base_max_v_);

  if (max_wheel_linear_speed_ <= 0.0) {
    max_wheel_linear_speed_ = max_v_;
  }

  if (wheel_separation_ <= 0.0) {
    enforce_diff_drive_kinematics_ = false;
    RCLCPP_WARN(
      node_->get_logger(),
      "%s: wheel_separation <= 0, disabling diff-drive kinematic feasibility check",
      plugin_name_.c_str());
  }

  // ── Issue 4: pivot watchdog parameters ───────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".pivot_timeout", rclcpp::ParameterValue(pivot_timeout_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".pivot_progress_epsilon", rclcpp::ParameterValue(pivot_progress_epsilon_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".pivot_progress_timeout", rclcpp::ParameterValue(pivot_progress_timeout_));
  node->get_parameter(plugin_name_ + ".pivot_timeout",           pivot_timeout_);
  node->get_parameter(plugin_name_ + ".pivot_progress_epsilon",  pivot_progress_epsilon_);
  node->get_parameter(plugin_name_ + ".pivot_progress_timeout",  pivot_progress_timeout_);
  pivot_timeout_          = std::max(0.5, pivot_timeout_);
  pivot_progress_epsilon_ = std::max(0.001, pivot_progress_epsilon_);
  pivot_progress_timeout_ = std::max(0.5, pivot_progress_timeout_);

  // ── Issue 20: junction consumed latch ─────────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".pivot_departure_distance",
    rclcpp::ParameterValue(pivot_departure_distance_));
  node->get_parameter(plugin_name_ + ".pivot_departure_distance", pivot_departure_distance_);
  pivot_departure_distance_ = std::max(0.05, pivot_departure_distance_);

  // ── Issue 21: hysteresis thresholds ──────────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".no_arc_heading_exit_threshold",
    rclcpp::ParameterValue(no_arc_heading_exit_threshold_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rotate_to_heading_exit_threshold",
    rclcpp::ParameterValue(rotate_to_heading_exit_threshold_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rpp_rotate_to_heading_exit_angle",
    rclcpp::ParameterValue(rpp_rotate_to_heading_exit_angle_));
  node->get_parameter(
    plugin_name_ + ".no_arc_heading_exit_threshold", no_arc_heading_exit_threshold_);
  node->get_parameter(
    plugin_name_ + ".rotate_to_heading_exit_threshold", rotate_to_heading_exit_threshold_);
  node->get_parameter(
    plugin_name_ + ".rpp_rotate_to_heading_exit_angle", rpp_rotate_to_heading_exit_angle_);
  no_arc_heading_exit_threshold_ = std::clamp(
    std::abs(no_arc_heading_exit_threshold_), 0.002, no_arc_heading_error_threshold_);
  rotate_to_heading_exit_threshold_ = std::clamp(
    std::abs(rotate_to_heading_exit_threshold_), 0.02, rotate_to_heading_threshold_);
  rpp_rotate_to_heading_exit_angle_ = std::clamp(
    std::abs(rpp_rotate_to_heading_exit_angle_), 0.02, rpp_rotate_to_heading_min_angle_);

  // ── Issue 22: adaptive lateral ceiling ───────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".strict_line_adaptive_max_limit",
    rclcpp::ParameterValue(strict_line_adaptive_max_limit_));
  node->get_parameter(
    plugin_name_ + ".strict_line_adaptive_max_limit", strict_line_adaptive_max_limit_);
  strict_line_adaptive_max_limit_ = std::max(
    strict_line_max_lateral_error_, strict_line_adaptive_max_limit_);
  strict_line_release_lateral_error_ = std::max(
    strict_line_release_lateral_error_,
    strict_line_adaptive_max_limit_);

  // ── Issues 23/25: capture and pivot entry speeds ─────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".min_capture_speed", rclcpp::ParameterValue(min_capture_speed_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".pivot_entry_speed", rclcpp::ParameterValue(pivot_entry_speed_));
  node->get_parameter(plugin_name_ + ".min_capture_speed", min_capture_speed_);
  node->get_parameter(plugin_name_ + ".pivot_entry_speed",  pivot_entry_speed_);
  min_capture_speed_ = std::clamp(min_capture_speed_, 0.0, max_v_);
  pivot_entry_speed_ = std::clamp(pivot_entry_speed_,  0.0, max_v_);

  // ── Issue 14: degenerate segment fallback ────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".allow_degenerate_segment_fallback",
    rclcpp::ParameterValue(allow_degenerate_segment_fallback_));
  node->get_parameter(
    plugin_name_ + ".allow_degenerate_segment_fallback",
    allow_degenerate_segment_fallback_);

  // ── Issue 13: path drop tolerance ────────────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".path_max_drop_fraction",
    rclcpp::ParameterValue(path_max_drop_fraction_));
  node->get_parameter(plugin_name_ + ".path_max_drop_fraction", path_max_drop_fraction_);
  path_max_drop_fraction_ = std::clamp(path_max_drop_fraction_, 0.0, 1.0);

  // ── Issue 33: control point offset ───────────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".control_point_offset_x",
    rclcpp::ParameterValue(control_point_offset_x_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".control_point_offset_y",
    rclcpp::ParameterValue(control_point_offset_y_));
  node->get_parameter(plugin_name_ + ".control_point_offset_x", control_point_offset_x_);
  node->get_parameter(plugin_name_ + ".control_point_offset_y", control_point_offset_y_);
  if (!std::isfinite(control_point_offset_x_)) {
    control_point_offset_x_ = 0.0;
  }
  if (!std::isfinite(control_point_offset_y_)) {
    control_point_offset_y_ = 0.0;
  }

  // ── Issue 34: warn on allow_reverse + rpp_use_rotate_to_heading combo ────
  if (allow_reverse_ && rpp_use_rotate_to_heading_) {
    RCLCPP_WARN(
      node_->get_logger(),
      "%s: 'allow_reverse' and 'rpp_use_rotate_to_heading' are both enabled. "
      "RPP rotate-to-heading only triggers for positive heading error; the controller may "
      "use reverse motion while skipping the in-place rotation that clears the heading. "
      "Consider disabling one of these parameters.",
      plugin_name_.c_str());
  }

  // ── Issue 5: warn on overlapping rotation authorities ────────────────────
  if (waypoint_pivot_enabled_ && rpp_use_rotate_to_heading_ &&
    control_mode_ == ControlMode::Rpp)
  {
    RCLCPP_WARN(
      node_->get_logger(),
      "%s: 'waypoint_pivot_enabled' and 'rpp_use_rotate_to_heading' are both active in RPP "
      "mode. Both can issue in-place rotation commands at a waypoint approach. Review which "
      "authority should take precedence, and consider disabling 'rpp_use_rotate_to_heading' "
      "when waypoint pivots are in use.",
      plugin_name_.c_str());
  }
}

nav_msgs::msg::Path NextNav2Controller::transformPathToGlobalFrame(const nav_msgs::msg::Path & path) const
{
  nav_msgs::msg::Path transformed;
  transformed.header = path.header;
  transformed.header.frame_id = global_frame_;
  transformed.poses.reserve(path.poses.size());

  if (path.poses.empty()) {
    return transformed;
  }

  // Determine the source frame (path header takes priority; fall back to first pose header)
  const std::string src_frame =
    !path.header.frame_id.empty() ? path.header.frame_id
    : path.poses.front().header.frame_id;

  // If already in global frame, return as-is
  if (src_frame.empty() || src_frame == global_frame_) {
    for (const auto & pose : path.poses) {
      geometry_msgs::msg::PoseStamped p = pose;
      p.header.frame_id = global_frame_;
      transformed.poses.push_back(p);
    }
    return transformed;
  }

  // Issue 29: look up the transform ONCE so every path point shares the same
  // map->odom snapshot; per-pose lookups can introduce subtle path distortion
  // when the transform changes mid-loop.
  geometry_msgs::msg::TransformStamped tf_stamped;
  try {
    tf_stamped = tf_->lookupTransform(
      global_frame_, src_frame, rclcpp::Time(0),
      rclcpp::Duration::from_seconds(transform_tolerance_));
  } catch (const tf2::TransformException & ex) {
    throw nav2_core::PlannerException(
            std::string("next_nav2_controller: TF lookup failed for path transform: ") + ex.what());
  }

  std::size_t dropped = 0;
  for (const auto & pose : path.poses) {
    try {
      geometry_msgs::msg::PoseStamped out_pose;
      tf2::doTransform(pose, out_pose, tf_stamped);
      out_pose.header.frame_id = global_frame_;
      transformed.poses.push_back(out_pose);
    } catch (const tf2::TransformException & ex) {
      ++dropped;
      RCLCPP_WARN_THROTTLE(
        node_->get_logger(), *node_->get_clock(), 2000,
        "%s: transformPathToGlobalFrame: dropped a path point (%s). Path may have gaps.",
        plugin_name_.c_str(), ex.what());
    }
  }

  // Issue 13: fail loudly when too many points were dropped or the result is
  // unusably short; a partially-transformed path is usually worse than no path.
  if (transformed.poses.size() < 2) {
    throw nav2_core::PlannerException(
            "next_nav2_controller: fewer than 2 path points survived TF transform into " +
            global_frame_);
  }
  if (dropped > 0) {
    const double drop_fraction =
      static_cast<double>(dropped) / static_cast<double>(path.poses.size());
    if (drop_fraction > path_max_drop_fraction_) {
      throw nav2_core::PlannerException(
              "next_nav2_controller: too many path points failed TF transform (" +
              std::to_string(dropped) + "/" + std::to_string(path.poses.size()) +
              " dropped). Path geometry may be corrupted.");
    }
  }

  return transformed;
}

void NextNav2Controller::rebuildSegments()
{
  segments_.clear();

  if (global_plan_.poses.empty()) {
    return;
  }

  double cumulative = 0.0;

  for (std::size_t i = 0; i + 1 < global_plan_.poses.size(); ++i) {
    const auto & p0 = global_plan_.poses[i].pose.position;
    const auto & p1 = global_plan_.poses[i + 1].pose.position;

    const double dx = p1.x - p0.x;
    const double dy = p1.y - p0.y;
    const double len = std::hypot(dx, dy);

    if (len <= kTiny) {
      continue;
    }

    Segment segment;
    segment.p0 = p0;
    segment.p1 = p1;
    segment.dx = dx;
    segment.dy = dy;
    segment.length = len;
    segment.ux = dx / len;
    segment.uy = dy / len;
    segment.heading = std::atan2(dy, dx);
    segment.cumulative_start = cumulative;

    segments_.push_back(segment);
    cumulative += len;
  }

  if (segments_.empty()) {
    if (!allow_degenerate_segment_fallback_) {
      // Issue 14: in production mode, refuse to operate on nonsense geometry
      throw nav2_core::PlannerException(
              "next_nav2_controller: all path segments are degenerate (too short). "
              "Set allow_degenerate_segment_fallback=true to tolerate this in development.");
    }

    const auto & g = global_plan_.poses.back().pose;
    const double gyaw = tf2::getYaw(g.orientation);

    RCLCPP_WARN(
      node_->get_logger(),
      "%s: rebuildSegments(): all path segments are degenerate (too short) — "
      "using 1cm fallback segment at goal. Check planner output quality.",
      plugin_name_.c_str());

    Segment fallback;
    fallback.p1 = g.position;
    fallback.p0 = g.position;
    fallback.p0.x -= 0.01 * std::cos(gyaw);
    fallback.p0.y -= 0.01 * std::sin(gyaw);
    fallback.dx = fallback.p1.x - fallback.p0.x;
    fallback.dy = fallback.p1.y - fallback.p0.y;
    fallback.length = std::max(0.01, std::hypot(fallback.dx, fallback.dy));
    fallback.ux = fallback.dx / fallback.length;
    fallback.uy = fallback.dy / fallback.length;
    fallback.heading = std::atan2(fallback.dy, fallback.dx);
    fallback.cumulative_start = 0.0;
    segments_.push_back(fallback);
  }
}

void NextNav2Controller::buildVelocityProfile()
{
  if (!speed_profile_enabled_ || segments_.empty()) {
    return;
  }

  const int n = static_cast<int>(segments_.size());

  // Initialize every segment to the robot's design-maximum speed.
  for (auto & seg : segments_) {
    seg.speed_limit = base_max_v_;
  }

  // ── Centripetal limit at each inter-segment junction ─────────────────────
  // Curvature at junction i↔i+1 is approximated as heading_change / half_arc.
  // Speed limit: v = sqrt(a_lat / κ)  (centripetal: a = v² κ  →  v = √(a/κ))
  for (int i = 0; i + 1 < n; ++i) {
    const double hdg_change = std::abs(
      angles::shortest_angular_distance(segments_[i].heading, segments_[i + 1].heading));
    if (hdg_change < 0.05) {
      continue;
    }

    const double half_arc = std::min(segments_[i].length, segments_[i + 1].length);
    if (half_arc < kTiny) {
      continue;
    }

    const double kappa = hdg_change / half_arc;
    const double v_junction = std::max(
      speed_profile_min_speed_,
      std::sqrt(speed_profile_max_lateral_accel_ / kappa));

    segments_[i].speed_limit     = std::min(segments_[i].speed_limit,     v_junction);
    segments_[i + 1].speed_limit = std::min(segments_[i + 1].speed_limit, v_junction);
  }

  // ── Backward pass: propagate deceleration constraints upstream ───────────
  // Ensures v[i] is low enough that the robot can brake to v[i+1] within L[i].
  // Uses decel_lim_v_ (separate from acc_lim_v_) so braking profile is accurate.
  for (int i = n - 2; i >= 0; --i) {
    const double v_next = segments_[i + 1].speed_limit;
    const double v_reachable = std::sqrt(
      std::max(0.0, v_next * v_next + 2.0 * decel_lim_v_ * segments_[i].length));
    segments_[i].speed_limit = std::min(segments_[i].speed_limit, v_reachable);
  }

  // ── Forward pass: clip physically unreachable accelerations ──────────────
  for (int i = 1; i < n; ++i) {
    const double v_prev = segments_[i - 1].speed_limit;
    const double v_reachable = std::sqrt(
      std::max(0.0, v_prev * v_prev + 2.0 * acc_lim_v_ * segments_[i - 1].length));
    segments_[i].speed_limit = std::min(segments_[i].speed_limit, v_reachable);
  }

  // Final clamp to valid range.
  for (auto & seg : segments_) {
    seg.speed_limit = std::clamp(seg.speed_limit, speed_profile_min_speed_, base_max_v_);
  }

  RCLCPP_DEBUG(
    node_->get_logger(),
    "%s buildVelocityProfile(): %d segments, speed range [%.2f, %.2f] m/s",
    plugin_name_.c_str(), n,
    segments_.front().speed_limit,
    segments_.back().speed_limit);
}

NextNav2Controller::Projection NextNav2Controller::projectPointOntoSegment(
  const geometry_msgs::msg::Point & point,
  const Segment & segment) const
{
  Projection projection;

  const double rel_x = point.x - segment.p0.x;
  const double rel_y = point.y - segment.p0.y;

  const double raw_t = rel_x * segment.ux + rel_y * segment.uy;
  projection.t = std::clamp(raw_t, 0.0, segment.length);

  projection.point = pointOnSegment(segment, projection.t);

  const double off_x = point.x - projection.point.x;
  const double off_y = point.y - projection.point.y;
  projection.lateral_error = (-segment.uy * off_x) + (segment.ux * off_y);

  return projection;
}

geometry_msgs::msg::Point NextNav2Controller::pointOnSegment(
  const Segment & segment,
  double t_along) const
{
  geometry_msgs::msg::Point p;
  const double t = std::clamp(t_along, 0.0, segment.length);
  p.x = segment.p0.x + segment.ux * t;
  p.y = segment.p0.y + segment.uy * t;
  p.z = 0.0;
  return p;
}

geometry_msgs::msg::Point NextNav2Controller::computeControlPoint(
  double base_x,
  double base_y,
  double yaw) const
{
  geometry_msgs::msg::Point p;
  const double cos_yaw = std::cos(yaw);
  const double sin_yaw = std::sin(yaw);
  p.x = base_x + (control_point_offset_x_ * cos_yaw) - (control_point_offset_y_ * sin_yaw);
  p.y = base_y + (control_point_offset_x_ * sin_yaw) + (control_point_offset_y_ * cos_yaw);
  p.z = 0.0;
  return p;
}

geometry_msgs::msg::Point NextNav2Controller::pointOnPath(
  double progress_m,
  std::size_t * segment_index) const
{
  geometry_msgs::msg::Point p;
  p.x = 0.0;
  p.y = 0.0;
  p.z = 0.0;

  if (segments_.empty()) {
    if (segment_index != nullptr) {
      *segment_index = 0;
    }
    return p;
  }

  const double total_path_length = segments_.back().cumulative_start + segments_.back().length;
  const double clamped_progress = std::clamp(progress_m, 0.0, total_path_length);

  // Always scan from index 0 so this function is stateless and safe to call
  // for lookahead / scoring regardless of current_segment_index_.
  std::size_t idx = 0;

  while (idx + 1 < segments_.size()) {
    const double seg_end = segments_[idx].cumulative_start + segments_[idx].length;
    if (clamped_progress <= seg_end) {
      break;
    }
    ++idx;
  }

  if (segment_index != nullptr) {
    *segment_index = idx;
  }

  const Segment & seg = segments_[idx];
  return pointOnSegment(seg, clamped_progress - seg.cumulative_start);
}

void NextNav2Controller::reanchorToNearestSegment(
  const geometry_msgs::msg::Point & robot_point,
  Projection & projection,
  double robot_yaw,
  bool use_heading_score)
{
  if (segments_.empty()) {
    return;
  }

  const std::size_t current_idx = std::min(current_segment_index_, segments_.size() - 1);
  Projection current_projection = projectPointOntoSegment(robot_point, segments_[current_idx]);
  const double current_dx = robot_point.x - current_projection.point.x;
  const double current_dy = robot_point.y - current_projection.point.y;
  const double current_dist_sq = (current_dx * current_dx) + (current_dy * current_dy);
  const double current_progress = segments_[current_idx].cumulative_start + current_projection.t;

  const double min_allowed_progress = std::max(
    0.0,
    stable_progress_m_ - (2.0 * std::max(0.02, progress_backtrack_tolerance_)));

  std::size_t best_idx = current_idx;
  Projection best_projection = current_projection;
  double best_score = std::numeric_limits<double>::infinity();

  for (std::size_t idx = 0; idx < segments_.size(); ++idx) {
    const Segment & candidate_segment = segments_[idx];
    Projection candidate_projection = projectPointOntoSegment(robot_point, candidate_segment);
    const double candidate_progress = candidate_segment.cumulative_start + candidate_projection.t;
    if (candidate_progress + progress_backtrack_tolerance_ < min_allowed_progress) {
      continue;
    }

    const double dx = robot_point.x - candidate_projection.point.x;
    const double dy = robot_point.y - candidate_projection.point.y;
    const double dist_sq = (dx * dx) + (dy * dy);
    const double segment_hysteresis = 0.002 * std::abs(
      static_cast<double>(idx) - static_cast<double>(current_idx));
    const double backtrack_penalty = 0.10 * std::pow(
      std::max(0.0, stable_progress_m_ - candidate_progress),
      2.0);
    // Issue 28: heading agreement – prefer segments with heading close to robot yaw
    // so the reanchor does not jump to a parallel/reversed segment nearby.
    const double heading_penalty = use_heading_score
      ? 0.15 * std::pow(
          std::abs(angles::shortest_angular_distance(robot_yaw, candidate_segment.heading)),
          2.0)
      : 0.0;
    const double score = dist_sq + segment_hysteresis + backtrack_penalty + heading_penalty;

    if (score < best_score) {
      best_score = score;
      best_idx = idx;
      best_projection = candidate_projection;
    }
  }

  const double switch_margin_sq = 0.01 * 0.01;
  const bool strong_distance_improvement = (best_score + switch_margin_sq) < current_dist_sq;
  const bool progress_recovery = (current_progress + progress_backtrack_tolerance_) < (
    segments_[best_idx].cumulative_start + best_projection.t);
  if (best_idx != current_idx && !(strong_distance_improvement || progress_recovery)) {
    best_idx = current_idx;
    best_projection = current_projection;
  }

  current_segment_index_ = best_idx;
  projection = best_projection;
}

void NextNav2Controller::advanceSegmentIfReady(
  const geometry_msgs::msg::Point & robot_point,
  Projection & projection)
{
  while (current_segment_index_ + 1 < segments_.size()) {
    const Segment & segment = segments_[current_segment_index_];
    const double remaining = segment.length - projection.t;
    const double dist_to_end = std::hypot(
      robot_point.x - segment.p1.x,
      robot_point.y - segment.p1.y);

    // Issue 6: require BOTH along-track AND spatial proximity (true AND, not OR).
    // OR semantics allowed premature advances driven by spatial proximity alone,
    // causing heading jumps and spurious pivot activations at corners.
    const double spatial_threshold = segment_finish_epsilon_ * 2.0;
    const bool track_done = remaining <= segment_finish_epsilon_;
    const bool spatial_done = dist_to_end <= spatial_threshold;
    if (!(track_done && spatial_done)) {
      break;
    }

    ++current_segment_index_;
    projection = projectPointOntoSegment(robot_point, segments_[current_segment_index_]);
  }
}

NextNav2Controller::CandidateResult NextNav2Controller::scoreCandidate(
  double candidate_v,
  double candidate_w,
  const geometry_msgs::msg::PoseStamped & robot_pose,
  const geometry_msgs::msg::Twist & current_velocity,
  const Segment & segment,
  const geometry_msgs::msg::Point & target_point,
  double heading_reference,
  bool final_approach,
  bool goal_reached,
  double lateral_error_limit) const
{
  CandidateResult result;
  result.v = candidate_v;
  result.w = candidate_w;

  const int steps = std::max(1, static_cast<int>(std::ceil(horizon_time_ / rollout_dt_)));
  const double dt = horizon_time_ / static_cast<double>(steps);

  const auto footprint = costmap_ros_->getRobotFootprint();

  double x = robot_pose.pose.position.x;
  double y = robot_pose.pose.position.y;
  double theta = tf2::getYaw(robot_pose.pose.orientation);

  double obstacle_integral = 0.0;
  double lateral_error_integral = 0.0;

  // Track simulated progress along the full path so scoring remains valid
  // when a rollout crosses into a future segment near a turn.
  const double total_path_length = segments_.back().cumulative_start + segments_.back().length;
  const geometry_msgs::msg::Point init_robot_point = computeControlPoint(x, y, theta);
  double sim_progress = segment.cumulative_start +
    projectPointOntoSegment(init_robot_point, segment).t;
  std::size_t sim_seg_idx = current_segment_index_;

  for (int i = 0; i < steps; ++i) {
    x += candidate_v * std::cos(theta) * dt;
    y += candidate_v * std::sin(theta) * dt;
    theta = angles::normalize_angle(theta + candidate_w * dt);

    const geometry_msgs::msg::Point simulated_point = computeControlPoint(x, y, theta);

    // Advance sim_seg_idx forward as the simulation progresses along the path.
    while (sim_seg_idx + 1 < segments_.size()) {
      const Projection fwd = projectPointOntoSegment(simulated_point, segments_[sim_seg_idx]);
      const double remaining_on_seg = segments_[sim_seg_idx].length - fwd.t;
      if (remaining_on_seg > segment_finish_epsilon_) {
        break;
      }
      ++sim_seg_idx;
    }
    const Segment & sim_seg = segments_[sim_seg_idx];
    const Projection simulated_projection = projectPointOntoSegment(simulated_point, sim_seg);
    sim_progress = std::min(
      total_path_length,
      sim_seg.cumulative_start + simulated_projection.t);

    // Issue 26: use the heading of the currently-simulated segment as the
    // reference so scoring is aware of upcoming turns, not just the initial
    // segment heading that was valid at tick entry.
    const double sim_heading_ref = final_approach ? heading_reference : sim_seg.heading;

    const double simulated_lateral_error = std::abs(simulated_projection.lateral_error);
    lateral_error_integral += simulated_lateral_error;
    if (strict_line_tracking_ && strict_line_path_captured_ &&
      !final_approach && !goal_reached && simulated_lateral_error > lateral_error_limit) {
      result.feasible = false;
      return result;
    }

    if (!allow_unknown_space_ && footprintTouchesUnknown(x, y, theta, footprint)) {
      result.feasible = false;
      return result;
    }

    const double cost = poseCost(x, y, theta, footprint);
    if (cost < 0.0 || cost >= static_cast<double>(nav2_costmap_2d::LETHAL_OBSTACLE)) {
      result.feasible = false;
      return result;
    }

    const double normalized_cost = cost >= static_cast<double>(nav2_costmap_2d::INSCRIBED_INFLATED_OBSTACLE)
      ? 1.0
      : std::clamp(
      cost / static_cast<double>(nav2_costmap_2d::MAX_NON_OBSTACLE),
      0.0,
      1.0);

    obstacle_integral += normalized_cost;
    (void)sim_heading_ref;  // used below at end-of-horizon
  }

  const geometry_msgs::msg::Point end_point = computeControlPoint(x, y, theta);

  const Segment & final_sim_seg = segments_[sim_seg_idx];
  const Projection end_projection = projectPointOntoSegment(end_point, final_sim_seg);
  const double end_progress = sim_progress;
  // Issue 26: use the final simulated segment's heading as the terminal
  // reference so candidates that track upcoming turns are properly rewarded.
  const double final_sim_heading_ref =
    final_approach ? heading_reference : final_sim_seg.heading;
  const double heading_error_end =
    std::abs(angles::shortest_angular_distance(theta, final_sim_heading_ref));
  const double target_dx = target_point.x - end_point.x;
  const double target_dy = target_point.y - end_point.y;
  const double target_distance_end = std::hypot(target_dx, target_dy);
  const double target_heading = std::atan2(target_dy, target_dx);
  const double target_bearing_error_end = target_distance_end > 0.02
    ? std::abs(angles::shortest_angular_distance(theta, target_heading))
    : 0.0;

  const double v_ref =
    std::abs(current_velocity.linear.x) > 0.01 ? current_velocity.linear.x : last_command_.linear.x;
  const double w_ref =
    std::abs(current_velocity.angular.z) > 0.01 ? current_velocity.angular.z : last_command_.angular.z;
  const double dv = candidate_v - v_ref;
  const double dw = candidate_w - w_ref;

  const double smooth_cost = (dv * dv) + (0.4 * dw * dw);
  const double command_jerk =
    std::pow(candidate_v - last_command_.linear.x, 2) + 0.4 *
    std::pow(candidate_w - last_command_.angular.z, 2);

  const double lateral_error_avg = lateral_error_integral / static_cast<double>(steps);

  double score =
    (w_obstacle_ * (obstacle_integral / static_cast<double>(steps))) +
    (w_smooth_ * (smooth_cost + command_jerk)) +
    (w_angular_rate_ * candidate_w * candidate_w);

  if (strict_line_tracking_ && strict_line_path_captured_ && !final_approach && !goal_reached) {
    score += strict_line_lateral_weight_scale_ * w_lateral_ * lateral_error_avg * lateral_error_avg;
  }

  if (final_approach && !goal_reached) {
    // Penalise angular rate during final approach to avoid overshooting the goal.
    score += 0.3 * std::abs(candidate_w);
  }

  result.feasible = true;
  result.score = score;
  result.projected_progress = end_progress;
  result.lateral_error_end = std::abs(end_projection.lateral_error);
  result.heading_error_end = heading_error_end;
  result.target_distance_end = target_distance_end;
  result.target_bearing_error_end = target_bearing_error_end;
  result.obstacle_cost_avg = obstacle_integral / static_cast<double>(steps);

  return result;
}

double NextNav2Controller::poseCost(
  double x,
  double y,
  double theta,
  const std::vector<geometry_msgs::msg::Point> & footprint) const
{
  if (!footprint.empty()) {
    return collision_checker_.footprintCostAtPose(x, y, theta, footprint);
  }

  unsigned int mx = 0;
  unsigned int my = 0;
  if (!costmap_->worldToMap(x, y, mx, my)) {
    return -1.0;
  }

  return static_cast<double>(costmap_->getCost(mx, my));
}

bool NextNav2Controller::pointIsUnknown(double x, double y) const
{
  unsigned int mx = 0;
  unsigned int my = 0;
  if (!costmap_->worldToMap(x, y, mx, my)) {
    return true;
  }
  const auto cost = costmap_->getCost(mx, my);
  return cost == nav2_costmap_2d::NO_INFORMATION;
}

bool NextNav2Controller::footprintTouchesUnknown(
  double x,
  double y,
  double theta,
  const std::vector<geometry_msgs::msg::Point> & footprint) const
{
  if (allow_unknown_space_) {
    return false;
  }

  if (pointIsUnknown(x, y)) {
    return true;
  }

  if (footprint.empty()) {
    return false;
  }

  std::vector<geometry_msgs::msg::Point> oriented;
  nav2_costmap_2d::transformFootprint(x, y, theta, footprint, oriented);
  if (oriented.empty()) {
    return pointIsUnknown(x, y);
  }

  for (const auto & point : oriented) {
    if (pointIsUnknown(point.x, point.y)) {
      return true;
    }
  }

  const double resolution = std::max(0.01, unknown_check_resolution_);
  for (std::size_t i = 0; i < oriented.size(); ++i) {
    const auto & a = oriented[i];
    const auto & b = oriented[(i + 1) % oriented.size()];
    const double dx = b.x - a.x;
    const double dy = b.y - a.y;
    const double distance = std::hypot(dx, dy);
    const int samples = std::max(1, static_cast<int>(std::ceil(distance / resolution)));
    for (int s = 0; s <= samples; ++s) {
      const double alpha = static_cast<double>(s) / static_cast<double>(samples);
      const double px = a.x + alpha * dx;
      const double py = a.y + alpha * dy;
      if (pointIsUnknown(px, py)) {
        return true;
      }
    }
  }

  // Issue 18: rasterize the interior of the footprint polygon at the costmap
  // resolution so a blob of NO_INFORMATION fully enclosed by the footprint is
  // not missed by the edge-only check above.
  double bb_min_x = oriented[0].x, bb_max_x = oriented[0].x;
  double bb_min_y = oriented[0].y, bb_max_y = oriented[0].y;
  for (const auto & pt : oriented) {
    bb_min_x = std::min(bb_min_x, pt.x);
    bb_max_x = std::max(bb_max_x, pt.x);
    bb_min_y = std::min(bb_min_y, pt.y);
    bb_max_y = std::max(bb_max_y, pt.y);
  }

  // Point-in-polygon ray-casting test
  auto point_in_polygon = [&](double px, double py) -> bool {
    bool inside = false;
    const std::size_t n = oriented.size();
    for (std::size_t i = 0, j = n - 1; i < n; j = i++) {
      const double xi = oriented[i].x, yi = oriented[i].y;
      const double xj = oriented[j].x, yj = oriented[j].y;
      if (((yi > py) != (yj > py)) &&
        (px < (xj - xi) * (py - yi) / (yj - yi) + xi))
      {
        inside = !inside;
      }
    }
    return inside;
  };

  for (double gx = bb_min_x + resolution * 0.5; gx < bb_max_x; gx += resolution) {
    for (double gy = bb_min_y + resolution * 0.5; gy < bb_max_y; gy += resolution) {
      if (point_in_polygon(gx, gy) && pointIsUnknown(gx, gy)) {
        return true;
      }
    }
  }

  return false;
}

geometry_msgs::msg::TwistStamped NextNav2Controller::makeZeroCommand(const rclcpp::Time & stamp) const
{
  geometry_msgs::msg::TwistStamped cmd;
  cmd.header.stamp = stamp;
  cmd.header.frame_id = base_frame_;
  cmd.twist.linear.x = 0.0;
  cmd.twist.linear.y = 0.0;
  cmd.twist.linear.z = 0.0;
  cmd.twist.angular.x = 0.0;
  cmd.twist.angular.y = 0.0;
  cmd.twist.angular.z = 0.0;
  return cmd;
}

double NextNav2Controller::clamp01(double value)
{
  return std::clamp(value, 0.0, 1.0);
}

bool NextNav2Controller::diffDriveKinematicsFeasible(double v, double w) const
{
  if (!allow_reverse_ && v < -kTiny) {
    return false;
  }

  if (!enforce_diff_drive_kinematics_ || wheel_separation_ <= kTiny) {
    return true;
  }

  const double half_track = 0.5 * wheel_separation_;
  const double v_left = v - (w * half_track);
  const double v_right = v + (w * half_track);
  const double wheel_limit = std::max(0.0, max_wheel_linear_speed_);
  if (wheel_limit <= 0.0) {
    return true;
  }

  return (std::abs(v_left) <= wheel_limit + kTiny) && (std::abs(v_right) <= wheel_limit + kTiny);
}

void NextNav2Controller::publishDebug(
  const geometry_msgs::msg::Point & projection_point,
  const geometry_msgs::msg::Point & target_point,
  double lateral_error,
  double distance_remaining,
  double heading_error_signed,
  double cmd_v,
  double cmd_w,
  bool final_approach,
  bool in_rotate,
  const rclcpp::Time & stamp) const
{
  if (projection_pub_ && projection_pub_->is_activated()) {
    geometry_msgs::msg::PointStamped msg;
    msg.header.stamp = stamp;
    msg.header.frame_id = global_frame_;
    msg.point = projection_point;
    projection_pub_->publish(msg);
  }

  if (target_pub_ && target_pub_->is_activated()) {
    geometry_msgs::msg::PointStamped msg;
    msg.header.stamp = stamp;
    msg.header.frame_id = global_frame_;
    msg.point = target_point;
    target_pub_->publish(msg);
  }

  if (segment_index_pub_ && segment_index_pub_->is_activated()) {
    std_msgs::msg::Int32 msg;
    msg.data = static_cast<int32_t>(current_segment_index_);
    segment_index_pub_->publish(msg);
  }

  if (lateral_error_pub_ && lateral_error_pub_->is_activated()) {
    std_msgs::msg::Float64 msg;
    msg.data = lateral_error;
    lateral_error_pub_->publish(msg);
  }

  if (distance_remaining_pub_ && distance_remaining_pub_->is_activated()) {
    std_msgs::msg::Float64 msg;
    msg.data = distance_remaining;
    distance_remaining_pub_->publish(msg);
  }

  if (heading_error_pub_ && heading_error_pub_->is_activated()) {
    std_msgs::msg::Float64 msg;
    msg.data = heading_error_signed;
    heading_error_pub_->publish(msg);
  }

  if (cmd_v_pub_ && cmd_v_pub_->is_activated()) {
    std_msgs::msg::Float64 msg;
    msg.data = cmd_v;
    cmd_v_pub_->publish(msg);
  }

  if (cmd_w_pub_ && cmd_w_pub_->is_activated()) {
    std_msgs::msg::Float64 msg;
    msg.data = cmd_w;
    cmd_w_pub_->publish(msg);
  }

  if (final_approach_pub_ && final_approach_pub_->is_activated()) {
    std_msgs::msg::Float64 msg;
    msg.data = final_approach ? 1.0 : 0.0;
    final_approach_pub_->publish(msg);
  }

  if (rotate_mode_pub_ && rotate_mode_pub_->is_activated()) {
    std_msgs::msg::Float64 msg;
    msg.data = in_rotate ? 1.0 : 0.0;
    rotate_mode_pub_->publish(msg);
  }
}

// Issue 2/19: Swept collision check for in-place pivot.
// Samples the footprint at equal angular increments between from_yaw and to_yaw
// and rejects if any sample hits an obstacle or (when unknown space is blocked)
// an unknown cell.
bool NextNav2Controller::isPivotCollisionFree(
  double from_yaw, double to_yaw, double x, double y,
  const std::vector<geometry_msgs::msg::Point> & footprint) const
{
  const double sweep = std::abs(angles::shortest_angular_distance(from_yaw, to_yaw));
  // At least 4 samples; finer sampling every ~8.5° (~0.15 rad) for accuracy.
  const int samples = std::max(4, static_cast<int>(std::ceil(sweep / 0.15)));
  for (int i = 0; i <= samples; ++i) {
    const double alpha = static_cast<double>(i) / static_cast<double>(samples);
    const double theta = angles::normalize_angle(
      from_yaw + alpha * angles::shortest_angular_distance(from_yaw, to_yaw));

    if (!allow_unknown_space_ && footprintTouchesUnknown(x, y, theta, footprint)) {
      return false;
    }

    const double cost = poseCost(x, y, theta, footprint);
    if (cost < 0.0 ||
      cost >= static_cast<double>(nav2_costmap_2d::LETHAL_OBSTACLE))
    {
      return false;
    }
  }
  return true;
}

}  // namespace next_nav2_controller

PLUGINLIB_EXPORT_CLASS(next_nav2_controller::NextNav2Controller, nav2_core::Controller)
