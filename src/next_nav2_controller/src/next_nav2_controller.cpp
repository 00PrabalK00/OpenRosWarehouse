#include "next_nav2_controller/next_nav2_controller.hpp"

#include <algorithm>
#include <cctype>
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
  motion_intent_service_ = node_->create_service<next_ros2ws_interfaces::srv::SetMotionIntent>(
    motion_intent_service_name_,
    std::bind(
      &NextNav2Controller::handleSetMotionIntent,
      this,
      std::placeholders::_1,
      std::placeholders::_2));
  motion_intent_status_pub_ =
    node_->create_publisher<next_ros2ws_interfaces::msg::MotionIntentStatus>(
    motion_intent_status_topic_,
    rclcpp::SystemDefaultsQoS());

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
  last_progress_checkpoint_m_ = 0.0;
  last_progress_checkpoint_stamp_ = node_->now();
  last_command_ = geometry_msgs::msg::Twist();
  last_command_stamp_ = node_->now();
  last_plan_refresh_stamp_ = rclcpp::Time();
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
  motion_intent_ = MotionIntent();

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
  motion_intent_status_pub_.reset();
  motion_intent_service_.reset();

  global_plan_.poses.clear();
  raw_plan_.poses.clear();
  segments_.clear();

  blocked_ = false;
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
  {
    std::lock_guard<std::mutex> intent_lock(motion_intent_mutex_);
    motion_intent_ = MotionIntent();
  }
  current_segment_index_ = 0;
  t_along_segment_ = 0.0;
  stable_progress_m_ = 0.0;
  last_plan_refresh_stamp_ = rclcpp::Time();
  last_command_ = geometry_msgs::msg::Twist();
  last_command_stamp_ = rclcpp::Time();
  last_progress_checkpoint_m_ = 0.0;
  last_progress_checkpoint_stamp_ = rclcpp::Time();
  blocked_since_ = rclcpp::Time();
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
  if (motion_intent_status_pub_) {
    motion_intent_status_pub_->on_activate();
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
  if (motion_intent_status_pub_) {
    motion_intent_status_pub_->on_deactivate();
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
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
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
  // Issue 17: snapshot motion_intent under its own lock briefly so a pending
  // service update isn't blocked for the full compute cycle.
  MotionIntent active_motion_intent;
  {
    std::lock_guard<std::mutex> intent_lock(motion_intent_mutex_);
    if (
      motion_intent_.active &&
      motion_intent_.timeout_sec > 0.0 &&
      (now - motion_intent_.received_at).seconds() > motion_intent_.timeout_sec)
    {
      motion_intent_ = MotionIntent();
    }
    active_motion_intent = motion_intent_;
  }
  const auto current_motion_state = [&](bool rotating) {
      if (active_motion_intent.active) {
        if (active_motion_intent.mode == "insertion") {
          return std::string("INSERTION");
        }
        if (
          active_motion_intent.mode == "alignment" ||
          active_motion_intent.mode == "precision")
        {
          return std::string("ALIGNMENT");
        }
        if (active_motion_intent.mode == "recovery") {
          return std::string("RECOVERY");
        }
      }
      return std::string(rotating ? "ROTATE" : "FOLLOW_PATH");
    };

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

  advanceSegmentIfReady(robot_control_point, projection);

  Segment & segment = segments_[current_segment_index_];

  double absolute_progress = segment.cumulative_start + projection.t;
  if (absolute_progress > stable_progress_m_) {
    stable_progress_m_ = absolute_progress;
  }

  if (absolute_progress + progress_backtrack_tolerance_ < stable_progress_m_)
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
  double intent_target_error = goal_distance;
  if (active_motion_intent.active) {
    geometry_msgs::msg::PoseStamped intent_target = active_motion_intent.target_pose;
    if (intent_target.header.frame_id.empty()) {
      intent_target = goal_pose;
    } else if (intent_target.header.frame_id != global_frame_) {
      geometry_msgs::msg::PoseStamped transformed_intent_target;
      if (nav2_util::transformPoseInTargetFrame(
          intent_target, transformed_intent_target, *tf_, global_frame_, transform_tolerance_))
      {
        intent_target = transformed_intent_target;
      }
    }
    const double intent_dx = intent_target.pose.position.x - robot_pose.pose.position.x;
    const double intent_dy = intent_target.pose.position.y - robot_pose.pose.position.y;
    intent_target_error = std::hypot(intent_dx, intent_dy);
  }
  const double total_path_length = segments_.back().cumulative_start + segments_.back().length;
  const double along_track_remaining = std::max(0.0, total_path_length - absolute_progress);
  const bool on_last_segment = (current_segment_index_ + 1) >= segments_.size();
  // Issue 8: trigger final approach from along-track remaining, not Euclidean
  // distance to the goal pose. A laterally-offset robot that is spatially
  // "near" the goal would otherwise enter the terminal phase early and drop
  // strict-line precision before it has actually finished translating along
  // the shelf axis.
  const bool final_approach = on_last_segment && along_track_remaining <= final_approach_distance_;

  // Issue 10: honor motion-intent tolerances for terminal behavior. When the
  // caller has not specified, fall back to sensible defaults.
  const double terminal_xy_tol =
    (active_motion_intent.active && active_motion_intent.xy_tolerance > kTiny)
    ? active_motion_intent.xy_tolerance : 0.03;
  const double terminal_yaw_tol =
    (active_motion_intent.active && active_motion_intent.yaw_tolerance > kTiny)
    ? active_motion_intent.yaw_tolerance : 0.05;

  // Issue 4: explicit terminal phase split. In TRANSLATE we drive forward with
  // corridor precision and no heading injection. In YAW_SETTLE we force v=0
  // and clean up residual yaw.
  const bool arrived_xy =
    on_last_segment && along_track_remaining <= terminal_xy_tol;

  const double abs_lateral_error = std::abs(projection.lateral_error);

  // Issue 7: keep the strict-line corridor state machine live through final
  // approach so lateral precision is preserved for the last centimeters.
  if (strict_line_tracking_) {
    if (!strict_line_path_captured_) {
      if (abs_lateral_error <= strict_line_capture_lateral_error_) {
        strict_line_path_captured_ = true;
      }
    } else if (abs_lateral_error >= strict_line_release_lateral_error_) {
      strict_line_path_captured_ = false;
    }
  }
  const bool strict_line_locked = strict_line_tracking_ && strict_line_path_captured_;

  // Issue 9: allow the terminal speed envelope to decay cleanly to zero. The
  // previous 0.2 floor meant the controller kept "doing something" through the
  // final centimeters instead of settling, which amplified Issue 4 and Issue 20.
  double final_scale = 1.0;
  if (final_approach && final_approach_distance_ > kTiny) {
    final_scale = std::clamp(along_track_remaining / final_approach_distance_, 0.0, 1.0);
  }

  const double adaptive_lookahead = lookahead_distance_ +
    (lookahead_speed_gain_ * std::abs(velocity.linear.x));
  const double lookahead_nominal = std::clamp(
    adaptive_lookahead,
    lookahead_min_distance_,
    lookahead_max_distance_);
  const double lookahead = std::max(0.05, lookahead_nominal * final_scale);
  // Issue 9: in final approach do NOT floor at min_tracking_speed_ — let v
  // decay to zero along with final_scale so the robot can actually stop.
  double runtime_max_v = final_approach
    ? (max_v_ * final_scale)
    : std::max(min_tracking_speed_, max_v_ * final_scale);
  if (speed_profile_enabled_) {
    // Apply the precomputed per-segment velocity profile.  The backward pass in
    // buildVelocityProfile() already propagated braking constraints upstream, so
    // the current segment's limit naturally enforces anticipatory deceleration.
    runtime_max_v = std::min(runtime_max_v, segment.speed_limit);
  }
  if (active_motion_intent.active && active_motion_intent.max_linear_speed > kTiny) {
    runtime_max_v = std::min(runtime_max_v, active_motion_intent.max_linear_speed);
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
  // RPP owns the rotate-to-heading authority (see rotation_active_ below).
  const bool rotate_in_place_mode = false;

  // Issue 15: clamp dt to a bounded control-period window so missed or delayed
  // cycles don't open wide rate-limit windows that let command_v/w jump far.
  // Upper bound (0.2 s) tolerates a few missed 20 Hz cycles without letting a
  // stall-induced spike rewrite the rate envelope.
  const double dt_cmd = std::clamp((now - last_command_stamp_).seconds(), 0.02, 0.2);

  const double v_window = std::max(0.05, std::abs(acc_lim_v_) * dt_cmd);
  const double w_window = std::max(0.05, std::abs(acc_lim_w_) * dt_cmd);

  // Issue 16: always use measured state. Substituting last_command_ near zero
  // kept stale rotation alive during the terminal settle phase, which fed the
  // "slight pre-stop turn" symptom.
  const double v_feedback = velocity.linear.x;
  const double w_feedback = velocity.angular.z;

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
      publishMotionIntentStatus(
        active_motion_intent,
        current_motion_state(false),
        reason,
        intent_target_error,
        stop.twist.linear.x,
        stop.twist.angular.z,
        now);
      return stop;
    };

  {
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

    // Issue 9: in final approach don't floor the desired speed at
    // min_tracking_speed_ — let it track final_scale all the way to zero so the
    // terminal phase can actually settle rather than "keep doing something".
    double command_v = final_approach
      ? std::min(runtime_max_v, rpp_desired_linear_vel_ * final_scale)
      : std::min(runtime_max_v, std::max(min_tracking_speed_, rpp_desired_linear_vel_ * final_scale));
    if (allow_reverse_ && x_robot < 0.0) {
      command_v = -command_v;
    } else if (!allow_reverse_) {
      command_v = std::max(0.0, command_v);
    }
    // Issue 4: once the robot has arrived at the goal xy, force v=0. Any
    // residual yaw cleanup is handled by the rotate-to-heading machinery below.
    if (arrived_xy) {
      command_v = 0.0;
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
    // Issue 4: NO heading-blend injection during final approach. Mixing yaw
    // cleanup with the last centimeters of translation causes the "slight
    // pre-stop turn" symptom. Residual yaw is cleaned up by the rotate-to-
    // heading state machine once the robot is at the goal xy (arrived_xy).
    // Issue 20: apply an angular clamp tied to forward speed in ALL phases,
    // including final approach, so |w| cannot dominate linear motion as v
    // decays. The small additive term keeps very-low-speed corrective yaw
    // possible without gating it entirely on |v|.
    {
      const double tracking_w_limit = std::max(0.20, 2.0 * std::abs(command_v) + 0.05);
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

    // Issue 4/21: hysteresis for RPP rotate-to-heading mode. When the robot
    // has arrived at the goal xy, the entry threshold collapses to the caller's
    // yaw tolerance so residual yaw is cleaned up in a dedicated YAW phase
    // instead of mixed into translation. Exit threshold is half of entry so a
    // small settling oscillation doesn't bounce us out of the rotation.
    const double rotate_min_angle = arrived_xy
      ? std::max(terminal_yaw_tol, 0.02)
      : rpp_rotate_to_heading_min_angle_;
    const double rotate_exit_angle = arrived_xy
      ? std::max(0.5 * terminal_yaw_tol, 0.01)
      : rpp_rotate_to_heading_exit_angle_;
    if (!in_rpp_rotate_mode_) {
      in_rpp_rotate_mode_ = rpp_use_rotate_to_heading_ &&
        (final_approach
          ? (rotate_heading_error > rotate_min_angle)
          : (target_behind || rotate_heading_error > rotate_min_angle));
    } else {
      in_rpp_rotate_mode_ = rpp_use_rotate_to_heading_ &&
        (final_approach
          ? (rotate_heading_error > rotate_exit_angle)
          : (target_behind || rotate_heading_error > rotate_exit_angle));
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


    const double obstacle_speed_scale = std::clamp(
      1.0 - (obstacle_speed_reduction_gain_ * obstacle_cost_avg),
      min_obstacle_speed_scale_,
      1.0);
    // Issue 9: keep the min_tracking_speed_ floor out of final approach so
    // obstacle-scaled runtime_max_v can also decay to zero.
    runtime_max_v = final_approach
      ? (runtime_max_v * obstacle_speed_scale)
      : std::max(min_tracking_speed_, runtime_max_v * obstacle_speed_scale);
    const double runtime_min_v_cmd = allow_reverse_ ? -runtime_max_v : 0.0;
    command_v = std::clamp(command_v, runtime_min_v_cmd, runtime_max_v);
    command_v = std::clamp(command_v, v_min, v_max);
    if (arrived_xy) {
      command_v = 0.0;
    }

    // Issue 3/18: preserve desired curvature after all forward-speed clamps.
    // Earlier stages computed command_w from the original command_v; without
    // this recompute, obstacle-cost slowdown / rate limiting produces a tighter
    // arc than intended. Issue 4/20: no final-approach heading injection; the
    // w clamp tied to |v| is applied in every phase so angular can't dominate
    // linear motion as v decays.
    if (!in_rpp_rotate_mode_) {
      command_w = curvature * command_v;
      const double tracking_w_limit = std::max(0.20, 2.0 * std::abs(command_v) + 0.05);
      const double clamped_w_limit = std::min(max_w_, tracking_w_limit);
      command_w = std::clamp(command_w, -clamped_w_limit, clamped_w_limit);
      enforce_diff_drive_limits(command_v, command_w);
    }
    command_w = std::clamp(command_w, w_min, w_max);
    if (active_motion_intent.active && active_motion_intent.max_angular_speed > kTiny) {
      const double intent_max_w = std::clamp(active_motion_intent.max_angular_speed, 0.0, max_w_);
      command_w = std::clamp(command_w, -intent_max_w, intent_max_w);
    }
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
      in_rpp_rotate_mode_ && rpp_use_rotate_to_heading_;
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
    publishMotionIntentStatus(
      active_motion_intent,
      current_motion_state(intentional_rotation_rpp),
      obstacle_cost_avg > 0.0 ? "obstacle_cost" : "path_tracking",
      intent_target_error,
      cmd.twist.linear.x,
      cmd.twist.angular.z,
      now);
    publishDebug(projection.point, rpp_target_point, projection.lateral_error,
      goal_distance, heading_error_reference_signed,
      cmd.twist.linear.x, cmd.twist.angular.z,
      final_approach, in_rpp_rotate_mode_, now);
    return cmd;
  }
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
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
  blocked_ = false;
  RCLCPP_DEBUG(node_->get_logger(), "%s: cancel() called", plugin_name_.c_str());
  return true;
}

void NextNav2Controller::reset()
{
  std::lock_guard<std::mutex> lock(data_mutex_);
  in_rpp_rotate_mode_ = false;
  strict_line_path_captured_ = false;
  blocked_ = false;
  blocked_since_ = rclcpp::Time();
  stable_progress_m_ = 0.0;
  last_progress_checkpoint_m_ = 0.0;
  last_progress_checkpoint_stamp_ = rclcpp::Time();
  last_command_ = geometry_msgs::msg::Twist();
  last_command_stamp_ = rclcpp::Time();
  RCLCPP_DEBUG(node_->get_logger(), "%s: reset() called", plugin_name_.c_str());
}

void NextNav2Controller::handleSetMotionIntent(
  const std::shared_ptr<next_ros2ws_interfaces::srv::SetMotionIntent::Request> request,
  std::shared_ptr<next_ros2ws_interfaces::srv::SetMotionIntent::Response> response)
{
  // Issue 17: use the dedicated motion_intent mutex so a busy compute cycle
  // cannot block service updates.
  std::lock_guard<std::mutex> lock(motion_intent_mutex_);

  std::string mode = request ? request->mode : "";
  std::transform(
    mode.begin(), mode.end(), mode.begin(),
    [](unsigned char c) {return static_cast<char>(std::tolower(c));});

  if (mode.empty() || mode == "normal" || mode == "clear") {
    motion_intent_ = MotionIntent();
    response->accepted = true;
    response->intent_id = 0;
    response->message = "Motion intent cleared";
    return;
  }

  MotionIntent intent;
  intent.active = true;
  intent.intent_id = next_motion_intent_id_++;
  intent.mode = mode;
  intent.source = request ? request->source : "";
  intent.target_pose = request->target_pose;
  intent.max_linear_speed = std::max(0.0F, request->max_linear_speed);
  intent.max_angular_speed = std::max(0.0F, request->max_angular_speed);
  intent.xy_tolerance = std::max(0.0F, request->xy_tolerance);
  intent.yaw_tolerance = std::max(0.0F, request->yaw_tolerance);
  intent.timeout_sec = std::max(0.0F, request->timeout_sec);
  intent.received_at = node_->now();
  motion_intent_ = intent;

  response->accepted = true;
  response->intent_id = intent.intent_id;
  response->message =
    "Motion intent accepted: mode=" + intent.mode + " source=" + intent.source;
}

void NextNav2Controller::publishMotionIntentStatus(
  const MotionIntent & intent_snapshot,
  const std::string & state,
  const std::string & limiting_factor,
  double target_error,
  double cmd_v,
  double cmd_w,
  const rclcpp::Time & stamp) const
{
  (void)stamp;
  if (!motion_intent_status_pub_ || !motion_intent_status_pub_->is_activated()) {
    return;
  }

  // Issue 17: publish from a locally-passed snapshot so we don't need to
  // reacquire motion_intent_mutex_ here (avoids nesting).
  next_ros2ws_interfaces::msg::MotionIntentStatus msg;
  msg.intent_id = intent_snapshot.intent_id;
  msg.active = intent_snapshot.active;
  msg.source = intent_snapshot.source;
  msg.mode = intent_snapshot.active ? intent_snapshot.mode : "normal";
  msg.state = state;
  msg.limiting_factor = limiting_factor;
  msg.target_error = static_cast<float>(target_error);
  msg.cmd_v = static_cast<float>(cmd_v);
  msg.cmd_w = static_cast<float>(cmd_w);
  motion_intent_status_pub_->publish(msg);
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
    plugin_name_ + ".strict_line_k_heading",
    rclcpp::ParameterValue(strict_line_k_heading_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".strict_line_k_lateral",
    rclcpp::ParameterValue(strict_line_k_lateral_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".debug_publishers", rclcpp::ParameterValue(debug_publishers_));
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".control_mode", rclcpp::ParameterValue(control_mode_name_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".motion_intent_service_name",
    rclcpp::ParameterValue(motion_intent_service_name_));
  nav2_util::declare_parameter_if_not_declared(
    node,
    plugin_name_ + ".motion_intent_status_topic",
    rclcpp::ParameterValue(motion_intent_status_topic_));
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
  node->get_parameter(plugin_name_ + ".enforce_diff_drive_kinematics", enforce_diff_drive_kinematics_);
  node->get_parameter(plugin_name_ + ".allow_reverse", allow_reverse_);
  node->get_parameter(plugin_name_ + ".wheel_separation", wheel_separation_);
  node->get_parameter(plugin_name_ + ".max_wheel_linear_speed", max_wheel_linear_speed_);
  node->get_parameter(plugin_name_ + ".final_approach_distance", final_approach_distance_);
  node->get_parameter(plugin_name_ + ".blocked_timeout", blocked_timeout_);
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
  node->get_parameter(plugin_name_ + ".strict_line_k_heading", strict_line_k_heading_);
  node->get_parameter(plugin_name_ + ".strict_line_k_lateral", strict_line_k_lateral_);
  node->get_parameter(plugin_name_ + ".debug_publishers", debug_publishers_);
  node->get_parameter(plugin_name_ + ".control_mode", control_mode_name_);
  node->get_parameter(plugin_name_ + ".motion_intent_service_name", motion_intent_service_name_);
  node->get_parameter(plugin_name_ + ".motion_intent_status_topic", motion_intent_status_topic_);
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

  if (motion_intent_service_name_.empty()) {
    motion_intent_service_name_ = "/controller_server/set_motion_intent";
  }
  if (motion_intent_status_topic_.empty()) {
    motion_intent_status_topic_ = "/controller_server/motion_intent_status";
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
  lookahead_speed_gain_ = std::max(0.0, lookahead_speed_gain_);
  lookahead_min_distance_ = std::max(0.05, lookahead_min_distance_);
  lookahead_max_distance_ = std::max(lookahead_min_distance_, lookahead_max_distance_);
  wheel_separation_ = std::max(0.0, wheel_separation_);
  max_wheel_linear_speed_ = std::max(0.0, max_wheel_linear_speed_);
  // rotate_in_place_max_v_ and base_rotate_in_place_max_v_ already set above
  obstacle_speed_reduction_gain_ = std::clamp(obstacle_speed_reduction_gain_, 0.0, 1.0);
  min_obstacle_speed_scale_ = std::clamp(min_obstacle_speed_scale_, 0.05, 1.0);
  unknown_check_resolution_ = std::max(0.01, unknown_check_resolution_);
  progress_stall_timeout_ = std::max(0.2, progress_stall_timeout_);
  progress_stall_epsilon_ = std::max(0.001, progress_stall_epsilon_);
  progress_stall_cmd_v_threshold_ = std::max(0.001, progress_stall_cmd_v_threshold_);
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
  strict_line_k_heading_ = std::max(0.0, strict_line_k_heading_);
  strict_line_k_lateral_ = std::max(0.0, strict_line_k_lateral_);
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
  rpp_collision_check_resolution_ = std::max(0.01, rpp_collision_check_resolution_);

  if (!allow_reverse_) {
    min_cruise_speed_ = std::min(min_cruise_speed_, max_v_);
  }
  segment_finish_epsilon_ = std::max(0.01, segment_finish_epsilon_);

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

  // ── RPP rotate-to-heading exit angle ─────────────────────────────────────
  nav2_util::declare_parameter_if_not_declared(
    node, plugin_name_ + ".rpp_rotate_to_heading_exit_angle",
    rclcpp::ParameterValue(rpp_rotate_to_heading_exit_angle_));
  node->get_parameter(
    plugin_name_ + ".rpp_rotate_to_heading_exit_angle", rpp_rotate_to_heading_exit_angle_);
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

}  // namespace next_nav2_controller

PLUGINLIB_EXPORT_CLASS(next_nav2_controller::NextNav2Controller, nav2_core::Controller)
