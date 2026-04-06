#ifndef NEXT_NAV2_CONTROLLER__NEXT_NAV2_CONTROLLER_HPP_
#define NEXT_NAV2_CONTROLLER__NEXT_NAV2_CONTROLLER_HPP_

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <random>

#include "geometry_msgs/msg/point.hpp"
#include "geometry_msgs/msg/point_stamped.hpp"
#include "geometry_msgs/msg/pose_stamped.hpp"
#include "geometry_msgs/msg/twist.hpp"
#include "geometry_msgs/msg/twist_stamped.hpp"
#include "nav2_core/controller.hpp"
#include "nav2_costmap_2d/cost_values.hpp"
#include "nav2_costmap_2d/costmap_2d.hpp"
#include "nav2_costmap_2d/costmap_2d_ros.hpp"
#include "nav2_costmap_2d/footprint_collision_checker.hpp"
#include "nav_msgs/msg/path.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "std_msgs/msg/float64.hpp"
#include "std_msgs/msg/int32.hpp"
#include "nav2_core/exceptions.hpp"
#include "tf2_ros/buffer.h"

namespace next_nav2_controller
{

class NextNav2Controller : public nav2_core::Controller
{
public:
  NextNav2Controller() = default;
  ~NextNav2Controller() override = default;

  void configure(
    const rclcpp_lifecycle::LifecycleNode::WeakPtr & parent,
    std::string name,
    std::shared_ptr<tf2_ros::Buffer> tf,
    std::shared_ptr<nav2_costmap_2d::Costmap2DROS> costmap_ros) override;

  void cleanup() override;
  void activate() override;
  void deactivate() override;

  void setPlan(const nav_msgs::msg::Path & path) override;

  geometry_msgs::msg::TwistStamped computeVelocityCommands(
    const geometry_msgs::msg::PoseStamped & pose,
    const geometry_msgs::msg::Twist & velocity,
    nav2_core::GoalChecker * goal_checker) override;

  void setSpeedLimit(const double & speed_limit, const bool & percentage) override;

  // Issue 32: explicit cancel/reset so pivot, progress, and MPC state are cleared
  bool cancel();
  void reset();

private:
  enum class ControlMode
  {
    Rollout,
    Rpp,
    Mpc
  };

  struct Segment
  {
    geometry_msgs::msg::Point p0;
    geometry_msgs::msg::Point p1;
    double dx {0.0};
    double dy {0.0};
    double length {0.0};
    double ux {1.0};
    double uy {0.0};
    double heading {0.0};
    double cumulative_start {0.0};
    double speed_limit {1e9};  // precomputed velocity-profile cap (m/s)
  };

  struct Projection
  {
    double t {0.0};
    double lateral_error {0.0};
    geometry_msgs::msg::Point point;
  };

  struct CandidateResult
  {
    bool feasible {false};
    double score {0.0};
    double v {0.0};
    double w {0.0};
    double projected_progress {0.0};
    double lateral_error_end {0.0};
    double heading_error_end {0.0};
    double target_distance_end {0.0};
    double target_bearing_error_end {0.0};
    double obstacle_cost_avg {0.0};
  };

  void declareAndLoadParameters();

  nav_msgs::msg::Path transformPathToGlobalFrame(const nav_msgs::msg::Path & path) const;
  void rebuildSegments();
  void buildVelocityProfile();

  Projection projectPointOntoSegment(
    const geometry_msgs::msg::Point & point,
    const Segment & segment) const;

  geometry_msgs::msg::Point pointOnSegment(
    const Segment & segment,
    double t_along) const;

  geometry_msgs::msg::Point computeControlPoint(
    double base_x,
    double base_y,
    double yaw) const;

  geometry_msgs::msg::Point pointOnPath(
    double progress_m,
    std::size_t * segment_index = nullptr) const;

  void reanchorToNearestSegment(
    const geometry_msgs::msg::Point & robot_point,
    Projection & projection,
    double robot_yaw = 0.0,
    bool use_heading_score = false);

  void advanceSegmentIfReady(const geometry_msgs::msg::Point & robot_point, Projection & projection);

  CandidateResult scoreCandidate(
    double candidate_v,
    double candidate_w,
    const geometry_msgs::msg::PoseStamped & robot_pose,
    const geometry_msgs::msg::Twist & current_velocity,
    const Segment & segment,
    const geometry_msgs::msg::Point & target_point,
    double heading_reference,
    bool final_approach,
    bool goal_reached,
    double lateral_error_limit) const;

  double poseCost(
    double x,
    double y,
    double theta,
    const std::vector<geometry_msgs::msg::Point> & footprint) const;
  bool pointIsUnknown(double x, double y) const;
  bool footprintTouchesUnknown(
    double x,
    double y,
    double theta,
    const std::vector<geometry_msgs::msg::Point> & footprint) const;

  geometry_msgs::msg::TwistStamped makeZeroCommand(const rclcpp::Time & stamp) const;

  static double clamp01(double value);
  bool diffDriveKinematicsFeasible(double v, double w) const;

  void publishDebug(
    const geometry_msgs::msg::Point & projection_point,
    const geometry_msgs::msg::Point & target_point,
    double lateral_error,
    double distance_remaining,
    double heading_error_signed,
    double cmd_v,
    double cmd_w,
    bool final_approach,
    bool in_rotate,
    const rclcpp::Time & stamp) const;

  // Issue 2: check that the full angular sweep of a pivot is collision-free
  bool isPivotCollisionFree(
    double from_yaw,
    double to_yaw,
    double x,
    double y,
    const std::vector<geometry_msgs::msg::Point> & footprint) const;

  rclcpp_lifecycle::LifecycleNode::SharedPtr node_;
  std::string plugin_name_;
  std::shared_ptr<tf2_ros::Buffer> tf_;
  std::shared_ptr<nav2_costmap_2d::Costmap2DROS> costmap_ros_;
  nav2_costmap_2d::Costmap2D * costmap_ {nullptr};
  mutable nav2_costmap_2d::FootprintCollisionChecker<nav2_costmap_2d::Costmap2D *> collision_checker_;

  nav_msgs::msg::Path raw_plan_;
  nav_msgs::msg::Path global_plan_;
  std::vector<Segment> segments_;

  std::string global_frame_;
  std::string base_frame_;

  mutable std::mutex data_mutex_;

  std::size_t current_segment_index_ {0};
  double t_along_segment_ {0.0};
  double stable_progress_m_ {0.0};
  double last_progress_checkpoint_m_ {0.0};
  rclcpp::Time last_progress_checkpoint_stamp_;

  bool blocked_ {false};
  rclcpp::Time blocked_since_;
  geometry_msgs::msg::Twist last_command_;
  rclcpp::Time last_command_stamp_;
  rclcpp::Time last_plan_refresh_stamp_;

  double lookahead_distance_ {0.65};
  double lookahead_speed_gain_ {0.9};
  double lookahead_min_distance_ {0.30};
  double lookahead_max_distance_ {1.25};
  double segment_finish_epsilon_ {0.12};
  double max_v_ {0.75};
  double base_max_v_ {0.75};
  double max_w_ {1.2};
  double acc_lim_v_ {2.5};
  double decel_lim_v_ {2.5};  // separate deceleration limit for velocity profile braking pass
  double acc_lim_w_ {3.5};
  double horizon_time_ {1.0};
  int num_samples_ {84};
  int sample_grid_v_ {7};
  int sample_grid_w_ {13};
  double w_lateral_ {8.0};
  double w_heading_ {5.0};
  double w_progress_ {8.5};
  double w_smooth_ {0.6};
  double w_obstacle_ {8.0};
  double w_target_ {7.0};
  double w_bearing_ {3.0};
  double w_angular_rate_ {0.08};
  double w_misaligned_forward_ {3.0};
  bool enforce_diff_drive_kinematics_ {true};
  bool allow_reverse_ {false};
  double wheel_separation_ {0.51};
  double max_wheel_linear_speed_ {1.25};
  double final_approach_distance_ {0.8};
  double blocked_timeout_ {2.5};
  double rotate_to_heading_threshold_ {0.90};
  double rotate_in_place_max_v_ {0.00};
  double base_rotate_in_place_max_v_ {0.00};  // stores configured value; never ratcheted down
  double obstacle_speed_reduction_gain_ {0.28};
  double min_obstacle_speed_scale_ {0.55};
  bool allow_unknown_space_ {false};
  double unknown_check_resolution_ {0.05};
  double progress_stall_timeout_ {2.5};
  double progress_stall_epsilon_ {0.05};
  double progress_stall_cmd_v_threshold_ {0.06};
  bool abort_on_blocked_timeout_ {false};
  bool abort_on_progress_stall_ {false};

  double rollout_dt_ {0.10};
  double transform_tolerance_ {0.2};
  double plan_refresh_period_ {0.25};
  double progress_backtrack_tolerance_ {0.08};
  double min_tracking_speed_ {0.07};
  double min_cruise_speed_ {0.30};
  bool strict_line_tracking_ {true};
  double strict_line_max_lateral_error_ {0.08};
  double strict_line_max_lateral_growth_ {0.03};
  bool strict_line_adaptive_lateral_limit_ {true};
  double strict_line_capture_lateral_error_ {0.18};
  double strict_line_release_lateral_error_ {0.37};
  double strict_line_speed_slowdown_error_ {0.03};
  double strict_line_min_speed_scale_ {0.20};
  double strict_line_lateral_weight_scale_ {3.0};
  bool strict_line_geometric_control_ {true};
  double strict_line_k_heading_ {2.8};
  double strict_line_k_lateral_ {1.8};
  double strict_line_rotate_only_error_ {0.35};
  bool disallow_arcing_ {false};
  double no_arc_heading_error_threshold_ {0.05};

  ControlMode control_mode_ {ControlMode::Rollout};
  std::string control_mode_name_ {"rollout"};

  // RPP-style tracking parameters
  bool rpp_use_velocity_scaled_lookahead_ {true};
  double rpp_desired_linear_vel_ {0.45};
  double rpp_regulated_linear_scaling_min_radius_ {0.90};
  double rpp_regulated_linear_scaling_min_speed_ {0.08};
  bool rpp_use_rotate_to_heading_ {true};
  double rpp_rotate_to_heading_min_angle_ {0.785};
  double rpp_rotate_to_heading_angular_vel_ {1.0};
  bool rpp_use_collision_detection_ {true};
  double rpp_max_allowed_time_to_collision_ {1.0};
  double rpp_collision_check_resolution_ {0.05};
  double rpp_segment_lookahead_turn_threshold_ {0.20};
  bool rpp_use_obstacle_fallback_ {true};
  double rpp_obstacle_fallback_cost_threshold_ {0.35};
  int rpp_obstacle_fallback_v_samples_ {3};
  int rpp_obstacle_fallback_w_samples_ {7};

  // MPC-style random-shooting optimization
  int mpc_steps_ {10};
  int mpc_iterations_ {2};
  int mpc_num_samples_ {24};
  double mpc_noise_v_ {0.08};
  double mpc_noise_w_ {0.20};
  bool mpc_warm_start_ {true};
  double mpc_terminal_heading_weight_ {5.0};
  double mpc_terminal_lateral_weight_ {8.0};
  double mpc_terminal_target_weight_ {7.0};
  int mpc_seed_ {42};
  std::mt19937 mpc_rng_ {42};
  std::vector<double> mpc_v_sequence_;
  std::vector<double> mpc_w_sequence_;

  // ── Waypoint pivot: fast in-place turn at segment junctions ─────────────
  bool waypoint_pivot_enabled_ {true};
  double waypoint_pivot_threshold_ {0.45};          // rad  – heading change that triggers a pivot
  double waypoint_pivot_approach_distance_ {0.40};  // m    – start braking this far from the junction
  double waypoint_pivot_center_epsilon_ {0.14};     // m    – enter full pivot when this close
  double waypoint_pivot_angular_vel_ {2.2};         // rad/s – fast pivot angular speed
  double waypoint_pivot_done_threshold_ {0.08};     // rad  – pivot complete when error < this

  // Pivot runtime state (reset on setPlan / cleanup)
  bool in_waypoint_pivot_ {false};
  double pivot_target_heading_ {0.0};

  // Issue 4: pivot watchdog – abort if no progress or total time exceeded
  rclcpp::Time pivot_start_stamp_;
  double pivot_start_yaw_ {0.0};
  double pivot_last_yaw_ {0.0};
  rclcpp::Time pivot_last_yaw_progress_stamp_;
  double pivot_timeout_ {10.0};           // param (s)
  double pivot_progress_epsilon_ {0.05};  // param (rad) – min yaw step to count as progress
  double pivot_progress_timeout_ {3.0};   // param (s)  – abort if no progress for this long

  // Issue 20: junction consumed latch – prevent immediate re-pivot at same junction
  double pivot_consumed_waypoint_progress_ {-1.0};  // -1 = none consumed
  double pivot_departure_distance_ {0.30};           // param (m) – move this far before re-arming

  // Issue 21: hysteresis state for multiple rotation authorities
  bool in_no_arc_rotate_mode_ {false};
  bool in_rotate_to_heading_mode_ {false};
  bool in_rpp_rotate_mode_ {false};
  bool strict_line_path_captured_ {false};
  double no_arc_heading_exit_threshold_ {0.03};        // param (rad)
  double rotate_to_heading_exit_threshold_ {0.55};     // param (rad)
  double rpp_rotate_to_heading_exit_angle_ {0.45};     // param (rad)

  // Issue 22: hard ceiling for adaptive lateral limit widening
  double strict_line_adaptive_max_limit_ {0.20};  // param (m)

  // Issue 23/25: separate speed floors for open tracking vs waypoint capture
  double min_capture_speed_ {0.0};   // param (m/s) – allowed floor near waypoint
  double pivot_entry_speed_ {0.03};  // param (m/s) – approach speed at exact junction

  // Issue 5/34: allow degenerate segment fallback for dev convenience (not production)
  bool allow_degenerate_segment_fallback_ {true};  // param

  // Issue 13: path drop tolerance
  double path_max_drop_fraction_ {0.20};  // param – fail if > this fraction of points dropped

  // Issue 33: control point offset from base_link (for non-centred UGVs)
  double control_point_offset_x_ {0.0};  // param (m) – forward offset
  double control_point_offset_y_ {0.0};  // param (m) – lateral offset

  // ── Velocity profile: anticipatory speed scheduling from path curvature ──
  bool speed_profile_enabled_ {true};
  double speed_profile_max_lateral_accel_ {0.8};  // m/s² – centripetal acceleration cap
  double speed_profile_min_speed_ {0.15};         // m/s  – floor for profile-computed limits

  bool debug_publishers_ {false};
  rclcpp_lifecycle::LifecyclePublisher<geometry_msgs::msg::PointStamped>::SharedPtr projection_pub_;
  rclcpp_lifecycle::LifecyclePublisher<geometry_msgs::msg::PointStamped>::SharedPtr target_pub_;
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::Int32>::SharedPtr segment_index_pub_;
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::Float64>::SharedPtr lateral_error_pub_;
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::Float64>::SharedPtr distance_remaining_pub_;
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::Float64>::SharedPtr heading_error_pub_;
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::Float64>::SharedPtr cmd_v_pub_;
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::Float64>::SharedPtr cmd_w_pub_;
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::Float64>::SharedPtr final_approach_pub_;
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::Float64>::SharedPtr rotate_mode_pub_;
};

}  // namespace next_nav2_controller

#endif  // NEXT_NAV2_CONTROLLER__NEXT_NAV2_CONTROLLER_HPP_
