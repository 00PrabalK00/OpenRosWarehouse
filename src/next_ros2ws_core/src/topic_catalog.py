#!/usr/bin/env python3

from typing import Any, Dict

# Canonical topic/service catalog.
# This file is the single source of truth for default endpoint names.
DEFAULT_TOPICS: Dict[str, str] = {
    # Service endpoints
    'service_save_zone': '/save_zone',
    'service_delete_zone': '/delete_zone',
    'service_update_zone_params': '/update_zone_params',
    'service_reorder_zones': '/reorder_zones',
    'service_get_zones': '/get_zones',
    'service_save_path': '/save_path',
    'service_delete_path': '/delete_path',
    'service_get_paths': '/get_paths',
    'service_save_layout': '/save_layout',
    'service_load_layout': '/load_layout',
    'service_delete_layout': '/delete_layout',
    'service_get_layouts': '/get_layouts',
    'service_set_control_mode': '/set_control_mode',
    'service_controller_set_parameters': '/controller_server/set_parameters',
    'service_controller_get_parameters': '/controller_server/get_parameters',
    'service_safety_emergency_stop': 'safety/emergency_stop',
    'service_safety_override': 'safety/override',
    'service_safety_status': 'safety/status',
    'service_safety_clear_state': 'safety/clear_state',
    'service_stack_set_mode': '/stack/set_mode',
    'service_stack_status': '/stack/status',
    'service_stack_shutdown': '/stack/shutdown',
    'service_map_upload': '/map_manager/upload_map',
    'service_map_set_active': '/map_manager/set_active_map',
    'service_map_get_active': '/map_manager/get_active_map',
    'service_slam_save_map': '/slam_toolbox/save_map',
    'service_slam_serialize': '/slam_toolbox/serialize_map',
    'service_slam_deserialize': '/slam_toolbox/deserialize_map',
    'service_map_server_load_map': '/map_server/load_map',
    'service_clear_global_costmap': '/global_costmap/clear_entirely_global_costmap',
    'service_clear_local_costmap': '/local_costmap/clear_entirely_local_costmap',
    'service_auto_relocate': '/auto_relocate',
    'service_map_layers_get': '/map_layers/get',
    'service_map_layers_add': '/map_layers/add',
    'service_map_layers_delete': '/map_layers/delete',
    'service_map_layers_clear': '/map_layers/clear',
    'service_mission_start_sequence': '/mission/start_sequence',
    'service_mission_stop_sequence': '/mission/stop_sequence',
    'service_mission_sequence_status': '/mission/sequence_status',
    'service_mission_status': '/mission/status',
    'service_mission_resume': '/mission/resume',
    'service_mission_clear': '/mission/clear',
    'service_editor_preview': '/map_editor/preview',
    'service_editor_overwrite': '/map_editor/overwrite',
    'service_editor_save_current': '/map_editor/save_current',
    'service_editor_reload': '/map_editor/reload',
    'service_editor_export': '/map_editor/export',
    'service_settings_get_mappings': '/settings/get_mappings',
    'service_settings_save_mappings': '/settings/save_mappings',
    'service_arbitrator_request_goal': '/arbitrator/request_goal',
    'service_arbitrator_cancel_goal': '/arbitrator/cancel_goal',
    'service_navigate_to_goal_pose': '/navigate_to_goal_pose',
    # Publishers
    'publisher_goal_pose': '/goal_pose',
    'publisher_initial_pose': '/initialpose',
    'publisher_cmd_vel_manual': '/cmd_vel_joy',
    # Action endpoints
    'action_go_to_zone': '/go_to_zone',
    'action_follow_path': '/zone_follow_path',
    # Subscriptions
    'subscription_map': '/map',
    'subscription_plan': '/plan',
    'subscription_global_plan': '/global_plan',
    'subscription_control_mode': '/control_mode',
    'subscription_set_estop': '/set_estop',
    'subscription_safety_obstacle_stop': '/safety/obstacle_stop_active',
    'subscription_amcl_pose': '/amcl_pose',
    # slam_toolbox publishes its live pose on /pose in this stack.
    'subscription_slam_pose': '/pose',
    'subscription_pose_fallback': '/pose',
    'subscription_odom_filtered': '/odometry/filtered',
    'subscription_odom': '/odometry/filtered',
    'subscription_scan_overlay_primary': '/scan_combined_cloud',
    'subscription_scan_overlay_fallback': '/scan',
    'subscription_scan_union_cloud': '/scan_combined_cloud',
    'subscription_scan_combined': '/scan_combined',
    # UI/runtime helper defaults
    'diagnostic_scan_front': '/scan',
    'diagnostic_scan_rear': '/scan2',
    'ui_speed_primary': '/odometry/filtered',
    'ui_speed_fallback_cmd_vel': '/cmd_vel',
    'ui_speed_fallback_cmd_vel_nav': '/cmd_vel_nav',
    'ui_speed_fallback_wheel_cmd_vel': '/wheel_controller/cmd_vel_unstamped',
    # SAFETY: UI rosbridge publishes to mux input, not directly to hardware.
    # safety_controller remains the sole writer on /wheel_controller/cmd_vel_unstamped.
    'ui_manual_cmd_vel_publish': '/cmd_vel_joy',
    'ui_rosbridge_url': '',
    'ui_rosbridge_auto_reconnect': 'true',
    'ui_rosbridge_map_topic': '/map',
    'ui_rosbridge_map_type': 'nav_msgs/msg/OccupancyGrid',
    'ui_rosbridge_map_compression': 'none',
    'ui_lift_cmd_topic': '/lift/cmd_rpm',
    'ui_lift_status_topic': '/lift/status',
    'ui_action_mapping_prefix': 'action_node_',
    'ui_diagnostics_topic': '/diagnostics',
    'ui_robot_mode_topic': '/robot_mode',
    'ui_battery_state_topic': '/battery_state',
    'ui_battery_temperature_topic': '/battery_temperature',
    'ui_battery_charge_cycles_topic': '/battery_charge_cycles',
}


def sanitize_topic_overrides(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}

    cleaned: Dict[str, str] = {}
    for key, value in raw.items():
        topic_key = str(key or '').strip()
        if not topic_key:
            continue
        cleaned[topic_key] = str(value or '').strip()
    return cleaned


def default_topics() -> Dict[str, str]:
    return dict(DEFAULT_TOPICS)


def merge_topic_overrides(raw: Any) -> Dict[str, str]:
    merged = default_topics()
    merged.update(sanitize_topic_overrides(raw))
    return merged


def extract_topic_overrides(raw: Any) -> Dict[str, str]:
    cleaned = sanitize_topic_overrides(raw)
    overrides: Dict[str, str] = {}
    for key, value in cleaned.items():
        if key not in DEFAULT_TOPICS or DEFAULT_TOPICS.get(key) != value:
            overrides[key] = value
    return overrides
