cmake_minimum_required(VERSION 3.8)
project(next_ros2ws_interfaces)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Find dependencies
find_package(ament_cmake REQUIRED)
find_package(rosidl_default_generators REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(action_msgs REQUIRED)

# Generate interfaces
rosidl_generate_interfaces(${PROJECT_NAME}
  "msg/Zone.msg"
  "msg/Path.msg"
  "msg/Layout.msg"
  "msg/MotionIntentStatus.msg"
  "action/GoToZone.action"
  "action/FollowPath.action"
  "srv/SetStackMode.srv"
  "srv/SaveZone.srv"
  "srv/UploadMap.srv"
  "srv/SetActiveMap.srv"
  "srv/GetActiveMap.srv"
  "srv/DeleteZone.srv"
  "srv/UpdateZoneParams.srv"
  "srv/ReorderZones.srv"
  "srv/SavePath.srv"
  "srv/DeletePath.srv"
  "srv/SaveLayout.srv"
  "srv/LoadLayout.srv"
  "srv/DeleteLayout.srv"
  "srv/SetMaxSpeed.srv"
  "srv/SetControlMode.srv"
  "srv/SetMotionIntent.srv"
  "srv/SetSafetyOverride.srv"
  "srv/GetZones.srv"
  "srv/GetPaths.srv"
  "srv/GetLayouts.srv"
  "srv/GetMapLayers.srv"
  "srv/AddMapLayerObject.srv"
  "srv/DeleteMapLayerObject.srv"
  "srv/ClearMapLayer.srv"
  "srv/StartSequence.srv"
  "srv/StopSequence.srv"
  "srv/GetSequenceStatus.srv"
  "srv/GetMissionStatus.srv"
  "srv/ResumeMission.srv"
  "srv/ClearMissionState.srv"
  "srv/GetEditorMapPreview.srv"
  "srv/OverwriteEditorMap.srv"
  "srv/SaveCurrentMap.srv"
  "srv/ReloadEditorMap.srv"
  "srv/ExportEditorMap.srv"
  "srv/GetUiMappings.srv"
  "srv/SaveUiMappings.srv"
  "srv/RequestNavigation.srv"
  "srv/CancelNavigation.srv"
  DEPENDENCIES geometry_msgs action_msgs
)

ament_export_dependencies(rosidl_default_runtime)
ament_package()
