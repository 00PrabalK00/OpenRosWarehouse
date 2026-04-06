cmake_minimum_required(VERSION 3.5)
project(ugv_bringup)

# Default to C++17 (required by nav2 / rclcpp ≥ Humble)
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)
# uncomment the following section in order to fill in
# further dependencies manually.
# find_package(<dependency> REQUIRED)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  # the following line skips the linter which checks for copyrights
  # uncomment the line when a copyright and license is not present in all source files
  #set(ament_cmake_copyright_FOUND TRUE)
  # the following line skips cpplint (only works in a git repo)
  # uncomment the line when this package is not in a git repo
  #set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()

  add_test(
    NAME ugv_bringup_source_integrity
    COMMAND
      ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/scripts/verify_my_bot_source.py
      --source-root ${CMAKE_CURRENT_SOURCE_DIR}
      --allow-missing-install
  )

  add_test(
    NAME ugv_bringup_workspace_mirror
    COMMAND
      ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/scripts/sync_workspace_mirror.py
      --ui-ws-root ${CMAKE_CURRENT_SOURCE_DIR}
      --check
      --allow-missing-mirror
  )
endif()

install(
  DIRECTORY config
  DESTINATION share/${PROJECT_NAME}
  PATTERN "active_map_config.yaml" EXCLUDE
  PATTERN "zones.yaml" EXCLUDE
  PATTERN "path_metadata.yaml" EXCLUDE
  PATTERN "robot_profiles/registry.yaml" EXCLUDE
  PATTERN "robot_profiles/bringup_reports" EXCLUDE
  PATTERN "robot_profiles/bringup_reports/*" EXCLUDE
)

install(
  DIRECTORY urdf launch maps scripts
  DESTINATION share/${PROJECT_NAME}
)

# NOTE: Runtime-generated state/config files are intentionally excluded from
# install payload so source remains canonical and robot-local runtime data
# stays writable outside version-controlled source trees.

ament_package()
