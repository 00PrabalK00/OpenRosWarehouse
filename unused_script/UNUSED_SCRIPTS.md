# Unused Scripts Moved From ROS2 Packages

This folder holds package Python scripts that were moved out of `src/` after checking Graphify and repository references.

Check method:

1. Ran Graphify queries for ROS2 scripts, entry points, imports, calls, and candidate files.
2. Checked every package `setup.py` `console_scripts` entry.
3. Checked launch files for `Node(... executable=...)` references.
4. Checked package CMake install/test rules.
5. Ran exact text searches across `src/`, `wiki/`, `AGENTS.md`, `CLAUDE.md`, and `test.sh`, excluding generated folders.

Files moved:

| New path | Original path | Why considered unused |
| --- | --- | --- |
| `unused_script/next_ros2ws_web/src/mode_ops.py` | `src/next_ros2ws_web/src/mode_ops.py` | Not in `next_ros2ws_web/setup.py` console scripts, not launched, not imported by `zone_web_ui.py` or other code. Only documentation listed it as a helper file. |
| `unused_script/next_ros2ws_web/src/safety_ops.py` | `src/next_ros2ws_web/src/safety_ops.py` | Not in console scripts, not launched, not imported by runtime code or tests. Only documentation listed it as a helper file. |
| `unused_script/next_ros2ws_web/src/zone_ops.py` | `src/next_ros2ws_web/src/zone_ops.py` | Not in console scripts, not launched, not imported by runtime code or tests. Only documentation listed it as a helper file. |
| `unused_script/next_ros2ws_reflector/src/kinematic_sim_driver.py` | `src/next_ros2ws_reflector/src/kinematic_sim_driver.py` | Not in `next_ros2ws_reflector/setup.py` console scripts and not launched by `reflector_localization.launch.py`. Text references only appear inside its own file and as an unused parameter block in `config/reflector_localizer.yaml`. |
| `unused_script/next_ros2ws_reflector/src/reflector_control_ui.py` | `src/next_ros2ws_reflector/src/reflector_control_ui.py` | Not in console scripts and not launched. Text references only appear inside its own file and as an unused parameter block in `config/reflector_localizer.yaml`. |
| `unused_script/next_ros2ws_reflector/src/reflector_map_exporter.py` | `src/next_ros2ws_reflector/src/reflector_map_exporter.py` | Graphify found no incoming calls/imports/references, and text search found no repo references outside the file path itself. Not installed as a console script and not launched. |
| `unused_script/next_ros2ws_reflector/src/test_route_driver.py` | `src/next_ros2ws_reflector/src/test_route_driver.py` | Not in console scripts and not launched. Text references only appear inside its own file and as an unused parameter block in `config/reflector_localizer.yaml`. |

Files checked but left in place:

| Path | Reason kept |
| --- | --- |
| `src/next_ros2ws_core/scripts/verify_db.py` | Referenced by wiki manual commands, so it may be used manually. |
| `src/ui_ws/scripts/nav2_launch_wrapper.sh` | Called by `map_manager.py`, `map_editor_manager.py`, `ros_bridge.py`, `stack_manager.py`, and installed by `ui_ws/CMakeLists.txt`. |
| `src/ui_ws/scripts/sync_workspace_mirror.py` | Registered as a CTest in `ui_ws/CMakeLists.txt` and referenced in wiki manual commands. |
| `src/ui_ws/scripts/verify_my_bot_source.py` | Registered as a CTest in `ui_ws/CMakeLists.txt` and referenced in wiki manual commands. |
