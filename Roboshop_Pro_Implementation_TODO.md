# Roboshop Pro Implementation TODO

Source spec: `/home/next/testBuild/Roboshop_Pro_Feature_Spec_Analysis.md`

Read status: full 3,225-line spec reviewed. The first Roboshop analysis block is duplicated once in the file, then the concrete "Robot Model and Device Configuration Workspace" implementation spec starts at line 2413. This TODO tracks the whole product scope, not only Device Config.

Status legend:

- `IMPLEMENT` means missing or incomplete and should be built.
- `VERIFY` means the repository appears to already have a related implementation and needs code/runtime verification.
- `PARTIAL` means something exists but does not meet the spec fully.
- `BLOCKED` means completion requires robot hardware, unavailable robot-side APIs, external manuals, credentials, or deployment infrastructure.
- `DONE` means implemented in this branch and covered by at least focused code/syntax checks.
Do not name anything with robotshop word

## Current Pass Progress

- `DONE` Shared Roboshop backend operations for role permissions, structured events, model validation, generated bundle previews, deterministic checksums and network health probing.
- `DONE` Server-side permission gates for robot profile create/clone/delete/save and deployment push/rollback routes, with permissive `admin` default unless role is overridden.
- `DONE` New APIs: `/api/roboshop/permissions`, `/api/roboshop/events`, `/api/roboshop/network_health`, `/api/roboshop/model/validate`, `/api/settings/robot_profiles/generated_bundle`.
- `DONE` Robot-builder metadata preservation for enabled state, topic, frame, driver, range, FOV, safety behavior and advanced metadata.
- `DONE` Device Config backend validation, publish permission gating, generated bundle preview, all required tree categories, add/duplicate/delete/enable-disable flows, and editable object inspector.
- `DONE` Focused tests for permission payloads, structured validation and deterministic generated bundles.
- `DONE` Generated deployment bundle now contains real deterministic file contents: URDF, xacro, static transforms, driver params, Nav2 geometry, safety controller config, validation report and manifest.
- `DONE` Device Config generated bundle panel previews and downloads generated files using the existing inspector styling.
- `DONE` Device Config structured event log panel is wired to `/api/roboshop/events`.
- `DONE` Backend deployment push now re-validates the saved model server-side before robot push, logs blocked pushes, and returns the validation report.
- `DONE` Focused Flask route tests cover permissions, validation, generated bundle content, deployment push validation blocking and structured events.

## Current Verification Notes

- `PASS` `python3 -m py_compile src/next_ros2ws_web/src/roboshop_ops.py src/next_ros2ws_web/src/zone_web_ui.py src/next_ros2ws_web/src/ros_bridge.py`
- `PASS` Device Config extracted JavaScript with `node --check -`
- `PASS` `python3 -m pytest src/next_ros2ws_web/test/test_roboshop_ops.py src/next_ros2ws_web/test/test_roboshop_routes.py`
- `PASS` focused `ament_flake8` for `roboshop_ops.py`, `test_roboshop_ops.py` and `test_roboshop_routes.py`
- `PASS` focused `ament_pep257` for `roboshop_ops.py`
- `PASS` `colcon build --symlink-install --packages-select next_ros2ws_web`
- `FAIL` `python3 -m pytest src/next_ros2ws_web/test` still fails in repo-wide flake8/pep257 baseline checks. The new Roboshop tests pass; failures are pre-existing style/docstring debt across root scripts, core modules, tools and existing web modules.
- `WARN` `gitnexus_detect_changes(scope=all)` reports critical risk because `zone_web_ui.py` is a central Flask route file and the graph maps line shifts to many routes. Focused impact checks for `zone_web_ui.py`, `device_config.html` and `push_robot_profile_deployment` were LOW before edits.

## A. Frontend Product Areas

| Spec Area | Status | Concrete TODO |
|---|---:|---|
| Home page robot discovery and connection | VERIFY | Verify robot discovery, manual IP entry, connection state, robot name, IP/host, software version, active map, active model, battery, mode, localization confidence, and alarms are visible and backed by real runtime data. |
| Connection failure UX | IMPLEMENT | Add clear failure reasons for network timeout, rejected robot API, wrong robot version, unavailable daemon, and stale backend state. |
| Account and permission UI | PARTIAL | Backend roles and Device Config push gating implemented. Still verify all other pages/actions and add full user management UI. |
| Shortcut key support | IMPLEMENT | Add a discoverable shortcut help panel and implement safe shortcuts for save, validate, search, cancel, zoom, pan, map tools, and emergency-safe actions. |
| Software exception handling | PARTIAL | Structured event API/log and Device Config event panel implemented. Still route more non-model backend failures into the event stream. |
| Robot Model / Model File page | PARTIAL | Backend validation, permissions, categories, add/delete/duplicate/enable-disable, editable inspector and generated file preview/download implemented. Still add drag editing, compare/merge and full rollback UI. |
| Map lifecycle UI | VERIFY | Verify create, open, upload, export, activate, repair, save, and map mismatch warnings. Ensure map edits cannot silently destroy dotted/map metadata. |
| Map object editor | VERIFY | Verify normal points, normal lines, advanced points, advanced lines, forbidden lines, virtual lines, curves, areas, patrol routes, AP, CP, ReturnPoint, and RobotHome are represented and persisted. |
| SLAM map building UI | VERIFY | Verify start/stop SLAM, live map preview, save map, map naming, map validation, and post-SLAM localization flow. |
| Localization UI | VERIFY | Verify initial pose, confirm localization, relocalization, confidence display, AMCL/scan/IMU/encoder status, and map compatibility warnings. |
| Navigation UI | VERIFY | Verify global planning, local planning, path navigation, free navigation, fixed path navigation, blocked-state behavior, cancel, retry, and safe stop. |
| Task and action planning UI | VERIFY | Verify task API actions, action sequencing, mission execution state, waiting for hardware actions, and separation between direct control and task commands. |
| AP workstation editor | VERIFY | Verify AP data fields, angle, action binding, recognition options, shelf/fork interaction fields, and validation before use. |
| CP charging point editor | VERIFY | Verify CP point fields, docking geometry, charger binding, charge duration, handshake IO, and safe charge workflow. |
| Recognition file management UI | PARTIAL | Verify template list/import/export/edit. Add complete reflector/shape parameters, recognition mode options, action binding, and diagnostics. |
| Safety zones UI | PARTIAL | Verify slowdown/stop/critical zones, detection radius, manual mode blocking, obstacle memory, docking override behavior, laser protection areas, and runtime overlays. |
| Laser limitation guidance | IMPLEMENT | Add validation/warnings for valid distance, reflectivity, black/specular objects, laser interference, strong light, and environment change risks. |
| Parameter hierarchy UI | PARTIAL | Show robot defaults, map overrides, route/action overrides, effective value, source of value, and conflict resolution. |
| Robot status dashboard | VERIFY | Verify battery, connection, mode, speed, localization, task state, alarms, map, stack status, confidence, and network health. |
| Running mode management | VERIFY | Verify automatic/manual/remote modes, safe joystick behavior, mode transitions, and blocked commands per mode. |
| Fork/jack/load UI | VERIFY | Verify lift/fork/jack commands, load/unload actions, interlocks, sensor confirmation, and task wait behavior. |
| Dispatch/RDS scenario UI | IMPLEMENT | Add or verify scenario object editing and RDS/dispatcher relationship if this product is expected to replace Roboshop dispatch setup. |
| Log analysis UI | PARTIAL | Add robot/backend/ROS/UI log collection, filtering, timestamp correlation, error-code grouping, export, and troubleshooting hints. |
| Push/Pull UI | PARTIAL | Implement full compare/merge/diff UI, backup snapshot visibility, staged deployment status, verification status, and rollback selection. |

## B. Backend And API

| Spec Area | Status | Concrete TODO |
|---|---:|---|
| Robot discovery API | VERIFY | Identify current discovery/connect APIs and verify they expose robot name, IP, version, ports, status, and error reasons. |
| Account permission backend | IMPLEMENT | Add persistent users/roles/permissions if absent. Enforce permissions server-side, not only by hiding buttons. |
| Shortcut preferences | IMPLEMENT | Persist user shortcut preferences and allow defaults reset. |
| Exception/event log API | IMPLEMENT | Add structured event records with severity, source, object, field, reason, required action, timestamp, and correlation id. |
| Robot profile storage | VERIFY | Verify DB and YAML storage stay in sync, schema version is persisted, profile version increments correctly, and corrupt profile recovery is safe. |
| Robot model schema | PARTIAL | Extend schema to include metadata, identity, chassis, devices, transforms, collision, safety zones, driver bindings, runtime interfaces, navigation outputs, generated file records, validation records, deployment history, compatibility metadata, and visual editor state. |
| Schema migration | IMPLEMENT | Add migration path for old profile/model schemas and compatibility warnings. |
| Model open/save/save-as | PARTIAL | Add backend support for save-as with new identity, version root, optional deployment identity stripping, and conflict checks. |
| Import pipeline | PARTIAL | Expand import beyond URDF to xacro, YAML, JSON, vendor model bundles, and store unmapped fields in advanced metadata. |
| Export pipeline | PARTIAL | Generated bundle now includes profile JSON, robot-builder JSON, URDF, xacro, static transforms, ROS-style driver YAML, Nav2 YAML, safety YAML, validation report and manifest. Still add diagnostics config and visualization layout export. |
| Generated output determinism | DONE | Deterministic generation tests cover same model input producing stable checksums plus stable URDF/xacro file contents. |
| Validation API | PARTIAL | Add strict field/object/system validation API returning grouped critical errors, warnings, suggestions, affected object, affected field, why it matters, and required action. |
| Deployment API | PARTIAL | Push route now validates saved model server-side before robot push and logs blocked pushes. Still add compare, stage/apply, service restart verification and robot-side apply verification. |
| Rollback API | VERIFY | Verify rollback restores previous active model, generated files, manifest, and robot-side state safely. |
| Map lifecycle backend | VERIFY | Verify map create/upload/download/activate/repair preserves YAML/image/dotted overlays and detects mismatch. |
| Map object backend | VERIFY | Verify all map objects are stored with type-specific fields and round-trip through UI/API. |
| SLAM backend | VERIFY | Verify SLAM start/stop/save APIs, map output management, and failure reporting. |
| Localization backend | VERIFY | Verify initial pose, confirm localization, relocalization, confidence, and map compatibility APIs. |
| Navigation backend | VERIFY | Verify path/free/fixed navigation APIs, cancel, status, blocked behavior, and route validation. |
| Task API backend | VERIFY | Verify task/action APIs for create/start/cancel/status and hardware action wait semantics. |
| Recognition backend | PARTIAL | Verify template CRUD/import/export, line/shape/reflector parameter persistence, runtime detector status, and API error messages. |
| Safety backend | PARTIAL | Verify safety controller zones, dynamic docking safety behavior, manual mode blocking, obstacle memory reset rules, and laser grouping/protection area APIs. |
| Robot control API layer | VERIFY | Verify control operations are separated from task operations and blocked in unsafe states. |
| Robot task API layer | VERIFY | Verify task operations have state, result, timeout, and recovery behavior. |
| Robot configuration API layer | PARTIAL | Verify config operations have validation, backups, diffs, and permission checks. |
| API port model | BLOCKED | External SEER daemon port behavior can only be mirrored/validated with robot-side APIs or exact manuals. |
| Network health API | PARTIAL | Port reachability and latency probe implemented. Packet loss, link type and discovery broadcast status still need robot/network integration. |

## C. ROS Integration

| Spec Area | Status | Concrete TODO |
|---|---:|---|
| Topic discovery | VERIFY | Verify ROS 2 topic catalog includes type, publishers/subscribers, frequency, health, and maps to profile fields. |
| TF tree viewer | PARTIAL | Verify transform availability and add duplicate/missing/unreachable frame validation from live TF. |
| URDF generation | PARTIAL | Verify generated URDF includes base link, chassis visual/collision, wheels, sensors, frames, joints, materials, and optional meshes. |
| Xacro generation | DONE | Generated bundle includes deterministic `robot_model.xacro` from the visual model. |
| Static transform output | DONE | Generated bundle includes static transform records from links and shape child frames. |
| Nav2 footprint generation | PARTIAL | Verify footprint export and add inflation suggestion, base frame, odom frame, obstacle/voxel source suggestions, speed limits, docking/shelf approach zones. |
| Driver parameter generation | PARTIAL | Generated bundle emits ROS-style driver parameter YAML from enabled model objects. Category-specific driver schemas and diagnostics config still need deeper per-device mapping. |
| Safety controller generation | PARTIAL | Generate safety controller zones, laser protection areas, docking overrides, slowdown/stop behavior, and recovery behavior from model schema. |
| Runtime overlays | PARTIAL | Add read-only runtime overlays for device online status, driver heartbeat, topic availability, TF availability, diagnostics, active model version. |
| Test mode | IMPLEMENT | Add permission-controlled test mode for IO, indicators, motors, drivers, and hardware actions. |
| Fork/jack/lift action integration | VERIFY | Verify action nodes and mission manager wait for sensor confirmation and enforce interlocks. |
| Shelf recognition integration | PARTIAL | Verify templates feed recognition geometry and docking solver; add UI validation of template vs solve quality. |
| Safety during docking | PARTIAL | Verify dynamic safety area behavior around shelf pickup/dropoff and ensure no unsafe recovery motion near shelf. |

## D. Configuration And Storage

| Spec Area | Status | Concrete TODO |
|---|---:|---|
| Robot profile metadata | PARTIAL | Store name, model, type, serial, manufacturer, software version, ROS domain, drive type, dimensions, map compatibility, supported devices, deployment target. |
| Device object storage | IMPLEMENT | Store all required object categories with enabled state, parameters, transforms, runtime interfaces, driver bindings, validation records, and advanced metadata. |
| Required device categories | PARTIAL | All categories are represented in Device Config with add flow. Type-specific schemas for every category still need deeper validation/generation. |
| Visual editor state | PARTIAL | Store collapsed tree state, canvas view, labels/layers/grid preferences separately from robot configuration state. |
| Version history | PARTIAL | Store version name, number, author, created/edited time, validation result, target, status, change summary, checksum, rollback reference. |
| Deployment manifest | PARTIAL | Store generated file list, checksums, schema version, validation result, backup reference, status, robot target, and verification result. |
| Audit trail | IMPLEMENT | Record who changed what, when, source/destination, before/after snapshot, and robot target for push/pull/rollback/config edits. |
| Local vs robot snapshots | IMPLEMENT | Store robot-side pulled snapshot before any merge/edit and local draft snapshots before push. |
| Parameter hierarchy | PARTIAL | Persist robot defaults, map overrides, route/action overrides, effective value source, and conflict state. |
| Recognition templates | VERIFY | Verify template storage includes shelf/pallet/cage geometry, reflector/shape method, opening/width/depth, tolerances, and action binding. |
| Map files | VERIFY | Verify YAML/image/dotted overlays/metadata remain consistent during edit/export/import. |

## E. Validation Checklist

| Validation Type | Status | Concrete TODO |
|---|---:|---|
| Field validation | PARTIAL | Validate names, required fields, numeric ranges, units, ROS topic names, frame names, IP/port, communication settings, selected driver compatibility. |
| Object validation | PARTIAL | Validate laser frame/topic/pose/driver, battery driver/timeout, chassis dimensions/base frame, collision polygon/primitive, camera stream/frame, charger communication, controller output/arbitration. |
| System validation | PARTIAL | Validate one primary chassis, one base frame, connected TF tree, no duplicate child frames, no conflicting command topics, footprint covers body, Nav2 footprint valid, safety zones linked to sensors, required devices enabled, generated files consistent, target compatibility. |
| Map validation | VERIFY | Validate map files, map-object references, forbidden/virtual lines, AP/CP object dependencies, and map mismatch state. |
| Navigation validation | VERIFY | Validate route endpoints, blocked path behavior, route constraints, localization confidence before navigation. |
| Recognition validation | PARTIAL | Validate template dimensions, shelf geometry, reflector/shape requirements, detector confidence, docking solve vs template consistency. |
| Safety validation | PARTIAL | Validate zones, laser coverage, overlap conflicts, docking overrides, manual mode blocking, obstacle memory reset. |
| Deployment validation | PARTIAL | Validate safe robot state, no mission running, no critical fault, stable connection, role permission, model schema compatibility, robot software compatibility. |
| Error message quality | PARTIAL | Structured model validation now includes object, field, why it matters and action required. Existing non-model routes still need conversion. |

## F. Testing And Verification

| Test Area | Status | Concrete TODO |
|---|---:|---|
| Unit tests | PARTIAL | Added focused Roboshop ops tests. Existing suite still has unrelated docking/lint debt; add API route and save/load tests next. |
| Frontend syntax tests | VERIFY | Keep JS syntax checks for large HTML templates. |
| API route tests | PARTIAL | Added focused Flask tests for Roboshop permission, validation, generated bundle, deployment push blocking and event routes. Profile CRUD, rollback, URDF upload/source, recognition and safety routes still need coverage. |
| Deterministic file generation tests | DONE | Assert generated bundle checksum and generated URDF/xacro contents are stable for identical input. |
| Migration tests | IMPLEMENT | Add old-schema profile fixtures and migration assertions. |
| Permission tests | PARTIAL | Verified server rejects deployment push for viewer role. Rollback/test-mode/import/export denial tests still need coverage. |
| Hardware-blocked tests | BLOCKED | Real robot pull/push verification, daemon ports, service restart verification, live TF/device heartbeat, charger/fork/jack hardware confirmation require robot or simulator. |
| Build checks | VERIFY | Run targeted `colcon build --symlink-install --packages-select ...` after changes. |
| Lint/docstyle | PARTIAL | Existing repo has large flake8/pep257 debt. Separate baseline cleanup from feature implementation to avoid mixing behavior changes with style-only churn. |

## G. Documentation And Operations

| Area | Status | Concrete TODO |
|---|---:|---|
| Operator workflow docs | IMPLEMENT | Document connect, create/open model, import, edit, validate, push, rollback, map lifecycle, recognition template, safety zone workflow. |
| Engineering docs | IMPLEMENT | Document model schema, generated files, deployment package structure, validation codes, permissions, API contracts. |
| Manual verification plans | IMPLEMENT | Add manual test plans for robot-connected pull/push, safety zones, docking, lift/fork/jack, charger, map/SLAM/localization. |
| Troubleshooting docs | IMPLEMENT | Add failure modes for connection, map mismatch, localization confidence, recognition failure, blocked navigation, push failure, rollback failure. |
| Safety notes | IMPLEMENT | Document when not to push, how to verify robot safe state, warning override policy, docking safety override rules, and hardware interlock expectations. |

## H. Current Immediate Implementation Queue

1. Finish backend robot model schema persistence so model objects retain enabled state, topic, driver, runtime, safety and advanced metadata through save/load.
2. Make Device Config inspector truly editable for model metadata, chassis, transforms, devices, topics, drivers, collision and safety fields.
3. Add all required device categories to the tree with configured/enabled/supported counts and validation state.
4. Add add/duplicate/delete/enable/disable object flows with dependency checks.
5. Add generated file preview panels for URDF, Nav2 footprint/config, safety controller config, driver params and deployment manifest.
6. Add version history compare and rollback UI backed by existing deployment APIs.
7. Add stricter validation objects with affected object/field/action metadata instead of plain strings.
8. Audit broader product areas from sections 1-35 and mark each one verified/partial/blocked against real routes and runtime behavior.
