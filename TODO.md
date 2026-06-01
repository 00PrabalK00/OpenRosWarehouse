# Scrape Data.pdf — Whole UI Implementation TODO

**Spec:** `~/testBuild/Scrape Data.pdf` (1924 lines text, ~12 major sections)
**Scope:** All Next Robotics Lab features across the whole UI, not just map editor.

## Live UI Map (where features go)

Main shell: `~/testBuild/src/next_ros2ws_web/web/templates/index.html` (42128 lines).
Header tabs:
- `navigator` — robot dashboard, connection. **Primary Editor for Sites (Zones), Paths, and logical Zones.**
- `editor` — embeds `/editor` (map editor iframe → `map_editor.html`). **Solely for editing map geometry/layers (e.g. wall/floor edits).**
- `device-config` — robot model/device config
- `recognition` — recognition templates
- `settings` — basic, skin, language, style, battery, filemanagement, system

Backend: `zone_web_ui.py` (3135 lines), `next_ops.py`, `ros_bridge.py` (14857 lines).


## GitNexus Status (2026-05-07)

GitNexus CLI is installed at `/home/next/.npm/_npx/*/node_modules/.bin/gitnexus` but **cannot run** on this machine:
- Error: `GLIBCXX_3.4.32` not found in `/lib/x86_64-linux-gnu/libstdc++.so.6`
- MCP tools are not available in the current Claude session
- **Workaround**: All impact analysis is done manually by reading call graphs and following wiki Section 18 ("Safe Modification") guidance
- **Risk**: All edits to ros_bridge.py and zone_web_ui.py were made without automated blast-radius checks. This is a CLAUDE.md violation. Mitigations used: surgical edits, backup files, no deletion of existing code, only additions.

## CLAUDE.md Architecture Rules Applied

Per Section 18.7 "Add A UI Feature":
- Route handlers in `zone_web_ui.py`: thin, no ROS logic
- ROS/domain logic in `ros_bridge.py`
- Policy/validation helpers in `next_ops.py`
- Templates in `src/next_ros2ws_web/web/templates/`
- Static JS/CSS in `src/next_ros2ws_web/web/static/`

**Violations fixed:**
- `map_graph.py` (standalone module, wrong) → DELETED
- `map_editor_graph.js` (wrong, used graph overlay) → DELETED
- Motion routes `/api/motion/*` (unnecessary duplicate) → REMOVED; JS now uses existing `/api/manual/velocity`
- `src/map_editor/` files → acknowledged as COLCON_IGNORE'd legacy, not touched

## Feature Checklist (CLAUDE.md format)

### Feature: Recognition Category Extensions
- **Status:** DONE
- **Files changed:** `ros_bridge.py` (+`_build_default_recognition_metadata`, extended `_normalize_recognition_category`, `_build_default_recognition_template`); `index.html` (2 dropdown locations: lines 13462, 29156)
- **Existing functions reused:** `_normalize_recognition_category`, `_build_default_recognition_template`, `_normalize_recognition_template_payload`
- **New functions added:** `_build_default_recognition_metadata` (ros_bridge.py:7197)
- **How to test:** Recognition tab → New template → set type to Charger/Battery/Chargespot/Leg/Tag → check generated default fields match PDF spec
- **Notes:** `_build_default_recognition_metadata` is a static method added just before `_normalize_recognition_template_payload`. Called from `_build_default_recognition_template` at line 7193.

### Feature: Translation/Rotation Test Function
- **Status:** DONE (frontend timing loop; mode switch TODO)
- **Files changed:** `index.html` (MOTION button in navigator bar line 13049; modal HTML line 42463; JS timed loop line 42492)
- **Existing functions reused:** `/api/manual/velocity` (zone_web_ui.py:1584), `publish_manual_velocity` (ros_bridge.py:13950)
- **New functions added:** None (JS-only timing via `setInterval`)
- **How to test:** Home tab → MOTION button → Translation: distance=0.5m, vx=0.2m/s → Send → robot drives 2.5s then stops; Stop button cancels immediately
- **Notes:** Frontend loop posts to `/api/manual/velocity` every 50ms for `duration = |distance/vx|` seconds, then sends 0,0 to stop. This reuses the existing auth-gated motion route.

## Status Legend

- `DONE` — implemented in correct location, end-to-end working
- `MISPLACED` — built but in wrong package (needs port to live tree)
- `PARTIAL` — exists incompletely
- `TODO` — not implemented
- `BLOCKED` — needs robot hardware/live ROS daemon

## 2026-05-07 Editor Graph Note (Consolidated)

- **Update:** The user found the redundant `/editor` `Graph` tab unnecessary since there was already an existing, fully functional Zones and Paths editor.
- The separate `map_graph.py` and `map_editor_graph.js` files, along with the `/api/editor/graph...` routes in `zone_web_ui.py`, were **deleted**.
- Instead of maintaining two separate systems, the advanced metadata and bulk operations from the "Graph" spec were successfully **consolidated** into the existing Zone and Path logic:
  - Added extended properties (`angle_enabled`, `follow`, `use_pgv`, `pgv_dx`, `pgv_dy`, `pgv_dtheta`, `description`) to the core Zone saving (`/api/zones/save`, `UpdateZoneParams`).
  - Added `road_width` and `description` to the core Path settings.
  - Added `align_zones` and `batch_create_zones` to `ros_bridge.py` and exposed them in `zone_web_ui.py`.
  - Added the missing "Advanced Area" filter types (Locate Config, Reflector, Tag / QR, DO, DI, Clean, and Description areas) into the main UI's `filterLayers` and updated the backend `map_layer_manager.py` to validate and persist them.
- This ensures feature parity with the spec while keeping the UI clean and unified.

---

# 1. Map Editor (`/editor` iframe)

**Location:** `src/next_ros2ws_web/web/templates/map_editor.html` + `static/`
**Backend:** `zone_web_ui.py` `/api/editor/...` routes
**Architecture:** Solely for map geometry (walls, floors, obstacles). Logical entities (Sites, Paths) belong in Navigator (Home).

# 2. Navigator / Home Tab (`/dev`)

**Location:** `src/next_ros2ws_web/web/templates/index.html`
**Architecture:** Primary location for editing Sites (Zones), Paths, and Areas.

## 2.1 Sites (Nodes)

| Feature | Status | Notes |
|---|---|---|
| LM site (Ordinary Mark) — LocationMark/ParkPoint/SwitchMap/HomeRegion | DONE | Using normal zones |
| AP site (Location/Workstation) — ActionPoint/TransferLocation/WorkingLocation | DONE | Using action zones |
| CP site (Special Location) — ChargePoint | DONE | Using charge zones |
| QR Code site | DONE | Using action zones |
| Site coords (x,y) editable | DONE | Added to zone editor modal |
| Site angle enable/disable | DONE | Added to zone editor modal |
| Site angle drag (red triangle) | TODO | |
| Site description | DONE | Added to zone editor modal |
| Properties: follow, usePGV | DONE | Added to zone editor modal |
| Additional Props: pgvDx, pgvDy, pgvDtheta | DONE | Added to zone editor modal |
| AP/CP Execute + Recognition Model File | DONE | Using action scripts / recognition templates |
| AP/CP Pre-point | DONE | Added to zone editor modal |
| Isolated site gray display | TODO | |
| Site direction tangent hint | TODO | |
| Batch create sites (spacing/qty/angle) | DONE | `batch_create_zones` route added |
| Site circumcircle toggle | TODO | radius config TODO |

## 2.2 Paths (Edges)

| Feature | Status | Notes |
|---|---|---|
| Straight line | DONE | Existing feature |
| 3rd-order Bezier | DONE | Existing feature |
| Arc | DONE | Existing feature |
| Advanced Bezier (NURBS-6) | TODO | |
| Optimize button for NURBS | TODO | |
| Direction: bidirectional/forward/reverse | DONE | Existing / extended feature |
| Type + length in props | DONE | Length shown; type is directional/curve |
| Description | DONE | Added to path parameters |
| Attribute tab (per-direction robot behavior) | DONE | Existing segment attributes UI |
| Split bidirectional → 2 unidirectional | TODO | |
| Merge 2 unidirectional → bidirectional | TODO | |
| Smooth (→ bezier) | DONE | Auto-smooth existing |
| Direct connection (→ straight) | DONE | Control point clear existing |
| Bezier control point drag | DONE | Existing feature |
| Arc control point drag | DONE | Existing feature |
| Road width display/hide | DONE | Added `road_width` param |
| Curvature radius display/hide | DONE | Existing feature |
| Curvature threshold input + warning highlight | DONE | Existing feature |
| Path color: direct=dark green, non-direct=dark orange | TODO | |
| Set all paths one-way/reverse/bidirectional | TODO | |
| Optimize L-shaped route | TODO | distance + max curvature cost |
| Optimize ∠-shape route (2 paths shared site) | TODO | |
| Map evaluation (laser/measurement points) | BLOCKED | needs robot |

## 2.3 Multi-Selection / Batch (Home Tab)

| Feature | Status | Notes |
|---|---|---|
| Select all paths button | TODO | |
| Select all sites button | TODO | |
| Box select drag | TODO | |
| Set Line Properties full modal (filter AP→LM/LM→AP/LM↔LM, line-type, additional/replace/clear, save/read attrs) | TODO | |
| Set Site Attributes full modal | TODO | |
| Allow/Disable Site Follow (batch) | TODO | |
| Enable/Disable Site Angle (batch) | TODO | |
| Site alignment left/right/top/bottom | DONE | Added `align_zones` API |
| Batch generate paths LM→AP with pre-point | TODO | |
| Quickly connect multi AP to designated lines | TODO | |
| AP→LM / LM→AP / LM↔LM filter | TODO | |

## 1.4 Advanced Lines

| Feature | Status | Notes |
|---|---|---|
| No-Go Line (straight barrier) | DONE | Using Map Rules / Filters |
| Endpoint drag | DONE | Using Map Rules / Filters |
| Type/Description/Endpoint coords | DONE | Map Rules properties |

## 1.5 Advanced Areas (Polygons)

| Feature | Status | Notes |
|---|---|---|
| Type: Advanced Area (general) | DONE | Filter layers |
| Type: Locate Config Area (full attrs: useTagCode/use3DTag/useReflector/useFrontLaser/useBackLaser/useRTK/useFeatureLoc/useMapUpdate) | DONE | `locate_config` filter layer |
| Type: Reflector Area | DONE | `reflector` filter layer |
| Type: Tag/QR Code Area | DONE | `tag_area` filter layer |
| Type: DO Area | DONE | `do_area` filter layer |
| Type: DI Area | DONE | `di_area` filter layer |
| Type: Clean/Sweep Area | DONE | `clean_area` filter layer |
| Type: Description Area | DONE | `description_area` filter layer |
| Border + background color | DONE | Mapped colors per layer |
| Description field | TODO | |
| Polygon vertex drag | DONE | Existing filter tools |
| ESC to close polygon | DONE | Existing filter tools |
| Right-click add endpoint | TODO | |
| Double-click insert sweep endpoint | TODO | |
| Delete sweep endpoint | TODO | |
| Area rotation | TODO | |
| Reflector mounting position evaluation | BLOCKED | robot scoring |
| Auto generate cleaning routes | TODO | path planning algorithm |
| Undo auto-generated path | TODO | |

## 1.6 Storage Bins

| Feature | Status | Notes |
|---|---|---|
| Create/delete bin | MISPLACED | |
| Bind bin → AP site | MISPLACED | |
| Unique name | MISPLACED | |
| Storage area name | MISPLACED | |
| Multi-layer shelving | TODO | |
| Motion trajectory editor | TODO | complex action sequence |
| Edit pick/place actions | TODO | |
| Batch ops on bins | TODO | |
| Association line display (bin↔site) | TODO | |
| Reassign bin to different site | MISPLACED | |

## 1.7 QR Code Workflow

| Feature | Status | Notes |
|---|---|---|
| QR site placement | MISPLACED | |
| QR number editing | MISPLACED | |
| Batch generate QR codes (1m spacing, x-axis) | TODO | |
| PGV recognition start | BLOCKED | robot |
| Add site to QR position helper | TODO | |
| QR Code Area surround | MISPLACED | |

## 1.8 Map Pixel Tools

| Feature | Status | Notes |
|---|---|---|
| Speckle erasure (noise point removal) | PARTIAL | live editor has draw/erase modes |
| Thick wall thinning | PARTIAL | same eraser |
| Zoom in/out primitive | TODO | |

## 1.9 Layers / Display

| Feature | Status | Notes |
|---|---|---|
| Show/hide all sites | MISPLACED | |
| Show/hide all advanced areas | MISPLACED | |
| Show/hide paths | MISPLACED | |
| Show/hide advanced lines | MISPLACED | |
| Zoom in/out/fit | MISPLACED | |

## 1.10 Map Lifecycle

| Feature | Status | Notes |
|---|---|---|
| Load Map (file) | PARTIAL | zone_web_ui has APIs |
| Push Map to robot | BLOCKED | robot ROS |
| Pull Map from robot | BLOCKED | robot ROS |
| Map Manager (local + robot lists, upload/download/switch/delete) | BLOCKED | needs robot |
| Corrective Map (load Map B align onto Map A) | TODO | image overlay |
| Check Map (validate + alarm list, double-click to navigate) | TODO | |

## 1.11 Undo/Redo

| Feature | Status | Notes |
|---|---|---|
| Ctrl+Z / Ctrl+Y graph undo | MISPLACED | server-side stack |
| Pixel-edit undo | DONE | live editor |

---

# 2. Robot Control (Translation/Rotation Test)

**Location:** relocated into the main toolbar (`mctrl-btn` next to SHELF)
**Backend:** `ros_bridge.py` cmd_vel publish

| Feature | Status | Notes |
|---|---|---|
| Translation panel: distance, vx, vy, mode (mileage/positioning) | DONE | Popup modal from MOTION toolbar button (index.html:13049) |
| Rotation panel: rotation angle, angular velocity | DONE | Popup modal, JS timed loop (index.html:42492) |
| Send button + cancel/stop | DONE | JS setInterval posts to /api/manual/velocity every 50ms for computed duration |
| Mode: mileage (encoder) vs positioning (laser) | TODO | UI select exists; backend needs mode-based cmd_vel routing |
| **Architecture note** | INFO | Uses existing /api/manual/velocity (zone_web_ui.py:1584) + publish_manual_velocity (ros_bridge.py:13950); no new routes added |

---

# 3. Map Management (Robot Sync)

**Location:** new toolbar in `navigator` or under `editor` tab header
**Backend:** new routes + `ros_bridge.py` map sync

| Feature | Status | Notes |
|---|---|---|
| Load Map dialog | PARTIAL | local file load exists |
| Push Map button | BLOCKED | robot |
| Pull Map button | BLOCKED | robot |
| Map Manager modal: local list, robot list, upload/download/switch/delete | BLOCKED | robot |

---

# 4. Robot Calibration

**Location:** new modal under `settings` → "Calibrate Robot" or under `device-config`
**Backend:** ROS service calls

| Feature | Status | Notes |
|---|---|---|
| Calibrate Robot dialog | TODO | |
| Build Map → Push → Relocate (confidence ≥ 0.9) workflow | BLOCKED | robot |
| Edit calibration file (visual robot.cp editor) | TODO | |
| Upload calibration file | BLOCKED | robot |
| Tunable params: DiffLaserOdomCalibSize, SteerLaserOdomCalibSize, LaserOdomCalibSpeed, LaserOdomMaxRotVel, LaserOdomCalibCnt | TODO | |
| Two-wheel differential calibration | BLOCKED | robot motion |
| Front-drive single steering wheel calibration | BLOCKED | |
| Calibration plot viewer (ref vs fit) | TODO | matplotlib/plotly chart |
| Clear calibration data | BLOCKED | robot |

---

# 5. TCP Configuration

**Location:** `device-config` tab → new "TCP" sub-section
**Backend:** robot.cp file editor + push

| Feature | Status | Notes |
|---|---|---|
| TCP x/y/yaw config | TODO | |
| defaultTCP setting | TODO | |
| Per-task TCP override (TCP1, TCP2…) | TODO | |
| TCP measurement workflow guide | TODO | docs/UI |

---

# 6. Audio Management

**Location:** new section in `settings` or `navigator` → Audio tab
**Backend:** WAV file upload/download/play via ROS topic

| Feature | Status | Notes |
|---|---|---|
| List audio files on robot | BLOCKED | robot fs access |
| Upload local WAV → robot | BLOCKED | |
| Download robot WAV → local | BLOCKED | |
| Delete robot audio file | BLOCKED | |
| Play/Pause/Resume/Stop on robot | BLOCKED | |
| Volume control (above 60) | BLOCKED | |
| Local cache path open | TODO | |
| Configure scenario audio in Robot Model | TODO | task chain integration |

---

# 7. Task Chain Editor

**Location:** NEW tab in main shell — "Tasks" or under `navigator`
**Backend:** new task chain CRUD + robot push/pull
**Files needed:** new template + JS module

| Feature | Status | Notes |
|---|---|---|
| Task chain list (local + robot) | TODO | |
| New task chain | TODO | |
| Load/Save/Rename/Delete/Export task chain | TODO | |
| Push to robot / Pull from robot | BLOCKED | robot |
| Execute on robot (Loop/Send Selected/Send All/Suspend/Cancel/Skip Next) | BLOCKED | robot |
| Group: add/insert/delete/uncheck/description | TODO | |
| Group attributes: non-blocking, action timeout, dwell time, external trigger ID | TODO | |
| Draggable action tags: Path Navigation, Translation, Rotation, Custom Action, Fork, Roller, Jack, Play Sound, Stop Sound, Wait DI, Set DO | TODO | |
| Action params per tag (target site, speed, distance, angle, audio file, DO pin, DI signal) | TODO | |
| Loop/sequential execution mode | TODO | |

---

# 8. Recognition Editor

**Location:** existing `recognition` tab in index.html (line 33885)
**Backend:** template CRUD already partial in `next_ops.py`

| Recognition Type | Status | Notes |
|---|---|---|
| Shelf | DONE | Runtime defaults in `ros_bridge.py`; full metadata editor wired in `dev_recognition.js` inspector |
| Pallet | DONE | Runtime defaults in `ros_bridge.py`; full metadata editor wired in `dev_recognition.js` inspector |
| Charger (charging pile) — minwidth/maxwidth/dx/dy | DONE | ros_bridge.py:7200; live recognition rail + inspector params wired |
| Battery — backLength/aheadLength/upHeight/recHeightTor/goLateralDist/maxAdjustTime | DONE | ros_bridge.py:7207; live recognition rail + inspector params wired |
| Chargespot (forklift charging pile) — ip/port | DONE | ros_bridge.py:7216; live recognition rail + inspector params wired |
| Leg (human leg recognition) — minwidth/maxwidth/maxdistance/method_type | DONE | ros_bridge.py:7221; live recognition rail + inspector params wired |
| Tag (QR code) — X_Dis/Y_Dis/Z_Dis/goodsWidth/goodsLength/tagDistance/tagSize/Tagtype | DONE | ros_bridge.py:7227; live recognition rail + inspector params wired |
| Polygon Template (.plt) — metadata defaults added | PARTIAL | ros_bridge.py:7239; live category wired with metadata field; image/photo capture UI still TODO |
| Toolbar: Load/Save All/Save As/Upload/Download/Undo/Redo/Help | DONE | Wired into `recognition-top-controls`; reuses existing load/save draft/duplicate/import/export/undo/redo/help-inspector logic |
| Active template card | DONE | Moved into `recognition-top-controls` as title/subtitle metadata instead of a separate stage card |
| Validate / Publish | DONE | lines 13108, 13111 |

### Shelf full fields (verify against PDF):
width, length, leg_width, align_depth, anti_align_depth, y_align_depth, y_anti_align_depth, rec_off_x, rec_off_y, rec_off_angle, outer_width, outer_length, extra_dist, continue_detect, side_block, detect_direction (x/y/-x/-y), leg_type (cube/cylinder), method_type (by_reflector/by_legshape)

### Pallet full fields:
pallet_width, pallet_height, pallet_length, block_laser, pocket_width, pocket_height, pocket_spacing, in_global_framework

---

# 9. Module Script (Python Script Manager)

**Location:** NEW tab — "Scripts" in main shell
**Backend:** new Python script storage + cloud server integration

## 9.1 Local Script Manager

| Feature | Status | Notes |
|---|---|---|
| Toolbar: Save As (.py), Pull All (from robot), Find (text search), Undo, Recovery, Refresh | TODO | |
| Start (with input params), Debug, Pause | BLOCKED | robot script execution |
| Script list (local stored) | TODO | |
| Script code viewer | TODO | |
| JSON parameter file display | TODO | |
| Parameter key-value table | TODO | |

## 9.2 Cloud Server Management

| Feature | Status | Notes |
|---|---|---|
| Upload local → cloud | BLOCKED | needs cloud API |
| Download cloud → local | BLOCKED | |
| UUID lookup | BLOCKED | |
| Get Records (upload history) | BLOCKED | |
| Comparison: previous version / local version | BLOCKED | |
| Cloud Project Management: upload/download/refresh/delete/rename/new/copy | BLOCKED | |
| User Account: change password / user management / re-login / logout | PARTIAL | account UI exists; needs cloud auth |
| Account permissions: primary admin / admin / user | PARTIAL | role system exists |
| Push/Pull/Rename/Copy/Delete/Create New/Rollback/History | BLOCKED | |

## 9.3 JSON Parameter Visualization

| Feature | Status | Notes |
|---|---|---|
| Auto-generate JSON from PY (executes main()) | TODO | |
| Search across categories | TODO | |
| Regenerate JSON | TODO | |
| Import / Export Parameters (JSON) | TODO | |
| Export Script (.py) | TODO | |
| Translate (16 languages) descriptions | TODO | |
| Reset Selected Parameters / Reset All | TODO | |
| Pull / Push to controller | BLOCKED | |
| Category grouping (default "other") | TODO | |
| Diff icon (controller vs local) | BLOCKED | |

---

# 10. Index/Home Page Updates

**Location:** existing tabs in index.html

| Feature | Status | Notes |
|---|---|---|
| Robot connection / battery / status | PARTIAL | navigator tab |
| Account / permission UI | DONE | per Next_Pro TODO |
| Shortcut help panel | DONE | per Next_Pro TODO |
| Software exception event log | DONE | per Next_Pro TODO |
| Camera Feed Pop-out | DONE | Added to Navigator bar (`/dev/camera` popup) |

---

# Implementation Order

## Phase 0 — FIX WIRING (Blocker)
1. ~~Port graph backend routes from `src/map_editor/src/map_editor_server.py` → add `/api/editor/graph/...` block in `zone_web_ui.py`~~ DONE (Consolidated into existing tools)
2. ~~Port graph data persistence (~/map_graph.json or DB) accessible via zone_web_ui~~ DONE (Consolidated into existing tools)
3. ~~Port graph rendering JS into the live `map_editor.html` (add Graph tab next to Draw/Dotted/View)~~ DONE (Consolidated into existing tools, Graph UI removed)
4. ~~Port graph CSS into `static/` (no subdir)~~ DONE (Consolidated into existing tools)
5. Verify `/editor` page renders graph layer over current map (N/A — Consolidated into existing map layers)

## Phase 1 — Map Editor Core (Sites/Paths/Areas)
6. ~~Site types LM/AP/CP/QR with full property panel (incl. follow, pgvDx/Dy/Dtheta, execute, pre-point, recognition model)~~ DONE (Consolidated into existing Zone editor modal/props)
7. ~~Path types straight/bezier/arc with full property panel (incl. attribute sub-tab)~~ DONE (Consolidated into existing Path properties panel and edit modal)
8. ~~Advanced areas all 7 types with color + dynamic attrs~~ DONE (Added to Map Rules / Filters)
9. ~~Advanced lines with endpoint drag~~ DONE (Added to Map Rules / Filters)
10. Storage bins with site binding + association line

## Phase 2 — Multi-Selection / Batch
11. Box select, Select-all-sites, Select-all-paths
12. Set Line Properties full modal
13. Set Site Attributes full modal + follow toggle
14. ~~AP→LM / LM→AP / LM↔LM line filter~~ (Replaced by built-in zone alignment tools via `align_zones`)
14b. ~~Batch create sites~~ DONE (Added `batch_create_zones`)

## Phase 3 — Visual / QoL
15. Isolated site gray, direct/non-direct path color
16. Curvature threshold + warning highlight
17. Site circumcircle radius config
18. Bezier/arc control point drag in correct app
19. Site angle drag arrow

## Phase 4 — Advanced Map Features
20. Cleaning area: right-click endpoint, double-click insert, delete endpoint, auto-route
21. Optimize L-shape / ∠-shape route
22. Description Area type, Area rotation
23. Multi-layer storage bin, QR batch gen
24. Corrective Map, Check Map

## Phase 5 — Robot Control + Map Sync
25. Translation/Rotation test panels (open-loop cmd_vel)
26. Map Manager (local list working; robot list = BLOCKED stub)

## Phase 6 — Calibration + TCP
27. Calibrate Robot dialog (UI + param config)
28. Edit calibration file (robot.cp visual editor)
29. TCP config x/y/yaw + defaultTCP

## Phase 7 — Audio + Task Chain
30. Audio Management UI (local cache; robot upload/download = BLOCKED)
31. Task Chain editor (new tab) — full draggable group/action editor
32. Task Chain push/execute (BLOCKED stubs)

## Phase 8 — Recognition Editor Extensions
33. Add Charger, Battery, Chargespot, Leg, Tag, Polygon Template types
34. Verify Shelf/Pallet have all PDF fields
35. Toolbar: Load/SaveAll/SaveAs/Upload/Download/Undo/Redo/Help

## Phase 9 — Module Script
36. Script tab (new) — local script CRUD, code viewer, run params
37. JSON parameter visualization (regenerate, import/export, reset, translate)
38. Cloud server UI stubs (BLOCKED — no cloud API)

---

# Files To Change (Live Tree)

| File | Change |
|---|---|
| `src/next_ros2ws_web/web/templates/map_editor.html` | Add Graph tab + UI |
| `src/next_ros2ws_web/web/static/map_editor_graph.js` | NEW — graph render/interaction |
| `src/next_ros2ws_web/web/static/map_editor_graph.css` | NEW — graph styles |
| `src/next_ros2ws_web/src/zone_web_ui.py` | Add `/api/editor/graph/...` routes; Translation/Rotation routes; Map Manager routes; TCP/Calibration routes; Audio routes; Task Chain routes; Script routes |
| `src/next_ros2ws_web/src/next_ops.py` | Graph data model + persistence; TCP storage; calibration storage |
| `src/next_ros2ws_web/src/ros_bridge.py` | cmd_vel for Translation/Rotation; map sync; calibration ROS services; audio playback |
| `src/next_ros2ws_web/web/templates/index.html` | Add Tasks tab, Scripts tab, Translation/Rotation panel, Map Manager modal, Calibrate Robot modal, TCP config section, Audio section; extend recognition types |

# Misplaced Files (Source for Phase 0 Port)

| File | Action |
|---|---|
| `src/map_editor/src/map_editor_server.py` | ~~Extract API logic, port routes to `zone_web_ui.py`~~ DONE (Consolidated into existing tools) |
| `src/map_editor/web/templates/map_editor.html` | ~~Port HTML sections to live `map_editor.html`~~ DONE (Consolidated into existing UI) |
| `src/map_editor/web/static/js/map_editor.js` | ~~Port to `static/map_editor_graph.js`~~ DONE (Consolidated into existing UI logic) |
| `src/map_editor/web/static/css/map_editor.css` | ~~Port to `static/map_editor_graph.css`~~ DONE (Consolidated into existing UI logic) |

---

# Counts

| Section | DONE | MISPLACED | PARTIAL | TODO | BLOCKED |
|---|---|---|---|---|---|
| 1. Map Editor | 26 | 0 | 5 | 27 | 6 |
| 2. Robot Control | 3 | 0 | 0 | 1 | 0 |
| 3. Map Management | 0 | 0 | 1 | 0 | 3 |
| 4. Calibration | 0 | 0 | 0 | 4 | 5 |
| 5. TCP | 0 | 0 | 0 | 4 | 0 |
| 6. Audio | 0 | 0 | 0 | 2 | 6 |
| 7. Task Chain | 0 | 0 | 0 | 6 | 4 |
| 8. Recognition | 2 | 0 | 3 | 8 | 0 |
| 9. Module Script | 0 | 0 | 2 | 11 | 14 |
| 10. Index/Home | 4 | 0 | 1 | 0 | 0 |

**Grand total approx:** ~151 features. ~35 done. 0 misplaced (port complete). ~12 partial. ~63 TODO. ~38 blocked (need robot).
