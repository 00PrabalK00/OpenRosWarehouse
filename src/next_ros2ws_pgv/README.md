# next_ros2ws_pgv

Standalone PGV Matrix Tag localization stack for the R3138 RS-485 code reader.
It can be launched by itself, or included from the main UI/Nav bringup with
`enable_pgv_localization:=true`.

## What it does

A downward-facing PGV reader under the robot detects fixed oriented floor
Matrix Tags. Each tag has a known map pose and direction (the *tag map*). From
a single reading the stack computes the absolute robot pose and feeds it to
AMCL/Nav2 through `/initialpose`, correcting localization drift without touching
wheel odometry.

```
PGV reader (RS-485)
   -> /pgv/tag  /pgv/code_detected  /pgv/pose  /pgv/angle_deg
pgv_localizer  (+ tag map + URDF/TF base_link->pgv_link)
   -> /pgv/corrected_pose (PoseWithCovarianceStamped)
   -> /initialpose  (nudges AMCL, == map->odom correction)
Nav2 continues navigation
```

## Nodes

| Node | Role |
|------|------|
| `pgv_reader` | Polls the R3138 over RS-485, publishes raw reading topics. |
| `tag_map_server` | Loads `tag_map.yaml`, publishes RViz markers and direction arrows for every Matrix Tag. |
| `pgv_localizer` | Tag map + PGV reading + TF mount -> corrected `map -> base_link` pose + `/initialpose`. |
| `pgv_diagnostics` | Health on `/diagnostics`: stale/comm, no code, unknown tag. |
| `pgv_calibrator` | Estimates `base_link->pgv_link` (hand-eye) + fit plot. |

## Camera mounting

PGV camera mounting: rear mounted, downward facing, on a bracket extending
12 cm behind the rear chassis edge, centered on the chassis width at ~24 cm
from each side and 10 cm below the robot base plane. Optical axis faces
straight down toward the floor.

### Clean ROS frame convention

`pgv_link` is a clean ROS localization frame **aligned with `base_link`**:
- `+X` = robot forward
- `+Y` = robot left  
- `+Z` = up
- yaw = counter-clockwise positive

The URDF **does not** rotate `pgv_link` 180°. Any R3138 raw sign convention
is handled inside `pgv_reader` via `pose_x_sign`, `pose_y_sign`,
`pose_yaw_sign` and `pose_yaw_offset_deg`.

URDF `base_link -> pgv_link` (from `ui_ws/urdf/pgv.xacro`):
```
x   = -0.52 m   # -(chassis_length/2) - 0.12 m bracket behind rear edge
y   =  0.00 m   # laterally centered
z   =  0.10 m   # 10 cm above base_link plane
yaw =   0 deg   # pgv_link aligned with base_link (no rotation)
```

Run `pgv_sign_test` (see below) to determine the correct `pose_*_sign`
settings before relying on localization.

### Sign verification procedure

Use the built-in sign-test node to map the raw R3138 readings into the clean
`pgv_link` frame:

```bash
ros2 run next_ros2ws_pgv pgv_sign_test
```

1. Place the robot so the PGV sees a known Matrix Tag.
2. Jog the robot **forward** (+X in `base_link`). Watch `pgv/pose.position.x`.
   - If x **increases** → `pose_x_sign: +1.0`
   - If x **decreases** → `pose_x_sign: -1.0`
3. Jog the robot **left** (+Y in `base_link`). Watch `pgv/pose.position.y`.
   - If y **increases** → `pose_y_sign: +1.0`
   - If y **decreases** → `pose_y_sign: -1.0`
4. Rotate the robot **counter-clockwise** (+yaw). Watch `pgv/angle_deg`.
   - If angle **increases** → `pose_yaw_sign: +1.0`
   - If angle **decreases** → `pose_yaw_sign: -1.0`
5. Write the verified signs into `config/pgv_localization.yaml` and restart
   `pgv_reader`.

> **Rule:** if you flip one axis sign, you must also flip `pose_yaw_sign` to
> keep the 2D rotation group consistent. A lone yaw flip without a matching
> axis flip will make computed poses jump to garbage points.

### Camera module (mechanical)

| Property | Value |
|----------|-------|
| Body (W x H x D) | ~60 mm x 68 mm x 43 mm |
| Front mounting face | ~47 mm x 47 mm pattern |
| Connector protrusion | ~13.5 mm above the housing |
| Mounting holes | 4x M4, 7 mm depth |
| Mounting holes | 1/4-20 UNC, 7 mm depth |

The `pgv_z = -0.10 m` drop is measured to the camera mounting level; account
for the ~68 mm body height + connector when fitting the bracket so the optical
face clears the chassis and points straight down at the floor.

## Transform math

```
T_map_base = T_map_tag  o  inverse(T_pgv_tag)  o  inverse(T_base_pgv)
```

`T_base_pgv` is the PGV mounting offset from URDF/static TF. The URDF
`base_link -> pgv_link` fixed joint is the source of truth. The localizer still
keeps `base_to_pgv_*` parameters only as a fallback for bench tests or bringup
when TF is not available.

## Correction gates

A reading is applied only if: a Matrix Tag is detected, the tag id is in the map,
the measured offset is within `max_offset_m`, the same tag is stable for
`stable_frames`, and (if `zone_enabled`) the corrected pose is inside the
localization zone rectangle.

## Calibration

`base_to_pgv_*` comes from an extrinsic calibration (2D hand-eye). While the
robot drives over a fixed Matrix Tag, the calibrator records odom poses (`A_i`) and
PGV readings (`B_i`) and solves for the constant mounting `X = T_base_pgv` that
makes `C_i = A_i o X o B_i` (the tag pose in odom) identical for all samples.
The solve uses the current URDF/static TF `base_link -> pgv_link` transform as
its initial guess/reference, falling back to `base_to_pgv_*` params if TF is not
available. It does not overwrite the URDF; it writes the fitted transform plus
the delta from the current reference so the mount can be reviewed and updated
intentionally. A pure straight line leaves the along-travel offset unobservable,
so an in-place spin is required.

```bash
ros2 launch next_ros2ws_pgv pgv_calibration.launch.py odom_topic:=/odom
ros2 service call /pgv_calibrator/start  std_srvs/srv/Trigger {}
# drive forward/back a few times, then spin in place one full turn
ros2 service call /pgv_calibrator/finish std_srvs/srv/Trigger {}
```

Result is written to `output_yaml` (default `/tmp/pgv_calibration.yaml`, already
in `pgv_localizer` param form, with `mount_reference` and
`mount_delta_from_reference` for review) and a **fit-quality plot** to
`plot_path` (default `/tmp/pgv_calibration_fit.png`): measured tag positions
before calibration (orange) vs after (red, should collapse to one point) plus
the per-sample residual. Live sample points are also published on
`pgv/calibration_samples` for RViz.

## Run

```bash
ros2 launch next_ros2ws_pgv pgv_localization.launch.py \
    port:=/dev/next/pgv \
    tag_map:=/abs/path/tag_map.yaml
```

Config: `config/pgv_localization.yaml` (params), `config/tag_map.yaml`
(Matrix Tag id -> map x/y/yaw). Edit the tag map to match the tags pasted on
the floor. `yaw_deg` is the printed/tag tape forward direction in the map
frame; set it correctly so Nav receives a useful heading correction.

## Mapping to the AGV setup workflow

| Workflow step | Where it lives here |
|---------------|---------------------|
| Mount PGV, set height/orientation | URDF `base_link -> pgv_link` |
| Brand / protocol (RS-485) | `pgv_reader` (R3138 P+F protocol) |
| `func = adjustLocalization` | `pgv_localizer` -> `/initialpose` correction |
| Calibrate PGV vs robot center | update URDF `base_link -> pgv_link`; optionally keep fallback params aligned |
| Matrix Tag positions/directions in software map | `config/tag_map.yaml` (authored in the map editor, see below) |
| Localization configuration area | `zone_enabled` + `zone_*` params |
| Enable tag-code localization | `pgv_localizer` running in the zone |
| Relocalization on detect | correction published as `/initialpose` |
| Alarms (comm, no code, mismatch) | `pgv_diagnostics` -> `/diagnostics` |

## Web UI (PGV PIP)

The operator UI (`next_ros2ws_web`) has a **PGV** button in the top
motor-control button row (next to LIFT / SHELF / CAMERA). It opens a draggable
picture-in-picture panel that:

- **Starts/stops the stack** — `START LOCALIZATION` / `START CALIBRATION` /
  `STOP` call backend routes that launch the standalone launch files as
  subprocesses (only one may own the RS-485 port at a time). Gated by the
  `test_mode:run` permission (admin/service roles), same as firmware update.
- **Shows live reading data** — subscribes over the existing rosbridge
  websocket to `pgv/pose`, `pgv/tag`, `pgv/angle_deg`, `pgv/code_detected`,
  `pgv/corrected_pose` and filters `/diagnostics` for PGV health.
- **Runs calibration** — `START` / `FINISH` / `CANCEL` call the
  `/pgv_calibrator/*` Trigger services via roslibjs; `FINISH` then loads the
  fit-quality plot and the solved `base->pgv` result.

Backend routes (`zone_web_ui.py`): `GET /api/pgv/status`,
`POST /api/pgv/launch`, `POST /api/pgv/shutdown`, `GET /api/pgv/plot`
(serves the fit PNG), `GET /api/pgv/calibration` (parsed result YAML).
Paths are overridable with `NEXT_PGV_PLOT`, `NEXT_PGV_CALIB_YAML`,
`NEXT_PGV_PORT`.

## Authoring Matrix Tags in the map editor

Floor Matrix Tags are their **own map layer** (stored in the existing
`qr_codes` table for compatibility, separate from zones / action points). The
operator UI has a **MATRIX TAGS** button in the map-editor (edit mode) button
row that opens a draggable panel with three ways to place tags and a tag-map
export:

- **Place on map** - toggle the tool, then click the map to drop the *Next tag
  id* at that point. The id auto-increments so you can click tag after tag.
- **Drop at robot** — drops the next id at the robot's current pose. The map
  pose is the robot base pose composed with the `base->pgv` offset (defaults
  match `pgv_localization.yaml` and `pgv_link`; override
  `NEXT_PGV_BASE_TO_PGV_X/Y/YAW_DEG`),
  because the tag sits under the camera. Align the robot with the tag's forward
  direction before capturing, or edit the row's `Yaw deg` manually.
- **Auto-add (drive over)** — toggle on, then drive the robot over the tags in
  sequence. The panel subscribes to `pgv/tag` + `pgv/code_detected`; each newly
  detected tag is appended at the robot pose automatically (known ids are
  skipped). This is the fastest way to register a real floor grid.

**Align to a line** fits a total-least-squares (PCA) line through every tag and
snaps them onto it, anchored on the lowest id. The fitted line direction is
written into every tag's yaw, which is what the PGV localizer uses for heading.
Leave the spacing field blank to just straighten; give a value to also space
them evenly from the first tag.

**Manual cleanup**: X/Y/Yaw of each tag are editable inline in the list.

**Auto-link**: dropping an action-point zone on top of a Matrix Tag (within
`NEXT_QR_LINK_RADIUS`, default 0.35 m) automatically sets that AP's `tag_id` and
enables PGV positioning, so the point and the tag connect with no extra step.

Matrix Tags render as green markers with their id and direction arrow on the
map (their own draw layer), and the panel lists them with per-row delete plus
**Clear all**. **Export tag map** writes `config/tag_map.yaml` (the file
`tag_map_server`, `pgv_localizer` and `pgv_diagnostics` read); restart PGV
localization from the PGV popup to load it.

Action points stay a separate layer: add an AP on top of a tag and link them by
id. The Matrix Tag store does not create or depend on APs.

Backend (`zone_web_ui.py` + `RosBridge`): `GET /api/qr/list`, `POST /api/qr/add`,
`POST /api/qr/capture`, `POST /api/qr/delete`, `POST /api/qr/clear`,
`POST /api/qr/export-tagmap`; store CRUD in `db_manager` (`qr_codes` table).
Export path is the package's `config/tag_map.yaml` (override `NEXT_PGV_TAGMAP`).

## Not included

- **Mission/route decisions**: Node-RED/operator flow still decides the route,
  branch, station, and task behavior. This package only provides trusted tag
  identity, offset, direction, and Nav localization correction.
