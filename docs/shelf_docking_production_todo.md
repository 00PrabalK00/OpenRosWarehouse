# Shelf Docking Production TODO

This file turns the production docking redesign into tracked implementation work.

## Safety Contract

- [x] Freeze the shelf plan during straight-in so live geometry cannot steer entry.
- [x] Keep the final approach split into staged pre-align targets instead of a direct rush into the shelf mouth.
- [ ] Introduce a dedicated docking supervisor with explicit states:
  `idle -> search -> candidate_tracking -> qualify -> pre_align_centerline -> hold_stable -> commit_dock_frame -> move_to_entry -> insert -> verify_seated -> success`
  and failure states:
  `abort_stop -> controlled_retreat -> fault`
- [ ] Enforce production commit semantics: after `commit_dock_frame`, fresh perception may only validate `continue`, `slow`, or `abort`; it must never reshape the committed entry reference.
- [ ] Add deterministic failure codes and fault categories for every docking exit path.

## Perception Pipeline

- [x] Publish candidate stability and confidence fields in `/shelf/status_json`.
- [x] Distinguish `detectability_confidence` from `control_usability_confidence`.
- [x] Track a temporal history of shelf pose samples instead of treating every scan as brand new.
- [x] Define N-of-M consistency explicitly:
  a pose sample is an inlier only if it stays within translation / yaw tolerances of the window mean pose, and commit-ready consistency requires at least `N` inliers out of the last `M` samples.
- [ ] Replace raw greedy hotspot grouping with a more robust clustering stage such as DBSCAN or constrained Euclidean clustering.
- [ ] Add scan-to-scan reflector identity persistence so hotspot clusters keep a stable track id over time.
- [ ] Add geometry-fit confidence with width bounds, depth bounds, symmetry checks, and reflector intensity balance scoring.
- [ ] Output pose uncertainty or covariance-like confidence terms for position, yaw, width, and model validity.
- [ ] Add temporal filtering on position and yaw that is strong enough to reject jitter but weak enough to preserve real motion.
- [ ] Add an N-of-M consistency latch for promotion from visible candidate to commit-eligible target.
- [ ] Add optional multi-sensor fusion hooks for reflector geometry, map consistency, odometry consistency, short-range depth, and side-clearance sensing.

## Dock Frame And Commit

- [x] Add shared data contracts for `ShelfPerceptionEstimate`, `CommittedDockFrame`, `DockingAttemptTelemetry`, and pre-entry gate samples.
- [ ] Create a `CommittedDockFrame` at the moment of docking commitment with:
  mouth center, entry pose, final pose, expected insertion depth, left/right clearances, timestamp, validity window, and confidence record.
- [ ] Store committed frames per attempt so verification and retreat use the same frozen reference.
- [ ] Reject commit when perception confidence is below the production threshold unless an explicit engineering override is enabled.

## Pre-Entry Qualification

- [x] Add a reusable temporal qualification gate utility that evaluates a window of pre-entry samples instead of a single sample.
- [ ] Replace the remaining ad hoc pre-entry checks in `zone_manager.py` with the shared qualification window metrics.
- [ ] Gate entry on sustained evidence:
  lateral error, heading error, entry distance, velocity settling, localization agreement, perception stability, and low angular command oscillation.
- [ ] Add zero-crossing detection on angular commands so the robot cannot enter while still hunting.
- [ ] Add odometry-versus-localization disagreement checks before entry is allowed.

## Pre-Align And Entry Staging

- [x] Simplify shelf pre-align to two POIs:
  `P1 centerline setup -> ALIGN_TO_HEADING -> P2 guarded entry`.
- [x] Render the generated shelf POIs on the map and popup for debugging.
- [ ] Move POI generation and execution ownership out of `zone_manager.py` into a dedicated supervisor/controller boundary.
- [ ] Add a hold-stable state at `P1` before advancing to `P2`.

## Motion Ownership

- [x] Add a hard docking motion gate scaffold that validates command owner, epoch, lease expiry, and command freshness.
- [x] Introduce an explicit docking motion lease or token that names the sole active motion owner during docking.
- [x] Extend command metadata so every command carries:
  source identity, docking state, and validity epoch.
- [ ] Reject stale or competing docking motion commands at the ownership boundary.
- [ ] Unify docking command ownership so the final insert controller is the only source of motion during committed entry.
- [ ] Integrate the docking motion lease with `navigation_arbitrator.py`, `mode_switcher.py`, and downstream safety gating.

## Final Insertion Controller

- [x] Define dock-frame math explicitly:
  mouth center is the origin, dock x is insertion direction, dock y is the lateral axis, and every cycle computes `s`, `e_y`, and `e_theta`.
- [x] Implement abort-before-command ordering in the frozen insertion controller.
- [x] Implement the bounded final-insert control law:
  `v = slow insert speed`, `w = clamp(k_theta * e_theta - k_y * e_y, -w_max, +w_max)` with a tighter heading-only angular cap when lateral error is already tiny.
- [x] Add jerk-limited ramping for linear and angular commands inside the frozen insertion controller scaffold.
- [x] Replace the legacy Nav2-style constrained straight-in stage in `zone_manager.py` with a direct frozen-centerline insertion loop that publishes through the docking motion gate.
- [ ] Track one-dimensional progress along the committed dock axis plus bounded lateral deviation.
- [ ] Cap or disable yaw correction during final insert so the tail cannot swing into the shelf leg.
- [ ] Add jerk-limited speed shaping for the final 20-40 cm.
- [ ] Account for braking distance in final insertion stop logic.
- [ ] Keep live perception as a validation input only during insert, never a replanning signal.

## Abort, Retreat, And Safety Arbitration

- [ ] Add continuous divergence monitors for lateral error, heading error, confidence drop, localization disagreement, and reflector disappearance timing.
- [ ] Add wheel-slip and commanded-versus-observed velocity mismatch monitoring.
- [ ] Add hooks for contact inference using bumper, current spike, or abnormal deceleration signals where available.
- [ ] Trigger immediate stop on confirmed divergence.
- [ ] Implement controlled retreat along the committed centerline to a safe reacquisition zone.
- [ ] Distinguish `abort_stop`, `controlled_retreat`, and `fault` in logs and telemetry.

## Verification

- [ ] Add a seated verification stage after insertion completes.
- [ ] Verify final depth, yaw, lateral centering, and consistency with the committed dock frame.
- [ ] Refuse to report success when verification fails.
- [ ] Add retry / retreat / human-assist policy outputs for failed verification.

## Calibration And Compensation

- [ ] Add commissioning routines for straight-line drift, left/right imbalance, deadband, minimum controllable yaw rate, stop latency, and floor-dependent slip.
- [ ] Add controller-side compensation for measured mechanical bias instead of assuming a perfectly symmetric base.
- [ ] Add time alignment between sensor timestamps, localization, and issued commands.
- [ ] Replace nominal-width-only shelf margins with calibrated swept-envelope margins.

## Telemetry And Review

- [x] Add shared telemetry structures for production docking attempts.
- [ ] Persist telemetry per attempt:
  committed frame, perception confidence over time, gate metrics, motion owner, commanded/measured twist, lateral/heading error traces, abort reason, dwell times, and verification outcome.
- [ ] Add structured failure categories such as:
  `weak_commit`, `prealign_unstable`, `authority_conflict`, `mid_insert_drift`, `model_invalidated`, `suspected_contact`, `mechanical_bias`.
- [ ] Add offline analysis scripts for docking failure clustering and trend review.

## Refactor Plan

- [ ] Extract production docking logic out of `zone_manager.py` into smaller modules:
  `docking_contracts.py`, `docking_gate.py`, `docking_supervisor.py`, `docking_insertion_controller.py`, `docking_telemetry.py`.
- [ ] Keep `zone_manager.py` as the orchestration entry point until the supervisor fully replaces the legacy flow.
- [ ] Add targeted tests for the pre-entry gate, committed dock frame creation, divergence abort logic, and verification rules.

## Current Foundations Landed

- [x] Two-POI shelf pre-align flow is in place.
- [x] Straight-in uses a frozen plan instead of live shelf refinement.
- [x] Detector status now has the scaffolding to report temporal stability and confidence.
- [x] Shared production docking contract types exist in core for the next refactor steps.
- [x] The shared temporal pre-entry gate is now wired into the live `zone_manager` pre-insert qualification path.
- [x] A docking supervisor scaffold with explicit state transitions and motion lease handling exists in core.
- [x] A dedicated frozen insertion controller scaffold exists in core so final entry logic has a home outside `zone_manager.py`.
- [x] A hard docking motion gate scaffold exists in core so stale or competing docking commands can be rejected by owner / epoch.
- [x] `zone_manager.py` now hands committed straight-in entry to the frozen insertion controller and publishes docking-time motion through the hard gate on the navigation lane.
