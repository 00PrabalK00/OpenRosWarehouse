"""
Geometry-aware dotted map visualization layer.

This module is display-only. It never mutates the underlying SLAM / occupancy
data; it only converts a noisy occupancy grid snapshot into a cleaner dotted
representation that is easier for operators to read.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import cv2
import numpy as np
from PIL import Image


DEFAULT_MAP_RESOLUTION_M_PER_PX = 0.05
# All preset names still accepted by the API for backward compatibility,
# but they all resolve to the single well-tuned configuration below.
SUPPORTED_PRESETS = ("balanced", "balanced_plus", "quality", "speed")


@dataclass(frozen=True)
class DottedPreset:
    name: str
    noise_open_m: float
    gap_close_m: float
    stabilize_radius_m: float
    min_arc_length_m: float
    min_component_area_m2: float
    min_component_extent_m: float
    wall_min_aspect: float
    wall_min_extent_m: float
    curvy_wall_min_extent_m: float
    curvy_wall_min_aspect: float
    curvy_wall_max_fill_ratio: float
    curvy_wall_min_support: float
    curvy_wall_min_arc_length_m: float
    curvy_wall_simplify_px: float
    object_max_extent_m: float
    object_max_area_m2: float
    hough_threshold: int
    hough_min_length_m: float
    hough_max_gap_m: float
    cluster_angle_deg: float
    cluster_perp_m: float
    cluster_gap_m: float
    dedupe_angle_deg: float
    dedupe_sep_m: float
    dedupe_overlap_ratio: float
    bridge_angle_deg: float
    bridge_gap_m: float
    bridge_lateral_m: float
    snap_angle_deg: float
    snap_min_length_m: float
    snap_min_confidence: float
    render_min_length_m: float
    render_min_snapped_length_m: float
    offaxis_keep_angle_deg: float
    offaxis_min_length_m: float
    isolation_short_length_m: float
    isolation_neighbor_m: float
    structural_axis_tolerance_deg: float
    structural_cluster_sep_m: float
    structural_merge_gap_m: float
    structural_min_length_m: float
    render_objects: bool
    spacing_min_px: int
    spacing_max_px: int
    object_simplify_px: float


# Single well-tuned rendering configuration. The preset parameter of
# generate_dotted_map() is still accepted for API compatibility, but all
# names resolve to this one configuration.
_CLEAN_PRESET = DottedPreset(
    name="clean",
    # ── Pre-processing ─────────────────────────────────────────────────────
    # Preprocessing: close only, no open.
    # SLAM walls are 1-2 pixels wide.  Any morphological opening destroys
    # them — even a 3×3 open after a 3×3 close erodes 1px walls to nothing.
    # We rely on the Hough minimum length and the confidence gate in
    # filter_representative_lines to suppress short noise segments instead.
    #
    # At 0.05 m/px: gap_close_m=0.04 → round(0.04/0.05)=1px radius → 3×3
    # kernel.  This bridges single-pixel gaps within a wall without merging
    # separate nearby walls (a 5×5 or larger kernel merges them, which
    # destroys the elongated aspect ratio needed for wall classification).
    noise_open_m=0.0,
    gap_close_m=0.04,
    # Stabilisation radius: find the medial axis of closed walls.
    stabilize_radius_m=0.075,
    # ── Component admission ────────────────────────────────────────────────
    min_arc_length_m=0.20,
    # At 0.05 m/px: 0.005 m² / 0.0025 = 2 px² minimum area.
    min_component_area_m2=0.005,
    # At 0.05 m/px: 0.10m = 2px minimum extent.
    min_component_extent_m=0.10,
    # ── Wall vs. object classification ────────────────────────────────────
    wall_min_aspect=2.0,
    wall_min_extent_m=3.0,
    # ── Curvy-wall path ────────────────────────────────────────────────────
    # Curvy-wall rendering skeletonises blobs.  The skeleton of a compact
    # noise blob is a cross — this is the primary source of the X-shaped
    # artifacts.  Requiring ≥ 4 m extent disables the path for all typical
    # noise blobs while still allowing genuinely large curved structures.
    curvy_wall_min_extent_m=4.0,
    curvy_wall_min_aspect=1.50,
    curvy_wall_max_fill_ratio=0.35,
    curvy_wall_min_support=0.70,
    curvy_wall_min_arc_length_m=4.0,
    curvy_wall_simplify_px=3.2,
    # ── Object rendering (disabled) ───────────────────────────────────────
    object_max_extent_m=0.90,
    object_max_area_m2=0.25,
    render_objects=False,
    # ── Hough line extraction ──────────────────────────────────────────────
    # Mask is thinned to a skeleton before Hough (see
    # extract_wall_segments_hough) so thick walls produce one detection
    # not two.
    hough_threshold=12,
    hough_min_length_m=0.20,
    hough_max_gap_m=0.15,
    # ── Segment clustering ─────────────────────────────────────────────────
    cluster_angle_deg=8.0,
    cluster_perp_m=0.15,
    cluster_gap_m=0.25,
    # ── Deduplication ─────────────────────────────────────────────────────
    # 0.22 m catches both edges of a ~20 cm thick SLAM wall.
    dedupe_angle_deg=8.0,
    dedupe_sep_m=0.22,
    dedupe_overlap_ratio=0.25,
    # ── Gap bridging ───────────────────────────────────────────────────────
    bridge_angle_deg=7.0,
    bridge_gap_m=0.40,
    bridge_lateral_m=0.12,
    # ── Axis snapping ─────────────────────────────────────────────────────
    snap_angle_deg=10.0,
    snap_min_length_m=0.60,
    snap_min_confidence=0.70,
    # ── Length filtering ──────────────────────────────────────────────────
    # Confidence gate in filter_representative_lines prevents short
    # noise segments from sneaking past the low length threshold.
    render_min_length_m=0.40,
    render_min_snapped_length_m=0.35,
    offaxis_keep_angle_deg=20.0,
    offaxis_min_length_m=1.5,
    # ── Isolation pruning ─────────────────────────────────────────────────
    isolation_short_length_m=0.80,
    isolation_neighbor_m=2.5,
    # ── Structural consolidation ───────────────────────────────────────────
    structural_axis_tolerance_deg=12.0,
    structural_cluster_sep_m=0.15,
    structural_merge_gap_m=0.60,
    structural_min_length_m=0.60,
    # ── Rendering ─────────────────────────────────────────────────────────
    spacing_min_px=2,
    spacing_max_px=6,
    object_simplify_px=2.0,
)

# Map all known preset names to the single configuration so that any
# existing callers passing a preset= argument continue to work.
PRESETS: Dict[str, DottedPreset] = {name: _CLEAN_PRESET for name in SUPPORTED_PRESETS}


def generate_dotted_map(
    pil_image: Image.Image,
    *,
    map_resolution_m_per_px: Optional[float] = None,
    preset: str = "balanced",
    occupied_thresh: int = 127,
    marker_type: str = "circle",
    dot_radius: int = 1,
    extra_lines: Optional[Sequence[Tuple[int, int, int, int]]] = None,
    bg_color: Tuple[int, int, int] = (240, 240, 240),
    dot_color: Tuple[int, int, int] = (30, 30, 30),
    debug: bool = False,
) -> Image.Image:
    resolution = _normalize_resolution(map_resolution_m_per_px, debug=debug)
    preset_cfg = PRESETS[_normalize_preset(preset)]

    gray = np.array(pil_image.convert("L"))
    occupied = preprocess_occupancy(gray, occupied_thresh, resolution, preset_cfg)
    stabilized, _ = stabilize_walls(occupied, resolution, preset_cfg)
    components = extract_components(occupied, stabilized, resolution, preset_cfg)

    wall_segments: List[Dict[str, Any]] = []
    curvy_wall_components: List[Dict[str, Any]] = []
    object_components: List[Dict[str, Any]] = []
    for component in components:
        candidate_segments = extract_wall_segments_hough(component, resolution, preset_cfg)
        component["candidate_segments"] = candidate_segments
        component.update(score_component_geometry(component, resolution, preset_cfg))
        if bool(component.get("curvy_wall_like")):
            curvy_wall_components.append(component)
        elif bool(component.get("wall_like")) and candidate_segments:
            wall_segments.extend(candidate_segments)
        elif bool(component.get("render_as_object")):
            object_components.append(component)

    wall_groups = cluster_segments_union_find(wall_segments, resolution, preset_cfg)
    wall_lines = fit_representative_lines(wall_groups, resolution, preset_cfg)
    wall_lines = deduplicate_parallel_lines(wall_lines, resolution, preset_cfg)
    wall_lines = bridge_line_gaps(wall_lines, resolution, preset_cfg)
    wall_lines = snap_representative_lines(wall_lines, resolution, preset_cfg)
    wall_lines = filter_representative_lines(wall_lines, resolution, preset_cfg)
    wall_lines = consolidate_major_walls(wall_lines, resolution, preset_cfg)
    wall_lines = deduplicate_parallel_lines(wall_lines, resolution, preset_cfg)
    wall_lines = prune_isolated_lines(wall_lines, resolution, preset_cfg)

    output = np.full((gray.shape[0], gray.shape[1], 3), bg_color, dtype=np.uint8)
    render_wall_lines(
        output,
        wall_lines,
        resolution,
        preset_cfg,
        marker_type=marker_type,
        dot_radius=dot_radius,
        dot_color=dot_color,
    )
    render_curvy_wall_contours(
        output,
        curvy_wall_components,
        resolution,
        preset_cfg,
        marker_type=marker_type,
        dot_radius=dot_radius,
        dot_color=dot_color,
    )
    if preset_cfg.render_objects:
        render_object_contours(
            output,
            object_components,
            resolution,
            preset_cfg,
            marker_type=marker_type,
            dot_radius=dot_radius,
            dot_color=dot_color,
        )

    if extra_lines:
        render_wall_lines(
            output,
            [_make_line_dict(*line) for line in extra_lines],
            resolution,
            preset_cfg,
            marker_type=marker_type,
            dot_radius=dot_radius,
            dot_color=dot_color,
        )

    return Image.fromarray(output, "RGB")


def preprocess_occupancy(
    gray: np.ndarray,
    occupied_thresh: int,
    resolution: float,
    preset: DottedPreset,
) -> np.ndarray:
    occupied = np.where(gray < int(occupied_thresh), np.uint8(255), np.uint8(0))

    # Close first: bridges small gaps in thin walls and thickens them so
    # they survive the subsequent opening step.  SLAM walls are often only
    # 1-2 pixels wide — applying an open before a close would erase them
    # entirely because the erode pass has nothing left to re-dilate.
    close_kernel = _kernel_from_meters(preset.gap_close_m, resolution, shape=cv2.MORPH_RECT)
    if close_kernel is not None:
        occupied = cv2.morphologyEx(occupied, cv2.MORPH_CLOSE, close_kernel)

    # Open after closing: walls are now thick enough to survive the erode
    # pass, while isolated noise pixels that did not connect to any real
    # structure are removed.
    open_kernel = _kernel_from_meters(preset.noise_open_m, resolution, shape=cv2.MORPH_RECT)
    if open_kernel is not None:
        occupied = cv2.morphologyEx(occupied, cv2.MORPH_OPEN, open_kernel)

    return occupied


def stabilize_walls(
    occupied: np.ndarray,
    resolution: float,
    preset: DottedPreset,
) -> Tuple[np.ndarray, np.ndarray]:
    if preset.stabilize_radius_m <= 0.0:
        support = cv2.distanceTransform(occupied, cv2.DIST_L2, 5) if occupied.any() else np.zeros_like(occupied, dtype=np.float32)
        return occupied.copy(), support

    kernel = _kernel_from_meters(preset.stabilize_radius_m, resolution, shape=cv2.MORPH_ELLIPSE)
    closed = cv2.morphologyEx(occupied, cv2.MORPH_CLOSE, kernel) if kernel is not None else occupied.copy()
    support = cv2.distanceTransform(closed, cv2.DIST_L2, 5) if closed.any() else np.zeros_like(closed, dtype=np.float32)

    radius_px = max(1.0, preset.stabilize_radius_m / resolution)
    core_thresh_px = max(0.75, radius_px * 0.45)
    core = np.uint8(support >= core_thresh_px) * 255

    reinforce_kernel = _kernel_from_pixels(max(1, int(round(radius_px * 0.5))), shape=cv2.MORPH_ELLIPSE)
    if reinforce_kernel is not None:
        core = cv2.dilate(core, reinforce_kernel, iterations=1)

    stabilized = cv2.bitwise_or(closed, core)
    return stabilized, support


def extract_components(
    occupied: np.ndarray,
    stabilized: np.ndarray,
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    component_source = stabilized if stabilized is not None and np.any(stabilized) else occupied
    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(component_source, connectivity=8)
    height, width = component_source.shape[:2]
    margin_px = max(2, _meters_to_px_int(0.10, resolution, min_px=2))
    dilate_kernel = _kernel_from_pixels(max(1, margin_px), shape=cv2.MORPH_ELLIPSE)
    components: List[Dict[str, Any]] = []

    for label in range(1, num_labels):
        x = int(stats[label, cv2.CC_STAT_LEFT])
        y = int(stats[label, cv2.CC_STAT_TOP])
        w = int(stats[label, cv2.CC_STAT_WIDTH])
        h = int(stats[label, cv2.CC_STAT_HEIGHT])
        area = int(stats[label, cv2.CC_STAT_AREA])
        if area <= 0:
            continue

        component_mask = np.uint8(labels[y:y + h, x:x + w] == label) * 255
        if max(w, h) < max(2, _meters_to_px_int(preset.min_component_extent_m * 0.4, resolution, min_px=2)) and area < 4:
            continue

        x0 = max(0, x - margin_px)
        y0 = max(0, y - margin_px)
        x1 = min(width, x + w + margin_px)
        y1 = min(height, y + h + margin_px)

        expanded = np.zeros((y1 - y0, x1 - x0), dtype=np.uint8)
        expanded[y - y0:y - y0 + h, x - x0:x - x0 + w] = component_mask
        if dilate_kernel is not None:
            expanded = cv2.dilate(expanded, dilate_kernel, iterations=1)

        stabilized_crop = cv2.bitwise_and(component_source[y0:y1, x0:x1], expanded)
        components.append(
            {
                "label": int(label),
                "x": x,
                "y": y,
                "width": w,
                "height": h,
                "area_px": area,
                "mask": component_mask,
                "crop_x": x0,
                "crop_y": y0,
                "crop_mask": stabilized_crop,
            }
        )

    return components


def score_component_geometry(
    component: Dict[str, Any],
    resolution: float,
    preset: DottedPreset,
) -> Dict[str, Any]:
    width_px = float(max(1, int(component.get("width", 1))))
    height_px = float(max(1, int(component.get("height", 1))))
    area_px = float(max(1, int(component.get("area_px", 1))))
    aspect = max(width_px, height_px) / max(1.0, min(width_px, height_px))
    extent_m = max(width_px, height_px) * resolution
    area_m2 = area_px * resolution * resolution
    fill_ratio = area_px / max(1.0, width_px * height_px)

    candidate_segments = component.get("candidate_segments", []) or []
    support_len_px = float(sum(float(seg.get("length_px", 0.0)) for seg in candidate_segments))
    support_score = _clamp((support_len_px * resolution) / max(0.25, extent_m), 0.0, 1.0)
    area_score = _clamp(area_m2 / max(preset.min_component_area_m2 * 4.0, 1e-6), 0.0, 1.0)
    extent_score = _clamp(extent_m / max(preset.min_component_extent_m * 3.5, 1e-6), 0.0, 1.0)
    aspect_score = _clamp((aspect - 1.8) / 5.5, 0.0, 1.0)
    thin_score = _clamp((0.55 - fill_ratio) / 0.40, 0.0, 1.0)

    wall_score = (
        0.20 * area_score
        + 0.30 * aspect_score
        + 0.20 * extent_score
        + 0.10 * thin_score
        + 0.20 * support_score
    )

    elongated_wall_fallback = (
        aspect >= 7.0 and extent_m >= 0.18
    ) or (
        aspect >= 4.5 and support_score >= 0.45 and extent_m >= 0.12
    )

    structural_shape = (
        aspect >= preset.wall_min_aspect
        or extent_m >= preset.wall_min_extent_m
    )
    straight_wall_like = bool(candidate_segments) and (
        elongated_wall_fallback
        or (wall_score >= 0.52 and structural_shape)
    )
    linearity_score = _clamp(
        0.55 * support_score + 0.30 * aspect_score + 0.15 * thin_score,
        0.0,
        1.0,
    )
    curvy_wall_like = (
        extent_m >= preset.curvy_wall_min_extent_m
        and area_m2 >= preset.min_component_area_m2
        and aspect >= preset.curvy_wall_min_aspect
        and fill_ratio <= preset.curvy_wall_max_fill_ratio
        and (
            (wall_score >= 0.34 and linearity_score < 0.58)
            or (support_score >= preset.curvy_wall_min_support and not straight_wall_like)
        )
    )
    render_as_object = (
        not straight_wall_like
        and not curvy_wall_like
        and extent_m <= preset.object_max_extent_m
        and area_m2 <= preset.object_max_area_m2
    )

    return {
        "wall_like": straight_wall_like,
        "curvy_wall_like": curvy_wall_like,
        "render_as_object": render_as_object,
        "wall_score": float(_clamp(wall_score, 0.0, 1.0)),
        "linearity_score": float(linearity_score),
        "aspect_ratio": float(aspect),
        "extent_m": float(extent_m),
        "area_m2": float(area_m2),
        "fill_ratio": float(fill_ratio),
        "support_score": float(support_score),
    }


def extract_wall_segments_hough(
    component: Dict[str, Any],
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    mask = component.get("crop_mask")
    if mask is None or not np.any(mask):
        return []

    # Thin the mask to a single-pixel skeleton before Hough detection.
    # This collapses thick SLAM walls (which appear as 3-10px bands) to a
    # centerline, so Hough detects one line per physical wall instead of
    # two parallel lines from both edges. Falls back to the original mask
    # if skeletonization is unavailable (no opencv-contrib ximgproc).
    hough_mask = _thin_mask(mask)
    if not np.any(hough_mask):
        hough_mask = mask

    min_length_px = max(6, _meters_to_px_int(preset.hough_min_length_m, resolution, min_px=6))
    max_gap_px = max(2, _meters_to_px_int(preset.hough_max_gap_m, resolution, min_px=2))
    lines = cv2.HoughLinesP(
        hough_mask,
        rho=1.0,
        theta=np.pi / 180.0,
        threshold=int(preset.hough_threshold),
        minLineLength=int(min_length_px),
        maxLineGap=int(max_gap_px),
    )
    if lines is None:
        return []

    crop_x = int(component.get("crop_x", 0))
    crop_y = int(component.get("crop_y", 0))
    source_score = float(component.get("wall_score", 0.0))
    segments: List[Dict[str, Any]] = []
    for line in lines:
        x1, y1, x2, y2 = [float(v) for v in line[0]]
        seg = _make_line_dict(
            x1 + crop_x,
            y1 + crop_y,
            x2 + crop_x,
            y2 + crop_y,
            source_score=source_score,
        )
        if seg is None:
            continue
        if seg["length_px"] < float(min_length_px):
            continue
        segments.append(seg)
    return segments


def cluster_segments_union_find(
    segments: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
) -> List[List[Dict[str, Any]]]:
    return _cluster_items(
        list(segments),
        lambda a, b: _segment_cluster_relation(a, b, resolution, preset),
    )


def fit_representative_lines(
    groups: Sequence[Sequence[Dict[str, Any]]],
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    representatives: List[Dict[str, Any]] = []
    fit_error_limit_px = max(1.0, 0.08 / resolution)
    length_ref_m = 1.2

    for group in groups:
        if not group:
            continue

        endpoints: List[Tuple[float, float]] = []
        weights: List[float] = []
        source_scores: List[float] = []
        total_support = 0.0
        for item in group:
            length_px = float(item.get("length_px", 1.0))
            endpoints.append((float(item["x1"]), float(item["y1"])))
            endpoints.append((float(item["x2"]), float(item["y2"])))
            weights.extend((length_px, length_px))
            source_scores.append(float(item.get("source_score", 0.0)))
            total_support += float(item.get("support_length_px", length_px))

        pts = np.asarray(endpoints, dtype=np.float32)
        wts = np.asarray(weights, dtype=np.float32)
        centroid = np.average(pts, axis=0, weights=wts)
        centered = pts - centroid
        cov = (centered * wts[:, None]).T @ centered / max(float(wts.sum()), 1.0)
        eigvals, eigvecs = np.linalg.eigh(cov)
        axis = eigvecs[:, int(np.argmax(eigvals))]
        if axis[0] < 0.0 or (abs(axis[0]) < 1e-6 and axis[1] < 0.0):
            axis = -axis
        projections = centered @ axis
        p1 = centroid + axis * float(np.min(projections))
        p2 = centroid + axis * float(np.max(projections))
        rep = _make_line_dict(
            float(p1[0]),
            float(p1[1]),
            float(p2[0]),
            float(p2[1]),
            source_score=float(np.mean(source_scores) if source_scores else 0.0),
            support_length_px=total_support,
            cluster_size=len(group),
        )
        if rep is None:
            continue

        normal = np.array([-axis[1], axis[0]], dtype=np.float32)
        residual = float(np.mean(np.abs(centered @ normal)))
        coverage_ratio = min(1.5, total_support / max(rep["length_px"], 1.0))
        length_score = _clamp((rep["length_px"] * resolution) / length_ref_m, 0.0, 1.0)
        support_score = _clamp(coverage_ratio / 1.0, 0.0, 1.0)
        fit_error_score = _clamp(1.0 - residual / fit_error_limit_px, 0.0, 1.0)
        rep["coverage_ratio"] = float(coverage_ratio)
        rep["fit_error_px"] = float(residual)
        rep["confidence"] = float(
            _clamp(
                0.35 * length_score
                + 0.30 * support_score
                + 0.20 * fit_error_score
                + 0.15 * float(rep.get("source_score", 0.0)),
                0.0,
                1.0,
            )
        )
        representatives.append(rep)

    return representatives


def deduplicate_parallel_lines(
    representatives: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    groups = _cluster_items(
        list(representatives),
        lambda a, b: _representative_dedupe_relation(a, b, resolution, preset),
    )
    return fit_representative_lines(groups, resolution, preset)


def bridge_line_gaps(
    representatives: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    groups = _cluster_items(
        list(representatives),
        lambda a, b: _representative_bridge_relation(a, b, resolution, preset),
    )
    return fit_representative_lines(groups, resolution, preset)


def snap_representative_lines(
    representatives: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    snap_basis_deg = _estimate_snap_basis_deg(representatives)
    snap_targets = (snap_basis_deg % 180.0, (snap_basis_deg + 90.0) % 180.0)
    snapped: List[Dict[str, Any]] = []
    for rep in representatives:
        current = dict(rep)
        length_m = float(rep.get("length_px", 0.0)) * resolution
        confidence = float(rep.get("confidence", 0.0))
        if length_m < preset.snap_min_length_m or confidence < preset.snap_min_confidence:
            snapped.append(current)
            continue

        angle = float(rep.get("angle_deg", 0.0)) % 180.0
        snap_target = None
        snap_delta = 180.0
        for candidate in snap_targets:
            delta = abs(angle - candidate)
            delta = min(delta, 180.0 - delta)
            if delta < snap_delta:
                snap_target = candidate
                snap_delta = delta

        if snap_target is None or snap_delta > preset.snap_angle_deg:
            snapped.append(current)
            continue

        midpoint = np.array([float(rep["mid_x"]), float(rep["mid_y"])], dtype=np.float32)
        half_len = float(rep.get("length_px", 0.0)) * 0.5
        snapped_angle_rad = math.radians(snap_target)
        axis = np.array([math.cos(snapped_angle_rad), math.sin(snapped_angle_rad)], dtype=np.float32)
        start = midpoint - axis * half_len
        end = midpoint + axis * half_len
        updated = _make_line_dict(
            float(start[0]),
            float(start[1]),
            float(end[0]),
            float(end[1]),
            source_score=float(rep.get("source_score", 0.0)),
            support_length_px=float(rep.get("support_length_px", rep.get("length_px", 0.0))),
            cluster_size=int(rep.get("cluster_size", 1)),
        )
        if updated is None:
            snapped.append(current)
            continue
        updated["confidence"] = float(rep.get("confidence", 0.0))
        updated["coverage_ratio"] = float(rep.get("coverage_ratio", 0.0))
        updated["fit_error_px"] = float(rep.get("fit_error_px", 0.0))
        updated["snapped"] = True
        updated["snap_basis_deg"] = float(snap_basis_deg)
        snapped.append(updated)

    return snapped


def filter_representative_lines(
    representatives: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    snap_basis_deg = _estimate_snap_basis_deg(representatives)
    filtered: List[Dict[str, Any]] = []
    for rep in representatives:
        length_m = float(rep.get("length_px", 0.0)) * resolution
        confidence = float(rep.get("confidence", 0.0))
        is_snapped = bool(rep.get("snapped"))
        base_min_length = preset.render_min_snapped_length_m if is_snapped else preset.render_min_length_m

        # Confidence-aware length gate: short walls need higher confidence
        # to survive. This prevents noise fragments from being promoted to
        # walls while still keeping real short dividers that have strong
        # geometric support.
        confident_length_m = base_min_length * 2.0
        if length_m < base_min_length:
            continue
        if length_m < confident_length_m and confidence < 0.40:
            continue

        angle = float(rep.get("angle_deg", 0.0)) % 180.0
        off_axis = min(
            _angle_diff_deg(angle, snap_basis_deg),
            _angle_diff_deg(angle, (snap_basis_deg + 90.0) % 180.0),
        )
        if off_axis > preset.offaxis_keep_angle_deg and length_m < preset.offaxis_min_length_m:
            continue
        filtered.append(rep)
    return filtered


def prune_isolated_lines(
    representatives: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    if not representatives:
        return []

    short_threshold = float(max(0.0, preset.isolation_short_length_m))
    neighbor_limit = float(max(0.0, preset.isolation_neighbor_m))
    if short_threshold <= 0.0 or neighbor_limit <= 0.0:
        return list(representatives)

    kept: List[Dict[str, Any]] = []
    for index, rep in enumerate(representatives):
        length_m = float(rep.get("length_px", 0.0)) * resolution
        if length_m >= short_threshold:
            kept.append(rep)
            continue

        mid_x = float(rep.get("mid_x", 0.0))
        mid_y = float(rep.get("mid_y", 0.0))
        confidence = float(rep.get("confidence", 0.0))

        # A short wall needs a structurally significant neighbor to survive.
        # Proximity to another short/weak wall alone is not enough — we
        # require at least one neighbor that is either long or confident.
        best_neighbor_strength = 0.0
        for other_index, other in enumerate(representatives):
            if index == other_index:
                continue
            dx = mid_x - float(other.get("mid_x", 0.0))
            dy = mid_y - float(other.get("mid_y", 0.0))
            dist_m = math.hypot(dx, dy) * resolution
            if dist_m > neighbor_limit:
                continue
            other_length_m = float(other.get("length_px", 0.0)) * resolution
            other_confidence = float(other.get("confidence", 0.0))
            # Neighbor strength: longer and closer neighbors count more
            proximity_factor = _clamp(1.0 - dist_m / neighbor_limit, 0.0, 1.0)
            strength = (
                0.6 * _clamp(other_length_m / max(short_threshold, 0.01), 0.0, 1.5)
                + 0.4 * other_confidence
            ) * (0.5 + 0.5 * proximity_factor)
            if strength > best_neighbor_strength:
                best_neighbor_strength = strength

        # Short + high confidence lines need weaker neighbors to survive;
        # short + low confidence lines need strong neighbors.
        required_strength = _clamp(0.45 - 0.3 * confidence, 0.15, 0.50)
        if best_neighbor_strength >= required_strength:
            kept.append(rep)
    return kept


def consolidate_major_walls(
    representatives: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
) -> List[Dict[str, Any]]:
    if not representatives:
        return []

    basis_deg = _estimate_snap_basis_deg(representatives)
    axis_defs: List[Tuple[np.ndarray, np.ndarray]] = []
    for angle_deg in (basis_deg % 180.0, (basis_deg + 90.0) % 180.0):
        angle_rad = math.radians(angle_deg)
        axis = np.array([math.cos(angle_rad), math.sin(angle_rad)], dtype=np.float32)
        normal = np.array([-axis[1], axis[0]], dtype=np.float32)
        axis_defs.append((axis, normal))

    offset_limit_px = max(2.0, preset.structural_cluster_sep_m / resolution)
    merge_gap_px = max(4.0, preset.structural_merge_gap_m / resolution)
    structural_min_length_m = max(0.0, float(preset.structural_min_length_m))
    axis_tol_deg = max(0.0, float(preset.structural_axis_tolerance_deg))

    passthrough: List[Dict[str, Any]] = []
    family_items: List[List[Dict[str, Any]]] = [[], []]

    for rep in representatives:
        angle = float(rep.get("angle_deg", 0.0)) % 180.0
        length_m = float(rep.get("length_px", 0.0)) * resolution
        diffs = [
            _angle_diff_deg(angle, basis_deg % 180.0),
            _angle_diff_deg(angle, (basis_deg + 90.0) % 180.0),
        ]
        family_idx = int(np.argmin(diffs))
        if diffs[family_idx] > axis_tol_deg or length_m < structural_min_length_m:
            passthrough.append(rep)
            continue

        axis, normal = axis_defs[family_idx]
        p1 = np.array([float(rep["x1"]), float(rep["y1"])], dtype=np.float32)
        p2 = np.array([float(rep["x2"]), float(rep["y2"])], dtype=np.float32)
        proj1 = float(np.dot(p1, axis))
        proj2 = float(np.dot(p2, axis))
        start = min(proj1, proj2)
        end = max(proj1, proj2)
        offset = float(np.dot(np.array([float(rep["mid_x"]), float(rep["mid_y"])], dtype=np.float32), normal))
        family_items[family_idx].append(
            {
                "line": rep,
                "offset": offset,
                "start": start,
                "end": end,
                "weight": max(float(rep.get("length_px", 0.0)), 1.0),
            }
        )

    consolidated: List[Dict[str, Any]] = []
    for family_idx, items in enumerate(family_items):
        if not items:
            continue
        items.sort(key=lambda item: item["offset"])
        offset_groups: List[List[Dict[str, Any]]] = []
        current_group: List[Dict[str, Any]] = []
        current_offset = 0.0
        current_weight = 0.0
        for item in items:
            if not current_group:
                current_group = [item]
                current_offset = item["offset"]
                current_weight = item["weight"]
                continue
            if abs(item["offset"] - current_offset) <= offset_limit_px:
                current_group.append(item)
                total_weight = current_weight + item["weight"]
                if total_weight > 0.0:
                    current_offset = (
                        current_offset * current_weight + item["offset"] * item["weight"]
                    ) / total_weight
                current_weight = total_weight
            else:
                offset_groups.append(current_group)
                current_group = [item]
                current_offset = item["offset"]
                current_weight = item["weight"]
        if current_group:
            offset_groups.append(current_group)

        axis, normal = axis_defs[family_idx]
        for group in offset_groups:
            sorted_group = sorted(group, key=lambda item: item["start"])
            active_start = float(sorted_group[0]["start"])
            active_end = float(sorted_group[0]["end"])
            weighted_offset = float(sorted_group[0]["offset"] * sorted_group[0]["weight"])
            total_weight = float(sorted_group[0]["weight"])
            source_scores = [float(sorted_group[0]["line"].get("confidence", 0.0))]
            support_length = float(sorted_group[0]["line"].get("support_length_px", sorted_group[0]["line"].get("length_px", 0.0)))
            cluster_size = int(sorted_group[0]["line"].get("cluster_size", 1))

            def _flush_current() -> None:
                nonlocal active_start, active_end, weighted_offset, total_weight, source_scores, support_length, cluster_size
                if active_end <= active_start:
                    return
                offset = weighted_offset / max(total_weight, 1.0)
                p_start = axis * active_start + normal * offset
                p_end = axis * active_end + normal * offset
                merged = _make_line_dict(
                    float(p_start[0]),
                    float(p_start[1]),
                    float(p_end[0]),
                    float(p_end[1]),
                    source_score=float(sum(source_scores) / max(len(source_scores), 1)),
                    support_length_px=float(max(support_length, active_end - active_start)),
                    cluster_size=int(max(cluster_size, len(source_scores))),
                )
                if merged is not None:
                    merged["confidence"] = float(max(source_scores) if source_scores else 0.0)
                    merged["snapped"] = True
                    merged["snap_basis_deg"] = float(basis_deg)
                    consolidated.append(merged)

            for item in sorted_group[1:]:
                if item["start"] <= active_end + merge_gap_px:
                    active_end = max(active_end, float(item["end"]))
                    weighted_offset += float(item["offset"] * item["weight"])
                    total_weight += float(item["weight"])
                    source_scores.append(float(item["line"].get("confidence", 0.0)))
                    support_length += float(item["line"].get("support_length_px", item["line"].get("length_px", 0.0)))
                    cluster_size += int(item["line"].get("cluster_size", 1))
                else:
                    _flush_current()
                    active_start = float(item["start"])
                    active_end = float(item["end"])
                    weighted_offset = float(item["offset"] * item["weight"])
                    total_weight = float(item["weight"])
                    source_scores = [float(item["line"].get("confidence", 0.0))]
                    support_length = float(item["line"].get("support_length_px", item["line"].get("length_px", 0.0)))
                    cluster_size = int(item["line"].get("cluster_size", 1))
            _flush_current()

    return filter_representative_lines(consolidated + passthrough, resolution, preset)


def render_wall_lines(
    output: np.ndarray,
    lines: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
    *,
    marker_type: str,
    dot_radius: int,
    dot_color: Tuple[int, int, int],
) -> None:
    for line in lines:
        length_px = float(line.get("length_px", 0.0))
        spacing_px = _adaptive_spacing(length_px, preset)
        xs, ys = _sample_line_points(
            float(line["x1"]),
            float(line["y1"]),
            float(line["x2"]),
            float(line["y2"]),
            spacing_px,
        )
        _draw_sampled_points(output, xs, ys, dot_radius, marker_type, dot_color)


def render_curvy_wall_contours(
    output: np.ndarray,
    components: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
    *,
    marker_type: str,
    dot_radius: int,
    dot_color: Tuple[int, int, int],
) -> None:
    min_arc_px = max(8.0, preset.curvy_wall_min_arc_length_m / resolution)
    simplify_px = max(1.2, float(preset.curvy_wall_simplify_px))

    for component in components:
        mask = component.get("crop_mask")
        if mask is None or not np.any(mask):
            continue

        skeleton = _thin_mask(np.asarray(mask, dtype=np.uint8))
        if skeleton is None or not np.any(skeleton):
            continue

        contours, _ = cv2.findContours(skeleton, cv2.RETR_LIST, cv2.CHAIN_APPROX_NONE)
        origin_x = int(component.get("crop_x", 0))
        origin_y = int(component.get("crop_y", 0))
        for contour in contours:
            if contour is None or len(contour) < 2:
                continue
            arc = cv2.arcLength(contour, closed=False)
            if arc < min_arc_px:
                continue
            simplified = cv2.approxPolyDP(contour, simplify_px, closed=False)
            if simplified is None or len(simplified) < 2:
                continue
            pts = simplified[:, 0, :].astype(np.float32)
            pts[:, 0] += origin_x
            pts[:, 1] += origin_y
            for idx in range(len(pts) - 1):
                p1 = pts[idx]
                p2 = pts[idx + 1]
                seg_len = float(np.linalg.norm(p2 - p1))
                if seg_len < 1.0:
                    continue
                xs, ys = _sample_line_points(
                    float(p1[0]),
                    float(p1[1]),
                    float(p2[0]),
                    float(p2[1]),
                    max(2, min(6, int(round(seg_len * 0.06)))),
                )
                _draw_sampled_points(output, xs, ys, dot_radius, marker_type, dot_color)


def render_object_contours(
    output: np.ndarray,
    components: Sequence[Dict[str, Any]],
    resolution: float,
    preset: DottedPreset,
    *,
    marker_type: str,
    dot_radius: int,
    dot_color: Tuple[int, int, int],
) -> None:
    min_arc_px = max(4.0, preset.min_arc_length_m / resolution)
    simplify_px = max(1.0, float(preset.object_simplify_px))

    for component in components:
        mask = component.get("mask")
        if mask is None or not np.any(mask):
            continue
        contours, _ = cv2.findContours(mask, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
        origin_x = int(component.get("x", 0))
        origin_y = int(component.get("y", 0))
        for contour in contours:
            arc = cv2.arcLength(contour, closed=True)
            if arc < min_arc_px:
                continue
            simplified = cv2.approxPolyDP(contour, simplify_px, closed=True)
            if simplified is None or len(simplified) < 2:
                continue
            pts = simplified[:, 0, :].astype(np.float32)
            pts[:, 0] += origin_x
            pts[:, 1] += origin_y
            for idx in range(len(pts)):
                p1 = pts[idx]
                p2 = pts[(idx + 1) % len(pts)]
                seg_len = float(np.linalg.norm(p2 - p1))
                if seg_len < 1.0:
                    continue
                xs, ys = _sample_line_points(
                    float(p1[0]),
                    float(p1[1]),
                    float(p2[0]),
                    float(p2[1]),
                    max(2, min(5, int(round(seg_len * 0.04)))),
                )
                _draw_sampled_points(output, xs, ys, dot_radius, marker_type, dot_color)


def _normalize_resolution(value: Optional[float], *, debug: bool = False) -> float:
    try:
        resolution = float(value)
    except Exception:
        resolution = 0.0
    if resolution > 0.0:
        return resolution
    if debug:
        print(
            f"[dotted_map] Missing or invalid map resolution, "
            f"falling back to {DEFAULT_MAP_RESOLUTION_M_PER_PX:.3f} m/px"
        )
    return DEFAULT_MAP_RESOLUTION_M_PER_PX


def _normalize_preset(raw: str) -> str:
    # All preset names now resolve to the same single configuration.
    value = str(raw or "balanced").strip().lower()
    return value if value in PRESETS else "balanced"


def _meters_to_px_int(
    meters: float,
    resolution: float,
    *,
    min_px: int = 1,
) -> int:
    if meters <= 0.0 or resolution <= 0.0:
        return int(min_px)
    return max(int(min_px), int(round(meters / resolution)))


def _kernel_from_pixels(radius_px: int, *, shape: int) -> Optional[np.ndarray]:
    radius = int(max(0, radius_px))
    if radius <= 0:
        return None
    size = int(radius * 2 + 1)
    return cv2.getStructuringElement(shape, (size, size))


def _kernel_from_meters(meters: float, resolution: float, *, shape: int) -> Optional[np.ndarray]:
    radius_px = _meters_to_px_int(meters, resolution, min_px=0)
    return _kernel_from_pixels(radius_px, shape=shape)


def _cluster_items(
    items: Sequence[Dict[str, Any]],
    relation,
) -> List[List[Dict[str, Any]]]:
    if not items:
        return []

    parent = list(range(len(items)))

    def find(idx: int) -> int:
        while parent[idx] != idx:
            parent[idx] = parent[parent[idx]]
            idx = parent[idx]
        return idx

    def union(a: int, b: int) -> None:
        ra = find(a)
        rb = find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            if relation(items[i], items[j]):
                union(i, j)

    groups: Dict[int, List[Dict[str, Any]]] = {}
    for idx, item in enumerate(items):
        root = find(idx)
        groups.setdefault(root, []).append(item)
    return list(groups.values())


def _segment_cluster_relation(
    a: Dict[str, Any],
    b: Dict[str, Any],
    resolution: float,
    preset: DottedPreset,
) -> bool:
    angle_diff = _angle_diff_deg(float(a["angle_deg"]), float(b["angle_deg"]))
    if angle_diff > preset.cluster_angle_deg:
        return False
    max_perp_px = max(2.0, preset.cluster_perp_m / resolution)
    max_gap_px = max(3.0, preset.cluster_gap_m / resolution)
    if not _line_pair_precheck(a, b, max_perp_px, max_gap_px):
        return False
    metrics = _line_pair_metrics(a, b, angle_diff_deg=angle_diff)
    return (
        metrics["lateral_sep_px"] <= max_perp_px
        and min(metrics["interval_gap_px"], metrics["endpoint_gap_px"]) <= max_gap_px
    )


def _representative_dedupe_relation(
    a: Dict[str, Any],
    b: Dict[str, Any],
    resolution: float,
    preset: DottedPreset,
) -> bool:
    angle_diff = _angle_diff_deg(float(a["angle_deg"]), float(b["angle_deg"]))
    if angle_diff > preset.dedupe_angle_deg:
        return False
    max_sep_px = max(1.5, preset.dedupe_sep_m / resolution)
    if not _line_pair_precheck(a, b, max_sep_px, max_sep_px):
        return False
    metrics = _line_pair_metrics(a, b, angle_diff_deg=angle_diff)
    return (
        metrics["overlap_ratio"] >= preset.dedupe_overlap_ratio
        and metrics["lateral_sep_px"] <= max_sep_px
    )


def _representative_bridge_relation(
    a: Dict[str, Any],
    b: Dict[str, Any],
    resolution: float,
    preset: DottedPreset,
) -> bool:
    angle_diff = _angle_diff_deg(float(a["angle_deg"]), float(b["angle_deg"]))
    if angle_diff > preset.bridge_angle_deg:
        return False
    max_gap_px = max(4.0, preset.bridge_gap_m / resolution)
    max_lateral_px = max(2.0, preset.bridge_lateral_m / resolution)
    if not _line_pair_precheck(a, b, max_lateral_px, max_gap_px):
        return False
    metrics = _line_pair_metrics(a, b, angle_diff_deg=angle_diff)
    return (
        metrics["interval_gap_px"] > 0.0
        and metrics["interval_gap_px"] <= max_gap_px
        and metrics["endpoint_gap_px"] <= max_gap_px
        and metrics["lateral_sep_px"] <= max_lateral_px
    )


def _line_pair_metrics(
    a: Dict[str, Any],
    b: Dict[str, Any],
    *,
    angle_diff_deg: Optional[float] = None,
) -> Dict[str, float]:
    angle_diff = (
        float(angle_diff_deg)
        if angle_diff_deg is not None
        else _angle_diff_deg(float(a["angle_deg"]), float(b["angle_deg"]))
    )

    a_ux = float(a["ux"])
    a_uy = float(a["uy"])
    b_ux = float(b["ux"])
    b_uy = float(b["uy"])
    if (a_ux * b_ux + a_uy * b_uy) < 0.0:
        b_ux = -b_ux
        b_uy = -b_uy

    axis_x = a_ux + b_ux
    axis_y = a_uy + b_uy
    axis_norm = math.hypot(axis_x, axis_y)
    if axis_norm < 1e-6:
        axis_x = a_ux
        axis_y = a_uy
    else:
        axis_x /= axis_norm
        axis_y /= axis_norm
    normal_x = -axis_y
    normal_y = axis_x

    a_x1 = float(a["x1"])
    a_y1 = float(a["y1"])
    a_x2 = float(a["x2"])
    a_y2 = float(a["y2"])
    b_x1 = float(b["x1"])
    b_y1 = float(b["y1"])
    b_x2 = float(b["x2"])
    b_y2 = float(b["y2"])

    a_proj1 = a_x1 * axis_x + a_y1 * axis_y
    a_proj2 = a_x2 * axis_x + a_y2 * axis_y
    b_proj1 = b_x1 * axis_x + b_y1 * axis_y
    b_proj2 = b_x2 * axis_x + b_y2 * axis_y
    a_min = min(a_proj1, a_proj2)
    a_max = max(a_proj1, a_proj2)
    b_min = min(b_proj1, b_proj2)
    b_max = max(b_proj1, b_proj2)

    overlap_len = max(0.0, min(a_max, b_max) - max(a_min, b_min))
    min_len = max(1.0, min(float(a.get("length_px", 1.0)), float(b.get("length_px", 1.0))))
    overlap_ratio = overlap_len / min_len
    interval_gap = 0.0 if overlap_len > 0.0 else max(a_min - b_max, b_min - a_max, 0.0)

    mid_dx = float(b["mid_x"]) - float(a["mid_x"])
    mid_dy = float(b["mid_y"]) - float(a["mid_y"])
    lateral_sep = abs(mid_dx * normal_x + mid_dy * normal_y)

    endpoint_gap = min(
        math.hypot(a_x1 - b_x1, a_y1 - b_y1),
        math.hypot(a_x1 - b_x2, a_y1 - b_y2),
        math.hypot(a_x2 - b_x1, a_y2 - b_y1),
        math.hypot(a_x2 - b_x2, a_y2 - b_y2),
    )

    return {
        "angle_diff_deg": float(angle_diff),
        "lateral_sep_px": float(lateral_sep),
        "overlap_len_px": float(overlap_len),
        "overlap_ratio": float(overlap_ratio),
        "interval_gap_px": float(interval_gap),
        "endpoint_gap_px": float(endpoint_gap),
    }


def _line_pair_precheck(
    a: Dict[str, Any],
    b: Dict[str, Any],
    max_lateral_px: float,
    max_gap_px: float,
) -> bool:
    mid_dx = float(b["mid_x"]) - float(a["mid_x"])
    mid_dy = float(b["mid_y"]) - float(a["mid_y"])
    reach = 0.5 * (float(a.get("length_px", 0.0)) + float(b.get("length_px", 0.0))) + max_gap_px
    max_mid_dist = reach + max_lateral_px
    if (mid_dx * mid_dx + mid_dy * mid_dy) > (max_mid_dist * max_mid_dist):
        return False

    lateral_sep = abs(mid_dx * (-float(a["uy"])) + mid_dy * float(a["ux"]))
    if lateral_sep > max_lateral_px:
        return False

    axial_sep = abs(mid_dx * float(a["ux"]) + mid_dy * float(a["uy"]))
    return axial_sep <= reach


def _angle_diff_deg(a_deg: float, b_deg: float) -> float:
    angle_diff = abs(float(a_deg) - float(b_deg))
    if angle_diff > 90.0:
        angle_diff = 180.0 - angle_diff
    return float(angle_diff)


def _estimate_snap_basis_deg(representatives: Sequence[Dict[str, Any]]) -> float:
    if not representatives:
        return 0.0

    bins = np.zeros(90, dtype=np.float32)
    for rep in representatives:
        angle_mod = float(rep.get("angle_deg", 0.0)) % 90.0
        bin_idx = int(round(angle_mod)) % 90
        weight = max(1.0, float(rep.get("length_px", 0.0)))
        bins[bin_idx] += weight

    if not np.any(bins):
        return 0.0

    smoothed = np.zeros_like(bins)
    for idx in range(90):
        smoothed[idx] = bins[(idx - 1) % 90] + bins[idx] + bins[(idx + 1) % 90]
    dominant_idx = int(np.argmax(smoothed))
    return float(dominant_idx)


def _make_line_dict(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    *,
    source_score: float = 0.0,
    support_length_px: Optional[float] = None,
    cluster_size: int = 1,
) -> Optional[Dict[str, Any]]:
    dx = float(x2) - float(x1)
    dy = float(y2) - float(y1)
    length = float(math.hypot(dx, dy))
    if length < 1e-6:
        return None
    ux = dx / length
    uy = dy / length
    angle = math.degrees(math.atan2(dy, dx)) % 180.0
    return {
        "x1": float(x1),
        "y1": float(y1),
        "x2": float(x2),
        "y2": float(y2),
        "mid_x": float((float(x1) + float(x2)) * 0.5),
        "mid_y": float((float(y1) + float(y2)) * 0.5),
        "ux": float(ux),
        "uy": float(uy),
        "length_px": float(length),
        "angle_deg": float(angle),
        "source_score": float(source_score),
        "support_length_px": float(support_length_px if support_length_px is not None else length),
        "cluster_size": int(cluster_size),
        "confidence": float(source_score),
    }


def _adaptive_spacing(length_px: float, preset: DottedPreset) -> int:
    return int(
        max(
            preset.spacing_min_px,
            min(
                preset.spacing_max_px,
                round(float(length_px) * 0.02),
            ),
        )
    )


def _sample_line_points(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    spacing_px: int,
) -> Tuple[np.ndarray, np.ndarray]:
    spacing = max(1, int(spacing_px))
    length = float(math.hypot(x2 - x1, y2 - y1))
    if length < 1.0:
        return (
            np.asarray([int(round(x1))], dtype=np.int32),
            np.asarray([int(round(y1))], dtype=np.int32),
        )
    steps = max(1, int(length / spacing))
    ts = np.linspace(0.0, 1.0, steps + 1, dtype=np.float32)
    xs = np.rint(x1 + (x2 - x1) * ts).astype(np.int32)
    ys = np.rint(y1 + (y2 - y1) * ts).astype(np.int32)
    return xs, ys


def _thin_mask(mask: np.ndarray) -> np.ndarray:
    src = np.uint8(mask > 0) * 255
    if not np.any(src):
        return src
    if hasattr(cv2, "ximgproc") and hasattr(cv2.ximgproc, "thinning"):
        return cv2.ximgproc.thinning(src)
    return src


_STAMP_CACHE: Dict[Tuple[str, int], np.ndarray] = {}


def _draw_sampled_points(
    img: np.ndarray,
    xs: np.ndarray,
    ys: np.ndarray,
    dot_radius: int,
    marker_type: str,
    color: Tuple[int, int, int],
) -> None:
    if xs.size == 0 or ys.size == 0:
        return

    h, w = img.shape[:2]
    valid = (xs >= 0) & (xs < w) & (ys >= 0) & (ys < h)
    if not np.any(valid):
        return

    xs = xs[valid]
    ys = ys[valid]
    if dot_radius <= 1 and marker_type == "circle":
        img[ys, xs] = color
        return

    stamp = _marker_stamp(marker_type, dot_radius)
    sh, sw = stamp.shape
    half_h = sh // 2
    half_w = sw // 2
    for x, y in zip(xs.tolist(), ys.tolist()):
        x0 = max(0, x - half_w)
        y0 = max(0, y - half_h)
        x1 = min(w, x0 + sw)
        y1 = min(h, y0 + sh)

        stamp_x0 = max(0, half_w - x)
        stamp_y0 = max(0, half_h - y)
        stamp_x1 = stamp_x0 + (x1 - x0)
        stamp_y1 = stamp_y0 + (y1 - y0)
        mask = stamp[stamp_y0:stamp_y1, stamp_x0:stamp_x1]
        if mask.size == 0:
            continue
        region = img[y0:y1, x0:x1]
        region[mask > 0] = color


def _marker_stamp(marker_type: str, dot_radius: int) -> np.ndarray:
    radius = max(1, int(dot_radius))
    key = (str(marker_type or "circle"), radius)
    cached = _STAMP_CACHE.get(key)
    if cached is not None:
        return cached

    size = radius * 2 + 1
    stamp = np.zeros((size, size), dtype=np.uint8)
    center = radius
    if marker_type == "plus":
        stamp[center, :] = 255
        stamp[:, center] = 255
    else:
        yy, xx = np.ogrid[:size, :size]
        mask = (xx - center) ** 2 + (yy - center) ** 2 <= radius ** 2
        stamp[mask] = 255
    _STAMP_CACHE[key] = stamp
    return stamp


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))
