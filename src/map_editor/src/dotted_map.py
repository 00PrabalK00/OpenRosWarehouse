"""
Geometry-aware dotted map visualization layer.

Pipeline:
1. Threshold + morphological clean-up to isolate true wall pixels
2. Adaptive blob filter — removes debris relative to largest wall blob
3. LINE SEGMENT DETECTOR (LSD) — finds straight geometric segments in the
   cleaned mask; dots are placed along perfectly straight lines, giving sharp
   corner handling and eliminating the pixel-path zigzag of plain contour walking
4. Contour fallback — any occupied pixels not covered by an LSD segment (curved
   walls, irregular shapes) are handled by the original contour-sampling method
5. User-drawn wall lines — extra segments supplied by the caller (from the map
   editor's line-drawing tool) are dotted on top

Result: solid wall lines → clean dotted geometric outlines.
Navigation/SLAM data is never modified — display only.
"""

import cv2
import numpy as np
from PIL import Image


def generate_dotted_map(
    pil_image: Image.Image,
    spacing: int = 3,
    dot_radius: int = 1,
    marker_type: str = 'circle',
    occupied_thresh: int = 127,
    thin_walls: bool = False,
    min_arc_length: float = 10.0,
    noise_open_size: int = 2,
    gap_close_size: int = 4,
    blob_keep_fraction: float = 0.03,
    use_lsd: bool = True,
    extra_lines: list = None,
    bg_color: tuple = (240, 240, 240),
    dot_color: tuple = (30, 30, 30),
) -> Image.Image:
    """
    Convert a PIL occupancy map into a clean dotted wall display.

    Args:
        pil_image:           Grayscale (or RGB) occupancy map.
        spacing:             Pixels between dot markers along a wall.
        dot_radius:          Circle radius or plus arm length in pixels.
        marker_type:         'circle' or 'plus'.
        occupied_thresh:     Pixels strictly below this → occupied wall.
        thin_walls:          Skeletonize walls to 1-pixel before sampling.
        min_arc_length:      Drop segments/contours shorter than this (pixels).
        noise_open_size:     Opening kernel — removes lone noise pixels.
        gap_close_size:      Closing kernel — bridges small wall gaps.
        blob_keep_fraction:  Keep blobs >= this fraction of largest blob (adaptive).
        use_lsd:             Use Line Segment Detector for geometry-aware dot placement.
        extra_lines:         List of (x1,y1,x2,y2) pixel-coord line segments to dot
                             on top (from the map editor's line-drawing tool).
        bg_color:            RGB background.
        dot_color:           RGB dot colour.

    Returns:
        RGB PIL Image — dotted wall display layer.
    """
    gray = np.array(pil_image.convert('L'))
    height, width = gray.shape

    # ── 1. Threshold ────────────────────────────────────────────────────────
    occupied = np.where(gray < occupied_thresh, np.uint8(255), np.uint8(0))

    # ── 2. Morphological clean-up ───────────────────────────────────────────
    if noise_open_size > 0:
        k = cv2.getStructuringElement(cv2.MORPH_RECT, (noise_open_size, noise_open_size))
        occupied = cv2.morphologyEx(occupied, cv2.MORPH_OPEN, k)

    if gap_close_size > 0:
        k = cv2.getStructuringElement(cv2.MORPH_RECT, (gap_close_size, gap_close_size))
        occupied = cv2.morphologyEx(occupied, cv2.MORPH_CLOSE, k)

    # ── 3. Adaptive blob filter ─────────────────────────────────────────────
    n_labels, labels, stats, _ = cv2.connectedComponentsWithStats(occupied, connectivity=8)
    if n_labels > 1:
        max_area = float(max(stats[lbl, cv2.CC_STAT_AREA] for lbl in range(1, n_labels)))
        area_cutoff = max(4.0, max_area * blob_keep_fraction)
        clean = np.zeros_like(occupied)
        for lbl in range(1, n_labels):
            if stats[lbl, cv2.CC_STAT_AREA] >= area_cutoff:
                clean[labels == lbl] = 255
        occupied = clean

    if thin_walls:
        occupied = cv2.ximgproc.thinning(occupied, thinningType=cv2.ximgproc.THINNING_ZHANGSUEN)

    output = np.full((height, width, 3), bg_color, dtype=np.uint8)

    # ── 4. LSD geometry-aware dot placement ────────────────────────────────
    # Track which pixels are covered by an LSD segment so the contour
    # fallback only handles what LSD missed.
    lsd_coverage = np.zeros((height, width), dtype=np.uint8)

    if use_lsd and occupied.any():
        lsd = cv2.createLineSegmentDetector(cv2.LSD_REFINE_STD)
        detected, _, _, _ = lsd.detect(occupied)

        if detected is not None and len(detected) > 0:
            for seg in detected:
                x1, y1, x2, y2 = seg[0]
                seg_len = np.hypot(x2 - x1, y2 - y1)
                if seg_len < min_arc_length:
                    continue

                # Dot placement: evenly spaced along the true geometric line
                n_steps = max(1, int(seg_len / spacing))
                for i in range(n_steps + 1):
                    t = i / n_steps
                    px = int(round(x1 + t * (x2 - x1)))
                    py = int(round(y1 + t * (y2 - y1)))
                    _draw_marker(output, px, py, dot_radius, marker_type, dot_color)

                # Mark this segment's neighbourhood as covered (thick line in mask)
                cv2.line(lsd_coverage,
                         (int(round(x1)), int(round(y1))),
                         (int(round(x2)), int(round(y2))),
                         255, max(1, gap_close_size))

    # ── 5. Contour fallback for pixels not covered by LSD ──────────────────
    residual = cv2.bitwise_and(occupied, cv2.bitwise_not(lsd_coverage))
    contours, _ = cv2.findContours(residual, cv2.RETR_LIST, cv2.CHAIN_APPROX_NONE)

    for contour in contours:
        if cv2.arcLength(contour, closed=False) < min_arc_length:
            continue
        pts = contour[:, 0, :]
        for p in _sample_contour(pts, spacing):
            _draw_marker(output, int(p[0]), int(p[1]), dot_radius, marker_type, dot_color)

    # ── 6. User-drawn extra wall lines ──────────────────────────────────────
    if extra_lines:
        for (x1, y1, x2, y2) in extra_lines:
            seg_len = np.hypot(x2 - x1, y2 - y1)
            if seg_len < 1:
                continue
            n_steps = max(1, int(seg_len / spacing))
            for i in range(n_steps + 1):
                t = i / n_steps
                px = int(round(x1 + t * (x2 - x1)))
                py = int(round(y1 + t * (y2 - y1)))
                _draw_marker(output, px, py, dot_radius, marker_type, dot_color)

    return Image.fromarray(output, 'RGB')


def _sample_contour(pts: np.ndarray, spacing: int) -> list:
    sampled = [pts[0]]
    acc = 0.0
    last = pts[0].astype(float)
    for i in range(1, len(pts)):
        cur = pts[i].astype(float)
        acc += float(np.linalg.norm(cur - last))
        last = cur
        if acc >= spacing:
            sampled.append(pts[i])
            acc = 0.0
    return sampled


def _draw_marker(img: np.ndarray, x: int, y: int, size: int, marker_type: str, color: tuple):
    h, w = img.shape[:2]
    if x < 0 or x >= w or y < 0 or y >= h:
        return
    if marker_type == 'plus':
        cv2.line(img, (max(0, x - size), y), (min(w - 1, x + size), y), color, 1)
        cv2.line(img, (x, max(0, y - size)), (x, min(h - 1, y + size)), color, 1)
    else:
        cv2.circle(img, (x, y), max(1, size), color, -1)
