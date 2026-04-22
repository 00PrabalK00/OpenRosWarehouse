"""
Shelf Geometric Refiner + Validator
====================================
Inspired by: Zhao et al., "A Method for Autonomous Shelf Recognition and
  Docking of Mobile Robots Based on 2D LiDAR" (2024).

Architecture
------------
  /scan + /shelf/status_json  -->  [ShelfGeometricRefiner]  -->  /shelf/refined_status_json

The refiner takes the coarse pose from the existing intensity-based shelf
detector and runs the paper's full pipeline *locally* around that pose so it
acts as both a validator (does the LiDAR geometry actually look like a shelf?)
and a refiner (fit individual leg centres then optimise the full shelf pose).

Pipeline stages (faithfully follow the paper):
  B – Local ROI crop + adjacent-scan-point clustering + trailing-point
      removal + cluster size filter                          (section 3.1)
  C – Geometric pruning: pair-distance, Pythagorean, right-triangle tests
                                                             (section 3.2)
  D – Shelf model screening: compare measured L/W to known model
                                                             (section 3.2)
  E – Leg centre fitting: PCA principal direction + nonlinear least-squares
      per cluster                                            (section 3.3)
  F – Shelf pose fitting: NLS for shelf centre (cx, cy) and orientation θ
      using all fitted leg centres                           (section 3.4)

Additionally the node maintains a temporal history of refined poses for a
stability score analogous to the paper's repeated-recognition approach
(section 3.5).

Topics
------
  Sub: /scan                   sensor_msgs/LaserScan  (raw LiDAR)
  Sub: /shelf/status_json      std_msgs/String        (detector output)
  Pub: /shelf/refined_status_json  std_msgs/String    (enhanced JSON)

The published JSON is a strict superset of the detector's JSON so existing
consumers (zone_manager) work without change — just point them at the new
topic.
"""

import collections
import json
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
from scipy.optimize import least_squares

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import LaserScan
from std_msgs.msg import String
import tf2_ros
from geometry_msgs.msg import PointStamped
from tf2_geometry_msgs import do_transform_point


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _norm_angle(a: float) -> float:
    a = float(a) % (2.0 * math.pi)
    if a > math.pi:
        a -= 2.0 * math.pi
    return a


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class ShelfModel:
    """Physical dimensions of the target shelf (all in metres)."""
    length_m: float    # L  long axis (depth direction)
    width_m: float     # W  short axis (opening / width direction)
    leg_h_m: float     # Gh leg size aligned with shelf axis
    leg_w_m: float     # Gw leg size perpendicular to shelf axis
    is_circular: bool  # True for round tubular legs, False for square


@dataclass
class ScanPoint:
    """A single LiDAR return projected into base_link, with original metadata."""
    x: float       # base_link
    y: float       # base_link
    r: float       # range in laser frame (used for trailing filter)
    idx: int       # original scan index (used to preserve proximity order)


@dataclass
class LegCluster:
    """One candidate shelf-leg cluster after Stage B."""
    points: List[ScanPoint]
    centroid: Tuple[float, float]
    pca_angle: float                           # Stage E input
    fitted_center: Optional[Tuple[float, float]] = None
    fit_residual: float = float('inf')


@dataclass
class ValidationResult:
    validated: bool
    reason: str
    num_legs: int = 0
    leg_clusters: List[LegCluster] = field(default_factory=list)
    leg_centers: List[Tuple[float, float]] = field(default_factory=list)
    model_error: float = float('inf')


@dataclass
class RefinedPose:
    cx: float
    cy: float
    yaw: float
    num_legs: int
    model_error: float
    sum_leg_residual: float
    pose_residual: float


# ===========================================================================
# Stage B+C+D  –  ShelfLegValidator
# ===========================================================================

class ShelfLegValidator:
    """
    Stages B, C and D from the paper.

    B  – ROI crop, adjacent-point clustering, trailing-point removal,
         cluster size filter.
    C  – Geometric pruning: pair-distance, Pythagorean right-triangle.
    D  – Model screening: compare measured long/short sides to ShelfModel.
    """

    def __init__(
        self,
        model: ShelfModel,
        roi_radius_m: float = 0.75,
        cluster_adj_dist_m: float = 0.10,
        cluster_min_points: int = 2,
        cluster_max_extent_m: float = 0.20,
        trailing_min_beta_deg: float = 15.0,
        leg_size_tolerance_m: float = 0.12,
        model_length_tolerance_m: float = 0.18,
        model_width_tolerance_m: float = 0.18,
        min_legs: int = 3,
    ):
        self._model = model
        self._roi_r = roi_radius_m
        self._adj_dist = cluster_adj_dist_m
        self._min_pts = cluster_min_points
        self._max_ext = cluster_max_extent_m
        self._min_beta = math.radians(trailing_min_beta_deg)
        self._leg_tol = leg_size_tolerance_m
        self._tol_L = model_length_tolerance_m
        self._tol_W = model_width_tolerance_m
        self._min_legs = min_legs

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def validate(
        self,
        scan_pts: List[ScanPoint],
        rough_cx: float,
        rough_cy: float,
        rough_yaw: float,
        angle_increment: float,
    ) -> ValidationResult:
        """Run all validation stages and return a ValidationResult."""

        # --- Stage B1: ROI crop ---
        roi = [p for p in scan_pts
               if math.hypot(p.x - rough_cx, p.y - rough_cy) <= self._roi_r]
        if len(roi) < self._min_pts:
            return ValidationResult(False, f'roi_too_few_points ({len(roi)})')

        # --- Stage B2: adjacent-scan-point clustering ---
        raw_clusters = self._cluster_adjacent(roi, angle_increment)

        # --- Stage B3: trailing-point removal + size filter ---
        leg_clusters = []
        for cluster_pts in raw_clusters:
            cleaned = self._remove_trailing_points(cluster_pts, angle_increment)
            if len(cleaned) < self._min_pts:
                continue
            if not self._passes_size_filter(cleaned):
                continue
            centroid, pca_angle = self._pca(cleaned)
            leg_clusters.append(LegCluster(
                points=cleaned,
                centroid=centroid,
                pca_angle=pca_angle,
            ))

        if len(leg_clusters) < self._min_legs:
            return ValidationResult(
                False,
                f'too_few_leg_clusters ({len(leg_clusters)}) need {self._min_legs}',
            )

        # --- Stages C + D: geometric pruning + model screening ---
        centers = [c.centroid for c in leg_clusters]
        best_set, model_err = self._screen_candidate_sets(centers)

        if best_set is None:
            return ValidationResult(False, 'no_candidate_satisfies_shelf_geometry')

        chosen_clusters = [leg_clusters[i] for i in best_set]
        chosen_centers = [centers[i] for i in best_set]

        return ValidationResult(
            validated=True,
            reason='ok',
            num_legs=len(chosen_clusters),
            leg_clusters=chosen_clusters,
            leg_centers=chosen_centers,
            model_error=model_err,
        )

    # ------------------------------------------------------------------
    # Stage B2: adjacent-scan-point clustering
    # ------------------------------------------------------------------

    def _cluster_adjacent(
        self, pts: List[ScanPoint], angle_increment: float,
    ) -> List[List[ScanPoint]]:
        """
        Group scan points into clusters by adjacency in scan-index order.
        A new cluster starts when the geometric distance between consecutive
        (by scan index) points exceeds the threshold, or the index gap is
        large (meaning far-apart scan rays, not just NaN-filtered ones).
        """
        if not pts:
            return []
        sorted_pts = sorted(pts, key=lambda p: p.idx)

        # Max allowed scan-index gap before forcing a cluster break.
        # A gap of 3 allows through 1–2 NaN-filtered neighbour rays.
        max_idx_gap = max(3, int(math.ceil(0.04 / max(angle_increment, 1e-6))))

        clusters: List[List[ScanPoint]] = []
        current: List[ScanPoint] = [sorted_pts[0]]

        for i in range(1, len(sorted_pts)):
            prev, curr = sorted_pts[i - 1], sorted_pts[i]
            idx_gap = curr.idx - prev.idx
            d = math.hypot(curr.x - prev.x, curr.y - prev.y)
            if d <= self._adj_dist and idx_gap <= max_idx_gap:
                current.append(curr)
            else:
                clusters.append(current)
                current = [curr]
        clusters.append(current)
        return clusters

    # ------------------------------------------------------------------
    # Stage B3a: trailing-point removal  (paper eq. 1)
    # ------------------------------------------------------------------

    def _remove_trailing_points(
        self, pts: List[ScanPoint], angle_increment: float,
    ) -> List[ScanPoint]:
        """
        For each consecutive pair (A, B) in scan-index order:
          β = arctan( r_B·sin(α) / (r_A − r_B·cos(α)) )
        If β < threshold AND r_A > r_B → A is a trailing artifact, remove it.
        """
        if len(pts) <= 1:
            return list(pts)
        sorted_pts = sorted(pts, key=lambda p: p.idx)
        to_remove: set = set()

        for i in range(len(sorted_pts) - 1):
            a = sorted_pts[i]
            b = sorted_pts[i + 1]
            alpha = angle_increment * max(1, b.idx - a.idx)
            denom = a.r - b.r * math.cos(alpha)
            if abs(denom) < 1e-9:
                continue
            beta = math.atan2(b.r * math.sin(alpha), denom)
            if beta < self._min_beta and a.r > b.r:
                to_remove.add(i)

        return [p for i, p in enumerate(sorted_pts) if i not in to_remove]

    # ------------------------------------------------------------------
    # Stage B3b: cluster size filter
    # ------------------------------------------------------------------

    def _passes_size_filter(self, pts: List[ScanPoint]) -> bool:
        """
        Retain only clusters whose spatial extent is consistent with a shelf
        leg (neither too small nor too large).
        """
        if len(pts) < self._min_pts:
            return False
        xs = [p.x for p in pts]
        ys = [p.y for p in pts]
        extent = math.hypot(max(xs) - min(xs), max(ys) - min(ys))
        # Must be smaller than the configured max extent (rejecting walls etc.)
        if extent > self._max_ext:
            return False
        # Must be at least somewhat larger than a single noise point.
        # Require non-zero spread OR ≥ 3 coincident returns on a round leg.
        if extent < 0.005 and len(pts) < 3:
            return False
        return True

    # ------------------------------------------------------------------
    # PCA: principal direction of a cluster
    # ------------------------------------------------------------------

    @staticmethod
    def _pca(pts: List[ScanPoint]) -> Tuple[Tuple[float, float], float]:
        """
        Return (centroid, pca_angle) for a cluster.
        pca_angle is the angle (radians, base_link frame) of the principal
        component direction (longest axis of point distribution).
        """
        arr = np.array([[p.x, p.y] for p in pts], dtype=float)
        centroid = arr.mean(axis=0)
        if len(arr) < 2:
            return (float(centroid[0]), float(centroid[1])), 0.0
        centered = arr - centroid
        cov = (centered.T @ centered) / len(arr)
        try:
            U, _, _ = np.linalg.svd(cov)
            angle = math.atan2(float(U[1, 0]), float(U[0, 0]))
        except np.linalg.LinAlgError:
            angle = 0.0
        return (float(centroid[0]), float(centroid[1])), angle

    # ------------------------------------------------------------------
    # Stage C: geometric pruning
    # ------------------------------------------------------------------

    def _is_right_triangle(
        self,
        a: Tuple[float, float],
        b: Tuple[float, float],
        c: Tuple[float, float],
    ) -> Tuple[bool, float, float]:
        """
        Check whether (a, b, c) approximately form a right triangle with legs
        close to the shelf model's L and W.  Returns (ok, measured_long,
        measured_short) where long/short are the measured leg lengths.
        """
        d01 = math.hypot(a[0] - b[0], a[1] - b[1])
        d12 = math.hypot(b[0] - c[0], b[1] - c[1])
        d02 = math.hypot(a[0] - c[0], a[1] - c[1])
        sides = sorted([d01, d12, d02])
        d_s, d_m, d_h = sides  # short, medium, hypotenuse candidate

        # Pythagorean check: hypotenuse² ≈ short² + medium²
        pyth_ok = abs(d_h ** 2 - d_s ** 2 - d_m ** 2) / max(d_h ** 2, 1e-6) < 0.20

        if not pyth_ok:
            return False, 0.0, 0.0

        L, W = self._model.length_m, self._model.width_m
        tL, tW = self._tol_L, self._tol_W

        # The two legs (non-hypotenuse sides) should be close to (L, W).
        err_LW = min(
            abs(d_s - W) + abs(d_m - L),
            abs(d_s - L) + abs(d_m - W),
        )
        if err_LW > (tL + tW):
            return False, 0.0, 0.0

        long_side = max(d_s, d_m)
        short_side = min(d_s, d_m)
        return True, long_side, short_side

    # ------------------------------------------------------------------
    # Stage C+D: try all candidate sets + model screening
    # ------------------------------------------------------------------

    def _screen_candidate_sets(
        self,
        centers: List[Tuple[float, float]],
    ) -> Tuple[Optional[List[int]], float]:
        """
        Enumerate all C(n,4) then C(n,3) combinations.  For each, check
        the right-triangle / rectangle geometry (Stage C) and measure L/W
        fit error against the shelf model (Stage D).  Return the index set
        and model error for the best candidate, or (None, inf) if none pass.
        """
        n = len(centers)
        best_indices: Optional[List[int]] = None
        best_err = float('inf')

        # --- Try 4-leg rectangle candidates ---
        if n >= 4:
            from itertools import combinations
            for quad in combinations(range(n), 4):
                ok, err = self._check_rectangle(
                    [centers[i] for i in quad]
                )
                if ok and err < best_err:
                    best_err = err
                    best_indices = list(quad)

        if best_indices is not None:
            return best_indices, best_err

        # --- Fallback: 3-leg right-triangle candidates ---
        from itertools import combinations
        for triple in combinations(range(n), 3):
            ok, mL, mW = self._is_right_triangle(*[centers[i] for i in triple])
            if not ok:
                continue
            L, W = self._model.length_m, self._model.width_m
            err = abs(mL - max(L, W)) + abs(mW - min(L, W))
            if err < self._tol_L + self._tol_W and err < best_err:
                best_err = err
                best_indices = list(triple)

        return best_indices, best_err

    def _check_rectangle(
        self, pts: List[Tuple[float, float]],
    ) -> Tuple[bool, float]:
        """
        Check whether 4 points approximately form a rectangle with sides
        close to the shelf model (L, W).  Returns (ok, model_error).
        """
        from itertools import combinations, permutations

        # Try all 3 ways to split 4 points into 2 width-pairs.
        L, W = self._model.length_m, self._model.width_m
        tL, tW = self._tol_L, self._tol_W
        best_err = float('inf')

        pair_splits = [
            ((0, 1), (2, 3)),
            ((0, 2), (1, 3)),
            ((0, 3), (1, 2)),
        ]
        for (i0, i1), (i2, i3) in pair_splits:
            w1 = math.hypot(pts[i0][0] - pts[i1][0], pts[i0][1] - pts[i1][1])
            w2 = math.hypot(pts[i2][0] - pts[i3][0], pts[i2][1] - pts[i3][1])
            m1 = ((pts[i0][0] + pts[i1][0]) / 2, (pts[i0][1] + pts[i1][1]) / 2)
            m2 = ((pts[i2][0] + pts[i3][0]) / 2, (pts[i2][1] + pts[i3][1]) / 2)
            depth = math.hypot(m1[0] - m2[0], m1[1] - m2[1])

            # Both pairs should look like L or W, depth like the other.
            for measured_w, measured_L in [
                (min(w1, w2, depth), max(w1, w2, depth)),
                (max(w1, w2), depth) if depth < max(w1, w2) else (depth, max(w1, w2)),
            ]:
                err = min(
                    abs(measured_w - W) + abs(measured_L - L),
                    abs(measured_w - L) + abs(measured_L - W),
                )
                if err < tL + tW and err < best_err:
                    best_err = err

        ok = best_err < float('inf')
        return ok, best_err


# ===========================================================================
# Stage E + F  –  ShelfPoseRefiner
# ===========================================================================

class ShelfPoseRefiner:
    """
    Stages E and F from the paper.

    E  – Per-leg nonlinear least-squares to find the precise leg centre from
         the raw cluster points (using the PCA direction as the leg axis
         orientation).
    F  – Full shelf pose NLS: optimise (cx, cy, theta) so the 4 (or 3)
         expected shelf-leg corners best explain the fitted leg centres.
    """

    def __init__(self, model: ShelfModel):
        self._model = model

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def refine(
        self,
        validation: ValidationResult,
        rough_cx: float,
        rough_cy: float,
        rough_yaw: float,
    ) -> Optional[RefinedPose]:
        """
        Takes a validated leg set and returns a RefinedPose, or None if the
        optimisation fails.
        """
        # Stage E: fit each leg centre.
        fitted_centers: List[Tuple[float, float]] = []
        leg_residuals: List[float] = []
        for lc in validation.leg_clusters:
            fc, res = self._fit_leg_center(lc)
            if fc is not None:
                fitted_centers.append(fc)
                leg_residuals.append(res)
            else:
                # Fallback to PCA centroid.
                fitted_centers.append(lc.centroid)
                leg_residuals.append(float('inf'))

        if len(fitted_centers) < 3:
            return None

        # Stage F: fit shelf pose from refined leg centres.
        refined_pose = self._fit_shelf_pose(
            fitted_centers,
            rough_cx,
            rough_cy,
            rough_yaw,
        )
        return refined_pose

    # ------------------------------------------------------------------
    # Stage E: leg centre fitting
    # ------------------------------------------------------------------

    def _fit_leg_center(
        self, lc: LegCluster,
    ) -> Tuple[Optional[Tuple[float, float]], float]:
        """
        NLS to find (pox, poy) minimising Σ min(e1_i, e2_i)² for rectangular
        legs, or Σ(r_i − r_leg)² for circular legs (paper eqs. 5–7).
        Returns (centre, rms_residual).
        """
        pts_arr = np.array([[p.x, p.y] for p in lc.points], dtype=float)
        if len(pts_arr) < 2:
            return None, float('inf')

        x0 = np.array([lc.centroid[0], lc.centroid[1]], dtype=float)
        alpha = lc.pca_angle
        m = self._model

        if m.is_circular:
            r_leg = m.leg_h_m / 2.0

            def _circ_res(params):
                dx = pts_arr[:, 0] - params[0]
                dy = pts_arr[:, 1] - params[1]
                return np.sqrt(dx ** 2 + dy ** 2) - r_leg

        else:
            Gh2, Gw2 = m.leg_h_m / 2.0, m.leg_w_m / 2.0
            ca, sa = math.cos(alpha), math.sin(alpha)

            def _rect_res(params):
                dx = np.abs(pts_arr[:, 0] - params[0])
                dy = np.abs(pts_arr[:, 1] - params[1])
                # Paper eq. 5
                e1 = np.abs(dx * ca + dy * sa - Gh2)
                e2 = np.abs(dx * sa - dy * ca - Gw2)
                # Take the minimum (nearest face) per point.
                return np.minimum(e1, e2)

        fn = _circ_res if m.is_circular else _rect_res

        try:
            result = least_squares(
                fn, x0,
                method='trf',
                ftol=1e-5, xtol=1e-5, gtol=1e-5,
                max_nfev=100,
            )
            if not result.success and result.cost > 1e3:
                return None, float('inf')
            rms = float(np.sqrt(np.mean(result.fun ** 2)))
            return (float(result.x[0]), float(result.x[1])), rms
        except Exception:
            return None, float('inf')

    # ------------------------------------------------------------------
    # Stage F: shelf pose fitting  (paper eqs. 8–11)
    # ------------------------------------------------------------------

    def _expected_corners(
        self, cx: float, cy: float, ca: float,
    ) -> List[Tuple[float, float]]:
        """
        Four expected leg-centre positions for a shelf with centre (cx, cy)
        and heading ca, using the shelf model L × W.
        Equation from paper:
          u0x = cx − cos(ca)·L/2 + sin(ca)·W/2
          u0y = cy − sin(ca)·L/2 − cos(ca)·W/2
        The other three corners follow by flipping the ±L/2, ±W/2 signs.
        """
        cca, sca = math.cos(ca), math.sin(ca)
        L2, W2 = self._model.length_m / 2.0, self._model.width_m / 2.0
        corners = []
        for sl, sw in [(-1, -1), (1, -1), (1, 1), (-1, 1)]:
            cx_c = cx + sl * cca * L2 - sw * sca * W2
            cy_c = cy + sl * sca * L2 + sw * cca * W2
            corners.append((cx_c, cy_c))
        return corners

    def _fit_shelf_pose(
        self,
        leg_centers: List[Tuple[float, float]],
        rough_cx: float,
        rough_cy: float,
        rough_yaw: float,
    ) -> Optional[RefinedPose]:
        """
        NLS over (cx, cy, ca) minimising the Euclidean distance from each
        fitted leg centre to its nearest expected shelf corner.
        Initialised from the rough detector pose.
        """
        lc_arr = np.array(leg_centers, dtype=float)  # (n, 2)

        def _residuals(params):
            cx, cy, ca = params
            corners = np.array(self._expected_corners(cx, cy, ca), dtype=float)
            # For each observed leg, distance to nearest expected corner.
            res = []
            for obs in lc_arr:
                dists = np.linalg.norm(corners - obs, axis=1)
                res.append(float(dists.min()))
            return res

        x0 = np.array([rough_cx, rough_cy, rough_yaw], dtype=float)
        # Bound ca to  ±2π around the initialisation to avoid wrapping jumps.
        ca_lo = rough_yaw - math.pi
        ca_hi = rough_yaw + math.pi

        try:
            result = least_squares(
                _residuals, x0,
                bounds=([-np.inf, -np.inf, ca_lo], [np.inf, np.inf, ca_hi]),
                method='trf',
                ftol=1e-6, xtol=1e-6, gtol=1e-6,
                max_nfev=300,
            )
            pose_residual = float(np.sqrt(np.mean(np.array(result.fun) ** 2)))
            cx_r, cy_r, ca_r = float(result.x[0]), float(result.x[1]), float(result.x[2])
            ca_r = _norm_angle(ca_r)

            # Sanity check: refined pose should not jump far from rough pose.
            if math.hypot(cx_r - rough_cx, cy_r - rough_cy) > 0.40:
                return None

            return RefinedPose(
                cx=cx_r,
                cy=cy_r,
                yaw=ca_r,
                num_legs=len(leg_centers),
                model_error=float('inf'),   # filled by caller
                sum_leg_residual=float('inf'),  # filled by caller
                pose_residual=pose_residual,
            )
        except Exception:
            return None


# ===========================================================================
# Temporal tracker
# ===========================================================================

class _TemporalTracker:
    """Ring-buffer of recent refined poses; computes a stability score."""

    def __init__(self, window: int = 8):
        self._window = max(3, window)
        self._buf: collections.deque = collections.deque(maxlen=self._window)

    def add(self, cx: float, cy: float, yaw: float, stamp: float) -> None:
        self._buf.append({'cx': cx, 'cy': cy, 'yaw': yaw, 't': stamp})

    def reset(self) -> None:
        self._buf.clear()

    def stability_score(
        self,
        pos_tol_m: float = 0.025,
        yaw_tol_rad: float = math.radians(3.0),
    ) -> float:
        """
        Simple consistency score in [0, 1].  Full score when every sample in
        the window is within pos_tol_m / yaw_tol_rad of the most recent.
        """
        samples = list(self._buf)
        if not samples:
            return 0.0
        latest = samples[-1]
        inliers = 0
        for s in samples:
            dp = math.hypot(s['cx'] - latest['cx'], s['cy'] - latest['cy'])
            dy = abs(_norm_angle(s['yaw'] - latest['yaw']))
            if dp <= pos_tol_m and dy <= yaw_tol_rad:
                inliers += 1
        fill = _clamp01(len(samples) / self._window)
        ratio = inliers / len(samples)
        return _clamp01(0.40 * fill + 0.60 * ratio)


# ===========================================================================
# ROS 2 node
# ===========================================================================

class ShelfGeometricRefinerNode(Node):
    """
    ROS 2 node wiring together the validator and refiner.

    Designed to run alongside the existing shelf_detector without modifying
    it.  Subscribes to the detector's JSON output and the raw scan; publishes
    a semantically identical JSON with the refined pose folded in.
    """

    def __init__(self):
        super().__init__('shelf_geometric_refiner')

        # ---- Parameters ----
        self.declare_parameter('input_status_topic', '/shelf/status_json')
        self.declare_parameter('output_status_topic', '/shelf/refined_status_json')
        self.declare_parameter('scan_topic', '/scan')
        self.declare_parameter('base_frame', 'base_link')

        # Shelf model
        self.declare_parameter('shelf_model_length_m', 0.90)
        self.declare_parameter('shelf_model_width_m', 0.71)
        self.declare_parameter('shelf_model_leg_h_m', 0.04)
        self.declare_parameter('shelf_model_leg_w_m', 0.04)
        self.declare_parameter('shelf_model_leg_is_circular', False)

        # Validator tuning
        self.declare_parameter('roi_radius_m', 0.75)
        self.declare_parameter('cluster_adj_dist_m', 0.10)
        self.declare_parameter('cluster_min_points', 2)
        self.declare_parameter('cluster_max_extent_m', 0.20)
        self.declare_parameter('trailing_min_beta_deg', 15.0)
        self.declare_parameter('model_length_tolerance_m', 0.18)
        self.declare_parameter('model_width_tolerance_m', 0.18)
        self.declare_parameter('min_legs_for_validation', 3)

        # Refinement acceptance
        self.declare_parameter('max_pose_residual_accept_m', 0.06)
        self.declare_parameter('max_model_error_accept_m', 0.12)
        self.declare_parameter('scan_max_age_sec', 0.40)

        # Temporal stability
        self.declare_parameter('temporal_window_size', 8)
        self.declare_parameter('temporal_pos_tol_m', 0.025)
        self.declare_parameter('temporal_yaw_tol_deg', 3.0)

        self._p: Dict = {}
        for name in self._parameters:
            self._p[name] = self.get_parameter(name).value

        # ---- Build model + stages ----
        model = ShelfModel(
            length_m=float(self._p['shelf_model_length_m']),
            width_m=float(self._p['shelf_model_width_m']),
            leg_h_m=float(self._p['shelf_model_leg_h_m']),
            leg_w_m=float(self._p['shelf_model_leg_w_m']),
            is_circular=bool(self._p['shelf_model_leg_is_circular']),
        )
        self._validator = ShelfLegValidator(
            model=model,
            roi_radius_m=float(self._p['roi_radius_m']),
            cluster_adj_dist_m=float(self._p['cluster_adj_dist_m']),
            cluster_min_points=int(self._p['cluster_min_points']),
            cluster_max_extent_m=float(self._p['cluster_max_extent_m']),
            trailing_min_beta_deg=float(self._p['trailing_min_beta_deg']),
            model_length_tolerance_m=float(self._p['model_length_tolerance_m']),
            model_width_tolerance_m=float(self._p['model_width_tolerance_m']),
            min_legs=int(self._p['min_legs_for_validation']),
        )
        self._refiner = ShelfPoseRefiner(model)
        self._tracker = _TemporalTracker(window=int(self._p['temporal_window_size']))

        # ---- TF ----
        self._tf_buf = tf2_ros.Buffer()
        self._tf_listener = tf2_ros.TransformListener(self._tf_buf, self)

        # ---- State ----
        self._latest_scan: Optional[LaserScan] = None
        self._latest_scan_stamp: float = 0.0
        # Cache last refinement to smooth pass-through when scan is stale.
        self._last_refined: Optional[RefinedPose] = None
        self._last_refined_stamp: float = 0.0

        # ---- ROS interfaces ----
        self._scan_sub = self.create_subscription(
            LaserScan,
            str(self._p['scan_topic']),
            self._scan_cb, 10,
        )
        self._status_sub = self.create_subscription(
            String,
            str(self._p['input_status_topic']),
            self._status_cb, 10,
        )
        self._pub = self.create_publisher(
            String,
            str(self._p['output_status_topic']),
            10,
        )

        self.get_logger().info(
            f'shelf_geometric_refiner ready — '
            f'in={self._p["input_status_topic"]} '
            f'out={self._p["output_status_topic"]} '
            f'model=({model.length_m:.2f}×{model.width_m:.2f}m '
            f'legs={model.leg_h_m*100:.0f}mm '
            f'{"circ" if model.is_circular else "rect"})'
        )

    # ---- Scan callback: just cache ----

    def _scan_cb(self, msg: LaserScan) -> None:
        self._latest_scan = msg
        self._latest_scan_stamp = time.monotonic()

    # ---- Status callback: validate + refine + republish ----

    def _status_cb(self, msg: String) -> None:
        try:
            payload: Dict = json.loads(msg.data)
        except Exception:
            return

        # Always forward the original payload, then try to enhance it.
        out = dict(payload)

        # Only run the paper pipeline when the detector has a valid live pose.
        candidate_valid = bool(payload.get('candidate_valid', False))
        center_pose = payload.get('center_pose') if isinstance(payload.get('center_pose'), dict) else None

        if not candidate_valid or center_pose is None:
            # Detector lost the shelf — reset temporal tracker and forward as-is.
            self._tracker.reset()
            self._last_refined = None
            out = self._annotate_no_refinement(out, 'detector_invalid')
            self._publish(out)
            return

        rough_cx = float(center_pose.get('x', 0.0))
        rough_cy = float(center_pose.get('y', 0.0))
        rough_yaw = float(center_pose.get('yaw', 0.0))

        # Check scan freshness.
        scan_age = time.monotonic() - self._latest_scan_stamp
        max_age = float(self._p['scan_max_age_sec'])
        if self._latest_scan is None or scan_age > max_age:
            out = self._annotate_no_refinement(out, f'scan_stale ({scan_age:.2f}s)')
            self._publish(out)
            return

        # Run the full pipeline.
        scan_pts = self._scan_to_baselink(self._latest_scan)
        if len(scan_pts) < 4:
            out = self._annotate_no_refinement(out, 'scan_transform_failed')
            self._publish(out)
            return

        angle_inc = float(self._latest_scan.angle_increment)
        validation = self._validator.validate(
            scan_pts, rough_cx, rough_cy, rough_yaw, angle_inc,
        )

        if not validation.validated:
            out = self._annotate_no_refinement(out, f'validation_failed:{validation.reason}')
            self._publish(out)
            return

        refined = self._refiner.refine(validation, rough_cx, rough_cy, rough_yaw)
        if refined is None:
            out = self._annotate_no_refinement(out, 'refiner_nls_failed')
            self._publish(out)
            return

        # Fill in fields the refiner doesn't own.
        refined.model_error = validation.model_error
        refined.sum_leg_residual = sum(
            lc.fit_residual for lc in validation.leg_clusters
            if math.isfinite(lc.fit_residual)
        )

        # Acceptance gate.
        max_pr = float(self._p['max_pose_residual_accept_m'])
        max_me = float(self._p['max_model_error_accept_m'])
        if refined.pose_residual > max_pr or refined.model_error > max_me:
            out = self._annotate_no_refinement(
                out,
                f'refinement_rejected (pose_res={refined.pose_residual:.3f} '
                f'model_err={refined.model_error:.3f})',
            )
            self._publish(out)
            return

        # Temporal tracking.
        self._tracker.add(refined.cx, refined.cy, refined.yaw, time.monotonic())
        tol_pos = float(self._p['temporal_pos_tol_m'])
        tol_yaw = math.radians(float(self._p['temporal_yaw_tol_deg']))
        temporal_score = self._tracker.stability_score(tol_pos, tol_yaw)

        # Override center_pose with the refined pose.
        refined_pose_dict = {
            'x': refined.cx,
            'y': refined.cy,
            'yaw': refined.yaw,
            'frame_id': center_pose.get('frame_id', 'base_link'),
        }
        out['center_pose'] = refined_pose_dict

        # Add refinement metadata fields.
        out['refiner_validated'] = True
        out['refiner_num_legs'] = refined.num_legs
        out['refiner_model_error_m'] = round(refined.model_error, 4)
        out['refiner_sum_leg_residual_m'] = round(refined.sum_leg_residual, 4)
        out['refiner_pose_residual_m'] = round(refined.pose_residual, 4)
        out['refiner_temporal_stability'] = round(temporal_score, 4)
        out['refined_center_pose'] = refined_pose_dict

        # Blend temporal score with original confidence signals.
        orig_usability = float(out.get('control_usability_confidence', 0.5))
        orig_stability = float(out.get('candidate_stability_score', 0.5))
        # Weighted combination: detector confidence + geometry confidence
        # + temporal consistency.
        geometry_score = _clamp01(1.0 - refined.model_error / max(max_me, 1e-6))
        blended_usability = _clamp01(
            0.45 * orig_usability
            + 0.30 * geometry_score
            + 0.25 * temporal_score
        )
        blended_stability = _clamp01(
            0.50 * orig_stability
            + 0.30 * geometry_score
            + 0.20 * temporal_score
        )
        out['control_usability_confidence'] = round(blended_usability, 4)
        out['candidate_stability_score'] = round(blended_stability, 4)
        # Keep originals under separate keys for debugging.
        out['detector_control_usability_confidence'] = round(orig_usability, 4)
        out['detector_candidate_stability_score'] = round(orig_stability, 4)

        self._last_refined = refined
        self._last_refined_stamp = time.monotonic()

        self._publish(out)
        self.get_logger().debug(
            f'Refined shelf: ({refined.cx:.3f}, {refined.cy:.3f}, '
            f'yaw={math.degrees(refined.yaw):.1f}°) '
            f'legs={refined.num_legs} model_err={refined.model_error:.3f} '
            f'pose_res={refined.pose_residual:.4f} stab={temporal_score:.2f}'
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _annotate_no_refinement(self, out: Dict, reason: str) -> Dict:
        out['refiner_validated'] = False
        out['refiner_reason'] = reason
        out['refined_center_pose'] = None
        return out

    def _publish(self, payload: Dict) -> None:
        msg = String()
        msg.data = json.dumps(payload)
        self._pub.publish(msg)

    def _scan_to_baselink(self, scan: LaserScan) -> List[ScanPoint]:
        """
        Project each valid scan return to base_link and return a list of
        ScanPoint objects (retaining the original range and index for the
        trailing-point filter).
        """
        base = str(self._p['base_frame'])
        frame_id = scan.header.frame_id
        try:
            tf = self._tf_buf.lookup_transform(
                base, frame_id, rclpy.time.Time(),
            )
            use_tf = True
        except (tf2_ros.LookupException, tf2_ros.ConnectivityException,
                tf2_ros.ExtrapolationException):
            use_tf = False

        result: List[ScanPoint] = []
        has_intensity = len(scan.intensities) == len(scan.ranges)

        for i, r in enumerate(scan.ranges):
            if not math.isfinite(r) or r < 0.05:
                continue

            angle = scan.angle_min + i * scan.angle_increment
            xl = r * math.cos(angle)
            yl = r * math.sin(angle)

            if use_tf:
                pt = PointStamped()
                pt.header.frame_id = frame_id
                pt.point.x, pt.point.y, pt.point.z = xl, yl, 0.0
                try:
                    pt_out = do_transform_point(pt, tf)
                    x, y = pt_out.point.x, pt_out.point.y
                except Exception:
                    x, y = xl, yl
            else:
                x, y = xl, yl

            result.append(ScanPoint(x=x, y=y, r=r, idx=i))

        return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(args=None):
    rclpy.init(args=args)
    node = ShelfGeometricRefinerNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.destroy_node()
        except Exception:
            pass
        try:
            rclpy.shutdown()
        except Exception:
            pass


if __name__ == '__main__':
    main()
