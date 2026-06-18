import itertools
import json
import math

import numpy as np
import random
import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import rclpy
from geometry_msgs.msg import Pose, PoseArray, PoseWithCovarianceStamped
from nav_msgs.msg import Odometry
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, qos_profile_sensor_data
from sensor_msgs.msg import LaserScan
from std_msgs.msg import String
from tf2_ros import Buffer, TransformException, TransformListener
from visualization_msgs.msg import Marker, MarkerArray

try:
    import yaml
except ImportError:  # pragma: no cover - ROS Humble normally includes PyYAML.
    yaml = None


Point = Tuple[float, float]


@dataclass(frozen=True)
class Reflector:
    reflector_id: str
    group: str
    x: float
    y: float


@dataclass
class Detection:
    point_base: Point
    source: str
    reflector_id: Optional[str] = None
    intensity: float = 0.0
    range_m: float = 0.0


@dataclass
class Match:
    detection: Detection
    reflector: Reflector


def normalize_angle(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


def yaw_from_quaternion(q) -> float:
    siny_cosp = 2.0 * (q.w * q.z + q.x * q.y)
    cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
    return math.atan2(siny_cosp, cosy_cosp)


def quaternion_from_yaw(yaw: float):
    from geometry_msgs.msg import Quaternion

    q = Quaternion()
    q.z = math.sin(yaw * 0.5)
    q.w = math.cos(yaw * 0.5)
    return q


def transform_point(point: Point, tx: float, ty: float, yaw: float) -> Point:
    c = math.cos(yaw)
    s = math.sin(yaw)
    return (c * point[0] - s * point[1] + tx, s * point[0] + c * point[1] + ty)


def inverse_transform_point(point: Point, tx: float, ty: float, yaw: float) -> Point:
    dx = point[0] - tx
    dy = point[1] - ty
    c = math.cos(yaw)
    s = math.sin(yaw)
    return (c * dx + s * dy, -s * dx + c * dy)


def distance(a: Point, b: Point) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


class ReflectorLocalizer(Node):
    def __init__(self) -> None:
        super().__init__("reflector_localizer")

        self.declare_parameter("reflector_map_file", "")
        self.declare_parameter("scan_topic", "scan")
        self.declare_parameter("odom_topic", "odom")
        self.declare_parameter("base_frame", "base_link")
        self.declare_parameter("map_frame", "map")
        self.declare_parameter("corrected_pose_topic", "reflector_pose")
        self.declare_parameter("detections_topic", "reflector/detections")
        self.declare_parameter("marker_topic", "reflector/markers")
        self.declare_parameter("status_topic", "reflector/status_json")
        self.declare_parameter("selected_triangle_topic", "reflector/selected_triangle")
        self.declare_parameter("min_reflectors", 3)
        self.declare_parameter("intensity_threshold", 35.0)
        self.declare_parameter("cluster_gap_threshold", 0.22)
        self.declare_parameter("min_cluster_points", 2)
        self.declare_parameter("max_detection_range", 14.0)
        self.declare_parameter("max_match_distance", 0.75)
        self.declare_parameter("triangle_side_tolerance", 0.45)
        self.declare_parameter("candidate_side_tolerance", 1.2)
        self.declare_parameter("max_triangle_candidates", 8)
        self.declare_parameter("residual_threshold", 0.28)
        self.declare_parameter("enable_geometry_fallback", True)
        self.declare_parameter("require_geometry_scan_hit", True)
        self.declare_parameter("scan_range_gate", 0.45)
        self.declare_parameter("scan_beam_window", 4)
        self.declare_parameter("publish_map_markers", True)
        self.declare_parameter("ransac_iterations", 50)
        self.declare_parameter("ransac_inlier_threshold", 0.25)
        self.declare_parameter("min_reflector_spacing", 0.5)
        self.declare_parameter("mahalanobis_threshold", 100.0)
        self.declare_parameter("spike_threshold", 0.8)
        self.declare_parameter("hysteresis_good_frames", 3)
        self.declare_parameter("hysteresis_bad_frames", 5)

        self.good_frames = 0
        self.bad_frames = 0
        self.tracking_active = False
        self.pose_history = []
        self.selected_reflector_ids: Optional[Tuple[str, ...]] = None
        
        self.ekf_X = None
        self.ekf_P = np.diag([0.1, 0.1, 0.05])
        self.Q = np.diag([0.001, 0.001, 0.0005])
        self.R = np.diag([0.05, 0.05, 0.02])

        self.declare_parameter("fallback_laser_to_base_x", 0.35)
        self.declare_parameter("fallback_laser_to_base_y", 0.0)
        self.declare_parameter("fallback_laser_to_base_yaw", math.pi)

        self.base_frame = self.get_parameter("base_frame").value
        self.map_frame = self.get_parameter("map_frame").value
        self.min_reflectors = int(self.get_parameter("min_reflectors").value)
        self.reflectors = self._load_reflector_map(self.get_parameter("reflector_map_file").value)
        self.reflectors_by_id: Dict[str, Reflector] = {r.reflector_id: r for r in self.reflectors}

        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        self.last_odom_pose: Optional[Tuple[float, float, float]] = None
        self.last_corrected_pose: Optional[Tuple[float, float, float]] = None
        self.last_log_time = 0.0
        self.last_reload_time = 0.0

        scan_topic = self.get_parameter("scan_topic").value
        odom_topic = self.get_parameter("odom_topic").value
        corrected_pose_topic = self.get_parameter("corrected_pose_topic").value
        detections_topic = self.get_parameter("detections_topic").value
        marker_topic = self.get_parameter("marker_topic").value
        status_topic = self.get_parameter("status_topic").value
        selected_triangle_topic = self.get_parameter("selected_triangle_topic").value

        self.pose_pub = self.create_publisher(PoseWithCovarianceStamped, corrected_pose_topic, 10)
        self.detections_pub = self.create_publisher(PoseArray, detections_topic, 10)
        self.marker_pub = self.create_publisher(MarkerArray, marker_topic, 10)
        self.status_pub = self.create_publisher(String, status_topic, 10)
        self.create_subscription(PoseWithCovarianceStamped, "/initialpose", self._on_initial_pose, 10)
        self.create_subscription(String, selected_triangle_topic, self._on_selected_triangle, 10)

        from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
        scan_qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
        )
        self.create_subscription(LaserScan, scan_topic, self._on_scan, scan_qos)
        self.create_subscription(Odometry, odom_topic, self._on_odom, QoSProfile(depth=30, reliability=ReliabilityPolicy.RELIABLE))
        self.get_logger().info(
            f"Loaded {len(self.reflectors)} known reflectors from {self.get_parameter('reflector_map_file').value}"
        )

    def _load_reflector_map(self, path: str) -> List[Reflector]:
        reflectors = []
        used_ids = set()

        def _record_id(record, index: int) -> str:
            for key in ("reflector_id", "id", "name"):
                raw = record.get(key)
                text = str(raw if raw is not None else "").strip()
                if text and text.lower() not in {"0", "none", "null"}:
                    candidate = text
                    break
            else:
                candidate = f"R{index + 1}"

            base = candidate
            suffix = 2
            while candidate in used_ids:
                candidate = f"{base}_{suffix}"
                suffix += 1
            used_ids.add(candidate)
            return candidate
        
        # 1. Try loading from YAML if path is provided
        if path and os.path.exists(path):
            if yaml is None:
                self.get_logger().warn("PyYAML not found, skipping YAML map load")
            else:
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        data = yaml.safe_load(handle) or {}
                    for index, entry in enumerate(data.get("reflectors", [])):
                        reflectors.append(
                            Reflector(
                                reflector_id=_record_id(entry, index),
                                group=str(entry.get("group", "")),
                                x=float(entry["x"]),
                                y=float(entry["y"]),
                            )
                        )
                    if reflectors:
                        self.get_logger().info(f"Loaded {len(reflectors)} reflectors from YAML: {path}")
                except Exception as e:
                    self.get_logger().error(f"Failed to load YAML map: {e}")

        # 2. Try loading from Database if YAML yielded nothing
        if not reflectors:
            db_path = os.path.expanduser("~/DB/robot_data.db")
            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute("SELECT reflector FROM map_layers WHERE id=1")
                    row = cursor.fetchone()
                    if row and row['reflector']:
                        raw_reflectors = json.loads(row['reflector'])
                        for index, r in enumerate(raw_reflectors):
                            reflectors.append(
                                Reflector(
                                    reflector_id=_record_id(r, index),
                                    group=str(r.get("group", "default")),
                                    x=float(r["x"]),
                                    y=float(r["y"]),
                                )
                            )
                    conn.close()
                    if reflectors:
                        self.get_logger().info(f"Loaded {len(reflectors)} reflectors from Database: {db_path}")
                except Exception as e:
                    self.get_logger().error(f"Failed to load reflectors from database: {e}")

        if len(reflectors) < self.min_reflectors:
            self.get_logger().warn(f"Reflector map only has {len(reflectors)} reflectors (need {self.min_reflectors})")
            
        return reflectors

    def _ekf_predict(self, odom_dx: float, odom_dy: float, odom_dyaw: float) -> None:
        if self.ekf_X is None:
            return
        yaw = self.ekf_X[2]
        c = math.cos(yaw)
        s = math.sin(yaw)
        self.ekf_X[0] += c * odom_dx - s * odom_dy
        self.ekf_X[1] += s * odom_dx + c * odom_dy
        self.ekf_X[2] = normalize_angle(self.ekf_X[2] + odom_dyaw)
        F = np.eye(3)
        F[0, 2] = -s * odom_dx - c * odom_dy
        F[1, 2] =  c * odom_dx - s * odom_dy
        self.ekf_P = F @ self.ekf_P @ F.T + self.Q

    def _ekf_update(self, z_pose: Tuple[float, float, float]) -> None:
        if self.ekf_X is None:
            self.ekf_X = np.array(z_pose, dtype=float)
            self.ekf_P = np.copy(self.R)
            return
        Y = np.array([z_pose[0] - self.ekf_X[0], z_pose[1] - self.ekf_X[1], normalize_angle(z_pose[2] - self.ekf_X[2])])
        S = self.ekf_P + self.R
        try:
            S_inv = np.linalg.inv(S)
        except np.linalg.LinAlgError:
            S_inv = np.linalg.pinv(S)
        K = self.ekf_P @ S_inv
        self.ekf_X += K @ Y
        self.ekf_X[2] = normalize_angle(self.ekf_X[2])
        self.ekf_P = (np.eye(3) - K) @ self.ekf_P

    def _mahalanobis_gate(self, z_pose: Tuple[float, float, float]) -> bool:
        if self.ekf_X is None:
            return True
        Y = np.array([z_pose[0] - self.ekf_X[0], z_pose[1] - self.ekf_X[1], normalize_angle(z_pose[2] - self.ekf_X[2])])
        S = self.ekf_P + self.R
        try:
            S_inv = np.linalg.inv(S)
        except np.linalg.LinAlgError:
            return True
        dist = float(Y.T @ S_inv @ Y)
        return dist < float(self.get_parameter("mahalanobis_threshold").value)

    def _check_temporal_spike(self, z_pose: Tuple[float, float, float]) -> bool:
        if len(self.pose_history) < 3:
            return False
        med_x = float(np.median([p[0] for p in self.pose_history]))
        med_y = float(np.median([p[1] for p in self.pose_history]))
        thresh = float(self.get_parameter("spike_threshold").value)
        return abs(z_pose[0] - med_x) > thresh or abs(z_pose[1] - med_y) > thresh

    def _check_geometry_confidence(self, matches: Sequence[Match], residual: float) -> bool:
        if len(matches) < 3:
            return False
        if residual > float(self.get_parameter("residual_threshold").value):
            return False
        map_points = [(m.reflector.x, m.reflector.y) for m in matches]
        min_spacing = float(self.get_parameter("min_reflector_spacing").value)
        for i in range(len(map_points)):
            for j in range(i + 1, len(map_points)):
                if distance(map_points[i], map_points[j]) < min_spacing:
                    return False
        return True

    def _ransac_pose(self, matches: Sequence[Match]) -> Tuple[Optional[Tuple[float, float, float]], float, List[Match]]:
        if len(matches) < 3:
            return None, float("inf"), []
        max_iter = int(self.get_parameter("ransac_iterations").value)
        inlier_thresh = float(self.get_parameter("ransac_inlier_threshold").value)
        best_inliers = []
        best_pose = None
        
        for _ in range(max_iter):
            sample = random.sample(list(matches), 3)
            pose, _ = self._solve_pose_unweighted(sample)
            inliers = []
            for m in matches:
                pred = transform_point(m.detection.point_base, pose[0], pose[1], pose[2])
                if distance(pred, (m.reflector.x, m.reflector.y)) < inlier_thresh:
                    inliers.append(m)
            if len(inliers) > len(best_inliers):
                best_inliers = inliers
                best_pose = pose
                
        if len(best_inliers) < 3:
            return None, float("inf"), []
        pose, residual = self._solve_pose(best_inliers)
        return pose, residual, best_inliers

    def _on_initial_pose(self, msg: PoseWithCovarianceStamped) -> None:
        p = msg.pose.pose.position
        q = msg.pose.pose.orientation
        yaw = yaw_from_quaternion(q)
        self.get_logger().info(f"Received manual relocalization: x={p.x:.2f}, y={p.y:.2f}, yaw={yaw:.2f}")
        self.get_logger().info("FORCING IMMEDIATE POSE PUBLISH")
        self.ekf_X = np.array([p.x, p.y, yaw], dtype=float)
        self.ekf_P = np.diag([1.0, 1.0, 0.5]) # Huge covariance to allow first match to easily gate
        self.tracking_active = True
        self.good_frames = 10 # Force it to start tracking immediately
        self.bad_frames = 0
        
        # Immediately publish to bypass any scanning delay and force broadcaster snap
        msg_out = PoseWithCovarianceStamped()
        msg_out.header.stamp = self.get_clock().now().to_msg()
        msg_out.header.frame_id = self.map_frame
        msg_out.pose.pose.position.x = float(p.x)
        msg_out.pose.pose.position.y = float(p.y)
        msg_out.pose.pose.orientation = q
        self.pose_pub.publish(msg_out)

    def _on_odom(self, msg: Odometry) -> None:
        pose = msg.pose.pose
        current_odom = (pose.position.x, pose.position.y, yaw_from_quaternion(pose.orientation))
        if self.last_odom_pose is not None and self.ekf_X is not None:
            dx = current_odom[0] - self.last_odom_pose[0]
            dy = current_odom[1] - self.last_odom_pose[1]
            dyaw = normalize_angle(current_odom[2] - self.last_odom_pose[2])
            c = math.cos(-self.last_odom_pose[2])
            s = math.sin(-self.last_odom_pose[2])
            base_dx = c * dx - s * dy
            base_dy = s * dx + c * dy
            self._ekf_predict(base_dx, base_dy, dyaw)
        self.last_odom_pose = current_odom

    def _on_selected_triangle(self, msg: String) -> None:
        raw = str(getattr(msg, "data", "") or "").strip()
        ids: List[str] = []
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                ids = [str(value).strip() for value in payload.get("map_ids", [])]
            elif isinstance(payload, list):
                ids = [str(value).strip() for value in payload]
        except Exception:
            ids = [part.strip() for part in raw.split(",")]

        cleaned = tuple(sorted({value for value in ids if value}))
        if len(cleaned) < self.min_reflectors:
            self.selected_reflector_ids = None
            self.get_logger().warn("Cleared reflector triangle selection")
            return
        self.selected_reflector_ids = cleaned
        self.get_logger().info(f"Locked reflector triangle: {', '.join(cleaned)}")

    def _on_scan(self, scan: LaserScan) -> None:
        self.get_logger().info(f"ON_SCAN: {len(scan.ranges)} points, reflectors={len(self.reflectors)}")
        # Lazy load/reload if we have no reflectors yet
        now = self.get_clock().now().nanoseconds * 1e-9
        if not self.reflectors and (now - self.last_reload_time > 5.0):
            self.last_reload_time = now
            self.reflectors = self._load_reflector_map(self.get_parameter("reflector_map_file").value)
            self.reflectors_by_id = {r.reflector_id: r for r in self.reflectors}
            
        if not self.reflectors:
            if random.random() < 0.1: self.get_logger().warn("ON_SCAN: No reflectors loaded, ignoring scan")
            return

        detections = self._detect_from_intensity(scan)
        mode = "intensity"
        candidates = self._triangle_candidates(detections)
        selected_candidate = self._selected_triangle_candidate(candidates)
        matches = selected_candidate["matches"] if selected_candidate is not None else self._associate_matches(detections)
        if len(matches) < self.min_reflectors and bool(self.get_parameter("enable_geometry_fallback").value):
            fallback_detections = self._detect_from_geometry_tags(scan)
            fallback_candidates = self._triangle_candidates(fallback_detections)
            fallback_selected = self._selected_triangle_candidate(fallback_candidates)
            fallback_matches = fallback_selected["matches"] if fallback_selected is not None else self._associate_matches(fallback_detections)
            if len(fallback_matches) >= len(matches):
                detections = fallback_detections
                matches = fallback_matches
                candidates = fallback_candidates
                selected_candidate = fallback_selected
                mode = "geometry_fallback"

        accepted = False
        residual = None
        reason = "not_enough_matches"
        solved_pose = None

        if True:
            self.get_logger().info(f"DETECTION STATS: detections={len(detections)}, matches={len(matches)}, mode={mode}")

        if len(matches) >= self.min_reflectors:
            self.get_logger().info(f"TRIANGULATING {len(matches)} matches...")
            solved_pose, residual, inliers = self._ransac_pose(matches)
            if solved_pose:
                self.get_logger().info(f"SOLVED: x={solved_pose[0]:.2f}, y={solved_pose[1]:.2f}, res={residual:.2f}")
            if solved_pose is not None:
                if self._check_geometry_confidence(inliers, residual):
                    self.pose_history.append(solved_pose)
                    if len(self.pose_history) > 5:
                        self.pose_history.pop(0)
                        
                    if self._check_temporal_spike(solved_pose):
                        reason = "temporal_spike"
                    elif not self._mahalanobis_gate(solved_pose):
                        reason = "mahalanobis_gated"
                    else:
                        accepted = True
                        reason = "accepted"
                else:
                    reason = "geometry_confidence_failed"
            else:
                reason = "ransac_failed"

        # Hysteresis
        if accepted:
            self.good_frames += 1
            self.bad_frames = 0
            if self.good_frames >= int(self.get_parameter("hysteresis_good_frames").value):
                self.tracking_active = True
        else:
            self.bad_frames += 1
            self.good_frames = 0
            if self.bad_frames >= int(self.get_parameter("hysteresis_bad_frames").value):
                self.tracking_active = False

        if self.tracking_active and accepted and solved_pose is not None:
            self._ekf_update(solved_pose)
            self.last_corrected_pose = tuple(self.ekf_X)
            self._publish_corrected_pose(scan, self.last_corrected_pose, residual, self.ekf_P)
        elif self.ekf_X is not None:
            self.last_corrected_pose = tuple(self.ekf_X)
            self._publish_corrected_pose(scan, self.last_corrected_pose, residual if residual else 999.0, self.ekf_P)

        self._publish_detections(scan, detections)
        self._publish_markers(scan, detections, matches, self.last_corrected_pose if self.last_corrected_pose else self.last_odom_pose, candidates)
        self._publish_status(scan, mode, detections, matches, accepted, residual, reason, self.last_corrected_pose, candidates, selected_candidate)

    def _detect_from_intensity(self, scan: LaserScan) -> List[Detection]:
        if not scan.intensities:
            return []

        # Log every scan for debug
        if scan.intensities:
            top_i = sorted(scan.intensities, reverse=True)[:5]
            self.get_logger().info(f"SCAN: top_i={top_i}")
        max_i = max(scan.intensities) if scan.intensities else 0.0
        threshold = float(self.get_parameter('intensity_threshold').value)
        above_thresh = len([i for i in scan.intensities if i >= threshold]) if scan.intensities else 0
        if True: # 20% of scans
            self.get_logger().info(f"SCAN RECEIVED: max_i={max_i:.1f}, points_above={above_thresh}, has_intensities={bool(scan.intensities)}")

        candidates = []
        threshold = float(self.get_parameter("intensity_threshold").value)
        max_range = float(self.get_parameter("max_detection_range").value)
        for index, range_m in enumerate(scan.ranges):
            if not math.isfinite(range_m) or range_m <= scan.range_min or range_m >= min(scan.range_max, max_range):
                continue
            intensity = scan.intensities[index] if index < len(scan.intensities) else 0.0
            if intensity < threshold:
                continue
            angle = scan.angle_min + index * scan.angle_increment
            candidates.append((index, range_m * math.cos(angle), range_m * math.sin(angle), intensity, range_m))

        if len(candidates) > 500:
            self.get_logger().warn(f"Too many intensity candidates: {len(candidates)}")
        if not candidates:
            return []

        clusters = []
        gap_threshold = float(self.get_parameter("cluster_gap_threshold").value)
        remaining = list(candidates)
        while remaining:
            seed = remaining.pop(0)
            group = [seed]

            changed = True
            while changed:
                changed = False
                still_remaining = []
                for candidate in remaining:
                    close = any(
                        math.hypot(candidate[1] - item[1], candidate[2] - item[2]) <= gap_threshold
                        for item in group
                    )
                    if close:
                        group.append(candidate)
                        changed = True
                    else:
                        still_remaining.append(candidate)
                remaining = still_remaining

            clusters.append(group)

        min_points = int(self.get_parameter("min_cluster_points").value)
        detections: List[Detection] = []
        for cluster in clusters:
            if len(cluster) < min_points:
                continue
            weight = sum(max(item[3], 1.0) for item in cluster)
            x_laser = sum(item[1] * max(item[3], 1.0) for item in cluster) / weight
            y_laser = sum(item[2] * max(item[3], 1.0) for item in cluster) / weight
            intensity = max(item[3] for item in cluster)
            range_m = sum(item[4] for item in cluster) / len(cluster)
            point_base = self._laser_to_base((x_laser, y_laser), scan.header.frame_id)
            detections.append(Detection(point_base=point_base, source="intensity", intensity=intensity, range_m=range_m))
        return detections

    def _detect_from_geometry_tags(self, scan: LaserScan) -> List[Detection]:
        if self.last_odom_pose is None:
            return []

        scan_beam_window = int(self.get_parameter("scan_beam_window").value)
        scan_range_gate = float(self.get_parameter("scan_range_gate").value)
        max_range = min(float(self.get_parameter("max_detection_range").value), scan.range_max)
        map_pose = self._get_current_map_pose()
        if map_pose is None:
            return []
        odom_x, odom_y, odom_yaw = map_pose
        detections: List[Detection] = []

        for reflector in self.reflectors:
            point_base = inverse_transform_point((reflector.x, reflector.y), odom_x, odom_y, odom_yaw)
            base_angle = math.atan2(point_base[1], point_base[0])
            # When not tracking, look 360 degrees to find anything. When tracking, stay focused.
            if self.tracking_active:
                if point_base[0] <= -2.0:
                    continue
                if abs(base_angle) > math.pi * 0.8:
                    continue
            range_base = math.hypot(point_base[0], point_base[1])
            if range_base < scan.range_min or range_base > max_range:
                continue

            point_laser = self._base_to_laser(point_base, scan.header.frame_id)
            angle = math.atan2(point_laser[1], point_laser[0])
            range_laser = math.hypot(point_laser[0], point_laser[1])
            using_base_projection = False
            if angle < scan.angle_min or angle > scan.angle_max:
                # Some Gazebo ray configurations rotate the physical sensor pose
                # but publish the parent link as frame_id. Use the base-frame
                # forward FOV as the fallback index in that case.
                angle = base_angle
                range_laser = range_base
                using_base_projection = True
                if angle < scan.angle_min or angle > scan.angle_max:
                    continue

            index = int(round((angle - scan.angle_min) / scan.angle_increment))
            matched_scan = False
            measured_range = range_laser
            for beam_index in range(max(0, index - scan_beam_window), min(len(scan.ranges), index + scan_beam_window + 1)):
                candidate_range = scan.ranges[beam_index]
                if math.isfinite(candidate_range) and abs(candidate_range - range_laser) <= scan_range_gate:
                    matched_scan = True
                    measured_range = candidate_range
                    break

            if not matched_scan and bool(self.get_parameter("require_geometry_scan_hit").value):
                continue

            if matched_scan:
                if using_base_projection:
                    measured_base = (measured_range * math.cos(base_angle), measured_range * math.sin(base_angle))
                else:
                    measured_laser = (measured_range * math.cos(angle), measured_range * math.sin(angle))
                    measured_base = self._laser_to_base(measured_laser, scan.header.frame_id)
                detections.append(
                    Detection(
                        point_base=measured_base,
                        source="geometry_tag",
                        reflector_id=reflector.reflector_id,
                        range_m=measured_range,
                    )
                )

        return self._limit_to_visible_triplet(detections)

    def _limit_to_visible_triplet(self, detections: List[Detection]) -> List[Detection]:
        if len(detections) <= 6:
            return detections
        detections.sort(key=lambda detection: detection.range_m)
        return detections[:6]

    def _associate_matches(self, detections: Sequence[Detection]) -> List[Match]:
        direct_matches = []
        used_ids = set()
        for detection in detections:
            if detection.reflector_id and detection.reflector_id in self.reflectors_by_id and detection.reflector_id not in used_ids:
                reflector = self.reflectors_by_id[detection.reflector_id]
                direct_matches.append(Match(detection=detection, reflector=reflector))
                used_ids.add(detection.reflector_id)
        if len(direct_matches) >= self.min_reflectors:
            return direct_matches

        nearest = self._associate_by_odom_nearest(detections)
        if len(nearest) >= self.min_reflectors:
            return nearest

        triangle = self._associate_by_triangle_shape(detections)
        if len(triangle) >= self.min_reflectors:
            return triangle
        return direct_matches

    def _get_current_map_pose(self) -> Optional[Tuple[float, float, float]]:
        # Use EKF state if tracking is active, otherwise try to seed from TF
        if self.ekf_X is not None and self.tracking_active:
            return tuple(self.ekf_X)
        
        # Fallback to TF lookup to handle manual relocalization / initial state
        try:
            # We want map -> base_link
            t = self.tf_buffer.lookup_transform(self.map_frame, self.base_frame, rclpy.time.Time())
            pos = t.transform.translation
            rot = t.transform.rotation
            return (pos.x, pos.y, yaw_from_quaternion(rot))
        except Exception:
            # Last resort: use last odom pose directly (assumes map=odom)
            return self.last_odom_pose

    def _associate_by_odom_nearest(self, detections: Sequence[Detection]) -> List[Match]:
        map_pose = self._get_current_map_pose()
        if map_pose is None:
            return []
        
        map_x, map_y, map_yaw = map_pose
        max_match_distance = float(self.get_parameter("max_match_distance").value)
        used_ids = set()
        matches = []
        for detection in detections:
            # Project base-link detection into map frame using our best pose estimate
            projected = transform_point(detection.point_base, map_x, map_y, map_yaw)
            # Use enumerate to get unique indices for matching within a single scan
            candidates = []
            for i, r in enumerate(self.reflectors):
                if i not in used_ids:
                    dist = distance(projected, (r.x, r.y))
                    candidates.append((dist, i, r))
            candidates.sort()
            
            if candidates and candidates[0][0] <= max_match_distance:
                dist, idx, r = candidates[0]
                # self.get_logger().info(f"MATCH: {dist:.2f}m to reflector_index {idx}")
                matches.append(Match(detection=detection, reflector=r))
                used_ids.add(idx)
        return matches

    def _associate_by_triangle_shape(self, detections: Sequence[Detection]) -> List[Match]:
        if len(detections) < 3:
            return []
        side_tolerance = float(self.get_parameter("triangle_side_tolerance").value)
        max_match_distance = float(self.get_parameter("max_match_distance").value)
        best_matches: List[Match] = []
        best_score = float("inf")

        for det_triplet in itertools.combinations(detections, 3):
            det_sides = self._sorted_side_lengths([d.point_base for d in det_triplet])
            for map_triplet in itertools.combinations(self.reflectors, 3):
                map_sides = self._sorted_side_lengths([(r.x, r.y) for r in map_triplet])
                if max(abs(a - b) for a, b in zip(det_sides, map_sides)) > side_tolerance:
                    continue
                for permuted_reflectors in itertools.permutations(map_triplet):
                    trial_matches = [Match(detection=d, reflector=r) for d, r in zip(det_triplet, permuted_reflectors)]
                    pose, residual = self._solve_pose(trial_matches)
                    if residual > side_tolerance:
                        continue
                    expanded = self._expand_matches_with_pose(detections, trial_matches, pose, max_match_distance)
                    _, expanded_residual = self._solve_pose(expanded)
                    score = expanded_residual - 0.02 * len(expanded)
                    if len(expanded) >= self.min_reflectors and score < best_score:
                        best_score = score
                        best_matches = expanded
        return best_matches

    def _expand_matches_with_pose(
        self,
        detections: Sequence[Detection],
        seed_matches: Sequence[Match],
        pose: Tuple[float, float, float],
        max_match_distance: float,
    ) -> List[Match]:
        used_detection_ids = {id(match.detection) for match in seed_matches}
        used_reflector_ids = {match.reflector.reflector_id for match in seed_matches}
        matches = list(seed_matches)
        pose_x, pose_y, pose_yaw = pose
        for detection in detections:
            if id(detection) in used_detection_ids:
                continue
            projected = transform_point(detection.point_base, pose_x, pose_y, pose_yaw)
            candidates = sorted(
                (distance(projected, (r.x, r.y)), r) for r in self.reflectors if r.reflector_id not in used_reflector_ids
            )
            if candidates and candidates[0][0] <= max_match_distance:
                self.get_logger().info(f"MATCH: {candidates[0][0]:.2f}m to {candidates[0][1].reflector_id}")
                matches.append(Match(detection=detection, reflector=candidates[0][1]))
                used_reflector_ids.add(candidates[0][1].reflector_id)
        return matches

    def _triangle_candidates(self, detections: Sequence[Detection]) -> List[Dict[str, object]]:
        if len(detections) < 3 or len(self.reflectors) < 3:
            return []

        side_tolerance = float(self.get_parameter("candidate_side_tolerance").value)
        max_candidates = max(1, int(self.get_parameter("max_triangle_candidates").value))
        candidates: List[Dict[str, object]] = []

        for det_triplet in itertools.combinations(enumerate(detections), 3):
            det_indices = tuple(index for index, _det in det_triplet)
            det_values = [det for _index, det in det_triplet]
            det_sides = self._sorted_side_lengths([d.point_base for d in det_values])
            for map_triplet in itertools.combinations(self.reflectors, 3):
                map_sides = self._sorted_side_lengths([(r.x, r.y) for r in map_triplet])
                side_error = max(abs(a - b) for a, b in zip(det_sides, map_sides))
                if side_error > side_tolerance:
                    continue
                for permuted_reflectors in itertools.permutations(map_triplet):
                    matches = [Match(detection=d, reflector=r) for d, r in zip(det_values, permuted_reflectors)]
                    pose, residual = self._solve_pose(matches)
                    map_ids = tuple(sorted(match.reflector.reflector_id for match in matches))
                    score = residual + 0.25 * side_error
                    candidates.append({
                        "id": f"T{len(candidates) + 1}",
                        "matches": matches,
                        "pose": pose,
                        "residual": residual,
                        "side_error": side_error,
                        "score": score,
                        "map_ids": map_ids,
                        "det_indices": det_indices,
                    })

        candidates.sort(key=lambda item: (float(item["score"]), float(item["residual"])))
        return candidates[:max_candidates]

    def _selected_triangle_candidate(self, candidates: Sequence[Dict[str, object]]) -> Optional[Dict[str, object]]:
        selected = self.selected_reflector_ids
        if not selected:
            return None
        selected_set = set(selected)
        matching = [
            candidate for candidate in candidates
            if set(candidate.get("map_ids", ())) == selected_set
        ]
        if not matching:
            return None
        return min(matching, key=lambda item: (float(item["score"]), float(item["residual"])))

    def _solve_pose_unweighted(self, matches: Sequence[Match]) -> Tuple[Tuple[float, float, float], float]:
        local_points = [match.detection.point_base for match in matches]
        map_points = [(match.reflector.x, match.reflector.y) for match in matches]
        local_centroid = (
            sum(point[0] for point in local_points) / len(local_points),
            sum(point[1] for point in local_points) / len(local_points),
        )
        map_centroid = (
            sum(point[0] for point in map_points) / len(map_points),
            sum(point[1] for point in map_points) / len(map_points),
        )

        cross = 0.0
        dot = 0.0
        for local, mapped in zip(local_points, map_points):
            lx = local[0] - local_centroid[0]
            ly = local[1] - local_centroid[1]
            mx = mapped[0] - map_centroid[0]
            my = mapped[1] - map_centroid[1]
            cross += lx * my - ly * mx
            dot += lx * mx + ly * my
        yaw = math.atan2(cross, dot)
        rotated_centroid = transform_point(local_centroid, 0.0, 0.0, yaw)
        pose = (
            map_centroid[0] - rotated_centroid[0],
            map_centroid[1] - rotated_centroid[1],
            normalize_angle(yaw),
        )

        residual_sq = 0.0
        for local, mapped in zip(local_points, map_points):
            predicted = transform_point(local, pose[0], pose[1], pose[2])
            residual_sq += distance(predicted, mapped) ** 2
        residual = math.sqrt(residual_sq / len(matches))
        return pose, residual

    def _solve_pose(self, matches: Sequence[Match]) -> Tuple[Tuple[float, float, float], float]:
        weights = [1.0 / max(0.1, m.detection.range_m) for m in matches]
        total_w = sum(weights)
        
        local_cx = sum(m.detection.point_base[0] * w for m, w in zip(matches, weights)) / total_w
        local_cy = sum(m.detection.point_base[1] * w for m, w in zip(matches, weights)) / total_w
        map_cx = sum(m.reflector.x * w for m, w in zip(matches, weights)) / total_w
        map_cy = sum(m.reflector.y * w for m, w in zip(matches, weights)) / total_w
        
        cross = 0.0
        dot = 0.0
        for m, w in zip(matches, weights):
            lx = m.detection.point_base[0] - local_cx
            ly = m.detection.point_base[1] - local_cy
            mx = m.reflector.x - map_cx
            my = m.reflector.y - map_cy
            cross += w * (lx * my - ly * mx)
            dot += w * (lx * mx + ly * my)
            
        yaw = math.atan2(cross, dot)
        rotated_cx = local_cx * math.cos(yaw) - local_cy * math.sin(yaw)
        rotated_cy = local_cx * math.sin(yaw) + local_cy * math.cos(yaw)
        
        pose = (map_cx - rotated_cx, map_cy - rotated_cy, normalize_angle(yaw))
        
        residual_sq = 0.0
        for m in matches:
            pred = transform_point(m.detection.point_base, pose[0], pose[1], pose[2])
            residual_sq += distance(pred, (m.reflector.x, m.reflector.y)) ** 2
        residual = math.sqrt(residual_sq / len(matches))
        return pose, residual

    def _publish_corrected_pose(self, scan: LaserScan, pose: Tuple[float, float, float], residual: float, covariance: np.ndarray = None) -> None:
        msg = PoseWithCovarianceStamped()
        msg.header.stamp = scan.header.stamp
        msg.header.frame_id = self.map_frame
        msg.pose.pose.position.x = pose[0]
        msg.pose.pose.position.y = pose[1]
        msg.pose.pose.orientation = quaternion_from_yaw(pose[2])
        if covariance is not None:
            msg.pose.covariance[0] = covariance[0, 0]
            msg.pose.covariance[7] = covariance[1, 1]
            msg.pose.covariance[35] = covariance[2, 2]
        else:
            position_variance = max(0.0025, min(0.25, residual * residual + 0.0025))
            yaw_variance = max(0.004, min(0.20, residual * residual * 0.5 + 0.004))
            msg.pose.covariance[0] = position_variance
            msg.pose.covariance[7] = position_variance
            msg.pose.covariance[35] = yaw_variance
        self.pose_pub.publish(msg)

    def _publish_detections(self, scan: LaserScan, detections: Sequence[Detection]) -> None:
        msg = PoseArray()
        msg.header.stamp = scan.header.stamp
        msg.header.frame_id = self.base_frame
        for detection in detections:
            pose = Pose()
            pose.position.x = detection.point_base[0]
            pose.position.y = detection.point_base[1]
            pose.orientation.w = 1.0
            msg.poses.append(pose)
        self.detections_pub.publish(msg)

    def _publish_markers(
        self,
        scan: LaserScan,
        detections: Sequence[Detection],
        matches: Sequence[Match],
        pose_for_projection: Optional[Tuple[float, float, float]],
        candidates: Sequence[Dict[str, object]] = (),
    ) -> None:
        markers = MarkerArray()
        now = scan.header.stamp
        if bool(self.get_parameter("publish_map_markers").value):
            for index, reflector in enumerate(self.reflectors):
                marker = Marker()
                marker.header.stamp = now
                marker.header.frame_id = self.map_frame
                marker.ns = "known_reflectors"
                marker.id = index
                marker.type = Marker.CYLINDER
                marker.action = Marker.ADD
                marker.text = reflector.reflector_id
                marker.pose.position.x = reflector.x
                marker.pose.position.y = reflector.y
                marker.pose.position.z = 0.2
                marker.pose.orientation.w = 1.0
                marker.scale.x = 0.16
                marker.scale.y = 0.16
                marker.scale.z = 0.4
                marker.color.r = 0.0
                marker.color.g = 0.9
                marker.color.b = 0.9
                marker.color.a = 0.9
                markers.markers.append(marker)

        matched_detection_ids = {id(match.detection) for match in matches}
        matched_detection_labels = {
            id(match.detection): match.reflector.reflector_id
            for match in matches
        }
        if pose_for_projection is not None:
            for index, detection in enumerate(detections):
                projected = transform_point(detection.point_base, pose_for_projection[0], pose_for_projection[1], pose_for_projection[2])
                marker = Marker()
                marker.header.stamp = now
                marker.header.frame_id = self.map_frame
                marker.ns = "detected_reflectors"
                marker.id = index
                marker.type = Marker.SPHERE
                marker.action = Marker.ADD
                marker.text = matched_detection_labels.get(id(detection), "")
                marker.pose.position.x = projected[0]
                marker.pose.position.y = projected[1]
                marker.pose.position.z = 0.32
                marker.pose.orientation.w = 1.0
                marker.scale.x = 0.22
                marker.scale.y = 0.22
                marker.scale.z = 0.22
                marker.color.r = 0.1 if id(detection) in matched_detection_ids else 1.0
                marker.color.g = 1.0 if id(detection) in matched_detection_ids else 0.55
                marker.color.b = 0.1
                marker.color.a = 0.95
                markers.markers.append(marker)

        # ── Triangulation Lines (Rays) ────────────────────────
        if matches and pose_for_projection is not None:
            line_marker = Marker()
            line_marker.header.stamp = now
            line_marker.header.frame_id = self.map_frame
            line_marker.ns = "triangulation"
            line_marker.id = 0
            line_marker.type = Marker.LINE_LIST
            line_marker.action = Marker.ADD
            line_marker.pose.orientation.w = 1.0
            line_marker.scale.x = 0.03 # line width
            line_marker.color.r = 0.0
            line_marker.color.g = 1.0
            line_marker.color.b = 1.0
            line_marker.color.a = 0.4

            for match in matches:
                # Start point: Robot position
                p_start_x = pose_for_projection[0]
                p_start_y = pose_for_projection[1]
                # End point: Reflector in map
                p_end_x = match.reflector.x
                p_end_y = match.reflector.y
                
                from geometry_msgs.msg import Point as RosPoint
                line_marker.points.append(RosPoint(x=p_start_x, y=p_start_y, z=0.1))
                line_marker.points.append(RosPoint(x=p_end_x, y=p_end_y, z=0.1))
            
            markers.markers.append(line_marker)

        if candidates:
            from geometry_msgs.msg import Point as RosPoint
            selected_ids = set(self.selected_reflector_ids or ())
            for index, candidate in enumerate(candidates):
                candidate_matches = candidate.get("matches", [])
                if not candidate_matches:
                    continue
                map_ids = list(candidate.get("map_ids", ()))
                is_selected = bool(selected_ids) and set(map_ids) == selected_ids
                marker = Marker()
                marker.header.stamp = now
                marker.header.frame_id = self.map_frame
                marker.ns = "reflector_triangle_candidates"
                marker.id = index
                marker.type = Marker.LINE_LIST
                marker.action = Marker.ADD
                marker.pose.orientation.w = 1.0
                marker.scale.x = 0.05 if is_selected else 0.025
                marker.color.r = 1.0 if is_selected else 0.95
                marker.color.g = 0.85 if is_selected else 0.65
                marker.color.b = 0.1 if is_selected else 0.2
                marker.color.a = 0.85 if is_selected else 0.35
                marker.text = json.dumps({
                    "id": candidate.get("id", f"T{index + 1}"),
                    "map_ids": map_ids,
                    "residual": round(float(candidate.get("residual", 999.0)), 3),
                    "score": round(float(candidate.get("score", 999.0)), 3),
                    "selected": is_selected,
                }, separators=(",", ":"))
                pts = [(m.reflector.x, m.reflector.y) for m in candidate_matches]
                for a, b in ((0, 1), (1, 2), (2, 0)):
                    if a < len(pts) and b < len(pts):
                        marker.points.append(RosPoint(x=pts[a][0], y=pts[a][1], z=0.16))
                        marker.points.append(RosPoint(x=pts[b][0], y=pts[b][1], z=0.16))
                markers.markers.append(marker)

        self.marker_pub.publish(markers)

    def _publish_status(
        self,
        scan: LaserScan,
        mode: str,
        detections: Sequence[Detection],
        matches: Sequence[Match],
        accepted: bool,
        residual: Optional[float],
        reason: str,
        solved_pose: Optional[Tuple[float, float, float]],
        candidates: Sequence[Dict[str, object]] = (),
        selected_candidate: Optional[Dict[str, object]] = None,
    ) -> None:
        candidate_payload = []
        for candidate in list(candidates)[: int(self.get_parameter("max_triangle_candidates").value)]:
            candidate_payload.append({
                "id": candidate.get("id", ""),
                "map_ids": list(candidate.get("map_ids", ())),
                "residual": float(candidate.get("residual", 999.0)),
                "score": float(candidate.get("score", 999.0)),
            })
        status = {
            "stamp_sec": float(scan.header.stamp.sec) + float(scan.header.stamp.nanosec) * 1e-9,
            "mode": mode,
            "detected": len(detections),
            "matched": len(matches),
            "matched_ids": [match.reflector.reflector_id for match in matches],
            "selected_ids": list(self.selected_reflector_ids or ()),
            "selected_candidate": selected_candidate.get("id") if selected_candidate else None,
            "candidates": candidate_payload,
            "residual": residual,
            "accepted": accepted,
            "confidence": 100.0 if accepted else 0.0,
            "reason": reason,
            "odom_pose": self._pose_to_dict(self.last_odom_pose),
            "corrected_pose": self._pose_to_dict(solved_pose if accepted else self.last_corrected_pose),
        }
        msg = String()
        msg.data = json.dumps(status, sort_keys=True)
        self.status_pub.publish(msg)

        now = self.get_clock().now().nanoseconds * 1e-9
        if now - self.last_log_time > 2.0:
            self.last_log_time = now
            residual_text = "none" if residual is None else f"{residual:.3f} m"
            self.get_logger().info(
                f"reflectors detected={len(detections)} matched={len(matches)} "
                f"residual={residual_text} correction={reason}"
            )

    def _laser_to_base(self, point_laser: Point, laser_frame: str) -> Point:
        tx, ty, yaw = self._lookup_laser_to_base(laser_frame)
        return transform_point(point_laser, tx, ty, yaw)

    def _base_to_laser(self, point_base: Point, laser_frame: str) -> Point:
        tx, ty, yaw = self._lookup_laser_to_base(laser_frame)
        return inverse_transform_point(point_base, tx, ty, yaw)

    def _lookup_laser_to_base(self, laser_frame: str) -> Tuple[float, float, float]:
        if not laser_frame or laser_frame == self.base_frame:
            return (0.0, 0.0, 0.0)
        try:
            transform = self.tf_buffer.lookup_transform(self.base_frame, laser_frame, rclpy.time.Time())
            translation = transform.transform.translation
            rotation = transform.transform.rotation
            return (translation.x, translation.y, yaw_from_quaternion(rotation))
        except TransformException:
            return (
                float(self.get_parameter("fallback_laser_to_base_x").value),
                float(self.get_parameter("fallback_laser_to_base_y").value),
                float(self.get_parameter("fallback_laser_to_base_yaw").value),
            )

    @staticmethod
    def _pose_to_dict(pose: Optional[Tuple[float, float, float]]) -> Optional[Dict[str, float]]:
        if pose is None:
            return None
        return {"x": pose[0], "y": pose[1], "yaw": pose[2]}

    @staticmethod
    def _sorted_side_lengths(points: Sequence[Point]) -> List[float]:
        return sorted(
            [
                distance(points[0], points[1]),
                distance(points[1], points[2]),
                distance(points[2], points[0]),
            ]
        )


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ReflectorLocalizer()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
