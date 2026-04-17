"""
Simple shelf detector — intensity-based reflector detection with proximity grouping.

Inspired by laser_centroid / laser_leg_shelf_detect reference implementations.
Keeps it dead simple: no DBSCAN, no EMA, no multi-frame consistency latch.

Pipeline (per scan):
  1. Filter LaserScan by intensity threshold → Cartesian points
  2. Greedy proximity grouping → clusters (one per reflector)
  3. Compute centroid of each cluster
  4. If 4 clusters: pair into front/back by distance → shelf center + yaw
     If 2 clusters: use as one pair (width) → midpoint + yaw estimate
  5. Transform pose from laser frame → base_link so inserter gets robot-relative coords
  6. Publish /shelf/status_json, provide /shelf/commit and /shelf/set_enabled

The UI shelf popup reads status_json and calls commit/enable.
The shelf_inserter reads status_json for live center_pose and committed_target_pose.
All poses are published in base_link frame (x+ = robot forward).
"""

import json
import math
import time

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import LaserScan
from std_msgs.msg import String
from std_srvs.srv import Trigger, SetBool
import tf2_ros
from geometry_msgs.msg import PointStamped
from tf2_geometry_msgs import do_transform_point


def _normalize(a: float) -> float:
    if not math.isfinite(a):
        return 0.0
    a = a % (2.0 * math.pi)
    if a > math.pi:
        a -= 2.0 * math.pi
    return a


def _dist(a, b):
    return math.hypot(a[0] - b[0], a[1] - b[1])


class ShelfDetector(Node):
    def __init__(self):
        super().__init__('shelf_detector')

        # --- Parameters ---
        self.declare_parameter('scan_topic', '/scan')
        self.declare_parameter('base_frame', 'base_link')
        self.declare_parameter('intensity_threshold', 35.0)
        self.declare_parameter('proximity_threshold', 0.15)
        self.declare_parameter('min_cluster_points', 2)
        self.declare_parameter('max_cluster_spread', 0.12)
        self.declare_parameter('expected_width_min', 0.55)
        self.declare_parameter('expected_width_max', 0.85)
        self.declare_parameter('expected_depth_min', 0.50)
        self.declare_parameter('expected_depth_max', 1.40)
        self.declare_parameter('max_range', 2.5)
        self.declare_parameter('status_topic', '/shelf/status_json')
        self.declare_parameter('publish_rate', 10.0)

        self._p = {}
        for p in self._parameters:
            self._p[p] = self.get_parameter(p).value

        # --- TF ---
        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self)

        # --- State ---
        self.enabled = False
        self.committed_pose = None
        self.live_pose = None
        self.hotspot_count = 0
        self.hotspot_points = []
        self.max_intensity = 0.0
        self.last_reason = 'detector_disabled'
        self.last_solve_time = 0.0
        self.candidate_valid = False
        self.front_intensity_sum = 0.0
        self.back_intensity_sum = 0.0

        # --- ROS interfaces ---
        self.scan_sub = self.create_subscription(
            LaserScan, self._p['scan_topic'], self._scan_cb, 10)

        self.status_pub = self.create_publisher(
            String, self._p['status_topic'], 10)

        self.commit_srv = self.create_service(
            Trigger, '/shelf/commit', self._commit_cb)
        self.enable_srv = self.create_service(
            SetBool, '/shelf/set_enabled', self._enable_cb)

        dt = 1.0 / self._p['publish_rate']
        self.status_timer = self.create_timer(dt, self._publish_status)

        self.get_logger().info(
            f'shelf_detector ready — scan={self._p["scan_topic"]} '
            f'intensity_thr={self._p["intensity_threshold"]}')

    # ---- TF helper ----
    def _transform_point(self, x, y, source_frame):
        """Transform a point from source_frame to base_link. Returns (x, y) or None."""
        base = self._p['base_frame']
        if source_frame == base:
            return (x, y)
        try:
            tf = self.tf_buffer.lookup_transform(base, source_frame, rclpy.time.Time())
            pt = PointStamped()
            pt.header.frame_id = source_frame
            pt.point.x = x
            pt.point.y = y
            pt.point.z = 0.0
            pt_out = do_transform_point(pt, tf)
            return (pt_out.point.x, pt_out.point.y)
        except (tf2_ros.LookupException, tf2_ros.ConnectivityException,
                tf2_ros.ExtrapolationException):
            return None

    def _get_yaw_offset(self, source_frame):
        """Get the yaw rotation from source_frame to base_link."""
        base = self._p['base_frame']
        if source_frame == base:
            return 0.0
        try:
            tf = self.tf_buffer.lookup_transform(base, source_frame, rclpy.time.Time())
            q = tf.transform.rotation
            # Extract yaw from quaternion
            siny_cosp = 2.0 * (q.w * q.z + q.x * q.y)
            cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
            return math.atan2(siny_cosp, cosy_cosp)
        except (tf2_ros.LookupException, tf2_ros.ConnectivityException,
                tf2_ros.ExtrapolationException):
            return None

    # ---- Services ----
    def _commit_cb(self, req, resp):
        if self.live_pose and self.candidate_valid:
            self.committed_pose = dict(self.live_pose)
            self.last_reason = 'committed'
            resp.success = True
            resp.message = 'Shelf pose committed'
            self.get_logger().info(
                f'COMMITTED shelf at ({self.committed_pose["x"]:.3f}, '
                f'{self.committed_pose["y"]:.3f}, '
                f'yaw={math.degrees(self.committed_pose["yaw"]):.1f}°)')
        elif not self.enabled:
            resp.success = False
            resp.message = 'Detector is disabled'
        elif not self.candidate_valid:
            resp.success = False
            resp.message = 'candidate_not_consistent'
            self.last_reason = 'candidate_not_consistent'
        else:
            resp.success = False
            resp.message = 'No shelf pose available'
        return resp

    def _enable_cb(self, req, resp):
        self.enabled = req.data
        if not self.enabled:
            self.live_pose = None
            self.committed_pose = None
            self.candidate_valid = False
            self.hotspot_count = 0
            self.hotspot_points = []
            self.max_intensity = 0.0
            self.last_reason = 'detector_disabled'
        else:
            self.last_reason = 'waiting_for_scan'
        resp.success = True
        resp.message = f'Detector {"enabled" if self.enabled else "disabled"}'
        self.get_logger().info(f'Detector {"enabled" if self.enabled else "disabled"}')
        return resp

    # ---- Scan callback ----
    def _scan_cb(self, msg: LaserScan):
        if not self.enabled:
            return

        frame_id = msg.header.frame_id

        # Step 1: intensity filter → Cartesian points in laser frame
        points, intensities = self._filter_points(msg)
        self.max_intensity = max(intensities) if intensities else 0.0

        if len(points) < 2:
            self.candidate_valid = False
            self.live_pose = None
            self.hotspot_count = 0
            self.hotspot_points = []
            self.last_reason = f'too_few_points ({len(points)})'
            return

        # Step 2: proximity grouping (in laser frame)
        clusters, cluster_intensities = self._group_points(points, intensities)
        self.hotspot_count = len(clusters)

        if len(clusters) < 2:
            self.candidate_valid = False
            self.live_pose = None
            self.hotspot_points = [[p[0], p[1]] for p in points]
            self.last_reason = f'too_few_clusters ({len(clusters)})'
            return

        # Compute centroids in laser frame
        centroids_laser = []
        centroid_intensities = []
        for cl, cl_int in zip(clusters, cluster_intensities):
            cx = sum(p[0] for p in cl) / len(cl)
            cy = sum(p[1] for p in cl) / len(cl)
            centroids_laser.append((cx, cy))
            centroid_intensities.append(sum(cl_int))

        # Transform centroids to base_link
        centroids = []
        for c in centroids_laser:
            transformed = self._transform_point(c[0], c[1], frame_id)
            if transformed is None:
                # TF not available yet — use laser frame as-is
                centroids = centroids_laser
                break
            centroids.append(transformed)

        self.hotspot_points = [[c[0], c[1]] for c in centroids]

        # Get yaw offset for transforming angles
        yaw_offset = self._get_yaw_offset(frame_id)
        if yaw_offset is None:
            yaw_offset = 0.0

        # Step 3: solve shelf geometry (in base_link frame)
        pose = self._solve_shelf(centroids, centroid_intensities, yaw_offset)

        if pose is None:
            self.candidate_valid = False
            self.live_pose = None
            self.last_reason = 'no_valid_geometry'
            return

        self.live_pose = pose
        self.candidate_valid = True
        self.last_solve_time = time.time()
        self.last_reason = ''

    def _filter_points(self, msg: LaserScan):
        """Filter scan by intensity and range, return Cartesian points + intensities."""
        threshold = self._p['intensity_threshold']
        max_range = self._p['max_range']
        points = []
        intensities = []
        for i in range(len(msg.ranges)):
            r = msg.ranges[i]
            if not math.isfinite(r) or r > max_range or r < 0.01:
                continue
            intensity = msg.intensities[i] if i < len(msg.intensities) else 0.0
            if intensity < threshold:
                continue
            angle = msg.angle_min + i * msg.angle_increment
            x = r * math.cos(angle)
            y = r * math.sin(angle)
            points.append((x, y))
            intensities.append(intensity)
        return points, intensities

    def _group_points(self, points, intensities):
        """Greedy single-linkage proximity grouping."""
        threshold = self._p['proximity_threshold']
        min_pts = self._p['min_cluster_points']
        max_spread = self._p['max_cluster_spread']

        remaining = list(zip(points, intensities))
        clusters = []
        cluster_intensities = []

        while remaining:
            seed_pt, seed_int = remaining.pop(0)
            group = [seed_pt]
            group_int = [seed_int]

            changed = True
            while changed:
                changed = False
                still_remaining = []
                for pt, inten in remaining:
                    if any(_dist(pt, gp) < threshold for gp in group):
                        group.append(pt)
                        group_int.append(inten)
                        changed = True
                    else:
                        still_remaining.append((pt, inten))
                remaining = still_remaining

            if len(group) < min_pts:
                continue
            if len(group) >= 2:
                max_d = max(_dist(group[i], group[j])
                            for i in range(len(group))
                            for j in range(i + 1, len(group)))
                if max_d > max_spread:
                    continue

            clusters.append(group)
            cluster_intensities.append(group_int)

        return clusters, cluster_intensities

    def _solve_shelf(self, centroids, centroid_intensities, yaw_offset):
        """
        Given 2-4 cluster centroids (already in base_link frame),
        compute shelf center + yaw.
        """
        n = len(centroids)
        w_min = self._p['expected_width_min']
        w_max = self._p['expected_width_max']
        d_min = self._p['expected_depth_min']
        d_max = self._p['expected_depth_max']

        if n == 4:
            return self._solve_4(centroids, centroid_intensities, yaw_offset,
                                 w_min, w_max, d_min, d_max)
        elif n == 3:
            return self._solve_3(centroids, centroid_intensities, yaw_offset,
                                 w_min, w_max)
        elif n == 2:
            return self._solve_2(centroids, centroid_intensities, yaw_offset,
                                 w_min, w_max)
        else:
            return None

    def _solve_4(self, centroids, intensities, yaw_offset, w_min, w_max, d_min, d_max):
        """4 centroids: pair into front/back by distance from robot."""
        indexed = sorted(range(4), key=lambda i: math.hypot(*centroids[i]))

        front_pair = [indexed[0], indexed[1]]
        back_pair = [indexed[2], indexed[3]]

        fw = _dist(centroids[front_pair[0]], centroids[front_pair[1]])
        bw = _dist(centroids[back_pair[0]], centroids[back_pair[1]])

        if not (w_min <= fw <= w_max and w_min <= bw <= w_max):
            return self._solve_2_best_pair(centroids, intensities, yaw_offset, w_min, w_max)

        fm = ((centroids[front_pair[0]][0] + centroids[front_pair[1]][0]) / 2,
              (centroids[front_pair[0]][1] + centroids[front_pair[1]][1]) / 2)
        bm = ((centroids[back_pair[0]][0] + centroids[back_pair[1]][0]) / 2,
              (centroids[back_pair[0]][1] + centroids[back_pair[1]][1]) / 2)
        depth = _dist(fm, bm)

        if not (d_min <= depth <= d_max):
            return self._solve_2_best_pair(centroids, intensities, yaw_offset, w_min, w_max)

        cx = (fm[0] + bm[0]) / 2
        cy = (fm[1] + bm[1]) / 2

        # Yaw = direction from front to back (into the shelf)
        yaw = _normalize(math.atan2(bm[1] - fm[1], bm[0] - fm[0]))

        self.front_intensity_sum = sum(intensities[i] for i in front_pair)
        self.back_intensity_sum = sum(intensities[i] for i in back_pair)

        self.get_logger().info(
            f'Solve 4-hotspot: center=({cx:.3f},{cy:.3f}) '
            f'yaw={math.degrees(yaw):.1f}° fw={fw:.3f} bw={bw:.3f} depth={depth:.3f}')

        return {
            'x': cx, 'y': cy, 'yaw': yaw,
            'frame_id': self._p['base_frame'],
            'front_width': fw, 'back_width': bw,
            'depth': depth,
        }

    def _solve_3(self, centroids, intensities, yaw_offset, w_min, w_max):
        return self._solve_2_best_pair(centroids, intensities, yaw_offset, w_min, w_max)

    def _solve_2_best_pair(self, centroids, intensities, yaw_offset, w_min, w_max):
        best = None
        best_err = float('inf')
        n = len(centroids)
        target_w = (w_min + w_max) / 2

        for i in range(n):
            for j in range(i + 1, n):
                w = _dist(centroids[i], centroids[j])
                if w_min <= w <= w_max:
                    err = abs(w - target_w)
                    if err < best_err:
                        best_err = err
                        best = (i, j)

        if best is None:
            return None

        return self._solve_2(
            [centroids[best[0]], centroids[best[1]]],
            [intensities[best[0]], intensities[best[1]]],
            yaw_offset, w_min, w_max)

    def _solve_2(self, centroids, intensities, yaw_offset, w_min, w_max):
        """2 centroids: treat as width pair, compute midpoint + perpendicular yaw."""
        w = _dist(centroids[0], centroids[1])
        if not (w_min <= w <= w_max):
            return None

        mx = (centroids[0][0] + centroids[1][0]) / 2
        my = (centroids[0][1] + centroids[1][1]) / 2

        # The pair line connects the two reflectors (the shelf opening edge).
        # We need the yaw PERPENDICULAR to this line, pointing from robot into shelf.
        dx = centroids[1][0] - centroids[0][0]
        dy = centroids[1][1] - centroids[0][1]
        # Perpendicular direction: rotate pair vector 90° — two options (±90°)
        perp_a = _normalize(math.atan2(dx, -dy))   # +90° rotation
        perp_b = _normalize(math.atan2(-dx, dy))    # -90° rotation
        # Pick the one that points from robot (origin) toward the midpoint
        bearing = math.atan2(my, mx)
        if abs(_normalize(perp_a - bearing)) < abs(_normalize(perp_b - bearing)):
            yaw = perp_a
        else:
            yaw = perp_b

        self.front_intensity_sum = sum(intensities)
        self.back_intensity_sum = 0.0

        self.get_logger().info(
            f'Solve 2-hotspot: center=({mx:.3f},{my:.3f}) '
            f'yaw={math.degrees(yaw):.1f}° width={w:.3f}')

        return {
            'x': mx, 'y': my, 'yaw': yaw,
            'frame_id': self._p['base_frame'],
            'front_width': w, 'back_width': -1.0,
            'depth': -1.0,
        }

    # ---- Status publishing ----
    def _publish_status(self):
        total_inten = self.front_intensity_sum + self.back_intensity_sum
        if total_inten > 0 and self.back_intensity_sum > 0:
            balance = abs(self.front_intensity_sum - self.back_intensity_sum) / total_inten
        else:
            balance = 1.0

        status = {
            'ok': True,
            'detector_enabled': self.enabled,
            'candidate_valid': self.enabled and self.candidate_valid,
            'candidate_consistent': self.enabled and self.candidate_valid,
            'committed_target_valid': self.committed_pose is not None,
            'committed_target_pose': self.committed_pose,
            'center_pose': self.live_pose,
            'hotspot_count': self.hotspot_count,
            'hotspot_points': self.hotspot_points,
            'max_intensity': self.max_intensity,
            'last_reason': self.last_reason,
            'candidate_intensity_balance_ratio': balance,
            'intensity_balance_ratio': balance,
            'front_intensity_sum': self.front_intensity_sum,
            'back_intensity_sum': self.back_intensity_sum,
        }

        msg = String()
        msg.data = json.dumps(status)
        self.status_pub.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = ShelfDetector()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == '__main__':
    main()
