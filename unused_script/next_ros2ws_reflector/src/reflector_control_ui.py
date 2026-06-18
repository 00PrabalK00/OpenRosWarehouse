import json
import math
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import Any, Dict, List, Optional

import rclpy
import yaml
from geometry_msgs.msg import PoseWithCovarianceStamped, Twist
from nav_msgs.msg import Odometry
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data
from sensor_msgs.msg import LaserScan
from std_msgs.msg import String

from reflector_localization.reflector_localizer import yaw_from_quaternion


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Reflector AGV Control</title>
  <style>
    :root {
      --ink: #10211d;
      --muted: #55716b;
      --panel: rgba(250, 247, 235, 0.88);
      --line: rgba(16, 33, 29, 0.14);
      --accent: #e8602a;
      --accent-2: #0c8f89;
      --good: #1f9d55;
      --bad: #c2410c;
      --shadow: 0 24px 70px rgba(18, 38, 34, 0.20);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font-family: "Avenir Next", "Trebuchet MS", Verdana, sans-serif;
      background:
        radial-gradient(circle at 12% 16%, rgba(232, 96, 42, 0.26), transparent 28rem),
        radial-gradient(circle at 84% 6%, rgba(12, 143, 137, 0.30), transparent 24rem),
        linear-gradient(135deg, #f4ebcf 0%, #dfe8db 52%, #cbded8 100%);
    }
    body:before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image:
        linear-gradient(rgba(16, 33, 29, 0.045) 1px, transparent 1px),
        linear-gradient(90deg, rgba(16, 33, 29, 0.045) 1px, transparent 1px);
      background-size: 42px 42px;
      mask-image: linear-gradient(to bottom, rgba(0,0,0,0.55), rgba(0,0,0,0.06));
    }
    main {
      width: min(1180px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 38px 0;
      position: relative;
    }
    header {
      display: grid;
      grid-template-columns: 1.3fr 0.7fr;
      gap: 18px;
      align-items: end;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      font-size: clamp(2.3rem, 6vw, 5.6rem);
      line-height: 0.86;
      letter-spacing: -0.08em;
    }
    .lede {
      color: var(--muted);
      font-size: 1.02rem;
      max-width: 32rem;
      margin: 12px 0 0;
    }
    .status-pill {
      justify-self: end;
      padding: 14px 18px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.38);
      backdrop-filter: blur(14px);
      font-weight: 800;
      box-shadow: var(--shadow);
    }
    .status-pill.good { color: var(--good); }
    .status-pill.bad { color: var(--bad); }
    .grid {
      display: grid;
      grid-template-columns: minmax(300px, 0.8fr) minmax(340px, 1.2fr);
      gap: 18px;
    }
    .map-panel {
      grid-column: 2;
      grid-row: 1 / 3;
      display: flex;
      flex-direction: column;
    }
    .map-container {
      flex: 1;
      position: relative;
      background: rgba(16, 33, 29, 0.05);
      border-radius: 18px;
      overflow: hidden;
      min-height: 400px;
    }
    canvas {
      display: block;
      width: 100%;
      height: 100%;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 22px;
      backdrop-filter: blur(18px);
      animation: rise 520ms ease both;
    }
    .panel:nth-child(2) { animation-delay: 80ms; }
    @keyframes rise {
      from { opacity: 0; transform: translateY(14px); }
      to { opacity: 1; transform: translateY(0); }
    }
    h2 {
      margin: 0 0 14px;
      font-size: 1rem;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .drive-pad {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 10px;
      margin: 10px 0 18px;
    }
    button {
      appearance: none;
      border: 0;
      border-radius: 18px;
      min-height: 58px;
      padding: 12px;
      background: #16332d;
      color: #fff8e6;
      font-weight: 900;
      letter-spacing: 0.03em;
      cursor: pointer;
      box-shadow: 0 12px 24px rgba(16, 33, 29, 0.20);
      transition: transform 120ms ease, background 120ms ease;
    }
    button:hover { transform: translateY(-1px); background: #224d44; }
    button.stop { grid-column: span 3; background: var(--accent); color: #1d130d; }
    button.ghost { background: rgba(16,33,29,0.08); color: var(--ink); box-shadow: none; border: 1px solid var(--line); }
    .sliders {
      display: grid;
      gap: 12px;
    }
    label { display: grid; gap: 6px; color: var(--muted); font-weight: 800; }
    input[type="range"] { accent-color: var(--accent-2); width: 100%; }
    .metric-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      background: rgba(255,255,255,0.38);
    }
    .metric .label { color: var(--muted); font-size: 0.78rem; font-weight: 900; text-transform: uppercase; letter-spacing: 0.08em; }
    .metric .value { margin-top: 6px; font-size: 1.35rem; font-weight: 950; }
    pre {
      overflow: auto;
      margin: 0;
      padding: 16px;
      border-radius: 18px;
      background: #10211d;
      color: #eaf5d6;
      font-size: 0.84rem;
      line-height: 1.5;
      max-height: 360px;
    }
    .topic-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      padding: 10px 0;
      border-bottom: 1px solid var(--line);
      font-weight: 800;
    }
    .topic-row:last-child { border-bottom: 0; }
    .topic-row span:last-child { color: var(--muted); font-variant-numeric: tabular-nums; }
    @media (max-width: 820px) {
      header, .grid, .metric-grid { grid-template-columns: 1fr; }
      .status-pill { justify-self: start; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Reflector AGV Control</h1>
        <p class="lede">Drive the simulated robot, watch reflector localization acceptance, and confirm scan/odom/EKF topics from this repo-launched UI.</p>
      </div>
      <div id="health" class="status-pill bad">Waiting for ROS topics</div>
    </header>
    <div class="grid">
      <section class="panel">
        <h2>Manual Drive</h2>
        <div class="drive-pad">
          <button class="ghost" onclick="sendCmd(0.0, yaw(), 0.45)">Turn Left</button>
          <button onclick="sendCmd(speed(), 0.0, 0.65)">Forward</button>
          <button class="ghost" onclick="sendCmd(0.0, -yaw(), 0.45)">Turn Right</button>
          <button class="ghost" onclick="sendCmd(0.0, -yaw()*0.55, 0.35)">Nudge L</button>
          <button onclick="sendCmd(-speed()*0.6, 0.0, 0.45)">Reverse</button>
          <button class="ghost" onclick="sendCmd(0.0, yaw()*0.55, 0.35)">Nudge R</button>
          <button class="stop" onclick="stopRobot()">Stop</button>
        </div>
        <div class="sliders">
          <label>Linear speed <strong><span id="speedLabel">0.35</span> m/s</strong><input id="speed" type="range" min="0.05" max="0.8" step="0.05" value="0.35"></label>
          <label>Yaw rate <strong><span id="yawLabel">0.75</span> rad/s</strong><input id="yaw" type="range" min="0.15" max="1.5" step="0.05" value="0.75"></label>
        </div>
      </section>
      <section class="panel map-panel">
        <h2>Map Visualization</h2>
        <div class="map-container">
          <canvas id="map"></canvas>
        </div>
      </section>
      <section class="panel">
        <h2>Localization</h2>
        <div class="metric-grid">
          <div class="metric"><div class="label">Detected</div><div id="detected" class="value">0</div></div>
          <div class="metric"><div class="label">Matched</div><div id="matched" class="value">0</div></div>
          <div class="metric"><div class="label">Residual</div><div id="residual" class="value">--</div></div>
        </div>
        <div class="topic-row"><span>/scan</span><span id="scanAge">--</span></div>
        <div class="topic-row"><span>/odom</span><span id="odomAge">--</span></div>
        <div class="topic-row"><span>/reflector_pose</span><span id="poseAge">--</span></div>
        <div class="topic-row"><span>/odometry/filtered</span><span id="filteredAge">--</span></div>
      </section>
      <section class="panel" style="grid-column: 1 / -1;">
        <h2>Debug JSON</h2>
        <pre id="debug">{}</pre>
      </section>
    </div>
  </main>
  <script>
    const speedEl = document.getElementById("speed");
    const yawEl = document.getElementById("yaw");
    const canvas = document.getElementById("map");
    const ctx = canvas.getContext("2d");
    const fmtAge = (age) => age === null || age === undefined ? "--" : `${age.toFixed(1)}s ago`;
    const speed = () => Number(speedEl.value);
    const yaw = () => Number(yawEl.value);

    function resizeCanvas() {
      const rect = canvas.parentElement.getBoundingClientRect();
      canvas.width = rect.width * window.devicePixelRatio;
      canvas.height = rect.height * window.devicePixelRatio;
    }
    window.addEventListener("resize", resizeCanvas);
    resizeCanvas();

    function drawMap(data) {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      const reflectors = data.reflectors || [];
      const odom = data.filtered_odom || data.odom;
      
      // Determine bounds
      let minX = 0, maxX = 18, minY = -4, maxY = 12;
      if (reflectors.length > 0) {
        minX = Math.min(...reflectors.map(r => r.x)) - 2;
        maxX = Math.max(...reflectors.map(r => r.x)) + 2;
        minY = Math.min(...reflectors.map(r => r.y)) - 2;
        maxY = Math.max(...reflectors.map(r => r.y)) + 2;
      }
      
      const padding = 40;
      const w = canvas.width - padding * 2;
      const h = canvas.height - padding * 2;
      const scale = Math.min(w / (maxX - minX), h / (maxY - minY));
      
      const toX = (x) => padding + (x - minX) * scale;
      const toY = (y) => canvas.height - (padding + (y - minY) * scale);

      // Draw Grid
      ctx.strokeStyle = "rgba(16, 33, 29, 0.1)";
      ctx.lineWidth = 1;
      for (let x = Math.floor(minX); x <= Math.ceil(maxX); x++) {
        ctx.beginPath(); ctx.moveTo(toX(x), toY(minY)); ctx.lineTo(toX(x), toY(maxY)); ctx.stroke();
      }
      for (let y = Math.floor(minY); y <= Math.ceil(maxY); y++) {
        ctx.beginPath(); ctx.moveTo(toX(minX), toY(y)); ctx.lineTo(toX(maxX), toY(y)); ctx.stroke();
      }

      // Draw Reflectors
      reflectors.forEach(r => {
        ctx.fillStyle = "var(--accent-2)";
        ctx.beginPath();
        ctx.arc(toX(r.x), toY(r.y), 5 * window.devicePixelRatio, 0, Math.PI * 2);
        ctx.fill();
        ctx.fillStyle = "var(--muted)";
        ctx.font = `bold ${10 * window.devicePixelRatio}px sans-serif`;
        ctx.fillText(r.id, toX(r.x) + 8, toY(r.y) + 3);
      });

      // Draw Robot
      if (odom) {
        const rx = toX(odom.x);
        const ry = toY(odom.y);
        ctx.save();
        ctx.translate(rx, ry);
        ctx.rotate(-odom.yaw); // Canvas Y is inverted
        
        // Body
        ctx.fillStyle = "var(--ink)";
        ctx.beginPath();
        ctx.rect(-15 * window.devicePixelRatio, -10 * window.devicePixelRatio, 30 * window.devicePixelRatio, 20 * window.devicePixelRatio);
        ctx.fill();
        
        // Heading
        ctx.strokeStyle = "var(--accent)";
        ctx.lineWidth = 3 * window.devicePixelRatio;
        ctx.beginPath();
        ctx.moveTo(0, 0);
        ctx.lineTo(20 * window.devicePixelRatio, 0);
        ctx.stroke();
        
        ctx.restore();
        
        // Label
        ctx.fillStyle = "var(--ink)";
        ctx.font = `bold ${12 * window.devicePixelRatio}px sans-serif`;
        ctx.fillText("AGV", rx + 18, ry - 18);
      }
    }

    speedEl.addEventListener("input", () => document.getElementById("speedLabel").textContent = speedEl.value);
    yawEl.addEventListener("input", () => document.getElementById("yawLabel").textContent = yawEl.value);
    async function sendCmd(linear, angular, duration) {
      await fetch("/api/cmd", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({linear, angular, duration})});
    }
    async function stopRobot() {
      await fetch("/api/stop", {method: "POST"});
    }
    async function refresh() {
      const data = await fetch("/api/status").then(r => r.json());
      drawMap(data);
      const reflector = data.reflector_status || {};
      document.getElementById("detected").textContent = reflector.detected ?? 0;
      document.getElementById("matched").textContent = reflector.matched ?? 0;
      document.getElementById("residual").textContent = reflector.residual == null ? "--" : `${reflector.residual.toFixed(3)} m`;
      document.getElementById("scanAge").textContent = fmtAge(data.topic_ages.scan);
      document.getElementById("odomAge").textContent = fmtAge(data.topic_ages.odom);
      document.getElementById("poseAge").textContent = fmtAge(data.topic_ages.reflector_pose);
      document.getElementById("filteredAge").textContent = fmtAge(data.topic_ages.filtered_odom);
      const healthy = data.topic_ages.scan !== null && data.topic_ages.odom !== null && reflector.accepted === true;
      const health = document.getElementById("health");
      health.className = `status-pill ${healthy ? "good" : "bad"}`;
      health.textContent = healthy ? "Reflector correction accepted" : "Waiting for accepted correction";
      document.getElementById("debug").textContent = JSON.stringify(data, null, 2);
    }
    setInterval(refresh, 650);
    refresh();
  </script>
</body>
</html>
"""


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class ControlHandler(BaseHTTPRequestHandler):
    server: "ControlHTTPServer"

    def log_message(self, fmt: str, *args: Any) -> None:
        self.server.ros_node.get_logger().debug(fmt % args)

    def do_GET(self) -> None:
        if self.path == "/" or self.path.startswith("/?"):
            self._send(200, HTML, "text/html; charset=utf-8")
            return
        if self.path == "/api/status":
            self._send_json(200, self.server.ros_node.snapshot())
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path == "/api/stop":
            self.server.ros_node.set_command(0.0, 0.0, 0.0)
            self._send_json(200, {"ok": True})
            return
        if self.path == "/api/cmd":
            length = int(self.headers.get("content-length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            linear = float(payload.get("linear", 0.0))
            angular = float(payload.get("angular", 0.0))
            duration = max(0.0, min(5.0, float(payload.get("duration", 0.5))))
            self.server.ros_node.set_command(linear, angular, duration)
            self._send_json(200, {"ok": True, "linear": linear, "angular": angular, "duration": duration})
            return
        self._send_json(404, {"error": "not found"})

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        self._send(status, json.dumps(payload, sort_keys=True).encode("utf-8"), "application/json")

    def _send(self, status: int, body: Any, content_type: str) -> None:
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ControlHTTPServer(ThreadedHTTPServer):
    def __init__(self, address, handler, ros_node: "ReflectorControlUi") -> None:
        super().__init__(address, handler)
        self.ros_node = ros_node


class ReflectorControlUi(Node):
    def __init__(self) -> None:
        super().__init__("reflector_control_ui")
        self.declare_parameter("host", "127.0.0.1")
        self.declare_parameter("port", 8088)
        self.declare_parameter("cmd_vel_topic", "cmd_vel")
        self.declare_parameter("scan_topic", "scan")
        self.declare_parameter("odom_topic", "odom")
        self.declare_parameter("reflector_status_topic", "reflector/status_json")
        self.declare_parameter("reflector_pose_topic", "reflector_pose")
        self.declare_parameter("filtered_odom_topic", "odometry/filtered")
        self.declare_parameter("reflector_map_file", "")

        self.lock = threading.Lock()
        self.latest: Dict[str, Any] = {
            "scan": None,
            "odom": None,
            "reflector_status": None,
            "reflector_pose": None,
            "filtered_odom": None,
        }
        self.current_twist = Twist()
        self.command_until = 0.0
        self.sent_stop = True

        self.reflectors = self._load_reflectors()

        self.cmd_pub = self.create_publisher(Twist, self.get_parameter("cmd_vel_topic").value, 10)
        self.create_subscription(LaserScan, self.get_parameter("scan_topic").value, self._on_scan, qos_profile_sensor_data)
        self.create_subscription(Odometry, self.get_parameter("odom_topic").value, self._on_odom, 30)
        self.create_subscription(String, self.get_parameter("reflector_status_topic").value, self._on_reflector_status, 10)
        self.create_subscription(PoseWithCovarianceStamped, self.get_parameter("reflector_pose_topic").value, self._on_reflector_pose, 10)
        self.create_subscription(Odometry, self.get_parameter("filtered_odom_topic").value, self._on_filtered_odom, 10)
        self.create_timer(0.1, self._publish_command)

        host = self.get_parameter("host").value
        port = int(self.get_parameter("port").value)
        self.httpd = ControlHTTPServer((host, port), ControlHandler, self)
        self.server_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.server_thread.start()
        self.get_logger().info(f"Reflector control UI listening at http://{host}:{port}")

    def _load_reflectors(self) -> List[Dict[str, Any]]:
        path = self.get_parameter("reflector_map_file").value
        if not path or not os.path.exists(path):
            return []
        try:
            with open(path, "r") as f:
                data = yaml.safe_load(f)
                return data.get("reflectors", [])
        except Exception as e:
            self.get_logger().error(f"Failed to load reflectors: {e}")
            return []

    def destroy_node(self) -> bool:
        self.httpd.shutdown()
        self.httpd.server_close()
        return super().destroy_node()

    def set_command(self, linear: float, angular: float, duration: float) -> None:
        with self.lock:
            self.current_twist = Twist()
            self.current_twist.linear.x = max(-0.9, min(0.9, linear))
            self.current_twist.angular.z = max(-1.8, min(1.8, angular))
            self.command_until = time.monotonic() + duration
            self.sent_stop = False
        if duration <= 0.0:
            self.cmd_pub.publish(Twist())

    def snapshot(self) -> Dict[str, Any]:
        now = time.monotonic()
        with self.lock:
            latest = json.loads(json.dumps(self.latest))
        ages = {}
        for key in ("scan", "odom", "reflector_pose", "filtered_odom"):
            stamp = latest.get(key, {}).get("received_monotonic") if latest.get(key) else None
            ages[key] = None if stamp is None else now - stamp
        return {
            "topic_ages": ages,
            "scan": latest.get("scan"),
            "odom": latest.get("odom"),
            "reflector_pose": latest.get("reflector_pose"),
            "filtered_odom": latest.get("filtered_odom"),
            "reflector_status": latest.get("reflector_status"),
            "reflectors": self.reflectors,
            "command_active": now < self.command_until,
        }

    def _publish_command(self) -> None:
        now = time.monotonic()
        with self.lock:
            active = now < self.command_until
            twist = self.current_twist if active else Twist()
            should_publish = active or not self.sent_stop
            if not active:
                self.sent_stop = True
        if should_publish:
            self.cmd_pub.publish(twist)

    def _on_scan(self, msg: LaserScan) -> None:
        finite_ranges = [value for value in msg.ranges if math.isfinite(value)]
        payload = {
            "received_monotonic": time.monotonic(),
            "frame_id": msg.header.frame_id,
            "beam_count": len(msg.ranges),
            "finite_ranges": len(finite_ranges),
            "range_min": min(finite_ranges) if finite_ranges else None,
            "range_max": max(finite_ranges) if finite_ranges else None,
            "intensity_count": len(msg.intensities),
            "strong_intensities": sum(1 for value in msg.intensities if value > 1000.0),
        }
        with self.lock:
            self.latest["scan"] = payload

    def _on_odom(self, msg: Odometry) -> None:
        self._store_odom("odom", msg)

    def _on_filtered_odom(self, msg: Odometry) -> None:
        self._store_odom("filtered_odom", msg)

    def _store_odom(self, key: str, msg: Odometry) -> None:
        pose = msg.pose.pose
        payload = {
            "received_monotonic": time.monotonic(),
            "frame_id": msg.header.frame_id,
            "child_frame_id": msg.child_frame_id,
            "x": pose.position.x,
            "y": pose.position.y,
            "yaw": yaw_from_quaternion(pose.orientation),
            "linear_x": msg.twist.twist.linear.x,
            "angular_z": msg.twist.twist.angular.z,
        }
        with self.lock:
            self.latest[key] = payload

    def _on_reflector_status(self, msg: String) -> None:
        try:
            payload = json.loads(msg.data)
        except json.JSONDecodeError:
            payload = {"raw": msg.data}
        payload["received_monotonic"] = time.monotonic()
        with self.lock:
            self.latest["reflector_status"] = payload

    def _on_reflector_pose(self, msg: PoseWithCovarianceStamped) -> None:
        pose = msg.pose.pose
        payload = {
            "received_monotonic": time.monotonic(),
            "frame_id": msg.header.frame_id,
            "x": pose.position.x,
            "y": pose.position.y,
            "yaw": yaw_from_quaternion(pose.orientation),
            "cov_x": msg.pose.covariance[0],
            "cov_y": msg.pose.covariance[7],
            "cov_yaw": msg.pose.covariance[35],
        }
        with self.lock:
            self.latest["reflector_pose"] = payload


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ReflectorControlUi()
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
