#!/usr/bin/env python3
import json
import os
import shlex
import signal
import subprocess
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import rclpy
from rclpy.node import Node
from lifecycle_msgs.srv import GetState
from nav2_msgs.srv import ManageLifecycleNodes
from next_ros2ws_interfaces.srv import SetStackMode
from std_srvs.srv import Trigger
from ament_index_python.packages import get_package_share_directory, PackageNotFoundError


_BRINGUP_ENV_VARS = ('NEXT_BRINGUP_PACKAGE', 'NEXT_ROBOT_PACKAGE')
_DEFAULT_BRINGUP_PACKAGE = 'ugv_bringup'


def _package_name_from_xml(package_xml_path: str) -> str:
    try:
        root = ET.parse(package_xml_path).getroot()
    except Exception:
        return ''
    name_node = root.find('name')
    if name_node is None:
        return ''
    return str(name_node.text or '').strip()


def _candidate_bringup_packages(search_roots=None):
    for env_name in _BRINGUP_ENV_VARS:
        value = str(os.getenv(env_name, '') or '').strip()
        if value:
            yield value

    roots = []
    if search_roots:
        for root in search_roots:
            if root:
                roots.append(os.path.abspath(os.path.expanduser(str(root))))

    cur = os.path.abspath(os.path.dirname(__file__))
    for _ in range(10):
        roots.append(cur)
        cur = os.path.dirname(cur)

    seen_roots = set()
    for root in roots:
        if not root or root in seen_roots:
            continue
        seen_roots.add(root)
        for package_xml in (
            os.path.join(root, 'package.xml'),
            os.path.join(root, 'src', 'ui_ws', 'package.xml'),
        ):
            package_name = _package_name_from_xml(package_xml)
            if package_name:
                yield package_name

    yield _DEFAULT_BRINGUP_PACKAGE


def _resolve_bringup_package_share(search_roots=None):
    seen = set()
    for package_name in _candidate_bringup_packages(search_roots):
        if not package_name or package_name in seen:
            continue
        seen.add(package_name)
        try:
            return package_name, get_package_share_directory(package_name)
        except PackageNotFoundError:
            continue
        except Exception:
            continue
    return _DEFAULT_BRINGUP_PACKAGE, ''


def _resolve_ui_root():
    # First try environment variable
    env_root = os.getenv('NEXT_UI_ROOT')
    if env_root:
        return os.path.abspath(os.path.expanduser(env_root))

    def _looks_like_ui_root(path):
        return (
            os.path.isfile(os.path.join(path, 'scripts', 'nav2_launch_wrapper.sh')) or
            os.path.isfile(os.path.join(path, 'navigation_bringup.launch.py'))
        )

    # Try to find wrapper script by walking up from current file
    cur = os.path.abspath(os.path.dirname(__file__))
    for _ in range(10):
        if _looks_like_ui_root(cur):
            return cur
        nested = os.path.join(cur, 'src', 'ui_ws')
        if _looks_like_ui_root(nested):
            return os.path.join(cur, 'src', 'ui_ws')
        cur = os.path.dirname(cur)

    # Try using ament_index with resolved bringup package name.
    _package_name, share_dir = _resolve_bringup_package_share(search_roots=[os.getcwd()])
    if share_dir and os.path.isdir(share_dir):
        real_share = os.path.realpath(share_dir)
        if os.path.isdir(real_share):
            return real_share

    # Try current working directory
    cwd = os.path.abspath(os.getcwd())
    nested = os.path.join(cwd, 'src', 'ui_ws')
    if _looks_like_ui_root(nested):
        return nested

    # Last resort: return cwd
    return os.path.abspath(os.getcwd())


def _find_setup_bash(start_dir):
    cur = os.path.abspath(start_dir)
    for _ in range(6):
        candidate = os.path.join(cur, 'install', 'local_setup.bash')
        if os.path.isfile(candidate):
            return candidate
        candidate = os.path.join(cur, 'install', 'setup.bash')
        if os.path.isfile(candidate):
            return candidate
        cur = os.path.dirname(cur)
    return os.path.join('install', 'local_setup.bash')


class StackManager(Node):
    def __init__(self):
        super().__init__("stack_manager")

        self.current_mode = "stopped"
        self.nav_proc = None
        self.slam_proc = None
        self.nav_log_fp = None
        self.slam_log_fp = None
        self.nav_pgid = None
        self.slam_pgid = None

        # Repo where install/setup.bash and launch files live
        self.repo_dir = _resolve_ui_root()
        self.bringup_package, _bringup_share = _resolve_bringup_package_share(
            search_roots=[self.repo_dir, os.getcwd()]
        )
        self.setup_bash = _find_setup_bash(self.repo_dir)
        self.log_dir = os.path.join(self.repo_dir, 'log', 'stack_manager')
        os.makedirs(self.log_dir, exist_ok=True)
        self.owner_lock_path = os.path.join(self.log_dir, 'owner.lock')
        self.owner_pid = os.getpid()
        self._stop_event = threading.Event()
        self._nav_health_lock = threading.Lock()
        self._nav_health = {
            "nav_ready": False,
            "nav_lifecycle_state": "inactive",
        }
        self._nav_lifecycle_nodes = (
            "controller_server",
            "planner_server",
            "behavior_server",
            "bt_navigator",
        )
        self._nav_state_clients = {
            node_name: self.create_client(GetState, f"/{node_name}/get_state")
            for node_name in self._nav_lifecycle_nodes
        }
        self._nav_manage_client = self.create_client(
            ManageLifecycleNodes,
            "/lifecycle_manager_navigation/manage_nodes",
        )
        self._nav_startup_thread = None
        self._nav_startup_lock = threading.Lock()
        
        # Log resolved paths for debugging
        self.get_logger().info(f"StackManager initialized:")
        self.get_logger().info(f"  repo_dir: {self.repo_dir}")
        self.get_logger().info(f"  bringup_package: {self.bringup_package}")
        self.get_logger().info(f"  setup_bash: {self.setup_bash}")
        self.get_logger().info(f"  setup_bash exists: {os.path.isfile(self.setup_bash)}")
        self.get_logger().info(f"  stack logs: {self.log_dir}")

        if not self._acquire_owner_lock():
            self.get_logger().warn(
                f"Another stack_manager owner is already active (pid={self._read_owner_pid()}). "
                "Exiting duplicate instance."
            )
            raise SystemExit(0)

        self.srv = self.create_service(SetStackMode, "/stack/set_mode", self.set_mode_callback)
        self.status_srv = self.create_service(Trigger, "/stack/status", self.status_callback)
        self.shutdown_srv = self.create_service(Trigger, "/stack/shutdown", self.shutdown_callback)

        # Optional auto-start mode so one launch command can bring up a usable stack.
        self.startup_mode = str(self.declare_parameter('startup_mode', '').value or '').strip().lower()
        self._startup_timer = None
        if self.startup_mode:
            self.get_logger().info(f"Configured startup_mode={self.startup_mode}; scheduling stack autostart.")
            self._startup_timer = self.create_timer(1.0, self._startup_once)
        self._nav_health_thread = threading.Thread(target=self._nav_health_poll_loop, daemon=True)
        self._nav_health_thread.start()

    @staticmethod
    def _is_running(proc) -> bool:
        return proc is not None and proc.poll() is None

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return False
        return True

    def _read_owner_pid(self) -> int:
        try:
            if not os.path.exists(self.owner_lock_path):
                return 0
            with open(self.owner_lock_path, "r", encoding="utf-8") as f:
                payload = json.load(f) or {}
            return int(payload.get("pid", 0) or 0)
        except Exception:
            return 0

    def _acquire_owner_lock(self) -> bool:
        existing_pid = self._read_owner_pid()
        if existing_pid and existing_pid != self.owner_pid and self._pid_alive(existing_pid):
            return False

        payload = {
            "pid": int(self.owner_pid),
            "created_at": time.time(),
            "repo_dir": self.repo_dir,
        }
        with open(self.owner_lock_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        return True

    def _release_owner_lock(self):
        try:
            existing_pid = self._read_owner_pid()
            if existing_pid == self.owner_pid and os.path.exists(self.owner_lock_path):
                os.unlink(self.owner_lock_path)
        except Exception:
            pass

    def _call_get_state(self, client, timeout_sec: float = 0.25) -> str:
        if client is None:
            return "unavailable"
        if not client.wait_for_service(timeout_sec=timeout_sec):
            return "unavailable"

        future = client.call_async(GetState.Request())
        done = threading.Event()
        future.add_done_callback(lambda _: done.set())
        if not done.wait(timeout=timeout_sec):
            return "timeout"
        try:
            response = future.result()
        except Exception:
            return "error"
        if response is None or getattr(response, "current_state", None) is None:
            return "unknown"
        label = str(response.current_state.label or "").strip().lower()
        return label or "unknown"

    def _compute_nav_health_snapshot(self):
        if not self._is_running(self.nav_proc):
            return {
                "nav_ready": False,
                "nav_lifecycle_state": "inactive",
            }

        labels = [self._call_get_state(self._nav_state_clients.get(name)) for name in self._nav_lifecycle_nodes]
        if labels and all(label == "active" for label in labels):
            return {
                "nav_ready": True,
                "nav_lifecycle_state": "active",
            }
        if any(label in ("timeout", "error", "unavailable") for label in labels):
            return {
                "nav_ready": False,
                "nav_lifecycle_state": "degraded",
            }
        if any(label == "activating" for label in labels):
            return {
                "nav_ready": False,
                "nav_lifecycle_state": "activating",
            }
        return {
            "nav_ready": False,
            "nav_lifecycle_state": "inactive",
        }

    def _nav_lifecycle_labels(self):
        return {
            node_name: self._call_get_state(self._nav_state_clients.get(node_name))
            for node_name in self._nav_lifecycle_nodes
        }

    def _call_manage_nodes(self, command: int, timeout_sec: float = 15.0):
        client = self._nav_manage_client
        if client is None:
            return False, "lifecycle manager client unavailable"
        if not client.wait_for_service(timeout_sec=min(2.0, timeout_sec)):
            return False, "lifecycle manager service unavailable"

        request = ManageLifecycleNodes.Request()
        request.command = int(command)
        future = client.call_async(request)
        done = threading.Event()
        future.add_done_callback(lambda _: done.set())
        if not done.wait(timeout=timeout_sec):
            return False, "lifecycle manager request timed out"
        try:
            response = future.result()
        except Exception as exc:
            return False, str(exc)
        if response is None:
            return False, "empty lifecycle manager response"
        return bool(getattr(response, "success", False)), str(getattr(response, "message", "") or "")

    def _ensure_nav_lifecycle_started(self):
        with self._nav_startup_lock:
            if self._nav_startup_thread is not None and self._nav_startup_thread.is_alive():
                return
            self._nav_startup_thread = threading.Thread(
                target=self._nav_lifecycle_recovery_loop,
                daemon=True,
            )
            self._nav_startup_thread.start()

    def _nav_lifecycle_recovery_loop(self):
        deadline = time.monotonic() + 45.0
        next_attempt_at = 0.0

        while self._is_running(self.nav_proc) and time.monotonic() < deadline:
            labels = self._nav_lifecycle_labels()
            if labels and all(label == "active" for label in labels.values()):
                self.get_logger().info("Nav lifecycle is active after launch.")
                return

            now_mono = time.monotonic()
            if now_mono < next_attempt_at:
                time.sleep(0.5)
                continue

            if any(label in ("unconfigured", "configuring", "cleaningup", "unknown", "timeout", "unavailable", "error") for label in labels.values()):
                command = ManageLifecycleNodes.Request.STARTUP
                command_name = "STARTUP"
                timeout_sec = 20.0
            elif any(label == "inactive" for label in labels.values()):
                command = ManageLifecycleNodes.Request.RESUME
                command_name = "RESUME"
                timeout_sec = 12.0
            else:
                next_attempt_at = now_mono + 2.0
                time.sleep(0.5)
                continue

            self.get_logger().warn(
                f"Nav lifecycle not ready after launch (states={labels}); requesting {command_name}."
            )
            ok, msg = self._call_manage_nodes(command, timeout_sec=timeout_sec)
            if ok:
                self.get_logger().info(f"Nav lifecycle {command_name} succeeded: {msg or 'ok'}")
            else:
                self.get_logger().warn(f"Nav lifecycle {command_name} did not complete cleanly: {msg}")
            next_attempt_at = time.monotonic() + 4.0

        labels = self._nav_lifecycle_labels()
        self.get_logger().warn(f"Nav lifecycle recovery exited before ready (states={labels})")

    def _nav_health_poll_loop(self):
        while not self._stop_event.wait(1.0):
            snapshot = self._compute_nav_health_snapshot()
            with self._nav_health_lock:
                self._nav_health = dict(snapshot)

    def _startup_once(self):
        if self._startup_timer is not None:
            try:
                self._startup_timer.cancel()
            except Exception:
                pass
            self._startup_timer = None

        mode = (self.startup_mode or '').strip().lower()
        if not mode:
            return

        if mode not in ('nav', 'slam', 'stop'):
            self.get_logger().warn(
                f'Ignoring invalid startup_mode="{mode}" (expected nav|slam|stop).'
            )
            return

        req = SetStackMode.Request()
        req.mode = mode
        resp = SetStackMode.Response()
        resp = self.set_mode_callback(req, resp)

        if resp.ok:
            self.get_logger().info(f'Auto-started stack mode: {mode}')
        else:
            self.get_logger().warn(f'Auto-start stack mode failed: {resp.message}')

    def set_mode_callback(self, request, response):
        mode = request.mode.lower().strip()
        self.get_logger().info(f"Requested mode: {mode}")

        try:
            if mode == "nav":
                self._stop_slam()
                if self._start_nav():
                    self.current_mode = "nav"
                    response.ok = True
                    response.message = "Switched to NAV mode."
                else:
                    response.ok = False
                    response.message = "Failed to start NAV stack."

            elif mode == "slam":
                self._stop_nav()
                if self._start_slam():
                    self.current_mode = "slam"
                    response.ok = True
                    response.message = "Switched to SLAM mode."
                else:
                    response.ok = False
                    response.message = "Failed to start SLAM stack."

            elif mode == "stop":
                self._stop_nav()
                self._stop_slam()
                self.current_mode = "stopped"
                response.ok = True
                response.message = "Stopped all stacks."

            else:
                response.ok = False
                response.message = f"Unknown mode: {mode}"

        except Exception as e:
            response.ok = False
            response.message = f"Error: {str(e)}"

        return response

    def status_callback(self, request, response):
        del request
        try:
            with self._nav_health_lock:
                nav_health = dict(self._nav_health)
            payload = {
                "mode": self.current_mode,
                "nav_running": self._is_running(self.nav_proc),
                "slam_running": self._is_running(self.slam_proc),
                "nav_ready": bool(nav_health.get("nav_ready", False)),
                "nav_lifecycle_state": str(nav_health.get("nav_lifecycle_state", "inactive")),
                "slam_ready": bool(self._is_running(self.slam_proc) and self.count_publishers('/map') > 0),
                "owner_pid": int(self.owner_pid),
                "startup_mode": str(self.startup_mode or ''),
            }
            response.success = True
            response.message = json.dumps(payload)
        except Exception as e:
            response.success = False
            response.message = f"Failed to get stack status: {e}"
        return response

    def shutdown_callback(self, request, response):
        del request
        try:
            self.shutdown_managed_stacks()
            response.success = True
            response.message = "Managed stacks stopped"
        except Exception as e:
            response.success = False
            response.message = f"Failed to stop managed stacks: {e}"
        return response

    def _open_stack_log(self, stack_name: str):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(self.log_dir, f"{stack_name}_{timestamp}.log")
        prefix = f"{stack_name}_"
        suffix = ".log"
        existing_logs = sorted(
            os.path.join(self.log_dir, name)
            for name in os.listdir(self.log_dir)
            if name.startswith(prefix) and name.endswith(suffix)
        )
        for old_path in existing_logs[:-4]:
            try:
                os.unlink(old_path)
            except Exception:
                pass
        log_fp = open(log_path, "a", encoding="utf-8", buffering=1)
        return log_fp, log_path

    @staticmethod
    def _close_log(log_fp):
        if log_fp is None:
            return
        try:
            log_fp.flush()
            log_fp.close()
        except Exception:
            pass

    def _spawn_managed_process(self, stack_name: str, cmd: str):
        env = os.environ.copy()
        env["STACK_MANAGER_MANAGED"] = "1"
        env["STACK_MANAGER_STACK"] = stack_name
        # FastDDS shared-memory transport is currently unstable on this host
        # and causes intermittent topic discovery failures for spawned stacks.
        env.setdefault("FASTDDS_BUILTIN_TRANSPORTS", "UDPv4")

        log_fp, log_path = self._open_stack_log(stack_name)
        self.get_logger().info(f"Launching {stack_name} stack: {cmd}")
        self.get_logger().info(f"{stack_name} logs: {log_path}")
        try:
            proc = subprocess.Popen(
                # Use --noprofile --norc to avoid sourcing .bashrc/.bash_profile.
                # Those files may source a different ROS distro overlay or set
                # conflicting environment variables on headless robot systems.
                # We only need the colcon install/setup.bash overlay sourced.
                ["bash", "--noprofile", "--norc", "-c", cmd],
                cwd=self.repo_dir,
                start_new_session=True,
                stdin=subprocess.DEVNULL,
                stdout=log_fp,
                stderr=subprocess.STDOUT,
                env=env,
            )
            try:
                pgid = os.getpgid(proc.pid)
            except Exception:
                pgid = proc.pid
            return proc, log_fp, pgid
        except Exception:
            self._close_log(log_fp)
            raise

    @staticmethod
    def _process_group_exists(pgid: int) -> bool:
        if pgid is None:
            return False
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return False
        return True

    def _terminate_managed_process(self, proc, stack_name: str, timeout_sec: float = 10.0, pgid_hint=None):
        if proc is None and pgid_hint is None:
            return None

        resolved_pgid = pgid_hint

        if proc is not None:
            try:
                resolved_pgid = os.getpgid(proc.pid)
            except ProcessLookupError:
                if resolved_pgid is None:
                    # Launch-parent may be gone while child nodes in that process group remain.
                    resolved_pgid = proc.pid
            except Exception as e:
                self.get_logger().warn(f"Could not resolve {stack_name} process group: {e}")

        if proc is not None and proc.poll() is not None:
            self.get_logger().info(f"{stack_name} process already exited (code={proc.returncode}).")
        elif proc is not None:
            self.get_logger().info(
                f"Stopping {stack_name} stack (pid={proc.pid}, pgid={resolved_pgid if resolved_pgid is not None else 'unknown'})..."
            )

        if resolved_pgid is not None:
            try:
                os.killpg(resolved_pgid, signal.SIGTERM)
            except ProcessLookupError:
                return None
            except Exception as e:
                self.get_logger().warn(f"Failed to send SIGTERM to {stack_name} process group: {e}")
        elif proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass

        parent_timed_out = False
        if proc is not None and proc.poll() is None:
            try:
                proc.wait(timeout=timeout_sec)
            except subprocess.TimeoutExpired:
                parent_timed_out = True
                self.get_logger().warn(f"{stack_name} did not exit after {timeout_sec:.1f}s; forcing SIGKILL.")
        else:
            # Parent may already be gone; give child processes a short grace period.
            time.sleep(0.2)

        lingering_group = self._process_group_exists(resolved_pgid)
        if parent_timed_out or lingering_group:
            if lingering_group:
                self.get_logger().warn(
                    f"{stack_name} process group {resolved_pgid} still alive after SIGTERM; sending SIGKILL."
                )
            if resolved_pgid is not None:
                try:
                    os.killpg(resolved_pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to SIGKILL {stack_name} process group: {e}")
            elif proc is not None:
                try:
                    proc.kill()
                except Exception:
                    pass

        try:
            if proc is not None:
                proc.wait(timeout=3.0)
        except Exception:
            pass

        self.get_logger().info(f"{stack_name} stack stopped.")
        return None

    def _start_nav(self):
        if self.nav_proc is not None and self.nav_proc.poll() is None:
            self.get_logger().info("Nav2 stack already running.")
            return True

        if not os.path.isfile(self.setup_bash):
            self.get_logger().error(f"Setup bash not found: {self.setup_bash}")
            return False

        use_sim = os.getenv("NEXT_USE_SIM_TIME", "").lower()
        if use_sim:
            # Legacy env-var override (backward compatibility).
            sim_arg = "use_sim_time:=true" if use_sim in ("1", "true", "yes", "on") else "use_sim_time:=false"
        else:
            # Prefer the ROS parameter set at launch time (use_sim_time:=true/false arg).
            try:
                sim_param = bool(self.get_parameter('use_sim_time').value)
            except Exception:
                sim_param = False
            sim_arg = "use_sim_time:=true" if sim_param else "use_sim_time:=false"

        wrapper_script = os.path.join(self.repo_dir, 'scripts', 'nav2_launch_wrapper.sh')
        if os.path.isfile(wrapper_script):
            cmd = (
                f"source {shlex.quote(self.setup_bash)} && "
                f"bash {shlex.quote(wrapper_script)} {sim_arg}"
            )
        else:
            self.get_logger().warn(
                f"Nav wrapper not found at {wrapper_script}; falling back to navigation.launch.py"
            )
            cmd = (
                f"source {shlex.quote(self.setup_bash)} && "
                f"ros2 launch {shlex.quote(self.bringup_package)} navigation.launch.py {sim_arg}"
            )

        try:
            self.nav_proc, self.nav_log_fp, self.nav_pgid = self._spawn_managed_process("nav", cmd)
        except Exception as e:
            self.get_logger().error(f"Failed to start Nav2 stack: {e}")
            self.nav_proc = None
            self.nav_log_fp = None
            self.nav_pgid = None
            return False
        self._ensure_nav_lifecycle_started()
        return True

    def _stop_nav(self):
        self.nav_proc = self._terminate_managed_process(
            self.nav_proc,
            "Nav2",
            timeout_sec=12.0,
            pgid_hint=self.nav_pgid,
        )
        self._close_log(self.nav_log_fp)
        self.nav_log_fp = None
        self.nav_pgid = None

    def _start_slam(self):
        if self.slam_proc is not None and self.slam_proc.poll() is None:
            self.get_logger().info("SLAM stack already running.")
            return True

        if not os.path.isfile(self.setup_bash):
            self.get_logger().error(f"Setup bash not found: {self.setup_bash}")
            return False

        use_sim = os.getenv("NEXT_USE_SIM_TIME", "").lower()
        if use_sim:
            sim_arg = "use_sim_time:=true" if use_sim in ("1", "true", "yes", "on") else "use_sim_time:=false"
        else:
            try:
                sim_param = bool(self.get_parameter('use_sim_time').value)
            except Exception:
                sim_param = False
            sim_arg = "use_sim_time:=true" if sim_param else "use_sim_time:=false"
        cmd = (
            f"source {shlex.quote(self.setup_bash)} && "
            f"ros2 launch {shlex.quote(self.bringup_package)} slam.launch.py {sim_arg}"
        )
        try:
            self.slam_proc, self.slam_log_fp, self.slam_pgid = self._spawn_managed_process("slam", cmd)
        except Exception as e:
            self.get_logger().error(f"Failed to start SLAM stack: {e}")
            self.slam_proc = None
            self.slam_log_fp = None
            self.slam_pgid = None
            return False
        return True

    def _stop_slam(self):
        self.slam_proc = self._terminate_managed_process(
            self.slam_proc,
            "SLAM",
            timeout_sec=12.0,
            pgid_hint=self.slam_pgid,
        )
        self._close_log(self.slam_log_fp)
        self.slam_log_fp = None
        self.slam_pgid = None

    def shutdown_managed_stacks(self):
        """Stop all processes launched by this StackManager instance."""
        self._stop_nav()
        self._stop_slam()
        self.current_mode = "stopped"

    def destroy_node(self):
        self._stop_event.set()
        self.shutdown_managed_stacks()
        self._release_owner_lock()
        return super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = StackManager()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.shutdown_managed_stacks()
        except Exception:
            pass
        try:
            node.destroy_node()
        except Exception:
            pass
        try:
            rclpy.shutdown()
        except Exception:
            pass  # Already shut down


if __name__ == "__main__":
    main()
