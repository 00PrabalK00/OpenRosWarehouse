import sys
import types
from pathlib import Path

import yaml


WEB_SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
CORE_SRC_ROOT = WEB_SRC_ROOT.parents[1] / "next_ros2ws_core" / "src"

for module_name in (
    "next_ros2ws_web.ros_bridge",
    "next_ros2ws_web",
    "next_ros2ws_core",
):
    sys.modules.pop(module_name, None)

web_package = types.ModuleType("next_ros2ws_web")
web_package.__path__ = [str(WEB_SRC_ROOT)]
core_package = types.ModuleType("next_ros2ws_core")
core_package.__path__ = [str(CORE_SRC_ROOT)]
sys.modules["next_ros2ws_web"] = web_package
sys.modules["next_ros2ws_core"] = core_package

from next_ros2ws_web.ros_bridge import RosBridge  # noqa: E402


class _Logger:
    def warn(self, _message):
        return None


def _bridge(tmp_path):
    bridge = object.__new__(RosBridge)
    bridge.ui_root = str(tmp_path)
    bridge.maps_dir = str(tmp_path / "maps")
    bridge.robot_profiles_dir = str(tmp_path / "profiles")
    bridge.robot_profile_registry_file = str(Path(bridge.robot_profiles_dir) / "registry.yaml")
    bridge.robot_profile_files = {}
    bridge.cached_settings_mappings = {}
    bridge.topic_config = {}
    bridge.active_robot_profile = None
    bridge.active_robot_profile_id = ""
    bridge.get_logger = lambda: _Logger()
    return bridge


def test_sanitize_profile_payload_migrates_legacy_schema():
    profile = RosBridge._sanitize_profile_payload(
        {
            "schema_version": 0,
            "robot_id": "legacy_bot",
            "base_frame": "base_link",
            "nav2_config": {
                "curated": {"controller_server": {"FollowPath.max_vel_x": 0.25}},
                "advanced_overrides": [
                    {"path": "/controller_server/ros__parameters/goal_checker_plugins", "value": ["simple"]}
                ],
            },
        },
        fallback_robot_id="legacy_bot",
    )

    assert profile["schema_version"] == RosBridge.PROFILE_SCHEMA_VERSION
    assert profile["robot_id"] == "legacy_bot"
    assert profile["nav2_config"]["advanced_overrides"][0]["path"] == (
        "controller_server/ros__parameters/goal_checker_plugins"
    )
    assert any("Migrated profile from schema v0" in item for item in profile["migration_warnings"])


def test_load_robot_profiles_prefers_db_and_exports_mirror(tmp_path):
    bridge = _bridge(tmp_path)
    db_profile = RosBridge._sanitize_profile_payload({"robot_id": "RobotDB"}, fallback_robot_id="RobotDB")
    bridge._load_robot_profiles_from_db = lambda: {"RobotDB": db_profile}

    profiles = bridge._load_robot_profiles()

    assert list(profiles.keys()) == ["RobotDB"]
    mirror_path = Path(bridge.robot_profile_files["RobotDB"])
    assert mirror_path.exists()
    payload = yaml.safe_load(mirror_path.read_text(encoding="utf-8"))
    assert payload["robot_id"] == "RobotDB"


def test_load_robot_profiles_imports_legacy_files_when_db_empty(tmp_path):
    bridge = _bridge(tmp_path)
    bridge._load_robot_profiles_from_db = lambda: {}
    saved = {}

    profile_dir = Path(bridge.robot_profiles_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)
    legacy_path = profile_dir / "legacy_bot.yaml"
    legacy_path.write_text("robot_id: legacy_bot\nbase_frame: base_link\n", encoding="utf-8")

    def _save_all_profiles_to_db(profiles):
        saved.update(profiles)

    bridge._save_all_profiles_to_db = _save_all_profiles_to_db

    profiles = bridge._load_robot_profiles()

    assert "legacy_bot" in profiles
    assert "legacy_bot" in saved
    assert Path(bridge.robot_profile_files["legacy_bot"]).exists()


def test_load_robot_profiles_recovers_corrupt_legacy_profile_with_safe_defaults(tmp_path):
    bridge = _bridge(tmp_path)
    bridge._load_robot_profiles_from_db = lambda: {}
    bridge._save_all_profiles_to_db = lambda profiles: None

    profile_dir = Path(bridge.robot_profiles_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "broken.yaml").write_text("robot_id: [unterminated\n", encoding="utf-8")

    profiles = bridge._load_robot_profiles()

    assert "broken" in profiles
    assert profiles["broken"]["robot_id"] == "broken"
    assert profiles["broken"]["base_frame"] == "base_link"
    assert Path(bridge.robot_profile_files["broken"]).exists()


def test_save_profile_to_disk_increments_version_and_writes_mirror(tmp_path):
    bridge = _bridge(tmp_path)
    bridge._load_robot_profiles_from_db = lambda: {}
    captured = {}

    def _save_robot_profile_to_db(profile):
        captured.update(profile)

    bridge._save_robot_profile_to_db = _save_robot_profile_to_db
    saved = bridge._save_profile_to_disk(
        {"robot_id": "RobotSave", "profile_version": 3},
        allow_overwrite=True,
        increment_version=True,
    )

    assert saved["profile_version"] == 4
    assert captured["profile_version"] == 4
    mirror_path = Path(bridge.robot_profile_files["RobotSave"])
    assert mirror_path.exists()
    payload = yaml.safe_load(mirror_path.read_text(encoding="utf-8"))
    assert payload["profile_version"] == 4


def test_sanitize_robot_builder_preserves_device_object_fields():
    raw_shape = {
        "id": "device_1",
        "type": "laser",
        "protocol": "tcp",
        "port": "8080",
        "ip_address": "192.168.1.10",
        "baud_rate": "115200",
        "timeout_s": "1.5",
        "diagnostic_source": "laser_driver",
        "parameters": {"angle_min": -1.57},
        "validation_records": [{"error": "none"}],
    }
    
    sanitized = RosBridge._sanitize_robot_builder({"shapes": [raw_shape]})
    shape = sanitized["shapes"][0]
    
    assert shape["protocol"] == "tcp"
    assert shape["port"] == "8080"
    assert shape["ip_address"] == "192.168.1.10"
    assert shape["baud_rate"] == 115200
    assert shape["timeout_s"] == 1.5
    assert shape["diagnostic_source"] == "laser_driver"
    assert shape["parameters"] == {"angle_min": -1.57}
    assert shape["validation_records"] == [{"error": "none"}]

def test_urdf_import_shapes():
    import rclpy
    from next_ros2ws_web.ros_bridge import RosBridge
    if not rclpy.ok():
        rclpy.init()

    urdf = """<?xml version="1.0" ?>
    <robot name="Robot1">
        <link name="base_link">
            <collision name="chassis">
                <origin xyz="0 0 0.1" rpy="0 0 0" />
                <geometry><box size="1 0.5 0.2"/></geometry>
            </collision>
        </link>
        <link name="laser_front_frame">
            <collision name="laser_front_frame_shape_1">
                <origin xyz="0 0 0" rpy="0 0 0" />
                <geometry><cylinder radius="0.05" length="0.1"/></geometry>
            </collision>
        </link>
        <joint name="laser_front_joint" type="fixed">
            <parent link="base_link"/>
            <child link="laser_front_frame"/>
            <origin xyz="0.45 0 0.2" rpy="0 0 0"/>
        </joint>
    </robot>
    """

    bridge = RosBridge()
    res = bridge._import_robot_builder_from_urdf(urdf, {'base_frame': 'base_link'})
    assert res['ok']
    builder = res['robot_builder']
    assert len(builder['shapes']) == 2
    assert builder['shapes'][1]['semantic_type'] == 'laser'
    assert builder['shapes'][1]['enabled'] is False
    assert builder['links'][1]['joint_xyz'][0] == 0.45

