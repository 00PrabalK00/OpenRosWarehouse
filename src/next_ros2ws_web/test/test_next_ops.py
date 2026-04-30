import sys
from pathlib import Path


PACKAGE_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

from next_ops import (  # noqa: E402
    generated_bundle,
    generate_static_transforms,
    generate_urdf,
    generate_xacro,
    permission_payload,
    validate_robot_model,
)


def _profile():
    return {
        "robot_id": "Robot1",
        "base_frame": "base_link",
        "sensor_frames": {"odom": "odom"},
    }


def _model():
    return {
        "base_link": "base_link",
        "links": [
            {"name": "base_link", "parent": "", "joint_xyz": [0, 0, 0], "joint_rpy": [0, 0, 0]},
            {
                "name": "front_laser",
                "parent": "base_link",
                "joint_xyz": [0.3, 0, 0.2],
                "joint_rpy": [0, 0, 0],
            },
        ],
        "shapes": [
            {
                "id": "base_chassis",
                "name": "base_chassis",
                "semantic_type": "chassis",
                "type": "box",
                "link": "base_link",
                "width": 0.5,
                "length": 0.8,
                "height": 0.3,
            },
            {
                "id": "front_laser",
                "name": "front_laser",
                "semantic_type": "laser",
                "type": "cylinder",
                "link": "front_laser",
                "child_frame": "front_laser",
                "topic": "/scan",
                "driver": "scanner_driver",
                "radius": 0.05,
                "height": 0.08,
                "range_m": 15.0,
            },
            {
                "id": "collision_footprint",
                "name": "collision_footprint",
                "semantic_type": "collision",
                "type": "box",
                "link": "base_link",
                "width": 0.55,
                "length": 0.85,
                "height": 0.05,
            },
        ],
    }


def test_permission_payload_admin_can_push():
    payload = permission_payload("admin")
    assert payload["can_push"] is True
    assert payload["can_rollback"] is True


def test_validation_reports_ready_model_without_critical_errors():
    report = validate_robot_model(_profile(), _model())
    assert report["ok"] is True
    assert report["ready_to_deploy"] is True
    assert report["summary"]["critical_errors"] == 0


def test_validation_reports_sensor_topic_error():
    model = _model()
    model["shapes"][1]["topic"] = ""
    report = validate_robot_model(_profile(), model)
    assert report["ready_to_deploy"] is False
    assert any(issue["code"] == "sensor_topic_required" for issue in report["errors"])


def test_generated_bundle_is_deterministic_for_same_input():
    first = generated_bundle(_profile(), _model())
    second = generated_bundle(_profile(), _model())
    assert first["manifest"]["checksum"] == second["manifest"]["checksum"]
    assert "nav2_geometry.yaml" in first["manifest"]["generated_files"]
    assert first["files"]["robot_model.urdf"] == second["files"]["robot_model.urdf"]
    assert first["files"]["robot_model.xacro"] == second["files"]["robot_model.xacro"]


def test_generated_urdf_and_xacro_include_real_model_geometry():
    urdf = generate_urdf(_model(), _profile())
    xacro = generate_xacro(_model(), _profile())
    assert '<robot name="Robot1">' in urdf
    assert 'base_chassis_visual' in urdf
    assert 'collision_footprint_collision' in urdf
    assert 'front_laser_visual">\n      <origin xyz="0 0 0" rpy="0 0 0"/>' in urdf
    assert 'xmlns:xacro="http://ros.org/wiki/xacro"' in xacro


def test_static_transforms_include_shape_child_frames_not_declared_as_links():
    model = _model()
    model["shapes"][1]["child_frame"] = "rear_laser"
    model["shapes"][1]["link"] = "base_link"
    transforms = generate_static_transforms(model)
    assert any(item["child_frame"] == "rear_laser" for item in transforms)
