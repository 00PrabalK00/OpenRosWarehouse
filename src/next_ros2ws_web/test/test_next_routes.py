import os
import sys
from pathlib import Path


PACKAGE_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

os.environ.setdefault("NEXT_WEB_UI_ROLE", "admin")

import zone_web_ui  # noqa: E402


def _profile():
    return {
        "robot_id": "Robot1",
        "base_frame": "base_link",
        "sensor_frames": {"odom": "odom"},
    }


def _model():
    return {
        "base_link": "base_link",
        "links": [{"name": "base_link", "parent": ""}],
        "shapes": [
            {
                "id": "base_chassis",
                "semantic_type": "chassis",
                "type": "box",
                "link": "base_link",
                "width": 0.5,
                "length": 0.8,
                "height": 0.3,
            },
            {
                "id": "collision",
                "semantic_type": "collision",
                "type": "box",
                "link": "base_link",
                "width": 0.55,
                "length": 0.85,
                "height": 0.05,
            },
            {
                "id": "front_laser",
                "semantic_type": "laser",
                "type": "cylinder",
                "link": "base_link",
                "child_frame": "front_laser",
                "topic": "/scan",
                "driver": "scanner_driver",
                "radius": 0.05,
                "height": 0.08,
                "range_m": 15.0,
            },
        ],
    }


class _FakeRosNode:
    def get_profile_robot_editor(self, robot_id="", reload_from_urdf=False):
        return {
            "ok": True,
            "robot_id": robot_id or "Robot1",
            "profile": _profile(),
            "robot_builder": _model(),
        }


class _InvalidModelRosNode(_FakeRosNode):
    def get_profile_robot_editor(self, robot_id="", reload_from_urdf=False):
        payload = super().get_profile_robot_editor(robot_id, reload_from_urdf)
        payload["robot_builder"] = {"base_link": "", "links": [], "shapes": []}
        return payload


def _client(monkeypatch, tmp_path):
    monkeypatch.setenv("NEXT_NEXT_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(zone_web_ui, "ros_node", _FakeRosNode())
    zone_web_ui.app.testing = True
    return zone_web_ui.app.test_client()


def test_permissions_route_blocks_viewer_push(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    response = client.post(
        "/api/settings/robot_profiles/deployment/push",
        json={"robot_id": "Robot1"},
        headers={"X-NEXT-ROLE": "viewer"},
    )
    assert response.status_code == 403
    assert response.get_json()["code"] == "permission_denied"


def test_model_validate_route_accepts_editor_role(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    response = client.post(
        "/api/next/model/validate",
        json={"profile": _profile(), "robot_builder": _model()},
        headers={"X-NEXT-ROLE": "editor"},
    )
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["ready_to_deploy"] is True


def test_generated_bundle_route_returns_file_contents(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    response = client.get(
        "/api/settings/robot_profiles/generated_bundle?robot_id=Robot1",
        headers={"X-NEXT-ROLE": "engineer"},
    )
    payload = response.get_json()
    assert response.status_code == 200
    assert "robot_model.urdf" in payload["files"]
    assert "nav2_geometry.yaml" in payload["manifest"]["generated_files"]


def test_deployment_push_route_validates_model_before_robot_push(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    monkeypatch.setattr(zone_web_ui, "ros_node", _InvalidModelRosNode())
    response = client.post(
        "/api/settings/robot_profiles/deployment/push",
        json={"robot_id": "Robot1"},
        headers={"X-NEXT-ROLE": "admin"},
    )
    payload = response.get_json()
    assert response.status_code == 400
    assert payload["message"] == "Validation errors block deployment push"
    assert payload["validation"]["summary"]["critical_errors"] > 0


def test_events_route_records_structured_event(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    response = client.post(
        "/api/next/events",
        json={"severity": "warning", "source": "test", "message": "route check"},
        headers={"X-NEXT-ROLE": "engineer"},
    )
    assert response.status_code == 200
    listed = client.get(
        "/api/next/events?limit=10",
        headers={"X-NEXT-ROLE": "viewer"},
    )
    payload = listed.get_json()
    assert payload["count"] == 1
    assert payload["events"][0]["message"] == "route check"
