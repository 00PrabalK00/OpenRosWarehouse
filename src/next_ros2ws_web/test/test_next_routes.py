import os
import sys
from pathlib import Path
from io import BytesIO


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

    def list_robot_profiles(self):
        return {
            "ok": True,
            "profiles": [
                {
                    "robot_id": "Robot1",
                    "config_state": {"summary": "deployed"},
                }
            ],
        }

    def create_robot_profile(self, overrides=None, activate=True):
        overrides = overrides or {}
        return {
            "ok": True,
            "profile": {"robot_id": overrides.get("robot_id", "Robot2")},
            "activate": activate,
        }

    def select_robot_profile(self, robot_id, apply_map=True):
        return {"ok": True, "robot_id": robot_id, "apply_map": apply_map}

    def delete_robot_profile(self, robot_id):
        return {"ok": True, "robot_id": robot_id}

    def rollback_profile_config_on_robot(self, robot_id, snapshot_id=""):
        return {"ok": True, "robot_id": robot_id, "snapshot_id": snapshot_id}

    def get_profile_urdf_source_editor(self, robot_id=""):
        return {"ok": True, "robot_id": robot_id or "Robot1", "source_text": "<robot/>"}

    def save_profile_urdf_source_editor(self, robot_id="", source_path="", source_text="", apply_runtime=True):
        return {
            "ok": True,
            "robot_id": robot_id or "Robot1",
            "source_path": source_path,
            "source_text": source_text,
            "apply_runtime": apply_runtime,
        }

    def upload_profile_urdf(self, robot_id="", filename="", content=b"", apply_runtime=True):
        return {
            "ok": True,
            "robot_id": robot_id or "Robot1",
            "filename": filename,
            "size": len(content or b""),
            "apply_runtime": apply_runtime,
        }

    def get_profile_nav2_parameter_editor(self, robot_id="", include_runtime=True):
        return {
            "ok": True,
            "robot_id": robot_id or "Robot1",
            "curated": {"controller_server": {"FollowPath.max_vel_x": 0.4}},
            "include_runtime": include_runtime,
        }

    def apply_profile_nav2_parameter_editor(
        self,
        robot_id="",
        apply_mode="offline",
        curated_updates=None,
        advanced_updates=None,
        restart_nav=False,
    ):
        return {
            "ok": True,
            "robot_id": robot_id or "Robot1",
            "apply_mode": apply_mode,
            "curated_updates": curated_updates or {},
            "advanced_updates": advanced_updates or {},
            "restart_nav": restart_nav,
        }

    def list_recognition_templates(self):
        return {
            "ok": True,
            "templates": [{"template_id": "tpl-1", "name": "Shelf Template"}],
        }

    def save_recognition_template(self, template):
        return {"ok": True, "template": template}

    def validate_recognition_template(self, template):
        return {"ok": True, "template": template, "validation": {"ok": True}}

    def publish_recognition_template(self, template):
        return {"ok": True, "template": template}

    def duplicate_recognition_template(self, template_id):
        return {"ok": True, "template_id": template_id, "duplicated_to": f"{template_id}-copy"}

    def delete_recognition_template(self, template_id):
        return {"ok": True, "template_id": template_id}

    def export_recognition_template(self, template_id):
        return {"ok": True, "template_id": template_id, "content": {"template_id": template_id}}

    def pull_all_recognition_templates(self):
        return {"ok": True, "message": "Pulled"}

    def push_all_recognition_templates(self):
        return {"ok": True, "message": "Pushed"}

    def clear_safety_state(self):
        return {"ok": True, "message": "cleared"}

    def set_safety_override(self, enable):
        return {"ok": True, "message": "override updated", "enabled": bool(enable)}

    def get_safety_status(self):
        return {"ok": True, "override_enabled": False, "latched": False}

    def set_estop(self, enabled):
        return {"ok": True, "enabled": bool(enabled)}


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


def test_robot_profile_crud_routes_use_node_contract(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)

    listed = client.get("/api/settings/robot_profiles")
    assert listed.status_code == 200
    assert listed.get_json()["profiles"][0]["robot_id"] == "Robot1"

    created = client.post(
        "/api/settings/robot_profiles/create",
        json={"overrides": {"robot_id": "Robot2"}, "activate": False},
    )
    assert created.status_code == 200
    assert created.get_json()["profile"]["robot_id"] == "Robot2"

    selected = client.post(
        "/api/settings/robot_profiles/select",
        json={"robot_id": "Robot1", "apply_map": False},
    )
    assert selected.status_code == 200
    assert selected.get_json()["apply_map"] is False

    deleted = client.post(
        "/api/settings/robot_profiles/delete",
        json={"robot_id": "Robot2"},
    )
    assert deleted.status_code == 200
    assert deleted.get_json()["robot_id"] == "Robot2"


def test_rollback_route_blocks_viewer_role(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    response = client.post(
        "/api/settings/robot_profiles/deployment/rollback",
        json={"robot_id": "Robot1", "snapshot_id": "snap-1"},
        headers={"X-NEXT-ROLE": "viewer"},
    )
    assert response.status_code == 403
    assert response.get_json()["code"] == "permission_denied"


def test_rollback_route_allows_admin(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    response = client.post(
        "/api/settings/robot_profiles/deployment/rollback",
        json={"robot_id": "Robot1", "snapshot_id": "snap-1"},
        headers={"X-NEXT-ROLE": "admin"},
    )
    assert response.status_code == 200
    assert response.get_json()["snapshot_id"] == "snap-1"


def test_test_mode_routes_block_viewer(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    indicators = client.post(
        "/api/next/test_mode/indicators",
        json={"pattern": "blink"},
        headers={"X-NEXT-ROLE": "viewer"},
    )
    motors = client.post(
        "/api/next/test_mode/motors",
        json={"action": "nudge"},
        headers={"X-NEXT-ROLE": "viewer"},
    )
    assert indicators.status_code == 403
    assert motors.status_code == 403


def test_test_mode_routes_allow_admin(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    indicators = client.post(
        "/api/next/test_mode/indicators",
        json={"pattern": "blink", "color": "#00ff00"},
        headers={"X-NEXT-ROLE": "admin"},
    )
    motors = client.post(
        "/api/next/test_mode/motors",
        json={"action": "nudge"},
        headers={"X-NEXT-ROLE": "admin"},
    )
    assert indicators.status_code == 200
    assert motors.status_code == 200


def test_import_routes_block_non_import_roles(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    source = client.get(
        "/api/settings/robot_profiles/urdf/source?robot_id=Robot1",
        headers={"X-NEXT-ROLE": "viewer"},
    )
    upload = client.post(
        "/api/settings/robot_profiles/urdf/upload",
        data={"robot_id": "Robot1", "file": (BytesIO(b"<robot/>"), "robot.urdf")},
        content_type="multipart/form-data",
        headers={"X-NEXT-ROLE": "viewer"},
    )
    editor = client.get(
        "/api/settings/robot_profiles/robot_editor?robot_id=Robot1&reload_from_urdf=true",
        headers={"X-NEXT-ROLE": "editor"},
    )
    assert source.status_code == 403
    assert upload.status_code == 403
    assert editor.status_code == 403


def test_import_routes_allow_engineer(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    source = client.get(
        "/api/settings/robot_profiles/urdf/source?robot_id=Robot1",
        headers={"X-NEXT-ROLE": "engineer"},
    )
    saved = client.post(
        "/api/settings/robot_profiles/urdf/source",
        json={
            "robot_id": "Robot1",
            "source_path": "robot.urdf",
            "source_text": "<robot name='Robot1'/>",
            "apply_runtime": False,
        },
        headers={"X-NEXT-ROLE": "engineer"},
    )
    upload = client.post(
        "/api/settings/robot_profiles/urdf/upload",
        data={"robot_id": "Robot1", "file": (BytesIO(b"<robot/>"), "robot.urdf")},
        content_type="multipart/form-data",
        headers={"X-NEXT-ROLE": "engineer"},
    )
    assert source.status_code == 200
    assert saved.status_code == 200
    assert upload.status_code == 200


def test_nav2_advanced_apply_blocks_non_import_roles(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    response = client.post(
        "/api/settings/robot_profiles/nav2_parameters/apply",
        json={
            "robot_id": "Robot1",
            "apply_mode": "offline",
            "curated_updates": {"controller_server": {"FollowPath.max_vel_x": 0.3}},
            "advanced_updates": {"controller_server/ros__parameters/goal_checker_plugins": ["simple"]},
        },
        headers={"X-NEXT-ROLE": "editor"},
    )
    assert response.status_code == 403
    assert response.get_json()["code"] == "permission_denied"


def test_nav2_advanced_apply_allows_engineer(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    response = client.post(
        "/api/settings/robot_profiles/nav2_parameters/apply",
        json={
            "robot_id": "Robot1",
            "apply_mode": "offline",
            "curated_updates": {"controller_server": {"FollowPath.max_vel_x": 0.3}},
            "advanced_updates": {"controller_server/ros__parameters/goal_checker_plugins": ["simple"]},
            "restart_nav": True,
        },
        headers={"X-NEXT-ROLE": "engineer"},
    )
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["restart_nav"] is True


def test_recognition_routes_block_unauthorized_roles(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    listing = client.get("/api/recognition/templates", headers={"X-NEXT-ROLE": "viewer"})
    saving = client.post(
        "/api/recognition/templates/save",
        json={"template": {"template_id": "tpl-1"}},
        headers={"X-NEXT-ROLE": "viewer"},
    )
    validating = client.post(
        "/api/recognition/templates/validate",
        json={"template": {"template_id": "tpl-1"}},
        headers={"X-NEXT-ROLE": "viewer"},
    )
    exporting = client.get(
        "/api/recognition/templates/export/tpl-1",
        headers={"X-NEXT-ROLE": "editor"},
    )
    pulling = client.post(
        "/api/recognition/templates/pull_all",
        headers={"X-NEXT-ROLE": "editor"},
    )
    pushing = client.post(
        "/api/recognition/templates/push_all",
        headers={"X-NEXT-ROLE": "editor"},
    )

    assert listing.status_code == 200
    assert saving.status_code == 403
    assert validating.status_code == 403
    assert exporting.status_code == 403
    assert pulling.status_code == 403
    assert pushing.status_code == 403


def test_recognition_routes_allow_expected_roles(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    saving = client.post(
        "/api/recognition/templates/save",
        json={"template": {"template_id": "tpl-1"}},
        headers={"X-NEXT-ROLE": "editor"},
    )
    validating = client.post(
        "/api/recognition/templates/validate",
        json={"template": {"template_id": "tpl-1"}},
        headers={"X-NEXT-ROLE": "editor"},
    )
    publishing = client.post(
        "/api/recognition/templates/publish",
        json={"template": {"template_id": "tpl-1"}},
        headers={"X-NEXT-ROLE": "editor"},
    )
    duplicating = client.post(
        "/api/recognition/templates/duplicate",
        json={"template_id": "tpl-1"},
        headers={"X-NEXT-ROLE": "editor"},
    )
    deleting = client.post(
        "/api/recognition/templates/delete",
        json={"template_id": "tpl-1"},
        headers={"X-NEXT-ROLE": "editor"},
    )
    exporting = client.get(
        "/api/recognition/templates/export/tpl-1",
        headers={"X-NEXT-ROLE": "engineer"},
    )
    pulling = client.post(
        "/api/recognition/templates/pull_all",
        headers={"X-NEXT-ROLE": "engineer"},
    )
    pushing = client.post(
        "/api/recognition/templates/push_all",
        headers={"X-NEXT-ROLE": "engineer"},
    )

    assert saving.status_code == 200
    assert validating.status_code == 200
    assert publishing.status_code == 200
    assert duplicating.status_code == 200
    assert deleting.status_code == 200
    assert exporting.status_code == 200
    assert pulling.status_code == 200
    assert pushing.status_code == 200


def test_safety_routes_block_non_admin_test_mode_roles(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    force_resume = client.post(
        "/api/safety/force_resume",
        headers={"X-NEXT-ROLE": "engineer", "X-API-Token": "nextsmr"},
    )
    override = client.post(
        "/api/safety/override",
        json={"enable": True},
        headers={"X-NEXT-ROLE": "engineer", "X-API-Token": "nextsmr"},
    )
    status = client.get("/api/safety/status", headers={"X-NEXT-ROLE": "viewer"})

    assert force_resume.status_code == 403
    assert override.status_code == 403
    assert status.status_code == 200


def test_safety_routes_allow_admin(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    force_resume = client.post(
        "/api/safety/force_resume",
        headers={"X-NEXT-ROLE": "admin", "X-API-Token": "nextsmr"},
    )
    override = client.post(
        "/api/safety/override",
        json={"enable": True},
        headers={"X-NEXT-ROLE": "admin", "X-API-Token": "nextsmr"},
    )

    assert force_resume.status_code == 200
    assert override.status_code == 200
    assert override.get_json()["enabled"] is True


def test_firmware_update_requires_configured_command(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    monkeypatch.delenv("NEXT_FIRMWARE_UPDATE_COMMAND", raising=False)
    response = client.post("/api/settings/update_firmware")
    payload = response.get_json()
    assert response.status_code == 400
    assert payload["ok"] is False
    assert "NEXT_FIRMWARE_UPDATE_COMMAND" in payload["message"]


def test_firmware_update_launches_configured_command(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    monkeypatch.setenv("NEXT_FIRMWARE_UPDATE_COMMAND", "echo firmware")

    launched = {}

    def _fake_popen(cmd, stdout=None, stderr=None, start_new_session=None):
        launched["cmd"] = cmd
        launched["stdout"] = stdout
        launched["stderr"] = stderr
        launched["start_new_session"] = start_new_session
        return object()

    monkeypatch.setattr(zone_web_ui.subprocess, "Popen", _fake_popen)
    response = client.post("/api/settings/update_firmware")
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["ok"] is True
    assert launched["cmd"] == ["/bin/bash", "-lc", "echo firmware"]
    assert launched["start_new_session"] is True
