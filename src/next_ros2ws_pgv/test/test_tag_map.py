import math
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tag_map import load_tag_map  # noqa: E402


def test_load_tag_map_treats_yaw_deg_as_matrix_tag_direction(tmp_path):
    path = tmp_path / "tag_map.yaml"
    path.write_text(
        """
frame_id: map
tags:
  103:
    x: 5.0
    y: 2.0
    yaw_deg: 90.0
""",
        encoding="utf-8",
    )

    frame_id, tags = load_tag_map(path)

    assert frame_id == "map"
    assert tags[103][0] == 5.0
    assert tags[103][1] == 2.0
    assert math.isclose(tags[103][2], math.pi / 2.0)


def test_load_tag_map_accepts_radian_yaw_for_compatibility(tmp_path):
    path = tmp_path / "tag_map.yaml"
    path.write_text(
        """
tags:
  7:
    x: 1.0
    y: -2.0
    yaw: 3.14159
""",
        encoding="utf-8",
    )

    _frame_id, tags = load_tag_map(path)

    assert math.isclose(tags[7][2], 3.14159)
