import math
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transforms2d import compose, inverse  # noqa: E402


def assert_pose_close(actual, expected, tol=1e-9):
    assert math.isclose(actual[0], expected[0], abs_tol=tol)
    assert math.isclose(actual[1], expected[1], abs_tol=tol)
    assert math.isclose(math.sin(actual[2]), math.sin(expected[2]), abs_tol=tol)
    assert math.isclose(math.cos(actual[2]), math.cos(expected[2]), abs_tol=tol)


def test_pose_composed_with_inverse_returns_identity():
    pose = (1.2, -0.4, math.radians(35.0))

    assert_pose_close(compose(pose, inverse(pose)), (0.0, 0.0, 0.0))


def test_matrix_tag_localization_transform_chain():
    t_map_tag = (5.0, 2.0, math.radians(90.0))
    t_pgv_tag = (0.04, 0.0, math.radians(0.0))
    t_base_pgv = (-0.12, 0.0, 0.0)

    t_map_pgv = compose(t_map_tag, inverse(t_pgv_tag))
    t_map_base = compose(t_map_pgv, inverse(t_base_pgv))

    assert_pose_close(t_map_base, (5.0, 2.08, math.radians(90.0)))
