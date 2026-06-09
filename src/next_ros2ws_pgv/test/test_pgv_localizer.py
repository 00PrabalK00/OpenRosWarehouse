import math
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pgv_localizer import transform_to_pose2d  # noqa: E402


class Obj:
    pass


def make_transform(x, y, yaw):
    msg = Obj()
    msg.transform = Obj()
    msg.transform.translation = Obj()
    msg.transform.rotation = Obj()
    msg.transform.translation.x = x
    msg.transform.translation.y = y
    msg.transform.translation.z = -0.10
    msg.transform.rotation.x = 0.0
    msg.transform.rotation.y = 0.0
    msg.transform.rotation.z = math.sin(yaw / 2.0)
    msg.transform.rotation.w = math.cos(yaw / 2.0)
    return msg


def test_transform_to_pose2d_reads_urdf_base_to_pgv_tf():
    pose = transform_to_pose2d(make_transform(-0.52, 0.0, math.radians(15.0)))

    assert math.isclose(pose[0], -0.52)
    assert math.isclose(pose[1], 0.0)
    assert math.isclose(pose[2], math.radians(15.0))
