import math
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pgv_reader import parse_pgv_frame  # noqa: E402


def test_parse_pgv_frame_scales_angle_from_tenths_of_degrees():
    frame = bytes.fromhex(
        "00 44 07 7f 7f 67 01 43 00 00 14 7a 00 00 00 00 01 1f 00 00 16"
    )

    data = parse_pgv_frame(frame, verify_checksum=False)

    assert data["code_read"] is True
    assert data["tag_flag"] is True
    assert data["tag_number"] == 159
    assert math.isclose(data["angle_deg"], 268.2)


def test_parse_pgv_frame_converts_tag_x_to_signed_mm():
    frame = bytearray.fromhex(
        "00 44 07 7f 7f 67 01 43 00 00 14 7a 00 00 00 00 01 1f 00 00 16"
    )
    frame[2] = 0x04
    frame[3] = 0x00
    frame[4] = 0x00
    frame[5] = 0x00

    data = parse_pgv_frame(bytes(frame), verify_checksum=False)

    assert data["x_mm"] == -0x800000
