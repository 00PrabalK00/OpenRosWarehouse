#!/usr/bin/env python3
"""Standalone ROS 2 driver for the Pepperl+Fuchs R3138 PGV AGV code reader.

The reader is an RS-485 follower in question-and-answer (query) mode. The
master sends a 2-byte data request and the reader replies with a 21-byte
result frame. This node polls the reader at a fixed rate (default 1 Hz) and
publishes the decoded position / heading / tag number.

Protocol reference (P+F R3138 manual, ResultPackMode = P+F):

  Data request (single reader, address 0):  C8 37

  21-byte response (byte indices 0..20):
    [0] flags   : bit1 = NP  (0 = code read, 1 = no code read)
    [1] tag     : bit6 = TAG (1 = code matrix / X is signed)
    [2] X23..X21 (low 3 bits)
    [3] X20..X14
    [4] X13..X07
    [5] X06..X00
    [6] Y13..Y07
    [7] Y06..Y00
    [8..9]  reserved
    [10] ANG13..ANG07
    [11] ANG06..ANG00
    [12..13] Num34..Num28 (byte 13)
    [14] Num27..Num21
    [15] Num20..Num14
    [16] Num13..Num07
    [17] Num06..Num00
    [18..19] reserved
    [20] XOR checksum

  X position : 24-bit, mm. Unsigned for code tape, signed when TAG = 1.
  Y position : 14-bit signed, mm.
  Angle      : 14-bit unsigned, degrees.
  Tag number : 35-bit unsigned.

This node does not depend on or wire into any other package. It only
publishes; nothing in the workspace subscribes to it by default.
"""

import math

import rclpy
from rclpy.node import Node

from geometry_msgs.msg import PoseStamped
from std_msgs.msg import Bool, Float32, Int64

try:
    import serial
except ImportError:  # pragma: no cover - surfaced at runtime with a clear message
    serial = None


# Data request for a single reader at address 0 (manual table 4-2).
PGV_DATA_REQUEST = bytes.fromhex("C8 37")
PGV_RESPONSE_LEN = 21


class PgvFrameError(ValueError):
    """Raised when a response frame cannot be decoded."""


def parse_pgv_frame(frame, verify_checksum=True):
    """Decode a 21-byte P+F R3138 response frame.

    Returns a dict with keys:
        code_read   : bool  - True when a code/tag was detected (NP == 0)
        tag_flag    : bool  - True when the TAG (code matrix) bit is set
        x_mm        : int   - X position in millimetres
        y_mm        : int   - Y position in millimetres
        angle_deg   : int   - heading angle in degrees
        tag_number  : int   - 35-bit tag number
        checksum_ok : bool  - XOR checksum matched

    Raises PgvFrameError on bad length or (when verify_checksum) checksum
    mismatch.
    """
    if len(frame) != PGV_RESPONSE_LEN:
        raise PgvFrameError(
            f"expected {PGV_RESPONSE_LEN} bytes, got {len(frame)}: {frame.hex(' ')}"
        )

    b = frame

    # XOR checksum over bytes 1..20 (indices 0..19), compared with byte 21.
    chk = 0
    for value in b[:20]:
        chk ^= value
    checksum_ok = (chk == b[20])
    if verify_checksum and not checksum_ok:
        raise PgvFrameError(
            f"checksum mismatch: computed 0x{chk:02X}, frame 0x{b[20]:02X}"
        )

    np_bit = b[0] & 0x02            # NP: 0 = code read, 1 = no code
    code_read = (np_bit == 0)
    tag_flag = bool(b[1] & 0x40)    # TAG bit -> X is signed when set

    # X: 24-bit. Byte[2] contributes only its low 3 bits.
    x = ((b[2] & 0x07) << 21) | (b[3] << 14) | (b[4] << 7) | b[5]
    if tag_flag and (x & 0x800000):           # 24-bit two's complement
        x -= 0x1000000

    # Y: 14-bit signed.
    y = (b[6] << 7) | b[7]
    if y & 0x2000:                            # 14-bit two's complement
        y -= 0x4000

    # Angle: 14-bit unsigned, degrees.
    angle = (b[10] << 7) | b[11]

    # Tag number: 35-bit unsigned across bytes 14..18 (indices 13..17).
    tag_number = (
        (b[13] << 28)
        | (b[14] << 21)
        | (b[15] << 14)
        | (b[16] << 7)
        | b[17]
    )

    return {
        "code_read": code_read,
        "tag_flag": tag_flag,
        "x_mm": x,
        "y_mm": y,
        "angle_deg": angle,
        "tag_number": tag_number,
        "checksum_ok": checksum_ok,
    }


class PgvReader(Node):
    """Polls the R3138 over RS-485 and publishes decoded results."""

    def __init__(self):
        super().__init__("pgv_reader")

        self.declare_parameter("port", "/dev/next/pgv")
        self.declare_parameter("baud", 115200)
        self.declare_parameter("poll_hz", 1.0)
        self.declare_parameter("read_timeout", 0.1)
        self.declare_parameter("frame_id", "pgv_link")
        # Reject frames whose XOR checksum does not match. Some firmware
        # revisions report a checksum that disagrees with the manual's
        # worked example, so default to warn-only.
        self.declare_parameter("strict_checksum", False)

        self.port = self.get_parameter("port").value
        self.baud = int(self.get_parameter("baud").value)
        self.poll_hz = float(self.get_parameter("poll_hz").value)
        self.read_timeout = float(self.get_parameter("read_timeout").value)
        self.frame_id = self.get_parameter("frame_id").value
        self.strict_checksum = bool(self.get_parameter("strict_checksum").value)

        self.pub_pose = self.create_publisher(PoseStamped, "pgv/pose", 10)
        self.pub_tag = self.create_publisher(Int64, "pgv/tag", 10)
        self.pub_angle = self.create_publisher(Float32, "pgv/angle_deg", 10)
        self.pub_detected = self.create_publisher(Bool, "pgv/code_detected", 10)

        self.ser = None
        self._warned_no_serial = False

        if serial is None:
            self.get_logger().error(
                "pyserial not installed. Install with: pip install pyserial"
            )
        else:
            self._open_serial()

        period = 1.0 / self.poll_hz if self.poll_hz > 0 else 1.0
        self.timer = self.create_timer(period, self._poll)
        self.get_logger().info(
            f"PGV reader polling {self.port} @ {self.baud} baud, {self.poll_hz} Hz"
        )

    def _open_serial(self):
        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baud,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=self.read_timeout,
            )
            self.get_logger().info(f"Opened serial port {self.port}")
        except Exception as exc:  # noqa: BLE001 - report and retry on next poll
            self.ser = None
            self.get_logger().warning(f"Could not open {self.port}: {exc}")

    def _poll(self):
        if serial is None:
            return
        if self.ser is None or not self.ser.is_open:
            self._open_serial()
            if self.ser is None:
                return

        try:
            self.ser.reset_input_buffer()
            self.ser.write(PGV_DATA_REQUEST)
            self.ser.flush()
            frame = self.ser.read(PGV_RESPONSE_LEN)
        except Exception as exc:  # noqa: BLE001 - drop port, reopen next cycle
            self.get_logger().warning(f"Serial error: {exc}")
            try:
                self.ser.close()
            except Exception:  # noqa: BLE001
                pass
            self.ser = None
            return

        if len(frame) != PGV_RESPONSE_LEN:
            self.get_logger().warning(
                f"Short read: {len(frame)} bytes ({frame.hex(' ')})"
            )
            return

        try:
            data = parse_pgv_frame(frame, verify_checksum=self.strict_checksum)
        except PgvFrameError as exc:
            self.get_logger().warning(f"Bad frame: {exc}")
            return

        if not data["checksum_ok"]:
            self.get_logger().debug("Frame checksum mismatch (warn-only)")

        self._publish(data)

    def _publish(self, data):
        now = self.get_clock().now().to_msg()

        detected = Bool()
        detected.data = data["code_read"]
        self.pub_detected.publish(detected)

        # Always publish angle and tag for diagnostics; values are 0 when no
        # code is present.
        angle_msg = Float32()
        angle_msg.data = float(data["angle_deg"])
        self.pub_angle.publish(angle_msg)

        tag_msg = Int64()
        tag_msg.data = int(data["tag_number"])
        self.pub_tag.publish(tag_msg)

        if not data["code_read"]:
            return

        pose = PoseStamped()
        pose.header.stamp = now
        pose.header.frame_id = self.frame_id
        pose.pose.position.x = data["x_mm"] / 1000.0   # mm -> m
        pose.pose.position.y = data["y_mm"] / 1000.0
        pose.pose.position.z = 0.0

        yaw = math.radians(data["angle_deg"])
        pose.pose.orientation.z = math.sin(yaw / 2.0)
        pose.pose.orientation.w = math.cos(yaw / 2.0)
        self.pub_pose.publish(pose)

    def destroy_node(self):
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:  # noqa: BLE001
                pass
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = PgvReader()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
