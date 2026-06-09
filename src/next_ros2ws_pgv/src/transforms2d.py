"""Minimal 2D rigid-transform helpers (x, y, theta) for PGV localization.

A pose is a 3-tuple ``(x, y, theta)`` meaning the transform that maps a point
expressed in the child frame into the parent frame:

    p_parent = R(theta) * p_child + (x, y)

These are pure functions with no ROS dependency so they can be unit tested in
isolation.
"""

import math


def compose(a, b):
    """Return a ∘ b: the pose of b expressed in a's parent frame.

    If ``a`` is parent->mid and ``b`` is mid->child, the result is
    parent->child.
    """
    ax, ay, at = a
    bx, by, bt = b
    c = math.cos(at)
    s = math.sin(at)
    x = ax + c * bx - s * by
    y = ay + s * bx + c * by
    t = _wrap(at + bt)
    return (x, y, t)


def inverse(a):
    """Return the inverse transform of ``a``."""
    ax, ay, at = a
    c = math.cos(at)
    s = math.sin(at)
    x = -(c * ax + s * ay)
    y = -(-s * ax + c * ay)
    return (x, y, _wrap(-at))


def _wrap(theta):
    """Wrap an angle to (-pi, pi]."""
    return math.atan2(math.sin(theta), math.cos(theta))


def yaw_to_quat(yaw):
    """Return (z, w) of the quaternion for a yaw-only rotation."""
    return (math.sin(yaw / 2.0), math.cos(yaw / 2.0))


def quat_to_yaw(z, w):
    """Recover yaw from a yaw-only (z, w) quaternion."""
    return 2.0 * math.atan2(z, w)
