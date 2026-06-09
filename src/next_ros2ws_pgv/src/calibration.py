"""PGV extrinsic calibration: estimate the base_link -> pgv_link transform.

This is a 2D hand-eye calibration. While the robot is driven (forward/back
then spun in place) over a single fixed floor code, two streams are recorded
per sample:

    A_i = T_odom_base   (robot pose from odometry)
    B_i = T_pgv_tag      (code pose measured in the PGV frame)

The unknown mounting transform X = T_base_pgv is constant, and the code is
fixed in the world, so

    C_i = A_i o X o B_i = T_odom_tag

must be identical for every sample. We solve for X = (x, y, yaw) that makes
all C_i agree (least squares). Pure functions, no ROS dependency, so they can
be unit tested and reused.

Observability note: pure straight-line motion leaves the offset *along* the
travel direction unobservable - an in-place rotation is required, which is
exactly why the calibration routine asks the operator to spin the robot.
"""

import math

import numpy as np
from scipy.optimize import least_squares


def _compose(a, b):
    ax, ay, at = a
    bx, by, bt = b
    c = math.cos(at)
    s = math.sin(at)
    return (
        ax + c * bx - s * by,
        ay + s * bx + c * by,
        _wrap(at + bt),
    )


def _wrap(t):
    return math.atan2(math.sin(t), math.cos(t))


def _world_tag_poses(A, B, X):
    """Return the list of C_i = A_i o X o B_i for the given mounting X."""
    return [_compose(_compose(a, X), b) for a, b in zip(A, B)]


def fit_base_to_pgv(A, B, x0=(0.0, 0.0, 0.0)):
    """Solve for the base_link -> pgv_link transform.

    Args:
        A: list of (x, y, yaw) odom->base poses.
        B: list of (x, y, yaw) pgv->tag measurements.
        x0: initial guess for (x, y, yaw).

    Returns:
        dict with keys:
            x, y, yaw           estimated mounting transform (m, m, rad)
            rms_position_m      RMS spread of the recovered tag position
            rms_angle_deg       RMS spread of the recovered tag angle
            num_samples         number of samples used
    """
    if len(A) != len(B):
        raise ValueError("A and B must have equal length")
    if len(A) < 3:
        raise ValueError("need at least 3 samples to calibrate")

    def residuals(p):
        X = (p[0], p[1], p[2])
        Cs = _world_tag_poses(A, B, X)
        c0 = Cs[0]
        res = []
        for c in Cs:
            res.append(c[0] - c0[0])
            res.append(c[1] - c0[1])
            res.append(_wrap(c[2] - c0[2]))
        return res

    sol = least_squares(residuals, x0=list(x0), method="lm")
    X = (float(sol.x[0]), float(sol.x[1]), _wrap(float(sol.x[2])))

    Cs = np.array(_world_tag_poses(A, B, X))
    pos_mean = Cs[:, :2].mean(axis=0)
    rms_pos = float(np.sqrt(np.mean(np.sum((Cs[:, :2] - pos_mean) ** 2, axis=1))))
    # circular spread of the angle column
    ang = Cs[:, 2]
    ang_mean = math.atan2(np.mean(np.sin(ang)), np.mean(np.cos(ang)))
    ang_dev = np.array([_wrap(a - ang_mean) for a in ang])
    rms_ang = float(np.sqrt(np.mean(ang_dev ** 2)))

    return {
        "x": X[0],
        "y": X[1],
        "yaw": X[2],
        "rms_position_m": rms_pos,
        "rms_angle_deg": math.degrees(rms_ang),
        "num_samples": len(A),
    }


def make_fit_plot(A, B, result, path):
    """Render the calibration fit-quality plot to ``path`` (PNG).

    Left panel: recovered code position in the odom frame, BEFORE calibration
    (X = identity, orange) vs AFTER calibration (red). A good fit collapses the
    red cloud to a tight cluster. Right panel: per-sample position residual.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    X = (result["x"], result["y"], result["yaw"])
    before = np.array(_world_tag_poses(A, B, (0.0, 0.0, 0.0)))
    after = np.array(_world_tag_poses(A, B, X))
    after_mean = after[:, :2].mean(axis=0)
    resid = np.sqrt(np.sum((after[:, :2] - after_mean) ** 2, axis=1)) * 1000.0

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

    ax1.scatter(before[:, 0], before[:, 1], s=12, c="orange",
                label="measured (uncalibrated)", alpha=0.7)
    ax1.scatter(after[:, 0], after[:, 1], s=12, c="red",
                label="fitted (calibrated)", alpha=0.7)
    ax1.scatter([after_mean[0]], [after_mean[1]], s=80, marker="x", c="black",
                label="fitted code center")
    ax1.set_title("Code position in odom frame")
    ax1.set_xlabel("x (m)")
    ax1.set_ylabel("y (m)")
    ax1.axis("equal")
    ax1.grid(True, alpha=0.3)
    ax1.legend(fontsize=8)

    ax2.plot(resid, color="red", marker=".")
    ax2.set_title("Per-sample residual after calibration")
    ax2.set_xlabel("sample")
    ax2.set_ylabel("residual (mm)")
    ax2.grid(True, alpha=0.3)

    txt = (
        f"x={X[0]*1000:.1f} mm  y={X[1]*1000:.1f} mm  "
        f"yaw={math.degrees(X[2]):.2f} deg\n"
        f"RMS pos={result['rms_position_m']*1000:.1f} mm  "
        f"RMS ang={result['rms_angle_deg']:.2f} deg  "
        f"n={result['num_samples']}"
    )
    fig.suptitle("PGV extrinsic calibration fit\n" + txt, fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path
