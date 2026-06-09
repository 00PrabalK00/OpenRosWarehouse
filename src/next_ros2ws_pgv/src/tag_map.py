"""Shared loader for the PGV Matrix Tag map.

The tag map records the known map-frame pose of every oriented floor Matrix
Tag. It is the ground truth that turns a PGV reading ("I see tag 42, it is
3 cm to my left at this angle") into an absolute robot pose.

YAML format::

    frame_id: map
    tags:
      39: {x: 1.0, y: 0.0, yaw_deg: 0.0}
      40: {x: 2.0, y: 0.0, yaw_deg: 0.0}

``yaw`` (radians) is accepted as an alternative to ``yaw_deg``.
Each tag's yaw is the Matrix Tag's forward/lane direction in the map frame;
without it the localizer cannot use the PGV angle to produce a correct robot
heading.
"""

import math

import yaml


class TagMapError(ValueError):
    """Raised when a tag map file is missing or malformed."""


def load_tag_map(path):
    """Load a tag map YAML file.

    Returns ``(frame_id, tags)`` where ``tags`` maps ``int`` tag id to a
    ``(x, y, yaw)`` tuple in metres / radians.
    """
    try:
        with open(path, "r") as handle:
            doc = yaml.safe_load(handle)
    except OSError as exc:
        raise TagMapError(f"cannot read tag map '{path}': {exc}") from exc

    if not isinstance(doc, dict):
        raise TagMapError(f"tag map '{path}' is not a mapping")

    frame_id = doc.get("frame_id", "map")
    raw = doc.get("tags", {})
    if not isinstance(raw, dict):
        raise TagMapError("tag map 'tags' must be a mapping of id -> pose")

    tags = {}
    for key, value in raw.items():
        try:
            tag_id = int(key)
        except (TypeError, ValueError) as exc:
            raise TagMapError(f"tag id '{key}' is not an integer") from exc
        if not isinstance(value, dict):
            raise TagMapError(f"tag {tag_id} pose must be a mapping")
        x = float(value.get("x", 0.0))
        y = float(value.get("y", 0.0))
        if "yaw_deg" in value:
            yaw = math.radians(float(value["yaw_deg"]))
        else:
            yaw = float(value.get("yaw", 0.0))
        tags[tag_id] = (x, y, yaw)

    return frame_id, tags
