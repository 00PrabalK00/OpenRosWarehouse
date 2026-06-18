from next2_shelf_simple.shelf_detector import ShelfDetector


def _make_detector():
    detector = ShelfDetector.__new__(ShelfDetector)
    detector._p = {
        'expected_width_min': 0.55,
        'expected_width_max': 0.85,
        'expected_depth_min': 0.50,
        'expected_depth_max': 1.40,
    }
    return detector


def test_select_geometry_subset_prunes_far_outlier_from_three_hotspots():
    detector = _make_detector()
    centroids = [
        (0.7230741410555812, 0.3341787438923323),
        (0.3823234007834282, -1.9430273813864787),
        (0.7091544726702771, -0.35165276453786126),
    ]
    intensities = [1000.0, 300.0, 1046.0]

    kept_centroids, kept_intensities = detector._select_geometry_subset(
        centroids, intensities
    )

    assert kept_centroids == [centroids[0], centroids[2]]
    assert kept_intensities == [intensities[0], intensities[2]]


def test_select_geometry_subset_keeps_plausible_three_leg_geometry():
    detector = _make_detector()
    centroids = [
        (0.70, 0.34),
        (0.70, -0.34),
        (1.60, 0.34),
    ]
    intensities = [900.0, 920.0, 840.0]

    kept_centroids, kept_intensities = detector._select_geometry_subset(
        centroids, intensities
    )

    assert kept_centroids == centroids
    assert kept_intensities == intensities
