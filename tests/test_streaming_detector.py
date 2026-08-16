import numpy as np

from freqscan.streaming.detector import NoiseFloorDetector, nearest_grid_index


def test_nearest_grid_index_array_input():
    grid = np.array([100.0, 110.0, 120.0])
    result = nearest_grid_index(grid, np.array([104.0, 106.0, 999.0]))
    assert list(result) == [0, 1, 2]


def test_never_flags_before_window_fills():
    detector = NoiseFloorDetector(n_bins=1, window=3, margin_db=6.0)
    for _ in range(3):
        result = detector.flag_hop(np.array([0]), np.array([-80.0]))
        assert result[0] == False


def test_flags_reading_above_baseline_plus_margin():
    detector = NoiseFloorDetector(n_bins=1, window=3, margin_db=6.0)
    for _ in range(3):
        detector.flag_hop(np.array([0]), np.array([-80.0]))
    result = detector.flag_hop(np.array([0]), np.array([-20.0]))
    assert result[0] == True


def test_reading_within_margin_not_flagged():
    detector = NoiseFloorDetector(n_bins=1, window=2, margin_db=6.0)
    detector.flag_hop(np.array([0]), np.array([-80.0]))
    detector.flag_hop(np.array([0]), np.array([-80.0]))
    result = detector.flag_hop(np.array([0]), np.array([-75.0]))
    assert result[0] == False


def test_bins_tracked_independently():
    detector = NoiseFloorDetector(n_bins=2, window=1, margin_db=6.0)
    detector.flag_hop(np.array([0, 1]), np.array([-80.0, -80.0]))
    result = detector.flag_hop(np.array([0, 1]), np.array([-80.0, -10.0]))
    assert result[0] == False
    assert result[1] == True


def test_window_slides_after_filling():
    detector = NoiseFloorDetector(n_bins=1, window=2, margin_db=6.0)
    detector.flag_hop(np.array([0]), np.array([-80.0]))
    detector.flag_hop(np.array([0]), np.array([-80.0]))
    result1 = detector.flag_hop(np.array([0]), np.array([-60.0]))
    assert result1[0] == True  # baseline (-80, -80) = -80; -60 > -74
    result2 = detector.flag_hop(np.array([0]), np.array([-60.0]))
    assert result2[0] == True  # baseline (-80, -60) = -70; -60 > -64


def test_flag_hop_flags_multiple_bins_in_one_call():
    detector = NoiseFloorDetector(n_bins=3, window=1, margin_db=6.0)
    detector.flag_hop(np.array([0, 1, 2]), np.array([-80.0, -80.0, -80.0]))
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-80.0, -10.0, -5.0]))
    assert list(result) == [False, True, True]


def test_spatial_disabled_by_default_ignores_hop_median():
    detector = NoiseFloorDetector(n_bins=3, window=1, margin_db=6.0)
    detector.flag_hop(np.array([0, 1, 2]), np.array([-80.0, -22.0, -22.0]))
    # Would fail a spatial check (power - hop median = 2 < any reasonable margin), but
    # spatial_margin_db is unset, so it's never consulted.
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -22.0, -22.0]))
    assert result[0] == True


def test_spatial_check_suppresses_a_temporally_flagged_bin():
    detector = NoiseFloorDetector(n_bins=3, window=1, margin_db=6.0, spatial_margin_db=8.0)
    detector.flag_hop(np.array([0, 1, 2]), np.array([-80.0, -22.0, -22.0]))
    # Temporally elevated (-20 > -80+6), but not spatially — hop median is -22, so this
    # bin isn't actually standing out from its neighbors right now.
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -22.0, -22.0]))
    assert result[0] == False


def test_spatial_check_allows_a_bin_that_also_stands_out_from_its_hop():
    detector = NoiseFloorDetector(n_bins=3, window=1, margin_db=6.0, spatial_margin_db=8.0)
    detector.flag_hop(np.array([0, 1, 2]), np.array([-80.0, -80.0, -80.0]))
    # Temporally elevated AND spatially elevated (-20 > -80+6, and -20 > -80+8 hop median).
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -80.0, -80.0]))
    assert result[0] == True


def test_prominence_disabled_by_default_ignores_hop_median():
    detector = NoiseFloorDetector(n_bins=3, window=1, margin_db=6.0)
    detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -80.0, -80.0]))
    # Steady signal: reading never deviates from its own history, so temporal never
    # fires. Would pass a prominence check (way above the hop median), but
    # prominence_margin_db is unset, so it's never consulted.
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -80.0, -80.0]))
    assert result[0] == False


def test_prominence_flags_a_steady_signal_that_stands_out_from_its_hop():
    detector = NoiseFloorDetector(n_bins=3, window=1, margin_db=6.0, prominence_margin_db=15.0)
    detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -80.0, -80.0]))
    # Temporal fails (reading == its own history, never deviates), but this is a steady
    # FM-carrier-like signal sitting ~60dB above the rest of its hop — prominence catches
    # it independently, on its own, with no temporal deviation required.
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -80.0, -80.0]))
    assert result[0] == True


def test_prominence_not_triggered_when_not_prominent_enough():
    detector = NoiseFloorDetector(n_bins=3, window=1, margin_db=6.0, prominence_margin_db=15.0)
    detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -30.0, -30.0]))
    # Only 10dB above the hop median — temporal fails (steady), and 10 < 15 so
    # prominence doesn't fire either.
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -30.0, -30.0]))
    assert result[0] == False


def test_prominence_skipped_when_temporal_already_flagged():
    detector = NoiseFloorDetector(n_bins=3, window=1, margin_db=6.0, prominence_margin_db=15.0)
    detector.flag_hop(np.array([0, 1, 2]), np.array([-80.0, -18.0, -18.0]))
    # Temporal already flags this (a genuine new deviation); prominence's OR means it
    # doesn't need to also pass — a hop median that would fail prominence
    # (-20 - -18 = -2 < 15) must not suppress an already-True temporal result.
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -18.0, -18.0]))
    assert result[0] == True


def test_prominence_and_spatial_margins_are_independent():
    # spatial_margin_db (AND-refinement) would suppress this since it never fires without
    # a temporal deviation first; prominence_margin_db (independent OR) still catches it.
    detector = NoiseFloorDetector(
        n_bins=3, window=1, margin_db=6.0, spatial_margin_db=8.0, prominence_margin_db=15.0
    )
    detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -80.0, -80.0]))
    result = detector.flag_hop(np.array([0, 1, 2]), np.array([-20.0, -80.0, -80.0]))
    assert result[0] == True
