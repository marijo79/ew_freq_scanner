from freqscan.streaming.detector import NoiseFloorDetector, hop_median


def test_never_flags_before_window_fills():
    detector = NoiseFloorDetector(window=3, margin_db=6.0)
    assert detector.flag(100e6, -80.0) is False
    assert detector.flag(100e6, -80.0) is False
    assert detector.flag(100e6, -80.0) is False


def test_flags_reading_above_baseline_plus_margin():
    detector = NoiseFloorDetector(window=3, margin_db=6.0)
    for _ in range(3):
        detector.flag(100e6, -80.0)
    assert detector.flag(100e6, -20.0) is True


def test_reading_within_margin_not_flagged():
    detector = NoiseFloorDetector(window=2, margin_db=6.0)
    detector.flag(100e6, -80.0)
    detector.flag(100e6, -80.0)
    assert detector.flag(100e6, -75.0) is False


def test_bins_tracked_independently():
    detector = NoiseFloorDetector(window=1, margin_db=6.0)
    detector.flag(100e6, -80.0)
    detector.flag(200e6, -80.0)
    assert detector.flag(100e6, -80.0) is False
    assert detector.flag(200e6, -10.0) is True


def test_window_slides_after_filling():
    detector = NoiseFloorDetector(window=2, margin_db=6.0)
    detector.flag(100e6, -80.0)
    detector.flag(100e6, -80.0)
    assert detector.flag(100e6, -60.0) is True  # baseline (-80, -80) = -80; -60 > -74
    assert detector.flag(100e6, -60.0) is True  # baseline (-80, -60) = -70; -60 > -64


def test_spatial_disabled_by_default_ignores_spatial_baseline():
    detector = NoiseFloorDetector(window=1, margin_db=6.0)
    detector.flag(100e6, -80.0)
    # Would fail a spatial check (power - baseline = 2 < any reasonable margin), but
    # spatial_margin_db is unset, so it's never consulted.
    assert detector.flag(100e6, -20.0, spatial_baseline=-22.0) is True


def test_spatial_check_suppresses_a_temporally_flagged_bin():
    detector = NoiseFloorDetector(window=1, margin_db=6.0, spatial_margin_db=8.0)
    detector.flag(100e6, -80.0)
    # Temporally elevated (-20 > -80+6), but not spatially — the whole hop is at -22ish,
    # so this bin isn't actually standing out from its neighbors right now.
    assert detector.flag(100e6, -20.0, spatial_baseline=-22.0) is False


def test_spatial_check_allows_a_bin_that_also_stands_out_from_its_hop():
    detector = NoiseFloorDetector(window=1, margin_db=6.0, spatial_margin_db=8.0)
    detector.flag(100e6, -80.0)
    # Temporally elevated AND spatially elevated (-20 > -80+6, and -20 > -80+8 hop median).
    assert detector.flag(100e6, -20.0, spatial_baseline=-80.0) is True


def test_spatial_check_skipped_when_no_baseline_given_even_if_configured():
    detector = NoiseFloorDetector(window=1, margin_db=6.0, spatial_margin_db=8.0)
    detector.flag(100e6, -80.0)
    assert detector.flag(100e6, -20.0) is True  # no spatial_baseline passed => temporal alone decides


def test_prominence_disabled_by_default_ignores_spatial_baseline():
    detector = NoiseFloorDetector(window=1, margin_db=6.0)
    detector.flag(100e6, -20.0)
    # Steady signal: reading never deviates from its own history, so temporal never
    # fires. Would pass a prominence check (way above the hop median), but
    # prominence_margin_db is unset, so it's never consulted.
    assert detector.flag(100e6, -20.0, spatial_baseline=-80.0) is False


def test_prominence_flags_a_steady_signal_that_stands_out_from_its_hop():
    detector = NoiseFloorDetector(window=1, margin_db=6.0, prominence_margin_db=15.0)
    detector.flag(100e6, -20.0)
    # Temporal fails (reading == its own history, never deviates), but this is a steady
    # FM-carrier-like signal sitting ~60dB above the rest of its hop — prominence catches
    # it independently, on its own, with no temporal deviation required.
    assert detector.flag(100e6, -20.0, spatial_baseline=-80.0) is True


def test_prominence_not_triggered_when_not_prominent_enough():
    detector = NoiseFloorDetector(window=1, margin_db=6.0, prominence_margin_db=15.0)
    detector.flag(100e6, -20.0)
    # Only 10dB above the hop median — temporal fails (steady), and 10 < 15 so
    # prominence doesn't fire either.
    assert detector.flag(100e6, -20.0, spatial_baseline=-30.0) is False


def test_prominence_skipped_when_temporal_already_flagged():
    detector = NoiseFloorDetector(window=1, margin_db=6.0, prominence_margin_db=15.0)
    detector.flag(100e6, -80.0)
    # Temporal already flags this (a genuine new deviation); prominence's OR means it
    # doesn't need to also pass — passing a spatial_baseline that would fail prominence
    # (-20 - -18 = -2 < 15) must not suppress an already-True temporal result.
    assert detector.flag(100e6, -20.0, spatial_baseline=-18.0) is True


def test_prominence_and_spatial_margins_are_independent():
    # spatial_margin_db (AND-refinement) would suppress this since it never fires without
    # a temporal deviation first; prominence_margin_db (independent OR) still catches it.
    detector = NoiseFloorDetector(window=1, margin_db=6.0, spatial_margin_db=8.0, prominence_margin_db=15.0)
    detector.flag(100e6, -20.0)
    assert detector.flag(100e6, -20.0, spatial_baseline=-80.0) is True


def test_hop_median_of_empty_list_is_none():
    assert hop_median([]) is None


def test_hop_median_computes_median_power():
    assert hop_median([-80.0, -70.0, -60.0]) == -70.0
