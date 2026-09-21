import numpy as np

from freqscan.plotting.common import (
    FALLBACK_SPECTRUM_YLIM,
    initial_freq_extent,
    initial_waterfall_clim,
    initial_ylim,
)
from freqscan.sdr.base import Channel, SweepState


def _channel(freq_start_mhz: float, freq_stop_mhz: float, sweep: dict) -> Channel:
    return Channel(
        label="test",
        freq_start_mhz=freq_start_mhz,
        freq_stop_mhz=freq_stop_mhz,
        state=SweepState(sweep=dict(sweep)),
    )


def _kafka_viewer_style_channel(freq_start_mhz: float, freq_stop_mhz: float, n_bins: int) -> Channel:
    """Mimics KafkaConsumerBackend.__init__'s own pre-population: every declared grid
    frequency present as a key immediately, value NaN until a real message arrives --
    the exact shape that broke initial_ylim()/initial_waterfall_clim()/
    initial_freq_extent() (see CLAUDE.md item 5's notes on all three)."""
    freqs = np.linspace(freq_start_mhz * 1e6, freq_stop_mhz * 1e6, n_bins, endpoint=False)
    sweep = {float(f): float("nan") for f in freqs}
    return _channel(freq_start_mhz, freq_stop_mhz, sweep)


def test_initial_ylim_with_only_nan_falls_back_instead_of_returning_nan():
    channel = _kafka_viewer_style_channel(75.0, 125.0, 100)
    assert initial_ylim(channel, timeout=0.1) == FALLBACK_SPECTRUM_YLIM


def test_initial_ylim_ignores_nan_and_uses_real_values():
    channel = _channel(75.0, 125.0, {100e6: -40.0, 101e6: float("nan"), 102e6: -60.0})
    assert initial_ylim(channel, margin_db=10.0, timeout=0.1) == (-70.0, -30.0)


def test_initial_waterfall_clim_with_only_nan_falls_back():
    channel = _kafka_viewer_style_channel(75.0, 125.0, 100)
    assert initial_waterfall_clim(channel, timeout=0.1) == FALLBACK_SPECTRUM_YLIM


def test_initial_waterfall_clim_ignores_nan_and_uses_real_values():
    channel = _channel(75.0, 125.0, {100e6: -80.0, 101e6: float("nan"), 102e6: -80.0})
    floor, ceiling = initial_waterfall_clim(channel, floor_percentile=50.0, span_db=40.0, floor_margin_db=3.0, timeout=0.1)
    assert floor == -83.0
    assert ceiling == -40.0


def test_initial_freq_extent_with_only_nan_falls_back_to_declared_range():
    """The Kafka-viewer margin bug: pre-populated NaN keys span the WIDE declared grid
    (e.g. a Pluto-stare channel's pre-edge-trim span), so a naive "peek at dict keys"
    check returns that wide range immediately -- looking correct (a tuple, not a
    crash) while actually never narrowing to where real data exists at all. This test
    only confirms the *fallback* path stays correct; the real regression is covered by
    the ignores_nan test below, which is where the old buggy version returned the wide
    75-125 range instead of the narrower real one."""
    channel = _kafka_viewer_style_channel(75.0, 125.0, 100)
    assert initial_freq_extent(channel, timeout=0.1) == (75.0, 125.0)


def test_initial_freq_extent_ignores_nan_and_narrows_to_real_bins():
    # Simulates a Pluto-stare Kafka-viewer channel: declared grid is the wide
    # 75-125MHz pre-trim span, but only the inner 80-120MHz ever gets real
    # (non-NaN) values -- scripts/pluto_stare.py trims the rest before ever
    # publishing a CSV line, so those edge frequencies are permanently NaN.
    freqs = np.linspace(75e6, 125e6, 100, endpoint=False)
    sweep = {float(f): float("nan") for f in freqs}
    real = [f for f in freqs if 80e6 <= f < 120e6]
    for f in real:
        sweep[float(f)] = -50.0
    channel = _channel(75.0, 125.0, sweep)

    lo, hi = initial_freq_extent(channel, timeout=0.1)

    assert lo == min(real) / 1e6
    assert hi == max(real) / 1e6
    assert lo > 75.0
    assert hi < 125.0
