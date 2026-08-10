from collections import deque

import numpy as np

from freqscan.sdr.base import Channel, SweepState
from freqscan.streaming.kafka_consumer import apply_message, nearest_grid_index


def _make_channel(grid: np.ndarray) -> Channel:
    channel = Channel(
        label="HackRF: 850-950 MHz",
        freq_start_mhz=grid[0] / 1e6,
        freq_stop_mhz=grid[-1] / 1e6,
        state=SweepState(history=deque(maxlen=10)),
    )
    for f in grid:
        channel.state.sweep[float(f)] = float("nan")
    return channel


def test_nearest_grid_index_exact_match():
    grid = np.array([100.0, 110.0, 120.0])
    assert nearest_grid_index(grid, 110.0) == 1


def test_nearest_grid_index_rounds_to_closer_neighbor():
    grid = np.array([100.0, 110.0, 120.0])
    assert nearest_grid_index(grid, 106.0) == 1  # closer to 110 than 100
    assert nearest_grid_index(grid, 104.0) == 0  # closer to 100 than 110


def test_nearest_grid_index_clamps_outside_range():
    grid = np.array([100.0, 110.0, 120.0])
    assert nearest_grid_index(grid, 5.0) == 0
    assert nearest_grid_index(grid, 999.0) == 2


def test_apply_message_snaps_to_nearest_canonical_freq():
    grid = np.array([900_000_000.0, 900_020_000.0, 900_040_000.0])
    channel = _make_channel(grid)

    # Producer's actual reading is a few Hz off the nominal grid value (real rtl_power/
    # hackrf_sweep bins don't always land exactly on a config-derived lattice) — it must
    # still land on the existing grid key, not create a new one.
    payload = {"bins": [{"freq_hz": 900_000_003.0, "power_dbm": -50.0}]}
    apply_message(channel, payload, grid)

    assert channel.state.sweep[900_000_000.0] == -50.0
    assert len(channel.state.sweep) == 3  # no new key was added


def test_apply_message_merges_multiple_bins_in_one_call():
    grid = np.array([900_000_000.0, 900_020_000.0, 900_040_000.0])
    channel = _make_channel(grid)

    payload = {
        "bins": [
            {"freq_hz": 900_000_000.0, "power_dbm": -50.0},
            {"freq_hz": 900_040_000.0, "power_dbm": -40.0},
        ]
    }
    apply_message(channel, payload, grid)

    assert channel.state.sweep[900_000_000.0] == -50.0
    assert channel.state.sweep[900_040_000.0] == -40.0
    assert np.isnan(channel.state.sweep[900_020_000.0])  # untouched bin stays NaN


def test_apply_message_overwrites_existing_value_on_later_call():
    grid = np.array([900_000_000.0, 900_020_000.0])
    channel = _make_channel(grid)

    apply_message(channel, {"bins": [{"freq_hz": 900_000_000.0, "power_dbm": -30.0}]}, grid)
    apply_message(channel, {"bins": [{"freq_hz": 900_000_000.0, "power_dbm": -20.0}]}, grid)

    assert channel.state.sweep[900_000_000.0] == -20.0
    assert len(channel.state.sweep) == 2
