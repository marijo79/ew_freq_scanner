from collections import deque

import numpy as np

from freqscan.sdr.base import Channel, SweepState
from freqscan.streaming.detector import nearest_grid_index
from freqscan.streaming.kafka_consumer import apply_message


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


def test_apply_message_uses_exact_index_no_snapping_needed():
    grid = np.array([900_000_000.0, 900_020_000.0, 900_040_000.0])
    channel = _make_channel(grid)

    # bin_index_deltas[0] is the absolute index (delta from an implicit 0) -- index 0
    # here means the producer's own nearest_grid_index() already resolved this bin to
    # the grid's first entry, so there's nothing left for the consumer to snap.
    payload = {"bin_index_deltas": [0], "power_dbm": [-50.0]}
    apply_message(channel, payload, grid)

    assert channel.state.sweep[900_000_000.0] == -50.0
    assert len(channel.state.sweep) == 3  # no new key was added


def test_apply_message_merges_multiple_bins_via_cumulative_deltas():
    grid = np.array([900_000_000.0, 900_020_000.0, 900_040_000.0])
    channel = _make_channel(grid)

    # index 0 (delta 0), then index 0+2=2 -- skipping the middle bin entirely.
    payload = {"bin_index_deltas": [0, 2], "power_dbm": [-50.0, -40.0]}
    apply_message(channel, payload, grid)

    assert channel.state.sweep[900_000_000.0] == -50.0
    assert channel.state.sweep[900_040_000.0] == -40.0
    assert np.isnan(channel.state.sweep[900_020_000.0])  # untouched bin stays NaN


def test_apply_message_consecutive_deltas_of_one_advance_index_by_one():
    grid = np.array([900_000_000.0, 900_020_000.0, 900_040_000.0])
    channel = _make_channel(grid)

    # The common keyframe case: a long contiguous run, every delta after the first is 1.
    payload = {"bin_index_deltas": [0, 1, 1], "power_dbm": [-50.0, -45.0, -40.0]}
    apply_message(channel, payload, grid)

    assert channel.state.sweep[900_000_000.0] == -50.0
    assert channel.state.sweep[900_020_000.0] == -45.0
    assert channel.state.sweep[900_040_000.0] == -40.0


def test_apply_message_overwrites_existing_value_on_later_call():
    grid = np.array([900_000_000.0, 900_020_000.0])
    channel = _make_channel(grid)

    apply_message(channel, {"bin_index_deltas": [0], "power_dbm": [-30.0]}, grid)
    apply_message(channel, {"bin_index_deltas": [0], "power_dbm": [-20.0]}, grid)

    assert channel.state.sweep[900_000_000.0] == -20.0
    assert len(channel.state.sweep) == 2
