import time
from dataclasses import dataclass, field

import numpy as np


class KeyframeScheduler:
    """Tracks whether it's time to publish a full-spectrum "keyframe" for one channel --
    every bin of the current hop, not just the ones NoiseFloorDetector.flag_hop()
    flagged (see KafkaSettings.keyframe_interval_s). Deliberately dumb/stateless beyond
    one timestamp: each backend calls due() right where it already checks flag_hop()'s
    result, and mark_sent() right after actually publishing a keyframe -- same call site,
    same per-hop granularity, just an additional independent publish.

    interval_s=None disables keyframes entirely (due() always False, mark_sent() a
    no-op) -- the KafkaSettings default, so this is opt-in per deployment. due() is
    True immediately on construction (next_at starts at 0.0) so a fresh consumer gets a
    real baseline on the very first hop instead of waiting a full interval first."""

    def __init__(self, interval_s: float | None):
        self._interval = interval_s
        self._next_at = 0.0

    def due(self, now: float | None = None) -> bool:
        if self._interval is None:
            return False
        return (now if now is not None else time.time()) >= self._next_at

    def mark_sent(self, now: float | None = None) -> None:
        if self._interval is not None:
            self._next_at = (now if now is not None else time.time()) + self._interval


def nearest_grid_index(canonical_freqs: np.ndarray, freq_hz):
    """Index (or array of indices, matching freq_hz's shape) of the canonical grid
    frequency closest to freq_hz. canonical_freqs must be sorted ascending.

    Used by each backend's producer thread to snap a hop's real, possibly
    slightly-off-grid bin centers onto the channel's fixed n_bins grid, both for
    updating detector state and for what actually gets published to Kafka (as this
    exact index, delta-encoded -- see kafka_publisher.build_payload()). The Kafka
    viewer's own consumer side (kafka_consumer.apply_message()) no longer needs this at
    all: since the wire format carries the exact index rather than freq_hz, there's
    nothing left to snap on the consuming end -- a byproduct of the switch away from
    freq_hz-keyed messages, not just a size optimization."""
    n = len(canonical_freqs)
    freq_hz = np.asarray(freq_hz)
    idx = np.searchsorted(canonical_freqs, freq_hz)
    idx_clamped = np.clip(idx, 1, n - 1)
    before = canonical_freqs[idx_clamped - 1]
    after = canonical_freqs[idx_clamped]
    prefer_before = (freq_hz - before) <= (after - freq_hz)
    result = np.where(prefer_before, idx_clamped - 1, idx_clamped)
    result = np.where(idx == 0, 0, result)
    result = np.where(idx == n, n - 1, result)
    return result


@dataclass
class NoiseFloorDetector:
    """Vectorized per-bin adaptive noise-floor detector over a channel's full n_bins grid.

    Each of the n_bins bins gets its own fixed-size rolling window (a circular buffer,
    not a Python dict of deques); a bin isn't flagged until its own window has filled
    once (cold start), so an adaptive baseline exists to compare against.

    Optionally also requires the reading to stand out from the rest of its own hop
    (spatial_margin_db, compared against that hop's own median), on top of standing
    out from its own history — catches a whole band being uniformly noisy without
    treating every bin in it as a separate signal. This is an AND: it can only suppress
    a bin the temporal check already flagged, never flag one on its own. None (default)
    skips this entirely, keeping pure temporal behavior.

    Separately, optionally also flags a bin regardless of its own history if it's simply
    prominent right now — far enough above the rest of its own hop (prominence_margin_db,
    also compared against that hop's median). This is an OR with the temporal check, not
    an AND: it exists for signals that are always there and always strong (e.g. a steady
    FM broadcast carrier) — the temporal check alone never flags these, since the
    adaptive baseline just learns to expect them, so nothing ever looks "new". A generous
    margin (bigger than spatial_margin_db) keeps this from re-triggering on generically
    noisy broadband bins, which sit close to their own hop's median by definition — only
    an isolated peak clears a large prominence margin. None (default) disables this.

    Processes one whole hop at a time (flag_hop), not one bin at a time — batches all the
    per-bin work (history mean, comparisons) into a handful of vectorized numpy
    operations instead of a Python-level loop over every bin. Measured ~9x faster at
    20,000 bins/hop than the equivalent per-bin Python loop; on weaker hardware (e.g. a
    Raspberry Pi), the per-bin loop can fail to keep up with hackrf_sweep's output rate
    at all, backlogging the whole process.
    """

    n_bins: int
    window: int
    margin_db: float
    spatial_margin_db: float | None = None
    prominence_margin_db: float | None = None
    _history: np.ndarray = field(init=False, repr=False)
    _fill_count: np.ndarray = field(init=False, repr=False)
    _cursor: np.ndarray = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._history = np.zeros((self.window, self.n_bins), dtype=np.float64)
        self._fill_count = np.zeros(self.n_bins, dtype=np.int64)
        self._cursor = np.zeros(self.n_bins, dtype=np.int64)

    def flag_hop(self, indices: np.ndarray, powers: np.ndarray) -> np.ndarray:
        """indices: this hop's bins, as indices into the channel's full n_bins grid
        (see nearest_grid_index). powers: their power readings, same length/order.
        Returns a boolean array, same length as indices, of which bins are signal."""
        filled = self._fill_count[indices] >= self.window
        baseline = self._history[:, indices].mean(axis=0)
        is_temporal = filled & (powers > baseline + self.margin_db)
        is_signal = is_temporal.copy()

        needs_hop_baseline = self.spatial_margin_db is not None or self.prominence_margin_db is not None
        if needs_hop_baseline and powers.size:
            hop_baseline = np.median(powers)
            if self.spatial_margin_db is not None:
                spatial_pass = powers > (hop_baseline + self.spatial_margin_db)
                is_signal = np.where(is_temporal, spatial_pass, is_signal)
            if self.prominence_margin_db is not None:
                prominence_pass = powers > (hop_baseline + self.prominence_margin_db)
                is_signal = np.where(~is_signal, prominence_pass, is_signal)

        cursor = self._cursor[indices]
        self._history[cursor, indices] = powers
        self._fill_count[indices] = np.minimum(self._fill_count[indices] + 1, self.window)
        self._cursor[indices] = (cursor + 1) % self.window

        return is_signal
