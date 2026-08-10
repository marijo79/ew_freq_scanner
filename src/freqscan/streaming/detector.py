import statistics
from collections import deque
from dataclasses import dataclass, field


def hop_median(powers: list[float]) -> float | None:
    """Median power across one hop's bins — the spatial signal baseline. None if empty."""
    return statistics.median(powers) if powers else None


@dataclass
class NoiseFloorDetector:
    """Tracks a per-bin rolling baseline and flags readings that exceed it by a margin.

    Each frequency bin gets its own fixed-size history; a bin isn't flagged until its
    history has filled once (cold start), so an adaptive baseline exists to compare
    against.

    Optionally also requires the reading to stand out from the rest of its own hop
    (spatial_margin_db, compared against that hop's hop_median()), on top of standing
    out from its own history — catches a whole band being uniformly noisy without
    treating every bin in it as a separate signal. This is an AND: it can only suppress
    a bin the temporal check already flagged, never flag one on its own. None (default)
    skips this entirely, keeping pure temporal behavior.

    Separately, optionally also flags a bin regardless of its own history if it's simply
    prominent right now — far enough above the rest of its own hop (prominence_margin_db,
    also compared against hop_median()). This is an OR with the temporal check, not an
    AND: it exists for signals that are always there and always strong (e.g. a steady FM
    broadcast carrier) — the temporal check alone never flags these, since the adaptive
    baseline just learns to expect them, so nothing ever looks "new". A generous margin
    (bigger than spatial_margin_db) keeps this from re-triggering on generically noisy
    broadband bins, which sit close to their own hop's median by definition — only an
    isolated peak clears a large prominence margin. None (default) disables this.
    """

    window: int
    margin_db: float
    spatial_margin_db: float | None = None
    prominence_margin_db: float | None = None
    _history: dict[float, deque] = field(default_factory=dict, repr=False)

    def flag(self, freq_hz: float, power_dbm: float, spatial_baseline: float | None = None) -> bool:
        history = self._history.setdefault(freq_hz, deque(maxlen=self.window))
        is_signal = False
        if len(history) == self.window:
            baseline = sum(history) / len(history)
            is_signal = power_dbm > baseline + self.margin_db
        history.append(power_dbm)

        if is_signal and self.spatial_margin_db is not None and spatial_baseline is not None:
            is_signal = power_dbm > spatial_baseline + self.spatial_margin_db

        if not is_signal and self.prominence_margin_db is not None and spatial_baseline is not None:
            is_signal = power_dbm > spatial_baseline + self.prominence_margin_db

        return is_signal
