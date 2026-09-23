from dataclasses import dataclass


@dataclass
class SweepLine:
    hz_low: float
    hz_step: float
    powers: list[float]


def parse_sweep_line(raw: str) -> SweepLine | None:
    """Parse one line of rtl_power/hackrf_sweep CSV output.

    Both tools emit the same shape: date, time, hz_low, hz_high, hz_step,
    samples, power, power, ... Returns None for blank, short, or malformed lines.
    """
    raw = raw.strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) < 7:
        return None
    try:
        hz_low = float(parts[2])
        hz_step = float(parts[4])
        powers = [float(p) for p in parts[6:]]
    except ValueError:
        return None
    return SweepLine(hz_low=hz_low, hz_step=hz_step, powers=powers)


def suppress_dc_spike(powers: list[float], width: int = 1) -> list[float]:
    """Null out the AD9361's own LO self-mixing/DC-offset spike, which lands exactly at
    the center bin of every hop's capture on a direct-conversion (zero-IF) receiver like
    the AD9361 -- a hardware artifact, not real RF. Replaces the center `2*width+1` bins
    with a linear interpolation between their immediate flanking bins.

    Confirmed live 2026-09-23: measured real peak frequencies across a 150MHz Pluto
    sweep landed in an exact 16MHz comb (retained_step = capture_bandwidth * (1 -
    2*edge_trim)), one spike per hop at that hop's own tuned LO frequency -- too regular
    to be real RF, and unaffected by edge_trim since the spike sits at the capture's
    center, nowhere near either edge trim_edges() crops.

    Operates on a hop's full, untrimmed power array (same index space as trim_edges()
    consumes) -- call this before trim_edges(), not after, since the center index here
    (n // 2, matching np.fft.fftshift's placement of the zero-frequency bin) is only
    meaningful pre-trim.
    """
    n = len(powers)
    center = n // 2
    lo = max(0, center - width - 1)
    hi = min(n - 1, center + width + 1)
    result = list(powers)
    if hi <= lo:
        return result
    left, right = powers[lo], powers[hi]
    span = hi - lo
    for i in range(lo + 1, hi):
        result[i] = left + (right - left) * (i - lo) / span
    return result


def trim_edges(
    hz_low: float, hz_step: float, powers: list[float], edge_trim: float
) -> tuple[list[float], list[float]]:
    """Drop unreliable bins at each edge of a sweep hop and compute their frequencies."""
    n = len(powers)
    trim = max(1, int(n * edge_trim))
    trimmed_powers = powers[trim : n - trim]
    freqs = [hz_low + hz_step * (i + trim) for i in range(len(trimmed_powers))]
    return freqs, trimmed_powers
