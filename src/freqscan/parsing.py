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


def trim_edges(
    hz_low: float, hz_step: float, powers: list[float], edge_trim: float
) -> tuple[list[float], list[float]]:
    """Drop unreliable bins at each edge of a sweep hop and compute their frequencies."""
    n = len(powers)
    trim = max(1, int(n * edge_trim))
    trimmed_powers = powers[trim : n - trim]
    freqs = [hz_low + hz_step * (i + trim) for i in range(len(trimmed_powers))]
    return freqs, trimmed_powers
