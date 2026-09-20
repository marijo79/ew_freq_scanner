"""Sweep a PlutoSDR-compatible device across one or more frequency ranges, emitting
rtl_power-format CSV.

Retunes the RX LO hop-by-hop by spawning `iio_attr`/`iio_readdev` per hop -- the same
subprocess-CLI pattern freqscan already uses for rtl_power/hackrf_sweep -- FFTs each hop's
raw IQ capture, and prints one CSV line per hop to stdout in the same
`date,time,hz_low,hz_high,hz_step,samples,power,power,...` format
`freqscan.parsing.parse_sweep_line()` expects, so PlutoBackend can reuse that same parser
unchanged. Multiple `--range` arguments are swept in turn from one persistent IIO
connection, one process -- mirroring hackrf_sweep's single-process-multi-range model,
since Pluto (like HackRF) is one exclusive-access physical radio; unlike RTL's one
subprocess per (independent) device, two Pluto sweep processes can't run concurrently
against the same hardware.

Mirrors rtl_power's own internal widen-then-crop approach (see CLAUDE.md item 2/3 on
RTLBackend/HackRFBackend): each hop captures wider than its nominal step and drops that
range's own edge-trim fraction off both edges (via `freqscan.parsing.trim_edges()|`)
before printing, so adjacent hops tile without gaps despite the AD9361's anti-aliasing
filter roll-off at the edges of every capture.

`--channel {1,2}` picks which AD9361 RX chain to capture -- RX1 (`voltage0`/`voltage1`) or
RX2 (`voltage2`/`voltage3`) on the `cf-ad9361-lpc` device. Both physically share the same
RX_LO/sampling_frequency/rf_bandwidth (one shared local oscillator and ADC clock domain
feeds both antenna inputs on the chip -- true of every AD9361-based radio, not a firmware
limitation), so those apply chip-wide regardless of --channel; only gain
(--gain-control-mode/--hardwaregain) is genuinely per-channel. Confirmed live 2026-09-03
that two separate instances of this script, one per channel, can run concurrently against
the same device with no buffer conflict, though both share the same physical USB/network
link so running both at once measurably reduces each channel's achievable throughput.

Example:
    python scripts/pluto_sweep.py --bin-width 25000 --channel 1 \\
        --range 100e6:115e6:0.1 --range 400e6:420e6:0.05
"""

import argparse
import datetime
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from freqscan.parsing import trim_edges  # noqa: E402

PLUTO_MAX_BANDWIDTH_HZ = 56_000_000

# channel -> (cf-ad9361-lpc I/Q channel pair, ad9361-phy gain-control channel)
_CHANNEL_MAP = {
    1: (("voltage0", "voltage1"), "voltage0"),
    2: (("voltage2", "voltage3"), "voltage1"),
}


@dataclass
class Range:
    freq_start: float  # Hz
    freq_stop: float  # Hz
    edge_trim: float


def _parse_range(spec: str) -> Range:
    parts = spec.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"--range must be START:STOP:EDGE_TRIM, got {spec!r}")
    try:
        freq_start, freq_stop, edge_trim = float(parts[0]), float(parts[1]), float(parts[2])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--range must be START:STOP:EDGE_TRIM, got {spec!r}") from exc
    if not (0 <= edge_trim < 0.5):
        raise argparse.ArgumentTypeError(f"--range edge_trim must be in [0, 0.5), got {edge_trim}")
    return Range(freq_start, freq_stop, edge_trim)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--uri", default="ip:192.168.2.1")
    parser.add_argument(
        "--range",
        dest="ranges",
        action="append",
        type=_parse_range,
        required=True,
        help="START:STOP:EDGE_TRIM in Hz (e.g. 100e6:115e6:0.1) -- repeatable",
    )
    parser.add_argument("--bin-width", type=float, default=25_000, help="Hz per FFT bin")
    parser.add_argument(
        "--capture-bandwidth",
        type=float,
        default=20_000_000,
        help=f"Per-hop instantaneous bandwidth in Hz (<= {PLUTO_MAX_BANDWIDTH_HZ:,})",
    )
    parser.add_argument(
        "--gain-control-mode",
        default="slow_attack",
        choices=["manual", "fast_attack", "slow_attack", "hybrid"],
    )
    parser.add_argument(
        "--hardwaregain", type=float, default=40.0, help="dB; only used when --gain-control-mode=manual"
    )
    parser.add_argument(
        "--repeat", action="store_true", help="Loop every configured range forever instead of running once"
    )
    parser.add_argument(
        "--channel", type=int, choices=[1, 2], default=1, help="Which AD9361 RX chain to capture"
    )
    return parser.parse_args()


def set_lo(uri: str, frequency_hz: int) -> None:
    subprocess.run(
        ["iio_attr", "-u", uri, "-c", "ad9361-phy", "altvoltage0", "frequency", str(frequency_hz)],
        check=True,
        capture_output=True,
    )


def set_rx_chain(
    uri: str,
    sampling_frequency_hz: int,
    rf_bandwidth_hz: int,
    gain_control_mode: str,
    hardwaregain: float,
    gain_channel: str,
) -> None:
    # sampling_frequency/rf_bandwidth are chip-wide (shared RX_LO/ADC clock, see module
    # docstring) -- setting them via either RX channel's own control channel has the same
    # effect, so if two instances of this script are running (one per RX channel) both
    # redundantly set the same values here, harmlessly, rather than needing to coordinate.
    attrs = [
        ("sampling_frequency", sampling_frequency_hz),
        ("rf_bandwidth", rf_bandwidth_hz),
        ("gain_control_mode", gain_control_mode),
    ]
    if gain_control_mode == "manual":
        attrs.append(("hardwaregain", hardwaregain))
    for attr, value in attrs:
        subprocess.run(
            ["iio_attr", "-u", uri, "-c", "ad9361-phy", "-i", gain_channel, attr, str(value)],
            check=True,
            capture_output=True,
        )


def capture_hop(uri: str, n_samples: int, iio_channels: tuple[str, str]) -> np.ndarray:
    proc = subprocess.run(
        [
            "iio_readdev",
            "-u",
            uri,
            "-b",
            str(n_samples),
            "-s",
            str(n_samples),
            "cf-ad9361-lpc",
            *iio_channels,
        ],
        check=True,
        capture_output=True,
    )
    raw = np.frombuffer(proc.stdout, dtype=np.int16)
    i, q = raw[0::2].astype(np.float64), raw[1::2].astype(np.float64)
    return i + 1j * q


def sweep_range(
    uri: str, r: Range, capture_bw: float, n_bins: int, iio_channels: tuple[str, str]
) -> None:
    retained_step = capture_bw * (1 - 2 * r.edge_trim)
    hz_step = capture_bw / n_bins
    window = np.hanning(n_bins)

    freq = r.freq_start
    while freq < r.freq_stop:
        lo = freq + capture_bw * (0.5 - r.edge_trim)
        set_lo(uri, int(lo))

        iq = capture_hop(uri, n_bins, iio_channels)
        spectrum = np.fft.fftshift(np.fft.fft(iq * window))
        power_db = 20 * np.log10(np.abs(spectrum) / n_bins + 1e-12)

        hz_low = lo - capture_bw / 2
        freqs, trimmed = trim_edges(hz_low, hz_step, list(power_db), r.edge_trim)

        now = datetime.datetime.now()
        hz_high = freqs[-1] + hz_step
        powers_str = ", ".join(f"{p:.2f}" for p in trimmed)
        print(
            f"{now:%Y-%m-%d}, {now:%H:%M:%S}, {freqs[0]:.1f}, {hz_high:.1f}, "
            f"{hz_step:.3f}, {len(trimmed)}, {powers_str}"
        )
        sys.stdout.flush()

        freq += retained_step


def main() -> None:
    args = parse_args()
    if args.capture_bandwidth > PLUTO_MAX_BANDWIDTH_HZ:
        sys.exit(f"--capture-bandwidth exceeds Pluto's ~{PLUTO_MAX_BANDWIDTH_HZ:,}Hz hardware max")

    n_bins = max(1, round(args.capture_bandwidth / args.bin_width))
    # trim_edges() drops max(1, int(n_bins * edge_trim)) bins from each side -- if
    # --bin-width is too coarse relative to --capture-bandwidth (fewer than a couple bins
    # per hop to begin with), edge-trimming can eat the entire hop, leaving trim_edges()
    # nothing to return and crashing sweep_range() with an IndexError instead of any
    # usable output. Check every configured range's own edge_trim up front.
    for r in args.ranges:
        trim = max(1, int(n_bins * r.edge_trim))
        if n_bins - 2 * trim < 1:
            sys.exit(
                f"--bin-width ({args.bin_width:,.0f}Hz) is too coarse for "
                f"--capture-bandwidth ({args.capture_bandwidth:,.0f}Hz): only {n_bins} bin(s) "
                f"per hop, but range {r.freq_start:,.0f}:{r.freq_stop:,.0f}'s edge_trim="
                f"{r.edge_trim} needs to drop {trim} bin(s) from each side ({2 * trim} total) "
                "-- decrease --bin-width, increase --capture-bandwidth, or lower that range's edge_trim"
            )
    iio_channels, gain_channel = _CHANNEL_MAP[args.channel]
    set_rx_chain(
        args.uri,
        int(args.capture_bandwidth),
        int(args.capture_bandwidth),
        args.gain_control_mode,
        args.hardwaregain,
        gain_channel,
    )

    while True:
        for r in args.ranges:
            sweep_range(args.uri, r, args.capture_bandwidth, n_bins, iio_channels)
        if not args.repeat:
            break


if __name__ == "__main__":
    main()
