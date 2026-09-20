"""Continuously monitor one fixed PlutoSDR-compatible frequency, emitting rtl_power-format
CSV -- the "stare" counterpart to scripts/pluto_sweep.py's "sweep" mode.

Sets the RX LO *once* at startup, then launches a single long-lived `iio_readdev`
subprocess (`-s 0` = stream forever, never restarted) and reads fixed-size chunks from
its continuous stdout in a loop -- unlike pluto_sweep.py, which restarts `iio_readdev`
fresh for every hop because it needs to retune between them. With no retuning, staying
on one process instead avoids that restart overhead entirely: hundreds of FFT windows
per second instead of roughly one per second, since dwell time on the one frequency of
interest matters more than frequency coverage here (see CLAUDE.md's "sweep vs. stare"
tradeoff notes). Each FFT window is printed as one CSV line, prefixed with which RX
channel it's from (`channel, date, time, hz_low, hz_high, hz_step, samples,
power, power, ...`) -- PlutoStareBackend strips that leading field before handing the
rest to the same `freqscan.parsing.parse_sweep_line()` every other backend uses
unchanged.

Still drops `--edge-trim` fraction of bins from each edge (via
`freqscan.parsing.trim_edges()`) before printing -- not to tile adjacent hops (there's
only one fixed window here, nothing to tile), but because the AD9361's anti-aliasing
filter still rolls off at the edges of any capture and those bins are still unreliable.

`--channel-configs` is a JSON list (same shape as `PLUTO_STARE__CHANNELS` in `.env`,
e.g. `[{"channel":1,"gain_control_mode":"manual","hardwaregain":30.0}, {"channel":2,...}]`)
naming every RX chain to capture -- RX1 (`voltage0`/`voltage1`) and/or RX2
(`voltage2`/`voltage3`) on the `cf-ad9361-lpc` device. Both physically share the same
RX_LO/sampling_frequency/rf_bandwidth (one shared local oscillator and ADC clock domain
feeds both antenna inputs on the chip -- true of every AD9361-based radio, not a
firmware limitation), so --frequency/--sample-rate always apply chip-wide regardless of
which channels are listed; only gain (gain_control_mode/hardwaregain) is genuinely
per-channel.

**One process captures every configured channel from a single `iio_readdev` connection**
-- found live 2026-09-04 that libiio's `usb:` backend (direct USB bulk transfer, unlike
the `ip:` network-over-USB-Ethernet-gadget backend) claims the USB interface
*exclusively*: two separate processes each opening their own `usb:` connection to the
same device (the original one-process-per-channel design) fails outright with "Unable to
claim interface: Device or resource busy" the moment a second one tries to connect, since
only `ip:` (whose IIOD network daemon on the Pluto itself multiplexes concurrent client
connections) tolerated that. `iio_readdev` interleaves multiple requested channels
per-sample-time in the exact order given on its command line (confirmed live by capturing
a few raw samples across all 4 sub-channels and inspecting the bytes directly), so one
buffer read yields `n_bins` interleaved samples of `(I1, Q1, I2, Q2, ...)` -- reshaping the
flat int16 array to `(n_bins, 2 * n_channels)` recovers each channel's own I/Q columns
directly, letting one process FFT and print a line per configured channel from one shared
capture.

Example:
    python scripts/pluto_stare.py --frequency 2437e6 --bin-width 10000 --sample-rate 2000000 \\
        --channel-configs '[{"channel":1,"gain_control_mode":"manual","hardwaregain":30.0},{"channel":2,"gain_control_mode":"manual","hardwaregain":50.0}]'
"""

import argparse
import datetime
import json
import subprocess
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from freqscan.parsing import trim_edges  # noqa: E402

PLUTO_MAX_BANDWIDTH_HZ = 56_000_000

# RX channel -> (cf-ad9361-lpc I/Q channel pair, ad9361-phy gain-control channel)
_CHANNEL_MAP = {
    1: (("voltage0", "voltage1"), "voltage0"),
    2: (("voltage2", "voltage3"), "voltage1"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--uri", default="ip:192.168.2.1")
    parser.add_argument("--frequency", type=float, required=True, help="Hz -- the fixed RX LO")
    parser.add_argument("--bin-width", type=float, default=10_000, help="Hz per FFT bin")
    parser.add_argument(
        "--sample-rate",
        type=float,
        default=20_000_000,
        help=f"Instantaneous bandwidth around --frequency, in Hz (<= {PLUTO_MAX_BANDWIDTH_HZ:,})",
    )
    parser.add_argument(
        "--edge-trim", type=float, default=0.1, help="Fraction of bins dropped from each edge"
    )
    parser.add_argument(
        "--channel-configs",
        required=True,
        help=(
            'JSON list of RX channels to capture, e.g. \'[{"channel":1,'
            '"gain_control_mode":"manual","hardwaregain":30.0}]\' -- gain_control_mode '
            '(default "slow_attack") and hardwaregain (default 40.0, only used when '
            'gain_control_mode is "manual") are optional per entry.'
        ),
    )
    return parser.parse_args()


def set_rx_chain(
    uri: str,
    frequency_hz: int,
    sample_rate_hz: int,
    rf_bandwidth_hz: int,
    channel_configs: list[dict],
) -> None:
    # frequency/sampling_frequency/rf_bandwidth are chip-wide (shared RX_LO/ADC clock, see
    # module docstring) -- set once via whichever channel's control path happens to be
    # first, it applies to the whole chip regardless of which channel's -i is used.
    subprocess.run(
        ["iio_attr", "-u", uri, "-c", "ad9361-phy", "altvoltage0", "frequency", str(frequency_hz)],
        check=True,
        capture_output=True,
    )
    first_gain_channel = _CHANNEL_MAP[channel_configs[0]["channel"]][1]
    for attr, value in [("sampling_frequency", sample_rate_hz), ("rf_bandwidth", rf_bandwidth_hz)]:
        subprocess.run(
            ["iio_attr", "-u", uri, "-c", "ad9361-phy", "-i", first_gain_channel, attr, str(value)],
            check=True,
            capture_output=True,
        )
    # Gain is genuinely per-channel.
    for cfg in channel_configs:
        gain_channel = _CHANNEL_MAP[cfg["channel"]][1]
        gain_control_mode = cfg.get("gain_control_mode", "slow_attack")
        subprocess.run(
            ["iio_attr", "-u", uri, "-c", "ad9361-phy", "-i", gain_channel, "gain_control_mode", gain_control_mode],
            check=True,
            capture_output=True,
        )
        if gain_control_mode == "manual":
            subprocess.run(
                [
                    "iio_attr", "-u", uri, "-c", "ad9361-phy", "-i", gain_channel,
                    "hardwaregain", str(cfg.get("hardwaregain", 40.0)),
                ],
                check=True,
                capture_output=True,
            )


def stare(
    uri: str, frequency: float, sample_rate: float, n_bins: int, edge_trim: float, channels: list[int]
) -> None:
    hz_step = sample_rate / n_bins
    hz_low = frequency - sample_rate / 2
    window = np.hanning(n_bins)
    n_sub = 2 * len(channels)  # I,Q per configured RX channel, in the same order as `channels`
    bytes_per_chunk = n_bins * 2 * n_sub  # int16 (2 bytes) x n_sub interleaved streams x n_bins samples

    iio_channels: list[str] = []
    for ch in channels:
        iio_channels.extend(_CHANNEL_MAP[ch][0])

    proc = subprocess.Popen(
        [
            "iio_readdev",
            "-u",
            uri,
            "-b",
            str(n_bins),
            "-s",
            "0",  # 0 = infinite: one continuous stream, never restarted between windows
            "cf-ad9361-lpc",
            *iio_channels,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        while True:
            chunk = proc.stdout.read(bytes_per_chunk)
            if len(chunk) < bytes_per_chunk:
                break  # process died or was killed mid-read
            # One row per sample-time, one (I, Q) column pair per configured channel, in
            # the exact order `iio_channels` was given on the command line (confirmed
            # live -- see module docstring).
            raw = np.frombuffer(chunk, dtype=np.int16).reshape(n_bins, n_sub)
            now = datetime.datetime.now()
            for idx, ch in enumerate(channels):
                i = raw[:, 2 * idx].astype(np.float64)
                q = raw[:, 2 * idx + 1].astype(np.float64)
                iq = i + 1j * q

                spectrum = np.fft.fftshift(np.fft.fft(iq * window))
                power_db = 20 * np.log10(np.abs(spectrum) / n_bins + 1e-12)
                freqs, trimmed = trim_edges(hz_low, hz_step, list(power_db), edge_trim)

                hz_high = freqs[-1] + hz_step
                powers_str = ", ".join(f"{p:.2f}" for p in trimmed)
                print(
                    f"{ch}, {now:%Y-%m-%d}, {now:%H:%M:%S}, {freqs[0]:.1f}, {hz_high:.1f}, "
                    f"{hz_step:.3f}, {len(trimmed)}, {powers_str}"
                )
            sys.stdout.flush()
    finally:
        proc.terminate()
        if proc.returncode is None:
            proc.wait(timeout=2)


def main() -> None:
    args = parse_args()
    if not (0 <= args.edge_trim < 0.5):
        sys.exit("--edge-trim must be in [0, 0.5)")
    if args.sample_rate > PLUTO_MAX_BANDWIDTH_HZ:
        sys.exit(f"--sample-rate exceeds Pluto's ~{PLUTO_MAX_BANDWIDTH_HZ:,}Hz hardware max")

    n_bins = max(1, round(args.sample_rate / args.bin_width))
    # Same crash-prevention check as pluto_sweep.py: trim_edges() drops max(1, int(n_bins *
    # edge_trim)) bins from each side -- too few bins per window and trimming eats the
    # entire capture, leaving trim_edges() nothing to return.
    trim = max(1, int(n_bins * args.edge_trim))
    if n_bins - 2 * trim < 1:
        sys.exit(
            f"--bin-width ({args.bin_width:,.0f}Hz) is too coarse for --sample-rate "
            f"({args.sample_rate:,.0f}Hz): only {n_bins} bin(s) per window, but "
            f"edge_trim={args.edge_trim} needs to drop {trim} bin(s) from each side "
            f"({2 * trim} total) -- decrease --bin-width, increase --sample-rate, or lower --edge-trim"
        )

    channel_configs = json.loads(args.channel_configs)
    channels = [cfg["channel"] for cfg in channel_configs]
    if len(channels) != len(set(channels)):
        sys.exit(f"--channel-configs lists the same RX channel more than once: {channels}")

    set_rx_chain(args.uri, int(args.frequency), int(args.sample_rate), int(args.sample_rate), channel_configs)
    stare(args.uri, args.frequency, args.sample_rate, n_bins, args.edge_trim, channels)


if __name__ == "__main__":
    main()
