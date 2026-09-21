import subprocess
import sys
import threading
from collections import deque
from pathlib import Path

import numpy as np

from freqscan.config import PlutoChannelConfig, PlutoSettings
from freqscan.csv_writer import CsvSweepWriter
from freqscan.parsing import parse_sweep_line
from freqscan.sdr.base import Channel, SDRBackend, SweepState
from freqscan.streaming.detector import KeyframeScheduler, NoiseFloorDetector, nearest_grid_index
from freqscan.streaming.kafka_publisher import KafkaSignalPublisher

_SWEEP_SCRIPT = Path(__file__).resolve().parent.parent.parent.parent / "scripts" / "pluto_sweep.py"


def _make_channels(settings: PlutoSettings, waterfall_rows: int) -> list[Channel]:
    # RX channel is the outer grouping (matches PlutoStareBackend and this module's own
    # per-RX-channel subprocess/thread split below) -- for N ranges and M RX channels,
    # channel list order is [RX_ch0's N ranges, RX_ch1's N ranges, ...].
    return [
        Channel(
            label=f"Pluto RX{ch.channel}: {r.freq_start:.0f}-{r.freq_stop:.0f} MHz",
            freq_start_mhz=r.freq_start,
            freq_stop_mhz=r.freq_stop,
            state=SweepState(history=deque(maxlen=waterfall_rows)),
        )
        for ch in settings.channels
        for r in settings.ranges
    ]


class PlutoBackend(SDRBackend):
    """One scripts/pluto_sweep.py subprocess per configured RX channel, each covering
    all configured ranges, demuxed by frequency -- same one-process-multi-range model as
    HackRFBackend within each RX channel's own process, since Pluto is also one
    exclusive-access physical radio (two ranges can't be swept concurrently against the
    same channel). RX1/RX2 physically share the same RX_LO/sample_rate/rf_bandwidth (see
    PlutoChannelConfig), so every subprocess gets the same --bin-width/--capture-bandwidth
    and --range set; only --channel and that channel's own gain differ.

    Confirmed live 2026-09-03 that two concurrent instances work correctly (each gets
    real, independent data, no buffer conflict) -- but both streams share the same
    physical USB/network link to the device, so running both at once measurably reduces
    each channel's achievable throughput compared to running just one."""

    def __init__(
        self,
        settings: PlutoSettings,
        waterfall_rows: int,
        publisher: KafkaSignalPublisher | None = None,
        detectors: list[NoiseFloorDetector] | None = None,
        partitions: list[int] | None = None,
        canonical_freqs: list[np.ndarray] | None = None,
        csv_writers: list[CsvSweepWriter] | None = None,
        keyframes: list[KeyframeScheduler] | None = None,
    ):
        super().__init__()
        self.settings = settings
        self.channels = _make_channels(settings, waterfall_rows)
        self._procs: list[subprocess.Popen] = []
        self._procs_lock = threading.Lock()
        self._publisher = publisher
        self._detectors = detectors
        self._partitions = partitions
        self._canonical_freqs = canonical_freqs
        self._csv_writers = csv_writers
        self._keyframes = keyframes

    def _range_index_for(self, hz_low: float) -> int | None:
        mhz = hz_low / 1e6
        for i, r in enumerate(self.settings.ranges):
            if r.freq_start <= mhz < r.freq_stop:
                return i
        return None

    def start(self) -> None:
        n_ranges = len(self.settings.ranges)
        for ch_idx, ch in enumerate(self.settings.channels):
            lo, hi = ch_idx * n_ranges, ch_idx * n_ranges + n_ranges
            threading.Thread(
                target=self._run_channel,
                args=(
                    ch,
                    self.channels[lo:hi],
                    self._detectors[lo:hi] if self._detectors is not None else None,
                    self._partitions[lo:hi] if self._partitions is not None else None,
                    self._canonical_freqs[lo:hi] if self._canonical_freqs is not None else None,
                    self._csv_writers[lo:hi] if self._csv_writers is not None else None,
                    self._keyframes[lo:hi] if self._keyframes is not None else None,
                ),
                daemon=True,
            ).start()

    def _run_channel(
        self,
        ch: PlutoChannelConfig,
        channels: list[Channel],
        detectors: list[NoiseFloorDetector] | None,
        partitions: list[int] | None,
        canonical_freqs: list[np.ndarray] | None,
        csv_writers: list[CsvSweepWriter] | None,
        keyframes: list[KeyframeScheduler] | None = None,
    ) -> None:
        range_args = []
        for r in self.settings.ranges:
            range_args += ["--range", f"{r.freq_start * 1e6}:{r.freq_stop * 1e6}:{r.edge_trim}"]
        cmd = [
            sys.executable, str(_SWEEP_SCRIPT),
            "--uri", self.settings.uri,
            "--bin-width", str(self.settings.bin_width),
            "--capture-bandwidth", str(self.settings.capture_bandwidth),
            "--channel", str(ch.channel),
            "--gain-control-mode", ch.gain_control_mode,
            "--hardwaregain", str(ch.hardwaregain),
            "--repeat",
            *range_args,
        ]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except OSError as exc:
            self.report_error(f"failed to launch pluto_sweep.py (RX{ch.channel}): {exc}")
            return
        with self._procs_lock:
            self._procs.append(proc)
        for raw in proc.stdout:
            line = parse_sweep_line(raw)
            if line is None:
                continue
            idx = self._range_index_for(line.hz_low)
            if idx is None:
                continue
            r = self.settings.ranges[idx]
            # pluto_sweep.py already crops each hop to its own edge_trim before printing
            # (see its widen-then-crop docstring), but a hop's last-in-range capture can
            # still overshoot this range's configured freq_stop by up to one retained
            # step (same overshoot HackRFBackend clips for its own hard minimum-segment
            # quirk) -- clip to the range actually configured rather than trusting the
            # whole hop belongs to it.
            range_start_hz, range_stop_hz = r.freq_start * 1e6, r.freq_stop * 1e6
            in_range = [
                (f, p)
                for f, p in zip(line.hz_low + line.hz_step * np.arange(len(line.powers)), line.powers)
                if range_start_hz <= f < range_stop_hz
            ]
            if not in_range:
                continue
            freqs, powers = zip(*in_range)
            channel = channels[idx]
            with channel.state.lock:
                for f, p in zip(freqs, powers):
                    channel.state.sweep[f] = p

            if csv_writers is not None:
                csv_writers[idx].write_hop(freqs, powers)

            if detectors is not None:
                detector = detectors[idx]
                partition = partitions[idx] if partitions is not None else -1
                freqs_arr = np.asarray(freqs)
                powers_arr = np.asarray(powers)
                indices = nearest_grid_index(canonical_freqs[idx], freqs_arr)
                flagged_mask = detector.flag_hop(indices, powers_arr)
                flagged = list(zip(freqs_arr[flagged_mask].tolist(), powers_arr[flagged_mask].tolist()))
                self._publisher.publish(channel.label, flagged, partition=partition)
                keyframe = keyframes[idx] if keyframes is not None else None
                if keyframe is not None and keyframe.due():
                    all_bins = list(zip(freqs_arr.tolist(), powers_arr.tolist()))
                    self._publisher.publish(channel.label, all_bins, partition=partition)
                    keyframe.mark_sent()

        proc.wait()
        if proc.returncode > 0:
            stderr_output = proc.stderr.read().strip() if proc.stderr else ""
            self.report_error(
                f"pluto_sweep.py (RX{ch.channel}) exited with code {proc.returncode}: {stderr_output}"
            )

    def stop(self) -> None:
        with self._procs_lock:
            for proc in self._procs:
                proc.terminate()
        if self._publisher is not None:
            self._publisher.flush()
        for writer in self._csv_writers or []:
            if writer is not None:
                writer.close()
