import subprocess
import threading
from collections import deque

import numpy as np

from freqscan.config import HackRFSettings, RangeConfig
from freqscan.csv_writer import CsvSweepWriter
from freqscan.parsing import parse_sweep_line, trim_edges
from freqscan.sdr.base import Channel, SDRBackend, SweepState
from freqscan.streaming.detector import KeyframeScheduler, NoiseFloorDetector, nearest_grid_index
from freqscan.streaming.kafka_publisher import KafkaSignalPublisher


def _make_channel(r: RangeConfig, waterfall_rows: int) -> Channel:
    return Channel(
        label=f"HackRF: {r.freq_start:.0f}-{r.freq_stop:.0f} MHz",
        freq_start_mhz=r.freq_start,
        freq_stop_mhz=r.freq_stop,
        state=SweepState(history=deque(maxlen=waterfall_rows)),
    )


class HackRFBackend(SDRBackend):
    """A single hackrf_sweep subprocess covering all configured ranges, demuxed by frequency."""

    def __init__(
        self,
        settings: HackRFSettings,
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
        self.channels = [_make_channel(r, waterfall_rows) for r in settings.ranges]
        self._proc: subprocess.Popen | None = None
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
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        freq_args = []
        for r in self.settings.ranges:
            freq_args += ["-f", f"{int(r.freq_start)}:{int(r.freq_stop)}"]
        cmd = [
            "hackrf_sweep", *freq_args,
            "-w", str(self.settings.bin_width),
            "-l", str(self.settings.lna_gain),
            "-g", str(self.settings.vga_gain),
            "-a", "1" if self.settings.amp_enable else "0",
        ]
        try:
            self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except OSError as exc:
            self.report_error(f"failed to launch hackrf_sweep: {exc}")
            return
        for raw in self._proc.stdout:
            line = parse_sweep_line(raw)
            if line is None:
                continue
            idx = self._range_index_for(line.hz_low)
            if idx is None:
                continue
            r = self.settings.ranges[idx]
            freqs, powers = trim_edges(line.hz_low, line.hz_step, line.powers, r.edge_trim)
            # hackrf_sweep has a hard minimum sweep width tied to its sample rate (~20MHz) —
            # a configured range narrower than that gets silently widened by the tool itself
            # (e.g. requesting 106:107 actually sweeps 106-126), so a hop's bins can extend
            # past this range's own freq_stop even though its hz_low matched. Clip to the
            # range actually configured rather than trusting the whole hop belongs to it.
            range_start_hz, range_stop_hz = r.freq_start * 1e6, r.freq_stop * 1e6
            in_range = [(f, p) for f, p in zip(freqs, powers) if range_start_hz <= f < range_stop_hz]
            if not in_range:
                continue
            freqs, powers = zip(*in_range)
            channel = self.channels[idx]
            with channel.state.lock:
                for f, p in zip(freqs, powers):
                    channel.state.sweep[f] = p

            if self._csv_writers is not None:
                self._csv_writers[idx].write_hop(freqs, powers)

            if self._detectors is not None:
                detector = self._detectors[idx]
                partition = self._partitions[idx] if self._partitions is not None else -1
                freqs_arr = np.asarray(freqs)
                powers_arr = np.asarray(powers)
                indices = nearest_grid_index(self._canonical_freqs[idx], freqs_arr)
                flagged_mask = detector.flag_hop(indices, powers_arr)
                flagged = list(zip(freqs_arr[flagged_mask].tolist(), powers_arr[flagged_mask].tolist()))
                self._publisher.publish(channel.label, flagged, partition=partition)
                keyframe = self._keyframes[idx] if self._keyframes is not None else None
                if keyframe is not None and keyframe.due():
                    all_bins = list(zip(freqs_arr.tolist(), powers_arr.tolist()))
                    self._publisher.publish(channel.label, all_bins, partition=partition)
                    keyframe.mark_sent()

        self._proc.wait()
        if self._proc.returncode > 0:
            stderr_output = self._proc.stderr.read().strip() if self._proc.stderr else ""
            self.report_error(
                f"hackrf_sweep exited with code {self._proc.returncode}: {stderr_output}"
            )

    def stop(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
        if self._publisher is not None:
            self._publisher.flush()
        for writer in self._csv_writers or []:
            if writer is not None:
                writer.close()
