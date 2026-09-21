import subprocess
import threading
from collections import deque

import numpy as np

from freqscan.config import DeviceConfig, RTLSettings
from freqscan.csv_writer import CsvSweepWriter
from freqscan.parsing import parse_sweep_line
from freqscan.sdr.base import Channel, SDRBackend, SweepState
from freqscan.streaming.detector import KeyframeScheduler, NoiseFloorDetector, nearest_grid_index
from freqscan.streaming.kafka_publisher import KafkaSignalPublisher

_SUFFIX_TO_HZ = {"k": 1e3, "K": 1e3, "m": 1e6, "M": 1e6, "g": 1e9, "G": 1e9}


def freq_str_to_mhz(spec: str) -> float:
    spec = spec.strip()
    if spec and spec[-1] in _SUFFIX_TO_HZ:
        hz = float(spec[:-1]) * _SUFFIX_TO_HZ[spec[-1]]
    else:
        hz = float(spec)
    return hz / 1e6


def _make_channel(dev: DeviceConfig, waterfall_rows: int) -> Channel:
    freq_start_mhz = freq_str_to_mhz(dev.freq_start)
    freq_stop_mhz = freq_str_to_mhz(dev.freq_stop)
    return Channel(
        label=f"RTL: Dev{dev.id} {freq_start_mhz:.0f}-{freq_stop_mhz:.0f} MHz",
        freq_start_mhz=freq_start_mhz,
        freq_stop_mhz=freq_stop_mhz,
        state=SweepState(history=deque(maxlen=waterfall_rows)),
    )


class RTLBackend(SDRBackend):
    """One rtl_power subprocess per configured device."""

    def __init__(
        self,
        settings: RTLSettings,
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
        self.channels = [_make_channel(dev, waterfall_rows) for dev in settings.devices]
        self._procs: list[subprocess.Popen] = []
        self._procs_lock = threading.Lock()
        self._publisher = publisher
        self._detectors = detectors
        self._partitions = partitions
        self._canonical_freqs = canonical_freqs
        self._csv_writers = csv_writers
        self._keyframes = keyframes

    def start(self) -> None:
        detectors = self._detectors or [None] * len(self.channels)
        partitions = self._partitions or [-1] * len(self.channels)
        canonical_freqs = self._canonical_freqs or [None] * len(self.channels)
        csv_writers = self._csv_writers or [None] * len(self.channels)
        keyframes = self._keyframes or [None] * len(self.channels)
        for dev, channel, detector, partition, freqs, csv_writer, keyframe in zip(
            self.settings.devices, self.channels, detectors, partitions, canonical_freqs, csv_writers, keyframes
        ):
            threading.Thread(
                target=self._run_device,
                args=(dev, channel, detector, partition, freqs, csv_writer, keyframe),
                daemon=True,
            ).start()

    def _run_device(
        self,
        dev: DeviceConfig,
        channel: Channel,
        detector: NoiseFloorDetector | None,
        partition: int,
        canonical_freqs: np.ndarray | None,
        csv_writer: CsvSweepWriter | None,
        keyframe: KeyframeScheduler | None = None,
    ) -> None:
        cmd = [
            "rtl_power",
            "-d", str(dev.id),
            "-f", f"{dev.freq_start}:{dev.freq_stop}:{dev.bin_width}",
        ]
        if self.settings.gain is not None:
            cmd += ["-g", str(self.settings.gain)]
        cmd += [
            "-i", str(self.settings.interval),
            "-c", f"{dev.edge_trim * 100:.0f}%",
            "-",
        ]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except OSError as exc:
            self.report_error(f"[{channel.label}] failed to launch rtl_power: {exc}")
            return
        with self._procs_lock:
            self._procs.append(proc)
        for raw in proc.stdout:
            line = parse_sweep_line(raw)
            if line is None:
                continue
            # rtl_power's own -c crop already discards unreliable edge bins and widens
            # each hop's capture so adjacent hops tile without gaps — no further trimming needed.
            freqs = [line.hz_low + line.hz_step * i for i in range(len(line.powers))]
            with channel.state.lock:
                for f, p in zip(freqs, line.powers):
                    channel.state.sweep[f] = p

            if csv_writer is not None:
                csv_writer.write_hop(freqs, line.powers)

            if detector is not None:
                freqs_arr = np.asarray(freqs)
                powers_arr = np.asarray(line.powers)
                # Snap this hop's real bin centers onto the channel's fixed grid before
                # updating detector state — rtl_power's actual hz_step and the grid
                # computed from nominal config can differ by sub-bin amounts.
                indices = nearest_grid_index(canonical_freqs, freqs_arr)
                flagged_mask = detector.flag_hop(indices, powers_arr)
                flagged = list(zip(freqs_arr[flagged_mask].tolist(), powers_arr[flagged_mask].tolist()))
                self._publisher.publish(channel.label, flagged, partition=partition)
                if keyframe is not None and keyframe.due():
                    all_bins = list(zip(freqs_arr.tolist(), powers_arr.tolist()))
                    self._publisher.publish(channel.label, all_bins, partition=partition)
                    keyframe.mark_sent()

        proc.wait()
        if proc.returncode > 0:
            stderr_output = proc.stderr.read().strip() if proc.stderr else ""
            self.report_error(
                f"[{channel.label}] rtl_power exited with code {proc.returncode}: {stderr_output}"
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
