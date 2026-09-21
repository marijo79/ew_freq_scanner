import json
import subprocess
import sys
import threading
from collections import deque
from pathlib import Path

import numpy as np

from freqscan.config import PlutoChannelConfig, PlutoStareSettings
from freqscan.csv_writer import CsvSweepWriter
from freqscan.parsing import parse_sweep_line
from freqscan.sdr.base import Channel, SDRBackend, SweepState
from freqscan.streaming.detector import KeyframeScheduler, NoiseFloorDetector, nearest_grid_index
from freqscan.streaming.kafka_publisher import KafkaSignalPublisher

_STARE_SCRIPT = Path(__file__).resolve().parent.parent.parent.parent / "scripts" / "pluto_stare.py"


def _make_channel(settings: PlutoStareSettings, ch: PlutoChannelConfig, waterfall_rows: int) -> Channel:
    half_span_mhz = settings.sample_rate / 2 / 1e6
    freq_mhz = settings.frequency
    return Channel(
        label=f"Pluto RX{ch.channel} (stare): {freq_mhz:.3f} MHz",
        freq_start_mhz=freq_mhz - half_span_mhz,
        freq_stop_mhz=freq_mhz + half_span_mhz,
        state=SweepState(history=deque(maxlen=waterfall_rows)),
    )


class PlutoStareBackend(SDRBackend):
    """One scripts/pluto_stare.py subprocess for *all* configured RX channels together,
    continuously monitoring the same fixed frequency -- the "stare" counterpart to
    PlutoBackend's "sweep" mode. RX1/RX2 physically share the same
    RX_LO/sample_rate/rf_bandwidth (see PlutoChannelConfig), so one process sets those
    once and captures every channel's I/Q from one interleaved `iio_readdev` buffer;
    only gain is genuinely per-channel (see scripts/pluto_stare.py's own docstring).

    **Deliberately one shared subprocess/connection, not one per channel** -- an earlier
    version spawned a separate pluto_stare.py instance (and thus a separate
    `iio_readdev` connection) per configured channel; that worked over the `ip:` network
    backend (its IIOD daemon on the Pluto itself multiplexes concurrent clients) but
    already measurably throttled each channel (observed ~18-20 lines/sec for a lone
    channel dropping to as low as ~2.5 lines/sec for the second one running
    concurrently, both at 20MHz sample_rate). Found live 2026-09-04 chasing that further
    that libiio's `usb:` backend (direct USB bulk transfer, meaningfully faster than
    `ip:` -- measured ~22MB/s vs ~16.5MB/s sustained for the same 2-channel capture) is
    flatly incompatible with that one-process-per-channel design: `usb:` claims the USB
    interface *exclusively*, so a second process's connection attempt fails outright
    ("Unable to claim interface: Device or resource busy") the moment the first one is
    already streaming. One shared connection sidesteps that entirely and is what makes
    `usb:` usable here at all with 2+ channels configured.

    **The real per-line throughput cost is Python-side FFT + formatting CPU time,
    which scales with `n_bins` (= `sample_rate / bin_width`), not raw USB bandwidth** --
    a follow-up no-FFT `dd` probe of raw `iio_readdev` throughput measured only ~22MB/s
    sustained over `usb:` for a 2-channel capture, which looked like it meant
    `sample_rate` had to be cut drastically to fit under it, but measuring this
    backend's actual reader loop directly shows real throughput at any practical
    `bin_width`/`sample_rate` pairing sits nowhere near that ceiling: e.g.
    `sample_rate=50_000_000`/`bin_width=25_000` (`n_bins` ~2000) measures ~214 real
    lines/sec for both RX channels together -- over 10x the display's ~20Hz need,
    using only a few MB/s. Raising `sample_rate` while raising `bin_width`
    proportionally (keeping `n_bins` roughly fixed) keeps real throughput about the
    same regardless of how high `sample_rate` goes; `bin_width` is the knob that
    actually controls throughput.

    `PLUTO_STARE__URI` stays a plain config value either way (`ip:...` or `usb:...`) --
    nothing here hardcodes which transport is used."""

    def __init__(
        self,
        settings: PlutoStareSettings,
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
        self.channels = [_make_channel(settings, ch, waterfall_rows) for ch in settings.channels]
        self._proc: subprocess.Popen | None = None
        self._proc_lock = threading.Lock()
        self._publisher = publisher
        self._detectors = detectors
        self._partitions = partitions
        self._canonical_freqs = canonical_freqs
        self._csv_writers = csv_writers
        self._keyframes = keyframes

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        detectors = self._detectors or [None] * len(self.channels)
        partitions = self._partitions or [-1] * len(self.channels)
        canonical_freqs = self._canonical_freqs or [None] * len(self.channels)
        csv_writers = self._csv_writers or [None] * len(self.channels)
        keyframes = self._keyframes or [None] * len(self.channels)
        # Index everything by RX channel number so the single reader loop below can
        # route each tagged line to the right Channel/detector/csv_writer/partition.
        by_channel_num = {
            ch.channel: (channel, detector, partition, freqs, csv_writer, keyframe)
            for ch, channel, detector, partition, freqs, csv_writer, keyframe in zip(
                self.settings.channels, self.channels, detectors, partitions, canonical_freqs, csv_writers, keyframes
            )
        }
        channel_configs = [
            {
                "channel": ch.channel,
                "gain_control_mode": ch.gain_control_mode,
                "hardwaregain": ch.hardwaregain,
            }
            for ch in self.settings.channels
        ]
        cmd = [
            sys.executable, str(_STARE_SCRIPT),
            "--uri", self.settings.uri,
            "--frequency", str(self.settings.frequency * 1e6),
            "--bin-width", str(self.settings.bin_width),
            "--sample-rate", str(self.settings.sample_rate),
            "--edge-trim", str(self.settings.edge_trim),
            "--channel-configs", json.dumps(channel_configs),
        ]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except OSError as exc:
            self.report_error(f"[Pluto stare] failed to launch pluto_stare.py: {exc}")
            return
        with self._proc_lock:
            self._proc = proc
        for raw in proc.stdout:
            tag, _, rest = raw.partition(",")
            try:
                channel_num = int(tag.strip())
            except ValueError:
                continue
            routed = by_channel_num.get(channel_num)
            if routed is None:
                continue
            channel, detector, partition, canonical_freqs_arr, csv_writer, keyframe = routed

            line = parse_sweep_line(rest)
            if line is None:
                continue
            freqs = line.hz_low + line.hz_step * np.arange(len(line.powers))
            powers = np.asarray(line.powers)
            with channel.state.lock:
                for f, p in zip(freqs.tolist(), powers.tolist()):
                    channel.state.sweep[f] = p

            if csv_writer is not None:
                csv_writer.write_hop(freqs, powers)

            if detector is not None:
                indices = nearest_grid_index(canonical_freqs_arr, freqs)
                flagged_mask = detector.flag_hop(indices, powers)
                flagged = list(zip(indices[flagged_mask].tolist(), powers[flagged_mask].tolist()))
                self._publisher.publish(channel.label, flagged, partition=partition)
                if keyframe is not None and keyframe.due():
                    all_bins = list(zip(indices.tolist(), powers.tolist()))
                    self._publisher.publish(channel.label, all_bins, partition=partition)
                    keyframe.mark_sent()

        proc.wait()
        if proc.returncode > 0:
            stderr_output = proc.stderr.read().strip() if proc.stderr else ""
            self.report_error(f"[Pluto stare] pluto_stare.py exited with code {proc.returncode}: {stderr_output}")

    def stop(self) -> None:
        with self._proc_lock:
            if self._proc is not None:
                self._proc.terminate()
        if self._publisher is not None:
            self._publisher.flush()
        for writer in self._csv_writers or []:
            if writer is not None:
                writer.close()
