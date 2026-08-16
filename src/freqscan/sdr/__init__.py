import sys
import time

import numpy as np

from freqscan.config import HackRFSettings, KafkaSettings, RTLSettings, Settings
from freqscan.sdr.base import Channel, CompositeBackend, SDRBackend
from freqscan.sdr.hackrf import HackRFBackend
from freqscan.sdr.rtl import RTLBackend, freq_str_to_mhz
from freqscan.streaming.detector import NoiseFloorDetector
from freqscan.streaming.kafka_publisher import KafkaSignalPublisher, build_producer

# Each channel's grid: (freq_start_hz, freq_stop_hz, bin_width_hz, n_bins) — computed
# once per channel and shared by detector sizing, metadata publishing, and per-hop bin
# snapping, so all three always agree on the same channel layout.
ChannelGrid = tuple[float, float, int, int]


def _rtl_channel_grids(rtl: RTLSettings) -> list[ChannelGrid]:
    grids = []
    for dev in rtl.devices:
        freq_start_hz = freq_str_to_mhz(dev.freq_start) * 1e6
        freq_stop_hz = freq_str_to_mhz(dev.freq_stop) * 1e6
        n_bins = round((freq_stop_hz - freq_start_hz) / dev.bin_width)
        grids.append((freq_start_hz, freq_stop_hz, dev.bin_width, n_bins))
    return grids


def _hackrf_channel_grids(hackrf: HackRFSettings) -> list[ChannelGrid]:
    grids = []
    for r in hackrf.ranges:
        freq_start_hz = r.freq_start * 1e6
        freq_stop_hz = r.freq_stop * 1e6
        n_bins = round((freq_stop_hz - freq_start_hz) / hackrf.bin_width)
        grids.append((freq_start_hz, freq_stop_hz, hackrf.bin_width, n_bins))
    return grids


def _canonical_freqs(grids: list[ChannelGrid]) -> list[np.ndarray]:
    return [
        np.linspace(freq_start_hz, freq_stop_hz, n_bins, endpoint=False)
        for freq_start_hz, freq_stop_hz, _bin_width_hz, n_bins in grids
    ]


def _build_rtl_detectors(rtl: RTLSettings, kafka: KafkaSettings, grids: list[ChannelGrid]) -> list[NoiseFloorDetector]:
    return [
        NoiseFloorDetector(
            n_bins=grids[i][3],
            window=kafka.baseline_window,
            margin_db=kafka.rtl_margins_db.get(dev.id, kafka.signal_margin_db),
            spatial_margin_db=kafka.rtl_spatial_margins_db.get(dev.id, kafka.spatial_margin_db),
            prominence_margin_db=kafka.rtl_prominence_margins_db.get(dev.id, kafka.prominence_margin_db),
        )
        for i, dev in enumerate(rtl.devices)
    ]


def _build_hackrf_detectors(
    hackrf: HackRFSettings, kafka: KafkaSettings, grids: list[ChannelGrid]
) -> list[NoiseFloorDetector]:
    return [
        NoiseFloorDetector(
            n_bins=grids[idx][3],
            window=kafka.baseline_window,
            margin_db=kafka.hackrf_margins_db.get(idx, kafka.signal_margin_db),
            spatial_margin_db=kafka.hackrf_spatial_margins_db.get(idx, kafka.spatial_margin_db),
            prominence_margin_db=kafka.hackrf_prominence_margins_db.get(idx, kafka.prominence_margin_db),
        )
        for idx in range(len(hackrf.ranges))
    ]


def _publish_metadata(
    publisher: KafkaSignalPublisher,
    grids: list[ChannelGrid],
    channels: list[Channel],
    partitions: list[int],
    run_epoch: float,
) -> None:
    for (freq_start_hz, freq_stop_hz, bin_width_hz, n_bins), channel, partition in zip(grids, channels, partitions):
        publisher.publish_metadata(
            channel.label, partition, freq_start_hz, freq_stop_hz, bin_width_hz, n_bins, run_epoch
        )


def build_backend(settings: Settings) -> SDRBackend:
    kafka = settings.kafka
    streaming = kafka is not None and kafka.enabled
    publisher = (
        KafkaSignalPublisher(build_producer(kafka), kafka.topic, kafka.metadata_topic) if streaming else None
    )

    # Same value on every metadata message published this run, regardless of channel —
    # lets a viewer tell this run's fresh metadata apart from an older run's stale
    # leftovers on a partition the current run doesn't touch (see kafka_consumer.py).
    run_epoch = time.time()

    backends: list[SDRBackend] = []
    next_partition = 0
    if settings.rtl is not None:
        grids = _rtl_channel_grids(settings.rtl)
        detectors = _build_rtl_detectors(settings.rtl, kafka, grids) if streaming else None
        canonical_freqs = _canonical_freqs(grids) if streaming else None
        n = len(settings.rtl.devices)
        partitions = list(range(next_partition, next_partition + n)) if streaming else None
        next_partition += n
        rtl_backend = RTLBackend(
            settings.rtl, settings.waterfall_rows, publisher, detectors, partitions, canonical_freqs
        )
        if streaming:
            _publish_metadata(publisher, grids, rtl_backend.channels, partitions, run_epoch)
        backends.append(rtl_backend)
    if settings.hackrf is not None:
        grids = _hackrf_channel_grids(settings.hackrf)
        detectors = _build_hackrf_detectors(settings.hackrf, kafka, grids) if streaming else None
        canonical_freqs = _canonical_freqs(grids) if streaming else None
        n = len(settings.hackrf.ranges)
        partitions = list(range(next_partition, next_partition + n)) if streaming else None
        next_partition += n
        hackrf_backend = HackRFBackend(
            settings.hackrf, settings.waterfall_rows, publisher, detectors, partitions, canonical_freqs
        )
        if streaming:
            _publish_metadata(publisher, grids, hackrf_backend.channels, partitions, run_epoch)
        backends.append(hackrf_backend)
    backend = backends[0] if len(backends) == 1 else CompositeBackend(backends)
    backend.publisher = publisher
    if streaming:
        # Generous timeout: this only runs once at startup, and a brand-new metadata
        # topic's delivery can lag past a short flush while the producer's client-side
        # topic metadata cache catches up — better to wait here than silently proceed
        # with metadata that hasn't actually reached the broker yet.
        remaining = publisher.flush(timeout=15.0)
        if remaining:
            print(
                f"freqscan: warning: {remaining} metadata message(s) not confirmed delivered "
                f"to '{kafka.metadata_topic}' after 15s",
                file=sys.stderr,
            )
    return backend
