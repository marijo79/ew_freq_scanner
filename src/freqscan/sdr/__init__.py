import os
import sys
import time
from datetime import datetime

import numpy as np

from freqscan.config import KafkaSettings, PlutoStareSettings, RangeConfig, RTLSettings, Settings
from freqscan.csv_writer import CsvSweepWriter, make_csv_path
from freqscan.sdr.base import Channel, CompositeBackend, SDRBackend
from freqscan.sdr.hackrf import HackRFBackend
from freqscan.sdr.pluto import PlutoBackend
from freqscan.sdr.pluto_stare import PlutoStareBackend
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


def _range_channel_grids(ranges: list[RangeConfig], bin_width: int) -> list[ChannelGrid]:
    """Shared by HackRF and Pluto: both are single-device backends with one shared
    bin_width across all their configured ranges (unlike RTL's per-device bin_width)."""
    grids = []
    for r in ranges:
        freq_start_hz = r.freq_start * 1e6
        freq_stop_hz = r.freq_stop * 1e6
        n_bins = round((freq_stop_hz - freq_start_hz) / bin_width)
        grids.append((freq_start_hz, freq_stop_hz, bin_width, n_bins))
    return grids


def _pluto_stare_channel_grids(pluto_stare: PlutoStareSettings) -> list[ChannelGrid]:
    """One grid per configured RX channel (see PlutoChannelConfig) -- all identical since
    RX1/RX2 physically share the same frequency/sample_rate/bin_width, just repeated once
    per capture process/plot channel."""
    freq_center_hz = pluto_stare.frequency * 1e6
    half_span_hz = pluto_stare.sample_rate / 2
    n_bins = round(pluto_stare.sample_rate / pluto_stare.bin_width)
    grid = (freq_center_hz - half_span_hz, freq_center_hz + half_span_hz, pluto_stare.bin_width, n_bins)
    return [grid] * len(pluto_stare.channels)


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


def _build_range_detectors(
    count: int,
    kafka: KafkaSettings,
    grids: list[ChannelGrid],
    margins_db: dict[int, float],
    spatial_margins_db: dict[int, float],
    prominence_margins_db: dict[int, float],
) -> list[NoiseFloorDetector]:
    """Shared by HackRF, Pluto (sweep), and Pluto (stare): all key their per-channel
    margin overrides by 0-based index into their own ranges/channels list (unlike RTL,
    keyed by DeviceConfig.id). `count` is `len(ranges)` for HackRF/Pluto-sweep or
    `len(channels)` for Pluto-stare -- only the count matters here, not the list's own
    element type."""
    return [
        NoiseFloorDetector(
            n_bins=grids[idx][3],
            window=kafka.baseline_window,
            margin_db=margins_db.get(idx, kafka.signal_margin_db),
            spatial_margin_db=spatial_margins_db.get(idx, kafka.spatial_margin_db),
            prominence_margin_db=prominence_margins_db.get(idx, kafka.prominence_margin_db),
        )
        for idx in range(count)
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


def build_backend(settings: Settings, csv_dir: str | None = None) -> SDRBackend:
    kafka = settings.kafka
    streaming = kafka is not None and kafka.enabled
    publisher = (
        KafkaSignalPublisher(build_producer(kafka), kafka.topic, kafka.metadata_topic) if streaming else None
    )

    # One shared timestamp for every CSV filename this run, so a run's files sort/group
    # together rather than drifting apart by the few seconds it takes to start every channel.
    csv_when = datetime.now() if csv_dir is not None else None
    if csv_dir is not None:
        os.makedirs(csv_dir, exist_ok=True)

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
        csv_writers = (
            [
                CsvSweepWriter(make_csv_path(csv_dir, f"RTL-Dev{dev.id}", g[0] / 1e6, g[1] / 1e6, csv_when))
                for dev, g in zip(settings.rtl.devices, grids)
            ]
            if csv_dir is not None
            else None
        )
        rtl_backend = RTLBackend(
            settings.rtl, settings.waterfall_rows, publisher, detectors, partitions, canonical_freqs, csv_writers
        )
        if streaming:
            _publish_metadata(publisher, grids, rtl_backend.channels, partitions, run_epoch)
        backends.append(rtl_backend)
    if settings.hackrf is not None:
        grids = _range_channel_grids(settings.hackrf.ranges, settings.hackrf.bin_width)
        detectors = (
            _build_range_detectors(
                len(settings.hackrf.ranges),
                kafka,
                grids,
                kafka.hackrf_margins_db,
                kafka.hackrf_spatial_margins_db,
                kafka.hackrf_prominence_margins_db,
            )
            if streaming
            else None
        )
        canonical_freqs = _canonical_freqs(grids) if streaming else None
        n = len(settings.hackrf.ranges)
        partitions = list(range(next_partition, next_partition + n)) if streaming else None
        next_partition += n
        csv_writers = (
            [CsvSweepWriter(make_csv_path(csv_dir, "HackRF", g[0] / 1e6, g[1] / 1e6, csv_when)) for g in grids]
            if csv_dir is not None
            else None
        )
        hackrf_backend = HackRFBackend(
            settings.hackrf, settings.waterfall_rows, publisher, detectors, partitions, canonical_freqs, csv_writers
        )
        if streaming:
            _publish_metadata(publisher, grids, hackrf_backend.channels, partitions, run_epoch)
        backends.append(hackrf_backend)
    if settings.pluto is not None:
        # RX channel is the outer grouping (matches PlutoBackend._make_channels): for N
        # ranges and M RX channels, grids/detectors/etc. repeat the same N-range layout
        # once per channel. Margin overrides stay keyed by range index (0..N-1), same
        # meaning for every RX channel, rather than a combined channel*range index --
        # simpler to configure, and each channel still gets its own fresh detector
        # instance (independent mutable state/history), not a shared one.
        n_ranges = len(settings.pluto.ranges)
        per_channel_grids = _range_channel_grids(settings.pluto.ranges, settings.pluto.bin_width)
        grids = per_channel_grids * len(settings.pluto.channels)
        detectors = (
            [
                d
                for _ in settings.pluto.channels
                for d in _build_range_detectors(
                    n_ranges,
                    kafka,
                    per_channel_grids,
                    kafka.pluto_margins_db,
                    kafka.pluto_spatial_margins_db,
                    kafka.pluto_prominence_margins_db,
                )
            ]
            if streaming
            else None
        )
        canonical_freqs = _canonical_freqs(grids) if streaming else None
        n = len(grids)
        partitions = list(range(next_partition, next_partition + n)) if streaming else None
        next_partition += n
        csv_writers = (
            [
                CsvSweepWriter(make_csv_path(csv_dir, f"Pluto-RX{ch.channel}", g[0] / 1e6, g[1] / 1e6, csv_when))
                for ch in settings.pluto.channels
                for g in per_channel_grids
            ]
            if csv_dir is not None
            else None
        )
        pluto_backend = PlutoBackend(
            settings.pluto, settings.waterfall_rows, publisher, detectors, partitions, canonical_freqs, csv_writers
        )
        if streaming:
            _publish_metadata(publisher, grids, pluto_backend.channels, partitions, run_epoch)
        backends.append(pluto_backend)
    if settings.pluto_stare is not None:
        grids = _pluto_stare_channel_grids(settings.pluto_stare)
        detectors = (
            _build_range_detectors(
                len(settings.pluto_stare.channels),
                kafka,
                grids,
                kafka.pluto_margins_db,
                kafka.pluto_spatial_margins_db,
                kafka.pluto_prominence_margins_db,
            )
            if streaming
            else None
        )
        canonical_freqs = _canonical_freqs(grids) if streaming else None
        n = len(settings.pluto_stare.channels)
        partitions = list(range(next_partition, next_partition + n)) if streaming else None
        next_partition += n
        csv_writers = (
            [
                CsvSweepWriter(make_csv_path(csv_dir, f"Pluto-stare-RX{ch.channel}", g[0] / 1e6, g[1] / 1e6, csv_when))
                for ch, g in zip(settings.pluto_stare.channels, grids)
            ]
            if csv_dir is not None
            else None
        )
        pluto_stare_backend = PlutoStareBackend(
            settings.pluto_stare,
            settings.waterfall_rows,
            publisher,
            detectors,
            partitions,
            canonical_freqs,
            csv_writers,
        )
        if streaming:
            _publish_metadata(publisher, grids, pluto_stare_backend.channels, partitions, run_epoch)
        backends.append(pluto_stare_backend)
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
