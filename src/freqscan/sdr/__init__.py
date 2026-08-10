import sys

from freqscan.config import HackRFSettings, KafkaSettings, RTLSettings, Settings
from freqscan.sdr.base import Channel, CompositeBackend, SDRBackend
from freqscan.sdr.hackrf import HackRFBackend
from freqscan.sdr.rtl import RTLBackend, freq_str_to_mhz
from freqscan.streaming.detector import NoiseFloorDetector
from freqscan.streaming.kafka_publisher import KafkaSignalPublisher, build_producer


def _build_rtl_detectors(rtl: RTLSettings, kafka: KafkaSettings) -> list[NoiseFloorDetector]:
    return [
        NoiseFloorDetector(
            window=kafka.baseline_window,
            margin_db=kafka.rtl_margins_db.get(dev.id, kafka.signal_margin_db),
            spatial_margin_db=kafka.rtl_spatial_margins_db.get(dev.id, kafka.spatial_margin_db),
            prominence_margin_db=kafka.rtl_prominence_margins_db.get(dev.id, kafka.prominence_margin_db),
        )
        for dev in rtl.devices
    ]


def _build_hackrf_detectors(hackrf: HackRFSettings, kafka: KafkaSettings) -> list[NoiseFloorDetector]:
    return [
        NoiseFloorDetector(
            window=kafka.baseline_window,
            margin_db=kafka.hackrf_margins_db.get(idx, kafka.signal_margin_db),
            spatial_margin_db=kafka.hackrf_spatial_margins_db.get(idx, kafka.spatial_margin_db),
            prominence_margin_db=kafka.hackrf_prominence_margins_db.get(idx, kafka.prominence_margin_db),
        )
        for idx in range(len(hackrf.ranges))
    ]


def _publish_rtl_metadata(
    publisher: KafkaSignalPublisher, rtl: RTLSettings, channels: list[Channel], partitions: list[int]
) -> None:
    for dev, channel, partition in zip(rtl.devices, channels, partitions):
        freq_start_hz = freq_str_to_mhz(dev.freq_start) * 1e6
        freq_stop_hz = freq_str_to_mhz(dev.freq_stop) * 1e6
        bin_width_hz = dev.bin_width
        n_bins = round((freq_stop_hz - freq_start_hz) / bin_width_hz)
        publisher.publish_metadata(channel.label, partition, freq_start_hz, freq_stop_hz, bin_width_hz, n_bins)


def _publish_hackrf_metadata(
    publisher: KafkaSignalPublisher, hackrf: HackRFSettings, channels: list[Channel], partitions: list[int]
) -> None:
    for r, channel, partition in zip(hackrf.ranges, channels, partitions):
        freq_start_hz = r.freq_start * 1e6
        freq_stop_hz = r.freq_stop * 1e6
        n_bins = round((freq_stop_hz - freq_start_hz) / hackrf.bin_width)
        publisher.publish_metadata(channel.label, partition, freq_start_hz, freq_stop_hz, hackrf.bin_width, n_bins)


def build_backend(settings: Settings) -> SDRBackend:
    kafka = settings.kafka
    streaming = kafka is not None and kafka.enabled
    publisher = (
        KafkaSignalPublisher(build_producer(kafka), kafka.topic, kafka.metadata_topic) if streaming else None
    )

    backends: list[SDRBackend] = []
    next_partition = 0
    if settings.rtl is not None:
        detectors = _build_rtl_detectors(settings.rtl, kafka) if streaming else None
        n = len(settings.rtl.devices)
        partitions = list(range(next_partition, next_partition + n)) if streaming else None
        next_partition += n
        rtl_backend = RTLBackend(settings.rtl, settings.waterfall_rows, publisher, detectors, partitions)
        if streaming:
            _publish_rtl_metadata(publisher, settings.rtl, rtl_backend.channels, partitions)
        backends.append(rtl_backend)
    if settings.hackrf is not None:
        detectors = _build_hackrf_detectors(settings.hackrf, kafka) if streaming else None
        n = len(settings.hackrf.ranges)
        partitions = list(range(next_partition, next_partition + n)) if streaming else None
        next_partition += n
        hackrf_backend = HackRFBackend(settings.hackrf, settings.waterfall_rows, publisher, detectors, partitions)
        if streaming:
            _publish_hackrf_metadata(publisher, settings.hackrf, hackrf_backend.channels, partitions)
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
