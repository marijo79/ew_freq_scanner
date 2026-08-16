import json
import threading
import time
from collections import deque

import numpy as np
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, KafkaError, KafkaException, TopicPartition

from freqscan.config import KafkaViewerSettings
from freqscan.sdr.base import Channel, SDRBackend, SweepState
from freqscan.streaming.detector import nearest_grid_index
from freqscan.streaming.kafka_publisher import oauth_cb

METADATA_TIMEOUT = 10.0  # seconds to wait for every channel's metadata before giving up


def apply_message(channel: Channel, payload: dict, canonical_freqs: np.ndarray) -> None:
    """Merge one decoded message's bins into channel, snapping each bin's freq_hz to the
    nearest canonical grid frequency (from metadata). Snapping — instead of using freq_hz
    as the dict key directly — matters because the producer's actual bin centers (as
    parsed from rtl_power/hackrf_sweep output) and a grid computed from nominal config
    values can differ by sub-bin amounts; without snapping, that mismatch would silently
    grow state.sweep with extra near-duplicate keys, reintroducing the exact "known bin
    count keeps changing" problem the metadata grid was built to avoid."""
    with channel.state.lock:
        for b in payload["bins"]:
            idx = nearest_grid_index(canonical_freqs, b["freq_hz"])
            channel.state.sweep[canonical_freqs[idx]] = b["power_dbm"]


def build_consumer(settings: KafkaViewerSettings) -> Consumer:
    config = {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": settings.security_protocol,
        "group.id": "freqscan-viewer",  # unused for offset commits; assign() bypasses group coordination
        "enable.auto.commit": False,
    }
    if "SASL" in settings.security_protocol:
        config["sasl.mechanisms"] = settings.sasl_mechanism
        if settings.sasl_mechanism == "OAUTHBEARER":
            config["oauth_cb"] = oauth_cb(settings.region)
    return Consumer(config)


class KafkaConsumerBackend(SDRBackend):
    """Reads freqscan's own published signal messages back out of Kafka to re-populate
    per-channel SweepState, so the same spectrum+waterfall plot can be drawn from a
    topic instead of live hardware.

    Channel layout comes from the producer's metadata topic (one message per channel,
    published once at startup on the same partition as that channel's signal data, see
    build_backend()) — not discovered from the signal messages themselves. Each channel's
    sweep state is pre-populated with every bin of its declared grid (as NaN) before any
    real data arrives, so the known bin count is fixed from frame one. The previous
    "discover channels lazily from the first message per partition" approach grew the
    known-bin set over time as new distinct frequencies got flagged for the first time,
    which visibly showed up as the whole waterfall reflowing/shifting for a while after
    startup — every column's geometric position in plotting.py's imshow depends on the
    *total* column count, so adding columns anywhere shifts everything, not just what's new.
    """

    def __init__(self, settings: KafkaViewerSettings, waterfall_rows: int):
        super().__init__()
        self._settings = settings
        self._waterfall_rows = waterfall_rows
        self.channels: list[Channel] = []
        self._canonical_freqs: list[np.ndarray] = []
        self._consumer = build_consumer(settings)
        self._stop_event = threading.Event()

        try:
            n_partitions = self._partition_count(settings.topic)
        except KafkaException as exc:
            self.report_error(f"failed to look up Kafka topic '{settings.topic}': {exc}")
            return

        print("freqscan-viewer: waiting for channel metadata...", flush=True)
        metadata = self._read_metadata(n_partitions)
        missing = [i for i in range(n_partitions) if i not in metadata]
        if missing:
            self.report_error(
                f"no metadata received for partition(s) {missing} of "
                f"'{settings.metadata_topic}' within {METADATA_TIMEOUT}s — "
                "is the producer running with --kafka_publisher?"
            )
            return

        for i in range(n_partitions):
            m = metadata[i]
            freqs = np.linspace(m["freq_start_hz"], m["freq_stop_hz"], m["n_bins"], endpoint=False)
            channel = Channel(
                label=m["channel"],
                freq_start_mhz=m["freq_start_hz"] / 1e6,
                freq_stop_mhz=m["freq_stop_hz"] / 1e6,
                state=SweepState(history=deque(maxlen=waterfall_rows)),
            )
            with channel.state.lock:
                for f in freqs:
                    channel.state.sweep[float(f)] = float("nan")
            self.channels.append(channel)
            self._canonical_freqs.append(freqs)
        print(f"freqscan-viewer: got metadata for {n_partitions} channel(s)", flush=True)

    def _partition_count(self, topic: str) -> int:
        metadata = self._consumer.list_topics(topic, timeout=10)
        topic_metadata = metadata.topics[topic]
        if topic_metadata.error is not None:
            raise KafkaException(topic_metadata.error)
        return len(topic_metadata.partitions)

    def _read_metadata(self, n_partitions: int) -> dict[int, dict]:
        partitions = [
            TopicPartition(self._settings.metadata_topic, i, offset=OFFSET_BEGINNING) for i in range(n_partitions)
        ]
        self._consumer.assign(partitions)

        collected: dict[int, dict] = {}
        eof = set()
        deadline = time.monotonic() + METADATA_TIMEOUT
        while len(eof) < n_partitions and time.monotonic() < deadline:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof.add(msg.partition())
                continue
            collected[msg.partition()] = json.loads(msg.value())
        return collected

    def start(self) -> None:
        if self._failure.is_set():
            return
        partitions = [
            TopicPartition(self._settings.topic, i, offset=OFFSET_END)  # only live messages from now on
            for i in range(len(self.channels))
        ]
        self._consumer.assign(partitions)
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    self.report_error(f"kafka consumer error: {msg.error()}")
                continue

            partition = msg.partition()
            payload = json.loads(msg.value())
            apply_message(self.channels[partition], payload, self._canonical_freqs[partition])

    def stop(self) -> None:
        self._stop_event.set()
        self._consumer.close()
