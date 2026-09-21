import json
import threading
import time

from aws_msk_iam_sasl_signer.MSKAuthTokenProvider import generate_auth_token
from confluent_kafka import Producer

from freqscan.config import KafkaSettings


def oauth_cb(region: str):
    def callback(_config_str: str) -> tuple[str, float]:
        token, expiry_ms = generate_auth_token(region)
        return token, expiry_ms / 1000

    return callback


def build_client_config(settings) -> dict:
    """Shared bootstrap/security config for both the producer (this module) and the
    read-only viewer's consumer (kafka_consumer.py) -- duck-typed against KafkaSettings/
    KafkaViewerSettings, which carry the same connection fields independently (see
    config.py's own note on why they're separate classes).

    OAUTHBEARER (real MSK) always signs its own token via oauth_cb and ignores
    sasl_username/sasl_password entirely; SCRAM-SHA-256/512 and PLAIN use those two
    fields directly instead. ssl_ca_location is for pinning a self-signed broker
    cert (e.g. a self-hosted SASL_SSL broker) -- unset for real MSK, whose cert
    already chains to a public CA."""
    config = {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": settings.security_protocol,
    }
    if "SASL" in settings.security_protocol:
        config["sasl.mechanisms"] = settings.sasl_mechanism
        if settings.sasl_mechanism == "OAUTHBEARER":
            config["oauth_cb"] = oauth_cb(settings.region)
        elif settings.sasl_username is not None:
            config["sasl.username"] = settings.sasl_username
            config["sasl.password"] = settings.sasl_password
    if settings.ssl_ca_location:
        config["ssl.ca.location"] = settings.ssl_ca_location
    return config


def build_producer(settings: KafkaSettings) -> Producer:
    config = {**build_client_config(settings), "compression.type": settings.compression_type}
    return Producer(config)


def build_payload(channel_label: str, bins: list[tuple[int, float]], timestamp: float) -> dict:
    """bins: (grid_index, power_dbm) pairs -- grid_index is this bin's absolute index
    into the channel's canonical frequency grid (see streaming.detector.nearest_grid_index()
    and the channel's own metadata: freq_start_hz + index*bin_width_hz recovers the real
    frequency), NOT freq_hz itself.

    Indices are delta-encoded on the wire (first value is the absolute index, every
    later one is the difference from the previous) rather than sent as-is. Every caller
    passes bins already frequency-sorted (each backend's own hop array is built
    ascending, and a boolean flag mask preserves that order), so deltas are never
    negative. This isn't just a smaller int than a float freq_hz -- measured live
    2026-09-21 against real captured messages: combined with gzip (see
    KafkaSettings.compression_type), delta-encoded indices get messages down to ~10% of
    the original freq_hz JSON, matching or slightly beating an equivalent protobuf
    encoding of the same data (gzip is very good at compressing the long runs of small
    deltas a keyframe's near-fully-contiguous bin range produces -- most deltas are
    exactly 1), so protobuf wasn't adopted for that marginal-or-negative difference."""
    indices = [idx for idx, _ in bins]
    powers = [power_dbm for _, power_dbm in bins]
    deltas = [indices[0], *(b - a for a, b in zip(indices, indices[1:]))] if indices else []
    return {
        "channel": channel_label,
        "timestamp": timestamp,
        "bin_index_deltas": deltas,
        "power_dbm": powers,
    }


def build_metadata_payload(
    channel_label: str,
    freq_start_hz: float,
    freq_stop_hz: float,
    bin_width_hz: float,
    n_bins: int,
    run_epoch: float,
) -> dict:
    """Describes a channel's true full grid — n_bins is the total bin count for the
    configured range, independent of how many (if any) are ever actually flagged.

    run_epoch is the same value (build_backend()'s own startup time) for every metadata
    message published in one run, regardless of channel — it lets a consumer tell this
    run's fresh metadata apart from an older run's stale leftovers still sitting on a
    partition the current run didn't touch (the metadata topic is append-only and never
    provisioned down, so an over-provisioned/previously-larger topic's unused partitions
    keep their last real message forever)."""
    return {
        "channel": channel_label,
        "freq_start_hz": freq_start_hz,
        "freq_stop_hz": freq_stop_hz,
        "bin_width_hz": bin_width_hz,
        "n_bins": n_bins,
        "run_epoch": run_epoch,
    }


class KafkaSignalPublisher:
    """Publishes one batched message per hop of flagged (grid_index, power_dbm) bins,
    plus one-time channel metadata (grid layout) on a separate topic, same partition."""

    def __init__(self, producer: Producer, topic: str, metadata_topic: str):
        self._producer = producer
        self._topic = topic
        self._metadata_topic = metadata_topic
        self._offsets: dict[int, int] = {}
        self._offsets_lock = threading.Lock()

    def publish(
        self,
        channel_label: str,
        bins: list[tuple[int, float]],
        partition: int = -1,
        on_delivery=None,
    ) -> None:
        if not bins:
            return
        payload = build_payload(channel_label, bins, time.time())

        def _record_offset(err, msg) -> None:
            if err is None:
                with self._offsets_lock:
                    self._offsets[msg.partition()] = msg.offset()
            if on_delivery is not None:
                on_delivery(err, msg)

        self._producer.produce(
            self._topic,
            key=channel_label.encode(),
            value=json.dumps(payload).encode(),
            partition=partition,
            on_delivery=_record_offset,
        )
        self._producer.poll(0)

    def publish_metadata(
        self,
        channel_label: str,
        partition: int,
        freq_start_hz: float,
        freq_stop_hz: float,
        bin_width_hz: float,
        n_bins: int,
        run_epoch: float,
    ) -> None:
        payload = build_metadata_payload(channel_label, freq_start_hz, freq_stop_hz, bin_width_hz, n_bins, run_epoch)
        self._producer.produce(
            self._metadata_topic,
            key=channel_label.encode(),
            value=json.dumps(payload).encode(),
            partition=partition,
        )
        self._producer.poll(0)

    def offsets(self) -> dict[int, int]:
        """Last delivered offset per partition, as of the most recent poll()."""
        with self._offsets_lock:
            return dict(self._offsets)

    def flush(self, timeout: float = 5.0) -> int:
        """Blocks until every outstanding message is delivered or timeout elapses.
        Returns the number of messages still undelivered (0 = fully flushed)."""
        return self._producer.flush(timeout)
