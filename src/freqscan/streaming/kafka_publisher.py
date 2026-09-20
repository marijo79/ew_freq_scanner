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
    return Producer(build_client_config(settings))


def build_payload(channel_label: str, bins: list[tuple[float, float]], timestamp: float) -> dict:
    return {
        "channel": channel_label,
        "timestamp": timestamp,
        "bins": [{"freq_hz": freq_hz, "power_dbm": power_dbm} for freq_hz, power_dbm in bins],
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
    """Publishes one batched message per hop of flagged (freq_hz, power_dbm) bins, plus
    one-time channel metadata (grid layout) on a separate topic, same partition."""

    def __init__(self, producer: Producer, topic: str, metadata_topic: str):
        self._producer = producer
        self._topic = topic
        self._metadata_topic = metadata_topic
        self._offsets: dict[int, int] = {}
        self._offsets_lock = threading.Lock()

    def publish(
        self,
        channel_label: str,
        bins: list[tuple[float, float]],
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
