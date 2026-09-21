import json
from dataclasses import dataclass

from freqscan.streaming.kafka_publisher import (
    KafkaSignalPublisher,
    build_client_config,
    build_metadata_payload,
    build_payload,
)


class _FakeMessage:
    def __init__(self, partition: int, offset: int):
        self._partition = partition
        self._offset = offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class _FakeProducer:
    """Mimics confluent_kafka's per-partition offset sequencing, honoring an explicit partition."""

    def __init__(self):
        self.produced: list[tuple] = []
        self._next_offset: dict[int, int] = {}

    def produce(self, topic, key, value, partition=-1, on_delivery=None) -> None:
        actual_partition = partition if partition != -1 else 0
        offset = self._next_offset.get(actual_partition, 0)
        self._next_offset[actual_partition] = offset + 1
        self.produced.append((topic, key, value, partition))
        if on_delivery is not None:
            on_delivery(None, _FakeMessage(partition=actual_partition, offset=offset))

    def poll(self, timeout: float) -> None:
        pass


def test_publish_tracks_offsets_per_partition():
    publisher = KafkaSignalPublisher(_FakeProducer(), "freqscan.signals", "freqscan.signals.metadata")

    publisher.publish("RTL: Dev0 80-120 MHz", bins=[(100, -42.0)])
    publisher.publish("RTL: Dev0 80-120 MHz", bins=[(101, -38.5)])

    assert publisher.offsets() == {0: 1}


def test_publish_calls_caller_on_delivery_alongside_offset_tracking():
    publisher = KafkaSignalPublisher(_FakeProducer(), "freqscan.signals", "freqscan.signals.metadata")
    received = []

    publisher.publish(
        "HackRF: 850-950 MHz",
        bins=[(200, -50.0)],
        on_delivery=lambda err, msg: received.append(msg),
    )

    assert len(received) == 1
    assert publisher.offsets() == {0: 0}


def test_publish_with_explicit_partition_routes_independently():
    publisher = KafkaSignalPublisher(_FakeProducer(), "freqscan.signals", "freqscan.signals.metadata")

    publisher.publish("HackRF: 850-950 MHz", bins=[(200, -50.0)], partition=2)
    publisher.publish("HackRF: 2300-2500 MHz", bins=[(9000, -60.0)], partition=5)
    publisher.publish("HackRF: 850-950 MHz", bins=[(201, -49.0)], partition=2)

    assert publisher.offsets() == {2: 1, 5: 0}


def test_publish_empty_bins_does_not_produce_or_track_offsets():
    producer = _FakeProducer()
    publisher = KafkaSignalPublisher(producer, "freqscan.signals", "freqscan.signals.metadata")

    publisher.publish("RTL: Dev0 80-120 MHz", bins=[])

    assert producer.produced == []
    assert publisher.offsets() == {}


def test_publish_metadata_sends_to_metadata_topic_on_given_partition():
    producer = _FakeProducer()
    publisher = KafkaSignalPublisher(producer, "freqscan.signals", "freqscan.signals.metadata")

    publisher.publish_metadata(
        "HackRF: 850-950 MHz",
        partition=3,
        freq_start_hz=850e6,
        freq_stop_hz=950e6,
        bin_width_hz=20_000,
        n_bins=5000,
        run_epoch=1_700_000_000.0,
    )

    assert len(producer.produced) == 1
    topic, key, value, partition = producer.produced[0]
    assert topic == "freqscan.signals.metadata"
    assert key == b"HackRF: 850-950 MHz"
    assert partition == 3
    assert json.loads(value) == {
        "channel": "HackRF: 850-950 MHz",
        "freq_start_hz": 850e6,
        "freq_stop_hz": 950e6,
        "bin_width_hz": 20_000,
        "n_bins": 5000,
        "run_epoch": 1_700_000_000.0,
    }
    # metadata isn't part of offset tracking (that's per-hop signal delivery only)
    assert publisher.offsets() == {}


def test_build_payload_shape():
    # index 100 then 101 -- consecutive, so the second delta is 1.
    payload = build_payload(
        channel_label="RTL: Dev0 80-120 MHz",
        bins=[(100, -42.0), (101, -38.5)],
        timestamp=1234.5,
    )
    assert payload == {
        "channel": "RTL: Dev0 80-120 MHz",
        "timestamp": 1234.5,
        "bin_index_deltas": [100, 1],
        "power_dbm": [-42.0, -38.5],
    }


def test_build_payload_delta_encodes_non_consecutive_indices():
    # first delta is the absolute index (500); then a gap of 3 (503-500); then
    # another gap of 2 (505-503).
    payload = build_payload(channel_label="HackRF: 850-950 MHz", bins=[(500, -60.0), (503, -55.0), (505, -50.0)], timestamp=1.0)
    assert payload["bin_index_deltas"] == [500, 3, 2]
    assert payload["power_dbm"] == [-60.0, -55.0, -50.0]


def test_build_payload_empty_bins():
    payload = build_payload(channel_label="HackRF: 850-950 MHz", bins=[], timestamp=1.0)
    assert payload["bin_index_deltas"] == []
    assert payload["power_dbm"] == []


@dataclass
class _FakeKafkaSettings:
    """Duck-typed stand-in for KafkaSettings/KafkaViewerSettings -- build_client_config()
    only relies on these attributes being present, not on either concrete class."""

    bootstrap_servers: str = "broker:9092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "OAUTHBEARER"
    region: str = "eu-central-1"
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_ca_location: str | None = None


def test_build_client_config_plaintext_has_no_sasl_or_ssl_keys():
    config = build_client_config(_FakeKafkaSettings(security_protocol="PLAINTEXT"))
    assert config == {"bootstrap.servers": "broker:9092", "security.protocol": "PLAINTEXT"}


def test_build_client_config_oauthbearer_sets_oauth_cb_not_username_password():
    config = build_client_config(
        _FakeKafkaSettings(
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_username="ignored",
            sasl_password="ignored",
        )
    )
    assert config["sasl.mechanisms"] == "OAUTHBEARER"
    assert callable(config["oauth_cb"])
    assert "sasl.username" not in config
    assert "sasl.password" not in config


def test_build_client_config_scram_sets_username_and_password():
    config = build_client_config(
        _FakeKafkaSettings(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="pi2",
            sasl_password="s3cret",
        )
    )
    assert config["sasl.mechanisms"] == "SCRAM-SHA-512"
    assert config["sasl.username"] == "pi2"
    assert config["sasl.password"] == "s3cret"
    assert "oauth_cb" not in config


def test_build_client_config_includes_ssl_ca_location_when_set():
    config = build_client_config(_FakeKafkaSettings(ssl_ca_location="/etc/freqscan/ca.pem"))
    assert config["ssl.ca.location"] == "/etc/freqscan/ca.pem"


def test_build_client_config_omits_ssl_ca_location_when_unset():
    config = build_client_config(_FakeKafkaSettings())
    assert "ssl.ca.location" not in config


def test_build_metadata_payload_shape():
    payload = build_metadata_payload(
        channel_label="RTL: Dev0 80-120 MHz",
        freq_start_hz=80e6,
        freq_stop_hz=120e6,
        bin_width_hz=10_000,
        n_bins=4000,
        run_epoch=1_700_000_000.0,
    )
    assert payload == {
        "channel": "RTL: Dev0 80-120 MHz",
        "freq_start_hz": 80e6,
        "freq_stop_hz": 120e6,
        "bin_width_hz": 10_000,
        "n_bins": 4000,
        "run_epoch": 1_700_000_000.0,
    }
