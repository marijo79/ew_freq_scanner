import json

import pytest

from freqscan.config import KafkaViewerSettings, PlutoChannelConfig, Settings


def test_rtl_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("RTL__GAIN", "50")
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps(
            [{"id": 0, "freq_start": "80M", "freq_stop": "120M", "bin_width": 10000, "edge_trim": 0.3}]
        ),
    )

    settings = Settings(_env_file=None)

    assert settings.rtl is not None
    assert settings.rtl.gain == 50
    assert settings.rtl.devices[0].id == 0
    assert settings.rtl.devices[0].freq_start == "80M"
    assert settings.rtl.devices[0].bin_width == 10000
    assert settings.rtl.devices[0].edge_trim == 0.3
    assert settings.hackrf is None


def test_rtl_gain_auto_maps_to_none(monkeypatch):
    monkeypatch.setenv("RTL__GAIN", "auto")
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )

    settings = Settings(_env_file=None)

    assert settings.rtl.gain is None


def test_hackrf_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("HACKRF__LNA_GAIN", "32")
    monkeypatch.setenv("HACKRF__BIN_WIDTH", "20000")
    monkeypatch.setenv(
        "HACKRF__RANGES",
        json.dumps([{"freq_start": 850, "freq_stop": 950, "edge_trim": 0.02}]),
    )

    settings = Settings(_env_file=None)

    assert settings.hackrf is not None
    assert settings.hackrf.lna_gain == 32
    assert settings.hackrf.bin_width == 20000
    assert settings.hackrf.ranges[0].freq_start == 850
    assert settings.rtl is None


def test_pluto_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("PLUTO__URI", "ip:192.168.2.1")
    monkeypatch.setenv("PLUTO__BIN_WIDTH", "50000")
    monkeypatch.setenv("PLUTO__CAPTURE_BANDWIDTH", "20000000")
    monkeypatch.setenv(
        "PLUTO__CHANNELS", json.dumps([{"channel": 1, "gain_control_mode": "manual", "hardwaregain": 40.0}])
    )
    monkeypatch.setenv(
        "PLUTO__RANGES",
        json.dumps([{"freq_start": 100, "freq_stop": 115, "edge_trim": 0.1}]),
    )

    settings = Settings(_env_file=None)

    assert settings.pluto is not None
    assert settings.pluto.uri == "ip:192.168.2.1"
    assert settings.pluto.bin_width == 50000
    assert settings.pluto.capture_bandwidth == 20000000
    assert settings.pluto.channels == [
        PlutoChannelConfig(channel=1, gain_control_mode="manual", hardwaregain=40.0)
    ]
    assert settings.pluto.ranges[0].freq_start == 100
    assert settings.rtl is None
    assert settings.hackrf is None


def test_pluto_channels_defaults_to_single_rx1(monkeypatch):
    monkeypatch.setenv("PLUTO__BIN_WIDTH", "50000")
    monkeypatch.setenv(
        "PLUTO__RANGES",
        json.dumps([{"freq_start": 100, "freq_stop": 115}]),
    )

    settings = Settings(_env_file=None)

    assert settings.pluto.channels == [PlutoChannelConfig(channel=1)]


def test_pluto_channels_rejects_duplicate_channel_numbers(monkeypatch):
    monkeypatch.setenv("PLUTO__BIN_WIDTH", "50000")
    monkeypatch.setenv(
        "PLUTO__RANGES",
        json.dumps([{"freq_start": 100, "freq_stop": 115}]),
    )
    monkeypatch.setenv("PLUTO__CHANNELS", json.dumps([{"channel": 1}, {"channel": 1}]))

    with pytest.raises(ValueError):
        Settings(_env_file=None)


def test_pluto_two_channels_load_from_env(monkeypatch):
    monkeypatch.setenv("PLUTO__BIN_WIDTH", "50000")
    monkeypatch.setenv(
        "PLUTO__RANGES",
        json.dumps([{"freq_start": 100, "freq_stop": 115}]),
    )
    monkeypatch.setenv(
        "PLUTO__CHANNELS",
        json.dumps([{"channel": 1, "hardwaregain": 30.0}, {"channel": 2, "hardwaregain": 50.0}]),
    )

    settings = Settings(_env_file=None)

    assert [c.channel for c in settings.pluto.channels] == [1, 2]
    assert [c.hardwaregain for c in settings.pluto.channels] == [30.0, 50.0]


def test_pluto_stare_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("PLUTO_STARE__URI", "ip:192.168.2.1")
    monkeypatch.setenv("PLUTO_STARE__FREQUENCY", "2437")
    monkeypatch.setenv("PLUTO_STARE__BIN_WIDTH", "10000")
    monkeypatch.setenv("PLUTO_STARE__SAMPLE_RATE", "20000000")
    monkeypatch.setenv("PLUTO_STARE__EDGE_TRIM", "0.1")
    monkeypatch.setenv(
        "PLUTO_STARE__CHANNELS", json.dumps([{"channel": 1, "gain_control_mode": "manual", "hardwaregain": 40.0}])
    )

    settings = Settings(_env_file=None)

    assert settings.pluto_stare is not None
    assert settings.pluto_stare.uri == "ip:192.168.2.1"
    assert settings.pluto_stare.frequency == 2437
    assert settings.pluto_stare.bin_width == 10000
    assert settings.pluto_stare.sample_rate == 20000000
    assert settings.pluto_stare.edge_trim == 0.1
    assert settings.pluto_stare.channels == [
        PlutoChannelConfig(channel=1, gain_control_mode="manual", hardwaregain=40.0)
    ]
    assert settings.pluto is None
    assert settings.rtl is None
    assert settings.hackrf is None


def test_pluto_sweep_and_stare_cannot_both_be_configured(monkeypatch):
    monkeypatch.setenv(
        "PLUTO__RANGES",
        json.dumps([{"freq_start": 100, "freq_stop": 115}]),
    )
    monkeypatch.setenv("PLUTO__BIN_WIDTH", "50000")
    monkeypatch.setenv("PLUTO_STARE__FREQUENCY", "2437")
    monkeypatch.setenv("PLUTO_STARE__BIN_WIDTH", "10000")

    with pytest.raises(ValueError):
        Settings(_env_file=None)


def test_both_backends_configured(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("HACKRF__BIN_WIDTH", "20000")
    monkeypatch.setenv(
        "HACKRF__RANGES",
        json.dumps([{"freq_start": 850, "freq_stop": 950, "edge_trim": 0.02}]),
    )

    settings = Settings(_env_file=None)

    assert settings.rtl is not None
    assert settings.hackrf is not None


def test_no_backend_configured_raises(monkeypatch):
    monkeypatch.delenv("RTL__DEVICES", raising=False)
    monkeypatch.delenv("HACKRF__RANGES", raising=False)
    monkeypatch.delenv("PLUTO__RANGES", raising=False)
    monkeypatch.delenv("PLUTO_STARE__FREQUENCY", raising=False)

    with pytest.raises(ValueError):
        Settings(_env_file=None)


def test_kafka_absent_by_default(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )

    settings = Settings(_env_file=None)

    assert settings.kafka is None


def test_kafka_enabled_defaults_true(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")

    settings = Settings(_env_file=None)

    assert settings.kafka is not None
    assert settings.kafka.enabled is True


def test_kafka_can_be_disabled_while_configured(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")
    monkeypatch.setenv("KAFKA__ENABLED", "false")

    settings = Settings(_env_file=None)

    assert settings.kafka.enabled is False
    assert settings.kafka.bootstrap_servers == "broker:9098"


def test_kafka_margins_keyed_by_id_and_range_index(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")
    monkeypatch.setenv("KAFKA__SIGNAL_MARGIN_DB", "8.0")
    monkeypatch.setenv("KAFKA__RTL_MARGINS_DB", json.dumps({"0": 5.0}))
    monkeypatch.setenv("KAFKA__HACKRF_MARGINS_DB", json.dumps({"1": 12.0}))
    monkeypatch.setenv("KAFKA__PLUTO_MARGINS_DB", json.dumps({"0": 6.0}))

    settings = Settings(_env_file=None)

    assert settings.kafka.signal_margin_db == 8.0
    assert settings.kafka.rtl_margins_db == {0: 5.0}
    assert settings.kafka.hackrf_margins_db == {1: 12.0}
    assert settings.kafka.pluto_margins_db == {0: 6.0}


def test_kafka_spatial_margin_defaults_to_none_disabled(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")

    settings = Settings(_env_file=None)

    assert settings.kafka.spatial_margin_db is None
    assert settings.kafka.rtl_spatial_margins_db == {}
    assert settings.kafka.hackrf_spatial_margins_db == {}


def test_kafka_spatial_margins_keyed_by_id_and_range_index(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")
    monkeypatch.setenv("KAFKA__SPATIAL_MARGIN_DB", "8.0")
    monkeypatch.setenv("KAFKA__RTL_SPATIAL_MARGINS_DB", json.dumps({"0": 5.0}))
    monkeypatch.setenv("KAFKA__HACKRF_SPATIAL_MARGINS_DB", json.dumps({"1": 12.0}))

    settings = Settings(_env_file=None)

    assert settings.kafka.spatial_margin_db == 8.0
    assert settings.kafka.rtl_spatial_margins_db == {0: 5.0}
    assert settings.kafka.hackrf_spatial_margins_db == {1: 12.0}


def test_kafka_prominence_margin_defaults_to_none_disabled(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")

    settings = Settings(_env_file=None)

    assert settings.kafka.prominence_margin_db is None
    assert settings.kafka.rtl_prominence_margins_db == {}
    assert settings.kafka.hackrf_prominence_margins_db == {}


def test_kafka_prominence_margins_keyed_by_id_and_range_index(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")
    monkeypatch.setenv("KAFKA__PROMINENCE_MARGIN_DB", "20.0")
    monkeypatch.setenv("KAFKA__RTL_PROMINENCE_MARGINS_DB", json.dumps({"0": 15.0}))
    monkeypatch.setenv("KAFKA__HACKRF_PROMINENCE_MARGINS_DB", json.dumps({"1": 25.0}))

    settings = Settings(_env_file=None)

    assert settings.kafka.prominence_margin_db == 20.0
    assert settings.kafka.rtl_prominence_margins_db == {0: 15.0}
    assert settings.kafka.hackrf_prominence_margins_db == {1: 25.0}


def test_kafka_viewer_settings_load_from_env_with_no_rtl_hackrf(monkeypatch):
    monkeypatch.delenv("RTL__DEVICES", raising=False)
    monkeypatch.delenv("HACKRF__RANGES", raising=False)
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")
    monkeypatch.setenv("KAFKA__TOPIC", "freqscan.signals")

    settings = KafkaViewerSettings(_env_file=None)

    assert settings.bootstrap_servers == "broker:9098"
    assert settings.topic == "freqscan.signals"
    assert settings.security_protocol == "SASL_SSL"


def test_kafka_viewer_settings_local_broker_override(monkeypatch):
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT")
    monkeypatch.setenv("KAFKA__SASL_MECHANISM", "")

    settings = KafkaViewerSettings(_env_file=None)

    assert settings.security_protocol == "PLAINTEXT"
    assert settings.sasl_mechanism == ""


def test_kafka_metadata_topic_defaults(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")

    settings = Settings(_env_file=None)

    assert settings.kafka.metadata_topic == "freqscan.signals.metadata"


def test_kafka_metadata_topic_overridable(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")
    monkeypatch.setenv("KAFKA__METADATA_TOPIC", "custom.metadata.topic")

    settings = Settings(_env_file=None)

    assert settings.kafka.metadata_topic == "custom.metadata.topic"


def test_kafka_viewer_settings_metadata_topic_default(monkeypatch):
    monkeypatch.setenv("KAFKA__BOOTSTRAP_SERVERS", "broker:9098")

    settings = KafkaViewerSettings(_env_file=None)

    assert settings.metadata_topic == "freqscan.signals.metadata"
