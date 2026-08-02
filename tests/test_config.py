import json

import pytest

from freqscan.config import Settings


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
    monkeypatch.setenv(
        "HACKRF__RANGES",
        json.dumps([{"freq_start": 850, "freq_stop": 950, "bin_width": 20000, "edge_trim": 0.02}]),
    )

    settings = Settings(_env_file=None)

    assert settings.hackrf is not None
    assert settings.hackrf.lna_gain == 32
    assert settings.hackrf.ranges[0].freq_start == 850
    assert settings.rtl is None


def test_both_backends_configured(monkeypatch):
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M"}]),
    )
    monkeypatch.setenv(
        "HACKRF__RANGES",
        json.dumps([{"freq_start": 850, "freq_stop": 950, "bin_width": 20000, "edge_trim": 0.02}]),
    )

    settings = Settings(_env_file=None)

    assert settings.rtl is not None
    assert settings.hackrf is not None


def test_no_backend_configured_raises(monkeypatch):
    monkeypatch.delenv("RTL__DEVICES", raising=False)
    monkeypatch.delenv("HACKRF__RANGES", raising=False)

    with pytest.raises(ValueError):
        Settings(_env_file=None)
