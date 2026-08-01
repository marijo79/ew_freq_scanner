import json

import pytest

from freqscan.config import Settings


def test_rtl_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("SDR", "rtl")
    monkeypatch.setenv("RTL__BIN_SIZE", "10k")
    monkeypatch.setenv("RTL__GAIN", "50")
    monkeypatch.setenv(
        "RTL__DEVICES",
        json.dumps([{"id": 0, "freq_start": "80M", "freq_stop": "120M", "label": "Device 0"}]),
    )

    settings = Settings()

    assert settings.sdr == "rtl"
    assert settings.rtl is not None
    assert settings.rtl.gain == 50
    assert settings.rtl.devices[0].id == 0
    assert settings.rtl.devices[0].freq_start == "80M"
    assert settings.hackrf is None


def test_hackrf_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("SDR", "hackrf")
    monkeypatch.setenv("HACKRF__LNA_GAIN", "32")
    monkeypatch.setenv(
        "HACKRF__RANGES",
        json.dumps(
            [{"freq_start": 850, "freq_stop": 950, "label": "850-950 MHz", "bin_width": 20000, "edge_trim": 0.02}]
        ),
    )

    settings = Settings()

    assert settings.sdr == "hackrf"
    assert settings.hackrf is not None
    assert settings.hackrf.lna_gain == 32
    assert settings.hackrf.ranges[0].freq_start == 850
    assert settings.rtl is None


def test_missing_matching_section_raises(monkeypatch):
    monkeypatch.setenv("SDR", "rtl")
    monkeypatch.delenv("RTL__DEVICES", raising=False)

    with pytest.raises(ValueError):
        Settings()
