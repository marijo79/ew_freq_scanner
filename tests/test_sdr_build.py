from freqscan.config import DeviceConfig, HackRFSettings, RangeConfig, RTLSettings, Settings
from freqscan.sdr import build_backend
from freqscan.sdr.base import CompositeBackend
from freqscan.sdr.hackrf import HackRFBackend
from freqscan.sdr.rtl import RTLBackend

_RTL = RTLSettings(devices=[DeviceConfig(id=0, freq_start="80M", freq_stop="120M")])
_HACKRF = HackRFSettings(ranges=[RangeConfig(freq_start=850, freq_stop=950, bin_width=20000)])


def test_build_backend_rtl_only():
    settings = Settings(_env_file=None, rtl=_RTL, hackrf=None)
    backend = build_backend(settings)
    assert isinstance(backend, RTLBackend)
    assert len(backend.channels) == 1


def test_build_backend_hackrf_only():
    settings = Settings(_env_file=None, rtl=None, hackrf=_HACKRF)
    backend = build_backend(settings)
    assert isinstance(backend, HackRFBackend)
    assert len(backend.channels) == 1


def test_build_backend_both_combines_channels_rtl_first():
    settings = Settings(_env_file=None, rtl=_RTL, hackrf=_HACKRF)
    backend = build_backend(settings)
    assert isinstance(backend, CompositeBackend)
    assert len(backend.channels) == 2
    assert backend.channels[0].label == "RTL: Dev0 80-120 MHz"
    assert backend.channels[1].label == "HackRF: 850-950 MHz"
