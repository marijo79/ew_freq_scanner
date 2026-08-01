from freqscan.config import Settings
from freqscan.sdr.base import SDRBackend
from freqscan.sdr.hackrf import HackRFBackend
from freqscan.sdr.rtl import RTLBackend


def build_backend(settings: Settings) -> SDRBackend:
    if settings.sdr == "rtl":
        assert settings.rtl is not None
        return RTLBackend(settings.rtl, settings.waterfall_rows)
    if settings.sdr == "hackrf":
        assert settings.hackrf is not None
        return HackRFBackend(settings.hackrf, settings.waterfall_rows)
    raise ValueError(f"Unknown SDR backend: {settings.sdr!r}")
