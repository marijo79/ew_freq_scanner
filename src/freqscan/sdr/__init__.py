from freqscan.config import Settings
from freqscan.sdr.base import CompositeBackend, SDRBackend
from freqscan.sdr.hackrf import HackRFBackend
from freqscan.sdr.rtl import RTLBackend


def build_backend(settings: Settings) -> SDRBackend:
    backends: list[SDRBackend] = []
    if settings.rtl is not None:
        backends.append(RTLBackend(settings.rtl, settings.waterfall_rows))
    if settings.hackrf is not None:
        backends.append(HackRFBackend(settings.hackrf, settings.waterfall_rows))
    if len(backends) == 1:
        return backends[0]
    return CompositeBackend(backends)
