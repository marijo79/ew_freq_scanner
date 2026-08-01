import subprocess
import threading
from collections import deque

from freqscan.config import DeviceConfig, RTLSettings
from freqscan.parsing import parse_sweep_line, trim_edges
from freqscan.sdr.base import Channel, SDRBackend, SweepState

_SUFFIX_TO_HZ = {"k": 1e3, "K": 1e3, "m": 1e6, "M": 1e6, "g": 1e9, "G": 1e9}


def _freq_str_to_mhz(spec: str) -> float:
    spec = spec.strip()
    if spec and spec[-1] in _SUFFIX_TO_HZ:
        hz = float(spec[:-1]) * _SUFFIX_TO_HZ[spec[-1]]
    else:
        hz = float(spec)
    return hz / 1e6


class RTLBackend(SDRBackend):
    """One rtl_power subprocess per configured device."""

    def __init__(self, settings: RTLSettings, waterfall_rows: int):
        self.settings = settings
        self.channels = [
            Channel(
                label=dev.label,
                freq_start_mhz=_freq_str_to_mhz(dev.freq_start),
                freq_stop_mhz=_freq_str_to_mhz(dev.freq_stop),
                state=SweepState(history=deque(maxlen=waterfall_rows)),
            )
            for dev in settings.devices
        ]
        self._procs: list[subprocess.Popen] = []
        self._procs_lock = threading.Lock()

    def start(self) -> None:
        for dev, channel in zip(self.settings.devices, self.channels):
            threading.Thread(target=self._run_device, args=(dev, channel), daemon=True).start()

    def _run_device(self, dev: DeviceConfig, channel: Channel) -> None:
        cmd = [
            "rtl_power",
            "-d", str(dev.id),
            "-f", f"{dev.freq_start}:{dev.freq_stop}:{self.settings.bin_size}",
            "-g", str(self.settings.gain),
            "-i", str(self.settings.interval),
            "-",
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        with self._procs_lock:
            self._procs.append(proc)
        for raw in proc.stdout:
            line = parse_sweep_line(raw)
            if line is None:
                continue
            freqs, powers = trim_edges(line.hz_low, line.hz_step, line.powers, self.settings.edge_trim)
            with channel.state.lock:
                for f, p in zip(freqs, powers):
                    channel.state.sweep[f] = p

    def stop(self) -> None:
        with self._procs_lock:
            for proc in self._procs:
                proc.terminate()
