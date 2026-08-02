import subprocess
import threading
from collections import deque

from freqscan.config import DeviceConfig, RTLSettings
from freqscan.parsing import parse_sweep_line
from freqscan.sdr.base import Channel, SDRBackend, SweepState

_SUFFIX_TO_HZ = {"k": 1e3, "K": 1e3, "m": 1e6, "M": 1e6, "g": 1e9, "G": 1e9}


def _freq_str_to_mhz(spec: str) -> float:
    spec = spec.strip()
    if spec and spec[-1] in _SUFFIX_TO_HZ:
        hz = float(spec[:-1]) * _SUFFIX_TO_HZ[spec[-1]]
    else:
        hz = float(spec)
    return hz / 1e6


def _make_channel(dev: DeviceConfig, waterfall_rows: int) -> Channel:
    freq_start_mhz = _freq_str_to_mhz(dev.freq_start)
    freq_stop_mhz = _freq_str_to_mhz(dev.freq_stop)
    return Channel(
        label=f"RTL: Dev{dev.id} {freq_start_mhz:.0f}-{freq_stop_mhz:.0f} MHz",
        freq_start_mhz=freq_start_mhz,
        freq_stop_mhz=freq_stop_mhz,
        state=SweepState(history=deque(maxlen=waterfall_rows)),
    )


class RTLBackend(SDRBackend):
    """One rtl_power subprocess per configured device."""

    def __init__(self, settings: RTLSettings, waterfall_rows: int):
        super().__init__()
        self.settings = settings
        self.channels = [_make_channel(dev, waterfall_rows) for dev in settings.devices]
        self._procs: list[subprocess.Popen] = []
        self._procs_lock = threading.Lock()

    def start(self) -> None:
        for dev, channel in zip(self.settings.devices, self.channels):
            threading.Thread(target=self._run_device, args=(dev, channel), daemon=True).start()

    def _run_device(self, dev: DeviceConfig, channel: Channel) -> None:
        cmd = [
            "rtl_power",
            "-d", str(dev.id),
            "-f", f"{dev.freq_start}:{dev.freq_stop}:{dev.bin_width}",
        ]
        if self.settings.gain is not None:
            cmd += ["-g", str(self.settings.gain)]
        cmd += [
            "-i", str(self.settings.interval),
            "-c", f"{dev.edge_trim * 100:.0f}%",
            "-",
        ]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except OSError as exc:
            self.report_error(f"[{channel.label}] failed to launch rtl_power: {exc}")
            return
        with self._procs_lock:
            self._procs.append(proc)
        for raw in proc.stdout:
            line = parse_sweep_line(raw)
            if line is None:
                continue
            # rtl_power's own -c crop already discards unreliable edge bins and widens
            # each hop's capture so adjacent hops tile without gaps — no further trimming needed.
            freqs = [line.hz_low + line.hz_step * i for i in range(len(line.powers))]
            with channel.state.lock:
                for f, p in zip(freqs, line.powers):
                    channel.state.sweep[f] = p

        proc.wait()
        if proc.returncode > 0:
            stderr_output = proc.stderr.read().strip() if proc.stderr else ""
            self.report_error(
                f"[{channel.label}] rtl_power exited with code {proc.returncode}: {stderr_output}"
            )

    def stop(self) -> None:
        with self._procs_lock:
            for proc in self._procs:
                proc.terminate()
