import subprocess
import threading
from collections import deque

from freqscan.config import HackRFSettings
from freqscan.parsing import parse_sweep_line, trim_edges
from freqscan.sdr.base import Channel, SDRBackend, SweepState


class HackRFBackend(SDRBackend):
    """A single hackrf_sweep subprocess covering all configured ranges, demuxed by frequency."""

    def __init__(self, settings: HackRFSettings, waterfall_rows: int):
        self.settings = settings
        self.channels = [
            Channel(
                label=r.label,
                freq_start_mhz=r.freq_start,
                freq_stop_mhz=r.freq_stop,
                state=SweepState(history=deque(maxlen=waterfall_rows)),
            )
            for r in settings.ranges
        ]
        self._proc: subprocess.Popen | None = None

    def _range_index_for(self, hz_low: float) -> int | None:
        mhz = hz_low / 1e6
        for i, r in enumerate(self.settings.ranges):
            if r.freq_start <= mhz < r.freq_stop:
                return i
        return None

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        freq_args = []
        for r in self.settings.ranges:
            freq_args += ["-f", f"{r.freq_start}:{r.freq_stop}"]
        bin_width = min(r.bin_width for r in self.settings.ranges)
        cmd = [
            "hackrf_sweep", *freq_args,
            "-w", str(bin_width),
            "-l", str(self.settings.lna_gain),
            "-g", str(self.settings.vga_gain),
        ]
        self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        for raw in self._proc.stdout:
            line = parse_sweep_line(raw)
            if line is None:
                continue
            idx = self._range_index_for(line.hz_low)
            if idx is None:
                continue
            edge_trim = self.settings.ranges[idx].edge_trim
            freqs, powers = trim_edges(line.hz_low, line.hz_step, line.powers, edge_trim)
            channel = self.channels[idx]
            with channel.state.lock:
                for f, p in zip(freqs, powers):
                    channel.state.sweep[f] = p

    def stop(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
