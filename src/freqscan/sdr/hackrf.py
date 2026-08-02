import subprocess
import threading
from collections import deque

from freqscan.config import HackRFSettings, RangeConfig
from freqscan.parsing import parse_sweep_line, trim_edges
from freqscan.sdr.base import Channel, SDRBackend, SweepState


def _make_channel(r: RangeConfig, waterfall_rows: int) -> Channel:
    return Channel(
        label=f"HackRF: {r.freq_start:.0f}-{r.freq_stop:.0f} MHz",
        freq_start_mhz=r.freq_start,
        freq_stop_mhz=r.freq_stop,
        state=SweepState(history=deque(maxlen=waterfall_rows)),
    )


class HackRFBackend(SDRBackend):
    """A single hackrf_sweep subprocess covering all configured ranges, demuxed by frequency."""

    def __init__(self, settings: HackRFSettings, waterfall_rows: int):
        super().__init__()
        self.settings = settings
        self.channels = [_make_channel(r, waterfall_rows) for r in settings.ranges]
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
            freq_args += ["-f", f"{int(r.freq_start)}:{int(r.freq_stop)}"]
        bin_width = min(r.bin_width for r in self.settings.ranges)
        cmd = [
            "hackrf_sweep", *freq_args,
            "-w", str(bin_width),
            "-l", str(self.settings.lna_gain),
            "-g", str(self.settings.vga_gain),
            "-a", "1" if self.settings.amp_enable else "0",
        ]
        try:
            self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except OSError as exc:
            self.report_error(f"failed to launch hackrf_sweep: {exc}")
            return
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

        self._proc.wait()
        if self._proc.returncode > 0:
            stderr_output = self._proc.stderr.read().strip() if self._proc.stderr else ""
            self.report_error(
                f"hackrf_sweep exited with code {self._proc.returncode}: {stderr_output}"
            )

    def stop(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
