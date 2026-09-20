import csv
import os
import threading
import time
from datetime import datetime


class CsvSweepWriter:
    """Appends every raw sweep bin (one row per bin) for a single channel to its own CSV
    file. One instance per channel — device/frequency-range identity lives in the filename
    (see make_csv_path), not in the row data, so the file itself only needs timestamp/freq/power."""

    def __init__(self, path: str):
        self._file = open(path, "w", newline="")
        self._writer = csv.writer(self._file)
        self._writer.writerow(["timestamp", "freq_hz", "power_dbm"])
        self._lock = threading.Lock()

    def write_hop(self, freqs, powers) -> None:
        timestamp = time.time()
        rows = [(timestamp, f, p) for f, p in zip(freqs, powers)]
        with self._lock:
            self._writer.writerows(rows)

    def close(self) -> None:
        with self._lock:
            self._file.close()


def make_csv_path(csv_dir: str, device: str, freq_start_mhz: float, freq_stop_mhz: float, when: datetime) -> str:
    """<device>_<freq_start-freq_stop>MHz_<yyyymmddHHMMSS>.csv, e.g. HackRF_2400-2500MHz_20260826143005.csv"""
    freq_range = f"{freq_start_mhz:.0f}-{freq_stop_mhz:.0f}MHz"
    filename = f"{device}_{freq_range}_{when.strftime('%Y%m%d%H%M%S')}.csv"
    return os.path.join(csv_dir, filename)
