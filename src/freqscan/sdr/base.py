import sys
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field

from freqscan.streaming.kafka_publisher import KafkaSignalPublisher


@dataclass
class SweepState:
    sweep: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    history: deque = field(default_factory=deque)
    # Display-only smoothing state (plotting.common.channel_snapshot()'s concern only,
    # same as `history` -- producers never touch these).
    last_raw_freqs: "np.ndarray | None" = None
    last_raw_powers: "np.ndarray | None" = None
    display_avg: "np.ndarray | None" = None
    display_avg_freqs: "np.ndarray | None" = None


@dataclass
class Channel:
    label: str
    freq_start_mhz: float
    freq_stop_mhz: float
    state: SweepState


class SDRBackend(ABC):
    channels: list[Channel]
    publisher: KafkaSignalPublisher | None = None

    def __init__(self) -> None:
        self.errors: list[str] = []
        self._failure = threading.Event()

    def report_error(self, message: str) -> None:
        """Record and print a startup/runtime failure from a backend thread."""
        print(message, file=sys.stderr)
        self.errors.append(message)
        self._failure.set()

    def wait_ready(self, timeout: float) -> bool:
        """Block up to `timeout` seconds; return False if a channel already reported a failure."""
        failed = self._failure.wait(timeout)
        return not failed

    @abstractmethod
    def start(self) -> None:
        """Launch the background thread(s)/subprocess(es) that populate channel states."""

    @abstractmethod
    def stop(self) -> None:
        """Terminate any running subprocess(es)."""


class CompositeBackend(SDRBackend):
    """Runs multiple backends concurrently, presenting their channels as one combined list."""

    def __init__(self, backends: list[SDRBackend]):
        super().__init__()
        self.backends = backends
        self.channels = [channel for backend in backends for channel in backend.channels]

    def start(self) -> None:
        for backend in self.backends:
            backend.start()

    def stop(self) -> None:
        for backend in self.backends:
            backend.stop()

    def wait_ready(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        for backend in self.backends:
            remaining = max(0.0, deadline - time.monotonic())
            if not backend.wait_ready(remaining):
                return False
        return True
