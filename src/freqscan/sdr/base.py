import threading
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field


@dataclass
class SweepState:
    sweep: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    history: deque = field(default_factory=deque)


@dataclass
class Channel:
    label: str
    freq_start_mhz: float
    freq_stop_mhz: float
    state: SweepState


class SDRBackend(ABC):
    channels: list[Channel]

    @abstractmethod
    def start(self) -> None:
        """Launch the background thread(s)/subprocess(es) that populate channel states."""

    @abstractmethod
    def stop(self) -> None:
        """Terminate any running subprocess(es)."""
