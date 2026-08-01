from typing import Literal

from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RangeConfig(BaseModel):
    """One HackRF swept frequency range."""

    freq_start: float  # MHz
    freq_stop: float  # MHz
    label: str
    bin_width: int
    edge_trim: float = 0.05


class DeviceConfig(BaseModel):
    """One physical RTL-SDR dongle."""

    id: int
    freq_start: str  # e.g. "80M", passed straight to rtl_power
    freq_stop: str
    label: str


class RTLSettings(BaseModel):
    devices: list[DeviceConfig]
    bin_size: str = "10k"
    gain: int = 50
    interval: int = 1
    edge_trim: float = 0.15


class HackRFSettings(BaseModel):
    ranges: list[RangeConfig]
    lna_gain: int = 32
    vga_gain: int = 20


class Settings(BaseSettings):
    sdr: Literal["rtl", "hackrf"]
    waterfall_rows: int = 100
    rtl: RTLSettings | None = None
    hackrf: HackRFSettings | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    @model_validator(mode="after")
    def _require_matching_section(self) -> "Settings":
        if self.sdr == "rtl" and self.rtl is None:
            raise ValueError("SDR=rtl requires RTL__* settings (e.g. RTL__DEVICES)")
        if self.sdr == "hackrf" and self.hackrf is None:
            raise ValueError("SDR=hackrf requires HACKRF__* settings (e.g. HACKRF__RANGES)")
        return self
