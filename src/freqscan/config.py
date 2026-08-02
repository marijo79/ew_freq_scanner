from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RangeConfig(BaseModel):
    """One HackRF swept frequency range."""

    freq_start: float  # MHz
    freq_stop: float  # MHz
    bin_width: int
    edge_trim: float = 0.05


class DeviceConfig(BaseModel):
    """One physical RTL-SDR dongle."""

    id: int
    freq_start: str  # e.g. "80M", passed straight to rtl_power
    freq_stop: str
    bin_width: int = 10_000  # Hz, passed to rtl_power's -f start:stop:bin_width
    edge_trim: float = 0.15  # crop percent for rtl_power's own -c flag


class RTLSettings(BaseModel):
    devices: list[DeviceConfig]
    gain: int | None = 50  # None (env: "auto") omits -g, letting the tuner auto-gain
    interval: int = 1


class HackRFSettings(BaseModel):
    ranges: list[RangeConfig]
    lna_gain: int = 32
    vga_gain: int = 20
    amp_enable: bool = False


class Settings(BaseSettings):
    waterfall_rows: int = 100
    rtl: RTLSettings | None = None
    hackrf: HackRFSettings | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
        env_parse_none_str="auto",
    )

    @model_validator(mode="after")
    def _require_at_least_one_backend(self) -> "Settings":
        if self.rtl is None and self.hackrf is None:
            raise ValueError(
                "At least one backend must be configured: set RTL__* and/or HACKRF__* settings"
            )
        return self
