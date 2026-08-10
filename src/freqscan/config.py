from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RangeConfig(BaseModel):
    """One HackRF swept frequency range."""

    freq_start: float  # MHz
    freq_stop: float  # MHz
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
    bin_width: int  # Hz, passed to hackrf_sweep's single -w flag — shared by every range,
    # since hackrf_sweep sweeps all configured ranges in one process with one FFT bin width
    lna_gain: int = 32
    vga_gain: int = 20
    amp_enable: bool = False


class KafkaSettings(BaseModel):
    """Present + enabled => flagged (above-noise-floor) bins are streamed to this MSK cluster."""

    enabled: bool = True  # flip off without deleting the rest of this section
    bootstrap_servers: str  # comma-separated host:9098 pairs (SASL/IAM public endpoint)
    topic: str = "freqscan.signals"
    # One-time, per-channel grid layout (freq_start_hz/freq_stop_hz/bin_width_hz/n_bins),
    # published once at startup on the same partition as that channel's signal data — lets
    # a consumer (e.g. kafka_viewer) build a fixed grid up front instead of discovering it
    # from whatever bins happen to get flagged over time. Must be provisioned with the same
    # partition count as `topic`.
    metadata_topic: str = "freqscan.signals.metadata"
    region: str = "eu-central-1"
    baseline_window: int = 50  # readings per bin kept for the rolling noise-floor baseline

    # Real MSK always uses SASL/IAM; override to "PLAINTEXT"/"" for a local, non-MSK broker
    # (e.g. a native Kafka instance used to smoke-test the producer without touching AWS).
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "OAUTHBEARER"

    # Signal-flagging margin (dB above a bin's adaptive baseline) — irrelevant, so kept out
    # of RangeConfig/DeviceConfig entirely, when Kafka streaming isn't configured.
    signal_margin_db: float = 8.0  # default for any device/range not listed below
    rtl_margins_db: dict[int, float] = {}  # keyed by DeviceConfig.id
    hackrf_margins_db: dict[int, float] = {}  # keyed by 0-based index into HACKRF__RANGES

    # Spatial check (optional, on top of the temporal one above): also require a bin to
    # stand out from the median of its own hop before flagging, not just its own history —
    # catches a whole band being uniformly noisy without treating that as N different
    # signals. None (default) = disabled, same per-device/range override pattern as above.
    spatial_margin_db: float | None = None
    rtl_spatial_margins_db: dict[int, float] = {}
    hackrf_spatial_margins_db: dict[int, float] = {}

    # Prominence check (optional, independent OR alongside the temporal one above, not a
    # refinement of it like spatial_margin_db): flags a bin regardless of its own history
    # if it's simply far enough above the median of its own hop right now. For signals
    # that are always there and always strong (e.g. a steady FM broadcast carrier) — the
    # temporal check alone never flags these, since the adaptive baseline just learns to
    # expect them. Use a generous margin (bigger than spatial_margin_db) so this doesn't
    # also catch generically noisy broadband bins. None (default) = disabled.
    prominence_margin_db: float | None = None
    rtl_prominence_margins_db: dict[int, float] = {}
    hackrf_prominence_margins_db: dict[int, float] = {}


class KafkaViewerSettings(BaseSettings):
    """Connection info for the read-only Kafka viewer (`python -m freqscan.kafka_viewer`).

    Deliberately separate from `KafkaSettings`: the viewer is a pure consumer with no
    RTL/HackRF hardware and none of the producer's noise-floor-detection concerns
    (margins, baseline window), so it doesn't go through `Settings`/`_require_at_least_one_backend`
    at all — just the same `KAFKA__*` connection variables already in `.env`.
    """

    bootstrap_servers: str
    topic: str = "freqscan.signals"
    metadata_topic: str = "freqscan.signals.metadata"
    region: str = "eu-central-1"
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "OAUTHBEARER"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="KAFKA__", extra="ignore")


class Settings(BaseSettings):
    waterfall_rows: int = 100
    rtl: RTLSettings | None = None
    hackrf: HackRFSettings | None = None
    kafka: KafkaSettings | None = None

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
