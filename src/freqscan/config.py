from typing import Literal

from pydantic import BaseModel, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _no_duplicate_channels(channels: list["PlutoChannelConfig"]) -> list["PlutoChannelConfig"]:
    seen = [c.channel for c in channels]
    if len(seen) != len(set(seen)):
        raise ValueError(f"channels lists the same RX channel more than once: {seen}")
    return channels


class RangeConfig(BaseModel):
    """One swept frequency range — shared by HackRF and Pluto, both single-device
    backends that sweep multiple configured ranges from one physical radio."""

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


class PlutoChannelConfig(BaseModel):
    """One AD9361 RX channel's own gain settings. RX1 and RX2 physically share the same
    RX_LO, sample_rate, and rf_bandwidth (there's exactly one shared local oscillator and
    ADC clock domain feeding both antenna inputs on the chip — not a firmware limitation,
    true of every AD9361-based radio), so those stay on PlutoSettings/PlutoStareSettings
    themselves; gain is the one thing that genuinely differs per RX chain. Confirmed live
    2026-09-03 that two separate `iio_readdev` processes, each reading only one channel's
    own voltage pair (voltage0/1 for RX1, voltage2/3 for RX2) off the same `cf-ad9361-lpc`
    device, capture real independent data concurrently with no buffer conflict — so each
    entry here gets its own capture subprocess and its own plot channel (see
    PlutoBackend/PlutoStareBackend), not just a config-only distinction."""

    channel: Literal[1, 2] = 1
    gain_control_mode: Literal["manual", "fast_attack", "slow_attack", "hybrid"] = "slow_attack"
    hardwaregain: float = 40.0  # dB; only sent to the device when gain_control_mode == "manual"


class PlutoSettings(BaseModel):
    uri: str = "ip:192.168.2.1"
    ranges: list[RangeConfig]
    bin_width: int  # Hz, shared by every range (one physical device, one sweep process)
    capture_bandwidth: int = 20_000_000  # Hz, per-hop instantaneous RX bandwidth (Pluto max ~56MHz)
    # One entry per RX channel to capture — AD9361 has no discrete on/off RX amplifier
    # (confirmed against this board's own RF schematic — TX/RX SMAs connect to the chip
    # through passive baluns only, no LNA/PA populated), so gain (see PlutoChannelConfig)
    # is the actual controllable per-channel setting, analogous in spirit to
    # HACKRF__LNA_GAIN/VGA_GAIN. Defaults to just RX1 for backward compatibility.
    channels: list[PlutoChannelConfig] = [PlutoChannelConfig(channel=1)]

    _validate_channels = field_validator("channels")(_no_duplicate_channels)


class PlutoStareSettings(BaseModel):
    """Continuous single-frequency monitoring — the "stare" counterpart to `PlutoSettings`'
    "sweep" mode (see scripts/pluto_stare.py). Mutually exclusive with `PlutoSettings`:
    both would fight over the same exclusive-access physical radio, so only one of
    PLUTO__* / PLUTO_STARE__* may be configured at a time (see Settings._require_at_least_one_backend)."""

    uri: str = "ip:192.168.2.1"
    frequency: float  # MHz, the fixed RX LO — never retuned once set
    bin_width: int  # Hz per FFT bin
    sample_rate: int = 20_000_000  # Hz, instantaneous bandwidth around `frequency` (Pluto max ~56MHz)
    edge_trim: float = 0.1  # dropped from each edge for filter roll-off, not hop-tiling (there's only one window)
    channels: list[PlutoChannelConfig] = [PlutoChannelConfig(channel=1)]

    _validate_channels = field_validator("channels")(_no_duplicate_channels)


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
    pluto_margins_db: dict[int, float] = {}  # keyed by 0-based index into PLUTO__RANGES

    # Spatial check (optional, on top of the temporal one above): also require a bin to
    # stand out from the median of its own hop before flagging, not just its own history —
    # catches a whole band being uniformly noisy without treating that as N different
    # signals. None (default) = disabled, same per-device/range override pattern as above.
    spatial_margin_db: float | None = None
    rtl_spatial_margins_db: dict[int, float] = {}
    hackrf_spatial_margins_db: dict[int, float] = {}
    pluto_spatial_margins_db: dict[int, float] = {}

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
    pluto_prominence_margins_db: dict[int, float] = {}


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
    # Which of plotting/{matplotlib,pyqtgraph}_backend.py renders the live plot (see
    # plotting/__init__.py's run() dispatcher) — matplotlib needs no extra dependency;
    # pyqtgraph needs the `pyqtgraph` package + a Qt binding installed (optional extra,
    # see pyproject.toml's `plot-pyqtgraph` group) but redraws meaningfully faster once
    # more than ~2 subplots are active (matplotlib's per-subplot blit overhead measured
    # live 2026-09 at ~150-285ms/frame).
    plot_backend: Literal["matplotlib", "pyqtgraph"] = "matplotlib"
    # Plot redraw interval in ms, one shared animation loop for the whole figure/window
    # (see plotting.run()) — not per-channel, so there's no way to redraw one backend's
    # plot faster than another's without a bigger animation redesign. 200ms suits
    # sweep-mode backends fine (hops arrive far slower than that anyway); a
    # PLUTO_STARE__* backend can produce hundreds of FFT windows/sec, so lowering this
    # (e.g. to 50) shows meaningfully more of that throughput instead of redrawing the
    # same single latest window 5x/sec.
    plot_refresh_ms: int = 200
    rtl: RTLSettings | None = None
    hackrf: HackRFSettings | None = None
    pluto: PlutoSettings | None = None
    pluto_stare: PlutoStareSettings | None = None
    kafka: KafkaSettings | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
        env_parse_none_str="auto",
    )

    @model_validator(mode="after")
    def _require_at_least_one_backend(self) -> "Settings":
        if self.rtl is None and self.hackrf is None and self.pluto is None and self.pluto_stare is None:
            raise ValueError(
                "At least one backend must be configured: set RTL__*, HACKRF__*, PLUTO__*, "
                "and/or PLUTO_STARE__* settings"
            )
        return self

    @model_validator(mode="after")
    def _pluto_sweep_and_stare_are_mutually_exclusive(self) -> "Settings":
        if self.pluto is not None and self.pluto_stare is not None:
            raise ValueError(
                "PLUTO__* (sweep) and PLUTO_STARE__* (stare) can't both be configured — "
                "they're two different modes for the same exclusive-access physical radio "
                "and can't run against it at the same time"
            )
        return self
