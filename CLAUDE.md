# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

EW Frequency Scanner — a single configurable program (`src/freqscan/`) that drives either an RTL-SDR or a HackRF to produce live spectrum + waterfall plots via matplotlib. Which SDR backend runs is chosen at startup by config, not by which script you invoke.

## Setup and running

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env   # then set SDR=rtl or SDR=hackrf and fill in the matching section
python -m freqscan      # requires rtl_power (SDR=rtl) or hackrf_sweep (SDR=hackrf) on PATH + hardware attached

pytest -q
```

## Configuration

`src/freqscan/config.py` defines a `pydantic_settings.BaseSettings` (`Settings`), loaded from `.env` (see `.env.example`). Top-level `SDR=rtl|hackrf` selects the backend; nested settings use `__` as the delimiter (e.g. `HACKRF__LNA_GAIN`), and list fields (`RTL__DEVICES`, `HACKRF__RANGES`) are JSON-encoded strings deserialized into `DeviceConfig`/`RangeConfig` lists. A model validator enforces that the section matching `SDR` is actually present.

## Architecture

1. **Config → backend**: `sdr/build_backend(settings)` dispatches on `settings.sdr` to construct either `RTLBackend` or `HackRFBackend` (`src/freqscan/sdr/{rtl,hackrf}.py`), both implementing the `SDRBackend` ABC (`sdr/base.py`): a list of `Channel`s (one per plotted column) and `start()`/`stop()`.
2. **Producer thread(s)**: each backend launches the underlying SDR CLI tool (`rtl_power` or `hackrf_sweep`) as a subprocess and parses its CSV stdout line by line via the shared `parsing.parse_sweep_line()` (`hz_low, hz_step, ..., power, power, ...`). `RTLBackend` runs one subprocess/thread per configured device (1:1 with a channel); `HackRFBackend` runs a single subprocess covering all configured ranges in one sweep and demuxes each parsed line to the right channel by frequency (`_range_index_for`). Parsed `{freq_hz: power_dbm}` pairs are merged into each channel's `SweepState.sweep` dict, guarded by `SweepState.lock`.
3. **Edge trimming**: `parsing.trim_edges()` drops a fraction of bins at each edge of every sweep hop (per-backend/per-range `edge_trim`) because SDR filter roll-off makes edge bins unreliable.
4. **Consumer (main thread)**: `plotting.run()` builds a 2-row grid (spectrum on top, waterfall below), one column per channel, dark-themed via `style_axes()`, with `add_freq_hint()` wiring a mouse-move handler on the waterfall axes. `matplotlib.animation.FuncAnimation` polls channel states on a timer (`interval=200ms`) via `update_channel()`, which redraws the spectrum line and pushes into each channel's `deque(maxlen=waterfall_rows)` waterfall history.

Both backends share all parsing, edge-trimming, and plotting code — only subprocess command construction and channel demuxing differ.

## Testing

`tests/test_parsing.py` and `tests/test_config.py` cover the two pure/testable layers: line parsing + edge trimming, and config loading/validation (including the missing-section error). Plotting/animation/thread glue is intentionally not unit tested — it's matplotlib UI and thread orchestration, not meaningfully testable without heavy mocking.
