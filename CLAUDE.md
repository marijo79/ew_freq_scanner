# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

EW Frequency Scanner — a single configurable program (`src/freqscan/`) that drives an RTL-SDR and/or a HackRF to produce live spectrum + waterfall plots via matplotlib. Which SDR backend(s) run is chosen at startup by config, not by which script you invoke — both can run at once, sharing one plot window.

## Setup and running

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env   # then fill in RTL__* and/or HACKRF__* — each section present is a backend that runs
python -m freqscan      # requires rtl_power and/or hackrf_sweep on PATH + hardware attached, matching whichever sections are configured

pytest -q
```

## Configuration

`src/freqscan/config.py` defines a `pydantic_settings.BaseSettings` (`Settings`), loaded from `.env` (see `.env.example`). There is no explicit backend selector — presence of the `RTL__*` / `HACKRF__*` nested section (using `__` as pydantic-settings' nested delimiter, e.g. `HACKRF__LNA_GAIN`) determines whether that backend runs; list fields (`RTL__DEVICES`, `HACKRF__RANGES`) are JSON-encoded strings deserialized into `DeviceConfig`/`RangeConfig` lists. A model validator enforces that at least one of the two sections is present.

## Architecture

1. **Config → backend**: `sdr/build_backend(settings)` constructs an `RTLBackend` and/or `HackRFBackend` (`src/freqscan/sdr/{rtl,hackrf}.py`) for whichever sections are configured. If both are present, they're wrapped in a `CompositeBackend` (`sdr/base.py`) that runs them concurrently and concatenates their channel lists (RTL channels first) — `plotting.py`/`__main__.py` only ever see one `SDRBackend`, single or composite, so they're unaware of how many real backends are underneath. All three implement the `SDRBackend` ABC (`sdr/base.py`): a list of `Channel`s (one per plotted column), `start()`/`stop()`, and shared startup-failure tracking (`report_error()` / `wait_ready()`, see below).
2. **Producer thread(s)**: each backend launches the underlying SDR CLI tool (`rtl_power` or `hackrf_sweep`) as a subprocess and parses its CSV stdout line by line via the shared `parsing.parse_sweep_line()` (`hz_low, hz_step, ..., power, power, ...`). `RTLBackend` runs one subprocess/thread per configured device (1:1 with a channel); `HackRFBackend` runs a single subprocess covering all configured ranges in one sweep and demuxes each parsed line to the right channel by frequency (`_range_index_for`). Parsed `{freq_hz: power_dbm}` pairs are merged into each channel's `SweepState.sweep` dict, guarded by `SweepState.lock`.
3. **Edge/roll-off handling differs by backend** — SDR filter roll-off makes the edge bins of every sweep hop unreliable, and each tool is handled with what it actually supports:
   - `RTLBackend` passes `rtl_power`'s own `-c <edge_trim*100>%` crop flag (recommended 20%-50%; see `rtl_power --help`). `rtl_power` widens each hop's capture window internally before cropping, so adjacent hops tile without gaps — do **not** re-add manual post-hoc trimming here, that was the original bug (created real frequency gaps because hops weren't widened to compensate).
   - `HackRFBackend` has no equivalent native flag, so it still uses `parsing.trim_edges()` to manually drop a fraction of bins from each edge of every hop (per-range `edge_trim`).
4. **Startup-failure detection**: `__main__.main()` calls `backend.start()`, then `backend.wait_ready(STARTUP_GRACE_PERIOD)` before opening any plot window. Each backend thread calls `self.report_error()` (sets a shared `threading.Event`) when its subprocess fails to launch (`OSError`, e.g. binary not on PATH) or exits with a positive return code (e.g. device not found) — `wait_ready()` returns `False` if that fired within the grace period, and `main()` stops the backend and exits(1) instead of opening a blank window.
5. **Consumer (main thread)**: `plotting.run()` builds a 2-row grid (spectrum on top, waterfall below), one column per channel, dark-themed via `style_axes()`, with `add_freq_hint()` wiring a mouse-move handler on the waterfall axes. `matplotlib.animation.FuncAnimation` polls channel states on a timer (`interval=200ms`) via `update_channel()`, which redraws the spectrum line and pushes into each channel's `deque(maxlen=waterfall_rows)` waterfall history.

Both backends share parsing and plotting code — subprocess command construction, edge-trimming strategy, and channel demuxing differ.

## Testing

`tests/test_parsing.py` and `tests/test_config.py` cover the two pure/testable layers: line parsing + edge trimming, and config loading/validation (including the no-backend-configured error). `tests/test_sdr_build.py` covers `build_backend()` dispatch and `CompositeBackend` channel composition — construction only, no subprocess/thread spawning. Plotting/animation/thread glue is intentionally not unit tested — it's matplotlib UI and thread orchestration, not meaningfully testable without heavy mocking.
