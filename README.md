# ew_freq_scanner

EW Frequency Scanner project — live spectrum + waterfall plots for RTL-SDR and/or HackRF, selected via configuration.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
# edit .env: fill in RTL__* and/or HACKRF__* — each section present is a backend that runs
```

## Configuration reference

All configuration lives in `.env` (see `.env.example`), read via `pydantic-settings`. There is no explicit backend selector — a backend runs if its section (`RTL__*` and/or `HACKRF__*`) is present; at least one must be.

### Shared

| Variable         | Type | Default | Meaning                                                                                   |
| ---------------- | ---- | ------- | ----------------------------------------------------------------------------------------- |
| `WATERFALL_ROWS` | int  | `100`   | Number of past sweeps kept per channel for the waterfall display (older rows scroll off). |

### RTL-SDR (`RTL__*`, present ⇒ this backend runs)

| Variable        | Type            | Default      | Meaning                                                                                                                                                                           |
| --------------- | --------------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RTL__GAIN`     | int or `"auto"` | `50`         | Tuner gain in dB, passed to `rtl_power -g`. Set to `auto` to omit `-g` entirely and let the tuner auto-gain instead. See [RTL-SDR gain explained](#rtl-sdr-gain-explained) below. |
| `RTL__INTERVAL` | int             | `1`          | Per-hop integration/averaging time in seconds, passed to `rtl_power -i`. See [Integration interval: RTL vs HackRF](#integration-interval-rtl-vs-hackrf) below.                    |
| `RTL__DEVICES`  | JSON list       | — (required) | One entry per physical RTL-SDR dongle, run concurrently. Each object:                                                                                                             |

`RTL__DEVICES` entry fields:

| Field        | Type        | Default | Meaning                                                                                                                                                                                                                                                                                                                           |
| ------------ | ----------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`         | int         | —       | Device index as reported by `rtl_test`/`rtl_power` (see "Finding RTL-SDR device IDs" below).                                                                                                                                                                                                                                      |
| `freq_start` | string      | —       | Lower edge of this device's swept range (e.g. `"80M"`), passed straight to `rtl_power`.                                                                                                                                                                                                                                           |
| `freq_stop`  | string      | —       | Upper edge of this device's swept range (e.g. `"120M"`).                                                                                                                                                                                                                                                                          |
| `bin_width`  | int (Hz)    | `10000` | Requested FFT bin size in Hz, passed to `rtl_power -f start:stop:bin_width`. The tool may use a smaller, more convenient bin size than requested. Same name/units as HackRF's `bin_width`, but set per-device here rather than shared across all devices.                                                                         |
| `edge_trim`  | float (0-1) | `0.15`  | Fraction of each hop's FFT bins to crop as unreliable edge/roll-off data, passed to `rtl_power`'s own `-c` crop flag (e.g. `0.3` → `-c 30%`). `rtl_power` recommends 20%-50%; it widens each hop's capture internally so cropping doesn't create gaps between hops. Set per-device here, matching HackRF's per-range `edge_trim`. |

### RTL-SDR gain explained

Unlike HackRF, `rtl_power` exposes only a **single** gain parameter (`RTL__GAIN`, `-g`) — there's no separate LNA/mixer/VGA control to set individually.

This isn't a limitation of `freqscan`'s config; it's how the tool itself works. RTL-SDR dongles use a tuner chip (typically a Rafael Micro R820T/R820T2/R828D) that does have multiple internal analog gain stages, same idea as HackRF's amp/LNA/VGA — but `librtlsdr` (the driver `rtl_power` is built on) doesn't expose them separately. Instead it maintains a fixed table of discrete *overall* gain values the tuner supports (for the R820T2, roughly 0, 0.9, 1.4, 2.7, ... up to ~49.6 dB), and `-g` just snaps to whichever supported value is closest to what you request.

`RTL__GAIN` can also be set to `auto` instead of a number, which omits `-g` from the `rtl_power` command entirely — this lets the tuner's own AGC pick a gain automatically rather than using a fixed value. Useful when signal strength varies a lot across a swept range and no single fixed gain works well for all of it, at the cost of less predictable/repeatable readings than a fixed gain.

### Integration interval: RTL vs HackRF

`RTL__INTERVAL` (`rtl_power -i`) is a genuine noise-reduction knob: for each hop, `rtl_power` integrates/averages samples over that many seconds before reporting one power reading per bin, then moves to the next hop. More integration time gives smoother, more reliable readings at the cost of slower updates — it's also why RTL, in practice, produces far fewer lines/sec than HackRF.

HackRF has **no equivalent setting** — `hackrf_sweep`'s `--help` output has no integration/averaging flag of any kind. It continuously fires single-shot FFTs as fast as the hardware allows and reports every one of them raw and un-averaged, with nothing in the tool throttling or smoothing that output.

Practically: RTL's readings arrive already averaged/smoothed, while HackRF's readings are raw single-shot snapshots with more inherent bin-to-bin noise variance. This also means RTL is deliberately paced by `RTL__INTERVAL`, while HackRF isn't paced by anything in its own CLI — it always runs at its own hardware-limited maximum sweep rate.

### HackRF (`HACKRF__*`, present ⇒ this backend runs)

| Variable             | Type      | Default      | Meaning                                                                                                                                               |
| -------------------- | --------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HACKRF__LNA_GAIN`   | int       | `32`         | RX LNA (IF) gain, 0-40dB in 8dB steps, passed to `hackrf_sweep -l`. See [HackRF gain stages explained](#hackrf-gain-stages-explained) below.          |
| `HACKRF__VGA_GAIN`   | int       | `20`         | RX VGA (baseband) gain, 0-62dB in 2dB steps, passed to `hackrf_sweep -g`. See [HackRF gain stages explained](#hackrf-gain-stages-explained) below.    |
| `HACKRF__AMP_ENABLE` | bool      | `false`      | Enables the fixed +14dB front-end RF amplifier, passed to `hackrf_sweep -a`. See [HackRF gain stages explained](#hackrf-gain-stages-explained) below. |
| `HACKRF__RANGES`     | JSON list | — (required) | One or more frequency ranges, all swept by a single `hackrf_sweep` process. Each object:                                                              |

`HACKRF__RANGES` entry fields:

| Field        | Type                        | Meaning                                                                                                                                                                                                                                    |
| ------------ | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `freq_start` | float (MHz)                 | Lower edge of this range.                                                                                                                                                                                                                  |
| `freq_stop`  | float (MHz)                 | Upper edge of this range.                                                                                                                                                                                                                  |
| `bin_width`  | int (Hz)                    | FFT bin width (frequency resolution), passed to `hackrf_sweep -w`.                                                                                                                                                                         |
| `edge_trim`  | float (0-1), default `0.05` | Fraction of each hop's bins manually dropped from each edge as unreliable roll-off data. Unlike `rtl_power`, `hackrf_sweep` has no native crop flag, so this is done in post-processing (`parsing.trim_edges()`), independently per range. |

Both list fields are single-line JSON strings — see `.env.example` for the exact syntax.

### HackRF gain stages explained

HackRF One's receive path has three gain stages, in signal order:

1. **RF amp** (`HACKRF__AMP_ENABLE`) — a fixed +14 dB front-end amplifier, on/off only.
2. **LNA gain** (`HACKRF__LNA_GAIN`) — 0-40 dB in 8 dB steps, applied early inside the transceiver chip's IF stage (HackRF's tooling calls it "LNA", though it's technically an IF gain stage rather than a classic front-end LNA). Because it acts early in the chain, it has the biggest effect on overall sensitivity/noise floor.
3. **VGA gain** (`HACKRF__VGA_GAIN`) — 0-62 dB in 2 dB steps, a baseband variable-gain amplifier applied after downconversion, right before the ADC. Its job is to fit the signal into the ADC's input range, not to pull weak signals out of the noise.

Getting these wrong shows up as:

- **LNA too low** — weak/distant signals get buried in the noise floor, since later gain stages amplify noise right along with them.
- **LNA too high** — strong nearby signals can overload the front-end, causing intermodulation distortion/spurs that show up as fake signals elsewhere in the spectrum.
- **VGA too low** — the ADC only sees a few bits of resolution, so real signal is lost in quantization noise even if LNA gain was fine.
- **VGA too high** — clipping, visible as flat-topped peaks in the spectrum plot — a clear sign to back the VGA gain down.

Practical tuning: raise LNA gain first for weak-signal sensitivity, watching for the overload/spurs symptom above; use VGA gain to fit the resulting signal into the ADC without clipping. Combined, LNA + VGA give up to 102 dB of adjustable gain. `freqscan`'s defaults (`LNA_GAIN=32`, `VGA_GAIN=20`, `AMP_ENABLE=false`) are moderate starting points, not tuned for any specific band.

## Running

```bash
python -m freqscan
```

Requires the underlying SDR CLI tool(s) on PATH and hardware attached: `rtl_power` for the RTL-SDR section, `hackrf_sweep` for the HackRF section. Both can run at once if both sections are configured.

### Finding RTL-SDR device IDs

Each entry in `RTL__DEVICES` needs an `id` matching a physical dongle's device index. To see which index maps to which dongle, run `rtl_test` (bundled with `librtlsdr`, installed alongside `rtl_power`) — it enumerates attached devices and doesn't exit on its own (Ctrl+C to stop):

```bash
rtl_test
```

```
Found 3 device(s):
  0:  RTLSDRBlog, Blog V4, SN: RTL_03
  1:  Nooelec, NESDR SMArt v5, SN: RTL_01
  2:  RTLSDRBlog, Blog V4, SN: RTL_02
```

The leading number is the device index to use as `id`. `rtl_power`'s own startup output lists the same enumeration if you'd rather not run a separate tool.

## Testing

```bash
pytest -q
```
