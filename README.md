# ew_freq_scanner

EW Frequency Scanner project — live spectrum + waterfall plots for RTL-SDR and/or HackRF, selected via configuration.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
# edit .env: fill in RTL__* and/or HACKRF__* — each section present is a backend that runs
# optionally also fill in KAFKA__* to stream flagged signals to an MSK cluster
```

### Installing on Raspberry Pi / other ARM boards

`numpy` and `matplotlib` publish prebuilt wheels for 64-bit ARM Linux (`aarch64`) but **not** for 32-bit ARM (`armv7l`) — check with `uname -m` before starting. If it says `armv7l`, plain `pip install` will fail trying to find/build packages that don't support 32-bit ARM at all; the practical fix is a 64-bit OS reinstall (Raspberry Pi OS 64-bit, any Pi 3/4/5), not fighting the build.

If you're on `aarch64`, `numpy`/`matplotlib` install fine as wheels, but **`confluent-kafka` has no ARM wheels at all** (any architecture) — pip will compile it from source, which needs its C dependency (`librdkafka`) present first:

```bash
sudo apt install librdkafka-dev build-essential python3-dev
pip install --upgrade pip   # old pip on Raspberry Pi OS may not recognize current wheel tags
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"     # confluent-kafka's build step can take several minutes on a Pi
```

If the Pi has 1GB RAM or less, the compile step can OOM — add swap first (`sudo dphys-swapfile swapoff && sudo nano /etc/dphys-swapfile` to raise `CONF_SWAPSIZE`, then `sudo dphys-swapfile setup && sudo dphys-swapfile swapon`) if `pip install` gets killed partway through.

## Configuration reference

All configuration lives in `.env` (see `.env.example`), read via `pydantic-settings`. There is no explicit backend selector — a backend runs if its section (`RTL__*` and/or `HACKRF__*`) is present; at least one must be. `KAFKA__*` is independent of that and fully optional (see [Kafka streaming](#kafka-streaming-kafka__-optional) below).

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
| `HACKRF__BIN_WIDTH`  | int (Hz)  | — (required) | FFT bin width (frequency resolution), passed to `hackrf_sweep`'s single `-w` flag. Shared by every range below — unlike RTL, where each device gets its own `bin_width` — because one physical HackRF runs one `hackrf_sweep` process covering all configured ranges together; the tool only takes one `-w` value for that whole process. |
| `HACKRF__RANGES`     | JSON list | — (required) | One or more frequency ranges, all swept by that single `hackrf_sweep` process. Each object:                                                           |

`HACKRF__RANGES` entry fields:

| Field        | Type                        | Meaning                                                                                                                                                                                                                                    |
| ------------ | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `freq_start` | float (MHz)                 | Lower edge of this range.                                                                                                                                                                                                                  |
| `freq_stop`  | float (MHz)                 | Upper edge of this range.                                                                                                                                                                                                                  |
| `edge_trim`  | float (0-1), default `0.05` | Fraction of each hop's bins manually dropped from each edge as unreliable roll-off data. Unlike `rtl_power`, `hackrf_sweep` has no native crop flag, so this is done in post-processing (`parsing.trim_edges()`), independently per range. |

Both list fields are single-line JSON strings — see `.env.example` for the exact syntax.

**A range narrower than ~20MHz still gets fully swept internally.** `hackrf_sweep` has a hard minimum sweep segment width tied to its sample rate (20MHz by default) — request something narrower (e.g. `freq_start: 106, freq_stop: 107`) and it silently widens the actual sweep to a full segment starting at `freq_start` (in that example, 106-126 MHz — check its own stdout, "Sweeping from X MHz to Y MHz", to see exactly what it did). `freqscan` still only publishes/plots the sub-slice you actually configured (bins outside `[freq_start, freq_stop)` are discarded after demuxing, in `HackRFBackend._run()`) — but the hardware is still spending sweep time on the wider segment behind the scenes, so a narrow range doesn't get you a faster update rate for that slice.

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

### Kafka streaming (`KAFKA__*`, optional)

If this section is present, `KAFKA__ENABLED` is true, *and* `freqscan` was launched with `--kafka_publisher`, bins whose reading rises above their own adaptive noise floor are published to an MSK cluster — everything else keeps running as pure local plotting, with zero extra overhead, if any of those three isn't true. The CLI flag is a separate, explicit opt-in on top of `.env`: it means having `KAFKA__*` configured never sends data anywhere unless you ask for it on that specific run.

| Variable                    | Type            | Default             | Meaning                                                                                                                                                                             |
| ---------------------------- | --------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `KAFKA__ENABLED`             | bool            | `true`               | Set to `false` to keep the rest of this section filled in but skip streaming entirely, without deleting anything.                                                                                                              |
| `KAFKA__BOOTSTRAP_SERVERS`   | string           | — (required)          | Comma-separated `host:9098` broker pairs (SASL/IAM public endpoint). From `terraform output bootstrap_brokers_public_sasl_iam` — see [terraform/README.md](terraform/README.md).                                              |
| `KAFKA__TOPIC`                | string           | `freqscan.signals`    | Kafka topic all flagged-signal messages are published to, keyed by channel label.                                                                                                                                              |
| `KAFKA__METADATA_TOPIC`       | string           | `freqscan.signals.metadata` | One-time per-channel grid layout (see "How it works" below), published once at startup. Must be provisioned with the same partition count as `KAFKA__TOPIC`.                                                            |
| `KAFKA__REGION`               | string           | `eu-central-1`        | AWS region the MSK cluster is in — used to sign SASL/IAM auth tokens.                                                                                                                                                           |
| `KAFKA__BASELINE_WINDOW`      | int              | `50`                  | Number of past readings kept per frequency bin to compute its adaptive noise-floor baseline. Independent of `WATERFALL_ROWS` — this tracks per-bin history for detection, not the UI's per-channel waterfall snapshots.        |
| `KAFKA__SIGNAL_MARGIN_DB`     | float            | `8.0`                 | Default margin, in dB above a bin's baseline, before it's flagged as signal and published. Applies to any RTL device id / HackRF range index not listed in the two maps below.                                                 |
| `KAFKA__RTL_MARGINS_DB`       | JSON object      | `{}`                  | Per-device margin overrides, keyed by `RTL__DEVICES`' `"id"` (as a string key in the JSON, e.g. `{"0": 10.0}`).                                                                                                                 |
| `KAFKA__HACKRF_MARGINS_DB`    | JSON object      | `{}`                  | Per-range margin overrides, keyed by 0-based position in `HACKRF__RANGES` (e.g. `{"1": 6.0}`).                                                                                                                                  |
| `KAFKA__SPATIAL_MARGIN_DB`    | float or unset   | unset (disabled)      | Optional second check: also require a bin to beat the median of *all* bins in that same hop by this many dB, on top of beating its own history — see "How it works" below. Unset/`None` = disabled, pure temporal behavior (the historical default). Applies to any RTL device id / HackRF range index not listed in the two maps below. |
| `KAFKA__RTL_SPATIAL_MARGINS_DB`   | JSON object  | `{}`                  | Per-device overrides for the spatial margin, same keying as `KAFKA__RTL_MARGINS_DB`.                                                                                                                                            |
| `KAFKA__HACKRF_SPATIAL_MARGINS_DB` | JSON object | `{}`                  | Per-range overrides for the spatial margin, same keying as `KAFKA__HACKRF_MARGINS_DB`.                                                                                                                                          |
| `KAFKA__PROMINENCE_MARGIN_DB` | float or unset | unset (disabled)  | Optional third, independent check: flags a bin regardless of its own history if it beats the median of its own hop by this many dB — see "How it works" below. Unset/`None` = disabled. Applies to any RTL device id / HackRF range index not listed in the two maps below. |
| `KAFKA__RTL_PROMINENCE_MARGINS_DB`   | JSON object | `{}`                  | Per-device overrides for the prominence margin, same keying as `KAFKA__RTL_MARGINS_DB`.                                                                                                                                        |
| `KAFKA__HACKRF_PROMINENCE_MARGINS_DB` | JSON object | `{}`                 | Per-range overrides for the prominence margin, same keying as `KAFKA__HACKRF_MARGINS_DB`.                                                                                                                                      |

How it works:

- **Adaptive per-bin baseline, not a fixed threshold.** Each frequency bin tracks its own rolling window of past readings (`KAFKA__BASELINE_WINDOW`). A bin is only ever flagged once that window has filled at least once (cold start) — until then, nothing from that bin is published.
- **Optional spatial check, on top of the per-bin one.** A single per-bin baseline can't tell "this bin is genuinely elevated" apart from "this whole band is just loud right now" — a band that's uniformly noisy has every bin's own baseline adapt to that noise too, so ordinary reading-to-reading jitter keeps crossing a fixed margin. If `KAFKA__SPATIAL_MARGIN_DB` (or a per-device/range override) is set, a bin must *also* beat the median power of every bin in that same hop by that margin — it's an AND with the per-bin check above, not a replacement, so it can only suppress a bin the per-bin check already flagged, never flag one it didn't. A uniformly loud band has every bin sitting near that hop's own median, so nothing stands out; a genuine narrowband signal riding on top of a noisy floor still clearly stands out from its neighbors in that same sweep. Tune the margin empirically per band — how "spread out" real activity is varies a lot by frequency range.
- **Optional prominence check, independent of the per-bin one — an OR, not a refinement.** The per-bin baseline only ever catches *new* activity — a signal that's always there and always strong (a steady FM broadcast carrier, say) trains straight into the baseline and never looks new again, no matter how strong it is. If `KAFKA__PROMINENCE_MARGIN_DB` (or a per-device/range override) is set, a bin is flagged if it beats its own hop's median by this margin, **regardless of its own history** — this is the setting for "I don't care if it's new, I care that it's clearly there right now." Use a *bigger* margin than `KAFKA__SPATIAL_MARGIN_DB` — a real narrowband peak (tens of dB above the floor) clears a generous margin easily, while generically noisy broadband bins (which sit close to their own hop's median by definition) don't.
- **One message per hop, not per bin or on a timer.** All bins flagged within a single sweep hop are batched into one JSON message and published immediately as that hop's data arrives — decoupled entirely from the plot's 200ms redraw cadence, so this runs the same whether or not the plot window is open.
- **Message format is JSON** — `{"channel": "...", "timestamp": <epoch seconds>, "bins": [{"freq_hz": ..., "power_dbm": ...}, ...]}` — chosen deliberately over a more compact format like Protobuf for now, since the downstream consumer/schema isn't finalized yet and JSON keeps messages readable with a plain console consumer while iterating.
- **Each channel gets its own fixed partition**, assigned deterministically at startup (`build_backend()`, not left to Kafka's default key-hash partitioner): channels are numbered 0..N-1 in the same RTL-then-HackRF order used everywhere else (`RTL__DEVICES` in order, then `HACKRF__RANGES` in order), one partition per channel. **Both `KAFKA__TOPIC` and `KAFKA__METADATA_TOPIC` must be provisioned with at least as many partitions as total channels** (`len(RTL__DEVICES) + len(HACKRF__RANGES)`) — if either has fewer, messages for the missing partitions fail to deliver silently (dropped, not retried elsewhere).
- **Channel metadata, published once at startup, on a separate topic.** Alongside the per-hop signal messages, `build_backend()` also publishes one message per channel to `KAFKA__METADATA_TOPIC` (same partition as that channel's signals): `{"channel": "...", "freq_start_hz": ..., "freq_stop_hz": ..., "bin_width_hz": ..., "n_bins": ...}`. `n_bins` is the channel's *true* total bin count from config, independent of how many (if any) ever actually get flagged. This exists so `python -m freqscan.kafka_viewer` can build a correctly-sized, stable grid up front instead of guessing one from whatever happens to get flagged over time — see the viewer section below for why that matters.
- **Auth is SASL/IAM**, via `aws-msk-iam-sasl-signer-python` — this machine's AWS credentials (same ones set up for Terraform, see [terraform/SETUP.md](terraform/SETUP.md)) are used to sign broker connections, not a separate Kafka username/password.
- Provisioning the MSK cluster itself is a separate Terraform step — see [terraform/README.md](terraform/README.md).

## Running

`freqscan` requires at least one of two flags — it refuses to start with neither, since running with neither would mean it does nothing observable at all:

```bash
python -m freqscan --plot                       # local plot only, no Kafka
python -m freqscan --kafka_publisher             # stream to Kafka only, no plot window (prints offsets every 10s)
python -m freqscan --plot --kafka_publisher      # both at once: watch locally while also streaming
```

Requires the underlying SDR CLI tool(s) on PATH and hardware attached: `rtl_power` for the RTL-SDR section, `hackrf_sweep` for the HackRF section. Both can run at once if both sections are configured.

### `--plot`

Opens the live spectrum + waterfall plot window (`plotting.run()`). Doesn't touch Kafka by itself, even if `KAFKA__*` is configured in `.env` — add `--kafka_publisher` too if you also want to stream while watching.

### `--kafka_publisher`

Publishes flagged signal bins to Kafka. This is a separate, explicit opt-in per run, on top of `KAFKA__*` being present in `.env` and `KAFKA__ENABLED=true` — `freqscan` never sends data to Kafka without this flag, regardless of `.env`. Conversely, passing it with no `KAFKA__*` section configured at all is treated as a misconfiguration and fails fast with an error, rather than silently doing nothing. Requires valid AWS credentials for the configured region (e.g. via `aws configure`, see [terraform/SETUP.md](terraform/SETUP.md)) — `freqscan` will fail to connect otherwise.

Without `--plot`, `freqscan` runs "headless": no plot window, and instead of drawing it prints a startup confirmation immediately, then each partition's last delivered offset to stdout every 10 seconds, e.g.:

```
freqscan: sending to Kafka started; reporting offsets every 10s
freqscan: kafka offsets: {0: 337476}
```

Stop with Ctrl+C.

### Kafka viewer (`python -m freqscan.kafka_viewer`)

```bash
python -m freqscan.kafka_viewer
# or, to keep more/fewer waterfall rows than the default 100:
python -m freqscan.kafka_viewer --waterfall-rows 200
```

Draws the same spectrum + waterfall plot as `python -m freqscan`, but reads from the Kafka topic instead of live hardware — for watching signals on a machine with no SDR attached at all (no `rtl_power`/`hackrf_sweep` needed). Reads the same `KAFKA__*` variables from `.env` as the producer (`KAFKA__BOOTSTRAP_SERVERS`, `KAFKA__TOPIC`, `KAFKA__METADATA_TOPIC`, etc. — see [Kafka streaming](#kafka-streaming-kafka__-optional) above), but doesn't need `RTL__*`/`HACKRF__*` configured at all.

On startup, it prints `waiting for channel metadata...` and reads `KAFKA__METADATA_TOPIC` from the beginning to learn each channel's true grid (label, frequency range, bin count) before doing anything else — up to `METADATA_TIMEOUT` (10s); if any channel's metadata never shows up (producer never ran with `--kafka_publisher`, or the topics aren't provisioned to match), it fails with a clear error rather than guessing. Once metadata arrives it prints `got metadata for N channel(s)`, pre-fills every channel's full bin grid (as empty/no-data), and only then starts consuming the live signals topic — showing messages published *after* it starts, not the topic's full backlog.

This metadata-first approach exists because of a real bug in an earlier version: without knowing the true grid up front, the viewer had to discover it lazily from whatever bins happened to get flagged over time (the topic only ever carries bins that cross the noise-floor margin, not every raw bin). The known-bin count kept growing for a while after startup, and since the waterfall image's column positions depend on the *total* column count, every newly-discovered bin anywhere in the range reshuffled the on-screen position of every already-drawn row — visible as the whole waterfall shifting, then more generally reflowing/changing height, for a while after startup. Reading real metadata up front fixes this at the root: the grid is fixed and correct from frame one.

Since the topic only ever carries bins that were flagged as above their noise floor, the viewer's plot is inherently sparser than the producer's own local plot, which draws every raw bin — that's expected, not a bug.

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
