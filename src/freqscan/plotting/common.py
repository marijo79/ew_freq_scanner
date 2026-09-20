import time

import numpy as np

from freqscan.sdr.base import Channel

# Fallback spectrum y-axis range, only used if a channel has no real data yet by the
# time initial_ylim()'s short wait times out. Both plot backends fix axis limits up
# front rather than auto-scaling every frame (matplotlib needs this for blit=True;
# pyqtgraph's own per-frame auto-range is also real redraw-cost, not free, so fixing it
# once matters there too) -- see initial_ylim(). A single hardcoded range can't fit
# every backend's real scale though (RTL/HackRF's calibrated dBm output and Pluto's own
# uncalibrated relative-dB FFT scale span very differently), hence peeking at real data.
FALLBACK_SPECTRUM_YLIM = (-140, 40)

# Spectrum/waterfall styling, matched live 2026-09-14 against a screenshot of SDR++'s
# actual amplitude+waterfall panels (connected to the same Pluto over IP) rather than
# generic GQRX-family assumptions -- see conversation. Shared here so both plot backends
# stay visually identical.
SPECTRUM_LINE_COLOR = "#66d9ef"  # pale cyan live trace, not lime
SPECTRUM_FILL_ALPHA = 0.18  # subtle dark-teal fill under the trace, not a bright glow
WATERFALL_COLORMAP = "turbo"  # blue -> cyan -> yellow -> red, not inferno's black/purple/orange
SPECTRUM_YTICK_STEP_DB = 10  # clean 10dB-step gridlines with labels, matching SDR++'s

# Display-only exponential moving average applied to each bin, across real captures (not
# redraw ticks -- see channel_snapshot()'s raw_unchanged gating). Confirmed live
# 2026-09-14 comparing a real Pluto stare feed against SDR++: freqscan was plotting
# completely raw, single-shot FFT snapshots, which on unaveraged noise swing wildly
# bin-to-bin (~-30 to +40 dB in the same trace) -- both a spiky, hard-to-read amplitude
# line and, since the waterfall is just this same data color-mapped over time, a grainy
# waterfall with no clean separation between the noise floor and real signals. SDR++
# (like most spectrum tools) averages several FFTs before displaying either. This is a
# purely cosmetic smoothing of what's *shown*; it doesn't touch KafkaSignalPublisher's
# own noise-floor detection, which has its own separate, independent baseline logic.
# alpha is the weight given to each new real capture -- 1/alpha is roughly the effective
# number of captures averaged together, and real-world convergence lag is
# (1/alpha)/captures-per-sec, not a fixed number of frames -- 0.25 (~4 captures) still
# looked visibly speckled against a live Pluto stare feed capturing ~100+/sec (confirmed
# live 2026-09-14), so lowered to ~12 captures here, which is still under 0.15s of lag at
# that capture rate. RTL/HackRF sweep-mode channels revisit any one bin far less often
# (once per full sweep, seconds to minutes apart), so the same alpha means much slower
# real-world convergence there -- acceptable since their trace wasn't the reported
# problem and their power values are already less noisy to begin with (calibrated dBm
# output vs. Pluto's raw uncalibrated FFT bins).
DISPLAY_AVG_ALPHA = 0.08


def initial_ylim(channel: Channel, margin_db: float = 15.0, timeout: float = 2.0) -> tuple[float, float]:
    """Peek at a channel's first real data to pick a tightly-fitted, fixed y-range,
    instead of one hardcoded guess that can't fit every backend's real scale. Falls
    back to FALLBACK_SPECTRUM_YLIM if no data arrives within `timeout` (backend.
    wait_ready() already ran before this, so data has usually already arrived by the
    time this is called -- the wait below is just a safety margin, not the common case).

    Filters out NaN before checking for "any data yet" -- required for
    KafkaConsumerBackend, whose state.sweep is pre-populated with NaN for every
    declared grid bin at construction (see its own docstring), so a plain truthiness/
    non-empty check on the dict passes immediately even with zero real (flagged) bins
    received yet, before any real signal has arrived over Kafka. Found live 2026-09-20
    running the viewer against a real, sparse (flagged-bins-only) topic: min/max over an
    all-NaN list is NaN, which crashed pyqtgraph_backend's setYRange(nan, nan) outright
    instead of waiting/falling back like this was meant to. A no-op for every other
    backend, whose state.sweep only ever holds real values once a hop actually lands."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with channel.state.lock:
            powers = [p for p in channel.state.sweep.values() if not np.isnan(p)]
        if powers:
            return (min(powers) - margin_db, max(powers) + margin_db)
        time.sleep(0.05)
    return FALLBACK_SPECTRUM_YLIM


def initial_waterfall_clim(
    channel: Channel, floor_percentile: float = 10.0, span_db: float = 40.0,
    floor_margin_db: float = 3.0, timeout: float = 2.0,
) -> tuple[float, float]:
    """Peek at a channel's first real data to pick a fixed waterfall color range anchored
    to the actual noise floor -- deliberately NOT initial_ylim()'s range, which pads a
    generous +-15dB on top of the line's own min/max for line headroom. That's fine for a
    line, but wrong for a color-mapped image: it pushes the noise floor into the middle
    of the colormap instead of near its cold end, which is exactly the "washed out, too
    green" symptom reported live 2026-09-14 comparing a real Pluto stare feed (turbo
    colormap) against SDR++'s fixed Min/Max waterfall sliders.

    floor_percentile picks a robust "typical noise" value (not the literal min, which one
    unusually quiet dip could drag down further than is representative); span_db is the
    dynamic range stretched across the rest of the colormap above that floor. A signal
    stronger than floor + span_db simply clips to the colormap's hottest color, which is
    normal and expected -- SDR++'s own fixed sliders clip the same way.

    Same NaN-filtering as initial_ylim() and for the same reason -- see its docstring."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with channel.state.lock:
            powers = [p for p in channel.state.sweep.values() if not np.isnan(p)]
        if powers:
            floor = float(np.percentile(powers, floor_percentile))
            return (floor - floor_margin_db, floor + span_db)
        time.sleep(0.05)
    return FALLBACK_SPECTRUM_YLIM


def initial_freq_extent(channel: Channel, timeout: float = 2.0) -> tuple[float, float]:
    """Peek at a channel's first real data to find the actual min/max frequency (MHz)
    real bins ever cover -- deliberately NOT always the same as channel.freq_start_mhz/
    freq_stop_mhz. For `PlutoStareBackend` specifically, that declared range is the
    WIDER pre-edge-trim capture span (`_pluto_stare_channel_grids()` in sdr/__init__.py
    sizes it from frequency +-sample_rate/2 before any trimming), while real captured
    bins only ever cover the narrower post-trim range -- scripts/pluto_stare.py's own
    trim_edges() call drops the rest before ever printing a CSV line, so those edge
    frequencies never reach `state.sweep` at all.

    Used to size the waterfall image's own extent/rect so it only ever colors where
    real data exists, instead of being stretched across the wider declared channel
    range and visually covering more of the frequency axis than the spectrum line's own
    real content does -- confirmed live 2026-09-14 as exactly this symptom on a
    narrowed (90-110MHz) Pluto stare view, right after fixing a separate
    spectrum/waterfall pixel-alignment bug made the mismatch obvious. For RTL/HackRF/
    Pluto-sweep and the Kafka viewer, real data converges to the same range as
    channel.freq_start_mhz/freq_stop_mhz anyway (their declared range is already
    clipped/published to match real achievable data -- see CLAUDE.md's sweep-mode
    per-hop clipping and the Kafka viewer's metadata-driven pre-population), so this is
    a safe, same-result substitution there, not a stare-mode-only special case."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with channel.state.lock:
            if channel.state.sweep:
                freqs_hz = channel.state.sweep.keys()
                return (min(freqs_hz) / 1e6, max(freqs_hz) / 1e6)
        time.sleep(0.05)
    return (channel.freq_start_mhz, channel.freq_stop_mhz)


def channel_snapshot(channel: Channel, waterfall_rows: int):
    """One channel's current frame: (mhz, powers, waterfall_matrix), or None if the
    channel has no data yet. `powers` and the waterfall_matrix's rows are the smoothed
    display average (see DISPLAY_AVG_ALPHA), not the raw instantaneous sweep.
    waterfall_matrix is (waterfall_rows, n_cols), newest row first, built from
    `channel.state.history` -- shared by both plot backends since it's pure data prep
    with no rendering-library dependency.

    Appends the current (smoothed) frame to `channel.state.history` as a side effect
    (this is the one and only place a frame gets recorded, regardless of which backend
    is drawing)."""
    state = channel.state
    with state.lock:
        if not state.sweep:
            return None
        freqs = sorted(state.sweep)
        powers = [state.sweep[f] for f in freqs]
        freqs_arr = np.asarray(freqs)
        powers_arr = np.asarray(powers)

        # Gates both the averaging update below and the history append further down --
        # the producer can be genuinely slower than the display's own redraw tick
        # (confirmed live 2026-09-04 with 2 concurrent Pluto stare channels sharing one
        # USB/IP link: one channel's real update rate measured ~1.6Hz against a
        # 20Hz/50ms display refresh, so 92 of 99 consecutive redraws were reading the
        # exact same still-unchanged state.sweep). Without this, a redraw tick that
        # finds no new raw data would still blend the same raw sample into the average
        # again, overweighting whichever capture happens to coincide with a slow
        # display tick purely by chance -- averaging must track real captures, not
        # redraw ticks.
        raw_unchanged = (
            state.last_raw_freqs is not None
            and state.last_raw_freqs.shape == freqs_arr.shape
            and np.array_equal(state.last_raw_freqs, freqs_arr)
            and np.array_equal(state.last_raw_powers, powers_arr)
        )
        state.last_raw_freqs = freqs_arr
        state.last_raw_powers = powers_arr

        if not raw_unchanged:
            # Realign the running average by frequency value on grid change, same
            # reasoning/approach as `history` below (and same fast path when the grid,
            # as usual, hasn't changed since last frame).
            if state.display_avg_freqs is None or not (
                state.display_avg_freqs.shape == freqs_arr.shape
                and np.array_equal(state.display_avg_freqs, freqs_arr)
            ):
                new_avg = np.full(freqs_arr.shape, np.nan)
                if state.display_avg is not None:
                    col_idx = np.searchsorted(freqs_arr, state.display_avg_freqs)
                    in_bounds = col_idx < freqs_arr.shape[0]
                    col_idx_clamped = np.minimum(col_idx, freqs_arr.shape[0] - 1)
                    matches = in_bounds & (freqs_arr[col_idx_clamped] == state.display_avg_freqs)
                    new_avg[col_idx[matches]] = state.display_avg[matches]
                state.display_avg = new_avg
                state.display_avg_freqs = freqs_arr

            # Cold-start bins (no prior average yet) just take this frame's raw value
            # instead of blending against NaN.
            cold = np.isnan(state.display_avg)
            state.display_avg = np.where(
                cold, powers_arr, DISPLAY_AVG_ALPHA * powers_arr + (1 - DISPLAY_AVG_ALPHA) * state.display_avg
            )

        powers = state.display_avg
        powers_arr = state.display_avg

        # Store each row's own freqs alongside its powers, not just powers — the known
        # bin set can grow over time (e.g. KafkaConsumerBackend only ever sees bins once
        # they're first flagged), so older rows may cover fewer/different frequencies
        # than the current one. Aligning by column position alone (assuming every row is
        # the same width, growing only at the end) misaligns as soon as a newly-seen
        # frequency sorts *before* an already-known one, visually showing as everything
        # shifting/compressing left until the history of narrower rows ages out.
        if not raw_unchanged:
            state.history.append((freqs_arr, powers_arr.copy()))

    mhz = [f / 1e6 for f in freqs]

    n_cols = len(freqs)
    current_freqs = np.asarray(freqs)
    rows = list(state.history)
    mat = np.full((waterfall_rows, n_cols), np.nan)
    for i, (row_freqs, row_powers) in enumerate(rows):
        target_row = len(rows) - 1 - i
        # Once the grid is stable (true for RTL/HackRF after their first hop or two, and
        # for the Kafka viewer from frame one thanks to its metadata-driven fixed grid —
        # see kafka_consumer.py), every row's own freqs is identical to the current one.
        # Skip the searchsorted/masking path entirely in that case — it's a straight
        # positional copy, ~15x cheaper, and matters a lot at tens of thousands of bins.
        if row_freqs.shape == current_freqs.shape and np.array_equal(row_freqs, current_freqs):
            mat[target_row, :] = row_powers
            continue
        col_idx = np.searchsorted(current_freqs, row_freqs)
        # row_freqs is always a subset of the current freqs (state.sweep keys only ever
        # accumulate), so this should always match exactly — guarded defensively rather
        # than assumed.
        in_bounds = col_idx < n_cols
        col_idx_clamped = np.minimum(col_idx, n_cols - 1)
        matches = in_bounds & (current_freqs[col_idx_clamped] == row_freqs)
        mat[target_row, col_idx[matches]] = row_powers[matches]

    return mhz, powers, mat
