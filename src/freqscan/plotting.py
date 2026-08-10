import matplotlib.animation as animation
import matplotlib.pyplot as plt
import numpy as np

from freqscan.sdr.base import Channel, SDRBackend


def update_channel(channel: Channel, line, ax_spec, wf_img, waterfall_rows: int) -> None:
    state = channel.state
    with state.lock:
        if not state.sweep:
            return
        freqs = sorted(state.sweep)
        powers = [state.sweep[f] for f in freqs]
        # Store each row's own freqs alongside its powers, not just powers — the known
        # bin set can grow over time (e.g. KafkaConsumerBackend only ever sees bins once
        # they're first flagged), so older rows may cover fewer/different frequencies
        # than the current one. Aligning by column position alone (assuming every row is
        # the same width, growing only at the end) misaligns as soon as a newly-seen
        # frequency sorts *before* an already-known one, visually showing as everything
        # shifting/compressing left until the history of narrower rows ages out.
        state.history.append((np.asarray(freqs), np.asarray(powers)))

    mhz = [f / 1e6 for f in freqs]
    line.set_data(mhz, powers)
    ax_spec.relim()
    ax_spec.autoscale_view()

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

    wf_img.set_data(mat)
    wf_img.set_extent([mhz[0], mhz[-1], waterfall_rows, 0])
    valid_cells = mat[~np.isnan(mat)]
    if valid_cells.size:
        wf_img.set_clim(valid_cells.min(), valid_cells.max())


def make_update(channels, lines, ax_specs, ax_wfs, wf_imgs, waterfall_rows):
    def update(_):
        for channel, line, ax_spec, ax_wf, wf_img in zip(channels, lines, ax_specs, ax_wfs, wf_imgs):
            # Channel.label can change after the plot is built (e.g. KafkaConsumerBackend
            # discovers it lazily from the first message on that partition).
            if ax_spec.get_title() != channel.label:
                ax_spec.set_title(channel.label)
                ax_wf.set_title(channel.label)
            update_channel(channel, line, ax_spec, wf_img, waterfall_rows)
        return lines + wf_imgs

    return update


def style_axes(fig, axes):
    dark = "#1e1e1e"
    fig.patch.set_facecolor(dark)
    for ax in axes:
        ax.set_facecolor(dark)
        ax.tick_params(colors="white")
        ax.xaxis.label.set_color("white")
        ax.yaxis.label.set_color("white")
        ax.title.set_color("white")
        for spine in ax.spines.values():
            spine.set_edgecolor("#444")


def add_freq_hint(fig, ax_wf):
    vline = ax_wf.axvline(x=0, color="white", lw=0.8, ls="--", visible=False, zorder=5)
    label = ax_wf.text(
        0, 2, "", color="white", fontsize=8, ha="center", va="top", zorder=6,
        bbox=dict(boxstyle="round,pad=0.2", fc="#1e1e1e", ec="#555", alpha=0.85),
    )

    def on_mouse_move(event):
        if event.inaxes is ax_wf and event.xdata is not None:
            vline.set_xdata([event.xdata])
            vline.set_visible(True)
            label.set_text(f"{event.xdata:.3f} MHz")
            label.set_position((event.xdata, 2))
            label.set_visible(True)
        else:
            vline.set_visible(False)
            label.set_visible(False)
        fig.canvas.draw_idle()

    fig.canvas.mpl_connect("motion_notify_event", on_mouse_move)


def run(backend: SDRBackend, waterfall_rows: int) -> None:
    n = len(backend.channels)
    fig, axes = plt.subplots(
        2, n, figsize=(11 * n, 8), gridspec_kw={"height_ratios": [1, 2]}, squeeze=False
    )

    lines = []
    wf_imgs = []
    ax_specs = []
    ax_wfs = []

    for col, channel in enumerate(backend.channels):
        ax_spec = axes[0][col]
        ax_wf = axes[1][col]
        ax_specs.append(ax_spec)
        ax_wfs.append(ax_wf)

        ax_spec.set_xlabel("Frequency (MHz)")
        ax_spec.set_ylabel("Power (dBm)")
        ax_spec.set_title(channel.label)
        ax_spec.grid(True, alpha=0.3)
        (line,) = ax_spec.plot([], [], lw=1, color="lime")
        lines.append(line)

        ax_wf.set_xlabel("Frequency (MHz)")
        ax_wf.set_ylabel("Time (newest at top)")
        ax_wf.set_title(channel.label)
        wf_img = ax_wf.imshow(
            np.full((waterfall_rows, 1), np.nan),
            aspect="auto",
            origin="upper",
            cmap="inferno",
            interpolation="nearest",
        )
        wf_imgs.append(wf_img)
        add_freq_hint(fig, ax_wf)

    style_axes(fig, ax_specs + ax_wfs)

    def on_close(_):
        backend.stop()

    fig.canvas.mpl_connect("close_event", on_close)

    ani = animation.FuncAnimation(
        fig,
        make_update(backend.channels, lines, ax_specs, ax_wfs, wf_imgs, waterfall_rows),
        interval=200,
        blit=False,
        cache_frame_data=False,
    )
    plt.tight_layout()
    plt.show()
