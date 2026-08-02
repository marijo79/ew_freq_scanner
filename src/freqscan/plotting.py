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
        state.history.append(powers[:])

    mhz = [f / 1e6 for f in freqs]
    line.set_data(mhz, powers)
    ax_spec.relim()
    ax_spec.autoscale_view()

    n_cols = len(freqs)
    rows = list(state.history)
    mat = np.full((waterfall_rows, n_cols), np.nan)
    for i, row in enumerate(rows):
        cols = min(len(row), n_cols)
        mat[len(rows) - 1 - i, :cols] = row[:cols]

    wf_img.set_data(mat)
    wf_img.set_extent([mhz[0], mhz[-1], waterfall_rows, 0])
    valid = mat[~np.isnan(mat)]
    if valid.size:
        wf_img.set_clim(valid.min(), valid.max())


def make_update(channels, lines, ax_specs, wf_imgs, waterfall_rows):
    def update(_):
        for channel, line, ax_spec, wf_img in zip(channels, lines, ax_specs, wf_imgs):
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
        make_update(backend.channels, lines, ax_specs, wf_imgs, waterfall_rows),
        interval=200,
        blit=False,
        cache_frame_data=False,
    )
    plt.tight_layout()
    plt.show()
