import matplotlib.animation as animation
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator

from freqscan.plotting.common import (
    SPECTRUM_FILL_ALPHA,
    SPECTRUM_LINE_COLOR,
    SPECTRUM_YTICK_STEP_DB,
    WATERFALL_COLORMAP,
    channel_snapshot,
    initial_freq_extent,
    initial_waterfall_clim,
    initial_ylim,
)
from freqscan.sdr.base import SDRBackend


def update_channel(channel, line, fill, wf_img, waterfall_rows: int, ybase: float) -> list:
    snap = channel_snapshot(channel, waterfall_rows)
    if snap is None:
        return []
    mhz, powers, mat = snap

    line.set_data(mhz, powers)

    # fill_between()'s returned PolyCollection has no set_data() -- update its vertices
    # in place instead (set_verts()), which is what keeps this SDR++-style area fill
    # compatible with FuncAnimation's blit=True: recreating the collection every frame
    # would work too, but at the tens-of-thousands-of-bins scale this project's wide
    # HackRF/Pluto ranges already reach (see CLAUDE.md item 5's matplotlib fast path),
    # that's real avoidable per-frame allocation.
    xs = np.asarray(mhz)
    verts = np.empty((xs.shape[0] + 2, 2))
    verts[0] = (xs[0], ybase)
    verts[1:-1, 0] = xs
    verts[1:-1, 1] = powers
    verts[-1] = (xs[-1], ybase)
    fill.set_verts([verts])

    wf_img.set_data(mat)
    # extent derived from mhz's own real bounds every frame -- NOT the fixed
    # initial_freq_extent() view range set once at setup. Those two can genuinely
    # differ: mat's actual column span always matches channel.state.sweep's full
    # current key set (channel_snapshot()'s own `sorted(state.sweep)`), which is the
    # wide declared grid, mostly NaN, for a KafkaConsumerBackend channel, but only the
    # narrow real range for a local PlutoStareBackend channel (see
    # initial_freq_extent()'s docstring for why those two cases differ). An extent that
    # doesn't match mat's true span stretches/squeezes the whole image into the wrong
    # width -- found live 2026-09-21 right after narrowing initial_freq_extent() to fix
    # a separate dead-margin bug: fixing the view's xlim alone, without also keeping
    # this extent in sync with the image's actual data width, made the waterfall
    # visibly narrower than the spectrum line above it and misaligned with it. See
    # pyqtgraph_backend.py's matching fix/comment -- same root cause, same fix, applied
    # to imshow()'s set_extent() instead of ImageItem's rect= kwarg.
    wf_img.set_extent([mhz[0], mhz[-1], waterfall_rows, 0])

    return [line, fill, wf_img]


def make_update(channels, lines, fills, wf_imgs, ybases, waterfall_rows, hint_artists):
    def update(_):
        artists = list(hint_artists)
        for channel, line, fill, wf_img, ybase in zip(channels, lines, fills, wf_imgs, ybases):
            artists += update_channel(channel, line, fill, wf_img, waterfall_rows, ybase)
        return artists

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


def add_freq_hint(fig, ax_wf) -> tuple:
    # animated=True + no direct draw_idle() call: this figure's line/wf_img are also
    # animated=True for blitting (see run()), and matplotlib's normal (non-blit) draw
    # path skips any animated=True artist entirely, assuming the animation's own blit
    # cycle will draw it instead. Calling fig.canvas.draw_idle() here (a full, non-blit
    # redraw) used to wipe out the spectrum line/waterfall image every time the mouse
    # moved over the waterfall, until the next animation tick redrew them — found live
    # 2026-09. Fix: these two artists only ever get their *data* updated here; they're
    # actually drawn by being included in make_update()'s returned artist list every
    # frame instead, the same blit cycle as everything else.
    vline = ax_wf.axvline(x=0, color="white", lw=0.8, ls="--", visible=False, zorder=5, animated=True)
    label = ax_wf.text(
        0, 2, "", color="white", fontsize=8, ha="center", va="top", zorder=6,
        bbox=dict(boxstyle="round,pad=0.2", fc="#1e1e1e", ec="#555", alpha=0.85),
        animated=True,
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

    fig.canvas.mpl_connect("motion_notify_event", on_mouse_move)
    return vline, label


def run(backend: SDRBackend, waterfall_rows: int, refresh_ms: int = 200) -> None:
    n = len(backend.channels)
    # Capped the same way pyqtgraph_backend.py caps its own window width
    # (min(1100 * n, 1900) px) -- uncapped, this grew wider than the screen with more
    # than ~2 channels active (11in/channel * 100 dpi default = 1100px/channel).
    fig, axes = plt.subplots(
        2, n, figsize=(min(11 * n, 19), 8), gridspec_kw={"height_ratios": [1, 2]}, squeeze=False
    )
    fig.canvas.manager.set_window_title("EWFrequencyScan")

    lines = []
    fills = []
    wf_imgs = []
    ax_specs = []
    ax_wfs = []
    ybases = []
    hint_artists = []

    for col, channel in enumerate(backend.channels):
        ax_spec = axes[0][col]
        ax_wf = axes[1][col]
        ax_specs.append(ax_spec)
        ax_wfs.append(ax_wf)

        ax_spec.set_xlabel("Frequency (MHz)")
        ax_spec.set_ylabel("Power (dBm)")
        ax_spec.set_title(channel.label)
        ax_spec.grid(True, alpha=0.3)
        # Clean 10dB-step gridlines with labels, matching SDR++'s own y-axis instead of
        # matplotlib's default auto-placed ticks.
        ax_spec.yaxis.set_major_locator(MultipleLocator(SPECTRUM_YTICK_STEP_DB))
        # x-range fixed up front from where real data actually exists
        # (initial_freq_extent()), not the potentially-wider channel.freq_start_mhz/
        # freq_stop_mhz -- see that function's docstring: for PlutoStareBackend those
        # declared bounds are the pre-edge-trim capture span, wider than what ever
        # actually gets plotted. Using the declared bounds here left a dead empty
        # margin on both sides of the real data on every axis -- confirmed live
        # 2026-09-14 on a narrowed (90-110MHz) Pluto stare view. Blitting needs stable
        # axis limits regardless, and this range is known before any data arrives
        # (initial_freq_extent() blocks briefly on first real data, same as
        # initial_ylim() below). y-range is fitted from this channel's own first real
        # data (see initial_ylim()) rather than one hardcoded guess, since different
        # backends' power scales differ a lot (RTL/HackRF's calibrated dBm vs. Pluto's
        # own uncalibrated relative-dB FFT output) — either way it's still fixed once
        # chosen, blitting can't change it later.
        freq_lo, freq_hi = initial_freq_extent(channel)
        ax_spec.set_xlim(freq_lo, freq_hi)
        ylim = initial_ylim(channel)
        ax_spec.set_ylim(*ylim)
        ybases.append(ylim[0])

        # SDR++-style area fill under the live trace: same pale cyan as the trace
        # itself, at low alpha -- matched live 2026-09-14 against a screenshot of
        # SDR++'s own amplitude panel rather than assumed.
        fill = ax_spec.fill_between(
            [freq_lo, freq_hi], ylim[0], ylim[0],
            color=SPECTRUM_LINE_COLOR, alpha=SPECTRUM_FILL_ALPHA, zorder=1, animated=True,
        )
        fills.append(fill)
        (line,) = ax_spec.plot([], [], lw=1, color=SPECTRUM_LINE_COLOR, zorder=2, animated=True)
        lines.append(line)

        ax_wf.set_xlabel("Frequency (MHz)")
        ax_wf.set_ylabel("Time (newest at top)")
        ax_wf.set_title(channel.label)
        ax_wf.set_xlim(freq_lo, freq_hi)
        # Color range fixed once, anchored to the actual noise floor (initial_waterfall_clim(),
        # not initial_ylim()'s wide +-15dB-padded line range) -- see that function's
        # docstring for why plain min/max auto-ranging and the line's own padded range
        # both washed out contrast on raw, unaveraged FFT noise (confirmed live
        # 2026-09-14 against SDR++, which uses fixed Min/Max levels for the same reason).
        wf_vmin, wf_vmax = initial_waterfall_clim(channel)
        wf_img = ax_wf.imshow(
            np.full((waterfall_rows, 1), np.nan),
            aspect="auto",
            origin="upper",
            cmap=WATERFALL_COLORMAP,
            vmin=wf_vmin,
            vmax=wf_vmax,
            interpolation="nearest",
            extent=[freq_lo, freq_hi, waterfall_rows, 0],
            animated=True,
        )
        wf_imgs.append(wf_img)
        hint_artists.extend(add_freq_hint(fig, ax_wf))

    style_axes(fig, ax_specs + ax_wfs)

    def on_close(_):
        backend.stop()

    fig.canvas.mpl_connect("close_event", on_close)

    ani = animation.FuncAnimation(
        fig,
        make_update(backend.channels, lines, fills, wf_imgs, ybases, waterfall_rows, hint_artists),
        interval=refresh_ms,
        blit=True,
        cache_frame_data=False,
    )
    plt.tight_layout()
    plt.show()
