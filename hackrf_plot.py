import subprocess
import threading
from collections import deque
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation

RANGES = [
 #   {"freq_start": 420,  "freq_stop": 470, "label": "420–470 MHz",   "bin_width": 200_000, "edge_trim": 0.05},
 #   {"freq_start": 850,  "freq_stop": 900, "label": "850–900 MHz",   "bin_width": 200_000, "edge_trim": 0.05},
 #   {"freq_start": 2000, "freq_stop": 2600, "label": "2000–2600 MHz","bin_width": 200_000, "edge_trim": 0.05},
    {"freq_start": 850,  "freq_stop": 950,  "label": "850–950 MHz",   "bin_width": 20_000,  "edge_trim": 0.02},
    {"freq_start": 2300, "freq_stop": 2500, "label": "2300–2500 MHz", "bin_width": 100_000, "edge_trim": 0.05},
    {"freq_start": 5700, "freq_stop": 5900, "label": "5700–5900 MHz", "bin_width": 100_100, "edge_trim": 0.05},
]
LNA_GAIN       = 32       # 0–40 dB, 8 dB steps
VGA_GAIN       = 20       # 0–62 dB, 2 dB steps
WATERFALL_ROWS = 100

states = [
    {"sweep": {}, "lock": threading.Lock(), "history": deque(maxlen=WATERFALL_ROWS)}
    for _ in RANGES
]

_proc = None


def range_index_for(hz_low):
    mhz = hz_low / 1e6
    for i, r in enumerate(RANGES):
        if r["freq_start"] <= mhz < r["freq_stop"]:
            return i
    return None


def launch_hackrf_sweep():
    global _proc
    freq_args = []
    for r in RANGES:
        freq_args += ["-f", f"{r['freq_start']}:{r['freq_stop']}"]

    bin_width = min(r["bin_width"] for r in RANGES)
    cmd = ["hackrf_sweep", *freq_args, "-w", str(bin_width), "-l", str(LNA_GAIN), "-g", str(VGA_GAIN)]
    _proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    for raw in _proc.stdout:
        raw = raw.strip()
        if not raw:
            continue
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) < 7:
            continue
        try:
            hz_low  = float(parts[2])
            hz_step = float(parts[4])
            powers  = [float(p) for p in parts[6:]]
            n       = len(powers)

            idx = range_index_for(hz_low)
            if idx is None:
                continue
            trim    = max(1, int(n * RANGES[idx]["edge_trim"]))
            powers  = powers[trim: n - trim]
            freqs   = [hz_low + hz_step * (i + trim) for i in range(len(powers))]
            state = states[idx]
            with state["lock"]:
                for f, p in zip(freqs, powers):
                    state["sweep"][f] = p
        except ValueError:
            continue


def update_device(state, line, ax_spec, wf_img):
    with state["lock"]:
        if not state["sweep"]:
            return
        freqs  = sorted(state["sweep"])
        powers = [state["sweep"][f] for f in freqs]
        state["history"].append(powers[:])

    mhz = [f / 1e6 for f in freqs]
    line.set_data(mhz, powers)
    ax_spec.relim()
    ax_spec.autoscale_view()

    n_cols = len(freqs)
    rows   = list(state["history"])
    mat    = np.full((WATERFALL_ROWS, n_cols), np.nan)
    for i, row in enumerate(rows):
        cols = min(len(row), n_cols)
        mat[len(rows) - 1 - i, :cols] = row[:cols]

    wf_img.set_data(mat)
    wf_img.set_extent([mhz[0], mhz[-1], WATERFALL_ROWS, 0])
    valid = mat[~np.isnan(mat)]
    if valid.size:
        wf_img.set_clim(valid.min(), valid.max())


def make_update(lines, ax_specs, wf_imgs):
    def update(_):
        for state, line, ax_spec, wf_img in zip(states, lines, ax_specs, wf_imgs):
            update_device(state, line, ax_spec, wf_img)
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


def main():
    threading.Thread(target=launch_hackrf_sweep, daemon=True).start()

    fig, axes = plt.subplots(
        2, 3, figsize=(33, 8), gridspec_kw={"height_ratios": [1, 2]}
    )

    lines    = []
    wf_imgs  = []
    ax_specs = []
    ax_wfs   = []

    for col, r in enumerate(RANGES):
        ax_spec = axes[0][col]
        ax_wf   = axes[1][col]
        ax_specs.append(ax_spec)
        ax_wfs.append(ax_wf)

        ax_spec.set_xlabel("Frequency (MHz)")
        ax_spec.set_ylabel("Power (dBm)")
        ax_spec.set_title(f"Spectrum — {r['label']}")
        ax_spec.grid(True, alpha=0.3)
        line, = ax_spec.plot([], [], lw=1, color="lime")
        lines.append(line)

        ax_wf.set_xlabel("Frequency (MHz)")
        ax_wf.set_ylabel("Time (newest at top)")
        ax_wf.set_title(f"Waterfall — {r['label']}")
        wf_img = ax_wf.imshow(
            np.full((WATERFALL_ROWS, 1), np.nan),
            aspect="auto",
            origin="upper",
            cmap="inferno",
            interpolation="nearest",
        )
        wf_imgs.append(wf_img)
        add_freq_hint(fig, ax_wf)

    style_axes(fig, ax_specs + ax_wfs)

    def on_close(_):
        if _proc is not None:
            _proc.terminate()

    fig.canvas.mpl_connect("close_event", on_close)

    ani = animation.FuncAnimation(
        fig, make_update(lines, ax_specs, wf_imgs),
        interval=200, blit=False, cache_frame_data=False,
    )
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()