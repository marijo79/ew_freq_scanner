import subprocess
import threading
from collections import deque
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation

DEVICES = [
    {"id": 0, "freq_start": "80M",  "freq_stop": "120M", "label": "Device 0  80–120 MHz"},
    {"id": 1, "freq_start": "420M", "freq_stop": "470M", "label": "Device 1 420–470 MHz"},
    {"id": 2, "freq_start": "850M", "freq_stop": "900M", "label": "Device 2 850–900 MHz"},
]
BIN_SIZE       = "10k"
GAIN           = 50
INTERVAL       = 1
WATERFALL_ROWS = 100
EDGE_TRIM      = 0.15  # fraction of bins dropped from each edge of every FFT hop

states = [
    {"sweep": {}, "lock": threading.Lock(), "history": deque(maxlen=WATERFALL_ROWS)}
    for _ in DEVICES
]


def launch_rtl_power(dev, state):
    cmd = [
        "rtl_power",
        "-d", str(dev["id"]),
        "-f", f"{dev['freq_start']}:{dev['freq_stop']}:{BIN_SIZE}",
        "-g", str(GAIN),
        "-i", str(INTERVAL),
        "-",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    for raw in proc.stdout:
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
            trim    = max(1, int(n * EDGE_TRIM))
            powers  = powers[trim: n - trim]
            freqs   = [hz_low + hz_step * (i + trim) for i in range(len(powers))]
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
    for dev, state in zip(DEVICES, states):
        threading.Thread(target=launch_rtl_power, args=(dev, state), daemon=True).start()

    fig, axes = plt.subplots(
        2, 3, figsize=(33, 8), gridspec_kw={"height_ratios": [1, 2]}
    )

    lines   = []
    wf_imgs = []
    ax_specs = []
    ax_wfs   = []

    for col, dev in enumerate(DEVICES):
        ax_spec = axes[0][col]
        ax_wf   = axes[1][col]
        ax_specs.append(ax_spec)
        ax_wfs.append(ax_wf)

        ax_spec.set_xlabel("Frequency (MHz)")
        ax_spec.set_ylabel("Power (dBm)")
        ax_spec.set_title(f"Spectrum — {dev['label']}")
        ax_spec.grid(True, alpha=0.3)
        line, = ax_spec.plot([], [], lw=1, color="lime")
        lines.append(line)

        ax_wf.set_xlabel("Frequency (MHz)")
        ax_wf.set_ylabel("Time (newest at top)")
        ax_wf.set_title(f"Waterfall — {dev['label']}")
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

    ani = animation.FuncAnimation(
        fig, make_update(lines, ax_specs, wf_imgs),
        interval=200, blit=False, cache_frame_data=False,
    )
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()