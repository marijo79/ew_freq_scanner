import pyqtgraph as pg
from pyqtgraph.Qt import QtCore

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

# Same dark theme as the matplotlib backend's style_axes(). Must be set before creating
# any GraphicsLayoutWidget/PlotItem -- pyqtgraph reads these as defaults at widget
# creation time, not something later per-widget calls override cleanly.
pg.setConfigOption("background", "#1e1e1e")
pg.setConfigOption("foreground", "w")

# Fixed left-axis label width for BOTH the spectrum and waterfall row of a channel.
# Confirmed live 2026-09-14 via direct pixel measurement of a screenshot: matching data
# range with setXRange() on both plots does NOT guarantee matching pixel columns --
# pyqtgraph sizes each row's left-axis label area independently from its own tick text
# width, and "Power (dBm)" (e.g. "-30".."50") vs "Time (newest at top)" (e.g.
# "0".."600") have different digit counts, so the two rows ended up with visibly
# different plot-area widths (measured ~6% off, enough to misalign a station's spike in
# the spectrum line from its own streak in the waterfall by over 1MHz by the right edge
# of a 20MHz-wide view). Forcing both rows' axis area to one fixed width makes their
# plot areas -- and therefore every frequency's pixel column -- identical regardless of
# either row's actual tick label content. Sized for something like "-140" (RTL/HackRF's
# wider calibrated dBm range, see FALLBACK_SPECTRUM_YLIM) with a little headroom.
LEFT_AXIS_WIDTH_PX = 55


def _add_freq_hint(plot_item):
    """Same hover crosshair as matplotlib_backend.add_freq_hint(), pyqtgraph's own way:
    an InfiniteLine + TextItem added directly to the plot rather than a separate
    blit-vs-non-blit concern (pyqtgraph doesn't have matplotlib's blit/full-redraw
    split, so there's no equivalent of that backend's animated=True workaround needed
    here -- moving these items' position is just a normal, cheap per-item update)."""
    vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen("w", width=0.8, style=QtCore.Qt.PenStyle.DashLine))
    vline.setVisible(False)
    plot_item.addItem(vline, ignoreBounds=True)

    label = pg.TextItem(color="w", anchor=(0.5, 1.0), fill=pg.mkBrush("#1e1e1e"))
    label.setVisible(False)
    plot_item.addItem(label, ignoreBounds=True)

    def on_move(scene_pos):
        if plot_item.sceneBoundingRect().contains(scene_pos):
            x = plot_item.vb.mapSceneToView(scene_pos).x()
            vline.setPos(x)
            vline.setVisible(True)
            label.setText(f"{x:.3f} MHz")
            label.setPos(x, 2)
            label.setVisible(True)
        else:
            vline.setVisible(False)
            label.setVisible(False)

    plot_item.scene().sigMouseMoved.connect(on_move)
    # keep a reference on the plot item itself so the closure/signal connection isn't
    # garbage-collected (Qt's signal/slot doesn't keep on_move alive on its own)
    plot_item._freq_hint_handler = on_move


def run(backend: SDRBackend, waterfall_rows: int, refresh_ms: int = 200) -> None:
    n = len(backend.channels)
    app = pg.mkQApp("FreqScan")
    win = pg.GraphicsLayoutWidget(show=True, title="FreqScan")
    win.resize(min(1100 * n, 1900), 800)

    colormap = pg.colormap.get(WATERFALL_COLORMAP)
    lut = colormap.getLookupTable(0.0, 1.0, 256)

    # SDR++-style fill: same pale cyan as the line, at low alpha -- matched live
    # 2026-09-14 against a screenshot of SDR++'s own amplitude panel rather than assumed.
    fill_color = pg.mkColor(SPECTRUM_LINE_COLOR)
    fill_color.setAlpha(int(255 * SPECTRUM_FILL_ALPHA))

    curves = []
    images = []
    image_rects = []
    wf_levels = []

    for col, channel in enumerate(backend.channels):
        p_spec = win.addPlot(row=0, col=col)
        p_spec.setTitle(channel.label, color="w")
        p_spec.setLabel("bottom", "Frequency (MHz)")
        p_spec.setLabel("left", "Power (dBm)")
        p_spec.showGrid(x=True, y=True, alpha=0.3)
        # Clean 10dB-step gridlines with labels, matching SDR++'s own y-axis instead of
        # pyqtgraph's default auto-placed ticks.
        p_spec.getAxis("left").setTickSpacing(SPECTRUM_YTICK_STEP_DB, SPECTRUM_YTICK_STEP_DB)
        # Fixed width -- see LEFT_AXIS_WIDTH_PX above for why this matters for
        # spectrum/waterfall alignment.
        p_spec.getAxis("left").setWidth(LEFT_AXIS_WIDTH_PX)
        # x-range fixed up front from where real data actually exists
        # (initial_freq_extent()), not the potentially-wider channel.freq_start_mhz/
        # freq_stop_mhz -- see that function's docstring: for PlutoStareBackend those
        # declared bounds are the pre-edge-trim capture span, wider than what ever
        # actually gets plotted. Using the declared bounds here left a dead empty
        # margin on both sides of the real data on every axis -- confirmed live
        # 2026-09-14 on a narrowed (90-110MHz) Pluto stare view. y-range fitted from
        # this channel's own first real data (initial_ylim()) since different backends'
        # power scales differ a lot. Auto-ranging every frame is real per-frame cost in
        # pyqtgraph too, not free — disabling it isn't just a blit-compatibility
        # requirement like in matplotlib.
        freq_lo, freq_hi = initial_freq_extent(channel)
        p_spec.setXRange(freq_lo, freq_hi, padding=0)
        ylim = initial_ylim(channel)
        p_spec.setYRange(*ylim, padding=0)
        p_spec.setMouseEnabled(x=False, y=False)
        # pyqtgraph fills to fillLevel automatically on every setData(), no manual
        # per-frame polygon work needed unlike the matplotlib backend.
        curve = p_spec.plot(pen=pg.mkPen(SPECTRUM_LINE_COLOR, width=1), fillLevel=ylim[0], brush=pg.mkBrush(fill_color))
        curves.append(curve)

        p_wf = win.addPlot(row=1, col=col)
        p_wf.setTitle(channel.label, color="w")
        p_wf.setLabel("bottom", "Frequency (MHz)")
        p_wf.setLabel("left", "Time (newest at top)")
        # Same fixed width as p_spec above -- this is the actual fix for
        # spectrum/waterfall alignment, not the matching setXRange() call below (which
        # only matches data range, not pixel geometry).
        p_wf.getAxis("left").setWidth(LEFT_AXIS_WIDTH_PX)
        p_wf.setXRange(freq_lo, freq_hi, padding=0)
        p_wf.setYRange(0, waterfall_rows, padding=0)
        p_wf.setMouseEnabled(x=False, y=False)
        p_wf.invertY(True)  # newest row (index 0) at the top, matching "Time (newest at top)"
        img = pg.ImageItem()
        p_wf.addItem(img)
        images.append(img)
        image_rects.append(QtCore.QRectF(freq_lo, 0, freq_hi - freq_lo, waterfall_rows))
        # Color range fixed once, anchored to the actual noise floor -- see
        # initial_waterfall_clim()'s docstring and matplotlib_backend.py's matching
        # comment for why this differs from the spectrum panel's own initial_ylim().
        wf_levels.append(initial_waterfall_clim(channel))

        _add_freq_hint(p_wf)

    def update():
        for channel, curve, img, rect, levels in zip(
            backend.channels, curves, images, image_rects, wf_levels
        ):
            snap = channel_snapshot(channel, waterfall_rows)
            if snap is None:
                continue
            mhz, powers, mat = snap
            curve.setData(mhz, powers)
            # pyqtgraph's ImageItem expects (x, y) axis order -- opposite of mat's own
            # (row=time, col=frequency) numpy convention -- hence the transpose. mat's
            # row 0 is already the newest (see channel_snapshot()); with the rect's y=0
            # edge at the *top* of the view (invertY(True) above), column 0 of the
            # transposed array needs to stay the newest row too, so no flip here --
            # confirmed live 2026-09-04 that an earlier np.flipud() here was backwards,
            # visibly scrolling new data in at the bottom and aging it toward the top,
            # the opposite of the "Time (newest at top)" axis label (matplotlib_backend.py
            # never needed this: imshow's own origin="upper" puts row 0 at the top with
            # no extra flip, which is the behavior to match here).
            img_data = mat.T
            # rect= and lut= must both be passed directly on every setImage() call, not
            # just once at setup via setRect()/setLookupTable() -- confirmed live
            # 2026-09-04 that setImage() unconditionally resets the item's rect to raw
            # pixel dimensions (0, 0, n_cols, waterfall_rows) on every call, silently
            # discarding a setup-time setRect(); since the view's axis range stays fixed
            # at the intended MHz/row range (set once above and never auto-ranged), the
            # image ends up rendered completely outside the visible viewport, which
            # showed as a flat solid color rather than blank -- passing rect= here keeps
            # the image's coordinate space correct on every single frame instead of just
            # the first. lut= has the same reset-on-setImage() behavior (that fix
            # confirmed separately, also live 2026-09-04): a one-time setLookupTable()
            # call alone doesn't survive the first real setImage().
            img.setImage(img_data, autoLevels=False, levels=levels, lut=lut, rect=rect)

    timer = QtCore.QTimer()
    timer.timeout.connect(update)
    timer.start(refresh_ms)

    app.aboutToQuit.connect(backend.stop)

    if hasattr(pg, "exec"):
        pg.exec()
    else:  # pragma: no cover - older pyqtgraph versions
        app.exec()
