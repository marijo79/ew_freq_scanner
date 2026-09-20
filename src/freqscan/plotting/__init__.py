from freqscan.sdr.base import SDRBackend


def run(backend: SDRBackend, waterfall_rows: int, refresh_ms: int = 200, plot_backend: str = "matplotlib") -> None:
    """Opens the live spectrum/waterfall plot window, dispatching to one of two
    interchangeable rendering implementations (see CLAUDE.md's plotting section):

    - "matplotlib" (default): the original implementation. No extra dependencies beyond
      what this project already requires.
    - "pyqtgraph": built for real-time plotting specifically, using Qt's own widgets
      instead of matplotlib's general-purpose vector rendering -- meaningfully faster
      redraws once more than ~2 subplots are active (matplotlib's per-subplot blit
      overhead was measured live 2026-09 at ~150-285ms/frame; see
      plotting/matplotlib_backend.py). Needs `pyqtgraph` and a Qt binding
      (PySide6/PyQt5/PyQt6) installed -- optional extra, see pyproject.toml's
      `plot-pyqtgraph` group.

    Both implementations share the exact same data-prep logic (plotting/common.py) --
    the rolling waterfall history, frequency-based row alignment, and initial-y-range
    fitting are implemented once and reused by both, so they can't silently diverge.
    """
    if plot_backend == "matplotlib":
        from freqscan.plotting.matplotlib_backend import run as _run
    elif plot_backend == "pyqtgraph":
        from freqscan.plotting.pyqtgraph_backend import run as _run
    else:
        raise ValueError(f"unknown plot_backend {plot_backend!r} -- expected 'matplotlib' or 'pyqtgraph'")
    _run(backend, waterfall_rows, refresh_ms)
