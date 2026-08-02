import sys

from freqscan.config import Settings
from freqscan.plotting import run
from freqscan.sdr import build_backend

STARTUP_GRACE_PERIOD = 2.0  # seconds to wait for an early subprocess failure before opening the plot


def main() -> None:
    settings = Settings()
    backend = build_backend(settings)
    backend.start()

    if not backend.wait_ready(STARTUP_GRACE_PERIOD):
        backend.stop()
        print("freqscan: SDR backend failed to start; not opening the plot window.", file=sys.stderr)
        sys.exit(1)

    run(backend, settings.waterfall_rows)


if __name__ == "__main__":
    main()
