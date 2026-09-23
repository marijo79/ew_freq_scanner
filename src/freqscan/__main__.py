import argparse
import os
import sys
import time

from freqscan.config import Settings
from freqscan.plotting import run
from freqscan.sdr import build_backend
from freqscan.sdr.base import SDRBackend

STARTUP_GRACE_PERIOD = 2.0  # seconds to wait for an early subprocess failure before opening the plot
OFFSET_REPORT_INTERVAL = 10.0  # seconds between headless-mode Kafka offset reports


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="freqscan")
    parser.add_argument(
        "--plot",
        action="store_true",
        help="open the live spectrum/waterfall plot window",
    )
    parser.add_argument(
        "--kafka_publisher",
        action="store_true",
        help="publish flagged signal bins to Kafka (requires KAFKA__* configured in .env)",
    )
    parser.add_argument(
        "--csv",
        metavar="DIR",
        default=None,
        help=(
            "dump every raw sweep bin (timestamp, freq_hz, power_dbm) to one CSV file per "
            "channel in this directory, named <device>_<freq_start-freq_stop>MHz_<yyyymmddHHMMSS>.csv "
            "(created if missing)"
        ),
    )
    parser.add_argument(
        "--config",
        metavar="FILE",
        default=None,
        help="path to a .env-style config file to load instead of the default ./.env (e.g. conf/pluto_3_ranges)",
    )
    args = parser.parse_args()
    if not args.plot and not args.kafka_publisher and not args.csv:
        parser.error("at least one of --plot, --kafka_publisher, or --csv must be given")
    return args


def run_headless(backend: SDRBackend) -> None:
    if backend.publisher is not None:
        print("freqscan: sending to Kafka started; reporting offsets every 10s", flush=True)
    else:
        print("freqscan: running headless (no --plot)", flush=True)
    try:
        while True:
            time.sleep(OFFSET_REPORT_INTERVAL)
            if backend.publisher is not None:
                print(f"freqscan: kafka offsets: {backend.publisher.offsets()}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        backend.stop()


def main() -> None:
    args = parse_args()
    if args.config is not None and not os.path.isfile(args.config):
        print(f"freqscan: --config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)
    settings = Settings(_env_file=args.config) if args.config is not None else Settings()
    if not args.kafka_publisher:
        settings.kafka = None
    elif settings.kafka is None:
        print("freqscan: --kafka_publisher given but no KAFKA__* configured in .env", file=sys.stderr)
        sys.exit(1)

    backend = build_backend(settings, csv_dir=args.csv)
    backend.start()

    if not backend.wait_ready(STARTUP_GRACE_PERIOD):
        backend.stop()
        print("freqscan: SDR backend failed to start.", file=sys.stderr)
        sys.exit(1)

    if args.plot:
        run(backend, settings.waterfall_rows, settings.plot_refresh_ms, settings.plot_backend)
    else:
        run_headless(backend)


if __name__ == "__main__":
    main()
