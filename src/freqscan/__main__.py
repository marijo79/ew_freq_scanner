import argparse
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
    args = parser.parse_args()
    if not args.plot and not args.kafka_publisher:
        parser.error("at least one of --plot or --kafka_publisher must be given")
    return args


def run_headless(backend: SDRBackend) -> None:
    print("freqscan: sending to Kafka started; reporting offsets every 10s", flush=True)
    try:
        while True:
            time.sleep(OFFSET_REPORT_INTERVAL)
            print(f"freqscan: kafka offsets: {backend.publisher.offsets()}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        backend.stop()


def main() -> None:
    args = parse_args()
    settings = Settings()
    if not args.kafka_publisher:
        settings.kafka = None
    elif settings.kafka is None:
        print("freqscan: --kafka_publisher given but no KAFKA__* configured in .env", file=sys.stderr)
        sys.exit(1)

    backend = build_backend(settings)
    backend.start()

    if not backend.wait_ready(STARTUP_GRACE_PERIOD):
        backend.stop()
        print("freqscan: SDR backend failed to start.", file=sys.stderr)
        sys.exit(1)

    if args.plot:
        run(backend, settings.waterfall_rows)
    else:
        run_headless(backend)


if __name__ == "__main__":
    main()
