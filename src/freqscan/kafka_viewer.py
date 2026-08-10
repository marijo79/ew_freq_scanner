import argparse
import sys

from freqscan.config import KafkaViewerSettings
from freqscan.plotting import run
from freqscan.streaming.kafka_consumer import KafkaConsumerBackend

STARTUP_GRACE_PERIOD = 5.0  # seconds to wait for a topic-lookup/connection failure before opening the plot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="freqscan-viewer", description=__doc__)
    parser.add_argument(
        "--waterfall-rows",
        type=int,
        default=100,
        help="number of past sweeps kept per channel for the waterfall display (default: 100)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = KafkaViewerSettings()
    backend = KafkaConsumerBackend(settings, args.waterfall_rows)
    backend.start()

    if not backend.wait_ready(STARTUP_GRACE_PERIOD):
        backend.stop()
        print("freqscan-viewer: failed to connect to Kafka.", file=sys.stderr)
        sys.exit(1)

    run(backend, args.waterfall_rows)


if __name__ == "__main__":
    main()