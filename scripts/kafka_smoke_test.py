"""Send one real message through freqscan's Kafka producer path and confirm delivery.

Takes its own connection args rather than reading the project's .env, so it can be
pointed at a disposable local broker without touching real configuration:

    python scripts/kafka_smoke_test.py --bootstrap-servers localhost:9092 --security-protocol PLAINTEXT
"""

import argparse
import sys

from freqscan.config import KafkaSettings
from freqscan.streaming.kafka_publisher import KafkaSignalPublisher, build_producer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", default="freqscan.signals")
    parser.add_argument("--security-protocol", default="PLAINTEXT")
    parser.add_argument("--sasl-mechanism", default="OAUTHBEARER")
    parser.add_argument("--region", default="eu-central-1")
    parser.add_argument("--channel-label", default="smoke-test")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    kafka = KafkaSettings(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        region=args.region,
    )
    publisher = KafkaSignalPublisher(build_producer(kafka), kafka.topic, kafka.metadata_topic)

    result: dict = {}

    def on_delivery(err, msg) -> None:
        result["err"] = err
        result["msg"] = msg

    publisher.publish(
        args.channel_label,
        bins=[(100_000_000.0, -42.0), (100_010_000.0, -38.5)],
        on_delivery=on_delivery,
    )
    publisher.flush(timeout=10.0)

    if "err" not in result:
        print("FAILED: no delivery callback fired within timeout", file=sys.stderr)
        sys.exit(1)
    if result["err"] is not None:
        print(f"FAILED: {result['err']}", file=sys.stderr)
        sys.exit(1)

    msg = result["msg"]
    print(f"OK: delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")
    print(f"key={msg.key().decode()} value={msg.value().decode()}")


if __name__ == "__main__":
    main()
