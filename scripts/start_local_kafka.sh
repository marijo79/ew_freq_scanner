#!/usr/bin/env bash
# Start (and, if needed, first-time-format) a local native Kafka broker for smoke-testing
# freqscan's streaming path without touching MSK. KRaft storage lives under log.dirs from
# server.properties (default install puts it in /tmp, which most systems wipe on reboot —
# this script reformats it automatically whenever it's missing).
#
#   scripts/start_local_kafka.sh
#   KAFKA_HOME=/path/to/kafka scripts/start_local_kafka.sh   # override install location
#
# Then smoke-test with: python scripts/kafka_smoke_test.py --bootstrap-servers localhost:9092 --security-protocol PLAINTEXT
#
# For a producer on another machine (e.g. a Raspberry Pi) to reach this broker, two things
# in config/server.properties must both be set, not just the firewall/port:
#   - listeners=PLAINTEXT://:9092              (binds all interfaces — usually already right)
#   - advertised.listeners=PLAINTEXT://<LAN IP>:9092   (NOT "localhost" — the broker hands
#     this address back to clients after the initial bootstrap for actual produce/fetch
#     requests; left as "localhost" it works fine from this machine but silently breaks any
#     remote client right after the first connection, which is easy to misdiagnose as a
#     firewall/network problem instead of a config one)
# Changing advertised.listeners requires restarting the broker (this script/kafka-server-stop.sh) —
# it isn't hot-reloaded.

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka/kafka_2.13-4.3.1}"
CONFIG="$KAFKA_HOME/config/server.properties"
BROKER_PORT="${BROKER_PORT:-9092}"

if [[ ! -f "$CONFIG" ]]; then
    echo "error: no server.properties at $CONFIG (set KAFKA_HOME to your Kafka install dir)" >&2
    exit 1
fi

if ss -ltn 2>/dev/null | grep -q ":${BROKER_PORT} "; then
    echo "Kafka already listening on localhost:${BROKER_PORT} — nothing to do."
    exit 0
fi

LOG_DIR="$(grep -E '^log.dirs=' "$CONFIG" | head -1 | cut -d= -f2- | cut -d, -f1)"

if [[ -z "$LOG_DIR" ]]; then
    echo "error: could not read log.dirs from $CONFIG" >&2
    exit 1
fi

if [[ ! -f "$LOG_DIR/meta.properties" ]]; then
    echo "No KRaft storage found at $LOG_DIR — formatting..."
    CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
    "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$CLUSTER_ID" -c "$CONFIG"
else
    echo "Reusing existing KRaft storage at $LOG_DIR"
fi

echo "Starting Kafka broker..."
mkdir -p "$KAFKA_HOME/logs"
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG" \
    > "$KAFKA_HOME/logs/broker-console.log" 2>&1 < /dev/null &

for _ in $(seq 1 30); do
    if ss -ltn 2>/dev/null | grep -q ":${BROKER_PORT} "; then
        echo "Kafka is up on localhost:${BROKER_PORT} (console log: $KAFKA_HOME/logs/broker-console.log)"
        exit 0
    fi
    sleep 1
done

echo "error: Kafka did not open port ${BROKER_PORT} within 30s — check $KAFKA_HOME/logs/broker-console.log" >&2
exit 1