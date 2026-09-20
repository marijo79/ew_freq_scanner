#!/usr/bin/env bash
# Start (and, if needed, first-time-format) a local native Kafka broker for smoke-testing
# freqscan's streaming path without touching MSK. KRaft storage lives under log.dirs from
# server.properties (default install puts it in /tmp, which most systems wipe on reboot —
# this script reformats it automatically whenever it's missing).
#
# It also ensures the signals + metadata topics exist with enough partitions — a topic
# auto-created by the first produce (e.g. right after a storage wipe/reformat) comes back
# with Kafka's default of 1 partition, which then throws _UNKNOWN_PARTITION as soon as a
# backend tries to publish a channel numbered >= 1 (build_backend() assigns each channel a
# fixed partition, see CLAUDE.md item 6 — there's no fallback if the topic is too small).
# This runs every time, even if the broker was already up, since topic partition counts
# aren't touched by a broker restart.
#
#   scripts/start_local_kafka.sh
#   KAFKA_HOME=/path/to/kafka scripts/start_local_kafka.sh   # override install location
#   KAFKA_TOPIC=... KAFKA_METADATA_TOPIC=... TOPIC_PARTITIONS=6 scripts/start_local_kafka.sh
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
# it isn't hot-reloaded. This LAN IP can also change on its own (new network/router) even when
# nothing about the Kafka setup changed — re-check it against `hostname -I` whenever a remote
# producer/viewer suddenly can't connect, and update both server.properties and every machine's
# own .env (KAFKA__BOOTSTRAP_SERVERS) to match.

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka/kafka_2.13-4.3.1}"
CONFIG="$KAFKA_HOME/config/server.properties"
BROKER_PORT="${BROKER_PORT:-9092}"
SIGNALS_TOPIC="${KAFKA_TOPIC:-freqscan.signals}"
METADATA_TOPIC="${KAFKA_METADATA_TOPIC:-freqscan.signals.metadata}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-6}"
# ADMIN_BOOTSTRAP/KAFKA_CLIENT_CONFIG: for a broker secured with SASL_SSL (e.g. the
# pi-remote-access tunnel VM's broker, see project_vm_kafka_broker memory) instead of
# this script's default open-localhost dev setup — "localhost" won't pass that broker's
# TLS hostname check (the cert only covers the broker's real hostname), and an
# unauthenticated kafka-topics.sh call gets rejected outright, not just a plaintext
# no-op, once the listener is SASL_SSL-only. Both default to the plain local-broker
# behavior (localhost, no --command-config) so this is backward compatible.
#   ADMIN_BOOTSTRAP=marius.vilimas.net:9092 KAFKA_CLIENT_CONFIG=~/kafka/tls/admin-client.properties \
#     KAFKA_HOME=~/kafka/kafka_2.13-4.3.1 scripts/start_local_kafka.sh
ADMIN_BOOTSTRAP="${ADMIN_BOOTSTRAP:-localhost:${BROKER_PORT}}"
CLIENT_CONFIG_ARGS=()
if [[ -n "${KAFKA_CLIENT_CONFIG:-}" ]]; then
    CLIENT_CONFIG_ARGS=(--command-config "$KAFKA_CLIENT_CONFIG")
fi

if [[ ! -f "$CONFIG" ]]; then
    echo "error: no server.properties at $CONFIG (set KAFKA_HOME to your Kafka install dir)" >&2
    exit 1
fi

ensure_topic() {
    local topic="$1" desired="$2" describe current
    if describe="$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$ADMIN_BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" --describe --topic "$topic" 2>/dev/null)"; then
        current="$(grep -oP '(?<=PartitionCount: )\d+' <<<"$describe" | head -1)"
        if [[ "$current" -lt "$desired" ]]; then
            echo "Topic $topic has $current partition(s), raising to $desired..."
            "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$ADMIN_BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" --alter --topic "$topic" --partitions "$desired"
        else
            echo "Topic $topic already has $current partition(s) (>= $desired) — leaving as-is."
        fi
    else
        echo "Creating topic $topic with $desired partition(s)..."
        "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$ADMIN_BOOTSTRAP" "${CLIENT_CONFIG_ARGS[@]}" --create --topic "$topic" --partitions "$desired" --replication-factor 1
    fi
}

if ss -ltn 2>/dev/null | grep -q ":${BROKER_PORT} "; then
    echo "Kafka already listening on localhost:${BROKER_PORT} — skipping broker startup."
else
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

    up=""
    for _ in $(seq 1 30); do
        if ss -ltn 2>/dev/null | grep -q ":${BROKER_PORT} "; then
            up=1
            break
        fi
        sleep 1
    done

    if [[ -z "$up" ]]; then
        echo "error: Kafka did not open port ${BROKER_PORT} within 30s — check $KAFKA_HOME/logs/broker-console.log" >&2
        exit 1
    fi
    echo "Kafka is up on localhost:${BROKER_PORT} (console log: $KAFKA_HOME/logs/broker-console.log)"
fi

ensure_topic "$SIGNALS_TOPIC" "$TOPIC_PARTITIONS"
ensure_topic "$METADATA_TOPIC" "$TOPIC_PARTITIONS"
