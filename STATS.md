# Kafka Throughput Stats

## 2026-09-01 — Raspberry Pi producer, live over LAN

**Measured:**
- **~143 KB/s** (1,470,463 bytes / 10.01s), measured via on-disk log growth of the
  `freqscan.signals` topic partitions (`du -sb /tmp/kraft-combined-logs/freqscan.signals-*`),
  sampled 10s apart.
- **~1,186 messages/s** (2,028,150 − 2,016,290 = 11,860 msgs / 10s), measured via
  `kafka-get-offsets.sh --bootstrap-server <broker> --topic freqscan.signals`, summed across
  all 6 partitions, sampled 10s apart.
- Implied average message size: ~120 bytes (consistent with small flagged-bin JSON payloads).

Both measurements were taken over the same kind of 10-second window, independently
(disk bytes vs. partition offsets), and agree with each other.

**Note**: this is `freqscan.signals` only — noise-floor-*flagged* bins (see CLAUDE.md item 6),
not the Pi's full raw sweep throughput. The flagged rate depends heavily on the detector's
margin settings and how much actual signal activity is present, not just on scan
configuration.

**Broker/topic config** (this PC, `.env`, shared infrastructure both sides talk to):
```
KAFKA__BOOTSTRAP_SERVERS=192.168.88.35:9092
KAFKA__TOPIC=freqscan.signals
KAFKA__METADATA_TOPIC=freqscan.signals.metadata
KAFKA__SECURITY_PROTOCOL=PLAINTEXT
```
`freqscan.signals` provisioned with 6 partitions (see `scripts/start_local_kafka.sh`).

**Pi-side RF/detector config** (Pi's own `.env`, producing the data measured above):

RTL backend disabled (both `RTL__DEVICES` lines commented out) — only HackRF is active.

```
HACKRF__LNA_GAIN=32
HACKRF__VGA_GAIN=20
HACKRF__AMP_ENABLE=false
HACKRF__BIN_WIDTH=50000
HACKRF__RANGES=[{"freq_start":850,"freq_stop":950,"edge_trim":0.02},{"freq_start":2300,"freq_stop":2500,"edge_trim":0.05},{"freq_start":5700,"freq_stop":5900,"edge_trim":0.05}]
```
3 ranges (850-950MHz, 2300-2500MHz, 5700-5900MHz), 500MHz total swept bandwidth,
50kHz bin width → ~10,000 bins across all 3 channels.

Detector (`KAFKA__*`, same file):
```
KAFKA__BASELINE_WINDOW=25
KAFKA__SIGNAL_MARGIN_DB=8.0
KAFKA__SPATIAL_MARGIN_DB=8.0
KAFKA__PROMINENCE_MARGIN_DB=20.0
KAFKA__RTL_PROMINENCE_MARGINS_DB={"0":12.0}   # inactive — RTL backend not running
```
No per-HackRF-range margin overrides set — all 3 ranges use the defaults above.
