# iot-lakehouse-flink-iceberg

> **Status: 🚧 WIP — M2 of 6 complete.** End-to-end pipeline in progress; the Flink → Iceberg sink (M3) lands next. Treat this as a build-in-public — see [docs/progress.md](docs/progress.md) for the live milestone tracker.

End-to-end IoT lakehouse:

```
  ┌─────────┐   MQTT    ┌────────┐        ┌───────┐   Iceberg   ┌──────────┐   SQL   ┌─────────────┐
  │ sensors │ ────────▶ │ HiveMQ │ ─────▶ │ Kafka │ ──────────▶ │  S3 /    │ ──────▶ │ Snowflake / │
  └─────────┘           │ broker │        └───────┘             │  MinIO   │         │   Trino     │
                        └────────┘            │                 │ Iceberg  │         └─────────────┘
                                              ▼                 └──────────┘
                                         ┌────────┐                   ▲
                                         │ Flink  │───────────────────┘
                                         │  jobs  │
                                         └────────┘
```

| Stage | Status | Notes |
|---|---|---|
| M1 — Local docker-compose stack | ✅ 2026-05-01 | All 8 services healthy on first boot |
| M2 — MQTT simulator → Kafka | ✅ 2026-05-06 | 1494 msgs lossless across 3 partitions, keyed by `device_id` |
| M3 — Flink Kafka → Iceberg sink (MinIO) | ⏳ next | Java Flink job, Avro + Schema Registry |
| M4 — Windowed aggregations | ⏳ | Per-device rolling stats |
| M5 — Snowflake external tables over Iceberg | ⏳ | Or Trino — choice deferred to M5 |
| M6 — Prometheus + Grafana + CI + docs polish | ⏳ | |

## Run the M2 pipeline locally

```bash
# 1. Stack up
cd docker && docker compose up -d

# 2. Wait for Kafka, then create the topic
docker exec docker-kafka-1 kafka-topics \
  --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic iot.telemetry --partitions 3 --replication-factor 1

# 3. Install Python deps
cd ../sample-data && pip install -r requirements.txt

# 4. In one shell — the bridge (MQTT subscriber → Kafka producer)
python3 mqtt_kafka_bridge.py

# 5. In another shell — the simulator (9 devices × 3 sites)
python3 sensor_simulator.py --rate-hz 5

# 6. Verify in a third shell
docker exec docker-kafka-1 kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic iot.telemetry \
  --from-beginning --max-messages 5 --property print.key=true
```

You should see JSON payloads keyed by `device_id`, e.g.
`site-london-dev-00 :: {"device_id": "...", "temperature_c": 22.1, ...}`.

## Layout

| Dir | Purpose |
|---|---|
| `docker/` | docker-compose stack: HiveMQ, Kafka, Schema Registry, MinIO, Nessie, Flink JM+TM |
| `sample-data/` | Python MQTT sensor simulator + MQTT→Kafka bridge (M2 stand-in; M3 replaces the bridge with Flink) |
| `flink-jobs/` | Java Flink jobs — to be filled in at M3 |
| `docs/decisions.md` | Running log of non-obvious choices |
| `docs/progress.md` | Live milestone tracker |

## Why this exists

A practical reference for the streaming-lakehouse pattern that's becoming the default for IoT and CDC workloads — open-table format (Iceberg) with a streaming compute engine (Flink) on object storage, queryable from any SQL engine. The deliberate constraints:

- HiveMQ CE (not Mosquitto) — matches what shows up in production IoT
- Nessie locally for git-like branching of table state; Glue in AWS deployment
- Smooth Gaussian-walked sensor values so a downstream anomaly-detection project (`iot-anomaly-ml`) has a stable baseline to learn against

Decisions and trade-offs as they're made: [docs/decisions.md](docs/decisions.md).
