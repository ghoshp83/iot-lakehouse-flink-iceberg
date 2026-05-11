# iot-lakehouse-flink-iceberg

End-to-end IoT lakehouse on the modern stack: **Kafka 4 (KRaft) · Flink 2.2 · Java 17 · Iceberg 1.10 · Confluent Platform 8.2 · Nessie · MinIO**. Runs locally with one `docker compose up`; M3 ships a real, queryable Iceberg table.

```
  ┌─────────┐  MQTT    ┌────────┐    ┌────────┐    ┌───────┐    ┌──────────┐    ┌──────────┐    ┌─────────────┐
  │ sensors │ ───────▶ │ HiveMQ │ ─▶ │ bridge │ ─▶ │ Kafka │ ─▶ │  Flink   │ ─▶ │  MinIO   │ ─▶ │ Snowflake / │
  └─────────┘          │ broker │    │ Avro+SR│    │ KRaft │    │  jobs    │    │ Iceberg  │    │   Trino     │
                       └────────┘    └────────┘    └───┬───┘    └────┬─────┘    └─────┬────┘    └─────────────┘
                                                      │             │                │
                                                      ▼             ▼                ▼
                                                  ┌──────────────┐ ┌──────────┐
                                                  │   Schema     │ │  Nessie  │
                                                  │   Registry   │ │  catalog │
                                                  └──────────────┘ └──────────┘
```

| Stage | Status | Notes |
|---|---|---|
| M1 — Local docker-compose stack | ✅ 2026-05-01 | All services healthy (ZK dropped — Kafka 4 KRaft) |
| M2 — MQTT simulator → Kafka | ✅ 2026-05-06 | 1494 msgs lossless across 3 partitions, keyed by `device_id` |
| M3a — Flink Kafka (Avro/SR) → print | ✅ 2026-05-10 | 30,150 records deserialized; proves the Avro read path |
| M3 — Flink Kafka → Iceberg on MinIO | ✅ 2026-05-11 | **57,042 Kafka offsets = 57,042 Iceberg rows**, exact match across `kafka-get-offsets`, Iceberg snapshot `total-records`, and `pyarrow` row count |
| M4 — Windowed aggregations | ⏳ | Per-device rolling stats into a second Iceberg table |
| M5 — Snowflake external tables over Iceberg | ⏳ | Or Trino — choice deferred to M5 |
| M6 — Prometheus + Grafana + CI + jar slimming | ⏳ | |

## Run the full pipeline locally

```bash
# 1. Stack up (Kafka KRaft self-bootstraps; minio-init creates the warehouse bucket idempotently)
cd docker && docker compose up -d

# 2. Create the topic
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic iot.telemetry --partitions 3 --replication-factor 1

# 3. Build the Flink fat jar (Java 17 + Maven; takes ~12s + ~3g heap)
cd ../flink-jobs && MAVEN_OPTS="-Xmx3g" mvn clean package -DskipTests

# 4. Install Python deps for the simulator + Avro bridge
cd ../sample-data && pip install -r requirements.txt

# 5. Start the simulator and the bridge (two terminals)
python3 sensor_simulator.py --rate-hz 5
python3 mqtt_kafka_bridge.py    # publishes Avro to Schema Registry

# 6. Submit the Flink job
docker cp ../flink-jobs/target/iot-lakehouse-flink-0.1.0-SNAPSHOT.jar \
  docker-flink-jobmanager-1:/tmp/job.jar
docker exec docker-flink-jobmanager-1 flink run -d /tmp/job.jar
```

After ~30 s the first checkpoint commits and the Iceberg table starts filling on MinIO:

```bash
# Browse the table layout (data files + manifests + metadata json under iot/telemetry_<uuid>/)
docker run --rm --network docker_lakehouse --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 admin admin12345 >/dev/null && \
   mc ls --recursive local/warehouse/iot/"
```

Verify the round-trip is lossless:

```bash
# Kafka offsets
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic iot.telemetry
# iot.telemetry:0:12676
# iot.telemetry:1:6338
# iot.telemetry:2:38028  → 57,042 total

# Iceberg snapshot summary self-reports the same total — no external query engine needed
```

## Layout

| Dir | Purpose |
|---|---|
| `docker/` | docker-compose stack: HiveMQ, Kafka (KRaft), Schema Registry, MinIO + `minio-init` bootstrap, Nessie, Flink JM+TM |
| `sample-data/` | Python MQTT sensor simulator + Avro bridge (Confluent SR) — the bridge exists because HiveMQ CE has no native Kafka connector |
| `flink-jobs/` | Maven module for the Java Flink job. `KafkaToIcebergJob` reads Avro from SR, maps Telemetry → RowData, sinks Parquet to `s3://warehouse/iot/telemetry/` via Nessie catalog |
| `flink-jobs/src/main/avro/telemetry.avsc` | Single source of truth for the on-the-wire schema — both the bridge and the Flink job read it |
| `docs/decisions.md` | Running log of non-obvious choices (Iceberg dep dance, KRaft cluster IDs, Avro logical types, checkpoint→commit, etc.) |
| `docs/progress.md` | Live milestone tracker |

## Why this exists

A practical reference for the streaming-lakehouse pattern that's becoming the default for IoT and CDC workloads — open-table format (Iceberg) with a streaming compute engine (Flink) on object storage, queryable from any SQL engine. The deliberate constraints:

- **HiveMQ CE** (not Mosquitto) — matches what shows up in production IoT
- **Nessie** locally for git-like branching of table state; Glue in AWS deployment
- **Kafka 4 KRaft** — no ZooKeeper, one less service in the topology
- **Smooth Gaussian-walked sensor values** so a downstream anomaly-detection project (`iot-anomaly-ml`) has a stable baseline to learn against

Decisions and trade-offs as they're made: [docs/decisions.md](docs/decisions.md).
