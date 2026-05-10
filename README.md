# iot-lakehouse-flink-iceberg

> **Status: 🚧 WIP — M3 half-pipeline complete (M2 + Avro/Flink read path).** The Flink → Iceberg *sink* swap is the next slice. Treat this as a build-in-public — see [docs/progress.md](docs/progress.md) for the live milestone tracker.

End-to-end IoT lakehouse on the modern stack: **Kafka 4 (KRaft) · Flink 2.2 · Java 17 · Iceberg 1.10 · Confluent Platform 8.2**.

```
  ┌─────────┐  MQTT    ┌────────┐    ┌────────┐    ┌───────┐    ┌──────────┐    ┌──────────┐    ┌─────────────┐
  │ sensors │ ───────▶ │ HiveMQ │ ─▶ │ bridge │ ─▶ │ Kafka │ ─▶ │  Flink   │ ─▶ │  S3 /    │ ─▶ │ Snowflake / │
  └─────────┘          │ broker │    │ Avro+SR│    │ KRaft │    │  jobs    │    │  MinIO   │    │   Trino     │
                       └────────┘    └────────┘    └───┬───┘    └──────────┘    │ Iceberg  │    └─────────────┘
                                                       │             ▲          └──────────┘
                                                       ▼             │
                                                  ┌─────────────────────┐
                                                  │  Schema Registry    │
                                                  └─────────────────────┘
```

| Stage | Status | Notes |
|---|---|---|
| M1 — Local docker-compose stack | ✅ 2026-05-01 | All 7 services healthy (ZK dropped — Kafka 4 KRaft) |
| M2 — MQTT simulator → Kafka | ✅ 2026-05-06 | 1494 msgs lossless across 3 partitions, keyed by `device_id` |
| M3a — Flink Kafka (Avro/SR) → print | ✅ 2026-05-10 | 30,150 records deserialized + printed; proves the Avro read path |
| M3 — Flink Kafka → Iceberg sink (MinIO) | 🟡 in progress | Iceberg + Nessie deps already in pom; sink swap is the next slice |
| M4 — Windowed aggregations | ⏳ | Per-device rolling stats |
| M5 — Snowflake external tables over Iceberg | ⏳ | Or Trino — choice deferred to M5 |
| M6 — Prometheus + Grafana + CI + docs polish | ⏳ | |

## Run the M3 half-pipeline locally

```bash
# 1. Stack up (Kafka KRaft self-bootstraps; no ZooKeeper)
cd docker && docker compose up -d

# 2. Create the topic
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic iot.telemetry --partitions 3 --replication-factor 1

# 3. Build the Flink fat jar (Java 17 + Maven)
cd ../flink-jobs && MAVEN_OPTS="-Xmx3g" mvn clean package -DskipTests

# 4. Install Python deps for the simulator + Avro bridge
cd ../sample-data && pip install -r requirements.txt

# 5. Start the simulator and bridge (two terminals)
python3 sensor_simulator.py --rate-hz 5
python3 mqtt_kafka_bridge.py    # publishes Avro to Schema Registry

# 6. Submit the Flink job
docker cp ../flink-jobs/target/iot-lakehouse-flink-0.1.0-SNAPSHOT.jar \
  docker-flink-jobmanager-1:/tmp/job.jar
docker exec docker-flink-jobmanager-1 flink run -d /tmp/job.jar

# 7. Watch the print sink
docker logs -f docker-flink-taskmanager-1 | grep "device="
```

Expected output, one line per Avro record:
```
device=site-london-dev-00 site=site-london ts=2026-05-10T19:04:50.576Z temp=22.10 hum=51.30 pres=1012.40 vib=0.0820
```

## Layout

| Dir | Purpose |
|---|---|
| `docker/` | docker-compose stack: HiveMQ, Kafka (KRaft), Schema Registry, MinIO, Nessie, Flink JM+TM |
| `sample-data/` | Python MQTT sensor simulator + Avro bridge (Confluent SR) — bridges stay because HiveMQ CE has no native Kafka connector |
| `flink-jobs/` | Maven module for Java Flink jobs. `KafkaToPrintJob` (M3a) reads Avro from SR; the Iceberg sink replaces the print sink at M3 final |
| `flink-jobs/src/main/avro/telemetry.avsc` | Single source of truth for the on-the-wire schema — both the bridge and the Flink job read it |
| `docs/decisions.md` | Running log of non-obvious choices |
| `docs/progress.md` | Live milestone tracker |

## Why this exists

A practical reference for the streaming-lakehouse pattern that's becoming the default for IoT and CDC workloads — open-table format (Iceberg) with a streaming compute engine (Flink) on object storage, queryable from any SQL engine. The deliberate constraints:

- HiveMQ CE (not Mosquitto) — matches what shows up in production IoT
- Nessie locally for git-like branching of table state; Glue in AWS deployment
- Smooth Gaussian-walked sensor values so a downstream anomaly-detection project (`iot-anomaly-ml`) has a stable baseline to learn against

Decisions and trade-offs as they're made: [docs/decisions.md](docs/decisions.md).
