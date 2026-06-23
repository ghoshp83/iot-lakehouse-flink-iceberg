# IoT Lakehouse — Flink + Iceberg

End-to-end streaming lakehouse for IoT telemetry: MQTT sensors feed Kafka via a Schema Registry bridge, Flink streams the data into Apache Iceberg tables on S3-compatible storage, queryable from any SQL engine.

## Architecture

```mermaid
flowchart LR
    subgraph Devices
        S[IoT Sensors<br/>9 devices × 3 sites]
    end

    subgraph Ingest
        HMQ[HiveMQ CE<br/>MQTT 1883]
        BR[Python Bridge<br/>Protobuf + Schema Registry]
    end

    subgraph Streaming
        K[Kafka 4<br/>KRaft · 3 partitions]
        SR[Schema Registry<br/>Confluent 8.2]
        FL[Flink 2.2<br/>Java 17]
    end

    subgraph Lakehouse
        ICE[Iceberg 1.10<br/>Parquet files]
        NES[Nessie Catalog<br/>Git-like branching]
        MINIO[MinIO<br/>S3-compatible]
    end

    S -->|MQTT| HMQ
    HMQ -->|subscribe| BR
    BR -->|Protobuf + key| K
    K --- SR
    K -->|consume| FL
    FL -->|checkpoint commit| ICE
    ICE --- NES
    ICE -->|Parquet| MINIO
```

**Data flow:** Simulated IoT devices publish JSON readings over MQTT to HiveMQ. A Python bridge subscribes, serializes to Protobuf via Schema Registry, and produces to Kafka (keyed by `device_id`). Flink consumes the Protobuf stream, strips the Confluent wire-format header, parses with the generated Java class, maps records to Iceberg RowData, and sinks Parquet files to MinIO through the Nessie catalog. Every Flink checkpoint triggers an Iceberg commit — the table is always consistent.

## Run the full pipeline locally

**Prerequisites:** Docker (with Compose), Java 17, Maven, Python 3.10+, `grpcio-tools` (`pip install grpcio-tools`).

```bash
# 1. Start the stack (healthchecks gate start order; kafka-init creates topics; minio-init creates bucket)
cd docker && docker compose up -d

# 2. Build the Flink fat jar (also generates Java Protobuf classes)
cd ../flink-jobs && MAVEN_OPTS="-Xmx3g" mvn clean package -DskipTests

# 3. Install Python dependencies and generate Protobuf stubs
cd ../sample-data && pip install -r requirements.txt
cd .. && bash scripts/gen_proto.sh

# 4. Start the simulator and bridge (two terminals)
cd sample-data
python3 sensor_simulator.py --rate-hz 5
python3 mqtt_kafka_bridge.py

# 5. Submit the Flink job (append mode; or use KafkaToIcebergUpsertJob for upsert)
docker cp ../flink-jobs/target/iot-lakehouse-flink-0.1.0-SNAPSHOT.jar \
  docker-flink-jobmanager-1:/tmp/job.jar
docker exec docker-flink-jobmanager-1 flink run -d /tmp/job.jar
```

After ~30 seconds the first checkpoint commits and Iceberg rows appear on MinIO:

```bash
# Verify Kafka offsets
docker exec kafka kafka-get-offsets \
  --bootstrap-server localhost:9092 --topic iot.telemetry

# Browse Iceberg table files on MinIO
docker run --rm --network docker_lakehouse --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 admin admin12345 >/dev/null && \
   mc ls --recursive local/warehouse/iot/"
```

## Why this stack

| Choice | Rationale |
|--------|-----------|
| **Kafka 4 (KRaft)** | No ZooKeeper dependency — single-process combined controller+broker. Simpler ops, matches the direction all new Kafka deployments are heading. |
| **Confluent Platform 8.2** | Provides Schema Registry + production-grade Kafka images. The `.proto` file is the single contract between the Python bridge and the Java Flink job. |
| **Protobuf wire format** | Compact binary encoding, backward-compatible evolution via field numbers, language-neutral codegen. Better fit for IoT than JSON or Avro — smaller payloads and faster serialization. |
| **Flink 2.2 on Java 17** | Long Term Support release. Native Iceberg sink with checkpoint-driven commits — no custom logic for exactly-once delivery to the table. |
| **Iceberg 1.10** | Open table format with ACID guarantees, schema evolution, and time travel. Decouples storage from compute — any SQL engine can read the table. |
| **Nessie** | Git-like catalog branching for the Iceberg table. Locally simulates what Glue or Unity Catalog provide in cloud deployments. |
| **MinIO** | S3-compatible object storage for local development. Iceberg's `S3FileIO` talks to it identically to real S3. |
| **HiveMQ CE** | Production-grade MQTT broker (not Mosquitto). CE is free; the protocol layer is identical to Enterprise for basic pub/sub. |
| **Smooth Gaussian-walk sensor values** | Synthetic data with realistic drift so a downstream anomaly-detection project has a stable baseline to learn against. |

## Operational characteristics

| Metric | Observed at M3 |
|--------|---------------|
| End-to-end lossless delivery | 57,042 Kafka offsets = 57,042 Iceberg rows (exact match across `kafka-get-offsets`, Iceberg snapshot `total-records`, and `pyarrow` row count) |
| Checkpoint interval | 30 s (configurable) |
| Iceberg commit trigger | Every successful Flink checkpoint |
| Fat jar size | ~151 MB (Iceberg + AWS bundle dominates; slimming deferred) |
| Simulator throughput | 9 devices × 5 Hz = 45 msgs/s sustained |

## Repository layout

| Directory | Purpose |
|-----------|---------|
| `docker/` | docker-compose stack: HiveMQ, Kafka (KRaft), Schema Registry, MinIO + `minio-init` bootstrap, Nessie, Flink JM+TM |
| `sample-data/` | Python MQTT sensor simulator + Protobuf bridge (Confluent SR) |
| `flink-jobs/` | Maven module: `KafkaToIcebergJob` consumes Protobuf from Kafka, maps to RowData, sinks Parquet via Nessie catalog |
| `proto/telemetry.proto` | Single source of truth for the on-the-wire schema (Protobuf) |
| `scripts/` | Proto codegen helper (`gen_proto.sh` for Python stubs; Java via Maven) |

## Honest disclaimer

| Feature | Status | Notes |
|---------|--------|-------|
| MQTT → Kafka → Flink → Iceberg pipeline | **Real, working** | Verified lossless at 57K rows |
| Kafka 4 KRaft (no ZooKeeper) | **Real** | Single-node; multi-broker config is documented but not exercised |
| Iceberg table with Parquet on MinIO | **Real** | Append-only job + upsert job (equality deletes on `device_id, ts`) |
| Iceberg upsert (equality deletes) | **Real** | `KafkaToIcebergUpsertJob` with `format-version=2` and merge-on-read |
| Schema evolution | **Real** | `firmware_version` field added to proto + Iceberg schema; pre-evolution Parquet files still read (null for new column) |
| Protobuf wire format via Schema Registry | **Real** | `proto/telemetry.proto` is the schema contract; SR handles compatibility |
| Sensor simulator | **Illustrative** | Synthetic Gaussian-walk data; not a real device fleet |
| Healthchecks per service | **Real** | Every service has `healthcheck:` blocks; `depends_on: condition: service_healthy` ensures start order |
| DLQ topic | **Real** | `iot.telemetry.dlq` created on boot by `kafka-init`; retention 30 days |
| Time travel demo | **Real** | `scripts/time_travel_demo.py` queries the table as-of any snapshot via pyiceberg |
| Monitoring (Prometheus + Grafana) | **Planned** | Not yet wired |
| CI pipeline | **Planned** | No `.github/workflows/` yet |
| DLQ routing in Flink | **Planned** | DLQ topic exists; Flink-side routing of deserialization failures is not yet wired |

## Roadmap

- **Flink state + exactly-once** — RocksDB backend, S3 checkpoints, savepoints
- **Monitoring** — Prometheus + Grafana dashboards for throughput, lag, checkpoint duration
- **CI** — GitHub Actions with Maven build + Testcontainers integration tests

## License

[MIT](LICENSE)
