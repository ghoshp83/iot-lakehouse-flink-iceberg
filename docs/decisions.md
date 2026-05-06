# Design decisions log

Running log of non-obvious choices. Update as decisions are made or revised.

## 2026-04-15 — Kick-off

- **Broker**: HiveMQ CE (not Mosquitto) — reflects production IoT deployments and your target stack.
- **Catalog**: Nessie locally for git-like branching of table state; Glue in AWS deployment.
- **Storage**: MinIO locally as S3-compatible store; real S3 in AWS deployment.
- **Compute**: Flink 1.18 Java jobs (matches Flink_Unstructured_Data_Processor conventions for reuse).
- **Query layer choice deferred**: Snowflake vs Trino — decide at M5 based on which gives better Iceberg external-table UX at that time.
- **Port remap**: Flink UI → 8082 (HiveMQ control center uses 8080, Schema Registry uses 8081).

## 2026-05-01 — M1 stack-up findings

- **Nessie registry**: `projectnessie/nessie` on Docker Hub stops at 0.76.6 (Sep-2024). Newer versions including 0.99.0 publish to **GHCR** at `ghcr.io/projectnessie/nessie:<v>`. Compose updated.
- **HiveMQ CE has no HTTP control center**: the 8080 mapping in the original skeleton was based on the Enterprise edition's control center (a paid feature). CE only exposes MQTT (1883) and MQTT-over-WebSocket (8000). 8080 mapping replaced with 8000.
- **Healthchecks deferred to M2**: every service comes up but `docker compose ps` only reports `running`, not `healthy`. M1 health verification done manually via curl/nc. M2 should add `healthcheck:` blocks per service so downstream `depends_on: condition: service_healthy` works and `compose ps` self-reports.
- **M1 verified healthy 2026-05-01 ~08:18 UTC**: ZK 3.8.4 (2 connections), Kafka broker id=1 API responsive, Schema Registry returns `[]` on `/subjects`, HiveMQ MQTT 1883 reachable + started in 2.2s, MinIO `/minio/health/live` 200 + console on 9001, Nessie API v2 created repo (defaultBranch=main), Flink 1.18.1 JM + 1 TM with 4 slots on UI port 8082.

## 2026-05-06 — M2 build + ZK persistence fix

- **Kafka cluster-ID mismatch on 2nd boot** (`InconsistentClusterIdException`): Kafka has a `kafka-data` volume but Zookeeper had no volume in M1 compose, so ZK's `clusterId` regenerated on restart while Kafka kept its old `meta.properties`. Fix: added `zk-data` (`/var/lib/zookeeper/data`) and `zk-log` (`/var/lib/zookeeper/log`) volumes to the zookeeper service. Recovered by `compose down -v` then `up`. Without this fix the stack only worked for one boot cycle.
- **MQTT → Kafka stand-in**: M2 ships a Python `mqtt_kafka_bridge.py` (paho-mqtt subscriber → kafka-python producer, keyed by `device_id`). M3 will replace this with a Flink job that does the same hop plus Avro parsing into Iceberg-ready rows. Topic shape: MQTT `sensors/<site>/<device>/telemetry` → Kafka `iot.telemetry` (3 partitions). Schema is JSON for M2; switching to Avro + Schema Registry comes with the M3 Flink rewrite.
- **Sensor schema** (locked for M2; revisit if Iceberg ergonomics push back): `device_id`, `site_id`, `ts` (ISO-8601 UTC), `temperature_c`, `humidity_pct`, `pressure_hpa`, `vibration_g`. Smooth Gaussian-walk values per device so iot-anomaly-ml has a stable baseline to learn against.
- **M2 verified 2026-05-06**: 9 devices × 3 sites × 5 Hz simulator ran ~33 s, produced 1494 MQTT messages, all 1494 forwarded to Kafka (offsets 498+332+664=1494 across the 3 partitions). End-to-end lossless.
