# Progress

Live milestone tracker. Update each session. Detailed engineering rationale lives in [decisions.md](decisions.md); this file is the at-a-glance status.

## Milestones

| # | Milestone | Status | Target wk | Done | Verification |
|---|---|---|---|---|---|
| M1 | Local docker-compose stack up | ✅ | wk 1–2 | 2026-05-01 | All 8 services up; manual probes (ZK srvr, Kafka API versions, SR `/subjects`, MinIO `/health/live`, Nessie API v2, Flink UI on 8082, HiveMQ MQTT 1883) |
| M2 | MQTT sensor simulator → visible in Kafka | ✅ | wk 3–4 | 2026-05-06 | 1494 sim msgs → 1494 Kafka offsets across 3 partitions, lossless, keyed by `device_id` |
| M3a | Flink job Kafka (Avro/SR) → print sink | ✅ | wk 5–6 | 2026-05-10 | 30,150 Avro records deserialized via Schema Registry and printed by TaskManager. Confirms Avro path before swapping the sink. |
| M3 | Flink job Kafka → Iceberg on MinIO | ✅ | wk 5–6 | 2026-05-11 | 8 Parquet files committed via Nessie catalog; **57,042 rows in table = 57,042 Kafka offsets (exact match)**; Iceberg snapshot summary self-reports the same total. |
| M4 | Windowed aggregations | ⏳ | wk 7–8 | — | Per-device rolling temp/humidity/vibration stats in a second Iceberg table |
| M5 | Snowflake external tables over Iceberg | ⏳ | wk 9–10 | — | Snowflake `SELECT *` over the Iceberg table returns rows; latency acceptable |
| M6 | Prometheus + Grafana + CI + docs polish | ⏳ | wk 11–12 | — | CI green badge; Grafana dashboard screenshot in README |

## Session log

| Date | Slice | Outcome |
|---|---|---|
| 2026-04-15 | Kick-off | Scaffolded `docker/`, `flink-jobs/`, `sample-data/`, `docs/decisions.md`, layout |
| 2026-05-01 | M1 | Stack up; two compose fixes (Nessie GHCR registry, HiveMQ port 8000 not 8080); manual health probes; healthchecks deferred to M2 (still deferred — see Open follow-ups below) |
| 2026-05-06 | M2 + ZK persistence fix | `sensor_simulator.py` (paho → HiveMQ) + `mqtt_kafka_bridge.py` (paho → kafka-python). 9 devices × 3 sites × 5 Hz, end-to-end lossless. Compose fix: `zk-data` + `zk-log` volumes (without them Kafka throws `InconsistentClusterIdException` on 2nd boot because Kafka has a persistent volume but ZK lost its `clusterId` on restart). First WIP push to GitHub. |
| 2026-05-10 | M3a + stack version bump | Stack moved to **Kafka 4 (KRaft, no ZK)** via Confluent Platform 8.2 + **Flink 2.2 on Java 17**. New `flink-jobs/` Maven module (Avro code-gen, fat-jar shade) with `KafkaToPrintJob` reading Avro from Schema Registry. Bridge rewritten with `confluent-kafka` + `AvroSerializer` (subject `iot.telemetry-value` auto-registered). End-to-end verified: 30,150 records deserialized + printed by Flink TM. Iceberg + Nessie deps already in pom — next slice is the sink swap. |
| 2026-05-11 | **M3 final** + repo flips public | Print sink replaced by `FlinkSink.forRowData` writing Parquet to `s3://warehouse/iot/telemetry/` via Nessie catalog. `KafkaToPrintJob` renamed to `KafkaToIcebergJob`. Compose gains a `minio-init` one-shot that idempotently creates the `warehouse` bucket. Three classpath surprises hit + fixed in-line (Hadoop `Configuration` static init pulls Woodstox; `Configuration(false)` does **not** skip the static init; iceberg-flink-runtime expects `org.apache.hadoop.thirdparty.com.google.common.*` from hadoop-shaded-guava). End-to-end verified: 57,042 Kafka offsets → 57,042 Iceberg rows, exact match, confirmed both by `pyarrow.parquet` row count and the Iceberg snapshot's `total-records` summary. Repo flipped to public; M3 LinkedIn post drafted. |

## Open follow-ups (not blocking M4)

- **Healthchecks per service** (deferred from M1). Add `healthcheck:` blocks so `docker compose ps` self-reports `healthy` and `depends_on: condition: service_healthy` works.
- **Flink job test harness**. No tests yet — add a Testcontainers-based integration test in M6.
- **CI**. No `.github/workflows/` yet. Wire in M6.
- **Snowflake vs Trino decision**. Deferred to M5 — pick whichever has the better Iceberg external-table UX at that time.
- **Iceberg fat-jar slimming**. Current fat jar is 151 MB (iceberg-aws-bundle dominates). M6 polish can shade-relocate or split off an S3-only profile.
