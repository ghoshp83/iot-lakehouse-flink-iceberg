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

## 2026-05-10 — Stack version bump + M3a half-pipeline

Bumped the whole stack mid-build (M2 → M3) rather than carrying the older versions for the rest of the milestones.

- **Kafka 4 (KRaft, no ZooKeeper)** — Confluent Platform 8.2 / Apache Kafka 4.2. Confluent dropped ZK in CP 8.0; running 8.2 means a single-node combined controller+broker process with `KAFKA_PROCESS_ROLES=broker,controller`. Net win: one less service, simpler ops story for the public README.
- **Flink 2.2 on Java 17** (`flink:2.2.0-java17`). Flink 2 is Long Term Support; Java 17 LTS is the default for new builds.
- **Connectors**: `flink-connector-kafka:4.0.1-2.0` (paired with Flink 2.0 ABI), `iceberg-flink-runtime-2.0:1.10.1`, `iceberg-nessie:1.10.1`, `iceberg-aws-bundle:1.10.1`. All already in pom even though only Avro+Kafka are used at M3a — surfaces dep conflicts before the M3-final sink rewrite.
- **KRaft cluster_id format**: must be a 22-char base64 string. First attempt with `iot-lakehouse-cluster-1` failed format/storage-tool validation; fixed to `MkU3OEVBNTcwNTJENDM2Qk` and pinned in compose so the kafka-data volume is durable across restarts. The `kafka-data` and `zk-data`/`zk-log` volumes were dropped (`compose down -v`) on the version flip — one-time cost.
- **`ParameterTool` removed in Flink 2.0**. The job uses a 7-line manual `--key value` arg parser instead. Replacement APIs (`Configuration.fromMap`, `GlobalConfiguration`) exist but aren't worth the import surface for four optional knobs.
- **Avro `timestamp-millis` decodes to `java.time.Instant`, not `long`**. The print formatter initially used `%d` and threw `IllegalFormatConversionException`. Switched to `%s` (Instant has a sensible `toString`). Worth knowing for the Iceberg sink — the Iceberg Flink connector handles `Instant` → `timestamptz` natively via Avro logical types, so no manual conversion needed there either.
- **Bridge serializer**: switched from `kafka-python` (raw bytes) to `confluent-kafka[avro]` with `AvroSerializer` + `SchemaRegistryClient`. Schema is loaded from the same `.avsc` the Flink job code-gens against, so the bridge and the consumer share one source of truth. Subject `iot.telemetry-value` auto-registers on first publish.
- **Partition skew**: with 9 device keys hashing across 3 partitions via `confluent-kafka`'s default murmur2, observed offsets ran roughly 1:0.5:3 instead of 1:1:1. Acceptable at this fleet size; revisit only if real-world key cardinality stays this small (which it won't in practice).

## 2026-05-11 — M3 final (Iceberg sink swap)

The print sink became a Nessie-catalog Iceberg sink writing Parquet on MinIO. `KafkaToPrintJob` renamed to `KafkaToIcebergJob`. Five things were non-obvious enough to be worth recording.

- **Checkpointing is the commit trigger**. `FlinkSink.forRowData(...).append()` writes files continuously but only *commits* them to the Iceberg table on a successful Flink checkpoint. Without `env.enableCheckpointing(30_000)`, the bucket fills up with Parquet files but the Iceberg snapshot stays empty — `total-records=0` forever. 30 s keeps the demo snappy; production tunes this for the write-amplification vs visibility-latency trade-off.
- **Iceberg verification = snapshot summary == Kafka offsets**. Iceberg's own snapshot metadata json reports `total-records` per snapshot, which can be checked against `kafka-get-offsets`. End-to-end: `12676+6338+38028 = 57042` Kafka offsets ⇒ `57042` in `total-records` ⇒ `57042` from `sum(pyarrow.parquet.read_table(f).num_rows)` across the 8 data files. Three independent counts agreeing is the strongest "lossless" claim this stack can make from inside Flink alone.
- **MinIO bucket must pre-exist**. S3FileIO doesn't auto-create buckets — only objects. Added a one-shot `minio-init` service to docker-compose that `mc mb -p local/warehouse`s on stack boot. Idempotent; no-ops on subsequent boots.
- **Hadoop's `Configuration` static init is non-negotiable**. `CatalogLoader.custom(name, props, hadoopConfig, impl)` requires a Hadoop `Configuration` instance even though Nessie + S3FileIO never touch HDFS. `new Configuration(false)` looks like it should skip the config-file loading, but the `Configuration` class's *static* initializer still triggers a Woodstox load — so `woodstox-core` + `stax2-api` need to be on the classpath regardless of whether you actually parse any XML. The first job submission failed with `ClassNotFoundException: com.ctc.wstx.io.InputBootstrapper` until those two tiny jars were added.
- **iceberg-flink-runtime expects hadoop-shaded-guava**. Tried blanket-excluding all hadoop-common transitives to keep the jar small; the writer task failed with `NoClassDefFoundError: org.apache.hadoop.thirdparty.com.google.common.collect.Interners`. Iceberg's Hadoop wrappers (`HadoopCatalog`, `HadoopFileIO`, etc.) reach into `org.apache.hadoop.thirdparty.*` whether or not you use them. Final pom keeps hadoop-shaded-guava and prunes only the safe-to-drop transitives (HDFS, YARN, MapReduce, Kerberos servers, Jersey, Jetty). Fat jar lands at 151 MB — to be shrunk in M6.
