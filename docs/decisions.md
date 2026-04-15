# Design decisions log

Running log of non-obvious choices. Update as decisions are made or revised.

## 2026-04-15 — Kick-off

- **Broker**: HiveMQ CE (not Mosquitto) — reflects production IoT deployments and your target stack.
- **Catalog**: Nessie locally for git-like branching of table state; Glue in AWS deployment.
- **Storage**: MinIO locally as S3-compatible store; real S3 in AWS deployment.
- **Compute**: Flink 1.18 Java jobs (matches Flink_Unstructured_Data_Processor conventions for reuse).
- **Query layer choice deferred**: Snowflake vs Trino — decide at M5 based on which gives better Iceberg external-table UX at that time.
- **Port remap**: Flink UI → 8082 (HiveMQ control center uses 8080, Schema Registry uses 8081).
