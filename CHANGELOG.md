# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-07-14

### Added
- **Per-job data-quality metrics** (`TelemetryRouter`): Flink counters
  `records_valid`, `records_parse_failed`, `records_validation_failed`, so DLQ
  volume is visible per-job in the Flink UI and Prometheus/Grafana instead of
  only as traffic on the DLQ topic.
- **Late-event dead-lettering** in the aggregation job: readings that arrive
  after their 1-minute window closed are side-output as `stage=late` DLQ
  records (with `device_id` and `event_ts`) instead of being dropped silently.
  No `allowedLateness` on purpose — the sink is an append-only Iceberg table,
  so a re-fired window would append a duplicate row rather than update one.
- **Idle-source watermarks**: all three jobs mark a Kafka partition idle after
  1 minute of silence, so a dead device or skewed keying can no longer stall
  the combined watermark and stop windows from firing.
- **Snapshot rollback runbook** (`scripts/rollback_demo.sh` + RUNBOOK section):
  undo a bad commit by moving the Iceberg current-snapshot pointer back via
  Trino's `rollback_to_snapshot` — a metadata operation, nothing rewritten.

### Changed
- The valid-vs-dead-letter routing is now a single shared operator
  (`TelemetryRouter`) instead of copies in each job.
- The windowed aggregation job now applies the same physical-plausibility gate
  as the append/upsert jobs and writes its rejects to the DLQ; previously
  out-of-range readings flowed into the per-minute aggregates and parse
  failures were dropped without a trace.
- Version bumped to `0.3.0` (fat jar is now `iot-lakehouse-flink-0.3.0.jar`).

## [0.2.0] - 2026-07-08

### Added
- **Hidden day partitioning** on the Iceberg tables (`IcebergPartitions.byEventDay`):
  `day(ts)` for `telemetry` / `telemetry_upsert` and `day(window_start)` for
  `telemetry_1m_agg`, so time-range queries prune to the days they touch instead
  of scanning all history.
- **Physical-plausibility data-quality gate** (`TelemetryValidator`): readings
  that decode cleanly but are out of range (stuck sensor, NaN, impossible
  humidity) are routed to the DLQ instead of poisoning downstream aggregates.
- **Structured JSON dead-letter records** (`DlqEnvelope`): each rejected record
  carries a `stage` discriminator (`deserialize` vs `validate`), `reason`, and
  `payload_len` / `device_id`, JSON-escaped so the DLQ topic stays queryable.
- **Iceberg table maintenance** (`scripts/maintenance.sql`,
  `scripts/maintenance_demo.sh`): Trino `ALTER TABLE … EXECUTE` compaction,
  snapshot expiry, and orphan-file removal to counter the small-files growth of
  checkpoint-driven streaming writes.

### Changed
- Both telemetry writers now validate readings after a successful decode before
  mapping to Iceberg `RowData`.
- Version bumped to `0.2.0` (fat jar is now `iot-lakehouse-flink-0.2.0.jar`).

## [0.1.0] - 2026-06-24

### Added
- End-to-end pipeline: MQTT → Kafka (Protobuf via Schema Registry) → Flink →
  Iceberg on MinIO via Nessie; append and upsert (equality-delete) jobs.
- 1-minute tumbling-window per-device aggregation job.
- DLQ routing, RocksDB state with S3-backed checkpoints, Prometheus + Grafana
  monitoring, Trino SQL layer, schema-evolution and time-travel demos, CI.

[0.2.0]: https://github.com/ghoshp83/iot-lakehouse-flink-iceberg/releases/tag/v0.2.0
[0.1.0]: https://github.com/ghoshp83/iot-lakehouse-flink-iceberg/releases/tag/v0.1.0
