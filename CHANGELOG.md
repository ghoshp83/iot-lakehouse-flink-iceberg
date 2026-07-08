# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
