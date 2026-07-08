-- Iceberg table maintenance for the IoT lakehouse.
--
-- Streaming writers commit a fresh set of data files on every Flink checkpoint
-- (~30s), so a busy table accumulates thousands of small Parquet files and a
-- long chain of snapshots. Left unattended this inflates planning time, storage,
-- and metadata size. Run these procedures on a schedule (e.g. hourly compaction,
-- daily expiry) against the Trino container.
--
-- Usage:  docker exec -i docker-trino-1 trino --catalog iceberg --schema iot < scripts/maintenance.sql
--
-- Retention thresholds default to Trino's 7-day floor; going lower needs
-- iceberg.expire-snapshots.min-retention / iceberg.remove-orphan-files.min-retention
-- raised in the catalog properties first.

-- 1. Compaction: rewrite small files into larger ones (bin-packing).
ALTER TABLE telemetry EXECUTE optimize;
ALTER TABLE telemetry_upsert EXECUTE optimize;
ALTER TABLE telemetry_1m_agg EXECUTE optimize;

-- 2. Expire old snapshots so time-travel history and metadata don't grow forever.
ALTER TABLE telemetry EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE telemetry_upsert EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE telemetry_1m_agg EXECUTE expire_snapshots(retention_threshold => '7d');

-- 3. Remove orphan files left by failed writes / expired snapshots.
ALTER TABLE telemetry EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE telemetry_upsert EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE telemetry_1m_agg EXECUTE remove_orphan_files(retention_threshold => '7d');
