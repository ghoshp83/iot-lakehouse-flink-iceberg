#!/usr/bin/env bash
# Query Iceberg tables through Trino.
# Requires: docker-compose stack running with data in the telemetry table.
set -euo pipefail

TRINO="docker exec docker-trino-1 trino --catalog iceberg --schema iot"

echo "=== Trino Query Demo ==="

echo ""
echo "1. Tables in the iot namespace:"
$TRINO --execute "SHOW TABLES"

echo ""
echo "2. Row count:"
$TRINO --execute "SELECT COUNT(*) AS total_rows FROM telemetry"

echo ""
echo "3. Latest 10 readings:"
$TRINO --execute "SELECT device_id, ts, temperature_c, humidity_pct, firmware_version FROM telemetry ORDER BY ts DESC LIMIT 10"

echo ""
echo "4. Per-device statistics:"
$TRINO --execute "
  SELECT device_id,
         COUNT(*)            AS readings,
         ROUND(AVG(temperature_c), 2) AS avg_temp,
         ROUND(MIN(temperature_c), 2) AS min_temp,
         ROUND(MAX(temperature_c), 2) AS max_temp
  FROM telemetry
  GROUP BY device_id
  ORDER BY device_id"

echo ""
echo "5. Table snapshots (Iceberg metadata):"
$TRINO --execute "SELECT snapshot_id, committed_at, operation FROM \"telemetry\$snapshots\" ORDER BY committed_at DESC LIMIT 5"

echo ""
echo "=== Done ==="
echo "Grafana: http://localhost:3000  |  Flink UI: http://localhost:8082  |  Trino: localhost:8083"
