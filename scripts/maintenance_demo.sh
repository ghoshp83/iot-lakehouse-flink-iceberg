#!/usr/bin/env bash
# Iceberg table maintenance: compaction, snapshot expiry, orphan-file removal.
# Requires: docker-compose stack running with data in the telemetry tables.
#
# Shows the small-files problem before/after so the effect is visible: it prints
# the data-file count from Iceberg's "$files" metadata table, compacts, then
# prints it again.
set -euo pipefail

TRINO="docker exec -i docker-trino-1 trino --catalog iceberg --schema iot"

echo "=== Iceberg Maintenance Demo ==="

echo ""
echo "1. Data-file count BEFORE compaction (small files from per-checkpoint commits):"
$TRINO --execute "SELECT COUNT(*) AS data_files FROM \"telemetry\$files\""

echo ""
echo "2. Running compaction + snapshot expiry + orphan removal (scripts/maintenance.sql):"
$TRINO < "$(dirname "$0")/maintenance.sql"

echo ""
echo "3. Data-file count AFTER compaction (small files bin-packed into fewer, larger files):"
$TRINO --execute "SELECT COUNT(*) AS data_files FROM \"telemetry\$files\""

echo ""
echo "4. Remaining snapshots after expiry:"
$TRINO --execute "SELECT COUNT(*) AS snapshots FROM \"telemetry\$snapshots\""

echo ""
echo "=== Done ==="
