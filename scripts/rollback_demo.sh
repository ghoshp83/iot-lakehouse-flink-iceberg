#!/usr/bin/env bash
# Iceberg snapshot rollback: undo a bad commit without rewriting any data.
# Requires: docker-compose stack running with data in the telemetry table.
#
# Every Flink checkpoint commits a new snapshot; rollback just moves the
# table's current-snapshot pointer back to an earlier one. The demo shows the
# row count and snapshot pointer before and after so the effect is visible.
#
# Usage:
#   bash scripts/rollback_demo.sh              # roll back to the previous snapshot
#   bash scripts/rollback_demo.sh <snapshot>   # roll back to a specific snapshot id
set -euo pipefail

TRINO="docker exec -i docker-trino-1 trino --catalog iceberg --schema iot"
TABLE="telemetry"

echo "=== Iceberg Snapshot Rollback Demo ==="

echo ""
echo "1. Snapshot history (newest first):"
$TRINO --execute "SELECT snapshot_id, committed_at, operation
                  FROM \"${TABLE}\$snapshots\" ORDER BY committed_at DESC LIMIT 5"

echo ""
echo "2. Current state:"
$TRINO --execute "SELECT COUNT(*) AS row_count FROM ${TABLE}"

TARGET="${1:-}"
if [ -z "$TARGET" ]; then
    TARGET=$($TRINO --output-format TSV --execute \
        "SELECT snapshot_id FROM \"${TABLE}\$snapshots\"
         ORDER BY committed_at DESC OFFSET 1 LIMIT 1" | tr -d '[:space:]')
fi
if [ -z "$TARGET" ]; then
    echo "No earlier snapshot to roll back to — the table has a single commit."
    exit 1
fi

echo ""
echo "3. Rolling back to snapshot $TARGET ..."
$TRINO --execute "CALL iceberg.system.rollback_to_snapshot('iot', '${TABLE}', ${TARGET})"

echo ""
echo "4. State after rollback (pointer moved, no data rewritten):"
$TRINO --execute "SELECT COUNT(*) AS row_count FROM ${TABLE}"

echo ""
echo "NOTE: the rolled-back-past snapshot still exists until expire_snapshots"
echo "runs (scripts/maintenance.sql), so you can roll FORWARD to it again with"
echo "the same procedure. Stop or restart the writer job before rolling back,"
echo "or its next checkpoint will commit on top of the restored snapshot."
echo ""
echo "=== Done ==="
