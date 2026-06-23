"""Iceberg time-travel demo via pyiceberg.

Connects to the Nessie catalog, lists all snapshots of the telemetry table,
and queries the table as-of each snapshot to show how Iceberg maintains a
complete audit trail of every committed state.

Usage:
    pip install pyiceberg[s3fs]
    python scripts/time_travel_demo.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pyiceberg.catalog import load_catalog


def main() -> int:
    p = argparse.ArgumentParser(description="Iceberg time-travel demo")
    p.add_argument("--nessie-uri", default="http://localhost:19120/api/v2")
    p.add_argument("--warehouse", default="s3://warehouse/")
    p.add_argument("--s3-endpoint", default="http://localhost:9000")
    p.add_argument("--table", default="iot.telemetry")
    args = p.parse_args()

    catalog = load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": args.nessie_uri,
            "warehouse": args.warehouse,
            "s3.endpoint": args.s3_endpoint,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "admin12345",
            "s3.region": "us-east-1",
        },
    )

    table = catalog.load_table(args.table)

    snapshots = list(table.metadata.snapshots)
    if not snapshots:
        print("No snapshots found — run the pipeline first to populate the table.")
        return 1

    print(f"Table: {args.table}")
    print(f"Current schema: {table.schema()}")
    print(f"Total snapshots: {len(snapshots)}\n")

    print("=" * 80)
    print("SNAPSHOT HISTORY")
    print("=" * 80)

    for i, snap in enumerate(snapshots):
        ts = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc)
        summary = snap.summary or {}
        total_records = summary.get("total-records", "?")
        added = summary.get("added-records", "?")
        operation = summary.get("operation", "?")

        print(f"\n  Snapshot {i + 1}/{len(snapshots)}")
        print(f"  ID:         {snap.snapshot_id}")
        print(f"  Timestamp:  {ts.isoformat()}")
        print(f"  Operation:  {operation}")
        print(f"  Added rows: {added}")
        print(f"  Total rows: {total_records}")

    print("\n" + "=" * 80)
    print("TIME TRAVEL QUERIES")
    print("=" * 80)

    first = snapshots[0]
    last = snapshots[-1]

    print(f"\n--- As-of FIRST snapshot (ID {first.snapshot_id}) ---")
    scan_first = table.scan(snapshot_id=first.snapshot_id).to_arrow()
    print(f"  Rows: {len(scan_first)}")
    if len(scan_first) > 0:
        print(f"  Sample:\n{scan_first.slice(0, min(3, len(scan_first))).to_pandas().to_string(index=False)}")

    if first.snapshot_id != last.snapshot_id:
        print(f"\n--- As-of LATEST snapshot (ID {last.snapshot_id}) ---")
        scan_last = table.scan(snapshot_id=last.snapshot_id).to_arrow()
        print(f"  Rows: {len(scan_last)}")
        if len(scan_last) > 0:
            print(f"  Sample:\n{scan_last.slice(0, min(3, len(scan_last))).to_pandas().to_string(index=False)}")

        growth = len(scan_last) - len(scan_first)
        print(f"\n  Row growth between first and last snapshot: {growth:+d}")

    print("\nTime-travel demo complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
