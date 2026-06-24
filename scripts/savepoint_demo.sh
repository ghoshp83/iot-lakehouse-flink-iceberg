#!/usr/bin/env bash
# Demonstrates Flink savepoint: create, cancel, restore.
# Requires a running job in the docker-compose stack.
set -euo pipefail

JM="docker-flink-jobmanager-1"
JAR="/tmp/job.jar"
SP_DIR="s3://warehouse/flink-savepoints"

echo "=== Flink Savepoint Demo ==="

echo ""
echo "Step 1: Finding running Flink job..."
JOB_ID=$(docker exec "$JM" flink list -r 2>/dev/null \
  | grep -oP '[0-9a-f]{32}' | head -1 || true)

if [ -z "$JOB_ID" ]; then
    echo "No running job found. Submit one first:"
    echo "  docker exec $JM flink run -d $JAR"
    exit 1
fi
echo "Running job: $JOB_ID"

echo ""
echo "Step 2: Triggering savepoint..."
SP_PATH=$(docker exec "$JM" flink savepoint "$JOB_ID" "$SP_DIR" 2>&1 \
  | grep -oP 's3://\S+' || true)

if [ -z "$SP_PATH" ]; then
    echo "Savepoint trigger failed. Check Flink logs."
    exit 1
fi
echo "Savepoint: $SP_PATH"

echo ""
echo "Step 3: Cancelling job..."
docker exec "$JM" flink cancel "$JOB_ID"
sleep 3

echo ""
echo "Step 4: Restoring from savepoint..."
docker exec "$JM" flink run -d -s "$SP_PATH" "$JAR"

echo ""
echo "=== Done ==="
echo "Kafka consumption resumes from the exact offset captured in the savepoint."
echo "Verify: docker exec $JM flink list -r"
