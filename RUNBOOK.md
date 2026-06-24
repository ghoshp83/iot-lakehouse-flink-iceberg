# Runbook — IoT Lakehouse

Operational playbook for the local docker-compose stack.

## Stack lifecycle

```bash
# Start (healthchecks gate ordering; topics + bucket created automatically)
cd docker && docker compose up -d

# Status (all services should show "healthy")
docker compose ps

# Stop (preserves data volumes)
docker compose down

# Full reset (destroys data)
docker compose down -v
```

## Common operations

### Check Kafka offsets
```bash
docker exec kafka kafka-get-offsets \
  --bootstrap-server localhost:9092 --topic iot.telemetry
```

### Check DLQ
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic iot.telemetry.dlq \
  --from-beginning --max-messages 10
```

### List Kafka topics
```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

### Check Schema Registry subjects
```bash
curl -s http://localhost:8081/subjects | python3 -m json.tool
```

### Browse Iceberg files on MinIO
```bash
docker run --rm --network docker_lakehouse --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 admin admin12345 >/dev/null && \
   mc ls --recursive local/warehouse/iot/"
```

### Submit Flink job (append mode)
```bash
docker cp flink-jobs/target/iot-lakehouse-flink-0.1.0-SNAPSHOT.jar \
  docker-flink-jobmanager-1:/tmp/job.jar
docker exec docker-flink-jobmanager-1 flink run -d /tmp/job.jar
```

### Submit Flink job (upsert mode)
```bash
docker cp flink-jobs/target/iot-lakehouse-flink-0.1.0-SNAPSHOT.jar \
  docker-flink-jobmanager-1:/tmp/job.jar
docker exec docker-flink-jobmanager-1 flink run -d \
  -c com.github.ghoshp83.iotlakehouse.KafkaToIcebergUpsertJob /tmp/job.jar
```

### List running Flink jobs
```bash
docker exec docker-flink-jobmanager-1 flink list
```

### Cancel a Flink job
```bash
docker exec docker-flink-jobmanager-1 flink cancel <job-id>
```

### Trigger a Flink savepoint
```bash
docker exec docker-flink-jobmanager-1 flink savepoint <job-id> \
  s3://warehouse/flink-savepoints
```

### Restore from a savepoint
```bash
docker exec docker-flink-jobmanager-1 flink run -d \
  -s s3://warehouse/flink-savepoints/<savepoint-dir> /tmp/job.jar
```

### Submit Flink windowed aggregation job
```bash
docker exec docker-flink-jobmanager-1 flink run -d \
  -c com.github.ghoshp83.iotlakehouse.WindowedAggregationJob /tmp/job.jar
```

### Query Iceberg tables via Trino
```bash
docker exec docker-trino-1 trino --catalog iceberg --schema iot \
  --execute "SELECT * FROM telemetry LIMIT 10"
```

### Run Trino demo queries
```bash
bash scripts/trino_demo.sh
```

### Full savepoint demo (create → cancel → restore)
```bash
bash scripts/savepoint_demo.sh
```

### Open monitoring dashboards
- **Grafana:** http://localhost:3000 (admin / admin, or anonymous viewer)
- **Prometheus:** http://localhost:9090
- **Flink UI:** http://localhost:8082

### Check Prometheus targets
```bash
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `kafka` stays `starting` | Port conflict on 9092 | Check `lsof -i :9092` |
| Schema Registry fails to start | Kafka not healthy yet | Wait for Kafka healthcheck; check `docker logs schema-registry` |
| Flink job `FAILED` immediately | Fat jar not on classpath | Re-run `docker cp` with the correct container name |
| Iceberg table has 0 rows | Checkpointing not enabled | Verify `env.enableCheckpointing(30_000L)` in the job |
| MinIO `NoSuchBucket` | `minio-init` didn't run | Check `docker logs docker-minio-init-1`; run `mc mb -p local/warehouse` manually |
| `ClassNotFoundException` in Flink | Dependency missing from fat jar | Check `mvn dependency:tree` and shade plugin config |
| Prometheus target DOWN for Flink | Metrics plugin not loaded | Verify `ENABLE_BUILT_IN_PLUGINS` includes `flink-metrics-prometheus-2.2.0.jar` |
| Grafana shows "No data" | No Flink job running or Prometheus not scraping | Check Prometheus targets at `:9090/targets`; ensure a Flink job is submitted |
| Savepoint fails | S3 plugin not loaded or MinIO unreachable | Verify `ENABLE_BUILT_IN_PLUGINS` includes `flink-s3-fs-hadoop-2.2.0.jar` |

## Ports

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | Bootstrap (external) |
| Schema Registry | 8081 | REST API |
| HiveMQ | 1883 | MQTT |
| HiveMQ | 8000 | MQTT over WebSocket |
| MinIO | 9000 | S3 API |
| MinIO | 9001 | Console UI |
| Nessie | 19120 | REST API |
| Flink UI | 8082 | Web dashboard (remapped from 8081) |
| Flink Prometheus | 9249 | Metrics endpoint |
| Kafka Exporter | 9308 | Kafka metrics for Prometheus |
| Prometheus | 9090 | Metrics storage + query |
| Grafana | 3000 | Dashboards (admin/admin) |
| Trino | 8083 | SQL query engine (remapped from 8080) |
