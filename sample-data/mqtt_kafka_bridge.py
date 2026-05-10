"""MQTT -> Kafka bridge for the IoT lakehouse.

Subscribes to ``sensors/+/+/telemetry`` on HiveMQ, parses the JSON payload
emitted by ``sensor_simulator.py`` and re-emits it as an Avro record to a
single Kafka topic via the Confluent Schema Registry. Keyed by ``device_id``
so per-device partitioning is preserved downstream.

This is the M2 stand-in. M3 keeps the bridge in place (HiveMQ CE has no
native Kafka connector) and adds a Flink job that consumes the Avro stream
into Iceberg.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

# Schema file is the source of truth, shared with the Flink job at
# flink-jobs/src/main/avro/telemetry.avsc — we load it here so the bridge
# always registers the same definition the Flink job expects.
DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "flink-jobs" / "src" / "main" / "avro" / "telemetry.avsc"


def _to_epoch_millis(ts: str | int | float) -> int:
    if isinstance(ts, (int, float)):
        return int(ts)
    return int(datetime.fromisoformat(str(ts)).astimezone(timezone.utc).timestamp() * 1000)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mqtt-host", default="localhost")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--mqtt-topic", default="sensors/+/+/telemetry")
    p.add_argument("--kafka-bootstrap", default="localhost:9092")
    p.add_argument("--kafka-topic", default="iot.telemetry")
    p.add_argument("--schema-registry-url", default="http://localhost:8081")
    p.add_argument("--schema-path", type=Path, default=DEFAULT_SCHEMA_PATH)
    args = p.parse_args()

    schema_str = args.schema_path.read_text(encoding="utf-8")
    sr_client = SchemaRegistryClient({"url": args.schema_registry_url})
    avro_serializer = AvroSerializer(sr_client, schema_str)
    key_serializer = StringSerializer("utf_8")

    producer = Producer({
        "bootstrap.servers": args.kafka_bootstrap,
        "linger.ms": 20,
        "acks": "1",
    })

    forwarded = {"n": 0, "errors": 0}
    t_start = time.time()
    value_ctx = SerializationContext(args.kafka_topic, MessageField.VALUE)
    key_ctx = SerializationContext(args.kafka_topic, MessageField.KEY)

    def on_message(_client, _userdata, msg):
        try:
            payload = json.loads(msg.payload)
            record = {
                "device_id": payload["device_id"],
                "site_id": payload["site_id"],
                "ts": _to_epoch_millis(payload["ts"]),
                "temperature_c": float(payload["temperature_c"]),
                "humidity_pct": float(payload["humidity_pct"]),
                "pressure_hpa": float(payload["pressure_hpa"]),
                "vibration_g": float(payload["vibration_g"]),
            }
            producer.produce(
                topic=args.kafka_topic,
                key=key_serializer(record["device_id"], key_ctx),
                value=avro_serializer(record, value_ctx),
            )
            producer.poll(0)
            forwarded["n"] += 1
            if forwarded["n"] % 200 == 0:
                elapsed = time.time() - t_start
                print(f"[bridge] forwarded {forwarded['n']} ({forwarded['n'] / elapsed:.1f}/s)", flush=True)
        except Exception as e:  # noqa: BLE001 — log and skip on per-message failure
            forwarded["errors"] += 1
            if forwarded["errors"] <= 5:
                print(f"[bridge] ERROR on msg: {e}", file=sys.stderr, flush=True)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-kafka-bridge")
    client.on_message = on_message
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=30)
    client.subscribe(args.mqtt_topic, qos=0)
    print(
        f"[bridge] mqtt://{args.mqtt_host}:{args.mqtt_port}/{args.mqtt_topic} "
        f"-> kafka://{args.kafka_bootstrap}/{args.kafka_topic} "
        f"(SR={args.schema_registry_url})",
        flush=True,
    )

    stop = False
    def _shutdown(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    client.loop_start()
    while not stop:
        time.sleep(0.5)
    client.loop_stop()
    client.disconnect()
    producer.flush(timeout=5)
    print(f"[bridge] stopped after {forwarded['n']} msgs ({forwarded['errors']} errors)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
