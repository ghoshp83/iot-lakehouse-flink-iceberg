"""MQTT → Kafka bridge for the IoT lakehouse.

Subscribes to `sensors/+/+/telemetry` on HiveMQ and forwards each message
verbatim to a single Kafka topic, keyed by device_id so per-device
partitioning is preserved downstream.

This is the M2 stand-in. M3 replaces it with a Flink job that does the
same hop plus parsing into Iceberg-ready rows.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mqtt-host", default="localhost")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--mqtt-topic", default="sensors/+/+/telemetry")
    p.add_argument("--kafka-bootstrap", default="localhost:9092")
    p.add_argument("--kafka-topic", default="iot.telemetry")
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap,
        key_serializer=lambda k: k.encode() if k else None,
        value_serializer=lambda v: v if isinstance(v, bytes) else v.encode(),
        linger_ms=20,
        acks=1,
    )

    forwarded = {"n": 0}
    t_start = time.time()

    def on_message(_client, _userdata, msg):
        try:
            payload = json.loads(msg.payload)
            key = payload.get("device_id")
        except (json.JSONDecodeError, AttributeError):
            key = None
        producer.send(args.kafka_topic, key=key, value=msg.payload)
        forwarded["n"] += 1
        if forwarded["n"] % 200 == 0:
            elapsed = time.time() - t_start
            print(f"[bridge] forwarded {forwarded['n']} ({forwarded['n'] / elapsed:.1f}/s)", flush=True)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-kafka-bridge")
    client.on_message = on_message
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=30)
    client.subscribe(args.mqtt_topic, qos=0)
    print(f"[bridge] mqtt://{args.mqtt_host}:{args.mqtt_port}/{args.mqtt_topic} → kafka://{args.kafka_bootstrap}/{args.kafka_topic}", flush=True)

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
    producer.close()
    print(f"[bridge] stopped after {forwarded['n']} msgs", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
