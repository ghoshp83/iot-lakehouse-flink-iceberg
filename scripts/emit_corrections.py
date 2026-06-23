"""Emit 'correction' records to Kafka to exercise Iceberg upsert.

Reads N recent records from Kafka, tweaks the sensor values (simulating
a recalibration correction), and re-publishes them with the same
(device_id, ts) key pair. When the upsert job is running, the Iceberg
table should reflect the corrected values, not duplicates.
"""

from __future__ import annotations

import argparse
import sys
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent / "sample-data"))
import telemetry_pb2


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-bootstrap", default="localhost:9092")
    p.add_argument("--kafka-topic", default="iot.telemetry")
    p.add_argument("--schema-registry-url", default="http://localhost:8081")
    p.add_argument("--num-corrections", type=int, default=10)
    p.add_argument("--temp-offset", type=float, default=2.0,
                   help="Add this offset to temperature_c to simulate recalibration")
    args = p.parse_args()

    sr_client = SchemaRegistryClient({"url": args.schema_registry_url})
    proto_deserializer = ProtobufDeserializer(
        telemetry_pb2.Telemetry, {"use.deprecated.format": False}
    )
    proto_serializer = ProtobufSerializer(
        telemetry_pb2.Telemetry, sr_client, {"use.deprecated.format": False}
    )
    key_serializer = StringSerializer("utf_8")

    consumer = Consumer({
        "bootstrap.servers": args.kafka_bootstrap,
        "group.id": "correction-emitter-temp",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.kafka_topic])

    producer = Producer({
        "bootstrap.servers": args.kafka_bootstrap,
        "acks": "all",
        "enable.idempotence": True,
    })

    value_ctx = SerializationContext(args.kafka_topic, MessageField.VALUE)
    key_ctx = SerializationContext(args.kafka_topic, MessageField.KEY)

    collected = []
    deadline = time.time() + 10
    while len(collected) < args.num_corrections and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        record = proto_deserializer(msg.value(), None)
        if record is not None:
            collected.append(record)

    consumer.close()

    if not collected:
        print("[corrections] No records found to correct", file=sys.stderr)
        return 1

    print(f"[corrections] Collected {len(collected)} records, applying +{args.temp_offset}C offset")
    for original in collected:
        corrected = telemetry_pb2.Telemetry(
            device_id=original.device_id,
            site_id=original.site_id,
            ts=original.ts,
            temperature_c=original.temperature_c + args.temp_offset,
            humidity_pct=original.humidity_pct,
            pressure_hpa=original.pressure_hpa,
            vibration_g=original.vibration_g,
        )
        producer.produce(
            topic=args.kafka_topic,
            key=key_serializer(corrected.device_id, key_ctx),
            value=proto_serializer(corrected, value_ctx),
        )

    producer.flush(timeout=5)
    print(f"[corrections] Published {len(collected)} corrected records "
          f"(same device_id+ts, temperature_c += {args.temp_offset})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
