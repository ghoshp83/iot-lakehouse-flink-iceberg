package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.avro.Telemetry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * M3 half-pipeline job: Kafka (Avro via Schema Registry) -> print sink.
 * <p>
 * Lets us prove the Avro deserialization path against the running stack
 * before adding the Iceberg sink. The next M3 slice swaps the print sink
 * for an Iceberg FlinkSink writing Parquet to MinIO via the Nessie catalog.
 */
public final class KafkaToPrintJob {

    private KafkaToPrintJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String bootstrap = params.getOrDefault("kafka.bootstrap", "kafka:29092");
        String topic = params.getOrDefault("kafka.topic", "iot.telemetry");
        String groupId = params.getOrDefault("kafka.group", "flink-iot-print");
        String schemaRegistryUrl = params.getOrDefault("schema.registry.url", "http://schema-registry:8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Telemetry> source = KafkaSource.<Telemetry>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(Telemetry.class, schemaRegistryUrl))
                .build();

        DataStream<Telemetry> stream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-iot-telemetry");

        stream.map(t -> String.format(
                        "device=%s site=%s ts=%s temp=%.2f hum=%.2f pres=%.2f vib=%.4f",
                        t.getDeviceId(), t.getSiteId(), t.getTs(),
                        t.getTemperatureC(), t.getHumidityPct(), t.getPressureHpa(), t.getVibrationG()))
                .print();

        env.execute("iot-lakehouse-m3-kafka-to-print");
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--") && i + 1 < args.length) {
                out.put(a.substring(2), args[++i]);
            }
        }
        return out;
    }
}
