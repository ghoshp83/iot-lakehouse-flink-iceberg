package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class KafkaRoundTripIT {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:8.2.0"));

    private static final String TOPIC = "test.telemetry";

    @Test
    void produceAndDeserializeProtobufViaKafka() throws Exception {
        TelemetryProto.Telemetry original = TelemetryProto.Telemetry.newBuilder()
                .setDeviceId("it-sensor-1")
                .setSiteId("it-site")
                .setTs(System.currentTimeMillis())
                .setTemperatureC(21.5)
                .setHumidityPct(48.0)
                .setPressureHpa(1015.0)
                .setVibrationG(0.02)
                .setFirmwareVersion("2.0.0")
                .build();

        byte[] wireBytes = wrapConfluentFormat(original.toByteArray(), 99);

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(prodProps)) {
            producer.send(new ProducerRecord<>(TOPIC, "it-sensor-1", wireBytes)).get();
        }

        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        ConfluentProtobufDeserializer deser = new ConfluentProtobufDeserializer();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consProps)) {
            consumer.subscribe(List.of(TOPIC));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(30));

            assertEquals(1, records.count());
            ConsumerRecord<String, byte[]> record = records.iterator().next();

            TelemetryProto.Telemetry parsed = deser.deserialize(record.value());

            assertEquals("it-sensor-1", parsed.getDeviceId());
            assertEquals("it-site", parsed.getSiteId());
            assertEquals(21.5, parsed.getTemperatureC(), 0.001);
            assertEquals("2.0.0", parsed.getFirmwareVersion());
        }
    }

    @Test
    void dlqDeserializerHandlesGarbageFromKafka() throws Exception {
        byte[] garbage = {0x00, 0x00, 0x00, 0x00, 0x01, 0x00, (byte) 0xDE, (byte) 0xAD};

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        String dlqTopic = "test.dlq";
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(prodProps)) {
            producer.send(new ProducerRecord<>(dlqTopic, "bad-key", garbage)).get();
        }

        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-group");
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        DlqProtobufDeserializer dlq = new DlqProtobufDeserializer();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consProps)) {
            consumer.subscribe(List.of(dlqTopic));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(30));

            assertEquals(1, records.count());
            byte[] value = records.iterator().next().value();

            DeserializationResult result = dlq.deserialize(value);
            assertFalse(result.isSuccess());
            assertNotNull(result.getError());
        }
    }

    private static byte[] wrapConfluentFormat(byte[] protoBytes, int schemaId) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + protoBytes.length);
        buf.put((byte) 0x00);
        buf.putInt(schemaId);
        buf.put((byte) 0x00);
        buf.put(protoBytes);
        return buf.array();
    }
}
