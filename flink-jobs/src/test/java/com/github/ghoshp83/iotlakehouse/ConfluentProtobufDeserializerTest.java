package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class ConfluentProtobufDeserializerTest {

    private final ConfluentProtobufDeserializer deser = new ConfluentProtobufDeserializer();

    @Test
    void deserializesConfluentWireFormat() throws Exception {
        TelemetryProto.Telemetry original = TelemetryProto.Telemetry.newBuilder()
                .setDeviceId("sensor-1")
                .setSiteId("site-a")
                .setTs(1_700_000_000_000L)
                .setTemperatureC(22.5)
                .setHumidityPct(55.0)
                .setPressureHpa(1013.25)
                .setVibrationG(0.1)
                .setFirmwareVersion("1.0.0")
                .build();

        byte[] wireBytes = wrapConfluentFormat(original.toByteArray(), 1);

        TelemetryProto.Telemetry result = deser.deserialize(wireBytes);

        assertEquals("sensor-1", result.getDeviceId());
        assertEquals("site-a", result.getSiteId());
        assertEquals(1_700_000_000_000L, result.getTs());
        assertEquals(22.5, result.getTemperatureC(), 0.001);
        assertEquals(55.0, result.getHumidityPct(), 0.001);
        assertEquals(1013.25, result.getPressureHpa(), 0.001);
        assertEquals(0.1, result.getVibrationG(), 0.001);
        assertEquals("1.0.0", result.getFirmwareVersion());
    }

    @Test
    void handlesMessageWithoutFirmwareVersion() throws Exception {
        TelemetryProto.Telemetry original = TelemetryProto.Telemetry.newBuilder()
                .setDeviceId("sensor-2")
                .setSiteId("site-b")
                .setTs(1_700_000_001_000L)
                .setTemperatureC(18.0)
                .setHumidityPct(60.0)
                .setPressureHpa(1012.0)
                .setVibrationG(0.05)
                .build();

        byte[] wireBytes = wrapConfluentFormat(original.toByteArray(), 42);

        TelemetryProto.Telemetry result = deser.deserialize(wireBytes);

        assertEquals("sensor-2", result.getDeviceId());
        assertEquals("", result.getFirmwareVersion());
    }

    @Test
    void returnsNullForTooShortPayload() throws Exception {
        assertNull(deser.deserialize(null));
        assertNull(deser.deserialize(new byte[5]));
    }

    @Test
    void dlqDeserializerCatchesErrors() {
        DlqProtobufDeserializer dlq = new DlqProtobufDeserializer();

        byte[] garbage = {0x00, 0x00, 0x00, 0x00, 0x01, 0x00, (byte) 0xFF, (byte) 0xFF};
        DeserializationResult result = dlq.deserialize(garbage);

        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertNotNull(result.getFailedPayload());
    }

    @Test
    void dlqDeserializerSuccessPath() throws Exception {
        DlqProtobufDeserializer dlq = new DlqProtobufDeserializer();

        TelemetryProto.Telemetry original = TelemetryProto.Telemetry.newBuilder()
                .setDeviceId("test")
                .setSiteId("s")
                .setTs(1000L)
                .setTemperatureC(20.0)
                .setHumidityPct(50.0)
                .setPressureHpa(1013.0)
                .setVibrationG(0.0)
                .build();

        byte[] wireBytes = wrapConfluentFormat(original.toByteArray(), 1);
        DeserializationResult result = dlq.deserialize(wireBytes);

        assertTrue(result.isSuccess());
        assertEquals("test", result.getTelemetry().getDeviceId());
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
