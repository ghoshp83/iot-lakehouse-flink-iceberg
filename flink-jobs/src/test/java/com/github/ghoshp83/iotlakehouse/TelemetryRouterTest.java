package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelemetryRouterTest {

    private static TelemetryProto.Telemetry.Builder valid() {
        return TelemetryProto.Telemetry.newBuilder()
                .setDeviceId("sensor-1")
                .setSiteId("site-a")
                .setTs(1_700_000_000_000L)
                .setTemperatureC(22.5)
                .setHumidityPct(55.0)
                .setPressureHpa(1013.25)
                .setVibrationG(0.1);
    }

    @Test
    void validTelemetryPassesThrough() {
        // null means "main output" — the reading reaches the Iceberg table.
        assertNull(TelemetryRouter.route(
                DeserializationResult.success(valid().build())));
    }

    @Test
    void parseFailureRoutesToDlqAsDeserializeStage() {
        String dlq = TelemetryRouter.route(
                DeserializationResult.failure(new byte[] {1, 2, 3}, "bad magic byte"));
        assertTrue(dlq.contains("\"stage\":\"deserialize\""), dlq);
        assertTrue(dlq.contains("bad magic byte"), dlq);
    }

    @Test
    void implausibleReadingRoutesToDlqAsValidateStage() {
        // A decodable-but-nonsense reading must not reach the table via ANY job;
        // the router is the shared gate that guarantees it.
        String dlq = TelemetryRouter.route(DeserializationResult.success(
                valid().setTemperatureC(9000.0).build()));
        assertTrue(dlq.contains("\"stage\":\"validate\""), dlq);
        assertTrue(dlq.contains("temperature_c"), dlq);
        assertTrue(dlq.contains("sensor-1"), dlq);
    }
}
