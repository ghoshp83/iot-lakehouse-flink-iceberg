package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelemetryValidatorTest {

    /** A plausible reading, mutated per-test to exercise one failure at a time. */
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
    void acceptsAPlausibleReading() {
        assertNull(TelemetryValidator.validate(valid().build()));
    }

    @Test
    void acceptsReadingsAtTheBounds() {
        // Boundary values are inclusive — a sensor legitimately reading exactly
        // 100% humidity must not be dropped.
        assertNull(TelemetryValidator.validate(valid()
                .setTemperatureC(TelemetryValidator.TEMP_MAX_C)
                .setHumidityPct(TelemetryValidator.HUMIDITY_MAX_PCT)
                .setPressureHpa(TelemetryValidator.PRESSURE_MIN_HPA)
                .setVibrationG(TelemetryValidator.VIBRATION_MIN_G)
                .build()));
    }

    @Test
    void rejectsAStuckHotSensor() {
        String reason = TelemetryValidator.validate(valid().setTemperatureC(9000.0).build());
        assertNotNull(reason);
        assertTrue(reason.contains("temperature_c"), reason);
    }

    @Test
    void rejectsImpossibleHumidity() {
        assertNotNull(TelemetryValidator.validate(valid().setHumidityPct(250.0).build()));
    }

    @Test
    void rejectsNaNAndInfinity() {
        // NaN passes a naive min/max check (all comparisons are false), so it must
        // be caught explicitly or it silently corrupts every average downstream.
        assertNotNull(TelemetryValidator.validate(valid().setPressureHpa(Double.NaN).build()));
        assertNotNull(TelemetryValidator.validate(valid().setVibrationG(Double.POSITIVE_INFINITY).build()));
    }

    @Test
    void rejectsEmptyDeviceIdAndNonPositiveTs() {
        assertNotNull(TelemetryValidator.validate(valid().setDeviceId("").build()));
        assertNotNull(TelemetryValidator.validate(valid().setTs(0).build()));
    }
}
