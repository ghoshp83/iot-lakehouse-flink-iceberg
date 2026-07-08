package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;

/**
 * Physical-plausibility gate for telemetry readings.
 *
 * <p>The Protobuf deserializer only catches messages that fail to <em>parse</em>.
 * A reading can decode perfectly and still be nonsense — a stuck sensor
 * reporting 9000&deg;C, a NaN from a division-by-zero on the device, humidity of
 * 250%. Left alone these poison every downstream aggregate. This gate rejects
 * out-of-range readings to the same dead-letter path as parse failures, keeping
 * the Iceberg table trustworthy.
 *
 * <p>Bounds are deliberately wide: they exist to catch broken sensors, not to
 * second-guess the physics of an unusual-but-real reading.
 */
public final class TelemetryValidator {

    static final double TEMP_MIN_C = -60.0;
    static final double TEMP_MAX_C = 150.0;
    static final double HUMIDITY_MIN_PCT = 0.0;
    static final double HUMIDITY_MAX_PCT = 100.0;
    static final double PRESSURE_MIN_HPA = 800.0;
    static final double PRESSURE_MAX_HPA = 1100.0;
    static final double VIBRATION_MIN_G = 0.0;
    static final double VIBRATION_MAX_G = 50.0;

    private TelemetryValidator() {}

    /**
     * @return {@code null} if the reading is physically plausible, otherwise a
     *     human-readable reason it was rejected (suitable for a DLQ record).
     */
    public static String validate(TelemetryProto.Telemetry t) {
        if (t.getDeviceId().isEmpty()) {
            return "empty device_id";
        }
        if (t.getTs() <= 0) {
            return "non-positive ts: " + t.getTs();
        }
        String reason;
        if ((reason = checkRange("temperature_c", t.getTemperatureC(), TEMP_MIN_C, TEMP_MAX_C)) != null) {
            return reason;
        }
        if ((reason = checkRange("humidity_pct", t.getHumidityPct(), HUMIDITY_MIN_PCT, HUMIDITY_MAX_PCT)) != null) {
            return reason;
        }
        if ((reason = checkRange("pressure_hpa", t.getPressureHpa(), PRESSURE_MIN_HPA, PRESSURE_MAX_HPA)) != null) {
            return reason;
        }
        if ((reason = checkRange("vibration_g", t.getVibrationG(), VIBRATION_MIN_G, VIBRATION_MAX_G)) != null) {
            return reason;
        }
        return null;
    }

    private static String checkRange(String field, double value, double min, double max) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return field + " not finite: " + value;
        }
        if (value < min || value > max) {
            return field + " out of range [" + min + ", " + max + "]: " + value;
        }
        return null;
    }
}
