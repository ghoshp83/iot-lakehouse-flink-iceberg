package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;

import java.io.Serializable;

public final class DeserializationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TelemetryProto.Telemetry telemetry;
    private final byte[] failedPayload;
    private final String error;

    private DeserializationResult(TelemetryProto.Telemetry telemetry,
                                  byte[] failedPayload, String error) {
        this.telemetry = telemetry;
        this.failedPayload = failedPayload;
        this.error = error;
    }

    public static DeserializationResult success(TelemetryProto.Telemetry t) {
        return new DeserializationResult(t, null, null);
    }

    public static DeserializationResult failure(byte[] payload, String error) {
        return new DeserializationResult(null, payload, error);
    }

    public boolean isSuccess() { return telemetry != null; }
    public TelemetryProto.Telemetry getTelemetry() { return telemetry; }
    public byte[] getFailedPayload() { return failedPayload; }
    public String getError() { return error; }
}
