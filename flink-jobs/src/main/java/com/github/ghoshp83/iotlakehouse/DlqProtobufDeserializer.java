package com.github.ghoshp83.iotlakehouse;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public final class DlqProtobufDeserializer
        implements DeserializationSchema<DeserializationResult> {

    private static final long serialVersionUID = 1L;
    private final ConfluentProtobufDeserializer inner = new ConfluentProtobufDeserializer();

    @Override
    public DeserializationResult deserialize(byte[] message) {
        try {
            var telemetry = inner.deserialize(message);
            if (telemetry == null) {
                return DeserializationResult.failure(message, "null payload or too short");
            }
            return DeserializationResult.success(telemetry);
        } catch (Exception e) {
            return DeserializationResult.failure(message, e.getMessage());
        }
    }

    @Override
    public boolean isEndOfStream(DeserializationResult next) {
        return false;
    }

    @Override
    public TypeInformation<DeserializationResult> getProducedType() {
        return TypeInformation.of(DeserializationResult.class);
    }
}
