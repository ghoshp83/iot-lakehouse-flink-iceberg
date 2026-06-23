package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Deserializes Confluent Schema Registry wire-format Protobuf messages.
 * Format: magic(1) + schemaId(4) + msgIndexCount(varint) + [indices...] + protobuf payload.
 */
public final class ConfluentProtobufDeserializer
        implements DeserializationSchema<TelemetryProto.Telemetry> {

    private static final long serialVersionUID = 1L;

    @Override
    public TelemetryProto.Telemetry deserialize(byte[] message) throws IOException {
        if (message == null || message.length < 6) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(message);
        buf.get();      // magic byte 0x00
        buf.getInt();   // schema ID (not used — we parse with the compiled class)

        int indexCount = readVarint(buf);
        for (int i = 0; i < indexCount; i++) {
            readVarint(buf);
        }

        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);
        return TelemetryProto.Telemetry.parseFrom(payload);
    }

    @Override
    public boolean isEndOfStream(TelemetryProto.Telemetry nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TelemetryProto.Telemetry> getProducedType() {
        return TypeInformation.of(TelemetryProto.Telemetry.class);
    }

    private static int readVarint(ByteBuffer buf) {
        int result = 0;
        int shift = 0;
        byte b;
        do {
            b = buf.get();
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }
}
