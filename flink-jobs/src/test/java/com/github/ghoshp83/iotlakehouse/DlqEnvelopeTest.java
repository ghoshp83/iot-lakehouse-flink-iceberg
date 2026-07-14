package com.github.ghoshp83.iotlakehouse;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DlqEnvelopeTest {

    @Test
    void parseFailureCarriesStageAndPayloadLength() {
        String json = DlqEnvelope.forParseFailure("too short", new byte[]{1, 2, 3});
        assertTrue(json.contains("\"stage\":\"deserialize\""), json);
        assertTrue(json.contains("\"reason\":\"too short\""), json);
        assertTrue(json.contains("\"payload_len\":3"), json);
    }

    @Test
    void nullPayloadReportsZeroLength() {
        String json = DlqEnvelope.forParseFailure("null payload", null);
        assertTrue(json.contains("\"payload_len\":0"), json);
    }

    @Test
    void validationFailureCarriesStageAndDevice() {
        String json = DlqEnvelope.forValidationFailure("temperature_c out of range", "sensor-7");
        assertTrue(json.contains("\"stage\":\"validate\""), json);
        assertTrue(json.contains("\"device_id\":\"sensor-7\""), json);
        assertTrue(json.contains("\"reason\":\"temperature_c out of range\""), json);
    }

    @Test
    void lateEventCarriesStageDeviceAndEventTime() {
        // The event timestamp is what an operator needs to decide whether a late
        // reading is worth replaying through the upsert corrections path.
        String json = DlqEnvelope.forLateEvent("sensor-3", 1_700_000_000_000L);
        assertTrue(json.contains("\"stage\":\"late\""), json);
        assertTrue(json.contains("\"device_id\":\"sensor-3\""), json);
        assertTrue(json.contains("\"event_ts\":1700000000000"), json);
    }

    @Test
    void escapesQuotesAndControlCharsSoTheRecordStaysValidJson() {
        // A raw exception message can contain quotes and newlines; unescaped they
        // would break the JSON record and make the DLQ unparseable.
        String json = DlqEnvelope.forValidationFailure("bad \"value\"\nline2", "d1");
        assertTrue(json.contains("\\\"value\\\""), json);
        assertTrue(json.contains("\\n"), json);
        assertFalse(json.contains("value\"\nline2"), json);
        // Exactly the structural quotes remain: an even number of unescaped quotes.
        assertTrue(json.startsWith("{") && json.endsWith("}"), json);
    }
}
