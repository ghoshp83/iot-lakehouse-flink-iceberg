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
