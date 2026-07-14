package com.github.ghoshp83.iotlakehouse;

/**
 * Structured dead-letter records.
 *
 * <p>The DLQ used to carry a bare error string, which is impossible to triage at
 * scale: you cannot tell a parse failure from a range rejection, cannot group by
 * failure stage, and cannot filter by device without regex-scraping free text.
 *
 * <p>Each rejected record is instead written as a compact JSON object with a
 * {@code stage} discriminator so an operator (or a Trino query over the DLQ
 * topic) can aggregate failures by cause. Values are JSON-escaped so an error
 * message containing a quote or newline cannot corrupt the record.
 */
public final class DlqEnvelope {

    private DlqEnvelope() {}

    /** A message that failed to deserialize (bad wire format, truncated payload). */
    public static String forParseFailure(String reason, byte[] failedPayload) {
        int len = failedPayload == null ? 0 : failedPayload.length;
        return "{" + str("stage", "deserialize") + ","
                + str("reason", reason) + ","
                + num("payload_len", len) + "}";
    }

    /** A message that decoded but failed the physical-plausibility gate. */
    public static String forValidationFailure(String reason, String deviceId) {
        return "{" + str("stage", "validate") + ","
                + str("device_id", deviceId) + ","
                + str("reason", reason) + "}";
    }

    /**
     * A valid reading that arrived after its event-time window had already
     * closed and been written, so including it would require rewriting an
     * emitted aggregate.
     */
    public static String forLateEvent(String deviceId, long eventTsMillis) {
        return "{" + str("stage", "late") + ","
                + str("device_id", deviceId) + ","
                + num("event_ts", eventTsMillis) + "}";
    }

    private static String str(String key, String value) {
        return quote(key) + ":" + quote(value);
    }

    private static String num(String key, long value) {
        return quote(key) + ":" + value;
    }

    private static String quote(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder b = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        return b.append('"').toString();
    }
}
