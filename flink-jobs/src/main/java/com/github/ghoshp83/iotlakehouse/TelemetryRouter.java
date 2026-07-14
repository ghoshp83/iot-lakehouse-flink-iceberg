package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Routes each consumed Kafka record to exactly one of two places: valid
 * telemetry to the main output, everything else (parse failures, readings that
 * fail the physical-plausibility gate) to the dead-letter side output as a
 * structured {@link DlqEnvelope} record.
 *
 * <p>This is the single routing point shared by all jobs, so the append,
 * upsert, and aggregation pipelines cannot drift apart in what they accept.
 *
 * <p>Every routing decision increments a Flink counter
 * ({@code records_valid}, {@code records_parse_failed},
 * {@code records_validation_failed}), so DLQ volume is visible per-job in the
 * Flink UI and the Prometheus/Grafana stack instead of only as opaque traffic
 * on the DLQ topic.
 */
public final class TelemetryRouter
        extends ProcessFunction<DeserializationResult, TelemetryProto.Telemetry> {

    private static final long serialVersionUID = 1L;

    /** Side output carrying {@link DlqEnvelope} JSON strings. */
    public static final OutputTag<String> DLQ_TAG =
            new OutputTag<>("dlq", TypeInformation.of(String.class));

    private transient Counter recordsValid;
    private transient Counter recordsParseFailed;
    private transient Counter recordsValidationFailed;

    @Override
    public void open(OpenContext openContext) {
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        recordsValid = metrics.counter("records_valid");
        recordsParseFailed = metrics.counter("records_parse_failed");
        recordsValidationFailed = metrics.counter("records_validation_failed");
    }

    @Override
    public void processElement(DeserializationResult value, Context ctx,
            Collector<TelemetryProto.Telemetry> out) {
        String dlqRecord = route(value);
        if (dlqRecord == null) {
            recordsValid.inc();
            out.collect(value.getTelemetry());
        } else {
            (value.isSuccess() ? recordsValidationFailed : recordsParseFailed).inc();
            ctx.output(DLQ_TAG, dlqRecord);
        }
    }

    /**
     * The pure routing decision, kept free of Flink runtime types so it can be
     * unit-tested directly.
     *
     * @return {@code null} if the record is valid telemetry, otherwise the
     *     structured DLQ record to emit.
     */
    static String route(DeserializationResult value) {
        if (!value.isSuccess()) {
            return DlqEnvelope.forParseFailure(value.getError(), value.getFailedPayload());
        }
        String reason = TelemetryValidator.validate(value.getTelemetry());
        if (reason != null) {
            return DlqEnvelope.forValidationFailure(reason, value.getTelemetry().getDeviceId());
        }
        return null;
    }
}
