package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Upsert variant: writes to an Iceberg v2 table with equality deletes on (device_id, ts).
 * Duplicate or corrected readings for the same device at the same timestamp replace
 * the prior row rather than appending a second copy.
 */
public final class KafkaToIcebergUpsertJob {

    private static final String DB = "iot";
    private static final String TABLE = "telemetry_upsert";

    private KafkaToIcebergUpsertJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String bootstrap = params.getOrDefault("kafka.bootstrap", "kafka:29092");
        String topic = params.getOrDefault("kafka.topic", "iot.telemetry");
        String groupId = params.getOrDefault("kafka.group", "flink-iot-iceberg-upsert");
        String nessieUri = params.getOrDefault("nessie.uri", "http://nessie:19120/api/v2");
        String nessieRef = params.getOrDefault("nessie.ref", "main");
        String warehouse = params.getOrDefault("warehouse", "s3://warehouse/");
        String s3Endpoint = params.getOrDefault("s3.endpoint", "http://minio:9000");
        String s3Key = params.getOrDefault("s3.access-key", "admin");
        String s3Secret = params.getOrDefault("s3.secret-key", "admin12345");

        CatalogLoader catalogLoader = nessieCatalogLoader(
                nessieUri, nessieRef, warehouse, s3Endpoint, s3Key, s3Secret);

        ensureTable(catalogLoader);

        TableLoader tableLoader = TableLoader.fromCatalog(
                catalogLoader, TableIdentifier.of(DB, TABLE));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000L);

        KafkaSource<DeserializationResult> source = KafkaSource
                .<DeserializationResult>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DlqProtobufDeserializer())
                .setProperty("isolation.level", "read_committed")
                .build();

        WatermarkStrategy<DeserializationResult> watermarks = WatermarkStrategy
                .<DeserializationResult>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((r, recordTs) ->
                        r.isSuccess() ? r.getTelemetry().getTs() : recordTs);

        OutputTag<String> dlqTag = new OutputTag<>("dlq",
                TypeInformation.of(String.class));

        SingleOutputStreamOperator<RowData> rows = env
                .fromSource(source, watermarks, "kafka-iot-telemetry")
                .process(new ProcessFunction<DeserializationResult, RowData>() {
                    @Override
                    public void processElement(DeserializationResult value,
                            Context ctx, Collector<RowData> out) {
                        if (!value.isSuccess()) {
                            ctx.output(dlqTag, DlqEnvelope.forParseFailure(
                                    value.getError(), value.getFailedPayload()));
                            return;
                        }
                        String reason = TelemetryValidator.validate(value.getTelemetry());
                        if (reason != null) {
                            ctx.output(dlqTag, DlqEnvelope.forValidationFailure(
                                    reason, value.getTelemetry().getDeviceId()));
                        } else {
                            out.collect(toRowData(value.getTelemetry()));
                        }
                    }
                });

        FlinkSink.forRowData(rows)
                .tableLoader(tableLoader)
                .equalityFieldColumns(java.util.List.of("device_id", "ts"))
                .upsert(true)
                .append();

        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("iot.telemetry.dlq")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        rows.getSideOutput(dlqTag).sinkTo(dlqSink);

        env.execute("iot-lakehouse-kafka-to-iceberg-upsert");
    }

    private static Schema icebergSchema() {
        return new Schema(
                Types.NestedField.required(1, "device_id", Types.StringType.get()),
                Types.NestedField.required(2, "site_id", Types.StringType.get()),
                Types.NestedField.required(3, "ts", Types.TimestampType.withZone()),
                Types.NestedField.required(4, "temperature_c", Types.DoubleType.get()),
                Types.NestedField.required(5, "humidity_pct", Types.DoubleType.get()),
                Types.NestedField.required(6, "pressure_hpa", Types.DoubleType.get()),
                Types.NestedField.required(7, "vibration_g", Types.DoubleType.get()),
                Types.NestedField.optional(8, "firmware_version", Types.StringType.get()));
    }

    private static RowData toRowData(TelemetryProto.Telemetry t) {
        GenericRowData row = new GenericRowData(8);
        row.setField(0, StringData.fromString(t.getDeviceId()));
        row.setField(1, StringData.fromString(t.getSiteId()));
        row.setField(2, TimestampData.fromInstant(Instant.ofEpochMilli(t.getTs())));
        row.setField(3, t.getTemperatureC());
        row.setField(4, t.getHumidityPct());
        row.setField(5, t.getPressureHpa());
        row.setField(6, t.getVibrationG());
        String fw = t.getFirmwareVersion();
        row.setField(7, fw.isEmpty() ? null : StringData.fromString(fw));
        return row;
    }

    private static CatalogLoader nessieCatalogLoader(
            String uri, String ref, String warehouse, String s3Endpoint, String s3Key, String s3Secret) {
        Map<String, String> props = new HashMap<>();
        props.put("catalog-impl", "org.apache.iceberg.nessie.NessieCatalog");
        props.put("uri", uri);
        props.put("ref", ref);
        props.put("warehouse", warehouse);
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", s3Endpoint);
        props.put("s3.access-key-id", s3Key);
        props.put("s3.secret-access-key", s3Secret);
        props.put("s3.path-style-access", "true");
        props.put("client.region", "us-east-1");
        return CatalogLoader.custom("nessie", props, new Configuration(false),
                "org.apache.iceberg.nessie.NessieCatalog");
    }

    private static void ensureTable(CatalogLoader loader) {
        Catalog catalog = loader.loadCatalog();
        Namespace ns = Namespace.of(DB);
        if (catalog instanceof SupportsNamespaces) {
            SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            if (!nsCatalog.namespaceExists(ns)) {
                nsCatalog.createNamespace(ns);
            }
        }
        TableIdentifier id = TableIdentifier.of(DB, TABLE);
        if (!catalog.tableExists(id)) {
            Schema schema = icebergSchema();
            catalog.createTable(id, schema,
                    IcebergPartitions.byEventDay(schema, "ts"),
                    Map.of("format-version", "2",
                           "write.delete.mode", "merge-on-read",
                           "write.update.mode", "merge-on-read"));
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--") && i + 1 < args.length) {
                out.put(a.substring(2), args[++i]);
            }
        }
        return out;
    }
}
