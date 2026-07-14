package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.proto.TelemetryProto;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
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

public final class WindowedAggregationJob {

    private static final String DB = "iot";
    private static final String TABLE = "telemetry_1m_agg";

    private WindowedAggregationJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String bootstrap = params.getOrDefault("kafka.bootstrap", "kafka:29092");
        String topic = params.getOrDefault("kafka.topic", "iot.telemetry");
        String groupId = params.getOrDefault("kafka.group", "flink-iot-windowed-agg");
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

        // Route through the same gate as the append/upsert jobs: previously this
        // job aggregated readings that the other jobs reject (a 9000 degree stuck
        // sensor still polluted every per-minute average) and dropped parse
        // failures without a trace.
        SingleOutputStreamOperator<TelemetryProto.Telemetry> validTelemetry = env
                .fromSource(source, watermarks, "kafka-iot-telemetry")
                .process(new TelemetryRouter());

        SingleOutputStreamOperator<RowData> aggregated = validTelemetry
                .keyBy(TelemetryProto.Telemetry::getDeviceId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new TelemetryAggregator(), new AggToRowData());

        FlinkSink.forRowData(aggregated)
                .tableLoader(tableLoader)
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
        validTelemetry.getSideOutput(TelemetryRouter.DLQ_TAG).sinkTo(dlqSink);

        env.execute("iot-lakehouse-windowed-aggregation-1m");
    }

    private static Schema icebergSchema() {
        return new Schema(
                Types.NestedField.required(1, "device_id", Types.StringType.get()),
                Types.NestedField.required(2, "window_start", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "window_end", Types.TimestampType.withZone()),
                Types.NestedField.required(4, "reading_count", Types.LongType.get()),
                Types.NestedField.required(5, "avg_temperature_c", Types.DoubleType.get()),
                Types.NestedField.required(6, "min_temperature_c", Types.DoubleType.get()),
                Types.NestedField.required(7, "max_temperature_c", Types.DoubleType.get()),
                Types.NestedField.required(8, "avg_humidity_pct", Types.DoubleType.get()),
                Types.NestedField.required(9, "avg_pressure_hpa", Types.DoubleType.get()));
    }

    static class Accumulator {
        String deviceId;
        long count;
        double sumTemp, minTemp = Double.MAX_VALUE, maxTemp = -Double.MAX_VALUE;
        double sumHumidity, sumPressure;
    }

    private static class TelemetryAggregator
            implements AggregateFunction<TelemetryProto.Telemetry, Accumulator, Accumulator> {

        @Override
        public Accumulator createAccumulator() { return new Accumulator(); }

        @Override
        public Accumulator add(TelemetryProto.Telemetry v, Accumulator a) {
            a.deviceId = v.getDeviceId();
            a.count++;
            a.sumTemp += v.getTemperatureC();
            a.minTemp = Math.min(a.minTemp, v.getTemperatureC());
            a.maxTemp = Math.max(a.maxTemp, v.getTemperatureC());
            a.sumHumidity += v.getHumidityPct();
            a.sumPressure += v.getPressureHpa();
            return a;
        }

        @Override
        public Accumulator getResult(Accumulator a) { return a; }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            a.count += b.count;
            a.sumTemp += b.sumTemp;
            a.minTemp = Math.min(a.minTemp, b.minTemp);
            a.maxTemp = Math.max(a.maxTemp, b.maxTemp);
            a.sumHumidity += b.sumHumidity;
            a.sumPressure += b.sumPressure;
            return a;
        }
    }

    private static class AggToRowData
            extends ProcessWindowFunction<Accumulator, RowData, String, TimeWindow> {

        @Override
        public void process(String key, Context ctx,
                Iterable<Accumulator> elements, Collector<RowData> out) {
            Accumulator a = elements.iterator().next();
            GenericRowData row = new GenericRowData(9);
            row.setField(0, StringData.fromString(a.deviceId));
            row.setField(1, TimestampData.fromInstant(
                    Instant.ofEpochMilli(ctx.window().getStart())));
            row.setField(2, TimestampData.fromInstant(
                    Instant.ofEpochMilli(ctx.window().getEnd())));
            row.setField(3, a.count);
            row.setField(4, a.sumTemp / a.count);
            row.setField(5, a.minTemp);
            row.setField(6, a.maxTemp);
            row.setField(7, a.sumHumidity / a.count);
            row.setField(8, a.sumPressure / a.count);
            out.collect(row);
        }
    }

    private static CatalogLoader nessieCatalogLoader(
            String uri, String ref, String warehouse, String s3Endpoint,
            String s3Key, String s3Secret) {
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
            catalog.createTable(id, schema, IcebergPartitions.byEventDay(schema, "window_start"));
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
