package com.github.ghoshp83.iotlakehouse;

import com.github.ghoshp83.iotlakehouse.avro.Telemetry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * M3 final job: Kafka (Avro via Schema Registry) -> Iceberg table on MinIO via Nessie catalog.
 * <p>
 * Replaces the M3a print sink. Creates the table on first run, then streams Avro records
 * into Parquet files under s3://warehouse/iot/telemetry/.
 */
public final class KafkaToIcebergJob {

    private static final String DB = "iot";
    private static final String TABLE = "telemetry";

    private KafkaToIcebergJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String bootstrap = params.getOrDefault("kafka.bootstrap", "kafka:29092");
        String topic = params.getOrDefault("kafka.topic", "iot.telemetry");
        String groupId = params.getOrDefault("kafka.group", "flink-iot-iceberg");
        String schemaRegistryUrl = params.getOrDefault("schema.registry.url", "http://schema-registry:8081");
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
        // Checkpointing is what triggers Iceberg commits — without it, files are written
        // but never committed and the table stays empty. 30 s keeps the demo snappy.
        env.enableCheckpointing(30_000L);

        KafkaSource<Telemetry> source = KafkaSource.<Telemetry>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(Telemetry.class, schemaRegistryUrl))
                .build();

        DataStream<Telemetry> stream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-iot-telemetry");

        DataStream<RowData> rows = stream
                .map(KafkaToIcebergJob::toRowData)
                .returns(TypeInformation.of(RowData.class));

        FlinkSink.forRowData(rows)
                .tableLoader(tableLoader)
                .append();

        env.execute("iot-lakehouse-m3-kafka-to-iceberg");
    }

    /** Iceberg schema mirrors telemetry.avsc field-for-field (timestamps go to timestamptz). */
    private static Schema icebergSchema() {
        return new Schema(
                Types.NestedField.required(1, "device_id", Types.StringType.get()),
                Types.NestedField.required(2, "site_id", Types.StringType.get()),
                Types.NestedField.required(3, "ts", Types.TimestampType.withZone()),
                Types.NestedField.required(4, "temperature_c", Types.DoubleType.get()),
                Types.NestedField.required(5, "humidity_pct", Types.DoubleType.get()),
                Types.NestedField.required(6, "pressure_hpa", Types.DoubleType.get()),
                Types.NestedField.required(7, "vibration_g", Types.DoubleType.get()));
    }

    private static RowData toRowData(Telemetry t) {
        GenericRowData row = new GenericRowData(7);
        row.setField(0, StringData.fromString(t.getDeviceId().toString()));
        row.setField(1, StringData.fromString(t.getSiteId().toString()));
        row.setField(2, TimestampData.fromInstant(t.getTs()));
        row.setField(3, t.getTemperatureC());
        row.setField(4, t.getHumidityPct());
        row.setField(5, t.getPressureHpa());
        row.setField(6, t.getVibrationG());
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
        // new Configuration(false) skips loading core-default.xml/core-site.xml — those would
        // require Woodstox on the classpath, which we don't ship. We never touch HDFS anyway.
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
            catalog.createTable(id, icebergSchema(), PartitionSpec.unpartitioned());
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
