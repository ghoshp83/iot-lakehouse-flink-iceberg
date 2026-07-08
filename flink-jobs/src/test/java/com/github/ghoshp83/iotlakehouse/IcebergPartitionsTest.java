package com.github.ghoshp83.iotlakehouse;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IcebergPartitionsTest {

    private static final Schema TELEMETRY = new Schema(
            Types.NestedField.required(1, "device_id", Types.StringType.get()),
            Types.NestedField.required(3, "ts", Types.TimestampType.withZone()));

    @Test
    void partitionsByDayOfTheEventTimeColumn() {
        PartitionSpec spec = IcebergPartitions.byEventDay(TELEMETRY, "ts");

        // Exactly one partition field: a day transform over ts. This is what lets
        // a time-range query prune to the days it touches instead of scanning all
        // history — the whole point of partitioning a streaming table.
        assertEquals(1, spec.fields().size());
        PartitionField field = spec.fields().get(0);
        assertEquals("day", field.transform().toString());
        assertEquals(TELEMETRY.findField("ts").fieldId(), field.sourceId());
    }

    @Test
    void hiddenPartitionKeepsTheRawColumnQueryable() {
        // Hidden partitioning means the derived field is named <col>_day but the
        // schema still exposes the raw ts column — consumers filter on ts, not on
        // a partition column they must know about.
        PartitionSpec spec = IcebergPartitions.byEventDay(TELEMETRY, "ts");
        assertEquals("ts_day", spec.fields().get(0).name());
    }

    @Test
    void rejectsAnUnknownColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> IcebergPartitions.byEventDay(TELEMETRY, "does_not_exist"));
    }
}
