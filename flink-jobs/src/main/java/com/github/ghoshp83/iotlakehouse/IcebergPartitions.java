package com.github.ghoshp83.iotlakehouse;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * Partition specs for the lakehouse tables.
 *
 * <p>Streaming writers commit a fresh set of data files on every Flink
 * checkpoint. Landing them in a single unpartitioned table means every
 * time-range query has to scan the whole history. Day-level partitioning on the
 * event-time column lets Iceberg prune to the days a query actually touches. The
 * transform is <em>hidden</em>: queries still filter on the raw {@code ts}
 * column, with no derived partition column to remember.
 */
public final class IcebergPartitions {

    private IcebergPartitions() {}

    /**
     * Day-level partitioning on an event-time column (e.g. {@code ts} or
     * {@code window_start}). For the upsert table the source column is also an
     * equality-delete key, so equality deletes still match the exact row.
     */
    public static PartitionSpec byEventDay(Schema schema, String eventTimeColumn) {
        return PartitionSpec.builderFor(schema).day(eventTimeColumn).build();
    }
}
