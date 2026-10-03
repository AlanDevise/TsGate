package com.alandevise.tsgate.metadata;

import java.util.List;

/**
 * Cached metadata for an annotated POJO class.
 * <p>Stores the measurement and time, tag, and field columns for reuse by write conversion and result mapping.</p>
 *
 * @param entityType   business entity class
 *                     <p>
 *                     for example {@code AccruePoint.class}
 * @param measurement  logical table name
 * @param timeColumn   time column metadata
 * @param tagColumns   tag column metadata
 * @param fieldColumns field column metadata
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public record TSDBEntityMetadata(Class<?> entityType,
                                 String measurement,
                                 TSDBColumnMetadata timeColumn,
                                 List<TSDBColumnMetadata> tagColumns,
                                 List<TSDBColumnMetadata> fieldColumns) {

    /**
     * Creates an entity metadata snapshot.
     *
     * @param entityType   business entity class, for example {@code AccruePoint.class}
     * @param measurement  logical table name, for example {@code "ACCRUE"}
     * @param timeColumn   time column metadata, for example {@code TSDBColumnMetadata} named {@code "time"}
     * @param tagColumns   tag column metadata, for example {@code "device_code"} and {@code "point_type"}
     * @param fieldColumns field column metadata, for example {@code "value"} and {@code "status"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TSDBEntityMetadata(Class<?> entityType,
                              String measurement,
                              TSDBColumnMetadata timeColumn,
                              List<TSDBColumnMetadata> tagColumns,
                              List<TSDBColumnMetadata> fieldColumns) {
        this.entityType = entityType;
        this.measurement = measurement;
        this.timeColumn = timeColumn;
        this.tagColumns = List.copyOf(tagColumns);
        this.fieldColumns = List.copyOf(fieldColumns);
    }
}
