package com.alandevise.tsgate.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Common write model converted from an annotated POJO.
 * <p>Consumed directly by table-model adapters; this version does not expose an IoTDB tree-model conversion API.</p>
 *
 * @param measurement -- GETTER --
 *                    Logical table name.
 * @param timestamp   -- GETTER --
 *                    Epoch millisecond timestamp.
 * @param tags        -- GETTER --
 *                    Tag column values.
 * @param fields      -- GETTER --
 *                    Field column values.
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public record TSDBRecord(String measurement,
                         Long timestamp,
                         Map<String, String> tags,
                         Map<String, Object> fields) {

    /**
     * Creates an immutable time-series database record.
     *
     * <p>Defensively copies the supplied {@code tags} and {@code fields}
     * and wraps them in unmodifiable views. Consequently:
     * <ul>
     *   <li>Later changes to the original maps do not affect this record.</li>
     *   <li>The exposed maps cannot be modified; attempts throw {@link UnsupportedOperationException}.</li>
     *   <li>Tag and field iteration preserves the input map order through {@link LinkedHashMap}.</li>
     * </ul>
     *
     * @param measurement logical table name, for example {@code "ACCRUE"}
     * @param timestamp   epoch millisecond timestamp, for example {@code 1783000000000L}
     * @param tags        tag column values, for example {@code Map.of("device_code", "device001")}
     * @param fields      field column values, for example {@code Map.of("value", 12.34D, "status", 1)}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     * @see Collections#unmodifiableMap(Map)
     */
    public TSDBRecord(String measurement,
                      Long timestamp,
                      Map<String, String> tags,
                      Map<String, Object> fields) {
        this.measurement = measurement;
        this.timestamp = timestamp;
        this.tags = tags == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(tags));
        this.fields = fields == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

}
