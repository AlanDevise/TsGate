package com.alandevise.tsdb.model;

/**
 * Common aggregation functions.
 * <p>Used by {@code aggregate} in annotated POJO fluent queries. Adapters translate these functions into
 * SQL aggregate expressions for the IoTDB table model or InfluxDB 3 Core.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public enum AggregationFunctionEnum {
    /**
     * Count.
     */
    COUNT,
    /**
     * Sum.
     */
    SUM,
    /**
     * Arithmetic mean.
     */
    AVG,
    /**
     * Minimum value.
     */
    MIN,
    /**
     * Maximum value.
     */
    MAX,
    /**
     * First value.
     */
    FIRST,
    /**
     * Last value.
     */
    LAST
}
