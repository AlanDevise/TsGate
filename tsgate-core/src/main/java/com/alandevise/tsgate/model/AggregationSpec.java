package com.alandevise.tsgate.model;

/**
 * Aggregation specification for one field.
 * <p>Fluent queries collect {@code field/function/alias} here, and adapters translate the specification to backend SQL.</p>
 *
 * @param field    aggregation field name
 * @param function aggregation function
 * @param alias    aggregate result alias
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public record AggregationSpec(String field,
                              AggregationFunctionEnum function,
                              String alias) {
}
