package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.TSDBQuery;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Validates shared query arguments before adapter-specific SQL translation or database I/O.
 * <p>Does not normalize the query, set defaults, or enforce backend-specific upper limits.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-10-03
 */
public final class TSDBQueryValidator {

    private TSDBQueryValidator() {
    }

    /**
     * Rejects absent queries, invalid pagination or time bounds, malformed filters, and malformed aggregations.
     * <p>Null limits and offsets, zero offsets, equal or one-sided time bounds, and positive pagination-probe
     * limits remain valid. Comparisons require one operand, BETWEEN requires exactly two, and IN requires at least one.
     * Filter operands must not contain null or non-finite Float/Double values. String literals, including the text
     * {@code "null"}, retain their literal meaning. Backend implementations retain responsibility for capability,
     * numeric precision and size limits. Aggregations require a non-null item and function, and nonblank field and alias.
     * Native SQL and the builder's optional-filter and aggregation normalization are unchanged.</p>
     *
     * @param query common query model to validate; its state is not modified
     * @throws TSDBException with {@code ARGUMENT_ERROR} when a shared argument is invalid
     */
    public static void validate(TSDBQuery query) {
        if (query == null) {
            throw argument("query must not be null");
        }
        if (query.getLimit() != null && query.getLimit() <= 0) {
            throw argument("limit must be greater than 0");
        }
        if (query.getOffset() != null && query.getOffset() < 0) {
            throw argument("offset must be greater than or equal to 0");
        }
        if (query.getStartTime() != null && query.getEndTime() != null
                && query.getStartTime() > query.getEndTime()) {
            throw argument("startTime must be less than or equal to endTime");
        }
        for (QueryFilter filter : query.getFilters()) {
            validateFilter(filter);
        }
        for (AggregationSpec aggregation : query.getAggregations()) {
            validateAggregation(aggregation);
        }
    }

    /** Rejects malformed aggregation specifications before any SQL translator can dereference them. */
    private static void validateAggregation(AggregationSpec aggregation) {
        if (aggregation == null) {
            throw argument("aggregation must not be null");
        }
        if (aggregation.function() == null) {
            throw argument("aggregation function must not be null");
        }
        if (aggregation.field() == null || aggregation.field().isBlank()) {
            throw argument("aggregation field must not be empty");
        }
        if (aggregation.alias() == null || aggregation.alias().isBlank()) {
            throw argument("aggregation alias must not be empty");
        }
    }

    /** Checks operand shape and portable literal validity without changing caller-owned filters. */
    private static void validateFilter(QueryFilter filter) {
        if (filter == null || filter.column() == null || filter.column().isBlank() || filter.operator() == null) {
            throw argument("Invalid query filter: column and operator must not be empty");
        }
        List<Object> values = filter.values();
        int required = switch (filter.operator()) {
            case BETWEEN -> 2;
            case IN -> -1;
            default -> 1;
        };
        if (values.isEmpty() || (required > 0 && values.size() != required)) {
            String expected = required < 0 ? "at least one value" : "exactly " + required + " value(s)";
            throw argument(filter.operator() + " filter requires " + expected + ": " + filter.column());
        }
        for (Object value : values) {
            if (value == null) {
                throw argument("Filter values must not be null: " + filter.column());
            }
            if ((value instanceof Double doubleValue && !Double.isFinite(doubleValue))
                    || (value instanceof Float floatValue && !Float.isFinite(floatValue))) {
                throw argument("Filter numbers must be finite: " + filter.column());
            }
        }
    }

    /**
     * Rejects colliding aggregate output names using the backend's physical column identity.
     * <p>The generated {@code window_start} name is reserved only when a time window is emitted.
     * Grouping tags and aggregate aliases share the same result namespace. Detail projections,
     * caller-owned query state, and native SQL remain unchanged. Adapters invoke this after their
     * unsupported-operation checks and before submitting the translated query.</p>
     *
     * @param query common query whose aggregate result names are validated
     * @param normalizeColumn backend column normalizer, preserving quoted case when applicable
     * @throws TSDBException with {@code ARGUMENT_ERROR} for duplicate aggregate output names
     */
    public static void validateAggregationOutputNames(TSDBQuery query, UnaryOperator<String> normalizeColumn) {
        if (query == null) {
            throw argument("query must not be null");
        }
        if (!query.hasAggregations()) {
            return;
        }
        Set<String> outputNames = new HashSet<>();
        if (query.getGroupByTime() != null && !query.getGroupByTime().isBlank()) {
            addAggregationOutputName(outputNames, "window_start", normalizeColumn);
        }
        for (String tag : query.getGroupByTags()) {
            addAggregationOutputName(outputNames, tag, normalizeColumn);
        }
        for (AggregationSpec aggregation : query.getAggregations()) {
            if (aggregation == null) {
                throw argument("aggregation must not be null");
            }
            addAggregationOutputName(outputNames, aggregation.alias(), normalizeColumn);
        }
    }

    private static void addAggregationOutputName(Set<String> outputNames, String name,
                                                 UnaryOperator<String> normalizeColumn) {
        String normalized = normalizeColumn.apply(name);
        if (normalized == null || normalized.isBlank()) {
            throw argument("aggregation output name must not be empty");
        }
        if (!outputNames.add(normalized)) {
            throw argument("Duplicate aggregation output name: " + name);
        }
    }

    private static TSDBException argument(String message) {
        return new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, message);
    }
}
