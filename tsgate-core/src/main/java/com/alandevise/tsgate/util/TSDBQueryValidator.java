package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.TSDBQuery;

import java.util.HashSet;
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
     * Rejects absent queries, non-positive explicit limits, negative offsets, and reversed time bounds.
     * <p>Null limits and offsets, zero offsets, equal or one-sided time bounds, and positive pagination-probe
     * limits remain valid. Backend implementations retain responsibility for their own capability and size limits.</p>
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
