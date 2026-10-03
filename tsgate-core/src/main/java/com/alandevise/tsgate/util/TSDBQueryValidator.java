package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.TSDBQuery;

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

    private static TSDBException argument(String message) {
        return new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, message);
    }
}
