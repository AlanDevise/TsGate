package com.alandevise.tsgate.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Common query result.
 * <p>Carries adapter query or native SQL results. columns stores column names and rows stores row data;
 * rowCount records the row count, while success and message describe the execution outcome.</p>
 * <p>Contains result data only, with no underlying TSDB connection or cursor resources.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
@Data
public class QueryResult {
    /**
     * Returned column names.
     */
    private List<String> columns = new ArrayList<>();
    /**
     * Returned data rows.
     */
    private List<Map<String, Object>> rows = new ArrayList<>();
    /**
     * Returned row count.
     */
    private int rowCount;
    /**
     * Execution outcome description.
     */
    private String message;
    /**
     * Whether execution succeeded.
     */
    private boolean success;

    /**
     * Creates a failed result.
     *
     * @param message failure reason, for example {@code "SQL must not be empty"}
     * @return a failed query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public static QueryResult failure(String message) {
        QueryResult result = new QueryResult();
        result.setSuccess(false);
        result.setMessage(message);
        return result;
    }
}
