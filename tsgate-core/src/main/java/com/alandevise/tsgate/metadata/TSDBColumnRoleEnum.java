package com.alandevise.tsgate.metadata;

/**
 * Role of an annotated column in the common TSDB model.
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public enum TSDBColumnRoleEnum {
    /**
     * Time column.
     */
    TIME,
    /**
     * Tag or dimension column.
     */
    TAG,
    /**
     * Field or metric-value column.
     */
    FIELD
}
