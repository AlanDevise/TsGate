package com.alandevise.tsgate.model;

/**
 * Common query filter operators.
 * <p>Describes WHERE conditions in annotated POJO queries. Tags typically use EQ, NE, and IN;
 * the query model provides EQ, NE, GT, GE, LT, LE, IN, and BETWEEN.
 * Supported operators on the primary time column depend on the backend's capabilities.
 * Numeric primary-time operands use whole epoch milliseconds; Instant operands use millisecond precision.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public enum OperatorEnum {
    /**
     * Equal to.
     */
    EQ,
    /**
     * Not equal to.
     */
    NE,
    /**
     * Greater than.
     */
    GT,
    /**
     * Greater than or equal to.
     */
    GE,
    /**
     * Less than.
     */
    LT,
    /**
     * Less than or equal to.
     */
    LE,
    /**
     * Membership in a set.
     */
    IN,
    /**
     * Inclusive range match.
     */
    BETWEEN
}
