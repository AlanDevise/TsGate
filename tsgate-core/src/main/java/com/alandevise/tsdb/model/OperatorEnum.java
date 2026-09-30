package com.alandevise.tsdb.model;

/**
 * Common query filter operators.
 * <p>Describes WHERE conditions in annotated POJO queries. Tags typically use EQ, NE, and IN;
 * fields support EQ, NE, GT, GE, LT, LE, IN, and BETWEEN.</p>
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
