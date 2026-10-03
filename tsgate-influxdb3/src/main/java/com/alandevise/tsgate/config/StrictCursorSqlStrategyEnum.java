package com.alandevise.tsgate.config;

/**
 * SQL strategy for InfluxDB 3 structured strict composite cursor continuation pages.
 * The strategy is selected explicitly; HTTP errors never trigger a strategy change.
 */
public enum StrictCursorSqlStrategyEnum {
    /** Uses one SELECT with lexicographic OR predicates; the default strategy. */
    OR,
    /** Uses disjoint SELECT branches with global ordering and pagination for older query planners. */
    UNION_ALL
}
