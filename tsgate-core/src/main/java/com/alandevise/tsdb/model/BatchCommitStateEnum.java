package com.alandevise.tsdb.model;

/**
 * Confirmed commit state at the end of a batch write.
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-11
 */
public enum BatchCommitStateEnum {

    /**
     * All requested records are confirmed committed.
     */
    SUCCESS,

    /**
     * No requested records are committed.
     */
    NOT_COMMITTED,

    /**
     * Some, but not all, requested records are confirmed committed.
     */
    PARTIALLY_COMMITTED,

    /**
     * Connection interruption or timeout prevents confirmation of the last submission's commit state.
     */
    UNKNOWN
}
