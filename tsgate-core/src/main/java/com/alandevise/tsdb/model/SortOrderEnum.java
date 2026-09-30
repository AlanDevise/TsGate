package com.alandevise.tsdb.model;

/**
 * Time sort direction in the common query model.
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public enum SortOrderEnum {
    /**
     * Ascending time order.
     */
    ASC,
    /**
     * Descending time order.
     */
    DESC;

    /**
     * Normalizes the sort direction, defaulting to descending time order when unspecified.
     *
     * @param order original sort direction, for example {@code null} or {@code SortOrderEnum.ASC}
     * @return a non-null sort direction
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public static SortOrderEnum normalize(SortOrderEnum order) {
        return order == null ? DESC : order;
    }
}
