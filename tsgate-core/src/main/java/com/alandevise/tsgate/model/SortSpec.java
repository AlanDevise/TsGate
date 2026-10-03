package com.alandevise.tsgate.model;

/**
 * One sort specification for a detail query.
 *
 * <p>Specifications form the SQL {@code ORDER BY} clause in collection order, for example
 * {@code time ASC, value DESC, status ASC}。</p>
 *
 * @param column sort column name
 * @param order  sort direction
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-08-25
 */
public record SortSpec(String column, SortOrderEnum order) {

    /**
     * Normalizes the sort column name and direction.
     *
     * @param column sort column name, for example {@code "value"}
     * @param order  sort direction, for example {@code SortOrderEnum.DESC}
     */
    public SortSpec {
        if (column == null || column.trim().isEmpty()) {
            throw new IllegalArgumentException("sort column must not be blank");
        }
        column = column.trim();
        order = SortOrderEnum.normalize(order);
    }
}
