package com.alandevise.tsdb.model;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Common query model produced by {@code TGQueryBuilder}.
 * <p>Expresses query intent without holding connections; each adapter translates it into its SQL dialect.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
@Getter
public class TSDBQuery {

    /**
     * Logical query table name, for example {@code "ACCRUE"}.
     */
    @Setter
    private String measurement;

    /**
     * Physical time column declared by the entity annotation; defaults to {@code "time"}.
     */
    private String timeColumn = "time";

    /**
     * Query entity class, for example {@code AccruePoint.class}.
     */
    @Setter
    private Class<?> recordType;

    /**
     * Inclusive query start in epoch milliseconds, for example {@code 1783000000000L}.
     */
    @Setter
    private Long startTime;

    /**
     * Query end in epoch milliseconds, for example {@code 1783086400000L}.
     */
    @Setter
    private Long endTime;

    /**
     * Next-page time cursor returned by the previous page, for example {@code 1783000000000L}.
     */
    @Setter
    private Long cursorTime;

    /**
     * Whether composite-cursor pagination is enabled.
     */
    @Setter
    private boolean strictCursor;

    /**
     * Composite-cursor sort columns, for example {@code List.of("time", "device_code", "point_type")}.
     */
    private List<String> cursorColumns = new ArrayList<>();

    /**
     * Composite-cursor values returned by the previous page, for example:
     * {@code Map.of("time", 1783000000000L, "device_code", "D001")}.
     */
    private Map<String, Object> cursorValues = new LinkedHashMap<>();

    /**
     * Maximum returned rows, for example {@code 100}.
     */
    @Setter
    private Integer limit;

    /**
     * Whether this query includes one extra cursor-pagination row used only to detect a following page.
     * Defaults to false. The template enables it only after increasing a cursor page's limit;
     * ordinary lists, offset pages, and count queries do not receive the extra row allowance.
     */
    @Setter
    private boolean paginationProbe;

    /**
     * Rows to skip for offset pagination, for example {@code 200}.
     */
    @Setter
    private Integer offset;

    /**
     * Time sort direction; defaults to descending order.
     */
    private SortOrderEnum order = SortOrderEnum.DESC;

    /**
     * Ordered sort specifications for a detail query.
     * <p>An empty collection preserves compatibility by sorting the time column according to {@link #order}.</p>
     */
    private List<SortSpec> sortSpecs = new ArrayList<>();

    /**
     * Selected columns for a detail query, for example {@code List.of("value", "status")}.
     */
    private List<String> selectColumns = new ArrayList<>();

    /**
     * Query filter conditions.
     */
    private List<QueryFilter> filters = new ArrayList<>();

    /**
     * Tag grouping columns for an aggregate query, for example {@code List.of("device_code", "point_type")}.
     */
    private List<String> groupByTags = new ArrayList<>();

    /**
     * Aggregation time window, for example {@code "5m"}.
     */
    @Setter
    private String groupByTime;

    /**
     * Field aggregation specifications.
     */
    private List<AggregationSpec> aggregations = new ArrayList<>();

    /**
     * Aggregation window time-zone ID, for example {@code "Asia/Shanghai"}.
     */
    @Setter
    private String timeZone;

    /**
     * Sets the physical time column declared by the entity annotation.
     *
     * @param timeColumn physical time column; defaults to {@code time} when blank
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-11
     */
    public void setTimeColumn(String timeColumn) {
        this.timeColumn = timeColumn == null || timeColumn.trim().isEmpty() ? "time" : timeColumn.trim();
    }

    /**
     * Sets the composite-cursor sort columns.
     *
     * @param cursorColumns composite-cursor sort columns, for example {@code List.of("time", "device_code", "point_type")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public void setCursorColumns(List<String> cursorColumns) {
        this.cursorColumns = copyStrings(cursorColumns);
    }

    /**
     * Sets the composite-cursor values returned by the previous page.
     *
     * @param cursorValues composite-cursor values, for example {@code Map.of("time", 1783000000000L, "device_code", "D001")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public void setCursorValues(Map<String, Object> cursorValues) {
        this.cursorValues = copyCursorValues(cursorValues);
    }

    /**
     * Sets the time sort direction.
     *
     * @param order time sort direction, for example {@code SortOrderEnum.DESC}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public void setOrder(SortOrderEnum order) {
        this.order = SortOrderEnum.normalize(order);
    }

    /**
     * Sets detail-query sort specifications.
     *
     * @param sortSpecs sort specifications, for example {@code List.of(new SortSpec("time", ASC), new SortSpec("value", DESC))}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    public void setSortSpecs(List<SortSpec> sortSpecs) {
        this.sortSpecs = sortSpecs == null ? new ArrayList<>() : new ArrayList<>(sortSpecs);
        this.sortSpecs.removeIf(java.util.Objects::isNull);
        if (!this.sortSpecs.isEmpty()) {
            this.order = this.sortSpecs.get(0).order();
        }
    }

    /**
     * Sets detail-query selected columns.
     *
     * @param selectColumns selected column names, for example {@code List.of("value", "status")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public void setSelectColumns(List<String> selectColumns) {
        this.selectColumns = copyStrings(selectColumns);
    }

    /**
     * Sets query filter conditions.
     *
     * @param filters filter conditions, for example {@code List.of(new QueryFilter("device_code", OperatorEnum.EQ, List.of("D001")))}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public void setFilters(List<QueryFilter> filters) {
        this.filters = filters == null ? new ArrayList<>() : new ArrayList<>(filters);
    }

    /**
     * Sets aggregate-query tag grouping columns.
     *
     * @param groupByTags tag grouping columns, for example {@code List.of("device_code", "point_type")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public void setGroupByTags(List<String> groupByTags) {
        this.groupByTags = copyStrings(groupByTags);
    }

    /**
     * Sets field aggregation specifications.
     *
     * @param aggregations aggregation specifications, for example {@code List.of(new AggregationSpec("value", AggregationFunctionEnum.AVG, "avg_value"))}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public void setAggregations(List<AggregationSpec> aggregations) {
        this.aggregations = aggregations == null ? new ArrayList<>() : new ArrayList<>(aggregations);
    }

    /**
     * Determines whether the query contains aggregation specifications.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public boolean hasAggregations() {
        return aggregations != null && !aggregations.isEmpty();
    }

    /**
     * Copies the query so later builder changes cannot affect the snapshot passed to the template.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TSDBQuery copy() {
        TSDBQuery copy = new TSDBQuery();
        copy.setMeasurement(measurement);
        copy.setTimeColumn(timeColumn);
        copy.setRecordType(recordType);
        copy.setStartTime(startTime);
        copy.setEndTime(endTime);
        copy.setCursorTime(cursorTime);
        copy.setStrictCursor(strictCursor);
        copy.setCursorColumns(cursorColumns);
        copy.setCursorValues(cursorValues);
        copy.setLimit(limit);
        copy.setPaginationProbe(paginationProbe);
        copy.setOffset(offset);
        copy.setOrder(order);
        copy.setSortSpecs(sortSpecs);
        copy.setSelectColumns(selectColumns);
        copy.setFilters(filters);
        copy.setGroupByTags(groupByTags);
        copy.setGroupByTime(groupByTime);
        copy.setAggregations(aggregations);
        copy.setTimeZone(timeZone);
        return copy;
    }

    /**
     * Copies composite-cursor values, dropping blank column names and preserving caller-provided column order.
     *
     * @param values original cursor values, for example {@code Map.of("time", 1783000000000L)}
     * @return sanitized cursor values
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static Map<String, Object> copyCursorValues(Map<String, Object> values) {
        Map<String, Object> copied = new LinkedHashMap<>();
        if (values == null) {
            return copied;
        }
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String key = entry.getKey();
            if (key != null && !key.trim().isEmpty() && entry.getValue() != null) {
                copied.put(key.trim(), entry.getValue());
            }
        }
        return copied;
    }

    /**
     * Copies a string list and removes null or blank entries to prevent invalid SQL column names.
     *
     * @param values original string list, for example {@code List.of("value", "status")}
     * @return sanitized string list
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static List<String> copyStrings(List<String> values) {
        if (values == null) {
            return new ArrayList<>();
        }
        List<String> copied = new ArrayList<>();
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                copied.add(value.trim());
            }
        }
        return copied;
    }

}
