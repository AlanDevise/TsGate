package com.alandevise.tsgate.core;

import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.OperatorEnum;
import com.alandevise.tsgate.model.PageResult;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.SortOrderEnum;
import com.alandevise.tsgate.model.SortSpec;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Fluent query builder for annotated POJOs.
 * <p>Collects query intent; {@link TGTemplate} and the selected adapter perform SQL generation
 * and execution. Query results are always mapped to application objects.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public class TGQueryBuilder<T> {

    private final TGTemplate template;
    private final Class<T> recordType;
    private final TSDBQuery query;
    private String database;

    /**
     * Creates a fluent query builder.
     *
     * @param template    query execution template, for example a Spring-injected {@code TGTemplate}
     * @param recordType  write POJO class, for example {@code AccruePoint.class}
     * @param measurement logical table name, for example {@code "ACCRUE"}
     * @param timeColumn  physical time column name, for example {@code "time"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    TGQueryBuilder(TGTemplate template,
                     Class<T> recordType,
                     String measurement,
                     String timeColumn) {
        this.template = template;
        this.recordType = recordType;
        this.query = new TSDBQuery();
        this.query.setRecordType(recordType);
        this.query.setMeasurement(measurement);
        this.query.setTimeColumn(timeColumn);
    }

    /**
     * Selects the query database; blank uses the adapter's configured default database.
     *
     * @param database database name, for example {@code "tsdb"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> database(String database) {
        this.database = database;
        return this;
    }

    /**
     * Selects the columns returned by a detail query.
     * <p>Defaults to all columns when omitted; otherwise the adapter uses these names in its SELECT list.
     * Accepts multiple fields, for example {@code select("value", "status")}.
     * Adapters append the time column when omitted, supporting subsequent pagination and result mapping.</p>
     * <p>Use database column names or annotated physical names. Annotated fields match physical column names;
     * unannotated DTOs also support exact field names and snake_case to camelCase matching.</p>
     *
     * @param columns selected column names, for example {@code "value", "status"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    public TGQueryBuilder<T> select(String... columns) {
        return select(columns == null ? null : Arrays.asList(columns));
    }

    /**
     * Selects detail-query columns from a collection.
     * <p>Useful for dynamically constructed field lists; equivalent to {@link #select(String...)}.</p>
     *
     * @param columns selected column names, for example {@code List.of("value", "status")}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    public TGQueryBuilder<T> select(Collection<String> columns) {
        this.query.setSelectColumns(copyStrings(columns));
        return this;
    }

    /**
     * Adds an equality filter for a tag.
     *
     * @param tagName tag column name, for example {@code "device_code"}
     * @param value   tag filter value, for example {@code "device001"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> whereTag(String tagName,
                                        Object value) {
        return where(tagName, OperatorEnum.EQ, value);
    }

    /**
     * Adds an IN filter for a tag.
     *
     * @param tagName tag column name, for example {@code "region"}
     * @param values  tag filter values, for example {@code List.of("beijing", "shanghai")}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> whereTagIn(String tagName,
                                          Collection<?> values) {
        return where(tagName, OperatorEnum.IN, values);
    }

    /**
     * Adds a single-value filter for a tag or field.
     *
     * @param column   filter column name, for example {@code "value"}
     * @param operator operator, for example {@code OperatorEnum.GE}
     * @param value    single filter value, for example {@code 10.5D}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> where(String column,
                                     OperatorEnum operator,
                                     Object value) {
        if (value == null) {
            return this;
        }
        return where(column, operator, List.of(value));
    }

    /**
     * Adds a multiple-value filter, typically IN or BETWEEN.
     *
     * @param column   filter column name, for example {@code "value"}
     * @param operator operator, for example {@code OperatorEnum.BETWEEN}
     * @param values   filter values, for example {@code List.of(10.0D, 20.0D)}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> where(String column,
                                     OperatorEnum operator,
                                     Collection<?> values) {
        if (isBlank(column) || operator == null || values == null || values.isEmpty()) {
            return this;
        }
        List<Object> copiedValues = new ArrayList<>();
        for (Object value : values) {
            if (value != null) {
                copiedValues.add(value);
            }
        }
        if (!copiedValues.isEmpty()) {
            this.query.getFilters().add(new QueryFilter(column.trim(), operator, copiedValues));
        }
        return this;
    }

    /**
     * Sets an epoch millisecond time range with both endpoints included.
     *
     * @param startTime inclusive start in epoch milliseconds, for example {@code 1783000000000L}
     * @param endTime   inclusive end in epoch milliseconds, for example {@code 1783086400000L}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> timeRange(Long startTime,
                                         Long endTime) {
        this.query.setStartTime(startTime);
        this.query.setEndTime(endTime);
        return this;
    }

    /**
     * Sets an Instant time range, converting both endpoints to epoch milliseconds.
     *
     * @param startTime start time, for example {@code Instant.parse("2026-07-03T00:00:00Z")}
     * @param endTime   end time, for example {@code Instant.parse("2026-07-04T00:00:00Z")}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> timeRange(Instant startTime,
                                         Instant endTime) {
        this.query.setStartTime(startTime == null ? null : startTime.toEpochMilli());
        this.query.setEndTime(endTime == null ? null : endTime.toEpochMilli());
        return this;
    }

    /**
     * Sets a time cursor for fetching the next page.
     * <p>Omit the cursor on the first request. When {@link PageResult#isHasNext()} is {@code true},
     * pass the previous response's {@link PageResult#getNextCursorTime()} to fetch the next page.
     * Ascending queries add {@code time > cursorTime}; descending queries add {@code time < cursorTime}.</p>
     * <p>This cursor contains time only, without tags or other sort fields. If more than one page of rows shares a timestamp,
     * use {@link #strictCursorPage()} with {@link #cursor(Map)} for composite-cursor pagination.</p>
     *
     * @param cursorTime next cursor from the previous page, for example {@code 1783000000000L}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    public TGQueryBuilder<T> cursorTime(Long cursorTime) {
        this.query.setCursorTime(cursorTime);
        return this;
    }

    /**
     * Sets a composite cursor for strict pagination without omitted rows.
     * <p>Omit this call on the first request. When {@link PageResult#isHasNext()} is {@code true},
     * pass the previous response's {@link PageResult#getNextCursor()} unchanged to fetch the next page.
     * The composite cursor typically contains time plus tags, for example {@code time/device_code/point_type/source}.</p>
     *
     * @param cursor previous page's composite cursor, for example {@code Map.of("time", 1783000000000L, "device_code", "D001")}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public TGQueryBuilder<T> cursor(Map<String, Object> cursor) {
        this.query.setCursorValues(cursor);
        return this;
    }

    /**
     * Selects the composite-cursor sort columns.
     * <p>Defaults to time plus every {@code @TGTag} column declared by the write POJO.
     * Explicit leading keys such as {@code cursorColumns("value")} still receive missing time and tag columns from the template.
     * When combined with custom mixed-direction {@code orderBy.../thenBy...} sorting, the columns and order declared here
     * must match the custom sort before stable unique-key columns are appended to both.</p>
     *
     * @param columns composite-cursor sort columns, for example {@code "time", "device_code", "point_type"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public TGQueryBuilder<T> cursorColumns(String... columns) {
        return cursorColumns(columns == null ? null : Arrays.asList(columns));
    }

    /**
     * Selects composite-cursor sort columns from a collection.
     * <p>Useful for dynamically constructed cursor columns; equivalent to {@link #cursorColumns(String...)}.</p>
     *
     * @param columns composite-cursor sort columns, for example {@code List.of("time", "device_code", "source")}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public TGQueryBuilder<T> cursorColumns(Collection<String> columns) {
        this.query.setCursorColumns(copyStrings(columns));
        return this;
    }

    /**
     * Sets the maximum rows returned by this query.
     *
     * @param limit maximum returned rows, for example {@code 100}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> limit(Integer limit) {
        this.query.setLimit(limit);
        return this;
    }

    /**
     * Controls total result-row and total-page counting for offset pagination.
     * <p>Defaults to true. False skips the count query and returns null for both totals;
     * one extra result row determines whether another page exists. List queries and cursor
     * pagination retain their existing behavior.</p>
     *
     * @param totalPageCount whether offset pages should count total rows and total pages
     * @return this builder
     */
    public TGQueryBuilder<T> totalPageCount(boolean totalPageCount) {
        this.query.setTotalPageCount(totalPageCount);
        return this;
    }

    /**
     * Sets the offset for traditional limit/offset queries.
     *
     * @param offset rows to skip, for example {@code 200}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> offset(Integer offset) {
        this.query.setOffset(offset);
        return this;
    }

    /**
     * Sorts by time in ascending order.
     *
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> orderByTimeAsc() {
        return replaceOrder(this.query.getTimeColumn(), SortOrderEnum.ASC);
    }

    /**
     * Sorts by time in descending order.
     *
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> orderByTimeDesc() {
        return replaceOrder(this.query.getTimeColumn(), SortOrderEnum.DESC);
    }

    /**
     * Uses the given field as the first ascending sort column, replacing earlier sort specifications.
     *
     * @param field field column name, for example {@code "value"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    public TGQueryBuilder<T> orderByFieldAsc(String field) {
        return replaceOrder(field, SortOrderEnum.ASC);
    }

    /**
     * Uses the given field as the first descending sort column, replacing earlier sort specifications.
     *
     * @param field field column name, for example {@code "value"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    public TGQueryBuilder<T> orderByFieldDesc(String field) {
        return replaceOrder(field, SortOrderEnum.DESC);
    }

    /**
     * Appends an ascending field sort used only when preceding sort values are equal.
     *
     * @param field field column name, for example {@code "status"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    public TGQueryBuilder<T> thenByFieldAsc(String field) {
        return appendOrder(field, SortOrderEnum.ASC);
    }

    /**
     * Appends a descending field sort used only when preceding sort values are equal.
     *
     * @param field field column name, for example {@code "status"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    public TGQueryBuilder<T> thenByFieldDesc(String field) {
        return appendOrder(field, SortOrderEnum.DESC);
    }

    /**
     * Sets the aggregation time window, for example {@code 5m} or {@code 1h}.
     *
     * @param interval window interval, for example {@code "5m"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> groupByTime(String interval) {
        this.query.setGroupByTime(interval);
        return this;
    }

    /**
     * Sets the aggregation time window using {@link Duration}.
     *
     * @param interval window interval, for example {@code Duration.ofMinutes(5)}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> groupByTime(Duration interval) {
        this.query.setGroupByTime(interval == null ? null : interval.toMillis() + "ms");
        return this;
    }

    /**
     * Adds a tag grouping column for an aggregate query.
     *
     * @param tagName tag grouping column, for example {@code "device_code"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> groupByTag(String tagName) {
        if (!isBlank(tagName)) {
            this.query.getGroupByTags().add(tagName.trim());
        }
        return this;
    }

    /**
     * Adds multiple tag grouping columns for an aggregate query.
     *
     * @param tagNames tag grouping columns, for example {@code List.of("device_code", "point_type")}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> groupByTags(Collection<String> tagNames) {
        this.query.getGroupByTags().addAll(copyStrings(tagNames));
        return this;
    }

    /**
     * Adds a field aggregation specification.
     *
     * @param field    aggregation field name, for example {@code "value"}
     * @param function aggregation function, for example {@code AggregationFunctionEnum.AVG}
     * @param alias    aggregate result alias, for example {@code "avg_value"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> aggregate(String field,
                                         AggregationFunctionEnum function,
                                         String alias) {
        if (!isBlank(field)) {
            AggregationFunctionEnum resolvedFunction = function == null ? AggregationFunctionEnum.AVG : function;
            String resolvedAlias = isBlank(alias)
                    ? field.trim() + "_" + resolvedFunction.name().toLowerCase(Locale.ROOT)
                    : alias.trim();
            this.query.getAggregations().add(new AggregationSpec(field.trim(), resolvedFunction, resolvedAlias));
        }
        return this;
    }

    /**
     * Sets the time zone for window aggregation, for example {@code Asia/Shanghai}.
     *
     * @param timeZone time-zone ID, for example {@code "Asia/Shanghai"}
     * @return this builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TGQueryBuilder<T> timeZone(String timeZone) {
        this.query.setTimeZone(timeZone);
        return this;
    }

    /**
     * Executes the query and maps rows to this builder's POJO class.
     *
     * @return mapped query results
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public List<T> list() {
        return template.queryList(database, recordType, query);
    }

    /**
     * Executes the query and maps rows to the requested result type.
     *
     * @param resultType result class, for example {@code ValueOnlyResult.class}
     * @param <R>        result object type
     * @return mapped query results
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public <R> List<R> list(Class<R> resultType) {
        return template.queryList(database, resultType, query);
    }

    /**
     * Executes a time-cursor page query.
     * <p>Set the page size with {@link #limit(Integer)} and pass the previous page's
     * {@code nextCursorTime} to {@link #cursorTime(Long)} to fetch the next page.</p>
     *
     * @return time-cursor page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    public PageResult<T> page() {
        return template.queryCursorPage(database, recordType, query);
    }

    /**
     * Executes a time-cursor page query and maps rows to the requested result type.
     * <p>Use a dedicated result type for partial selections; aggregate queries require offset pagination.</p>
     *
     * @param resultType result class, for example {@code ValueOnlyResult.class}
     * @param <R>        result object type
     * @return time-cursor page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    public <R> PageResult<R> page(Class<R> resultType) {
        return template.queryCursorPage(database, resultType, query);
    }

    /**
     * Executes a composite-cursor page query.
     * <p>Where {@link #page()} uses a time-only cursor, this method defaults to time plus tag columns
     * for sorting and next-page predicates, avoiding skipped rows that share a timestamp.
     * Explicit {@code orderBy.../thenBy...} specifications support different directions for individual cursor columns.</p>
     * <p>Column identity follows the backend: InfluxDB preserves physical case, while IoTDB folds unquoted names.
     * Pass the returned {@code nextCursor} unchanged. Missing, null, or ambiguous cursor columns fail explicitly.</p>
     *
     * @return composite-cursor page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public PageResult<T> strictCursorPage() {
        return template.queryStrictCursorPage(database, recordType, query);
    }

    /**
     * Executes a composite-cursor page query and maps rows to the requested result type.
     * <p>Use a dedicated result type for partial detail selections.
     * If {@link #select(String...)} is supplied, the template appends required cursor columns so the next cursor can be extracted.</p>
     *
     * @param resultType result class, for example {@code ValueOnlyResult.class}
     * @param <R>        result object type
     * @return composite-cursor page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public <R> PageResult<R> strictCursorPage(Class<R> resultType) {
        return template.queryStrictCursorPage(database, resultType, query);
    }

    /**
     * Executes a traditional limit/offset page query.
     * <p>Total rows and total pages are counted unless {@link #totalPageCount(boolean)} is false.</p>
     *
     * @param pageNum  one-based page number, for example {@code 2}
     * @param pageSize rows per page, for example {@code 50}
     * @return limit/offset page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public PageResult<T> page(int pageNum,
                              int pageSize) {
        return template.queryOffsetPage(database, recordType, query, pageNum, pageSize);
    }

    /**
     * Executes a traditional limit/offset page query and maps rows to the requested result type.
     * <p>Total rows and total pages are counted unless {@link #totalPageCount(boolean)} is false.</p>
     *
     * @param pageNum    one-based page number, for example {@code 2}
     * @param pageSize   rows per page, for example {@code 50}
     * @param resultType result class, for example {@code ValueOnlyResult.class}
     * @param <R>        result object type
     * @return limit/offset page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public <R> PageResult<R> page(int pageNum,
                                  int pageSize,
                                  Class<R> resultType) {
        return template.queryOffsetPage(database, resultType, query, pageNum, pageSize);
    }

    /**
     * Creates a query snapshot for inspecting the common model before adapter translation.
     *
     * @return current query model snapshot
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TSDBQuery build() {
        return query.copy();
    }

    /**
     * Replaces the primary sort column.
     */
    private TGQueryBuilder<T> replaceOrder(String column,
                                             SortOrderEnum order) {
        if (isBlank(column)) {
            return this;
        }
        this.query.setSortSpecs(List.of(new SortSpec(column, order)));
        return this;
    }

    /**
     * Appends a secondary sort; repeated columns update their direction in place to avoid duplicate ORDER BY expressions.
     */
    private TGQueryBuilder<T> appendOrder(String column,
                                            SortOrderEnum order) {
        if (isBlank(column)) {
            return this;
        }
        List<SortSpec> sortSpecs = new ArrayList<>(this.query.getSortSpecs());
        SortSpec requested = new SortSpec(column, order);
        for (int i = 0; i < sortSpecs.size(); i++) {
            if (sortSpecs.get(i).column().equals(requested.column())) {
                sortSpecs.set(i, requested);
                this.query.setSortSpecs(sortSpecs);
                return this;
            }
        }
        sortSpecs.add(requested);
        this.query.setSortSpecs(sortSpecs);
        return this;
    }

    /**
     * Copies string values and removes blank names to prevent empty SQL identifiers.
     *
     * @param values original string collection, for example {@code List.of("value", "status")}
     * @return string collection without null or blank values
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static List<String> copyStrings(Collection<String> values) {
        List<String> copied = new ArrayList<>();
        if (values == null) {
            return copied;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                copied.add(value.trim());
            }
        }
        return copied;
    }

    /**
     * Determines whether a string is null or blank.
     *
     * @param value string to inspect, for example {@code "value"}
     * @return whether the string is null or blank
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
