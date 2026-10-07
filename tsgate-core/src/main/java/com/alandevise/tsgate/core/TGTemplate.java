package com.alandevise.tsgate.core;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.metadata.DefaultTSDBMetadataResolver;
import com.alandevise.tsgate.metadata.TSDBColumnMetadata;
import com.alandevise.tsgate.metadata.TSDBEntityMetadata;
import com.alandevise.tsgate.metadata.TSDBMetadataResolver;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.PageResult;
import com.alandevise.tsgate.model.QueryResult;
import com.alandevise.tsgate.model.SortOrderEnum;
import com.alandevise.tsgate.model.SortSpec;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * <h2>TsGate template for application code</h2>
 * <p>Callers supply write POJOs annotated with {@code @TGMeasurement/@TGTime/@TGTag/@TGField}.
 * The template resolves annotations, converts objects to internal records, and delegates writes and queries to the selected adapter.
 * Query rows are mapped to application result objects without exposing {@link QueryResult}.</p>
 * <p>Map results support {@code Map}, {@code LinkedHashMap}, {@code HashMap}, and {@code TreeMap} only.
 * The default {@code Map} preserves result-column order; {@code TreeMap} sorts keys.
 * Unsupported Map types are rejected before database I/O, including queries with no results.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
@Slf4j
public class TGTemplate {

    private static final int DEFAULT_QUERY_LIMIT = 1_000;
    private static final int MAX_QUERY_LIMIT = 10_000;

    private final TSDBAdapter adapter;
    private final TSDBMetadataResolver metadataResolver;

    /**
     * Creates a TsGate template with the default annotation metadata resolver.
     *
     * @param adapter selected TSDB adapter, for example {@code new IoTDBTableAdapter(iotdbConfig, poolConfig)}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public TGTemplate(TSDBAdapter adapter) {
        this(adapter, new DefaultTSDBMetadataResolver());
    }

    /**
     * Creates a template with a custom metadata resolver for testing or alternative annotation strategies.
     *
     * @param adapter          selected TSDB adapter, for example a Spring-injected {@code IoTDBTableAdapter}
     * @param metadataResolver metadata resolver, for example {@code new DefaultTSDBMetadataResolver()}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public TGTemplate(TSDBAdapter adapter,
                        TSDBMetadataResolver metadataResolver) {
        this.adapter = adapter;
        this.metadataResolver = metadataResolver;
    }

    /**
     * Writes one annotated POJO to the adapter's configured default database.
     *
     * @param businessObject annotated business object, for example {@code new AccruePoint(...)}
     * @return whether the write succeeded
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public boolean write(Object businessObject) {
        return write(null, businessObject);
    }

    /**
     * Writes one annotated POJO to the selected database; missing databases or tables produce backend exceptions.
     *
     * @param database target database, for example {@code "tsdb"}; blank selects the adapter's configured default
     * @param businessObject annotated business object, for example {@code new AccruePoint(...)}
     * @return whether the write succeeded
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public boolean write(String database,
                         Object businessObject) {
        return batchWrite(database, Collections.singletonList(businessObject));
    }

    /**
     * Writes annotated POJOs in a batch to the adapter's configured default database.
     *
     * @param records annotated business objects, for example {@code List.of(point1, point2)}
     * @return whether the write succeeded
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public boolean batchWrite(Collection<?> records) {
        return batchWrite((String) null, records);
    }

    /**
     * Writes annotated POJOs in a batch to the selected database.
     *
     * @param database target database, for example {@code "tsdb"}; blank selects the adapter's configured default
     * @param records  annotated business objects, for example {@code List.of(point1, point2)}
     * @return whether the write succeeded
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public boolean batchWrite(String database,
                              Collection<?> records) {
        return batchWriteDetailed(database, records).isSuccess();
    }

    /**
     * Writes a batch to the adapter's default database and reports the confirmed commit boundary.
     * <p>All business objects are converted and validated before any database I/O.</p>
     *
     * @param records annotated business objects, for example {@code List.of(point1, point2)}
     * @return detailed batch-write result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-11
     */
    public BatchWriteResult batchWriteDetailed(Collection<?> records) {
        return batchWriteDetailed(null, records);
    }

    /**
     * Writes a batch to the selected database and reports the confirmed commit boundary.
     * <p>Checks the adapter's batch limit, copies the input into a stable snapshot, and converts every object to {@link TSDBRecord}.
     * The adapter receives the batch only after every record has converted and validated successfully. If any record
     * fails conversion or validation, the method throws immediately without submitting any records to the adapter,
     * preventing earlier records from being written before a later validation failure.</p>
     *
     * @param database target database, for example {@code "tsdb"}; blank selects the adapter default
     * @param records  annotated business objects, for example {@code List.of(point1, point2)}
     * @return detailed batch-write result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-11
     */
    public BatchWriteResult batchWriteDetailed(String database,
                                               Collection<?> records) {
        if (records == null || records.isEmpty()) {
            return BatchWriteResult.emptySuccess();
        }
        TSDBAdapter writeAdapter = requireDefaultAdapter();
        List<?> sourceSnapshot = createValidatedSnapshot(records, writeAdapter);
        List<TSDBRecord> converted = convertRecords(sourceSnapshot);
        return writeAdapter.batchWriteDetailed(database, converted);
    }

    /**
     * Creates a stable input snapshot, checking the adapter's record limit both before and after copying.
     *
     * @param records      original business objects
     * @param writeAdapter adapter used for this write
     * @return stable snapshot that satisfies the batch-size limit
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-13
     */
    private static List<?> createValidatedSnapshot(Collection<?> records,
                                                   TSDBAdapter writeAdapter) {
        int requestedRecords = records.size();
        // Check before List.copyOf(records) to reject obviously oversized collections early,
        // avoiding the allocation and copying cost of a very large input.
        validateBatchSizeOrThrow(requestedRecords, writeAdapter);
        List<?> sourceSnapshot;
        try {
            sourceSnapshot = List.copyOf(records);
        } catch (RuntimeException e) {
            throw batchValidationException(requestedRecords, 0, e);
        }
        // Recheck the stable snapshot in case another thread changed the source collection while it was copied.
        validateBatchSizeOrThrow(sourceSnapshot.size(), writeAdapter);
        return sourceSnapshot;
    }

    /**
     * Converts every object in the stable snapshot into a common record accepted by adapters.
     *
     * @param sourceSnapshot business object snapshot already validated against the batch-size limit
     * @return immutable common write records
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-13
     */
    private List<TSDBRecord> convertRecords(List<?> sourceSnapshot) {
        List<TSDBRecord> converted = new ArrayList<>(sourceSnapshot.size());
        try {
            for (Object source : sourceSnapshot) {
                converted.add(metadataResolver.toRecord(source));
            }
        } catch (RuntimeException e) {
            throw batchValidationException(sourceSnapshot.size(), converted.size(), e);
        }
        return List.copyOf(converted);
    }

    /**
     * Validates batch size and wraps failures as batch-write exceptions with zero confirmed submissions.
     *
     * @param requestedRecords requested record count
     * @param writeAdapter     adapter used for this write
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-13
     */
    private static void validateBatchSizeOrThrow(int requestedRecords,
                                                 TSDBAdapter writeAdapter) {
        try {
            validateBatchSizeBeforeConversion(requestedRecords, writeAdapter);
        } catch (RuntimeException e) {
            throw batchValidationException(requestedRecords, 0, e);
        }
    }

    /**
     * Checks the adapter's record limit before conversion to avoid unnecessary reflection and temporary allocations.
     *
     * @param requestedRecords requested record count
     * @param writeAdapter     adapter used for this write
     * @throws TSDBException if the adapter reports an invalid limit or the request exceeds it
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-13
     */
    private static void validateBatchSizeBeforeConversion(int requestedRecords,
                                                          TSDBAdapter writeAdapter) {
        int maxBatchRecords = writeAdapter.getMaxBatchRecords();
        if (maxBatchRecords <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "TSDB adapter max batch records must be greater than 0: "
                    + maxBatchRecords);
        }
        if (requestedRecords > maxBatchRecords) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "TSDB batch write record count exceeds adapter max batch records: "
                    + requestedRecords + " > " + maxBatchRecords);
        }
    }

    /**
     * Creates a pre-write validation failure with a confirmed zero-commit result.
     *
     * @param requestedRecords requested record count
     * @param validatedRecords records already converted and validated
     * @param cause            validation failure cause
     * @return batch-write exception carrying a zero-commit result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-13
     */
    private static TSDBBatchWriteException batchValidationException(int requestedRecords,
                                                                    int validatedRecords,
                                                                    RuntimeException cause) {
        BatchWriteResult result = new BatchWriteResult(requestedRecords, validatedRecords, 0, 0, 0,
                null, null, BatchCommitStateEnum.NOT_COMMITTED, false,
                "Batch validation failed before database I/O: " + cause.getMessage());
        return new TSDBBatchWriteException(result, cause);
    }

    /**
     * Validates the entity's annotation metadata before writing a batch to the adapter's default database.
     *
     * @param recordType business object class, for example {@code AccruePoint.class}
     * @param records    business objects, for example {@code List.of(point1, point2)}
     * @return whether the write succeeded
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public boolean batchWrite(Class<?> recordType,
                              Collection<?> records) {
        metadataResolver.resolve(recordType);
        return records == null || records.isEmpty() || batchWrite(records);
    }

    /**
     * Creates a fluent query builder for the selected write POJO class.
     * <p>recordType determines the measurement; the result class can be overridden in {@code list(ResultClass.class)}
     * or a pagination method.</p>
     *
     * @param recordType annotated write model class, for example {@code AccruePoint.class}
     * @param <T>        default query result type
     * @return fluent query builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public <T> TGQueryBuilder<T> query(Class<T> recordType) {
        TSDBEntityMetadata metadata = metadataResolver.resolve(recordType);
        return new TGQueryBuilder<>(this, recordType, metadata.measurement(),
                metadata.timeColumn().getColumnName());
    }

    /**
     * Executes native query SQL in the adapter's dialect and returns one map per row.
     * <p>The caller supplies the complete SQL; the template takes no database argument and does not qualify table names.</p>
     *
     * @param sql backend query SQL, for example {@code "SELECT * FROM ACCRUE LIMIT 10"}
     * @return one map per result row
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public List<Map<String, Object>> executeQuery(String sql) {
        QueryResult result = requireDefaultAdapter().executeQuery(sql);
        ensureSuccess(result);
        return copyRows(result.getRows());
    }

    /**
     * Executes native query SQL in the adapter's dialect and maps rows to the requested result type.
     *
     * @param sql        backend query SQL, for example {@code "SELECT time, value FROM ACCRUE LIMIT 10"}
     * @param resultType result class, for example {@code ValueOnlyResult.class}
     * @param <T>        result object type
     * @return mapped query results
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public <T> List<T> executeQuery(String sql,
                                    Class<T> resultType) {
        validateResultType(resultType);
        QueryResult result = requireDefaultAdapter().executeQuery(sql);
        ensureSuccess(result);
        return mapRows(resultType, result.getRows());
    }

    /**
     * Internal fluent-query entry point that executes a common query and maps its result rows.
     *
     * @param database   query database, for example {@code "tsdb"}; blank selects the adapter default
     * @param resultType result class, for example {@code ValueOnlyResult.class}
     * @param query      common query model, for example containing {@code measurement=ACCRUE, limit=100}
     * @param <T>        result object type
     * @return mapped query results
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    <T> List<T> queryList(String database,
                          Class<T> resultType,
                          TSDBQuery query) {
        validateResultType(resultType);
        QueryResult result = queryRaw(database, query);
        ensureSuccess(result);
        return mapRows(resultType, result.getRows());
    }

    /**
     * Executes a time-cursor page query.
     *
     * @param database   query database, for example {@code "tsdb"}; blank selects the adapter default
     * @param resultType result class, for example {@code AccruePoint.class}
     * @param query      common query model, for example containing {@code cursorTime=1783000000000L, limit=50}
     * @param <T>        result object type
     * @return time-cursor page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    <T> PageResult<T> queryCursorPage(String database,
                                      Class<T> resultType,
                                      TSDBQuery query) {
        validateResultType(resultType);
        int requestedLimit = resolveLimit(query.getLimit());
        TSDBQuery pageQuery = normalizeQuery(query);
        if (pageQuery.hasAggregations()) {
            throw new TSDBException(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                    "cursor page does not support aggregation query; use offset page instead");
        }
        rejectCustomSortForTimeCursorPage(pageQuery, "page()");
        pageQuery.setStrictCursor(false);
        pageQuery.setLimit(incrementForPageProbe(requestedLimit));
        pageQuery.setPaginationProbe(true);
        pageQuery.setOffset(null);
        QueryResult raw = requireDefaultAdapter().query(database, pageQuery);
        ensureSuccess(raw);

        List<Map<String, Object>> pageRows = trimPageRows(raw.getRows(), requestedLimit);
        boolean hasNext = pageRows.size() < safeRows(raw.getRows()).size();
        Long nextCursorTime = hasNext && !pageRows.isEmpty()
                ? extractTime(pageRows.get(pageRows.size() - 1), pageQuery.getTimeColumn()) : null;
        if (hasNext && nextCursorTime == null) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                    "Cannot extract a usable time cursor from query result: " + pageQuery.getTimeColumn());
        }
        return new PageResult<>(mapRows(resultType, pageRows), nextCursorTime, hasNext,
                requestedLimit, SortOrderEnum.normalize(pageQuery.getOrder()));
    }

    /**
     * Executes a composite-cursor page query.
     * <p>The default composite key is {@code time} plus the write POJO's {@code @TGTag} columns.
     * From the second page, generates lexicographic cursor predicates; ascending order uses, for example,
     * {@code (time > ?) OR (time = ? AND device_code > ?)}, avoiding skipped rows that share a timestamp.</p>
     *
     * @param database   query database, for example {@code "tsdb"}; blank selects the adapter default
     * @param resultType result class, for example {@code AccruePoint.class}
     * @param query      common query model, for example containing {@code cursorValues={time=1783000000000L}, limit=50}
     * @param <T>        result object type
     * @return composite-cursor page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    <T> PageResult<T> queryStrictCursorPage(String database,
                                            Class<T> resultType,
                                            TSDBQuery query) {
        validateResultType(resultType);
        int requestedLimit = resolveLimit(query.getLimit());
        TSDBQuery pageQuery = normalizeQuery(query);
        if (pageQuery.hasAggregations()) {
            throw new TSDBException(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                    "strictCursorPage does not support aggregation query");
        }
        normalizeStrictCursorIdentifiers(pageQuery);
        List<SortSpec> cursorSortSpecs = resolveStrictCursorSortSpecs(pageQuery);
        List<String> cursorColumns = cursorSortSpecs.stream().map(SortSpec::column).toList();
        pageQuery.setStrictCursor(true);
        pageQuery.setCursorTime(null);
        pageQuery.setCursorColumns(cursorColumns);
        pageQuery.setSortSpecs(cursorSortSpecs);
        pageQuery.setLimit(incrementForPageProbe(requestedLimit));
        pageQuery.setPaginationProbe(true);
        pageQuery.setOffset(null);
        appendMissingSelectColumns(pageQuery, cursorColumns);
        validateStrictCursorValues(pageQuery);

        QueryResult raw = requireDefaultAdapter().query(database, pageQuery);
        ensureSuccess(raw);

        List<Map<String, Object>> safeRows = safeRows(raw.getRows());
        boolean hasNext = safeRows.size() > requestedLimit;
        List<Map<String, Object>> currentRows = hasNext
                ? new ArrayList<>(safeRows.subList(0, requestedLimit))
                : safeRows;
        Map<String, Object> lastCursor = null;
        for (Map<String, Object> row : currentRows) {
            lastCursor = extractCursor(row, cursorColumns, pageQuery.getTimeColumn());
        }
        Map<String, Object> nextCursor = hasNext ? lastCursor : null;
        return new PageResult<>(mapRows(resultType, currentRows), nextCursor, hasNext,
                requestedLimit, SortOrderEnum.normalize(pageQuery.getOrder()));
    }

    /**
     * Executes a traditional limit/offset page query.
     *
     * @param database   query database, for example {@code "tsdb"}; blank selects the adapter default
     * @param resultType result class, for example {@code ValueStatusResult.class}
     * @param query      common query model, for example containing {@code measurement=ACCRUE, order=DESC}
     * @param pageNum    one-based page number, for example {@code 2}
     * @param pageSize   rows per page, for example {@code 50}
     * @param <T>        result object type
     * @return limit/offset page result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    <T> PageResult<T> queryOffsetPage(String database,
                                      Class<T> resultType,
                                      TSDBQuery query,
                                      int pageNum,
                                      int pageSize) {
        validateResultType(resultType);
        if (pageNum < 1) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "pageNum must be greater than or equal to 1");
        }
        int requestedPageNum = pageNum;
        int requestedPageSize = resolveLimit(pageSize);
        long offsetValue;
        try {
            offsetValue = Math.multiplyExact(requestedPageNum - 1L, requestedPageSize);
        } catch (ArithmeticException e) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "pageNum and pageSize produce an overflowing offset", e);
        }
        if (offsetValue > Integer.MAX_VALUE) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "pageNum and pageSize produce an offset greater than " + Integer.MAX_VALUE);
        }
        int offset = (int) offsetValue;

        TSDBQuery pageQuery = normalizeQuery(query);
        pageQuery.setCursorTime(null);
        pageQuery.setStrictCursor(false);
        pageQuery.setCursorValues(Collections.emptyMap());
        pageQuery.setLimit(requestedPageSize);
        pageQuery.setOffset(offset);
        TSDBAdapter queryAdapter = requireDefaultAdapter();
        long total = queryAdapter.count(database, pageQuery);
        if (total < 0L) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                    "adapter returned a negative offset page total: " + total);
        }

        if (total == 0L || offsetValue >= total) {
            return new PageResult<>(Collections.emptyList(), false,
                    requestedPageNum, requestedPageSize, offset,
                    SortOrderEnum.normalize(pageQuery.getOrder()), total);
        }

        QueryResult raw = queryAdapter.query(database, pageQuery);
        ensureSuccess(raw);

        List<Map<String, Object>> safeRows = safeRows(raw.getRows());
        boolean hasNext = offsetValue + safeRows.size() < total;
        return new PageResult<>(mapRows(resultType, safeRows), hasNext,
                requestedPageNum, requestedPageSize, offset,
                SortOrderEnum.normalize(pageQuery.getOrder()), total);
    }

    /**
     * Resolves composite-cursor columns and appends time plus all declared tags to form a stable unique key.
     *
     * @param query normalized query model, for example containing {@code recordType=AccruePoint.class}
     * @return composite-cursor columns, for example {@code List.of("time", "device_code", "point_type", "source")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private List<String> resolveStrictCursorColumns(TSDBQuery query) {
        List<String> configuredColumns = query.getCursorColumns();
        TSDBEntityMetadata metadata = metadataResolver.resolve(query.getRecordType());
        List<String> columns = new ArrayList<>();
        if (configuredColumns != null) {
            configuredColumns.forEach(column -> addColumnIfAbsent(columns, column));
        }
        addColumnIfAbsent(columns, query.getTimeColumn());
        for (TSDBColumnMetadata tagColumn : metadata.tagColumns()) {
            addColumnIfAbsent(columns, requireDefaultAdapter().normalizeColumnIdentifier(tagColumn.getColumnName()));
        }
        return columns;
    }

    /**
     * Resolves per-column sorting for strict cursors. Without custom sorting, preserves the original behavior:
     * time and tag columns all use the primary direction. Custom sorting retains column order and direction,
     * appending missing time and tag columns with the first sort's direction. Explicit cursorColumns supplied
     * alongside custom sorting must match that sorting before key columns are appended, keeping predicates aligned with ORDER BY.
     *
     * @param query normalized strict-cursor query
     * @return per-column strict-cursor sort specifications
     * @throws TSDBException if explicit cursor columns do not match custom sort columns
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private List<SortSpec> resolveStrictCursorSortSpecs(TSDBQuery query) {
        if (hasCustomDetailSort(query)) {
            List<SortSpec> customSorts = new ArrayList<>(query.getSortSpecs());
            if (query.getCursorColumns() != null && !query.getCursorColumns().isEmpty()) {
                validateCursorColumnsMatchSortSpecs(query.getCursorColumns(), customSorts);
            }
            SortOrderEnum order = customSorts.get(0).order();
            for (String column : resolveStrictCursorColumns(query)) {
                if (customSorts.stream().noneMatch(sort -> sort.column().equals(column))) {
                    customSorts.add(new SortSpec(column, order));
                }
            }
            return customSorts;
        }
        List<SortSpec> resolved = new ArrayList<>();
        SortOrderEnum order = SortOrderEnum.normalize(query.getOrder());
        for (String cursorColumn : resolveStrictCursorColumns(query)) {
            resolved.add(new SortSpec(cursorColumn, order));
        }
        return resolved;
    }

    /**
     * Validates that explicit cursor columns exactly match the mixed-direction sort column sequence.
     *
     * @param cursorColumns explicitly configured strict-cursor columns
     * @param sortSpecs     custom per-column sort specifications
     * @throws TSDBException if the column count, names, or order differ
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private static void validateCursorColumnsMatchSortSpecs(List<String> cursorColumns,
                                                            List<SortSpec> sortSpecs) {
        if (cursorColumns.size() != sortSpecs.size()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "strict cursorColumns must match custom sort columns in the same order");
        }
        for (int i = 0; i < cursorColumns.size(); i++) {
            if (!cursorColumns.get(i).equals(sortSpecs.get(i).column())) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "strict cursorColumns must match custom sort columns in the same order");
            }
        }
    }

    /**
     * Appends required cursor columns to an explicit selection so the next cursor can be extracted from result rows.
     *
     * @param query         normalized query model, for example containing {@code selectColumns=[value]}
     * @param cursorColumns composite-cursor columns, for example {@code List.of("time", "device_code")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static void appendMissingSelectColumns(TSDBQuery query,
                                                   List<String> cursorColumns) {
        if (query.getSelectColumns() == null || query.getSelectColumns().isEmpty()) {
            return;
        }
        List<String> selectColumns = new ArrayList<>(query.getSelectColumns());
        for (String cursorColumn : cursorColumns) {
            addColumnIfAbsent(selectColumns, cursorColumn);
        }
        query.setSelectColumns(selectColumns);
    }

    /**
     * Validates and extracts a composite cursor from a returned detail row.
     *
     * @param row           returned detail row, for example {@code Map.of("time", 1783000000000L, "device_code", "D001")}
     * @param cursorColumns composite-cursor columns, for example {@code List.of("time", "device_code")}
     * @param timeColumn    canonical physical time column
     * @return composite cursor to supply with the next page request
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private Map<String, Object> extractCursor(Map<String, Object> row,
                                               List<String> cursorColumns, String timeColumn) {
        Map<String, Object> canonicalRow = normalizeCursorKeys(row, TSDBErrorCodeEnum.QUERY_ERROR);
        Map<String, Object> cursor = new LinkedHashMap<>();
        for (String cursorColumn : cursorColumns) {
            Object value = canonicalRow.get(cursorColumn);
            if (cursorColumn.equals(timeColumn)) {
                value = toEpochMillis(value);
            }
            if (value == null) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                        "Cannot extract strict cursor column from query result: " + cursorColumn);
            }
            cursor.put(cursorColumn, value);
        }
        return cursor;
    }

    /** Canonicalizes strict-cursor identifiers before selection, sorting, and cursor validation. */
    private void normalizeStrictCursorIdentifiers(TSDBQuery query) {
        TSDBAdapter queryAdapter = requireDefaultAdapter();
        query.setTimeColumn(queryAdapter.normalizeColumnIdentifier(query.getTimeColumn()));
        query.setSelectColumns(query.getSelectColumns().stream()
                .map(queryAdapter::normalizeColumnIdentifier).distinct().toList());
        query.setCursorColumns(query.getCursorColumns().stream()
                .map(queryAdapter::normalizeColumnIdentifier).toList());
        Map<String, SortSpec> sorts = new LinkedHashMap<>();
        for (SortSpec sort : query.getSortSpecs()) {
            String column = queryAdapter.normalizeColumnIdentifier(sort.column());
            SortSpec previous = sorts.putIfAbsent(column, new SortSpec(column, sort.order()));
            if (previous != null && previous.order() != sort.order()) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Conflicting sort directions for physical column: " + column);
            }
        }
        query.setSortSpecs(new ArrayList<>(sorts.values()));
        query.setCursorValues(normalizeCursorKeys(query.getCursorValues(), TSDBErrorCodeEnum.ARGUMENT_ERROR));
    }

    /** Rejects ambiguous physical keys instead of guessing which value defines a cursor boundary. */
    private Map<String, Object> normalizeCursorKeys(Map<String, Object> values, TSDBErrorCodeEnum errorCode) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        if (values == null) {
            return normalized;
        }
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String column = requireDefaultAdapter().normalizeColumnIdentifier(entry.getKey());
            if (column == null || column.isBlank() || normalized.containsKey(column)) {
                throw new TSDBException(errorCode, "Invalid or ambiguous strict cursor column: " + column);
            }
            normalized.put(column, entry.getValue());
        }
        return normalized;
    }

    /** Requires an exact, complete cursor matching the final physical sort columns. */
    private static void validateStrictCursorValues(TSDBQuery query) {
        Map<String, Object> values = query.getCursorValues();
        if (values.isEmpty()) {
            return;
        }
        if (!values.keySet().equals(new LinkedHashSet<>(query.getCursorColumns()))) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Strict cursor values must exactly match the physical cursor columns: " + query.getCursorColumns());
        }
        for (String column : query.getCursorColumns()) {
            if (values.get(column) == null) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "Missing strict cursor value: " + column);
            }
        }
    }

    /**
     * Adds a physical column once, preserving case-distinct SELECT and cursor columns.
     *
     * @param columns destination column collection, for example {@code List.of("time")}
     * @param column  column to add, for example {@code "device_code"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static void addColumnIfAbsent(List<String> columns,
                                          String column) {
        if (column == null || column.trim().isEmpty()) {
            return;
        }
        for (String existing : columns) {
            if (existing != null && existing.equals(column.trim())) {
                return;
            }
        }
        columns.add(column.trim());
    }

    /**
     * Executes a query and retains the adapter's raw result for template mapping and pagination.
     *
     * @param database query database, for example {@code "tsdb"}; blank selects the adapter default
     * @param query    common query model, for example containing {@code measurement=ACCRUE, selectColumns=[value]}
     * @return the adapter's raw query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    QueryResult queryRaw(String database,
                         TSDBQuery query) {
        TSDBQuery normalized = normalizeQuery(query);
        return requireDefaultAdapter().query(database, normalized);
    }

    /**
     * Fills in measurement, limit, offset, and ordering so adapters receive a normalized query model.
     *
     * @param query original query model, for example containing {@code recordType=AccruePoint.class, limit=null}
     * @return normalized query model
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private TSDBQuery normalizeQuery(TSDBQuery query) {
        if (query == null || query.getRecordType() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "query recordType must not be null");
        }
        TSDBEntityMetadata metadata = metadataResolver.resolve(query.getRecordType());
        TSDBQuery normalized = query.copy();
        normalized.setPaginationProbe(false);
        normalized.setMeasurement(metadata.measurement());
        normalized.setTimeColumn(metadata.timeColumn().getColumnName());
        normalized.setLimit(resolveLimit(normalized.getLimit()));
        normalized.setOffset(resolveOffset(normalized.getOffset()));
        normalized.setOrder(SortOrderEnum.normalize(normalized.getOrder()));
        if (normalized.hasAggregations() && hasCustomDetailSort(normalized)) {
            throw new TSDBException(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                    "custom field sorting is only supported for detail list and offset page queries");
        }
        return normalized;
    }

    /**
     * Rejects field sorting for time-only cursors, preventing duplicates or omissions from time-only predicates.
     *
     * @param query     time-cursor query to execute
     * @param operation public operation name, for example {@code page()}
     * @throws TSDBException if the query includes a non-time sort specification
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private static void rejectCustomSortForTimeCursorPage(TSDBQuery query,
                                                          String operation) {
        if (hasCustomDetailSort(query)) {
            throw new TSDBException(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                    operation + " does not support custom field sorting; use list() or offset page instead");
        }
    }

    /**
     * Determines whether custom sorting includes anything beyond the single time column.
     */
    private static boolean hasCustomDetailSort(TSDBQuery query) {
        if (query.getSortSpecs() == null || query.getSortSpecs().isEmpty()) {
            return false;
        }
        return query.getSortSpecs().size() != 1
                || !query.getTimeColumn().equals(query.getSortSpecs().get(0).column());
    }

    /**
     * Maps raw result rows to the requested result type.
     *
     * @param resultType result class, for example {@code ValueOnlyResult.class}
     * @param rows       adapter result rows, for example {@code List.of(Map.of("value", 12.34D))}
     * @param <T>        result object type
     * @return mapped business objects
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private <T> List<T> mapRows(Class<T> resultType,
                                List<Map<String, Object>> rows) {
        List<T> mappedRows = new ArrayList<>();
        for (Map<String, Object> row : safeRows(rows)) {
            if (Map.class.isAssignableFrom(resultType)) {
                Map<String, Object> mapRow = resultType == TreeMap.class ? new TreeMap<>()
                        : resultType == HashMap.class ? new HashMap<>() : new LinkedHashMap<>();
                if (row != null) {
                    mapRow.putAll(row);
                }
                mappedRows.add(resultType.cast(mapRow));
                continue;
            }
            mappedRows.add(metadataResolver.toEntity(resultType, row));
        }
        return mappedRows;
    }

    /** Validates map result types before any query or count request, including empty-result queries. */
    private static void validateResultType(Class<?> resultType) {
        if (resultType == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "resultType must not be null");
        }
        if (Map.class.isAssignableFrom(resultType) && resultType != Map.class
                && resultType != LinkedHashMap.class && resultType != HashMap.class && resultType != TreeMap.class) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Supported Map result types are Map, LinkedHashMap, HashMap, and TreeMap: " + resultType.getName());
        }
    }

    /**
     * Copies raw rows so callers cannot mutate the adapter's result collection.
     *
     * @param rows adapter result rows, for example {@code List.of(Map.of("device_code", "D001"))}
     * @return copied result rows
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static List<Map<String, Object>> copyRows(List<Map<String, Object>> rows) {
        List<Map<String, Object>> copied = new ArrayList<>();
        for (Map<String, Object> row : safeRows(rows)) {
            copied.add(row == null ? new LinkedHashMap<>() : new LinkedHashMap<>(row));
        }
        return copied;
    }

    /**
     * Throws on adapter failure instead of exposing an apparently successful empty result list.
     *
     * @param result adapter query result, for example {@code QueryResult.failure("timeout")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void ensureSuccess(QueryResult result) {
        if (result == null) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR, "TSDB query result must not be null");
        }
        if (!result.isSuccess()) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR, result.getMessage());
        }
    }

    /**
     * Trims prefetched cursor results to the requested page size.
     * <p>This method supports time-only cursors. If more than pageSize rows share a timestamp,
     * later pages can skip remaining rows at that timestamp; callers should use {@code strictCursorPage()} instead.</p>
     *
     * @param rows  prefetched adapter rows, for example {@code List.of(row1, row2, row3)}
     * @param limit requested page size, for example {@code 50}
     * @return trimmed current page rows
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    private static List<Map<String, Object>> trimPageRows(List<Map<String, Object>> rows, int limit) {
        List<Map<String, Object>> safeRows = safeRows(rows);
        if (limit <= 0 || safeRows.size() <= limit) {
            return safeRows;
        }
        return new ArrayList<>(safeRows.subList(0, limit));
    }

    /**
     * Replaces a null adapter row list with an empty collection to simplify pagination handling.
     *
     * @param rows adapter result rows, for example {@code null} or {@code List.of(Map.of("value", 1.0D))}
     * @return a non-null row collection
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static List<Map<String, Object>> safeRows(List<Map<String, Object>> rows) {
        return rows == null ? Collections.emptyList() : rows;
    }

    /**
     * Extracts epoch milliseconds from adapter rows, supporting IoTDB time/Time and normalized InfluxDB _time names.
     *
     * @param row        one query result row, for example {@code Map.of("time", 1783000000000L)}
     * @param timeColumn physical time column declared by the entity annotation
     * @return epoch milliseconds, or null when parsing fails
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    private static Long extractTime(Map<String, Object> row, String timeColumn) {
        if (row == null) {
            return null;
        }
        // Try backend-specific time column candidates in order, continuing after an unparseable value.
        List<String> candidates = new ArrayList<>();
        addColumnIfAbsent(candidates, timeColumn);
        addColumnIfAbsent(candidates, "time");
        addColumnIfAbsent(candidates, "Time");
        addColumnIfAbsent(candidates, "_time");
        addColumnIfAbsent(candidates, "timestamp");
        for (String key : candidates) {
            Long epochMillis = toEpochMillis(row.get(key));
            if (epochMillis != null) {
                return epochMillis;
            }
            // SQL backends may change column-name case; prefer exact names before considering case variants.
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (!key.equals(entry.getKey()) && key.equalsIgnoreCase(entry.getKey())) {
                    epochMillis = toEpochMillis(entry.getValue());
                    if (epochMillis != null) {
                        return epochMillis;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Converts common time representations into epoch milliseconds.
     * Numeric values must be finite, integral, and within the signed-long range.
     * Instant and ISO values retain their existing millisecond resolution.
     *
     * @param value time value, for example {@code 1783000000000L}, {@code Instant.now()}, or {@code "2026-07-03T00:00:00Z"}
     * @return epoch milliseconds, or null when parsing fails
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    private static Long toEpochMillis(Object value) {
        try {
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                return ((Number) value).longValue();
            }
            if (value instanceof BigInteger) {
                return ((BigInteger) value).longValueExact();
            }
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).longValueExact();
            }
            if (value instanceof Float || value instanceof Double) {
                // Preserve the actual binary value, including exactly representable Long.MIN_VALUE.
                return new BigDecimal(((Number) value).doubleValue()).longValueExact();
            }
            if (value instanceof Number) {
                return new BigDecimal(value.toString()).longValueExact();
            }
            if (value instanceof Instant) {
                return ((Instant) value).toEpochMilli();
            }
            if (value instanceof Date) {
                return ((Date) value).getTime();
            }
            if (value instanceof CharSequence) {
                String text = value.toString().trim();
                return text.isEmpty() ? null : Instant.parse(text).toEpochMilli();
            }
        } catch (ArithmeticException | NumberFormatException | DateTimeParseException ignored) {
            return null;
        }
        return null;
    }

    /**
     * Applies a default query limit to prevent unbounded reads when callers omit one.
     *
     * @param limit query row limit, for example {@code 100}
     * @return normalized query row limit
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static int resolveLimit(Integer limit) {
        if (limit == null) {
            return DEFAULT_QUERY_LIMIT;
        }
        return validateLimit(limit);
    }

    /**
     * Applies a default query limit for offset pagination's pageSize overload.
     *
     * @param limit query row limit, for example {@code 100}
     * @return normalized query row limit
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static int resolveLimit(int limit) {
        return validateLimit(limit);
    }

    /**
     * Validates query or page size, preventing excessive limits and overflow from the pagination lookahead row.
     *
     * @param limit requested query row count, for example {@code 100}
     * @return validated row count, for example {@code 100}
     * @throws TSDBException if limit is outside {@code 1~10000}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private static int validateLimit(int limit) {
        if (limit <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "limit must be greater than 0");
        }
        if (limit > MAX_QUERY_LIMIT) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "limit must not exceed " + MAX_QUERY_LIMIT + ": " + limit);
        }
        return limit;
    }

    /**
     * Fetches one extra row to determine hasNext, using exact addition to detect overflow.
     *
     * @param limit validated page size, for example {@code 100}
     * @return actual adapter query limit, for example {@code 101}
     * @throws TSDBException if {@code limit + 1} overflows
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private static int incrementForPageProbe(int limit) {
        try {
            return Math.addExact(limit, 1);
        } catch (ArithmeticException e) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "limit is too large for page probe: " + limit, e);
        }
    }

    /**
     * Validates offset; null omits the offset clause and negative values are rejected.
     *
     * @param offset query offset, for example {@code 200}
     * @return normalized query offset
     * @throws TSDBException if offset is negative
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static Integer resolveOffset(Integer offset) {
        if (offset == null) {
            return null;
        }
        if (offset < 0) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "offset must be greater than or equal to 0");
        }
        return offset;
    }

    /**
     * Returns the default adapter with an explicit error for missing auto-configuration or incomplete setup.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    private TSDBAdapter requireDefaultAdapter() {
        if (adapter == null) {
            String message = "No TSDB adapter available";
            log.error(message);
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, message);
        }
        return adapter;
    }
}
