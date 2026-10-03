package com.alandevise.tsgate.adapter;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.QueryResult;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;

import java.util.Collection;
import java.util.Collections;

/**
 * Common adapter contract for table-model time-series databases.
 * <p>The core layer exchanges only {@link TSDBRecord} and {@link TSDBQuery}.
 * Each adapter translates writes and queries to its backend protocol or SQL dialect; adapters do not create databases or tables.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public interface TSDBAdapter {

    /**
     * Initializes the underlying connection, pool, or HTTP client.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    void init();

    /**
     * Closes the underlying connection resources.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    void close();

    /**
     * Executes caller-provided native query SQL without adding a database name.
     *
     * @param sql native TSDB query SQL, for example {@code "SELECT * FROM ACCRUE LIMIT 10"}
     * @return the query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    QueryResult executeQuery(String sql);

    /**
     * Writes one common record, delegating to the batch implementation by default.
     *
     * @param database target database, for example {@code "tsdb"}; a blank value selects the adapter default
     * @param tsdbRecord common write record, for example {@code new TSDBRecord("ACCRUE", 1783000000000L, tags, fields)}
     * @return whether the write succeeded
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    default boolean write(String database,
                          TSDBRecord tsdbRecord) {
        return batchWrite(database, Collections.singletonList(tsdbRecord));
    }

    /**
     * Writes a batch of common records.
     *
     * @param database target database, for example {@code "tsdb"}; a blank value selects the adapter default
     * @param records  common write records, for example {@code List.of(record1, record2)}
     * @return whether the write succeeded
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    default boolean batchWrite(String database,
                               Collection<TSDBRecord> records) {
        return batchWriteDetailed(database, records).isSuccess();
    }

    /**
     * Writes a batch and reports the confirmed commit boundary.
     *
     * @param database target database, for example {@code "tsdb"}; a blank value selects the adapter default
     * @param records  common write records, for example {@code List.of(record1, record2)}
     * @return detailed commit state, confirmed record count, and failed batch position
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-11
     */
    BatchWriteResult batchWriteDetailed(String database,
                                        Collection<TSDBRecord> records);

    /**
     * Returns the maximum records per batch request so the template can reject excessive inputs before converting business objects.
     * <p>Custom adapters default to {@link Integer#MAX_VALUE} to preserve compatibility with existing implementations.</p>
     *
     * @return maximum records accepted in one batch request
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-13
     */
    default int getMaxBatchRecords() {
        return Integer.MAX_VALUE;
    }

    /**
     * Executes a common query model.
     * <p>Built-in adapters reject a null query, nonpositive explicit limit, negative offset, and reversed
     * time bounds with {@code ARGUMENT_ERROR} before database I/O. A null limit leaves SQL unpaged;
     * backend result limits still apply. Zero offsets and equal time bounds are valid.</p>
     *
     * @param database target database, for example {@code "tsdb"}; a blank value selects the adapter default
     * @param query    common query model, for example containing {@code measurement=ACCRUE, limit=100}
     * @return the query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    QueryResult query(String database,
                      TSDBQuery query);

    /**
     * Returns the backend's canonical identity for a physical column.
     * <p>The default preserves case because quoted identifiers may distinguish {@code value} from {@code VALUE}.
     * Backends that fold unquoted identifiers must apply the same normalization to cursor input, SQL, and result keys.
     * This method must be deterministic and must not perform database I/O.</p>
     *
     * @param column physical column name, or null
     * @return canonical column name, or null for a null input
     */
    default String normalizeColumnIdentifier(String column) {
        return column == null ? null : column.trim();
    }

    /**
     * Counts query result rows before pagination.
     * <p>Detail queries should normally use a direct {@code COUNT(*)}; aggregate or grouped queries should count
     * rows in the aggregate result set. Built-in adapters use optimized backend SQL. To preserve compatibility,
     * custom adapters that do not override this method execute an unpaged query without cursors and count its returned rows.</p>
     *
     * @param database target database, for example {@code "tsdb"}; a blank value selects the adapter default
     * @param query    non-null common query model; pagination and cursor parameters are ignored
     * @return total rows before pagination
     * @throws TSDBException with {@code ARGUMENT_ERROR} if the query is null
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    default long count(String database,
                       TSDBQuery query) {
        if (query == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "query must not be null");
        }
        TSDBQuery countQuery = query.copy();
        countQuery.setPaginationProbe(false);
        countQuery.setCursorTime(null);
        countQuery.setStrictCursor(false);
        countQuery.setCursorValues(Collections.emptyMap());
        countQuery.setLimit(null);
        countQuery.setOffset(null);
        QueryResult result = query(database, countQuery);
        if (result == null) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                    "TSDB count fallback query result must not be null");
        }
        if (!result.isSuccess()) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR, result.getMessage());
        }
        return result.getRows() == null ? 0L : result.getRows().size();
    }

    /**
     * Returns the adapter's display name.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    String getAdapterName();
}
