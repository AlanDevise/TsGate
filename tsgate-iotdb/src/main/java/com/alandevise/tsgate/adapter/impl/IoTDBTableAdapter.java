package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.util.TimeWindowDuration;
import com.alandevise.tsgate.util.TimeWindowPlan;
import com.alandevise.tsgate.util.TSDBQueryValidator;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.config.IoTDBNodeDiscoveryModeEnum;
import com.alandevise.tsgate.config.IoTDBProperties;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.OperatorEnum;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.QueryResult;
import com.alandevise.tsgate.model.SortOrderEnum;
import com.alandevise.tsgate.model.SortSpec;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * Adapter for the IoTDB table model.
 * <p>Writes {@link TSDBRecord} values converted from annotated POJOs into existing tables using Tablets.
 * Queries translate the shared query model into table-model SQL. This adapter does not create databases, tables, or columns.</p>
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
@Slf4j
public class IoTDBTableAdapter implements TSDBAdapter {

    /**
     * Allowlist for simple unquoted identifiers that may be embedded directly in SQL.
     */
    private static final String IDENTIFIER_PATTERN = "[A-Za-z_][A-Za-z0-9_]*";

    private final IoTDBProperties config;
    private final IoTDBProperties.IoTDBPoolConfig pool;
    private final int tabletMaxRowSize;
    private final int maxBatchRecords;
    private final int maxQueryRows;
    private final boolean queryLogEnabled;
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
    private LifecycleState state = LifecycleState.NEW;

    private enum LifecycleState { NEW, READY, CLOSED }
    /**
     * The default database actually bound to the pool. Its value remains the stable borrow/return baseline
     * after initialization and physical pool recovery.
     */
    private String poolDatabase;
    /**
     * Return the IoTDB table SessionPool exposed by auto-configuration as an injectable official client bean.
     */
    private ITableSessionPool sessionPool;
    /**
     * A recoverable pool proxy with a stable public identity. It safely replaces the official IoTDB physical pool internally,
     * so injected {@link ITableSessionPool} beans do not retain a permanently invalid pool.
     * @since 2026-08-26
     */
    private RecoverableTableSessionPool recoverableSessionPool;

    /**
     * Create an IoTDB table-model adapter.
     * @param config IoTDB connection settings; for example {@code database=tsdb}
     * @param pool IoTDB connection pool settings; for example {@code nodeUrls=[127.0.0.1:16669]}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public IoTDBTableAdapter(IoTDBProperties config,
                             IoTDBProperties.IoTDBPoolConfig pool) {
        this(config, pool, true);
    }

    /**
     * Create an IoTDB table-model adapter with configurable SQL query logging.
     * Automatic node discovery depends on the number of endpoints in {@code nodeUrls}.
     * Connection, table, pool and endpoint settings are copied at construction and remain unchanged during recovery.
     * @param config IoTDB connection settings; for example {@code database=tsdb}
     * @param pool IoTDB connection pool settings; for example {@code nodeUrls=[127.0.0.1:16669]}
     * @param queryLogEnabled whether to log the executed SQL at DEBUG level
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-22
     */
    public IoTDBTableAdapter(IoTDBProperties config,
                             IoTDBProperties.IoTDBPoolConfig pool,
                             boolean queryLogEnabled) {
        config = snapshotConfiguration(config);
        pool = snapshotPoolConfiguration(pool);
        this.config = config;
        this.pool = pool;
        this.tabletMaxRowSize = resolveTabletMaxRowSize(config);
        this.maxBatchRecords = resolveMaxBatchRecords(config);
        this.maxQueryRows = resolveMaxQueryRows(config);
        this.queryLogEnabled = queryLogEnabled;
    }

    /** Copies adapter settings, including nested settings, without changing null validation behavior. */
    private static IoTDBProperties snapshotConfiguration(IoTDBProperties source) {
        if (source == null) {
            return null;
        }
        IoTDBProperties copy = new IoTDBProperties();
        copy.setEnable(source.isEnable());
        copy.setFailFast(source.isFailFast());
        copy.setDiscoveryMode(source.getDiscoveryMode());
        copy.setUsername(source.getUsername());
        copy.setPassword(source.getPassword());
        copy.setDatabase(source.getDatabase());
        copy.setMaxBatchRecords(source.getMaxBatchRecords());
        copy.setMaxQueryRows(source.getMaxQueryRows());
        IoTDBProperties.IoTDBConnectionConfig table = source.getTable();
        if (table == null) {
            copy.setTable(null);
        } else {
            IoTDBProperties.IoTDBConnectionConfig tableCopy = new IoTDBProperties.IoTDBConnectionConfig();
            tableCopy.setTabletMaxRowSize(table.getTabletMaxRowSize());
            tableCopy.setRpcCompressionEnabled(table.isRpcCompressionEnabled());
            copy.setTable(tableCopy);
        }
        copy.setPool(snapshotPoolConfiguration(source.getPool()));
        return copy;
    }

    /** The pool parameter may differ from properties.getPool(); preserve its independent settings. */
    private static IoTDBProperties.IoTDBPoolConfig snapshotPoolConfiguration(IoTDBProperties.IoTDBPoolConfig source) {
        if (source == null) {
            return null;
        }
        IoTDBProperties.IoTDBPoolConfig copy = new IoTDBProperties.IoTDBPoolConfig();
        copy.setEnabled(source.isEnabled());
        copy.setNodeUrls(source.getNodeUrls() == null ? null : new ArrayList<>(source.getNodeUrls()));
        copy.setMaxSize(source.getMaxSize());
        copy.setWaitToGetSessionTimeoutInMs(source.getWaitToGetSessionTimeoutInMs());
        copy.setConnectionTimeoutInMs(source.getConnectionTimeoutInMs());
        copy.setQueryTimeoutInMs(source.getQueryTimeoutInMs());
        copy.setMaxRetryCount(source.getMaxRetryCount());
        copy.setRetryIntervalInMs(source.getRetryIntervalInMs());
        copy.setFetchSize(source.getFetchSize());
        return copy;
    }

    /**
     * Initialize the IoTDB table-session pool once. Single-session mode is disabled.
     * <p>Repeated initialization preserves the exposed pool proxy. Failed initialization may be retried until this instance is closed.
     * The pool binds the configured default database; physical sessions connect on first borrow. Deployment must ensure that the
     * database already exists before that borrow and remains available throughout operation.</p>
     * @throws TSDBException if this instance is closed or initialization fails
     */
    @Override
    public void init() {
        lifecycleLock.writeLock().lock();
        try {
            if (state == LifecycleState.READY) {
                return;
            }
            if (state == LifecycleState.CLOSED) {
                throw stateError();
            }
            ITableSessionPool createdPool = null;
            try {
                validatePoolConfiguration();
                poolDatabase = normalizeConfiguredDatabase();
                List<String> nodeUrls = resolveNodeUrls();
                IoTDBNodeDiscoveryModeEnum discoveryMode = config.getDiscoveryMode();
                boolean nodeDiscoveryEnabled = resolveNodeDiscoveryEnabled(discoveryMode, nodeUrls.size());
                createdPool = buildPhysicalSessionPool(nodeUrls, nodeDiscoveryEnabled);
                RecoverableTableSessionPool newPool = new RecoverableTableSessionPool(createdPool);
                log.info("IoTDB table SessionPool resources initialized with a pool-level database, "
                                + "nodes: {}, maxSize: {}, database: {}, discoveryMode: {}, "
                                + "endpointCount: {}, autoFetch: {}, redirection: {}",
                        nodeUrls, pool.getMaxSize(), poolDatabase, discoveryMode,
                        nodeUrls.size(), nodeDiscoveryEnabled, nodeDiscoveryEnabled);
                recoverableSessionPool = newPool;
                sessionPool = newPool;
                state = LifecycleState.READY;
            } catch (RuntimeException | Error failure) {
                closePool(createdPool);
                poolDatabase = null;
                sessionPool = null;
                recoverableSessionPool = null;
                if (failure instanceof TSDBException || failure instanceof Error) {
                    throw failure;
                }
                throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                        "Failed to initialize IoTDB table SessionPool", failure);
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    /**
     * Wait for adapter operations to finish and permanently close this instance; repeated calls are harmless.
     * <p>Native operations bypass the adapter lifecycle lock. An already borrowed native session retains its physical pool
     * until it is returned, but the exposed proxy rejects new borrows after close. Callers must close borrowed sessions,
     * must not close the adapter-owned pool themselves, and must coordinate native operation shutdown separately.</p>
     */
    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (state == LifecycleState.CLOSED) {
                return;
            }
            state = LifecycleState.CLOSED;
            ITableSessionPool currentPool = sessionPool;
            sessionPool = null;
            recoverableSessionPool = null;
            poolDatabase = null;
            closePool(currentPool);
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    /**
     * Return the adapter-owned native pool, whose identity remains stable during initialization and connection recovery.
     * Native operations bypass adapter limits and lifecycle locking; callers must not close this pool.
     * @return initialized pool proxy
     * @throws TSDBException if this adapter is not initialized or is closed
     */
    public ITableSessionPool getSessionPool() {
        return withReady(() -> sessionPool);
    }

    private static void closePool(ITableSessionPool poolToClose) {
        if (poolToClose != null) {
            try {
                poolToClose.close();
            } catch (RuntimeException failure) {
                log.warn("Failed to close IoTDB table SessionPool", failure);
            }
        }
    }

    private <T> T withReady(Supplier<T> operation) {
        lifecycleLock.readLock().lock();
        try {
            if (state != LifecycleState.READY) {
                throw stateError();
            }
            return operation.get();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private TSDBException stateError() {
        return new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                state == LifecycleState.CLOSED ? "IoTDB adapter is closed" : "IoTDB adapter is not initialized");
    }

    /**
     * Build a new official IoTDB pool with explicit Tablet RPC compression settings.
     * @param nodeUrls node endpoints
     * @param nodeDiscoveryEnabled whether node discovery and redirection are enabled
     * @return new physical pool
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    private ITableSessionPool buildPhysicalSessionPool(List<String> nodeUrls,
                                                       boolean nodeDiscoveryEnabled) {
        return new TableSessionPoolBuilder()
                .nodeUrls(nodeUrls)
                .user(config.getUsername())
                .password(config.getPassword())
                // Bind a pool-level default database so new and returned sessions share the same database context.
                .database(poolDatabase)
                .maxSize(pool.getMaxSize())
                .waitToGetSessionTimeoutInMs(pool.getWaitToGetSessionTimeoutInMs())
                .connectionTimeoutInMs(pool.getConnectionTimeoutInMs())
                .queryTimeoutInMs(pool.getQueryTimeoutInMs())
                .maxRetryCount(pool.getMaxRetryCount())
                .retryIntervalInMs(pool.getRetryIntervalInMs())
                .fetchSize(pool.getFetchSize())
                .enableIoTDBRpcCompression(config.getTable().isRpcCompressionEnabled())
                .enableAutoFetch(nodeDiscoveryEnabled)
                .enableRedirection(nodeDiscoveryEnabled)
                .build();
    }

    /**
     * Build a physical pool from the current settings for proxy replacement after a query connection failure.
     * @return new physical pool
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    private ITableSessionPool buildPhysicalSessionPool() {
        List<String> nodeUrls = resolveNodeUrls();
        boolean nodeDiscoveryEnabled = resolveNodeDiscoveryEnabled(
                config.getDiscoveryMode(), nodeUrls.size());
        return buildPhysicalSessionPool(nodeUrls, nodeDiscoveryEnabled);
    }

    /**
     * Execute application-supplied native table-model SQL without issuing USE.
     * The pool has already selected the configured default database for the borrowed session.
     * @param sql IoTDB table-model SQL statement; for example {@code "SELECT * FROM tsdb.ACCRUE LIMIT 10"}
     * @return query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public QueryResult executeQuery(String sql) {
        return withReady(() -> {
            String trimmedSql = sql == null ? "" : sql.trim();
            if (trimmedSql.isEmpty()) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "SQL must not be empty");
            }
            return executeRawQuery(trimmedSql);
        });
    }

    /**
     * Write shared records in batches, combining records for the same measurement into Tablet batches.
     * @param database target database; blank uses the configured default when permitted; for example {@code "tsdb"}
     * @param records shared records to write; for example {@code List.of(record1, record2)}
     * @return batch outcome with an explicit commit boundary
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public BatchWriteResult batchWriteDetailed(String database,
                                               Collection<TSDBRecord> records) {
        return withReady(() -> writeReady(database, records));
    }

    private BatchWriteResult writeReady(String database, Collection<TSDBRecord> records) {
        if (records == null || records.isEmpty()) {
            return BatchWriteResult.emptySuccess();
        }
        List<TSDBRecord> snapshot;
        Map<String, TableBatch> batches;
        try {
            snapshot = List.copyOf(records);
            validateBatchSize(snapshot);
            batches = buildBatches(snapshot);
            validatePreparedBatches(batches);
        } catch (RuntimeException e) {
            throw preflightFailure(records.size(), e);
        }
        WriteProgress progress = new WriteProgress(countTablets(batches));
        try {
            if (sessionPool == null) {
                throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                        "IoTDB table SessionPool is not initialized");
            }
            try (ITableSession tableSession = sessionPool.getSession()) {
                useDatabaseIfNecessary(tableSession, database, true);
                for (TableBatch batch : batches.values()) {
                    writeBatch(tableSession, batch, progress);
                }
            }
            return BatchWriteResult.success(snapshot.size(), progress.totalBatches);
        } catch (IoTDBConnectionException | StatementExecutionException | RuntimeException e) {
            log.error("Failed to batch write records to IoTDB table model", e);
            if (progress.committedRecords >= snapshot.size()) {
                log.warn("All IoTDB records were confirmed committed although session cleanup failed", e);
                return BatchWriteResult.success(snapshot.size(), progress.totalBatches);
            }
            BatchCommitStateEnum state;
            if (!progress.writeAttempted) {
                state = BatchCommitStateEnum.NOT_COMMITTED;
            } else {
                state = BatchCommitStateEnum.UNKNOWN;
            }
            TSDBErrorCodeEnum underlyingErrorCode = errorCodeForIoTDBException(e, TSDBErrorCodeEnum.WRITE_ERROR);
            BatchWriteResult result = new BatchWriteResult(snapshot.size(), snapshot.size(),
                    progress.committedRecords, progress.totalBatches, progress.committedBatches,
                    progress.failedBatchIndex(), progress.currentMeasurement, state,
                    state == BatchCommitStateEnum.UNKNOWN || underlyingErrorCode == TSDBErrorCodeEnum.CONNECTION_ERROR,
                    "Failed to batch write records to IoTDB table model: " + e.getMessage());
            TSDBErrorCodeEnum errorCode = state == BatchCommitStateEnum.UNKNOWN
                    ? TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN
                    : underlyingErrorCode;
            throw new TSDBBatchWriteException(errorCode, result, e);
        }
    }

    /**
     * Convert validation failures before session acquisition into batch exceptions with a confirmed zero-commit outcome.
     * @param requestedRecords requested record count
     * @param cause underlying failure or exception chain
     * @return exception containing a {@link BatchCommitStateEnum#NOT_COMMITTED} outcome
     */
    private TSDBBatchWriteException preflightFailure(int requestedRecords, RuntimeException cause) {
        BatchWriteResult result = new BatchWriteResult(requestedRecords, 0, 0,
                0, 0, null, null, BatchCommitStateEnum.NOT_COMMITTED, false,
                "IoTDB batch validation failed before database I/O: " + cause.getMessage());
        return new TSDBBatchWriteException(result, cause);
    }

    /**
     * Translate the shared query model to IoTDB SQL and enforce the configured result budget.
     * Only pagination probes may materialize one additional sentinel row.
     * @param database target database; blank uses the configured default when permitted; for example {@code "tsdb"}
     * @param query shared query model; for example {@code measurement=ACCRUE, limit=100}
     * @return query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public QueryResult query(String database,
                             TSDBQuery query) {
        TSDBQueryValidator.validate(query);
        return withReady(() -> {
            if (query == null || isBlank(query.getMeasurement())) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "measurement must not be empty");
            }
            long rowBudget = (long) maxQueryRows + (query.isPaginationProbe() ? 1 : 0);
            return executeQuery(database, buildQuerySql(query), rowBudget);
        });
    }

    /**
     * Count result rows before pagination. Detail queries use {@code COUNT(*)}; aggregate/grouped queries
     * wrap the aggregate SQL in an outer count, matching PageHelper's aggregate-query semantics.
     * @param database target database; blank uses the configured default when permitted
     * @param query shared query model
     * @return total result rows before pagination
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    @Override
    public long count(String database,
                      TSDBQuery query) {
        return withReady(() -> {
            if (query == null || isBlank(query.getMeasurement())) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "measurement must not be empty");
            }
            QueryResult result = executeQuery(database, buildCountSql(query));
            return extractCount(result);
        });
    }

    /**
     * Return the adapter's display name.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public String getAdapterName() {
        return "IoTDB-Table";
    }

    /**
     * Return the configured maximum number of records in one application write batch.
     * @return configured {@code tsdb.iotdb.max-batch-records} value
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-13
     */
    @Override
    public int getMaxBatchRecords() {
        return maxBatchRecords;
    }

    /**
     * Pre-scan records by measurement and collect each table's records and column types.
     * @param records shared records to write; for example {@code List.of(record1, record2)}
     * @return column contexts grouped by table name
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private Map<String, TableBatch> buildBatches(Collection<TSDBRecord> records) {
        Map<String, TableBatch> batches = new LinkedHashMap<>();
        for (TSDBRecord tsdbRecord : records) {
            if (tsdbRecord == null) {
                continue;
            }
            validateRecord(tsdbRecord);
            TableBatch batch = batches.computeIfAbsent(tsdbRecord.measurement(), TableBatch::new);
            batch.addRecord(tsdbRecord);
        }
        for (TableBatch batch : batches.values()) {
            batch.finishColumnTypes();
        }
        return batches;
    }

    /**
     * Validate that every inferred column value can be normalized to its TSFile type before any database write.
     * @param batches stable batches grouped by measurement
     */
    private void validatePreparedBatches(Map<String, TableBatch> batches) {
        for (TableBatch batch : batches.values()) {
            for (PreparedRecord record : batch.records) {
                for (TableColumn column : batch.columns.values()) {
                    Object value = getColumnValue(record, column);
                    if (value != null) {
                        normalizeValue(value, column.type);
                    }
                }
            }
        }
    }

    /**
     * Count physical Tablets after splitting all measurement batches.
     * @param batches stable batches grouped by measurement
     * @return total Tablets to write
     */
    private int countTablets(Map<String, TableBatch> batches) {
        int count = 0;
        for (TableBatch batch : batches.values()) {
            count += (batch.records.size() + tabletMaxRowSize - 1) / tabletMaxRowSize;
        }
        return count;
    }

    /**
     * Assemble records for one measurement into Tablets and write each full or final partial batch.
     * @param tableSession borrowed IoTDB table-model session
     * @param batch batch-write context for one table; for example {@code "ACCRUE"}, {@code TableBatch}
     * @param progress progress of this write operation
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private void writeBatch(ITableSession tableSession,
                            TableBatch batch,
                            WriteProgress progress)
            throws IoTDBConnectionException, StatementExecutionException {
        List<String> columnNames = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<ColumnCategory> categories = new ArrayList<>();
        for (TableColumn column : batch.columns.values()) {
            columnNames.add(column.name);
            dataTypes.add(column.type);
            categories.add(column.category);
        }

        Tablet tablet = new Tablet(batch.tableName, columnNames, dataTypes, categories, tabletMaxRowSize);
        int rowIndex = 0;
        for (PreparedRecord record : batch.records) {
            if (rowIndex >= tablet.getMaxRowNumber()) {
                insertTablet(tableSession, batch.tableName, tablet, rowIndex, progress);
                tablet.reset();
                rowIndex = 0;
            }
            tablet.addTimestamp(rowIndex, record.timestamp());
            for (TableColumn column : batch.columns.values()) {
                Object value = getColumnValue(record, column);
                if (value != null) {
                    tablet.addValue(column.name, rowIndex, normalizeValue(value, column.type));
                }
            }
            rowIndex++;
        }
        if (tablet.getRowSize() > 0) {
            insertTablet(tableSession, batch.tableName, tablet, rowIndex, progress);
        }
    }

    /**
     * Write one Tablet and record its confirmed commit progress.
     * @param tableSession borrowed IoTDB table-model session
     * @param tableName table name; for example {@code "ACCRUE"}
     * @param tablet assembled table-model data batch; for example {@code Tablet}
     * @param rowCount number of valid rows in the current Tablet
     * @param progress progress of this write operation
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private void insertTablet(ITableSession tableSession,
                              String tableName,
                              Tablet tablet,
                              int rowCount,
                              WriteProgress progress)
            throws IoTDBConnectionException, StatementExecutionException {
        log.debug("Writing IoTDB tablet, database={}, table={}, rows={}, maxRows={}",
                config.getDatabase(), tableName, tablet.getRowSize(), tablet.getMaxRowNumber());
        progress.currentMeasurement = tableName;
        progress.writeAttempted = true;
        tableSession.insert(tablet);
        progress.committedBatches++;
        progress.committedRecords += rowCount;
    }

    /**
     * Read the configured Tablet row limit and reject invalid batch sizes before entering the write path.
     * @param config IoTDB connection settings; for example {@code config.getTable().getTabletMaxRowSize() == 1024}
     * @return maximum rows per Tablet
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private int resolveTabletMaxRowSize(IoTDBProperties config) {
        if (config == null
                || config.getTable() == null
                || config.getTable().getTabletMaxRowSize() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.iotdb.table.tablet-max-row-size must be greater than 0");
        }
        return config.getTable().getTabletMaxRowSize();
    }

    /**
     * Read and validate the maximum number of records in one application write batch.
     * @param config IoTDB connection settings; for example {@code maxBatchRecords=10000}
     * @return maximum records per application batch, for example {@code 10000}
     * @throws TSDBException if the configuration or arguments violate the requirements described above
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private int resolveMaxBatchRecords(IoTDBProperties config) {
        if (config == null || config.getMaxBatchRecords() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.iotdb.max-batch-records must be greater than 0");
        }
        return config.getMaxBatchRecords();
    }

    /**
     * Validates the maximum number of result rows materialized by a query.
     *
     * @param config IoTDB connection settings
     * @return the configured positive row limit, reserving capacity for one pagination sentinel
     */
    private int resolveMaxQueryRows(IoTDBProperties config) {
        if (config == null || config.getMaxQueryRows() <= 0 || config.getMaxQueryRows() == Integer.MAX_VALUE) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.iotdb.max-query-rows must be between 1 and 2147483646");
        }
        return config.getMaxQueryRows();
    }

    /**
     * Validate the application batch size before constructing any Tablet so an oversized batch cannot be partially written.
     * @param records shared records to write; for example {@code List.of(record1, record2)}
     * @throws TSDBException if the configuration or arguments violate the requirements described above
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private void validateBatchSize(Collection<TSDBRecord> records) {
        if (records.size() > maxBatchRecords) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "IoTDB batch write record count exceeds tsdb.iotdb.max-batch-records: "
                    + records.size() + " > " + maxBatchRecords);
        }
    }

    /**
     * Validate the pool and default database. Single-session execution is no longer supported.
     * @throws TSDBException if the configuration or arguments violate the requirements described above
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private void validatePoolConfiguration() {
        if (pool == null || !pool.isEnabled()) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "IoTDB single Session mode is disabled; tsdb.iotdb.pool.enabled must be true");
        }
        /*
         * The default database is the pool's stable baseline: create it before deployment and retain it during operation.
         * IoTDB 2.0.10 restores it when closing a cross-database session before returning that physical session to the pool.
         */
        if (isBlank(config.getDatabase())) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Required TSDB configuration is empty: tsdb.iotdb.database");
        }
    }

    /**
     * Execute a query and convert its IoTDB SessionDataSet into the shared QueryResult.
     * @param database target database; blank uses the configured default when permitted; for example {@code "tsdb"}
     * @param sql IoTDB table-model SQL statement; for example {@code "SELECT time, value FROM ACCRUE LIMIT 10"}
     * @return query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private QueryResult executeQuery(String database,
                                     String sql) {
        return executeQuery(database, sql, maxQueryRows);
    }

    /** Executes a structured query with a bounded result budget. */
    private QueryResult executeQuery(String database, String sql, long rowBudget) {
        QueryResult result = new QueryResult();
        try {
            executeReadWithSession(database, true, tableSession -> {
                logQuerySql(sql);
                SessionDataSet dataSet = tableSession.executeQueryStatement(sql);
                fillAndCloseQueryResult(result, dataSet, rowBudget);
            });
        } catch (TSDBException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to query data from IoTDB table model, sql: {}", sql, e);
            throw queryException(e, sql);
        }
        return result;
    }

    /**
     * Execute application-supplied native SQL.
     * <p>Neither issues USE nor rewrites the SQL. The borrowed session uses the pool's default database;
     * the SQL may explicitly reference {@code database.table}.</p>
     * @param sql IoTDB table-model SQL statement; for example {@code "SELECT * FROM tsdb.ACCRUE LIMIT 10"}
     * @return query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private QueryResult executeRawQuery(String sql) {
        QueryResult result = new QueryResult();
        try {
            executeReadWithSession(null, false, tableSession -> {
                logQuerySql(sql);
                SessionDataSet dataSet = tableSession.executeQueryStatement(sql);
                fillAndCloseQueryResult(result, dataSet, maxQueryRows);
            });
        } catch (TSDBException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to query data from IoTDB table model, sql: {}", sql, e);
            throw queryException(e, sql);
        }
        return result;
    }

    /**
     * Log the actual IoTDB query SQL when enabled by the shared settings.
     * @param sql IoTDB table-model SQL statement
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-22
     */
    private void logQuerySql(String sql) {
        if (queryLogEnabled) {
            log.debug("Executing IoTDB query SQL: {}", sql);
        }
    }

    /**
     * Convert an IoTDB query failure into an adapter exception with a stable error code.
     * @param cause underlying failure or exception chain
     * @param sql IoTDB table-model SQL statement
     * @return shared exception exposed to callers
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private TSDBException queryException(Throwable cause,
                                         String sql) {
        TSDBErrorCodeEnum errorCode = errorCodeForIoTDBException(cause, TSDBErrorCodeEnum.QUERY_ERROR);
        return new TSDBException(errorCode,
                "Failed to query data from IoTDB table model, sql: " + sql + ": " + cause.getMessage(), cause);
    }

    /**
     * Classify failures using IoTDB status codes first, then inspect the exception chain and messages when the client exposes no status code.
     * @param cause underlying failure or exception chain
     * @param fallback default error category for this operation
     * @return TSDB error code exposed to callers
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private TSDBErrorCodeEnum errorCodeForIoTDBException(Throwable cause,
                                                         TSDBErrorCodeEnum fallback) {
        Throwable current = cause;
        while (current != null) {
            if (current instanceof TSDBException tsdbException) {
                return tsdbException.getErrorCode();
            }
            if (current instanceof StatementExecutionException statementException) {
                TSDBErrorCodeEnum statusError = errorCodeForIoTDBStatus(statementException.getStatusCode());
                if (statusError != null) {
                    return statusError;
                }
            }
            current = current.getCause();
        }

        String message = rootMessage(cause).toLowerCase(java.util.Locale.ROOT);
        if (containsAny(message, "authentication", "wrong password", "permission denied",
                "no permission", "not have permission", "access denied", "not login")) {
            return TSDBErrorCodeEnum.PERMISSION_ERROR;
        }
        if (containsAny(message, "does not exist", "not exist", "not found", "no such")) {
            return TSDBErrorCodeEnum.RESOURCE_NOT_FOUND;
        }
        if (cause instanceof IoTDBConnectionException
                || containsAny(message, "failed to connect", "fail to reconnect", "connection refused",
                "connection reset", "broken pipe", "timed out", "timeout")) {
            return TSDBErrorCodeEnum.CONNECTION_ERROR;
        }
        return fallback == null ? TSDBErrorCodeEnum.INTERNAL_ERROR : fallback;
    }

    /**
     * Map an IoTDB server status to a shared error category. Return {@code null} when no explicit mapping exists,
     * allowing the caller to inspect exception types and messages instead.
     * @param statusCode IoTDB server status code
     * @return mapped TSDB error code, or {@code null} when no explicit mapping exists
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private TSDBErrorCodeEnum errorCodeForIoTDBStatus(int statusCode) {
        TSStatusCode status = TSStatusCode.representOf(statusCode);
        if (status == null) {
            return null;
        }
        return switch (status) {
            case INIT_AUTH_ERROR, WRONG_LOGIN_PASSWORD, NOT_LOGIN, NO_PERMISSION, USER_NOT_EXIST,
                 NOT_HAS_PRIVILEGE, UNKNOWN_AUTH_PRIVILEGE, AUTH_IO_EXCEPTION, ILLEGAL_PRIVILEGE,
                 NOT_HAS_PRIVILEGE_GRANTOPT, AUTH_OPERATE_EXCEPTION, ILLEGAL_PASSWORD, USER_LOGIN_LOCKED ->
                    TSDBErrorCodeEnum.PERMISSION_ERROR;
            case DATABASE_NOT_EXIST, TABLE_NOT_EXISTS, COLUMN_NOT_EXISTS, PATH_NOT_EXIST,
                 OBJECT_NOT_EXISTS, TYPE_NOT_FOUND, SEGMENT_NOT_FOUND, TABLE_IS_LOST ->
                    TSDBErrorCodeEnum.RESOURCE_NOT_FOUND;
            case UNSUPPORTED_OPERATION, UNSUPPORTED_SQL_DIALECT, UNSUPPORTED_FILL_TYPE ->
                    TSDBErrorCodeEnum.UNSUPPORTED_OPERATION;
            case CONFIGURATION_ERROR, DATABASE_CONFIG_ERROR, TTL_CONFIG_ERROR -> TSDBErrorCodeEnum.CONFIGURATION_ERROR;
            case ILLEGAL_PARAMETER, ILLEGAL_PATH, OVERSIZE_RECORD, NUMERIC_VALUE_OUT_OF_RANGE,
                 DATE_OUT_OF_RANGE -> TSDBErrorCodeEnum.ARGUMENT_ERROR;
            case INTERNAL_REQUEST_TIME_OUT, INTERNAL_REQUEST_RETRY_ERROR, PLAN_FAILED_NETWORK_PARTITION,
                 CAN_NOT_CONNECT_DATANODE, CAN_NOT_CONNECT_CONFIGNODE, CAN_NOT_CONNECT_AINODE,
                 NO_AVAILABLE_REPLICA, STORAGE_ENGINE_NOT_READY -> TSDBErrorCodeEnum.CONNECTION_ERROR;
            default -> null;
        };
    }

    /**
     * Find the deepest exception message for compatibility with client exceptions that expose no IoTDB status code.
     * @param cause underlying failure or exception chain
     * @return root failure message, or an empty string when unavailable
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private String rootMessage(Throwable cause) {
        Throwable current = cause;
        while (current != null && current.getCause() != null) {
            current = current.getCause();
        }
        return current == null || current.getMessage() == null ? "" : current.getMessage();
    }

    /**
     * Check whether text contains any candidate fragment to recognize messages across IoTDB client versions.
     * @param value source value
     * @param candidates candidate text fragments
     * @return {@code true} if any candidate is present; otherwise {@code false}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private boolean containsAny(String value,
                                String... candidates) {
        for (String candidate : candidates) {
            if (value.contains(candidate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Read IoTDB result rows within the configured budget, converting Binary text to Java strings.
     * Reject excess rows before materializing them, and publish no partial result.
     * @param result shared query result; for example {@code new QueryResult()}
     * @param dataSet IoTDB query result set; for example {@code tableSession.executeQueryStatement(sql)}
     * @param rowBudget maximum materialized rows, including any pagination sentinel
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private void fillQueryResult(QueryResult result,
                                 SessionDataSet dataSet, long rowBudget)
            throws IoTDBConnectionException, StatementExecutionException {
        List<String> columnNames = dataSet.getColumnNames();
        result.setColumns(columnNames);

        List<Map<String, Object>> rows = new ArrayList<>();
        while (dataSet.hasNext()) {
            if (rows.size() >= rowBudget) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                        "IoTDB query result exceeds tsdb.iotdb.max-query-rows: " + maxQueryRows
                                + "; use a narrower time range or pagination");
            }
            Map<String, Object> row = new LinkedHashMap<>();
            org.apache.tsfile.read.common.RowRecord rowRecord = dataSet.next();
            // Match field positions to the SDK's column order and preserve that order in the map.
            int fieldIndex = 0;
            for (String columnName : columnNames) {
                org.apache.tsfile.read.common.Field field = fieldIndex < rowRecord.getFields().size()
                        ? rowRecord.getFields().get(fieldIndex)
                        : null;
                // Preserve both missing fields and SDK null values as null without omitting their columns.
                Object value = field == null || field.getDataType() == null
                        ? null
                        : field.getObjectValue(field.getDataType());
                if (value instanceof Binary) {
                    // Copy raw BLOB bytes; decode textual Binary values as UTF-8.
                    value = field.getDataType() == TSDataType.BLOB
                            ? ((Binary) value).getValues().clone()
                            : ((Binary) value).getStringValue(StandardCharsets.UTF_8);
                }
                row.put(columnName, value);
                fieldIndex++;
            }
            rows.add(row);
        }

        // Publish the result only after all rows have been read successfully.
        result.setRows(rows);
        result.setRowCount(rows.size());
        result.setSuccess(true);
        result.setMessage("Query executed successfully");
    }

    /**
     * Read the result and close the server operation handle. If both fail, preserve the read failure
     * and suppress the cleanup failure so it cannot mask the original query error.
     * @param result shared query result
     * @param dataSet IoTDB query result set
     * @param rowBudget maximum materialized rows, including any pagination sentinel
     * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
     * @throws StatementExecutionException if the server fails to execute the operation or release its result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private void fillAndCloseQueryResult(QueryResult result,
                                         SessionDataSet dataSet, long rowBudget)
            throws IoTDBConnectionException, StatementExecutionException {
        executeAndClose(() -> fillQueryResult(result, dataSet, rowBudget), dataSet::closeOperationHandle);
    }

    /**
     * Read and close using try-with-resources exception semantics.
     * <p>Always attempt cleanup after a read failure, suppressing any cleanup failure onto it.
     * If reading succeeds but cleanup fails, propagate the cleanup failure.</p>
     * @param readAction result-reading action
     * @param closeAction result-cleanup action; for example {@code dataSet::closeOperationHandle}
     * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
     * @throws StatementExecutionException if the server fails to execute the operation or release its result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private static void executeAndClose(QueryResultAction readAction,
                                        QueryResultAction closeAction)
            throws IoTDBConnectionException, StatementExecutionException {
        Throwable primaryFailure = null;
        try {
            readAction.execute();
        } catch (IoTDBConnectionException | StatementExecutionException | RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                closeAction.execute();
            } catch (IoTDBConnectionException | StatementExecutionException | RuntimeException | Error closeFailure) {
                if (primaryFailure != null) {
                    if (primaryFailure != closeFailure) {
                        primaryFailure.addSuppressed(closeFailure);
                    }
                } else {
                    throw closeFailure;
                }
            }
        }
    }

    /**
     * Execute through the pool, issuing USE only when the requested database differs from the pool default.
     * @param database target database; blank uses the configured default when permitted; for example {@code "tsdb"}
     * @param action session operation to execute; for example {@code session -> session.insert(tablet)}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private void executeWithSession(String database,
                                    TableSessionAction action)
            throws IoTDBConnectionException, StatementExecutionException {
        executeWithSession(database, true, action);
    }

    /**
     * Execute through the pool with configurable fallback to the default database.
     * @param database target database; blank uses the configured default when permitted; for example {@code "tsdb"}
     * @param useConfiguredDatabase whether fallback to the configured default database is allowed; for example {@code true}
     * @param action session operation to execute; for example {@code session -> session.executeQueryStatement(sql)}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private void executeWithSession(String database,
                                    boolean useConfiguredDatabase,
                                    TableSessionAction action)
            throws IoTDBConnectionException, StatementExecutionException {
        if (sessionPool == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                    "IoTDB table SessionPool is not initialized");
        }
        try (ITableSession tableSession = sessionPool.getSession()) {
            useDatabaseIfNecessary(tableSession, database, useConfiguredDatabase);
            action.execute(tableSession);
        }
    }

    /**
     * Execute a safely replayable read. IoTDB 2.0.10 may return a null response after exhausting query RPC retries,
     * then throw a null-pointer exception from {@code execResp == null} and return an invalid session to the pool.
     * For that known defect or a definite connection failure, replace the failed pool version and retry once.
     * Propagate acquisition failures directly: both normal pool saturation and lost SDK slots can time out;
     * replacing the pool on acquisition timeout would bypass maxSize under normal load.
     * @param database target database; blank uses the configured default when permitted
     * @param useConfiguredDatabase whether fallback to the configured default database is allowed
     * @param action session operation to execute
     * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
     * @throws StatementExecutionException if the server fails to execute the operation or release its result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    private void executeReadWithSession(String database,
                                        boolean useConfiguredDatabase,
                                        TableSessionAction action)
            throws IoTDBConnectionException, StatementExecutionException {
        RecoverableTableSessionPool managedPool = recoverableSessionPool;
        if (managedPool == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                    "IoTDB table SessionPool is not initialized");
        }
        // Propagate acquisition timeouts; normal pool saturation does not justify replacing the pool.
        RecoverableTableSessionPool.BorrowedTableSession borrowedSession = managedPool.borrowSession();
        try (ITableSession tableSession = borrowedSession.session()) {
            useDatabaseIfNecessary(tableSession, database, useConfiguredDatabase);
            action.execute(tableSession);
            // Return immediately after success to avoid executing the retry below.
            return;
        } catch (IoTDBConnectionException | RuntimeException failure) {
            // Recover only connection failures and the known SDK defect; propagate other exceptions unchanged.
            if (!isRecoverableReadConnectionFailure(failure)) {
                throw failure;
            }
            // Condition replacement on the actual borrowed version so old requests cannot replace a recovered pool.
            boolean replaced = managedPool.replaceIfCurrent(
                    borrowedSession.version(), this::buildPhysicalSessionPool);
            log.warn("IoTDB read connection failed; retrying once with {} SessionPool version, cause={}",
                    replaced ? "a new" : "the current",
                    failure.toString());
        }
        // Retry a read at most once; propagate a second failure without recursively replacing the pool.
        executeWithSession(database, useConfiguredDatabase, action);
    }

    /**
     * Recognize only query failures that can safely recover through connection replacement, excluding SQL, permission, and application errors.
     * @param failure query failure
     * @return whether the pool should be replaced before retrying
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    private boolean isRecoverableReadConnectionFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof IoTDBConnectionException) {
                return true;
            }
            String message = current.getMessage();
            if (current instanceof NullPointerException
                    && message != null
                    && message.contains("TSExecuteStatementResp.getStatus()")
                    && message.contains("execResp")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Switch databases only when the target differs from the pool default.
     * <p>Using the default avoids an additional USE RPC. IoTDB 2.0.10's {@code TableSessionWrapper.close()}
     * restores the default after cross-database operations. That database must remain available because
     * this client version does not reliably replenish pool slots if restoration during close fails.</p>
     * @param tableSession borrowed IoTDB table-model session; for example {@code sessionPool.getSession()}
     * @param database target database; blank uses the configured default when permitted; for example {@code "tsdb"}
     * @param useConfiguredDatabase whether fallback to the configured default database is allowed; for example {@code true}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private void useDatabaseIfNecessary(ITableSession tableSession,
                                        String database,
                                        boolean useConfiguredDatabase)
            throws IoTDBConnectionException, StatementExecutionException {
        String resolvedDatabase = resolveDatabase(database, useConfiguredDatabase);
        if (!isBlank(resolvedDatabase) && !resolvedDatabase.equals(poolDatabase)) {
            tableSession.executeNonQueryStatement("USE " + resolvedDatabase);
        }
    }

    /**
     * Resolve the database for this operation. Native queries disable default fallback and do not issue USE,
     * although their sessions still retain the pool's default database context.
     * @param database target database; blank uses the configured default when permitted; for example {@code "tsdb"}
     * @param useConfiguredDatabase whether fallback to the configured default database is allowed; for example {@code true}
     * @return validated database name, or null when none applies
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private String resolveDatabase(String database,
                                   boolean useConfiguredDatabase) {
        if (!isBlank(database)) {
            return normalizeIdentifier(database);
        }
        if (useConfiguredDatabase && !isBlank(poolDatabase)) {
            return poolDatabase;
        }
        return null;
    }

    /**
     * Trim the configured default database and validate it before binding it to the pool.
     * @return normalized pool default database, for example {@code tsdb}
     */
    private String normalizeConfiguredDatabase() {
        return normalizeIdentifier(config.getDatabase().trim());
    }

    /**
     * Translate the shared query model into IoTDB table-model SQL.
     * @param query shared query model; for example {@code measurement=ACCRUE, limit=100}
     * @return IoTDB table-model SQL
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private String buildQuerySql(TSDBQuery query) {
        return buildQuerySql(query, true);
    }

    /**
     * Translate the shared query model into IoTDB SQL, optionally omitting irrelevant sorting and pagination for count subqueries.
     * @param query shared query model
     * @param includeOrderAndPage whether to include ORDER BY, LIMIT, and OFFSET
     * @return IoTDB table-model SQL
     */
    private String buildQuerySql(TSDBQuery query,
                                 boolean includeOrderAndPage) {
        List<String> selections = buildSelections(query);
        List<String> filters = buildFilters(query);
        List<String> groupExpressions = buildGroupExpressions(query);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(String.join(", ", selections));
        sql.append(" FROM ").append(normalizeIdentifier(query.getMeasurement()));
        if (!filters.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", filters));
        }
        if (!groupExpressions.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupExpressions));
        }
        if (includeOrderAndPage) {
            appendOrder(query, sql);
            if (query.getLimit() != null && query.getLimit() > 0) {
                sql.append(" LIMIT ").append(query.getLimit());
            }
            if (query.getOffset() != null && query.getOffset() > 0) {
                sql.append(" OFFSET ").append(query.getOffset());
            }
        }
        return sql.toString();
    }

    /**
     * Build the count SQL for pagination totals.
     * @param query shared query model
     * @return direct detail count SQL or an outer count over aggregate results
     */
    private String buildCountSql(TSDBQuery query) {
        TSDBQuery countQuery = query.copy();
        countQuery.setPaginationProbe(false);
        countQuery.setCursorTime(null);
        countQuery.setStrictCursor(false);
        countQuery.setCursorValues(Collections.emptyMap());
        countQuery.setLimit(null);
        countQuery.setOffset(null);
        TSDBQueryValidator.validate(countQuery);

        if (countQuery.hasAggregations()) {
            return "SELECT COUNT(*) AS total FROM ("
                    + buildQuerySql(countQuery, false) + ") AS tsdb_count";
        }

        StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total FROM ")
                .append(normalizeIdentifier(countQuery.getMeasurement()));
        List<String> filters = buildFilters(countQuery);
        if (!filters.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", filters));
        }
        return sql.toString();
    }

    /**
     * Extract the long count value from the single-row count result.
     * @param result shared query result
     * @return total row count
     */
    private static long extractCount(QueryResult result) {
        if (result == null || result.getRows() == null || result.getRows().isEmpty()) {
            return 0L;
        }
        Map<String, Object> row = result.getRows().get(0);
        Object value = row.entrySet().stream()
                .filter(entry -> "total".equalsIgnoreCase(entry.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseGet(() -> row.values().stream().findFirst().orElse(null));
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (RuntimeException e) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                    "IoTDB count query returned a non-numeric value: " + value, e);
        }
    }

    /**
     * Build the SELECT expressions, including the time window, grouping tags, and aggregates when required.
     * @param query shared query model; for example {@code selectColumns=[value, status]}
     * @return SELECT expressions
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private List<String> buildSelections(TSDBQuery query) {
        List<String> selections = new ArrayList<>();
        if (query.hasAggregations()) {
            if (!isBlank(query.getGroupByTime())) {
                selections.add(buildTimeWindowExpression(query)
                        + " AS window_start");
            }
            for (String tag : query.getGroupByTags()) {
                selections.add(normalizeIdentifier(tag));
            }
            for (AggregationSpec aggregation : query.getAggregations()) {
                selections.add(toAggregationSql(aggregation.function(), aggregation.field())
                        + " AS " + normalizeIdentifier(aggregation.alias()));
            }
            TSDBQueryValidator.validateAggregationOutputNames(query, this::normalizeColumnIdentifier);
            return selections;
        }
        if (query.getSelectColumns() == null || query.getSelectColumns().isEmpty()) {
            selections.add("*");
            return selections;
        }
        if (query.getSelectColumns().stream().noneMatch(query.getTimeColumn()::equalsIgnoreCase)) {
            selections.add(normalizeIdentifier(query.getTimeColumn()));
        }
        for (String column : query.getSelectColumns()) {
            selections.add(normalizeIdentifier(column));
        }
        return selections;
    }

    /**
     * Build WHERE predicates for the time range, pagination cursor, and application filters.
     * @param query shared query model; for example {@code startTime=1783000000000L, filters=[device_code=D001]}
     * @return WHERE predicate fragments
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private List<String> buildFilters(TSDBQuery query) {
        List<String> filters = new ArrayList<>();
        if (query.getStartTime() != null) {
            filters.add(normalizeIdentifier(query.getTimeColumn()) + " >= " + query.getStartTime());
        }
        if (query.getEndTime() != null) {
            filters.add(normalizeIdentifier(query.getTimeColumn()) + " <= " + query.getEndTime());
        }
        boolean hasStrictCursorValues = query.isStrictCursor()
                && query.getCursorValues() != null
                && !query.getCursorValues().isEmpty();
        if (hasStrictCursorValues) {
            filters.add(buildStrictCursorFilter(query));
        }
        if (!hasStrictCursorValues && query.getCursorTime() != null) {
            boolean desc = SortOrderEnum.normalize(query.getOrder()) == SortOrderEnum.DESC;
            filters.add(normalizeIdentifier(query.getTimeColumn()) + " " + (desc ? "<" : ">")
                    + " " + query.getCursorTime());
        }
        for (QueryFilter filter : query.getFilters()) {
            filters.add(buildFilterSql(filter));
        }
        return filters;
    }

    /**
     * Build a compound cursor predicate lexicographically in ORDER BY column order.
     * <p>Ascending example: {@code (time > 1783000000000) OR (time = 1783000000000 AND device_code > 'D001')}.</p>
     * @param query shared query model; for example {@code cursorColumns=[time, device_code]}
     * @return compound-cursor WHERE predicate
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private String buildStrictCursorFilter(TSDBQuery query) {
        List<SortSpec> cursorSortSpecs = resolveCursorSortSpecs(query);
        Map<String, Object> cursorValues = normalizeCursorValues(query.getCursorValues());
        if (cursorValues.size() != cursorSortSpecs.size()
                || cursorSortSpecs.stream().anyMatch(sort -> !cursorValues.containsKey(sort.column()))) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Strict cursor keys must exactly match the IoTDB cursor columns");
        }
        List<String> groups = new ArrayList<>();
        for (int i = 0; i < cursorSortSpecs.size(); i++) {
            List<String> parts = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                String equalsColumn = cursorSortSpecs.get(j).column();
                Object equalsValue = requireCursorValue(cursorValues, equalsColumn);
                parts.add(normalizeIdentifier(equalsColumn) + " = "
                        + formatCursorSqlValue(equalsColumn, equalsValue));
            }
            SortSpec compareSort = cursorSortSpecs.get(i);
            String compareColumn = compareSort.column();
            Object compareValue = requireCursorValue(cursorValues, compareColumn);
            String operator = compareSort.order() == SortOrderEnum.DESC ? "<" : ">";
            parts.add(normalizeIdentifier(compareColumn) + " " + operator + " "
                    + formatCursorSqlValue(compareColumn, compareValue));
            groups.add("(" + String.join(" AND ", parts) + ")");
        }
        return "(" + String.join(" OR ", groups) + ")";
    }

    /**
     * Build GROUP BY expressions. IoTDB requires full expressions rather than SELECT aliases.
     * @param query shared query model; for example {@code groupByTime=5m, groupByTags=[device_code]}
     * @return GROUP BY expressions
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private List<String> buildGroupExpressions(TSDBQuery query) {
        List<String> groupExpressions = new ArrayList<>();
        if (query.hasAggregations() && !isBlank(query.getGroupByTime())) {
            groupExpressions.add(buildTimeWindowExpression(query));
        }
        if (query.hasAggregations()) {
            for (String tag : query.getGroupByTags()) {
                groupExpressions.add(normalizeIdentifier(tag));
            }
        }
        return groupExpressions;
    }

    /**
     * Append ORDER BY for window_start in window aggregates, or time/compound-cursor columns in detail queries.
     * @param query shared query model; for example {@code order=DESC}
     * @param sql IoTDB table-model SQL statement; for example {@code new StringBuilder("SELECT * FROM ACCRUE")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private void appendOrder(TSDBQuery query,
                             StringBuilder sql) {
        if (query.hasAggregations()) {
            List<String> aggregateOrder = new ArrayList<>();
            String order = SortOrderEnum.normalize(query.getOrder()).name();
            if (!isBlank(query.getGroupByTime())) {
                aggregateOrder.add("window_start " + order);
            }
            for (String tag : query.getGroupByTags()) {
                aggregateOrder.add(normalizeIdentifier(tag) + " " + order);
            }
            if (!aggregateOrder.isEmpty()) {
                sql.append(" ORDER BY ").append(String.join(", ", aggregateOrder));
            }
            return;
        }
        if (!query.hasAggregations()) {
            SortOrderEnum order = SortOrderEnum.normalize(query.getOrder());
            if (query.isStrictCursor()) {
                List<String> orderExpressions = new ArrayList<>();
                for (SortSpec sortSpec : resolveCursorSortSpecs(query)) {
                    orderExpressions.add(normalizeIdentifier(sortSpec.column()) + " " + sortSpec.order().name());
                }
                sql.append(" ORDER BY ").append(String.join(", ", orderExpressions));
                return;
            }
            if (query.getSortSpecs() != null && !query.getSortSpecs().isEmpty()) {
                List<String> orderExpressions = new ArrayList<>();
                for (SortSpec sortSpec : query.getSortSpecs()) {
                    orderExpressions.add(normalizeIdentifier(sortSpec.column()) + " " + sortSpec.order().name());
                }
                sql.append(" ORDER BY ").append(String.join(", ", orderExpressions));
                return;
            }
            sql.append(" ORDER BY ").append(normalizeIdentifier(query.getTimeColumn()))
                    .append(' ').append(order.name());
        }
    }

    /**
     * Resolve compound cursor columns, falling back to cursorValues order when direct adapter calls have no template-normalized columns.
     * @param query shared query model; for example {@code cursorColumns=[time, device_code]}
     * @return compound cursor columns
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private List<String> resolveCursorColumns(TSDBQuery query) {
        List<String> columns = new ArrayList<>();
        if (query.getCursorColumns() != null && !query.getCursorColumns().isEmpty()) {
            for (String column : query.getCursorColumns()) {
                addColumnIfAbsent(columns, column);
            }
            return columns;
        }
        addColumnIfAbsent(columns, query.getTimeColumn());
        if (query.getCursorValues() != null) {
            for (String column : query.getCursorValues().keySet()) {
                addColumnIfAbsent(columns, column);
            }
        }
        return columns;
    }

    /**
     * Resolve per-column ordering for strict cursors. The template normalizes cursor and sort columns into the same order.
     * Direct adapter calls inherit the main direction for unspecified cursor columns and reject sort columns outside the cursor.
     * @param query shared query model
     * @return sort specifications in cursor-column order
     * @throws TSDBException if the configuration or arguments violate the requirements described above
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private List<SortSpec> resolveCursorSortSpecs(TSDBQuery query) {
        List<String> cursorColumns = resolveCursorColumns(query);
        List<SortSpec> configuredSorts = query.getSortSpecs() == null
                ? Collections.emptyList() : query.getSortSpecs();
        List<String> seenSortColumns = new ArrayList<>();
        for (SortSpec configuredSort : configuredSorts) {
            String normalized = normalizeColumnIdentifier(configuredSort.column());
            if (seenSortColumns.contains(normalized)) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Duplicate IoTDB strict cursor sort column: " + configuredSort.column());
            }
            seenSortColumns.add(normalized);
            if (!containsColumn(cursorColumns, configuredSort.column())) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Strict cursor sort column is not part of cursorColumns: " + configuredSort.column());
            }
        }
        List<SortSpec> resolved = new ArrayList<>();
        SortOrderEnum fallbackOrder = SortOrderEnum.normalize(query.getOrder());
        for (String cursorColumn : cursorColumns) {
            SortSpec configured = findSortSpec(configuredSorts, cursorColumn);
            resolved.add(configured == null
                    ? new SortSpec(cursorColumn, fallbackOrder) : new SortSpec(cursorColumn, configured.order()));
        }
        return resolved;
    }

    /**
     * Find a column's sort specification without regard to case.
     * @param sortSpecs ordered sort specifications
     * @param column column name; for example {@code device_code}
     * @return matching sort specification, or {@code null} when absent
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private static SortSpec findSortSpec(List<SortSpec> sortSpecs,
                                         String column) {
        for (SortSpec sortSpec : sortSpecs) {
            if (canonicalColumn(sortSpec.column()).equals(canonicalColumn(column))) {
                return sortSpec;
            }
        }
        return null;
    }

    /**
     * Check whether the cursor column collection contains the given sort column without regard to case.
     * @param columns target column collection; for example {@code [time, device_code]}
     * @param candidate column name to look up
     * @return {@code true} if a case-insensitive match exists
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private static boolean containsColumn(List<String> columns,
                                          String candidate) {
        for (String column : columns) {
            if (canonicalColumn(column).equals(canonicalColumn(candidate))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Read a compound cursor value using IoTDB's unquoted identifier normalization.
     * @param cursorValues cursor values with normalized column names
     * @param column required physical column
     * @return non-null cursor value
     */
    private Object requireCursorValue(Map<String, Object> cursorValues, String column) {
        Object value = cursorValues.get(normalizeColumnIdentifier(column));
        if (value == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Missing strict cursor value: " + column);
        }
        return value;
    }

    private Map<String, Object> normalizeCursorValues(Map<String, Object> cursorValues) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : cursorValues.entrySet()) {
            String column = normalizeColumnIdentifier(entry.getKey());
            if (isBlank(column) || normalized.containsKey(column)) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Invalid or duplicate IoTDB strict cursor column: " + entry.getKey());
            }
            normalized.put(column, entry.getValue());
        }
        return normalized;
    }

    /**
     * Normalize unquoted table-model column names to IoTDB's lowercase identity.
     * The adapter accepts simple unquoted identifiers; timestamp-like field names are not time aliases.
     * @param column physical column name
     * @return trimmed lowercase name, or null
     */
    @Override
    public String normalizeColumnIdentifier(String column) {
        return canonicalColumn(column);
    }

    private static String canonicalColumn(String column) {
        return column == null ? null : column.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * Convert a compound cursor value into an IoTDB SQL literal.
     * @param column column name; for example {@code "time"}
     * @param value source value; for example {@code 1783000000000L}
     * @return SQL literal
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private String formatCursorSqlValue(String column, Object value) {
        if (isTimeColumn(column)) {
            try {
                long millis;
                if (value instanceof BigInteger integer) {
                    millis = integer.longValueExact();
                } else if (value instanceof BigDecimal decimal) {
                    millis = decimal.longValueExact();
                } else if (value instanceof Byte || value instanceof Short
                        || value instanceof Integer || value instanceof Long) {
                    millis = ((Number) value).longValue();
                } else if (value instanceof Float || value instanceof Double) {
                    // Use the exact represented binary value, not a rounded decimal rendering.
                    millis = new BigDecimal(((Number) value).doubleValue()).longValueExact();
                } else if (value instanceof Number number) {
                    millis = new BigDecimal(number.toString()).longValueExact();
                } else if (value instanceof Instant instant) {
                    millis = instant.toEpochMilli();
                } else {
                    millis = Long.parseLong(String.valueOf(value));
                }
                return Long.toString(millis);
            } catch (ArithmeticException | NumberFormatException failure) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Invalid strict cursor time value: " + value, failure);
            }
        }
        return formatSqlValue(value);
    }

    /**
     * Check whether a column is a time column.
     * @param column column name; for example {@code "time"}
     * @return whether the column is a time column
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private boolean isTimeColumn(String column) {
        return column != null && "time".equalsIgnoreCase(column);
    }

    /**
     * Add a column only if no case-insensitive match already exists, avoiding duplicate compound-cursor sort columns.
     * @param columns target column collection; for example {@code List.of("time")}
     * @param column column name; for example {@code "device_code"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static void addColumnIfAbsent(List<String> columns, String column) {
        if (isBlank(column)) {
            return;
        }
        for (String existing : columns) {
            if (existing != null && canonicalColumn(existing).equals(canonicalColumn(column))) {
                return;
            }
        }
        columns.add(canonicalColumn(column));
    }

    /**
     * Builds a fixed-duration date_bin expression or explicit calendar-day boundaries.
     * Region-based day buckets use actual local midnights, including daylight-saving transitions.
     *
     * @param query query containing the window, time zone, and bounded time range
     * @return a timestamp-valued window, exposed by the SDK as epoch milliseconds
     */
    private String buildTimeWindowExpression(TSDBQuery query) {
        String timeColumn = normalizeIdentifier(query.getTimeColumn());
        TimeWindowPlan plan = TimeWindowPlan.resolve(query.getGroupByTime(), query.getTimeZone(),
                query.getStartTime(), query.getEndTime());
        if (plan.isCalendar()) {
            StringBuilder expression = new StringBuilder("CASE");
            for (TimeWindowPlan.Bucket bucket : plan.buckets()) {
                expression.append(" WHEN ").append(timeColumn).append(" >= ").append(bucket.startInclusive())
                        .append(" AND ").append(timeColumn).append(" < ").append(bucket.endExclusive())
                        .append(" THEN CAST(").append(bucket.startInclusive()).append(" AS TIMESTAMP)");
            }
            // TIMESTAMP preserves one CASE type across constant folding of small and large epoch values.
            return expression.append(" ELSE CAST(NULL AS TIMESTAMP) END").toString();
        }
        return "date_bin(" + TimeWindowDuration.parse(query.getGroupByTime()).toIoTDBInterval()
                + ", " + timeColumn + ", " + plan.fixedOrigin() + ")";
    }

    /**
     * Translate a shared aggregate function into an IoTDB aggregate expression.
     * @param function aggregate function; for example {@code AggregationFunctionEnum.AVG}
     * @param field aggregate field name; for example {@code "value"}
     * @return IoTDB aggregate expression, for example {@code "AVG(value)"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private String toAggregationSql(AggregationFunctionEnum function,
                                    String field) {
        String column = normalizeIdentifier(field);
        AggregationFunctionEnum resolved = function == null ? AggregationFunctionEnum.AVG : function;
        return switch (resolved) {
            case COUNT -> "COUNT(" + column + ")";
            case SUM -> "SUM(" + column + ")";
            case AVG -> "AVG(" + column + ")";
            case MIN -> "MIN(" + column + ")";
            case MAX -> "MAX(" + column + ")";
            case FIRST -> "FIRST(" + column + ")";
            case LAST -> "LAST(" + column + ")";
        };
    }

    /**
     * Translate a shared filter into an IoTDB WHERE predicate.
     * @param filter shared query filter; for example {@code new QueryFilter("device_code", OperatorEnum.EQ, List.of("D001"))}
     * @return IoTDB WHERE predicate, for example {@code "device_code = 'D001'"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private String buildFilterSql(QueryFilter filter) {
        if (filter == null
                || isBlank(filter.column())
                || filter.operator() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "Invalid query filter");
        }
        List<Object> values = filter.values();
        if (values == null || values.isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Filter values must not be empty: " + filter.column());
        }
        String column = normalizeIdentifier(filter.column());
        OperatorEnum operator = filter.operator();
        return switch (operator) {
            case EQ -> column + " = " + formatSqlValue(values.get(0));
            case NE -> column + " <> " + formatSqlValue(values.get(0));
            case GT -> column + " > " + formatSqlValue(values.get(0));
            case GE -> column + " >= " + formatSqlValue(values.get(0));
            case LT -> column + " < " + formatSqlValue(values.get(0));
            case LE -> column + " <= " + formatSqlValue(values.get(0));
            case IN -> column + " IN (" + joinSqlValues(values) + ")";
            case BETWEEN -> {
                if (values.size() < 2) {
                    throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                            "BETWEEN filter requires two values: " + column);
                }
                yield column + " BETWEEN " + formatSqlValue(values.get(0)) + " AND " + formatSqlValue(values.get(1));
            }
        };
    }

    /**
     * Join SQL literals for an IN predicate.
     * @param values values to format; for example {@code List.of("beijing", "shanghai")}
     * @return SQL literal list, for example {@code "'beijing', 'shanghai'"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private String joinSqlValues(List<Object> values) {
        List<String> sqlValues = new ArrayList<>();
        for (Object value : values) {
            if (value != null) {
                sqlValues.add(formatSqlValue(value));
            }
        }
        if (sqlValues.isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "IN filter values must not be empty");
        }
        return String.join(", ", sqlValues);
    }

    /**
     * Convert a Java value into an IoTDB SQL literal.
     * @param value source value; for example {@code "device001"}, {@code 12.34D}
     * @return IoTDB SQL literal
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private String formatSqlValue(Object value) {
        if (value instanceof Number
                || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Instant) {
            return ((Instant) value).toEpochMilli() + "";
        }
        return "'" + escapeSqlString(String.valueOf(value)) + "'";
    }

    /**
     * Validate the requirements for an IoTDB table-model record.
     * @param tsdbRecord shared record; for example {@code new TSDBRecord("ACCRUE", 1783000000000L, tags, fields)}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private void validateRecord(TSDBRecord tsdbRecord) {
        normalizeIdentifier(tsdbRecord.measurement());
        if (tsdbRecord.timestamp() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "timestamp must not be null for table model write");
        }
        if (tsdbRecord.fields() == null || tsdbRecord.fields().isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "fields must not be empty for table model write");
        }
    }

    /**
     * Read a value from the prepared row using its canonical physical column identity.
     * @param record prepared row with canonical column keys
     * @param column table-column definition; for example {@code new TableColumn("device_code", ColumnCategory.TAG, TSDataType.STRING)}
     * @return column value
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private Object getColumnValue(PreparedRecord record,
                                  TableColumn column) {
        return record.values().get(column.name);
    }

    /**
     * Resolve the configured IoTDB node endpoints.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private List<String> resolveNodeUrls() {
        if (pool.getNodeUrls() != null
                && !pool.getNodeUrls().isEmpty()) {
            return pool.getNodeUrls();
        }
        throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR, "nodeUrls must not be empty");
    }

    /**
     * Determine node discovery and redirection from the configured policy and endpoint count.
     * @param discoveryMode node discovery policy; for example {@link IoTDBNodeDiscoveryModeEnum#AUTO}
     * @param endpointCount number of configured endpoints
     * @return whether node discovery and redirection are enabled
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-23
     */
    private static boolean resolveNodeDiscoveryEnabled(IoTDBNodeDiscoveryModeEnum discoveryMode,
                                                       int endpointCount) {
        if (discoveryMode == null) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.iotdb.discovery-mode must not be empty");
        }
        return switch (discoveryMode) {
            case AUTO -> endpointCount > 1;
            case ENABLED -> true;
            case DISABLED -> false;
        };
    }

    /**
     * Convert a Java value into the TSFile value type required by Tablet.
     * @param value source value; for example {@code "device001"}, {@code 12.34D}
     * @param dataType IoTDB column type; for example {@code TSDataType.STRING}
     * @return value accepted by Tablet
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private Object normalizeValue(Object value,
                                  TSDataType dataType) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            if (dataType == TSDataType.INT64) {
                return number.longValue();
            }
            if (dataType == TSDataType.INT32) {
                return number.intValue();
            }
            if (dataType == TSDataType.DOUBLE) {
                return number.doubleValue();
            }
            if (dataType == TSDataType.FLOAT) {
                return number.floatValue();
            }
        }
        if (value instanceof Boolean && dataType == TSDataType.BOOLEAN) {
            return value;
        }
        if (dataType == TSDataType.STRING) {
            String text = value instanceof Enum<?> enumValue ? enumValue.name() : String.valueOf(value);
            return new Binary(text, StandardCharsets.UTF_8);
        }
        if (dataType == TSDataType.BLOB && value instanceof byte[] bytes) {
            return new Binary(bytes.clone());
        }
        if (dataType == TSDataType.DATE && value instanceof LocalDate) {
            return value;
        }
        throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                "Value type " + value.getClass().getName()
                + " cannot be normalized as IoTDB " + dataType);
    }

    /**
     * Validate and return a simple unquoted IoTDB identifier.
     * <p>The same allowlist applies to databases, tables, columns, and aliases. Reject blank values, expressions, and special characters
     * to prevent executable SQL fragments. This method validates only; it does not trim, change case, quote, or escape.</p>
     * @param identifier database, table, column, or alias identifier; for example {@code "tsdb"}, {@code "ACCRUE"}
     * @return valid identifier unchanged
     */
    private String normalizeIdentifier(String identifier) {
        if (isBlank(identifier) || !identifier.matches(IDENTIFIER_PATTERN)) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Invalid IoTDB identifier: " + identifier);
        }
        return identifier;
    }

    /**
     * Escape single quotes in a SQL string literal.
     * @param value source value; for example {@code "Alan's device"}
     * @return escaped string
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private String escapeSqlString(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    /**
     * Check whether a string is null or blank.
     * @param value source value; for example {@code "ACCRUE"}
     * @return whether the value is null or blank
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    /**
     * An operation performed with a table-model session.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @FunctionalInterface
    private interface TableSessionAction {
        /**
         * Perform one table-model operation with the available session.
         * @param tableSession borrowed IoTDB table-model session; for example {@code ITableSession}
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-07-02
         */
        void execute(ITableSession tableSession) throws IoTDBConnectionException, StatementExecutionException;
    }

    /**
     * A result-read or cleanup action that may throw IoTDB query exceptions.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    @FunctionalInterface
    private interface QueryResultAction {
        /**
         * Perform one result-read or cleanup operation.
         * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
         * @throws StatementExecutionException if the server fails to execute the operation or release its result
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-09-01
         */
        void execute() throws IoTDBConnectionException, StatementExecutionException;
    }

    /**
     * Batch-write context for one IoTDB table.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static final class TableBatch {
        /**
         * Target table name.
         */
        private final String tableName;
        /**
         * Column definitions in first-occurrence order.
         */
        private final Map<String, TableColumn> columns = new LinkedHashMap<>();
        /**
         * Prepared rows for the current measurement, keyed by canonical physical column identity.
         */
        private final List<PreparedRecord> records = new ArrayList<>();

        /**
         * Create a batch context for the specified table.
         * @param tableName table name; for example {@code "ACCRUE"}
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-07-02
         */
        private TableBatch(String tableName) {
            this.tableName = tableName;
        }

        /**
         * Snapshot a row's values and collect column definitions using IoTDB's lowercase column identity.
         * Case variants within one row are ambiguous and must fail before any session is borrowed.
         * @param tsdbRecord shared record; for example {@code new TSDBRecord("ACCRUE", 1783000000000L, tags, fields)}
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-07-02
         */
        private void addRecord(TSDBRecord tsdbRecord) {
            Map<String, Object> values = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : tsdbRecord.tags().entrySet()) {
                String name = addValue(values, entry.getKey(), entry.getValue());
                addColumn(name, ColumnCategory.TAG, TSDataType.STRING);
            }
            for (Map.Entry<String, Object> entry : tsdbRecord.fields().entrySet()) {
                String name = addValue(values, entry.getKey(), entry.getValue());
                addColumn(name, ColumnCategory.FIELD, inferDataType(entry.getValue()));
            }
            records.add(new PreparedRecord(tsdbRecord.timestamp(), values));
        }

        /**
         * Add one value without mutating the caller's maps or dropping duplicate/null entries.
         * @param values prepared row values
         * @param originalName original tag or field name
         * @param value tag or field value
         * @return canonical physical column name
         */
        private String addValue(Map<String, Object> values, String originalName, Object value) {
            String name = canonicalColumn(originalName);
            if (values.containsKey(name)) {
                throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                        "Duplicate IoTDB physical column in one record: " + originalName);
            }
            values.put(name, value);
            return name;
        }

        /**
         * Record a column definition and reject conflicting roles or types for the same name within a batch.
         * @param name column name; for example {@code "value"}
         * @param category column role; for example {@code ColumnCategory.FIELD}
         * @param dataType IoTDB column type; for example {@code TSDataType.DOUBLE}
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-07-02
         */
        private void addColumn(String name,
                               ColumnCategory category,
                               TSDataType dataType) {
            TableColumn existing = columns.get(name);
            if (existing != null) {
                if (existing.category != category
                        || (existing.type != null && dataType != null && existing.type != dataType)) {
                    throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                            "Column type conflict: " + name);
                }
                if (existing.type == null && dataType != null) {
                    columns.put(name, new TableColumn(name, category, dataType));
                }
                return;
            }
            columns.put(name, new TableColumn(name, category, dataType));
        }

        /**
         * After scanning the batch, omit fields that are always null rather than guessing their schema types.
         * addColumn infers types from non-null values; Tablet null bitmaps preserve nulls in the other rows.
         */
        private void finishColumnTypes() {
            columns.values().removeIf(column -> column.type == null);
            if (columns.values().stream().noneMatch(column -> column.category == ColumnCategory.FIELD)) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "At least one non-null field value is required in each IoTDB table batch: " + tableName);
            }
        }

        /**
         * Infer an IoTDB table-model column type from a Java value.
         * @param value source value; for example {@code 12.34D}
         * @return IoTDB table-model column type
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-07-02
         */
        private TSDataType inferDataType(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Boolean) {
                return TSDataType.BOOLEAN;
            }
            if (value instanceof Byte
                    || value instanceof Short
                    || value instanceof Integer) {
                return TSDataType.INT32;
            }
            if (value instanceof Long) {
                return TSDataType.INT64;
            }
            if (value instanceof Float) {
                return TSDataType.FLOAT;
            }
            if (value instanceof Double) {
                return TSDataType.DOUBLE;
            }
            if (value instanceof String || value instanceof Character || value instanceof Enum<?>) {
                return TSDataType.STRING;
            }
            if (value instanceof byte[]) {
                return TSDataType.BLOB;
            }
            if (value instanceof LocalDate) {
                return TSDataType.DATE;
            }
            throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                    "Unsupported IoTDB field Java type: " + value.getClass().getName());
        }
    }

    /**
     * Track confirmed progress for one write spanning multiple Tablets.
     */
    private static final class WriteProgress {
        /**
         * Total physical Tablets.
         */
        private final int totalBatches;
        /**
         * Number of Tablets with confirmed commits.
         */
        private int committedBatches;
        /**
         * Number of records with confirmed commits.
         */
        private int committedRecords;
        /**
         * Whether a Tablet write has been attempted.
         */
        private boolean writeAttempted;
        /**
         * Measurement currently being written.
         */
        private String currentMeasurement;

        /**
         * Create the write-progress tracker.
         * @param totalBatches total physical Tablets to write
         */
        private WriteProgress(int totalBatches) {
            this.totalBatches = totalBatches;
        }

        /**
         * Return the zero-based index of the currently failed batch.
         * @return confirmed batch count, or {@code null} before the first write attempt
         */
        private Integer failedBatchIndex() {
            return writeAttempted ? committedBatches : null;
        }
    }

    /**
     * Private row snapshot whose values use the same canonical keys as the table's column definitions.
     */
    private record PreparedRecord(long timestamp, Map<String, Object> values) {
    }

    /**
     * An IoTDB table-column definition.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private record TableColumn(String name,
                               ColumnCategory category,
                               TSDataType type) {
    }
}
