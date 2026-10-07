package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.util.TimeWindowDuration;
import com.alandevise.tsgate.util.TimeWindowPlan;
import com.alandevise.tsgate.util.TSDBQueryValidator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.config.InfluxDBProperties;
import com.alandevise.tsgate.config.StrictCursorSqlStrategyEnum;
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
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.write.WritePrecision;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * InfluxDB 3 Core adapter.
 * <p>Writes translate {@link TSDBRecord} values to line protocol;
 * queries translate the unified query model to InfluxDB 3 SQL and map JSON responses to {@link QueryResult}.
 * This adapter does not create databases or configure retention policies.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
@Slf4j
public class InfluxDBAdapter implements TSDBAdapter {

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final MediaType LINE_PROTOCOL = MediaType.parse("text/plain; charset=utf-8");
    private static final int WRITE_BATCH_MAX_LINES = 5_000;
    private static final int WRITE_BATCH_MAX_BYTES = 1024 * 1024;
    private static final int WRITE_LINE_MAX_BYTES = WRITE_BATCH_MAX_BYTES;
    private static final int LINE_PROTOCOL_COMPONENT_MAX_BYTES = 64 * 1024;
    private static final TypeReference<Map<String, Object>> ROW_TYPE = new TypeReference<>() {
    };

    private final InfluxDBProperties config;
    private final int maxBatchRecords;
    private final long maxBatchBytes;
    private final int maxQueryRows;
    private final long maxQueryResponseBytes;
    private final boolean queryLogEnabled;
    private final StrictCursorSqlStrategyEnum strictCursorSql;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
    private LifecycleState state = LifecycleState.NEW;
    private OkHttpClient client;
    private InfluxDBClient nativeClient;

    /**
     * Creates an InfluxDB adapter.
     *
     * @param config connection settings, for example {@code url=http://localhost:8181, database=tsdb}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public InfluxDBAdapter(InfluxDBProperties config) {
        this(config, true);
    }

    /**
     * Creates an InfluxDB adapter with configurable SQL logging.
     * Configuration is copied at construction, including HTTP settings; later changes to the
     * supplied properties do not reconfigure this adapter or its borrowed native client.
     *
     * @param config          connection settings, for example {@code url=http://localhost:8181, database=tsdb}
     * @param queryLogEnabled whether to log executed SQL at DEBUG level
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-22
     */
    public InfluxDBAdapter(InfluxDBProperties config,
                           boolean queryLogEnabled) {
        config = snapshotConfiguration(config);
        this.config = config;
        this.maxBatchRecords = resolveMaxBatchRecords(config);
        if (config.getMaxBatchBytes() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.influxdb.max-batch-bytes must be greater than 0");
        }
        this.maxBatchBytes = config.getMaxBatchBytes();
        if (config.getStrictCursorSql() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.influxdb.strict-cursor-sql must be or or union-all");
        }
        this.strictCursorSql = config.getStrictCursorSql();
        if (config.getMaxQueryRows() <= 0 || config.getMaxQueryRows() == Integer.MAX_VALUE) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.influxdb.max-query-rows must be between 1 and 2147483646");
        }
        if (config.getMaxQueryResponseBytes() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.influxdb.max-query-response-bytes must be greater than 0");
        }
        this.maxQueryRows = config.getMaxQueryRows();
        this.maxQueryResponseBytes = config.getMaxQueryResponseBytes();
        this.queryLogEnabled = queryLogEnabled;
    }

    /** Copies all settings without replacing explicitly supplied null values with defaults. */
    private static InfluxDBProperties snapshotConfiguration(InfluxDBProperties source) {
        if (source == null) {
            return null;
        }
        InfluxDBProperties copy = new InfluxDBProperties();
        copy.setEnable(source.isEnable());
        copy.setFailFast(source.isFailFast());
        copy.setUrl(source.getUrl());
        copy.setToken(source.getToken());
        copy.setDatabase(source.getDatabase());
        copy.setStrictCursorSql(source.getStrictCursorSql());
        copy.setMaxBatchRecords(source.getMaxBatchRecords());
        copy.setMaxBatchBytes(source.getMaxBatchBytes());
        copy.setMaxQueryRows(source.getMaxQueryRows());
        copy.setMaxQueryResponseBytes(source.getMaxQueryResponseBytes());
        InfluxDBProperties.HttpClientConfig http = source.getHttpClient();
        if (http == null) {
            copy.setHttpClient(null);
        } else {
            InfluxDBProperties.HttpClientConfig httpCopy = new InfluxDBProperties.HttpClientConfig();
            httpCopy.setMaxIdleConnections(http.getMaxIdleConnections());
            httpCopy.setKeepAliveDurationMs(http.getKeepAliveDurationMs());
            httpCopy.setConnectTimeoutMs(http.getConnectTimeoutMs());
            httpCopy.setReadTimeoutMs(http.getReadTimeoutMs());
            httpCopy.setWriteTimeoutMs(http.getWriteTimeoutMs());
            httpCopy.setCallTimeoutMs(http.getCallTimeoutMs());
            httpCopy.setRetryOnConnectionFailure(http.isRetryOnConnectionFailure());
            copy.setHttpClient(httpCopy);
        }
        return copy;
    }

    /**
     * Initializes the HTTP client and official Java client.
     *
     * <p>Initialization creates client resources without accessing or validating the default database.
     * Repeated initialization is a no-op. Failed initialization releases partial resources and may be
     * retried; a closed adapter cannot be reopened.</p>
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public void init() {
        lifecycleLock.writeLock().lock();
        OkHttpClient newClient = null;
        InfluxDBClient newNativeClient = null;
        try {
            if (state == LifecycleState.READY) {
                return;
            }
            if (state == LifecycleState.CLOSED) {
                throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                        "InfluxDB adapter is closed and cannot be reinitialized");
            }
            newClient = buildHttpClient();
            newNativeClient = buildNativeClient();
            client = newClient;
            nativeClient = newNativeClient;
            state = LifecycleState.READY;
            log.info("InfluxDB 3 Core client resources initialized, url: {}, database: {}; "
                            + "default database is not validated during initialization",
                    config.getUrl(), config.getDatabase());
        } catch (TSDBException e) {
            closeResources(newNativeClient, newClient);
            throw e;
        } catch (RuntimeException e) {
            closeResources(newNativeClient, newClient);
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Failed to initialize InfluxDB client resources", e);
        } catch (Error e) {
            closeResources(newNativeClient, newClient);
            throw e;
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    /**
     * Closes the official InfluxDB client and HTTP client.
     * Waits for in-flight adapter operations to finish, clears references, and permanently rejects
     * new operations. Repeated close calls are harmless. Direct use of the native client must be
     * coordinated by its caller with adapter shutdown.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (state == LifecycleState.CLOSED) {
                return;
            }
            state = LifecycleState.CLOSED;
            InfluxDBClient oldNativeClient = nativeClient;
            OkHttpClient oldClient = client;
            nativeClient = null;
            client = null;
            closeResources(oldNativeClient, oldClient);
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    /**
     * Releases complete or partially initialized client resources.
     */
    private static void closeResources(InfluxDBClient nativeResource, OkHttpClient httpResource) {
        try {
            if (nativeResource != null) {
                try {
                    nativeResource.close();
                    log.info("InfluxDB 3 official Java client closed");
                } catch (Exception e) {
                    log.error("Failed to close InfluxDB 3 official Java client", e);
                }
            }
        } finally {
            if (httpResource != null) {
                try {
                    httpResource.dispatcher().executorService().shutdown();
                } finally {
                    httpResource.connectionPool().evictAll();
                }
                log.info("InfluxDB 3 Core HTTP client closed");
            }
        }
    }

    /**
     * Checks lifecycle state while holding a read or write lock.
     */
    private void requireReady() {
        if (state != LifecycleState.READY) {
            throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                    state == LifecycleState.CLOSED ? "InfluxDB adapter is closed"
                            : "InfluxDB adapter is not initialized");
        }
    }

    private enum LifecycleState {NEW, READY, CLOSED}

    /**
     * Executes InfluxDB 3 SQL.
     *
     * @param sql InfluxDB 3 SQL, for example {@code "SELECT * FROM \"ACCRUE\" LIMIT 10"}
     * @return query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public QueryResult executeQuery(String sql) {
        String query = sql == null ? "" : sql.trim();
        if (query.isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "SQL must not be empty");
        }
        return executeQueryRequest(resolveDatabase(null), query);
    }

    /**
     * Writes a batch of unified records, encoding each record as one line of line protocol.
     *
     * @param database target database, for example {@code "tsdb"}; blank uses the configured default
     * @param records  unified records, for example {@code List.of(record1, record2)}
     * @return batch result describing the known commit boundary
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public BatchWriteResult batchWriteDetailed(String database, Collection<TSDBRecord> records) {
        lifecycleLock.readLock().lock();
        try {
            return batchWriteWhileReady(database, records);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Holds the lifecycle read lock across validation and every HTTP write in the batch.
     */
    private BatchWriteResult batchWriteWhileReady(String database, Collection<TSDBRecord> records) {
        requireReady();
        if (records == null || records.isEmpty()) {
            return BatchWriteResult.emptySuccess();
        }
        List<TSDBRecord> snapshot;
        List<PreparedWriteBatch> batches;
        HttpUrl url;
        try {
            validateBatchSize(records);
            // Snapshot, validate, and encode every record before sending to prevent partial writes caused by invalid data.
            snapshot = List.copyOf(records);
            validateBatchSize(snapshot);
            batches = prepareWriteBatches(snapshot, maxBatchBytes);
            url = apiUrl("write_lp")
                    .addQueryParameter("db", resolveDatabase(database))
                    .addQueryParameter("precision", "millisecond")
                    .addQueryParameter("accept_partial", "false")
                    .build();
        } catch (RuntimeException e) {
            // No HTTP request has been sent, so the entire batch is known to be uncommitted.
            throw preflightFailure(records.size(), e);
        }
        int committedRecords = 0;
        int committedBatches = 0;
        for (int batchIndex = 0; batchIndex < batches.size(); batchIndex++) {
            PreparedWriteBatch batch = batches.get(batchIndex);
            try {
                writePayload(url, batch.payload());
                // Count committed records only after receiving a successful response for this batch.
                committedRecords += batch.recordCount();
                committedBatches++;
            } catch (InfluxWriteHttpException e) {
                // An HTTP error does not prove a write failed; only definite rejection establishes the commit boundary.
                boolean rejected = isDefiniteWriteRejection(e.getStatusCode());
                BatchCommitStateEnum state = BatchCommitStateEnum.UNKNOWN;
                if (rejected) {
                    state = committedRecords == 0 ? BatchCommitStateEnum.NOT_COMMITTED
                            : BatchCommitStateEnum.PARTIALLY_COMMITTED;
                }
                // Preserve the lower bound from successful batches; retryability is independent of commit state.
                BatchWriteResult result = new BatchWriteResult(snapshot.size(), snapshot.size(),
                        committedRecords, batches.size(), committedBatches, batchIndex,
                        batch.measurement(), state, isRetryableWriteStatus(e.getStatusCode()), e.getMessage());
                TSDBErrorCodeEnum code = rejected
                        ? errorCodeForHttpStatus(e.getStatusCode(), TSDBErrorCodeEnum.WRITE_ERROR)
                        : TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN;
                throw new TSDBBatchWriteException(code, result, e);
            } catch (UncheckedIOException e) {
                // A lost response leaves this batch unknown; preserve the previously confirmed commit count.
                BatchWriteResult result = new BatchWriteResult(snapshot.size(), snapshot.size(),
                        committedRecords, batches.size(), committedBatches, batchIndex,
                        batch.measurement(), BatchCommitStateEnum.UNKNOWN, true, e.getMessage());
                throw new TSDBBatchWriteException(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, result, e);
            }
        }
        return BatchWriteResult.success(snapshot.size(), batches.size());
    }

    /**
     * Determines whether an {@code accept_partial=false} response definitely rejects the batch.
     * <p>Gateway timeouts, server errors, and unrecognized responses cannot establish zero commits.</p>
     *
     * @param statusCode HTTP status returned by the write request
     * @return {@code true} for definite rejection; otherwise the commit state is unknown
     * @since 2026-09-09
     */
    private static boolean isDefiniteWriteRejection(int statusCode) {
        return switch (statusCode) {
            case 400, 401, 403, 404, 405, 413, 415, 422, 429 -> true;
            default -> false;
        };
    }

    /**
     * Determines whether callers may retry an HTTP failure using an idempotency strategy.
     * <p>Retryability does not imply zero commits and never triggers automatic adapter replay.</p>
     *
     * @param statusCode HTTP status returned by the write request
     * @return {@code true} for transient errors, otherwise {@code false}
     * @since 2026-09-09
     */
    private static boolean isRetryableWriteStatus(int statusCode) {
        return statusCode == 408 || statusCode == 429
                || (statusCode >= 500 && statusCode < 600 && statusCode != 501 && statusCode != 505);
    }

    /**
     * Converts a preflight failure into a batch exception with confirmed zero commits.
     *
     * @param requestedRecords requested record count
     * @param cause            validation or payload construction failure
     * @return exception carrying a {@link BatchCommitStateEnum#NOT_COMMITTED} result
     */
    private static TSDBBatchWriteException preflightFailure(int requestedRecords, RuntimeException cause) {
        BatchWriteResult result = new BatchWriteResult(requestedRecords, 0, 0,
                0, 0, null, null, BatchCommitStateEnum.NOT_COMMITTED, false,
                "InfluxDB batch validation failed before HTTP I/O: " + cause.getMessage());
        return new TSDBBatchWriteException(result, cause);
    }

    /**
     * Encodes all records into bounded payloads before I/O, so validation or encoding failures cannot write data.
     *
     * @param records stable snapshot of the records
     * @param maxBatchBytes maximum combined UTF-8 payload bytes for the complete application batch
     * @return pending batches bounded by line count and UTF-8 byte size
     */
    private static List<PreparedWriteBatch> prepareWriteBatches(List<TSDBRecord> records, long maxBatchBytes) {
        List<PreparedWriteBatch> batches = new ArrayList<>();
        StringBuilder payload = new StringBuilder();
        int lineCount = 0;
        int payloadBytes = 0;
        long totalPayloadBytes = 0;
        String firstMeasurement = null;
        for (TSDBRecord tsdbRecord : records) {
            String line = buildLineProtocol(tsdbRecord);
            // Limit each line by its encoded UTF-8 size, rather than Java character count.
            int lineBytes = utf8Length(line);
            if (lineBytes > WRITE_LINE_MAX_BYTES) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "InfluxDB line protocol record exceeds maximum UTF-8 bytes: "
                                + lineBytes + " > " + WRITE_LINE_MAX_BYTES);
            }
            // Every line after the first requires one additional newline byte.
            long nextPayloadBytes = (long) payloadBytes + lineBytes + (payload.isEmpty() ? 0 : 1);
            // Seal the current batch before this line would exceed its line or byte limit.
            if (!payload.isEmpty()
                    && (lineCount >= WRITE_BATCH_MAX_LINES || nextPayloadBytes > WRITE_BATCH_MAX_BYTES)) {
                batches.add(new PreparedWriteBatch(payload.toString(), lineCount, firstMeasurement));
                payload.setLength(0);
                lineCount = 0;
                payloadBytes = 0;
                firstMeasurement = null;
            }
            // Count only bytes sent on the wire: physical requests have no separator between them.
            long additionalBytes = (long) lineBytes + (payload.isEmpty() ? 0 : 1);
            if (additionalBytes > maxBatchBytes - totalPayloadBytes) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "InfluxDB batch exceeds maximum combined UTF-8 payload bytes: " + maxBatchBytes);
            }
            totalPayloadBytes += additionalBytes;
            // The first measurement identifies failures; it does not determine batch boundaries.
            if (firstMeasurement == null) {
                firstMeasurement = tsdbRecord.measurement();
            }
            if (!payload.isEmpty()) {
                payload.append('\n');
                payloadBytes++;
            }
            payload.append(line);
            payloadBytes += lineBytes;
            lineCount++;
        }
        // Keep the final batch even when it has not reached a limit.
        if (!payload.isEmpty()) {
            batches.add(new PreparedWriteBatch(payload.toString(), lineCount, firstMeasurement));
        }
        return batches;
    }

    /**
     * Writes one bounded line protocol payload.
     *
     * @param url     InfluxDB write_lp endpoint, for example {@code /api/v3/write_lp?db=tsdb}
     * @param payload line protocol payload containing a bounded number of records
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-09
     */
    private void writePayload(HttpUrl url, String payload) {
        Request request = authorize(new Request.Builder()
                .url(url)
                .post(RequestBody.create(payload, LINE_PROTOCOL)))
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                return;
            }
            String responseBody = readBody(response);
            log.error("Failed to write records to InfluxDB 3 Core: {}", responseBody);
            throw new InfluxWriteHttpException(response.code(), responseBody);
        } catch (IOException e) {
            log.error("Failed to write records to InfluxDB 3 Core", e);
            throw new UncheckedIOException("Failed to write records to InfluxDB 3 Core", e);
        }
    }

    /**
     * Translates and executes the unified query model as InfluxDB SQL.
     *
     * @param database target database, for example {@code "tsdb"}; blank uses the configured default
     * @param query    unified query model, for example {@code measurement=ACCRUE, limit=100}
     * @return query result with aggregate window_start values normalized to epoch milliseconds
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public QueryResult query(String database, TSDBQuery query) {
        TSDBQueryValidator.validate(query);
        if (query == null || isBlank(query.getMeasurement())) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "measurement must not be empty");
        }
        requireNativeTimeColumn(query);
        // Only a pagination probe may fetch one extra row to determine whether another page exists.
        int rowLimit = maxQueryRows + (query.isPaginationProbe() ? 1 : 0);
        String sql = buildQuerySql(query, strictCursorSql);
        TSDBQueryValidator.validateAggregationOutputNames(query, this::normalizeColumnIdentifier);
        QueryResult result = executeQueryRequest(resolveDatabase(database), sql, rowLimit);
        if (query.hasAggregations() && !isBlank(query.getGroupByTime())) {
            // Only normalize adapter-generated window columns; ordinary fields and native SQL aliases remain unchanged.
            // InfluxDB may omit the UTC suffix; reuse its timestamp parsing rules before common mapping.
            for (Map<String, Object> row : result.getRows()) {
                Long epochMillis = parseTimeMillis(row.get("window_start"));
                if (epochMillis != null) {
                    row.put("window_start", epochMillis);
                }
            }
        }
        return result;
    }

    /**
     * Counts rows before pagination. Detail queries use {@code COUNT(*)}; grouped or aggregate queries use
     * an outer count over aggregate rows, matching PageHelper semantics for aggregate SQL.
     *
     * @param database target database; blank uses the configured default
     * @param query    unified query model; pagination and cursor settings are ignored
     * @return total row count before pagination
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    @Override
    public long count(String database, TSDBQuery query) {
        if (query == null || isBlank(query.getMeasurement())) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "measurement must not be empty");
        }
        requireNativeTimeColumn(query);
        String sql = buildCountSql(query);
        TSDBQueryValidator.validateAggregationOutputNames(query, this::normalizeColumnIdentifier);
        QueryResult result = executeQueryRequest(resolveDatabase(database), sql);
        return extractCount(result);
    }

    /**
     * Returns the display name of this adapter.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public String getAdapterName() {
        return "InfluxDB";
    }

    /**
     * Returns the configured maximum record count for a single batch write.
     *
     * @return configured {@code tsdb.influxdb.max-batch-records} value
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-13
     */
    @Override
    public int getMaxBatchRecords() {
        return maxBatchRecords;
    }

    /**
     * Returns the official InfluxDB Java client for auto-configuration as an injectable bean.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public InfluxDBClient getNativeClient() {
        lifecycleLock.readLock().lock();
        try {
            requireReady();
            return nativeClient;
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Executes SQL through the InfluxDB 3 query_sql API and maps JSON rows to a unified result.
     *
     * @param database query database, for example {@code "tsdb"}
     * @param sql      InfluxDB 3 SQL, for example {@code "SELECT time, value FROM \"ACCRUE\" LIMIT 10"}
     * @return query result
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private QueryResult executeQueryRequest(String database, String sql) {
        return executeQueryRequest(database, sql, maxQueryRows);
    }

    /**
     * Keeps resources open until the bounded response has been consumed and released.
     */
    private QueryResult executeQueryRequest(String database, String sql, int rowLimit) {
        lifecycleLock.readLock().lock();
        try {
            requireReady();
            return executeQueryWhileReady(database, sql, rowLimit);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private QueryResult executeQueryWhileReady(String database, String sql, int rowLimit) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("db", database);
        body.put("q", sql);
        Request request = authorize(new Request.Builder()
                .url(apiUrl("query_sql").build())
                .post(jsonBody(body)))
                .build();
        QueryResult result = new QueryResult();
        logQuerySql(sql);
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String responseBody = readBody(response);
                throw new TSDBException(errorCodeForHttpStatus(response.code(), TSDBErrorCodeEnum.QUERY_ERROR),
                        "InfluxDB SQL query rejected with HTTP " + response.code() + ": " + responseBody);
            }
            List<Map<String, Object>> rows = readRows(response, rowLimit);
            normalizeRows(rows);
            Set<String> columns = new LinkedHashSet<>();
            for (Map<String, Object> row : rows) {
                columns.addAll(row.keySet());
            }
            result.setColumns(new ArrayList<>(columns));
            result.setRows(rows);
            result.setRowCount(rows.size());
            result.setSuccess(true);
            result.setMessage("SQL executed successfully");
            return result;
        } catch (JsonProcessingException e) {
            log.error("Failed to parse query response from InfluxDB 3 Core", e);
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                    "Failed to parse query response from InfluxDB 3 Core", e);
        } catch (IOException e) {
            log.error("Failed to query data from InfluxDB 3 Core", e);
            throw new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR,
                    "Failed to query data from InfluxDB 3 Core", e);
        }
    }

    /**
     * Logs executed InfluxDB SQL when enabled by the common configuration.
     *
     * @param sql SQL submitted to the InfluxDB query_sql API
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-22
     */
    private void logQuerySql(String sql) {
        if (queryLogEnabled) {
            log.debug("Executing InfluxDB query SQL: {}", sql);
        }
    }

    /**
     * Builds an OkHttpClient from the connection configuration.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private OkHttpClient buildHttpClient() {
        InfluxDBProperties.HttpClientConfig httpClientConfig = config.getHttpClient();
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(
                        httpClientConfig.getMaxIdleConnections(),
                        httpClientConfig.getKeepAliveDurationMs(),
                        TimeUnit.MILLISECONDS))
                .connectTimeout(httpClientConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(httpClientConfig.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(httpClientConfig.getWriteTimeoutMs(), TimeUnit.MILLISECONDS)
                .followRedirects(false)
                .followSslRedirects(false)
                .retryOnConnectionFailure(httpClientConfig.isRetryOnConnectionFailure());
        if (httpClientConfig.getCallTimeoutMs() > 0) {
            builder.callTimeout(httpClientConfig.getCallTimeoutMs(), TimeUnit.MILLISECONDS);
        }
        return builder.build();
    }

    /**
     * Builds the official Java client for injection into application code.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private InfluxDBClient buildNativeClient() {
        ensureArrowAllocationManager();
        InfluxDBProperties.HttpClientConfig httpClientConfig = config.getHttpClient();
        ClientConfig.Builder builder = new ClientConfig.Builder()
                .host(config.getUrl())
                .database(resolveDatabase(config.getDatabase()))
                .writePrecision(WritePrecision.MS)
                .timeout(Duration.ofMillis(httpClientConfig.getConnectTimeoutMs()))
                .writeTimeout(Duration.ofMillis(httpClientConfig.getWriteTimeoutMs()))
                .queryTimeout(Duration.ofMillis(httpClientConfig.getReadTimeoutMs()));
        builder.token(isBlank(config.getToken()) ? new char[0] : config.getToken().trim().toCharArray());
        return InfluxDBClient.getInstance(builder.build());
    }

    /**
     * Supplies an Arrow allocator when no allocator was configured explicitly.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void ensureArrowAllocationManager() {
        if (isBlank(System.getProperty("arrow.allocation.manager.type"))
                && isBlank(System.getenv("ARROW_ALLOCATION_MANAGER_TYPE"))) {
            System.setProperty("arrow.allocation.manager.type", "Unsafe");
        }
    }

    /**
     * Translates the unified query model to InfluxDB 3 SQL.
     *
     * @param query unified query model, for example {@code measurement=ACCRUE, selectColumns=[value]}
     * @return InfluxDB 3 SQL
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String buildQuerySql(TSDBQuery query) {
        return buildQuerySql(query, true);
    }

    /** Selects the configured strategy without rewriting ordinary queries or first cursor pages. */
    private static String buildQuerySql(TSDBQuery query, StrictCursorSqlStrategyEnum strategy) {
        query = normalizeStrictCursorQuery(query);
        if (strategy == StrictCursorSqlStrategyEnum.UNION_ALL && query.isStrictCursor()) {
            if (query.hasAggregations()) {
                throw new TSDBException(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                        "UNION ALL strict cursor pagination does not support aggregate queries");
            }
            if (hasStrictCursorValues(query)) {
                return buildUnionCursorQuerySql(query);
            }
        }
        return buildQuerySql(query);
    }

    /** Trims strict cursor keys in a private query copy, preserving distinct physical-name casing. */
    private static TSDBQuery normalizeStrictCursorQuery(TSDBQuery query) {
        if (!hasStrictCursorValues(query)) {
            return query;
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : query.getCursorValues().entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isBlank() || entry.getValue() == null) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Strict cursor keys must not be null or blank, and values must not be null");
            }
            String column = key.trim();
            if (values.containsKey(column)) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Ambiguous strict cursor column after trimming: " + column);
            }
            values.put(column, entry.getValue());
        }
        TSDBQuery normalized = query.copy();
        normalized.setCursorValues(values);
        return normalized;
    }

    /**
     * Splits a lexicographic cursor into disjoint branches, then orders and limits their combined rows.
     * Hidden sort columns are available inside the derived table without changing the caller's projection.
     */
    private static String buildUnionCursorQuerySql(TSDBQuery query) {
        List<String> selections = buildSelections(query);
        List<String> branchSelections = new ArrayList<>(selections);
        List<SortSpec> sorts = resolveCursorSortSpecs(query);
        if (query.getSelectColumns() != null && !query.getSelectColumns().isEmpty()) {
            for (SortSpec sort : sorts) {
                String expression = quoteIdentifier(isTimeColumn(sort.column()) ? "time" : sort.column());
                if (!branchSelections.contains(expression)) {
                    branchSelections.add(expression);
                }
            }
        }
        List<String> commonClauses = buildWhereClauses(query, false);
        // Validate every cursor value before pruning, so an empty branch cannot hide an invalid cursor.
        List<String> cursorBranches = buildStrictCursorBranches(query);
        List<String> branches = new ArrayList<>();
        for (int i = 0; i < cursorBranches.size(); i++) {
            if (hasEmptyCursorTimeRange(query, sorts, i)) {
                continue;
            }
            List<String> clauses = new ArrayList<>(commonClauses);
            clauses.add(cursorBranches.get(i));
            StringBuilder branch = new StringBuilder("SELECT ")
                    .append(String.join(", ", branchSelections))
                    .append(" FROM ").append(quoteIdentifier(query.getMeasurement()));
            appendWhere(branch, clauses);
            branches.add(branch.toString());
        }
        StringBuilder sql = new StringBuilder("SELECT ").append(String.join(", ", selections));
        if (branches.isEmpty()) {
            // An explicit false predicate avoids sending contradictory time bounds to the query planner.
            sql.append(" FROM ").append(quoteIdentifier(query.getMeasurement()));
            List<String> clauses = new ArrayList<>(commonClauses);
            clauses.add("FALSE");
            appendWhere(sql, clauses);
        } else {
            sql.append(" FROM (").append(String.join(" UNION ALL ", branches))
                    .append(") AS \"__tsgate_cursor_rows\"");
        }
        appendOrder(query, sql);
        appendPagination(query, sql);
        return sql.toString();
    }

    /**
     * Prunes only branches whose cursor time predicate contradicts explicit inclusive time bounds.
     * Branches before the time key have no cursor time constraint and must remain, even for FIELD-first sorts.
     */
    private static boolean hasEmptyCursorTimeRange(TSDBQuery query, List<SortSpec> sorts, int branchIndex) {
        Long start = query.getStartTime();
        Long end = query.getEndTime();
        if (start != null && end != null && start > end) {
            return true;
        }
        for (int i = 0; i <= branchIndex; i++) {
            SortSpec sort = sorts.get(i);
            if (isTimeColumn(sort.column())) {
                long time = parseCursorTimeMillis(requireCursorValue(query.getCursorValues(), sort.column()));
                if (i < branchIndex) {
                    return (start != null && time < start) || (end != null && time > end);
                }
                return sort.order() == SortOrderEnum.DESC
                        ? start != null && time <= start
                        : end != null && time >= end;
            }
        }
        return false;
    }

    /**
     * Translates the unified model to SQL, allowing count subqueries to omit ordering and pagination.
     *
     * @param query               unified query model
     * @param includeOrderAndPage whether to include ORDER BY, LIMIT, and OFFSET
     * @return InfluxDB 3 SQL
     */
    private static String buildQuerySql(TSDBQuery query,
                                        boolean includeOrderAndPage) {
        List<String> selections = buildSelections(query);
        List<String> whereClauses = buildWhereClauses(query);
        List<String> groupExpressions = buildGroupExpressions(query);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(String.join(", ", selections));
        sql.append(" FROM ").append(quoteIdentifier(query.getMeasurement()));
        appendWhere(sql, whereClauses);
        if (!groupExpressions.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupExpressions));
        }
        if (includeOrderAndPage) {
            appendOrder(query, sql);
            appendPagination(query, sql);
        }
        return sql.toString();
    }

    /** Applies pagination once, after the complete result has been ordered. */
    private static void appendPagination(TSDBQuery query, StringBuilder sql) {
        if (query.getLimit() != null && query.getLimit() > 0) {
            sql.append(" LIMIT ").append(query.getLimit());
        }
        if (query.getOffset() != null && query.getOffset() > 0) {
            sql.append(" OFFSET ").append(query.getOffset());
        }
    }

    /**
     * Builds the SQL for the total row count.
     *
     * @param query unified query model
     * @return direct count SQL for detail queries or an outer count over aggregate rows
     */
    private static String buildCountSql(TSDBQuery query) {
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
                .append(quoteIdentifier(countQuery.getMeasurement()));
        appendWhere(sql, buildWhereClauses(countQuery));
        return sql.toString();
    }

    /**
     * Extracts the long value from a single count result row.
     *
     * @param result count query result
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
        try {
            return value instanceof Number number ? exactLong(number)
                    : new BigDecimal(String.valueOf(value)).longValueExact();
        } catch (RuntimeException e) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                    "InfluxDB count query returned an invalid or out-of-range integer: " + value, e);
        }
    }

    /**
     * Builds SELECT expressions, including window, grouping tags, and aggregate expressions when needed.
     *
     * @param query unified query model, for example {@code selectColumns=[value, status]}
     * @return SELECT expressions
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static List<String> buildSelections(TSDBQuery query) {
        List<String> selections = new ArrayList<>();
        if (query.hasAggregations()) {
            if (!isBlank(query.getGroupByTime())) {
                selections.add(buildTimeWindowExpression(query)
                        + " AS window_start");
            }
            for (String tag : query.getGroupByTags()) {
                selections.add(quoteIdentifier(tag));
            }
            for (AggregationSpec aggregation : query.getAggregations()) {
                selections.add(toSqlAggregation(aggregation.function(), aggregation.field(), query.getTimeColumn())
                        + " AS " + quoteIdentifier(aggregation.alias()));
            }
            return selections;
        }
        if (query.getSelectColumns() == null || query.getSelectColumns().isEmpty()) {
            selections.add("*");
            return selections;
        }
        if (query.getSelectColumns().stream().noneMatch(column -> query.getTimeColumn().equals(column))) {
            selections.add(quoteIdentifier(query.getTimeColumn()));
        }
        for (String column : query.getSelectColumns()) {
            selections.add(quoteIdentifier(column));
        }
        return selections;
    }

    /**
     * Builds WHERE clauses for time bounds, pagination cursors, and application filters.
     *
     * @param query unified query model, for example {@code startTime=1783000000000L, filters=[device_code=D001]}
     * @return WHERE clause fragments
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static List<String> buildWhereClauses(TSDBQuery query) {
        return buildWhereClauses(query, true);
    }

    /** Builds common filters, optionally including the original cursor predicate. */
    private static List<String> buildWhereClauses(TSDBQuery query, boolean includeCursor) {
        List<String> clauses = new ArrayList<>();
        if (query.getStartTime() != null) {
            clauses.add(quoteIdentifier(query.getTimeColumn()) + " >= timestamp "
                    + sqlString(Instant.ofEpochMilli(query.getStartTime()).toString()));
        }
        if (query.getEndTime() != null) {
            clauses.add(quoteIdentifier(query.getTimeColumn()) + " <= timestamp "
                    + sqlString(Instant.ofEpochMilli(query.getEndTime()).toString()));
        }
        boolean hasStrictCursorValues = hasStrictCursorValues(query);
        if (includeCursor && hasStrictCursorValues) {
            clauses.add(buildStrictCursorClause(query));
        }
        if (includeCursor && !hasStrictCursorValues && query.getCursorTime() != null) {
            boolean desc = SortOrderEnum.normalize(query.getOrder()) == SortOrderEnum.DESC;
            String operator = desc ? "<" : ">";
            clauses.add(quoteIdentifier(query.getTimeColumn()) + " " + operator + " timestamp "
                    + sqlString(Instant.ofEpochMilli(query.getCursorTime()).toString()));
        }
        for (QueryFilter filter : query.getFilters()) {
            clauses.add(buildFilterSql(filter));
        }
        return clauses;
    }

    private static boolean hasStrictCursorValues(TSDBQuery query) {
        return query.isStrictCursor() && query.getCursorValues() != null && !query.getCursorValues().isEmpty();
    }

    /**
     * Builds lexicographic composite cursor conditions in ORDER BY column order.
     * <p>Ascending example: {@code (time > timestamp '2026-07-01T00:00:00Z') OR (time = timestamp '...' AND "device_code" > 'D001')}.</p>
     *
     * @param query unified query model, for example {@code cursorColumns=[time, device_code]}
     * @return composite cursor WHERE fragment
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static String buildStrictCursorClause(TSDBQuery query) {
        return "(" + String.join(" OR ", buildStrictCursorBranches(query)) + ")";
    }

    /** Builds mutually exclusive cursor predicates in the final sort order. */
    private static List<String> buildStrictCursorBranches(TSDBQuery query) {
        List<SortSpec> cursorSortSpecs = resolveCursorSortSpecs(query);
        Map<String, Object> cursorValues = query.getCursorValues();
        List<String> groups = new ArrayList<>();
        for (int i = 0; i < cursorSortSpecs.size(); i++) {
            List<String> parts = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                String equalsColumn = cursorSortSpecs.get(j).column();
                Object equalsValue = requireCursorValue(cursorValues, equalsColumn);
                parts.add(formatCursorColumn(equalsColumn) + " = " + formatCursorSqlValue(equalsColumn, equalsValue));
            }
            SortSpec compareSort = cursorSortSpecs.get(i);
            String compareColumn = compareSort.column();
            Object compareValue = requireCursorValue(cursorValues, compareColumn);
            String operator = compareSort.order() == SortOrderEnum.DESC ? "<" : ">";
            parts.add(formatCursorColumn(compareColumn) + " " + operator + " "
                    + formatCursorSqlValue(compareColumn, compareValue));
            groups.add("(" + String.join(" AND ", parts) + ")");
        }
        return groups;
    }

    /**
     * Builds GROUP BY expressions for the time window and tags.
     *
     * @param query unified query model, for example {@code groupByTime=5m, groupByTags=[device_code]}
     * @return GROUP BY expressions
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static List<String> buildGroupExpressions(TSDBQuery query) {
        List<String> groupExpressions = new ArrayList<>();
        if (query.hasAggregations() && !isBlank(query.getGroupByTime())) {
            // Use the complete expression so an application field named window_start cannot shadow the alias.
            groupExpressions.add(buildTimeWindowExpression(query));
        }
        if (query.hasAggregations()) {
            for (String tag : query.getGroupByTags()) {
                groupExpressions.add(quoteIdentifier(tag));
            }
        }
        return groupExpressions;
    }

    /**
     * Appends ordering by window_start for aggregates, or by time and composite cursor columns for details.
     *
     * @param query unified query model, for example {@code order=DESC}
     * @param sql   SQL being assembled, for example {@code new StringBuilder("SELECT * FROM \"ACCRUE\"")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void appendOrder(TSDBQuery query, StringBuilder sql) {
        if (query.hasAggregations()) {
            List<String> aggregateOrder = new ArrayList<>();
            String order = SortOrderEnum.normalize(query.getOrder()).name();
            if (!isBlank(query.getGroupByTime())) {
                aggregateOrder.add("window_start " + order);
            }
            for (String tag : query.getGroupByTags()) {
                aggregateOrder.add(quoteIdentifier(tag) + " " + order);
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
                    orderExpressions.add(formatCursorColumn(sortSpec.column()) + " " + sortSpec.order().name());
                }
                sql.append(" ORDER BY ").append(String.join(", ", orderExpressions));
                return;
            }
            if (query.getSortSpecs() != null && !query.getSortSpecs().isEmpty()) {
                List<String> orderExpressions = new ArrayList<>();
                for (SortSpec sortSpec : query.getSortSpecs()) {
                    orderExpressions.add(quoteIdentifier(sortSpec.column()) + " " + sortSpec.order().name());
                }
                sql.append(" ORDER BY ").append(String.join(", ", orderExpressions));
                return;
            }
            sql.append(" ORDER BY ").append(quoteIdentifier(query.getTimeColumn())).append(' ').append(order.name());
        }
    }

    /**
     * Resolves composite cursor columns, falling back to cursorValues order for direct adapter calls.
     *
     * @param query unified query model, for example {@code cursorColumns=[time, device_code]}
     * @return composite cursor columns
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static List<String> resolveCursorColumns(TSDBQuery query) {
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
     * Resolves each strict cursor column's sort direction, defaulting to the primary direction.
     * Rejects sort columns outside the cursor to keep WHERE conditions aligned with ORDER BY.
     *
     * @param query strict cursor query
     * @return sort specifications in cursor column order
     * @throws TSDBException if a sort specification contains a non-cursor column
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private static List<SortSpec> resolveCursorSortSpecs(TSDBQuery query) {
        List<String> cursorColumns = resolveCursorColumns(query);
        List<SortSpec> configuredSorts = query.getSortSpecs() == null
                ? Collections.emptyList() : query.getSortSpecs();
        if (hasStrictCursorValues(query)
                && !query.getCursorValues().keySet().equals(new java.util.LinkedHashSet<>(cursorColumns))) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Strict cursor values must exactly match the physical cursor columns: " + cursorColumns);
        }
        Map<String, SortOrderEnum> directions = new LinkedHashMap<>();
        for (SortSpec configuredSort : configuredSorts) {
            SortOrderEnum previous = directions.putIfAbsent(configuredSort.column(), configuredSort.order());
            if (previous != null && previous != configuredSort.order()) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Conflicting sort directions for physical column: " + configuredSort.column());
            }
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
                    ? new SortSpec(cursorColumn, fallbackOrder) : configured);
        }
        return resolved;
    }

    /**
     * Checks whether the cursor columns include a sort column.
     *
     * @param columns   cursor columns, for example {@code [time, device_code]}
     * @param candidate sort column name to locate
     * @return {@code true} if a physical column matches exactly
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private static boolean containsColumn(List<String> columns,
                                          String candidate) {
        for (String column : columns) {
            if (column.equals(candidate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds the sort specification for an exact physical column name.
     *
     * @param sortSpecs sort specifications to search
     * @param column    target column, for example {@code device_code}
     * @return matching specification, or {@code null} when absent
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-09-01
     */
    private static SortSpec findSortSpec(List<SortSpec> sortSpecs,
                                         String column) {
        for (SortSpec sortSpec : sortSpecs) {
            if (sortSpec.column().equals(column)) {
                return sortSpec;
            }
        }
        return null;
    }

    /**
     * Reads a composite cursor value by its exact physical column name.
     *
     * @param cursorValues cursor values, for example {@code Map.of("time", 1783000000000L)}
     * @param column       cursor column, for example {@code "device_code"}
     * @return cursor value
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static Object requireCursorValue(Map<String, Object> cursorValues, String column) {
        Object value = readCursorValue(cursorValues, column);
        if (value == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Missing strict cursor value: " + column);
        }
        return value;
    }

    /**
     * Reads a column value from the cursor map.
     *
     * @param cursorValues cursor values, for example {@code Map.of("device_code", "D001")}
     * @param column       cursor column, for example {@code "device_code"}
     * @return cursor value, or null when absent
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static Object readCursorValue(Map<String, Object> cursorValues, String column) {
        if (cursorValues == null || column == null) {
            return null;
        }
        return cursorValues.get(column);
    }

    /**
     * Converts a composite cursor column to an InfluxDB SQL identifier.
     *
     * @param column cursor column, for example {@code "time"}
     * @return SQL identifier
     * @throws TSDBException if the cursor column is blank
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static String formatCursorColumn(String column) {
        if (column == null || column.trim().isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Strict cursor column must not be empty");
        }
        String normalizedColumn = column.trim();
        if (isTimeColumn(normalizedColumn)) {
            return "time";
        }
        return quoteIdentifier(normalizedColumn);
    }

    /**
     * Converts a composite cursor value to an InfluxDB SQL literal.
     *
     * @param column cursor column, for example {@code "time"}
     * @param value  cursor value, for example {@code 1783000000000L}
     * @return SQL literal
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static String formatCursorSqlValue(String column, Object value) {
        if (isTimeColumn(column)) {
            Long epochMillis = parseCursorTimeMillis(value);
            if (epochMillis == null) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Invalid strict cursor time value: " + value);
            }
            return "timestamp " + sqlString(Instant.ofEpochMilli(epochMillis).toString());
        }
        return formatSqlValue(value);
    }

    /**
     * Converts a composite cursor time value to epoch milliseconds.
     *
     * @param value time value, for example {@code 1783000000000L} or {@code "2026-07-01T00:00:00Z"}
     * @return epoch milliseconds, or null for an absent or unsupported cursor value
     * @throws TSDBException if a numeric or textual value cannot be converted exactly
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static Long parseCursorTimeMillis(Object value) {
        if (value instanceof Number) {
            try {
                return exactLong((Number) value);
            } catch (NumberFormatException | ArithmeticException e) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Invalid or out-of-range strict cursor time value: " + value, e);
            }
        }
        if (value instanceof Instant) {
            try {
                return ((Instant) value).toEpochMilli();
            } catch (ArithmeticException e) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "Out-of-range strict cursor time value: " + value, e);
            }
        }
        if (value instanceof CharSequence) {
            String text = value.toString().trim();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException ignored) {
                try {
                    return parseTimeMillis(text);
                } catch (TSDBException e) {
                    throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                            "Invalid or out-of-range strict cursor time value: " + value, e);
                }
            }
        }
        return null;
    }

    /**
     * Determines whether a column is the time column.
     *
     * @param column column name, for example {@code "time"}
     * @return whether this is the time column
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static boolean isTimeColumn(String column) {
        return column != null && "time".equals(column);
    }

    /**
     * Adds a physical column once while preserving case-distinct cursor columns.
     *
     * @param columns destination columns, for example {@code List.of("time")}
     * @param column  column to add, for example {@code "device_code"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static void addColumnIfAbsent(List<String> columns, String column) {
        if (isBlank(column)) {
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
     * Translates a unified filter to an InfluxDB WHERE fragment.
     *
     * @param filter unified filter, for example {@code new QueryFilter("device_code", OperatorEnum.EQ, List.of("D001"))}
     * @return InfluxDB WHERE fragment, for example {@code "\"device_code\" = 'D001'"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String buildFilterSql(QueryFilter filter) {
        if (filter == null || isBlank(filter.column()) || filter.operator() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "Invalid query filter");
        }
        List<Object> values = filter.values();
        if (values == null || values.isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Filter values must not be empty: " + filter.column());
        }
        String column = quoteIdentifier(filter.column());
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
                            "BETWEEN filter requires two values: " + filter.column());
                }
                yield column + " BETWEEN " + formatSqlValue(values.get(0)) + " AND " + formatSqlValue(values.get(1));
            }
        };
    }

    /**
     * Builds a fixed-duration date_bin expression or explicit civil calendar boundaries.
     *
     * @param query query carrying the window, zone, time column, and bounded time range
     * @return time window SQL expression
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String buildTimeWindowExpression(TSDBQuery query) {
        TimeWindowPlan plan = TimeWindowPlan.resolve(query.getGroupByTime(), query.getTimeZone(),
                query.getStartTime(), query.getEndTime());
        String column = quoteIdentifier(query.getTimeColumn());
        if (!plan.isCalendar()) {
            return "date_bin(interval '" + toSqlInterval(query.getGroupByTime()) + "', " + column
                    + ", timestamp '" + plan.fixedOrigin() + "')";
        }
        StringBuilder expression = new StringBuilder("CASE");
        for (TimeWindowPlan.Bucket bucket : plan.buckets()) {
            String start = "timestamp " + sqlString(Instant.ofEpochMilli(bucket.startInclusive()).toString());
            String end = "timestamp " + sqlString(Instant.ofEpochMilli(bucket.endExclusive()).toString());
            expression.append(" WHEN ").append(column).append(" >= ").append(start)
                    .append(" AND ").append(column).append(" < ").append(end)
                    .append(" THEN ").append(start);
        }
        return expression.append(" ELSE NULL END").toString();
    }

    /**
     * Translates a unified aggregate function to an InfluxDB SQL aggregate expression.
     *
     * @param function   aggregate function, for example {@code AggregationFunctionEnum.AVG}
     * @param field      aggregate field, for example {@code "value"}
     * @param timeColumn physical time column used to order FIRST/LAST
     * @return InfluxDB aggregate expression, for example {@code "AVG(\"value\")"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String toSqlAggregation(AggregationFunctionEnum function, String field, String timeColumn) {
        String valueColumn = quoteIdentifier(field);
        AggregationFunctionEnum resolved = function == null ? AggregationFunctionEnum.AVG : function;
        return switch (resolved) {
            case COUNT -> "COUNT(" + valueColumn + ")";
            case SUM -> "SUM(" + valueColumn + ")";
            case MIN -> "MIN(" + valueColumn + ")";
            case MAX -> "MAX(" + valueColumn + ")";
            case FIRST -> "FIRST_VALUE(" + valueColumn + " ORDER BY " + quoteIdentifier(timeColumn) + ")";
            case LAST -> "LAST_VALUE(" + valueColumn + " ORDER BY " + quoteIdentifier(timeColumn) + ")";
            default -> "AVG(" + valueColumn + ")";
        };
    }

    /**
     * Converts a unified write record to InfluxDB line protocol.
     *
     * @param tsdbRecord unified record, for example {@code new TSDBRecord("ACCRUE", 1783000000000L, tags, fields)}
     * @return one line of line protocol
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String buildLineProtocol(TSDBRecord tsdbRecord) {
        validateRecord(tsdbRecord);
        StringBuilder line = new StringBuilder();
        line.append(escapeLineProtocolMeasurement(tsdbRecord.measurement()));
        for (Map.Entry<String, String> entry : tsdbRecord.tags().entrySet()) {
            appendTag(line, entry.getKey(), entry.getValue());
        }
        line.append(' ');
        List<String> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : tsdbRecord.fields().entrySet()) {
            if (entry.getValue() != null) {
                fields.add(escapeLineProtocolKey(entry.getKey()) + "=" + formatLineProtocolFieldValue(entry.getValue()));
            }
        }
        if (fields.isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "At least one field value must be non-null");
        }
        line.append(String.join(",", fields));
        line.append(' ').append(tsdbRecord.timestamp());
        return line.toString();
    }

    /**
     * Validates a record against InfluxDB line protocol requirements.
     *
     * @param tsdbRecord unified record, for example {@code new TSDBRecord("ACCRUE", 1783000000000L, tags, fields)}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void validateRecord(TSDBRecord tsdbRecord) {
        if (tsdbRecord == null || isBlank(tsdbRecord.measurement())) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "measurement must not be empty for InfluxDB write");
        }
        if (tsdbRecord.measurement().startsWith("#")) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB measurement must not start with # because line protocol treats it as a comment");
        }
        if (tsdbRecord.fields() == null || tsdbRecord.fields().isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "fields must not be empty for InfluxDB write");
        }
        if (tsdbRecord.timestamp() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "timestamp must not be null for InfluxDB write");
        }
        validateLineProtocolComponent(tsdbRecord.measurement(), "measurement");
        if (tsdbRecord.tags() != null) {
            for (Map.Entry<String, String> entry : tsdbRecord.tags().entrySet()) {
                validateOptionalLineProtocolComponent(entry.getKey(), "tag key");
                validateOptionalLineProtocolComponent(entry.getValue(), "tag value");
            }
        }
        for (Map.Entry<String, Object> entry : tsdbRecord.fields().entrySet()) {
            validateLineProtocolComponent(entry.getKey(), "field key");
            Object value = entry.getValue();
            if (value != null && !(value instanceof Number) && !(value instanceof Boolean)) {
                validateLineProtocolComponent(String.valueOf(value), "field value");
            }
        }
    }

    /**
     * Validates the business batch size before sending any HTTP requests.
     *
     * @param records records to write, for example {@code List.of(record1, record2)}
     * @throws TSDBException if the record count exceeds {@code tsdb.influxdb.max-batch-records}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private void validateBatchSize(Collection<TSDBRecord> records) {
        if (records.size() > maxBatchRecords) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB batch write record count exceeds tsdb.influxdb.max-batch-records: "
                            + records.size() + " > " + maxBatchRecords);
        }
    }

    /**
     * Reads the maximum record count for a single business batch.
     *
     * @param config InfluxDB settings, for example {@code maxBatchRecords=10000}
     * @return maximum records per business batch, for example {@code 10000}
     * @throws TSDBException if the configuration is null or {@code maxBatchRecords <= 0}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private static int resolveMaxBatchRecords(InfluxDBProperties config) {
        if (config == null || config.getMaxBatchRecords() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.influxdb.max-batch-records must be greater than 0");
        }
        return config.getMaxBatchRecords();
    }

    /**
     * Validates a required line protocol component, rejecting control characters and excessive UTF-8 size.
     *
     * @param value         component value, for example {@code "device_001"}
     * @param componentName component name, for example {@code "tag value"}
     * @throws TSDBException if null, containing ISO control characters, or exceeding the UTF-8 byte limit
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private static void validateLineProtocolComponent(String value,
                                                      String componentName) {
        if (value == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB line protocol " + componentName + " must not be null");
        }
        if (value.codePoints().anyMatch(Character::isISOControl)) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB line protocol " + componentName + " must not contain control characters");
        }
        int bytes = utf8Length(value);
        if (bytes > LINE_PROTOCOL_COMPONENT_MAX_BYTES) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB line protocol " + componentName
                            + " exceeds maximum UTF-8 bytes: " + bytes + " > " + LINE_PROTOCOL_COMPONENT_MAX_BYTES);
        }
    }

    /**
     * Validates optional tag components; non-null values must satisfy the same safety constraints.
     *
     * @param value         component value, for example {@code "device_001"}; null is ignored
     * @param componentName component name, for example {@code "tag value"}
     * @throws TSDBException if a non-null component contains ISO control characters or exceeds the UTF-8 byte limit
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private static void validateOptionalLineProtocolComponent(String value,
                                                              String componentName) {
        if (value != null) {
            validateLineProtocolComponent(value, componentName);
        }
    }

    /**
     * Computes the actual UTF-8 byte length of a string.
     *
     * @param value string value, for example {@code "device"}
     * @return UTF-8 byte length; ASCII "device" requires {@code 6} bytes
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-10
     */
    private static int utf8Length(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    /**
     * Appends a line protocol tag, skipping blank keys or values.
     *
     * @param line  line protocol being assembled, for example {@code new StringBuilder("ACCRUE")}
     * @param key   tag key, for example {@code "device_code"}
     * @param value tag value, for example {@code "device001"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void appendTag(StringBuilder line, String key, String value) {
        if (isBlank(key) || isBlank(value)) {
            return;
        }
        line.append(',')
                .append(escapeLineProtocolKey(key))
                .append('=')
                .append(escapeLineProtocolTagValue(value));
    }

    /**
     * Normalizes InfluxDB time values to epoch milliseconds and provides _time for POJO mapping and pagination.
     *
     * @param rows InfluxDB result rows, for example {@code List.of(Map.of("time", "2026-07-03T00:00:00Z"))}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void normalizeRows(List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            Object time = row.get("time");
            Long epochMillis = parseTimeMillis(time);
            if (epochMillis != null) {
                row.put("time", epochMillis);
                // Preserve a physical field or native alias named _time, including an explicit null.
                if (!row.containsKey("_time")) {
                    row.put("_time", epochMillis);
                }
            }
        }
    }

    /**
     * Joins SQL literals for an IN condition.
     *
     * @param values IN values, for example {@code List.of("beijing", "shanghai")}
     * @return SQL literal list, for example {@code "'beijing', 'shanghai'"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String joinSqlValues(List<Object> values) {
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
     * Converts a Java value to an InfluxDB SQL literal.
     *
     * @param value Java value, for example {@code "device001"} or {@code 12.34D}
     * @return InfluxDB SQL literal
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String formatSqlValue(Object value) {
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Instant) {
            return "timestamp " + sqlString(((Instant) value).toString());
        }
        return sqlString(String.valueOf(value));
    }

    /**
     * Converts a Java value to a line protocol field value.
     * BigInteger values must fit a signed long and are written as integers without floating-point conversion.
     * BigDecimal values use IEEE 754 double precision and may round; overflow and nonzero underflow are rejected.
     *
     * @param value Java field value, for example {@code 12.34D} or {@code "normal"}
     * @return line protocol field value
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String formatLineProtocolFieldValue(Object value) {
        if (value instanceof BigInteger integer) {
            try {
                return integer.longValueExact() + "i";
            } catch (ArithmeticException exception) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "InfluxDB integer field must fit a signed 64-bit integer: " + integer, exception);
            }
        }
        if (value instanceof BigDecimal decimal) {
            double converted = decimal.doubleValue();
            if (!Double.isFinite(converted) || (converted == 0d && decimal.signum() != 0)) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "InfluxDB decimal field overflows or underflows double precision: " + decimal);
            }
            return Double.toString(converted);
        }
        if (value instanceof Float || value instanceof Double) {
            if (!Double.isFinite(((Number) value).doubleValue())) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "InfluxDB floating-point field must be finite: " + value);
            }
            return value.toString();
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return value + "i";
        }
        if (value instanceof Number) {
            throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                    "Unsupported InfluxDB numeric field type: " + value.getClass().getName());
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + String.valueOf(value).replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /**
     * Appends a WHERE clause when filter conditions exist.
     *
     * @param sql     SQL being assembled, for example {@code new StringBuilder("SELECT * FROM \"ACCRUE\"")}
     * @param clauses WHERE fragments, for example {@code List.of("\"device_code\" = 'D001'")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void appendWhere(StringBuilder sql, List<String> clauses) {
        if (clauses != null && !clauses.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", clauses));
        }
    }

    /**
     * Resolves the effective database, using the configured default when callers omit it.
     *
     * @param database explicitly requested database, for example {@code "tsdb"}
     * @return effective database name
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private String resolveDatabase(String database) {
        if (!isBlank(database)) {
            return database.trim();
        }
        if (!isBlank(config.getDatabase())) {
            return config.getDatabase().trim();
        }
        return "tsdb";
    }

    /**
     * Serializes an object as a JSON request body.
     *
     * @param value object to serialize, for example {@code Map.of("db", "tsdb", "q", "SELECT 1")}
     * @return JSON request body
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private RequestBody jsonBody(Object value) {
        try {
            return RequestBody.create(objectMapper.writeValueAsString(value), JSON);
        } catch (IOException e) {
            throw new TSDBException(TSDBErrorCodeEnum.INTERNAL_ERROR, "Failed to serialize JSON body", e);
        }
    }

    /**
     * Builds an InfluxDB v3 API URL.
     *
     * @param endpoint API endpoint, for example {@code "query_sql"}
     * @return HTTP URL builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private HttpUrl.Builder apiUrl(String endpoint) {
        return apiRootUrl("api").addPathSegment("v3").addPathSegment(endpoint);
    }

    /**
     * Builds an API root path from the configured base URL.
     *
     * @param firstPath first path segment, for example {@code "api"} or {@code "ping"}
     * @return HTTP URL builder
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private HttpUrl.Builder apiRootUrl(String firstPath) {
        HttpUrl baseUrl = HttpUrl.parse(config.getUrl());
        if (baseUrl == null) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Invalid InfluxDB url: " + config.getUrl());
        }
        return baseUrl.newBuilder().addPathSegment(firstPath);
    }

    /**
     * Adds an Authorization header when a token is configured.
     *
     * @param builder OkHttp request builder, for example {@code new Request.Builder().url(url)}
     * @return request builder with authentication applied
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private Request.Builder authorize(Request.Builder builder) {
        if (!isBlank(config.getToken())) {
            builder.header("Authorization", "Bearer " + config.getToken().trim());
        }
        return builder;
    }

    /**
     * Reads the InfluxDB JSON array response.
     *
     * @param response HTTP response, for example {@code client.newCall(request).execute()}
     * @return result rows from the response
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private List<Map<String, Object>> readRows(Response response, int rowLimit) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        if (response.code() == 204 || response.body() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                    "InfluxDB query response must contain a JSON array, including [] for no rows");
        }
        if (response.body().contentLength() > maxQueryResponseBytes) {
            throw responseLimitExceeded("response bytes", maxQueryResponseBytes);
        }
        // Bound decompressed bytes even for chunked or compressed responses with no content length.
        try (InputStream input = new BoundedResponseInputStream(response.body().byteStream(), maxQueryResponseBytes);
             JsonParser parser = objectMapper.getFactory().createParser(input)) {
            // Reject ambiguous columns before a Map can silently overwrite their earlier values.
            parser.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            JsonToken token = parser.nextToken();
            if (token == null) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                        "InfluxDB query response must contain a JSON array, including [] for no rows");
            }
            if (token != JsonToken.START_ARRAY) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                        "InfluxDB query response must be a JSON array of objects");
            }
            while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                if (token != JsonToken.START_OBJECT) {
                    throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                            "InfluxDB query response contains an invalid or incomplete row");
                }
                if (rows.size() >= rowLimit) {
                    throw responseLimitExceeded("rows", rowLimit);
                }
                rows.add(objectMapper.readValue(parser, ROW_TYPE));
            }
            if (parser.nextToken() != null) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                        "InfluxDB query response contains trailing JSON content");
            }
        }
        return rows;
    }

    /**
     * Reads the response body as text, returning an empty string for a missing body.
     *
     * @param response HTTP response, for example {@code client.newCall(request).execute()}
     * @return response text
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String readBody(Response response) throws IOException {
        // Error diagnostics are bounded independently from successful query result limits.
        return response.body() == null ? "" : response.peekBody(8 * 1024L).string();
    }

    private static TSDBException responseLimitExceeded(String kind, long limit) {
        return new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                "InfluxDB query exceeded maximum " + kind + ": " + limit
                        + "; add SQL filters or pagination, or increase the configured query limit");
    }

    /**
     * Enforces the byte budget before Jackson can materialize an oversized value or row.
     */
    private static final class BoundedResponseInputStream extends FilterInputStream {
        private final long limit;
        private long remaining;

        private BoundedResponseInputStream(InputStream source, long limit) {
            super(source);
            this.limit = limit;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value != -1) {
                consume(1);
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            int allowed = (int) Math.min(length, Math.min(remaining, Integer.MAX_VALUE - 1L) + 1L);
            int count = in.read(buffer, offset, allowed);
            if (count > 0) {
                consume(count);
            }
            return count;
        }

        private void consume(int count) {
            if (count > remaining) {
                throw responseLimitExceeded("response bytes", limit);
            }
            remaining -= count;
        }
    }

    /**
     * Escapes a SQL identifier using double quotes.
     *
     * @param identifier SQL identifier, for example {@code "device_code"}
     * @return quoted SQL identifier, for example {@code "\"device_code\""}
     * @throws TSDBException if the SQL identifier is blank
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB SQL identifier must not be empty");
        }
        String normalizedIdentifier = identifier.trim();
        return "\"" + normalizedIdentifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Escapes a SQL string literal using single quotes.
     *
     * @param value unescaped string, for example {@code "Alan's device"}
     * @return SQL string literal
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String sqlString(String value) {
        return "'" + String.valueOf(value).replace("'", "''") + "'";
    }

    /**
     * Escapes an InfluxDB 3 measurement. Equals signs are literal in measurement names.
     *
     * @param value unescaped measurement name
     * @return escaped measurement name
     */
    private static String escapeLineProtocolMeasurement(String value) {
        return value.replace("\\", "\\\\")
                .replace(" ", "\\ ")
                .replace(",", "\\,");
    }

    /**
     * Escapes a line protocol tag key or field key.
     *
     * @param value unescaped key, for example {@code "device code"}
     * @return escaped line protocol key
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String escapeLineProtocolKey(String value) {
        return value.replace("\\", "\\\\")
                .replace(" ", "\\ ")
                .replace(",", "\\,")
                .replace("=", "\\=");
    }

    /**
     * Escapes a line protocol tag value.
     *
     * @param value unescaped tag value, for example {@code "device,001"}
     * @return escaped tag value
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String escapeLineProtocolTagValue(String value) {
        return escapeLineProtocolKey(value);
    }

    /**
     * Parses an InfluxDB time value as epoch milliseconds.
     * <p>InfluxDB JSON may omit the UTC suffix; such values are explicitly parsed as UTC, never the JVM default zone.</p>
     *
     * @param value time value, for example {@code "2026-07-03T00:00:00Z"} or {@code 1783000000000L}
     * @return epoch milliseconds, or null only when the source value is null
     * @throws TSDBException if the value is invalid, fractional, or outside the long range
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static Long parseTimeMillis(Object value) {
        if (value instanceof Number) {
            try {
                return exactLong((Number) value);
            } catch (NumberFormatException | ArithmeticException e) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                        "InfluxDB returned an invalid or out-of-range time value: " + value, e);
            }
        }
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        try {
            return Instant.parse(text).toEpochMilli();
        } catch (DateTimeParseException | ArithmeticException ignored) {
            try {
                return Instant.parse(text + "Z").toEpochMilli();
            } catch (DateTimeParseException | ArithmeticException e) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                        "InfluxDB returned an invalid or out-of-range time value: " + value, e);
            }
        }
    }

    /**
     * Converts the actual binary value of Float/Double or the exact decimal value of other numbers.
     */
    private static long exactLong(Number number) {
        BigDecimal decimal = number instanceof Float || number instanceof Double
                ? new BigDecimal(number.doubleValue()) : new BigDecimal(number.toString());
        return decimal.longValueExact();
    }

    /**
     * Converts a short window notation to an InfluxDB interval literal.
     *
     * @param window time window, for example {@code "5m"} or {@code "300000"}
     * @return InfluxDB interval text, for example {@code "5 minutes"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String toSqlInterval(String window) {
        return TimeWindowDuration.parse(window).toSqlInterval();
    }

    /**
     * Checks whether a string is blank.
     *
     * @param value string to check, for example {@code "ACCRUE"}
     * @return whether the string is blank
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    /**
     * Validates that unified time metadata maps to InfluxDB's physical {@code time} column.
     *
     * @param query unified query model
     */
    private static void requireNativeTimeColumn(TSDBQuery query) {
        if (!"time".equals(query.getTimeColumn())) {
            throw new TSDBException(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                    "InfluxDB uses the fixed physical time column 'time'; unsupported @TGTime column: "
                            + query.getTimeColumn());
        }
    }

    /**
     * Maps an InfluxDB HTTP status to a stable adapter error code.
     *
     * @param statusCode HTTP status
     * @param fallback   default error classification for this operation
     * @return public TSDB error code
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private static TSDBErrorCodeEnum errorCodeForHttpStatus(int statusCode,
                                                            TSDBErrorCodeEnum fallback) {
        if (statusCode == 401 || statusCode == 403) {
            return TSDBErrorCodeEnum.PERMISSION_ERROR;
        }
        if (statusCode == 404) {
            return TSDBErrorCodeEnum.RESOURCE_NOT_FOUND;
        }
        if (statusCode == 405 || statusCode == 501) {
            return TSDBErrorCodeEnum.UNSUPPORTED_OPERATION;
        }
        if (statusCode == 408 || statusCode == 429 || statusCode >= 500) {
            return TSDBErrorCodeEnum.CONNECTION_ERROR;
        }
        return fallback == null ? TSDBErrorCodeEnum.INTERNAL_ERROR : fallback;
    }

    /**
     * One line protocol batch prepared before network I/O.
     *
     * @param payload     line protocol text
     * @param recordCount batch record count
     * @param measurement first record's measurement, used to identify failures
     */
    private record PreparedWriteBatch(String payload, int recordCount, String measurement) {
    }

    /**
     * Write exception for an explicit unsuccessful InfluxDB HTTP response.
     */
    private static final class InfluxWriteHttpException extends RuntimeException {

        private final int statusCode;

        /**
         * Creates an exception containing the HTTP status and response body.
         *
         * @param statusCode   HTTP status
         * @param responseBody response body
         */
        private InfluxWriteHttpException(int statusCode, String responseBody) {
            super("InfluxDB write failed with HTTP " + statusCode + ": " + responseBody);
            this.statusCode = statusCode;
        }

        /**
         * Returns the write response status for conversion to a unified adapter error code.
         *
         * @return InfluxDB HTTP response status
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-07-21
         */
        private int getStatusCode() {
            return statusCode;
        }
    }
}
