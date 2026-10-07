package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.config.InfluxDB1Properties;
import com.alandevise.tsgate.config.InfluxDB1HttpClientProperties;
import com.alandevise.tsgate.exception.*;
import com.alandevise.tsgate.model.*;
import com.alandevise.tsgate.metadata.DefaultTSDBMetadataResolver;
import com.alandevise.tsgate.util.TimeWindowDuration;
import com.alandevise.tsgate.util.TimeWindowPlan;
import com.alandevise.tsgate.util.TSDBQueryValidator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * InfluxDB OSS 1.x adapter using line protocol writes and bounded streaming InfluxQL responses.
 * Databases and retention policies must already exist. Native clients are borrowed from this
 * adapter; callers must coordinate direct native operations with adapter shutdown.
 */
@Slf4j
public class InfluxDB1Adapter implements TSDBAdapter {
    private static final MediaType LINE_PROTOCOL = MediaType.parse("text/plain; charset=utf-8");
    private static final int WRITE_BATCH_MAX_LINES = 5_000;
    private static final int WRITE_BATCH_MAX_BYTES = 1024 * 1024;
    private static final int WRITE_LINE_MAX_BYTES = WRITE_BATCH_MAX_BYTES;
    private static final int LINE_PROTOCOL_COMPONENT_MAX_BYTES = 64 * 1024;
    private static final TypeReference<List<String>> COLUMN_TYPE = new TypeReference<>() {
    };
    private static final TypeReference<List<Object>> VALUES_TYPE = new TypeReference<>() {
    };
    private static final TypeReference<Map<String, String>> TAG_TYPE = new TypeReference<>() {
    };
    private static final DefaultTSDBMetadataResolver PROJECTION_METADATA = new DefaultTSDBMetadataResolver();
    private final InfluxDB1Properties config;
    private final InfluxDB1HttpClientProperties httpConfig;
    private final int maxBatchRecords;
    private final long maxBatchBytes;
    private final int maxQueryRows;
    private final long maxQueryResponseBytes;
    private final boolean queryLogEnabled;
    private final ObjectMapper objectMapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
    private LifecycleState state = LifecycleState.NEW;
    private OkHttpClient client;
    private InfluxDB nativeClient;

    private enum LifecycleState {NEW, READY, CLOSED}

    /**
     * Creates an uninitialized adapter with default HTTP settings and debug query logging enabled.
     *
     * @param config connection settings and operation limits
     * @throws TSDBException if the configuration is invalid
     */
    public InfluxDB1Adapter(InfluxDB1Properties config) {
        this(config, new InfluxDB1HttpClientProperties(), true);
    }

    /**
     * Validates settings without creating clients or contacting the server; call {@link #init()} before use.
     * Connection and HTTP settings are copied at construction; later property changes do not reconfigure this instance.
     *
     * @param config connection settings and operation limits
     * @param httpConfig HTTP pool and timeout settings
     * @param queryLogEnabled whether generated InfluxQL is logged at debug level
     * @throws TSDBException if the configuration is invalid
     */
    public InfluxDB1Adapter(InfluxDB1Properties config, InfluxDB1HttpClientProperties httpConfig, boolean queryLogEnabled) {
        config = snapshotConfiguration(config);
        httpConfig = snapshotHttpConfiguration(httpConfig);
        this.config = config;
        if (httpConfig == null || httpConfig.getMaxIdleConnections() < 0 || httpConfig.getKeepAliveDurationMs() <= 0
                || httpConfig.getConnectTimeoutMs() < 0 || httpConfig.getReadTimeoutMs() < 0
                || httpConfig.getWriteTimeoutMs() < 0 || httpConfig.getCallTimeoutMs() < 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR, "Invalid InfluxDB1 HTTP pool or timeout settings");
        }
        this.httpConfig = httpConfig;
        this.maxBatchRecords = resolveMaxBatchRecords(config);
        if (config.getMaxBatchBytes() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.influxdb1.max-batch-bytes must be greater than 0");
        }
        this.maxBatchBytes = config.getMaxBatchBytes();
        if (isBlank(config.getUrl()) || HttpUrl.parse(config.getUrl()) == null || isBlank(config.getDatabase())) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR, "InfluxDB1 url and database must be valid and nonempty");
        }
        if (config.getMaxQueryRows() <= 0 || config.getMaxQueryRows() == Integer.MAX_VALUE || config.getMaxQueryResponseBytes() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR, "InfluxDB1 query limits must be positive; max-query-rows must be below 2147483647");
        }
        if (isBlank(config.getUsername()) && !isBlank(config.getPassword())) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR, "InfluxDB1 password requires a username");
        }
        this.maxQueryRows = config.getMaxQueryRows();
        this.maxQueryResponseBytes = config.getMaxQueryResponseBytes();
        this.queryLogEnabled = queryLogEnabled;
    }

    /** Copies connection settings while preserving nulls for the existing validation path. */
    private static InfluxDB1Properties snapshotConfiguration(InfluxDB1Properties source) {
        if (source == null) {
            return null;
        }
        InfluxDB1Properties copy = new InfluxDB1Properties();
        copy.setEnable(source.isEnable());
        copy.setFailFast(source.isFailFast());
        copy.setUrl(source.getUrl());
        copy.setDatabase(source.getDatabase());
        copy.setUsername(source.getUsername());
        copy.setPassword(source.getPassword());
        copy.setRetentionPolicy(source.getRetentionPolicy());
        copy.setMaxBatchRecords(source.getMaxBatchRecords());
        copy.setMaxBatchBytes(source.getMaxBatchBytes());
        copy.setMaxQueryRows(source.getMaxQueryRows());
        copy.setMaxQueryResponseBytes(source.getMaxQueryResponseBytes());
        return copy;
    }

    /** Copies the separately supplied HTTP configuration before either client is created. */
    private static InfluxDB1HttpClientProperties snapshotHttpConfiguration(InfluxDB1HttpClientProperties source) {
        if (source == null) {
            return null;
        }
        InfluxDB1HttpClientProperties copy = new InfluxDB1HttpClientProperties();
        copy.setMaxIdleConnections(source.getMaxIdleConnections());
        copy.setKeepAliveDurationMs(source.getKeepAliveDurationMs());
        copy.setConnectTimeoutMs(source.getConnectTimeoutMs());
        copy.setReadTimeoutMs(source.getReadTimeoutMs());
        copy.setWriteTimeoutMs(source.getWriteTimeoutMs());
        copy.setCallTimeoutMs(source.getCallTimeoutMs());
        copy.setRetryOnConnectionFailure(source.isRetryOnConnectionFailure());
        return copy;
    }

    /**
     * Creates local clients without checking server connectivity or creating a database.
     * Repeated initialization is a no-op; failed initialization can retry until the adapter is closed.
     *
     * @throws TSDBException if this adapter is closed or client creation fails
     */
    @Override
    public void init() {
        lifecycleLock.writeLock().lock();
        OkHttpClient createdHttp = null;
        InfluxDB createdNative = null;
        try {
            if (state == LifecycleState.READY) return;
            if (state == LifecycleState.CLOSED) throw stateError();
            createdHttp = httpBuilder().build();
            createdNative = isBlank(config.getUsername())
                    ? InfluxDBFactory.connect(config.getUrl(), httpBuilder())
                    : InfluxDBFactory.connect(config.getUrl(), config.getUsername(), config.getPassword(), httpBuilder());
            createdNative.setDatabase(config.getDatabase());
            if (!isBlank(config.getRetentionPolicy())) createdNative.setRetentionPolicy(config.getRetentionPolicy());
            client = createdHttp;
            nativeClient = createdNative;
            state = LifecycleState.READY;
        } catch (RuntimeException | Error e) {
            closeResources(createdNative, createdHttp);
            if (e instanceof TSDBException || e instanceof Error) throw e;
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR, "Failed to initialize InfluxDB1 resources", e);
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    /**
     * Waits for adapter operations to finish and permanently closes this instance; repeated calls are harmless.
     * Direct operations on the borrowed native client must be coordinated separately by the caller.
     */
    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (state == LifecycleState.CLOSED) return;
            state = LifecycleState.CLOSED;
            InfluxDB oldNative = nativeClient;
            OkHttpClient oldHttp = client;
            nativeClient = null;
            client = null;
            closeResources(oldNative, oldHttp);
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private static void closeResources(InfluxDB nativeResource, OkHttpClient httpResource) {
        try {
            if (nativeResource != null) {
                try {
                    nativeResource.close();
                } catch (RuntimeException e) {
                    log.warn("Failed to close InfluxDB1 native client", e);
                }
            }
        } finally {
            if (httpResource != null) {
                try {
                    httpResource.dispatcher().executorService().shutdown();
                } finally {
                    httpResource.connectionPool().evictAll();
                }
            }
        }
    }

    /**
     * Returns the adapter-owned client for operations outside the common API.
     * Callers must not close it; native operations bypass adapter limits and lifecycle locking.
     *
     * @return the borrowed, initialized native client
     * @throws TSDBException if this adapter is not initialized or is closed
     */
    public InfluxDB getNativeClient() {
        lifecycleLock.readLock().lock();
        try {
            requireReady();
            return nativeClient;
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private void requireReady() {
        if (state != LifecycleState.READY) throw stateError();
    }

    private TSDBException stateError() {
        return new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                state == LifecycleState.CLOSED ? "InfluxDB1 adapter is closed" : "InfluxDB1 adapter is not initialized");
    }

    /**
     * Returns the backend name used in diagnostics.
     *
     * @return {@code InfluxDB1}
     */
    @Override
    public String getAdapterName() {
        return "InfluxDB1";
    }

    /**
     * Returns the input record limit for one batch operation, before HTTP request splitting.
     *
     * @return configured maximum batch records
     */
    @Override
    public int getMaxBatchRecords() {
        return maxBatchRecords;
    }

    private OkHttpClient.Builder httpBuilder() {
        return new OkHttpClient.Builder().connectionPool(new ConnectionPool(httpConfig.getMaxIdleConnections(),
                        httpConfig.getKeepAliveDurationMs(), TimeUnit.MILLISECONDS))
                .connectTimeout(httpConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(httpConfig.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(httpConfig.getWriteTimeoutMs(), TimeUnit.MILLISECONDS)
                .callTimeout(httpConfig.getCallTimeoutMs(), TimeUnit.MILLISECONDS)
                .followRedirects(false)
                .followSslRedirects(false)
                .retryOnConnectionFailure(httpConfig.isRetryOnConnectionFailure());
    }

    /**
     * Executes one read-only InfluxQL statement against the configured default database.
     * Supports SELECT, SHOW, and EXPLAIN [ANALYZE] SELECT subject to server support and lexical validation.
     * Row and decompressed-response limits apply; partial or oversized results fail rather than truncate.
     *
     * @param sql a single supported read-only statement
     * @return bounded query rows with timestamps normalized to epoch milliseconds
     * @throws TSDBException if SQL is unsupported, execution fails, or a result limit is exceeded
     */
    @Override
    public QueryResult executeQuery(String sql) {
        if (isBlank(sql)) throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "InfluxQL must not be empty");
        validateReadOnlyNativeQuery(sql);
        return execute(resolveDatabase(null), sql.trim(), maxQueryRows, false).result();
    }

    /**
     * Validates the supported read-only lexical subset before sending SQL to the server.
     * InfluxDB 1.x executes mutations even for GET requests marked as read-only.
     * Slash syntax is intentionally unsupported because regex literals and division require parser context.
     */
    private static void validateReadOnlyNativeQuery(String sql) {
        List<String> words = new ArrayList<>(3);
        boolean terminated = false;
        for (int index = 0; index < sql.length(); ) {
            char current = sql.charAt(index);
            if (Character.isWhitespace(current)) {
                index++;
                continue;
            }
            if (terminated)
                throw unsupported("InfluxDB1 native queries accept only one statement and an optional trailing semicolon");
            if (current == ';') {
                terminated = true;
                index++;
                continue;
            }
            if (current == '/' || current == '`' || current == '-' && index + 1 < sql.length() && sql.charAt(index + 1) == '-')
                throw unsupported("InfluxDB1 native query slash syntax, comments, and backtick quoting are unsupported; use the borrowed native client for this syntax");
            if (current == '\'' || current == '"') {
                if (words.isEmpty())
                    throw unsupported("InfluxDB1 native queries must begin with SELECT, SHOW, or EXPLAIN SELECT");
                char quote = current;
                boolean closed = false;
                index++;
                while (index < sql.length()) {
                    char character = sql.charAt(index++);
                    if (character == '\\') {
                        if (index >= sql.length()) throw unsupported("Unterminated escape in InfluxDB1 native query");
                        index++;
                    } else if (character == quote) {
                        closed = true;
                        break;
                    }
                }
                if (!closed) throw unsupported("Unterminated quoted value in InfluxDB1 native query");
                continue;
            }
            if (Character.isLetterOrDigit(current) || current == '_') {
                int start = index++;
                while (index < sql.length() && (Character.isLetterOrDigit(sql.charAt(index)) || sql.charAt(index) == '_'))
                    index++;
                String word = sql.substring(start, index).toUpperCase(Locale.ROOT);
                if ("INTO".equals(word)) throw unsupported("InfluxDB1 native queries must not contain SELECT INTO");
                if (words.size() < 3) words.add(word);
                continue;
            }
            if (words.isEmpty())
                throw unsupported("InfluxDB1 native queries must begin with SELECT, SHOW, or EXPLAIN SELECT");
            index++;
        }
        if (words.isEmpty()) throw unsupported("InfluxDB1 native queries must contain a read-only statement");
        String first = words.get(0);
        if ("SELECT".equals(first) || "SHOW".equals(first)) return;
        if ("EXPLAIN".equals(first) && words.size() >= 2
                && ("SELECT".equals(words.get(1)) || "ANALYZE".equals(words.get(1)) && words.size() >= 3 && "SELECT".equals(words.get(2))))
            return;
        throw unsupported("InfluxDB1 native queries support only SELECT, SHOW, and EXPLAIN [ANALYZE] SELECT; use the borrowed native client for administrative operations");
    }

    /**
     * Executes the common query model with bounded results and time-only ordering.
     * Strict composite cursors are unsupported; time windows require explicit time bounds.
     *
     * @param database target database; null or blank selects the configured default
     * @param query common query model; pagination probes may read one row beyond the configured cap
     * @return query rows, with global ordering and slicing for grouped aggregations
     * @throws TSDBException if the query is invalid or unsupported, execution fails, or a limit is exceeded
     */
    @Override
    public QueryResult query(String database, TSDBQuery query) {
        validateQuery(query);
        int cap = maxQueryRows + (query.isPaginationProbe() ? 1 : 0);
        String sql = buildQuery(query, true);
        validateAggregationOutputNames(query);
        QueryResult result = execute(resolveDatabase(database), sql, cap, false).result();
        if (query.hasAggregations() && !isBlank(query.getGroupByTime())) {
            for (Map<String, Object> row : result.getRows()) {
                if (row.containsKey("window_start"))
                    throw queryError("InfluxDB1 result conflicts with the generated window_start column");
                row.put("window_start", row.get("time"));
            }
            if (!result.getColumns().contains("window_start")) result.getColumns().add("window_start");
        }
        // InfluxQL LIMIT/OFFSET applies per series. Grouped results require bounded global ordering and slicing.
        if (query.hasAggregations() && !query.getGroupByTags().isEmpty()) {
            Comparator<Map<String, Object>> comparator = isBlank(query.getGroupByTime())
                    ? (left, right) -> 0 : Comparator.comparingLong(row -> {
                Long time = parseTimeMillis(row.get("time"));
                return time == null ? 0L : time;
            });
            for (String tag : query.getGroupByTags())
                comparator = comparator.thenComparing(row -> String.valueOf(row.get(tag)));
            if (SortOrderEnum.normalize(query.getOrder()) == SortOrderEnum.DESC) comparator = comparator.reversed();
            result.getRows().sort(comparator);
            int offset = query.getOffset() == null ? 0 : Math.max(0, query.getOffset());
            int from = Math.min(offset, result.getRows().size());
            int to = query.getLimit() == null ? result.getRows().size()
                    : (int) Math.min(result.getRows().size(), (long) from + query.getLimit());
            result.setRows(new ArrayList<>(result.getRows().subList(from, to)));
            result.setRowCount(result.getRows().size());
        }
        return result;
    }

    /**
     * Streams and counts result rows; InfluxQL COUNT(*) counts non-null fields, not logical result rows.
     * Ignores pagination and cursors. The response-byte limit still applies, but the row limit does not.
     *
     * @param database target database; null or blank selects the configured default
     * @param query common query model, copied before removing pagination
     * @return total logical result rows before pagination
     * @throws TSDBException if the query is invalid or unsupported, execution fails, or the byte limit is exceeded
     */
    @Override
    public long count(String database, TSDBQuery query) {
        if (query == null) throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "query must not be null");
        TSDBQuery snapshot = query.copy();
        snapshot.setCursorTime(null);
        snapshot.setStrictCursor(false);
        snapshot.setCursorValues(Collections.emptyMap());
        snapshot.setPaginationProbe(false);
        snapshot.setLimit(null);
        snapshot.setOffset(null);
        validateQuery(snapshot);
        String sql = buildQuery(snapshot, false);
        validateAggregationOutputNames(snapshot);
        return execute(resolveDatabase(database), sql, maxQueryRows, true).count();
    }

    private ReadResult execute(String database, String sql, int rowLimit, boolean countOnly) {
        lifecycleLock.readLock().lock();
        try {
            requireReady();
            FormBody.Builder body = new FormBody.Builder().add("db", database).add("q", sql).add("epoch", "ms");
            if (!isBlank(config.getRetentionPolicy())) body.add("rp", config.getRetentionPolicy());
            Request request = authorize(new Request.Builder().url(apiUrl("query").build()).post(body.build())).build();
            if (queryLogEnabled) log.debug("Executing InfluxDB1 InfluxQL: {}", sql);
            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful())
                    throw new TSDBException(errorCodeForHttpStatus(response.code(), TSDBErrorCodeEnum.QUERY_ERROR),
                            "InfluxDB1 query rejected with HTTP " + response.code() + ": " + readBody(response));
                return readResponse(response, rowLimit, countOnly);
            } catch (JsonProcessingException e) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR, "Invalid InfluxDB1 JSON response", e);
            } catch (IOException e) {
                throw new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR, "InfluxDB1 query I/O failed", e);
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Parses the envelope and each values array incrementally, never materializing all series.
     */
    private ReadResult readResponse(Response response, int rowLimit, boolean countOnly) throws IOException {
        QueryResult result = new QueryResult();
        List<Map<String, Object>> rows = new ArrayList<>();
        Set<String> columnNames = new LinkedHashSet<>();
        long[] count = {0};
        if (response.body() == null) throw queryError("InfluxDB1 response body is missing");
        if (response.body().contentLength() > maxQueryResponseBytes)
            throw responseLimitExceeded("response bytes", maxQueryResponseBytes);
        try (InputStream input = new BoundedResponseInputStream(response.body().byteStream(), maxQueryResponseBytes);
             JsonParser parser = objectMapper.getFactory().createParser(input)) {
            requireToken(parser.nextToken(), JsonToken.START_OBJECT);
            boolean foundResults = false;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                requireToken(parser.currentToken(), JsonToken.FIELD_NAME);
                String field = parser.currentName();
                parser.nextToken();
                if ("error".equals(field)) throw queryError("InfluxDB1 query error: " + parser.getValueAsString());
                if ("results".equals(field)) {
                    foundResults = true;
                    requireToken(parser.currentToken(), JsonToken.START_ARRAY);
                    int statements = 0;
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        if (++statements > 1)
                            throw unsupported("Multiple InfluxQL statement results cannot be flattened safely");
                        readStatement(parser, rowLimit, countOnly, rows, columnNames, count);
                    }
                } else parser.skipChildren();
            }
            if (!foundResults || parser.nextToken() != null)
                throw queryError("Invalid or trailing InfluxDB1 response content");
        }
        result.setRows(rows);
        result.setColumns(new ArrayList<>(columnNames));
        result.setRowCount(rows.size());
        result.setSuccess(true);
        result.setMessage("InfluxQL executed successfully");
        return new ReadResult(result, count[0]);
    }

    private void readStatement(JsonParser parser, int rowLimit, boolean countOnly, List<Map<String, Object>> rows,
                               Set<String> columns, long[] count) throws IOException {
        requireToken(parser.currentToken(), JsonToken.START_OBJECT);
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            requireToken(parser.currentToken(), JsonToken.FIELD_NAME);
            String field = parser.currentName();
            parser.nextToken();
            if ("error".equals(field)) throw queryError("InfluxDB1 query error: " + parser.getValueAsString());
            if ("partial".equals(field) && parser.getValueAsBoolean())
                throw queryError("InfluxDB1 returned a partial result; narrow the query");
            if ("series".equals(field)) {
                requireToken(parser.currentToken(), JsonToken.START_ARRAY);
                while (parser.nextToken() != JsonToken.END_ARRAY)
                    readSeries(parser, rowLimit, countOnly, rows, columns, count);
            } else parser.skipChildren();
        }
    }

    private void readSeries(JsonParser parser, int rowLimit, boolean countOnly, List<Map<String, Object>> rows,
                            Set<String> columnNames, long[] count) throws IOException {
        requireToken(parser.currentToken(), JsonToken.START_OBJECT);
        List<String> columns = null;
        Map<String, String> tags = new LinkedHashMap<>();
        boolean valuesSeen = false;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            requireToken(parser.currentToken(), JsonToken.FIELD_NAME);
            String field = parser.currentName();
            parser.nextToken();
            if ("partial".equals(field) && parser.getValueAsBoolean())
                throw queryError("InfluxDB1 returned a partial series; narrow the query");
            switch (field) {
                case "columns" -> {
                    columns = objectMapper.readValue(parser, COLUMN_TYPE);
                    if (columns == null || columns.contains(null) || new HashSet<>(columns).size() != columns.size())
                        throw queryError("InfluxDB1 response contains invalid or duplicate columns");
                }
                case "tags" -> {
                    if (valuesSeen) throw queryError("InfluxDB1 series tags must precede values");
                    tags = objectMapper.readValue(parser, TAG_TYPE);
                }
                case "values" -> {
                    valuesSeen = true;
                    if (columns == null) throw queryError("InfluxDB1 values must follow their columns");
                    requireToken(parser.currentToken(), JsonToken.START_ARRAY);
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        requireToken(parser.currentToken(), JsonToken.START_ARRAY);
                        if (!countOnly && count[0] >= rowLimit) throw responseLimitExceeded("rows", rowLimit);
                        List<Object> values = objectMapper.readValue(parser, VALUES_TYPE);
                        if (values.size() != columns.size())
                            throw queryError("InfluxDB1 row width does not match its columns");
                        Map<String, Object> row = new LinkedHashMap<>();
                        if (tags != null) row.putAll(tags);
                        for (int i = 0; i < columns.size(); i++) {
                            if (row.containsKey(columns.get(i)))
                                throw queryError("InfluxDB1 tag and field columns collide");
                            row.put(columns.get(i), values.get(i));
                        }
                        normalizeRows(Collections.singletonList(row));
                        count[0] = Math.incrementExact(count[0]);
                        if (!countOnly) {
                            rows.add(row);
                            columnNames.addAll(row.keySet());
                        }
                    }
                }
                case "error" -> throw queryError("InfluxDB1 series error: " + parser.getValueAsString());
                default -> parser.skipChildren();
            }
        }
    }

    private static void requireToken(JsonToken actual, JsonToken expected) {
        if (actual != expected)
            throw queryError("Malformed InfluxDB1 response: expected " + expected + ", received " + actual);
    }

    private record ReadResult(QueryResult result, long count) {
    }

    private static TSDBException queryError(String message) {
        return new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR, message);
    }

    private static TSDBException unsupported(String message) {
        return new TSDBException(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, message);
    }

    /** Rejects aggregate outputs that collide with InfluxQL's implicit time column. */
    private void validateAggregationOutputNames(TSDBQuery query) {
        TSDBQueryValidator.validateAggregationOutputNames(query, this::normalizeColumnIdentifier);
        if (!query.hasAggregations()) return;
        if (query.getGroupByTags().contains("time") || query.getAggregations().stream()
                .anyMatch(aggregation -> "time".equals(aggregation.alias()))) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxQL aggregate output name conflicts with the implicit time column: time");
        }
    }

    private static void validateQuery(TSDBQuery query) {
        TSDBQueryValidator.validate(query);
        if (query == null || isBlank(query.getMeasurement()))
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "measurement must not be empty");
        if (!"time".equalsIgnoreCase(query.getTimeColumn()))
            throw unsupported("InfluxDB1 uses the fixed time column 'time'");
        if (query.isStrictCursor())
            throw unsupported("InfluxQL does not support the composite cursor ordering contract; use time cursor pagination");
        for (SortSpec sort : query.getSortSpecs()) {
            if (!"time".equalsIgnoreCase(sort.column())) throw unsupported("InfluxQL only supports ORDER BY time");
        }
        if (!query.hasAggregations() && (!isBlank(query.getGroupByTime()) || !query.getGroupByTags().isEmpty()))
            throw unsupported("InfluxQL grouping requires an aggregation");
        if (!query.hasAggregations() && !query.getSelectColumns().isEmpty()) {
            if (query.getRecordType() == null)
                throw unsupported("InfluxQL explicit projections require annotated recordType metadata to verify a field is selected; use SELECT * or native InfluxQL otherwise");
            boolean containsField = PROJECTION_METADATA.resolve(query.getRecordType()).fieldColumns().stream()
                    .anyMatch(field -> query.getSelectColumns().contains(field.getColumnName()));
            if (!containsField)
                throw unsupported("InfluxQL SELECT must include at least one annotated field; time/tag-only projections are unsupported");
        }
        if (!isBlank(query.getGroupByTime())) {
            if (query.getStartTime() == null || query.getEndTime() == null)
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                        "InfluxQL time-window aggregation requires an explicit bounded timeRange; the server's default end is now()");
        }
    }

    private static String buildQuery(TSDBQuery query, boolean page) {
        List<String> selections = new ArrayList<>();
        if (query.hasAggregations()) {
            for (AggregationSpec aggregation : query.getAggregations()) {
                String function = aggregation.function() == AggregationFunctionEnum.AVG ? "MEAN" : aggregation.function().name();
                selections.add(function + "(" + quoteIdentifier(aggregation.field()) + ") AS " + quoteIdentifier(aggregation.alias()));
            }
        } else {
            for (String column : query.getSelectColumns()) {
                if (!"time".equalsIgnoreCase(column)) selections.add(quoteIdentifier(column));
            }
            if (selections.isEmpty()) selections.add("*");
        }
        StringBuilder sql = new StringBuilder("SELECT ").append(String.join(", ", selections))
                .append(" FROM ").append(quoteIdentifier(query.getMeasurement()));
        List<String> clauses = new ArrayList<>();
        if (query.getStartTime() != null) clauses.add("time >= " + timeLiteral(query.getStartTime()));
        if (query.getEndTime() != null) clauses.add("time <= " + timeLiteral(query.getEndTime()));
        if (query.getCursorTime() != null)
            clauses.add("time " + (SortOrderEnum.normalize(query.getOrder()) == SortOrderEnum.DESC ? "< " : "> ") + timeLiteral(query.getCursorTime()));
        for (QueryFilter filter : query.getFilters()) clauses.add(filterSql(filter));
        if (!clauses.isEmpty()) sql.append(" WHERE ").append(String.join(" AND ", clauses));
        List<String> groups = new ArrayList<>();
        if (!isBlank(query.getGroupByTime())) {
            TimeWindowPlan plan = TimeWindowPlan.resolve(query.getGroupByTime(), query.getTimeZone(), query.getStartTime(), query.getEndTime());
            if (plan.isCalendar())
                throw unsupported("InfluxQL cannot express region calendar-day boundaries across DST; use UTC or a fixed offset");
            long offsetMillis = OffsetDateTime.parse(plan.fixedOrigin()).toInstant().toEpochMilli();
            groups.add("time(" + TimeWindowDuration.parse(query.getGroupByTime()).toIoTDBInterval() + ", " + offsetMillis + "ms)");
        }
        for (String tag : query.getGroupByTags()) groups.add(quoteIdentifier(tag));
        if (!groups.isEmpty()) sql.append(" GROUP BY ").append(String.join(", ", groups));
        if (!isBlank(query.getGroupByTime())) sql.append(" fill(none)");
        sql.append(" ORDER BY time ").append(SortOrderEnum.normalize(query.getOrder()).name());
        if (page && !(query.hasAggregations() && !query.getGroupByTags().isEmpty())) {
            if (query.getLimit() != null) sql.append(" LIMIT ").append(query.getLimit());
            if (query.getOffset() != null && query.getOffset() > 0) sql.append(" OFFSET ").append(query.getOffset());
        }
        return sql.toString();
    }

    private static String filterSql(QueryFilter filter) {
        if (filter == null || isBlank(filter.column()) || filter.operator() == null || filter.values() == null || filter.values().isEmpty())
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "Invalid InfluxDB1 query filter");
        String column = quoteIdentifier(filter.column());
        List<Object> values = filter.values();
        if (values.stream().anyMatch(Objects::isNull))
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "InfluxQL filter values must not be null");
        return switch (filter.operator()) {
            case EQ -> column + " = " + formatValue(values.get(0));
            case NE -> column + " != " + formatValue(values.get(0));
            case GT -> column + " > " + formatValue(values.get(0));
            case GE -> column + " >= " + formatValue(values.get(0));
            case LT -> column + " < " + formatValue(values.get(0));
            case LE -> column + " <= " + formatValue(values.get(0));
            case IN ->
                    "(" + String.join(" OR ", values.stream().map(value -> column + " = " + formatValue(value)).toList()) + ")";
            case BETWEEN -> {
                if (values.size() != 2)
                    throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "BETWEEN requires exactly two values");
                yield "(" + column + " >= " + formatValue(values.get(0)) + " AND " + column + " <= " + formatValue(values.get(1)) + ")";
            }
        };
    }

    private static String formatValue(Object value) {
        if (value instanceof Number number) {
            try {
                new BigDecimal(number.toString());
            } catch (NumberFormatException e) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "Non-finite query number", e);
            }
            return number.toString();
        }
        if (value instanceof Boolean) return value.toString();
        return sqlString(String.valueOf(value));
    }

    private static String timeLiteral(long millis) {
        return sqlString(Instant.ofEpochMilli(millis).toString());
    }

    private static String quoteIdentifier(String value) {
        if (isBlank(value))
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "InfluxQL identifier must not be empty");
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String sqlString(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    private String resolveDatabase(String value) {
        return isBlank(value) ? config.getDatabase() : value.trim();
    }

    private HttpUrl.Builder apiUrl(String endpoint) {
        HttpUrl base = HttpUrl.parse(config.getUrl());
        if (base == null) throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR, "Invalid InfluxDB1 URL");
        return base.newBuilder().addPathSegment(endpoint);
    }

    private Request.Builder authorize(Request.Builder request) {
        if (!isBlank(config.getUsername()))
            request.header("Authorization", Credentials.basic(config.getUsername(), config.getPassword() == null ? "" : config.getPassword()));
        return request;
    }

    private void writePayload(HttpUrl url, String payload) {
        Request request = authorize(new Request.Builder().url(url).post(RequestBody.create(payload, LINE_PROTOCOL))).build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) throw new InfluxWriteHttpException(response.code(), readBody(response));
        } catch (IOException e) {
            throw new UncheckedIOException("InfluxDB1 write I/O failed; commit status is unknown", e);
        }
    }

    /**
     * InfluxDB 1.x may partially persist a request even when it returns HTTP 400.
     */
    private static boolean isDefiniteWriteRejection(int status) {
        return status == 401 || status == 403 || status == 404 || status == 405 || status == 413 || status == 415 || status == 429;
    }

    private record PreparedWriteBatch(String payload, int recordCount, String measurement) {
    }

    private static final class InfluxWriteHttpException extends RuntimeException {
        private final int statusCode;

        private InfluxWriteHttpException(int statusCode, String body) {
            super("InfluxDB1 write returned HTTP " + statusCode + ": " + body);
            this.statusCode = statusCode;
        }

        private int getStatusCode() {
            return statusCode;
        }
    }


    /**
     * Validates and encodes all records before sending millisecond-precision line protocol writes.
     * Splits requests at 5,000 lines or 1 MiB; multiple requests are not atomic.
     *
     * @param database target database; null or blank selects the configured default
     * @param records input records; null or empty returns an empty success after checking adapter state
     * @return confirmed successful record and request counts
     * @throws TSDBBatchWriteException on validation or write failure, carrying confirmed and unknown commit state
     * @throws TSDBException if this adapter is not initialized or is closed
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
            HttpUrl.Builder writeUrl = apiUrl("write")
                    .addQueryParameter("db", resolveDatabase(database))
                    .addQueryParameter("precision", "ms");
            if (!isBlank(config.getRetentionPolicy())) writeUrl.addQueryParameter("rp", config.getRetentionPolicy());
            url = writeUrl.build();
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

    private static TSDBBatchWriteException preflightFailure(int requestedRecords, RuntimeException cause) {
        BatchWriteResult result = new BatchWriteResult(requestedRecords, 0, 0,
                0, 0, null, null, BatchCommitStateEnum.NOT_COMMITTED, false,
                "InfluxDB1 batch validation failed before HTTP I/O: " + cause.getMessage());
        return new TSDBBatchWriteException(result, cause);
    }

    private static boolean isRetryableWriteStatus(int statusCode) {
        return statusCode == 408 || statusCode == 429
                || (statusCode >= 500 && statusCode < 600 && statusCode != 501 && statusCode != 505);
    }

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
                        "InfluxDB1 line protocol record exceeds maximum UTF-8 bytes: "
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
                        "InfluxDB1 batch exceeds maximum combined UTF-8 payload bytes: " + maxBatchBytes);
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

    private static void validateRecord(TSDBRecord tsdbRecord) {
        if (tsdbRecord == null || isBlank(tsdbRecord.measurement())) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "measurement must not be empty for InfluxDB1 write");
        }
        if (tsdbRecord.measurement().startsWith("#")) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB1 measurements beginning with # would be interpreted as comments");
        }
        if (tsdbRecord.fields() == null || tsdbRecord.fields().isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "fields must not be empty for InfluxDB1 write");
        }
        if (tsdbRecord.timestamp() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "timestamp must not be null for InfluxDB1 write");
        }
        validateLineProtocolComponent(tsdbRecord.measurement(), "measurement");
        if (tsdbRecord.tags() != null) {
            for (Map.Entry<String, String> entry : tsdbRecord.tags().entrySet()) {
                validateWriteKey(entry.getKey());
                validateOptionalLineProtocolComponent(entry.getKey(), "tag key");
                validateOptionalLineProtocolComponent(entry.getValue(), "tag value");
            }
        }
        for (Map.Entry<String, Object> entry : tsdbRecord.fields().entrySet()) {
            validateWriteKey(entry.getKey());
            validateLineProtocolComponent(entry.getKey(), "field key");
            Object value = entry.getValue();
            if (value != null && !(value instanceof Number) && !(value instanceof Boolean)) {
                validateLineProtocolComponent(String.valueOf(value), "field value");
            }
        }
    }

    /**
     * Rejects protocol-reserved keys before a request can silently discard a point.
     */
    private static void validateWriteKey(String key) {
        if ("_field".equals(key) || "_measurement".equals(key) || "time".equals(key))
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "InfluxDB1 reserves tag and field key: " + key);
    }

    private void validateBatchSize(Collection<TSDBRecord> records) {
        if (records.size() > maxBatchRecords) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB1 batch write record count exceeds tsdb.influxdb1.max-batch-records: "
                            + records.size() + " > " + maxBatchRecords);
        }
    }

    private static int resolveMaxBatchRecords(InfluxDB1Properties config) {
        if (config == null || config.getMaxBatchRecords() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.influxdb1.max-batch-records must be greater than 0");
        }
        return config.getMaxBatchRecords();
    }

    private static void validateLineProtocolComponent(String value,
                                                      String componentName) {
        if (value == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB1 line protocol " + componentName + " must not be null");
        }
        if (value.codePoints().anyMatch(Character::isISOControl)) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB1 line protocol " + componentName + " must not contain control characters");
        }
        int bytes = utf8Length(value);
        if (bytes > LINE_PROTOCOL_COMPONENT_MAX_BYTES) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "InfluxDB1 line protocol " + componentName
                            + " exceeds maximum UTF-8 bytes: " + bytes + " > " + LINE_PROTOCOL_COMPONENT_MAX_BYTES);
        }
    }

    private static void validateOptionalLineProtocolComponent(String value,
                                                              String componentName) {
        if (value != null) {
            validateLineProtocolComponent(value, componentName);
        }
    }

    private static int utf8Length(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    private static void appendTag(StringBuilder line, String key, String value) {
        if (isBlank(key) || isBlank(value)) {
            return;
        }
        line.append(',')
                .append(escapeLineProtocolKey(key))
                .append('=')
                .append(escapeLineProtocolTagValue(value));
    }

    private static String formatLineProtocolFieldValue(Object value) {
        if (value instanceof Float || value instanceof Double) {
            if (!Double.isFinite(((Number) value).doubleValue())) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "Non-finite line protocol field value");
            }
            return value.toString();
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return value + "i";
        }
        if (value instanceof BigInteger integer) {
            try {
                return integer.longValueExact() + "i";
            } catch (ArithmeticException e) {
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "InfluxDB1 integer field is outside the signed 64-bit range", e);
            }
        }
        if (value instanceof BigDecimal decimal) {
            double binary = decimal.doubleValue();
            if (!Double.isFinite(binary) || new BigDecimal(binary).compareTo(decimal) != 0)
                throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "InfluxDB1 float64 cannot exactly represent this BigDecimal field");
            return Double.toString(binary);
        }
        if (value instanceof Number)
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "Unsupported InfluxDB1 numeric field type: " + value.getClass().getName());
        if (value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + String.valueOf(value).replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String escapeLineProtocolKey(String value) {
        return value.replace(" ", "\\ ")
                .replace(",", "\\,")
                .replace("=", "\\=");
    }

    /**
     * Measurement names escape commas and spaces; equals signs and backslashes remain literal.
     */
    private static String escapeLineProtocolMeasurement(String value) {
        return value.replace(" ", "\\ ").replace(",", "\\,");
    }

    private static String escapeLineProtocolTagValue(String value) {
        return escapeLineProtocolKey(value);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void normalizeRows(List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            Object time = row.get("time");
            Long epochMillis = parseTimeMillis(time);
            if (epochMillis != null) {
                row.put("time", epochMillis);
                // Preserve an actual field or native alias named _time, including an explicit null.
                if (!row.containsKey("_time")) row.put("_time", epochMillis);
            }
        }
    }

    private static Long parseTimeMillis(Object value) {
        if (value instanceof Number) {
            try {
                return exactLong((Number) value);
            } catch (NumberFormatException | ArithmeticException e) {
                throw new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                        "InfluxDB1 returned an invalid or out-of-range time value: " + value, e);
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
                        "InfluxDB1 returned an invalid or out-of-range time value: " + value, e);
            }
        }
    }

    private static long exactLong(Number number) {
        BigDecimal decimal = number instanceof Float || number instanceof Double
                ? new BigDecimal(number.doubleValue()) : new BigDecimal(number.toString());
        return decimal.longValueExact();
    }

    private static String readBody(Response response) throws IOException {
        // Error diagnostics are bounded independently from successful query result limits.
        return response.body() == null ? "" : response.peekBody(8 * 1024L).string();
    }

    private static TSDBException responseLimitExceeded(String kind, long limit) {
        return new TSDBException(TSDBErrorCodeEnum.QUERY_ERROR,
                "InfluxDB1 query exceeded maximum " + kind + ": " + limit
                        + "; add SQL filters or pagination, or increase the configured query limit");
    }

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
}
