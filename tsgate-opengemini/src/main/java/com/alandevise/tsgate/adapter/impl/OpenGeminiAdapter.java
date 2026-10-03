package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.config.InfluxDB1HttpClientProperties;
import com.alandevise.tsgate.config.InfluxDB1Properties;
import com.alandevise.tsgate.config.OpenGeminiHttpClientProperties;
import com.alandevise.tsgate.config.OpenGeminiProperties;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.QueryResult;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import org.influxdb.InfluxDB;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * openGemini adapter for its default storage engine and InfluxDB 1.x-compatible HTTP endpoints.
 * Reuses the bounded InfluxQL and line-protocol implementation without copying its transport code.
 * Databases and retention policies must already exist. The configured URL identifies one ts-sql
 * entry point or an external load balancer; this adapter does not discover or retry across nodes.
 * Protocol exceptions preserve the InfluxDB1 compatibility implementation's messages and causes.
 * New series may become query-visible after the server acknowledges a write; reads are not retried.
 */
public class OpenGeminiAdapter implements TSDBAdapter {
    private static final BigInteger MIN_INTEGER = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger MAX_INTEGER = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigDecimal MIN_INTEGER_LITERAL = new BigDecimal(MIN_INTEGER);
    private static final BigDecimal MAX_INTEGER_LITERAL = new BigDecimal(MAX_INTEGER);
    private final InfluxDB1Adapter delegate;
    private final ReentrantReadWriteLock operationLock = new ReentrantReadWriteLock();
    private final InfluxDB1Properties connectionSnapshot;
    private final InfluxDB1HttpClientProperties httpSnapshot;
    private InfluxDB nativeClient;

    /**
     * Creates an uninitialized adapter with default HTTP settings and debug query logging enabled.
     *
     * @param config connection settings and operation limits
     */
    public OpenGeminiAdapter(OpenGeminiProperties config) {
        this(config, new OpenGeminiHttpClientProperties(), true);
    }

    /**
     * Copies and validates settings without contacting the server; call {@link #init()} before use.
     * Later property changes do not reconfigure this instance. HTTP connection recovery is disabled
     * by default so failed writes are not automatically replayed by the compatibility client.
     *
     * @param config connection settings and operation limits
     * @param httpConfig HTTP pool and timeout settings
     * @param queryLogEnabled whether generated InfluxQL is logged at debug level
     */
    public OpenGeminiAdapter(OpenGeminiProperties config, OpenGeminiHttpClientProperties httpConfig,
                             boolean queryLogEnabled) {
        connectionSnapshot = connectionConfiguration(config);
        httpSnapshot = httpConfiguration(httpConfig);
        delegate = new InfluxDB1Adapter(connectionSnapshot, httpSnapshot, queryLogEnabled);
    }

    private static InfluxDB1Properties connectionConfiguration(OpenGeminiProperties source) {
        if (source == null) return null;
        InfluxDB1Properties copy = new InfluxDB1Properties();
        copy.setEnable(source.isEnable());
        copy.setFailFast(source.isFailFast());
        copy.setUrl(source.getUrl());
        copy.setDatabase(source.getDatabase());
        copy.setUsername(source.getUsername());
        copy.setPassword(source.getPassword());
        copy.setRetentionPolicy(source.getRetentionPolicy());
        copy.setMaxBatchRecords(source.getMaxBatchRecords());
        copy.setMaxQueryRows(source.getMaxQueryRows());
        copy.setMaxQueryResponseBytes(source.getMaxQueryResponseBytes());
        return copy;
    }

    private static InfluxDB1HttpClientProperties httpConfiguration(OpenGeminiHttpClientProperties source) {
        if (source == null) return null;
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
     * Creates local HTTP and compatible native-client resources without probing server connectivity.
     * Repeated initialization is harmless; resource creation failures can retry until the adapter is closed.
     */
    @Override
    public void init() {
        operationLock.writeLock().lock();
        try {
            delegate.init();
        } finally {
            operationLock.writeLock().unlock();
        }
    }

    /**
     * Waits for adapter operations to finish and permanently closes this instance; repeated calls are harmless.
     * Callers must coordinate borrowed native operations with shutdown separately.
     */
    @Override
    public void close() {
        operationLock.writeLock().lock();
        try {
            delegate.close();
        } finally {
            operationLock.writeLock().unlock();
        }
    }

    /**
     * Returns the borrowed InfluxDB-compatible native client for openGemini operations.
     * Callers must not close it. Native operations bypass adapter limits and lifecycle locking.
     * The type is {@link InfluxDB}, not the openGemini-specific Java SDK. Health and version requests
     * read the actual openGemini version header; other native operations preserve backend responses.
     *
     * @return initialized, adapter-owned compatibility client
     */
    public InfluxDB getNativeClient() {
        return withReadLock(() -> {
            InfluxDB raw = delegate.getNativeClient();
            synchronized (this) {
                if (nativeClient == null) nativeClient = OpenGeminiNativeClient.wrap(raw, connectionSnapshot, httpSnapshot);
                return nativeClient;
            }
        });
    }

    /**
     * Executes a single read-only InfluxQL statement against the configured database.
     * SELECT, SHOW, and EXPLAIN [ANALYZE] SELECT are accepted subject to server support;
     * mutations, multiple statements, comments, slash syntax, and backtick quoting are rejected.
     * Row and decompressed-response limits apply, and partial results fail. Server query errors
     * remain errors, including missing measurements in multi-source queries or subqueries.
     * Native numeric literals retain the backend precision boundary; this method does not parse or rewrite them.
     *
     * @param sql read-only InfluxQL statement
     * @return bounded rows with timestamps normalized to epoch milliseconds
     */
    @Override
    public QueryResult executeQuery(String sql) {
        return withReadLock(() -> delegate.executeQuery(sql));
    }

    /**
     * Writes validated millisecond-precision line protocol through the compatibility endpoint.
     * Requests split at 5,000 lines or 1 MiB; multiple requests are not atomic. Errors preserve
     * confirmed record counts and report UNKNOWN when the response does not prove the commit boundary.
     * Measurement names follow server restrictions and are checked for the entire batch before I/O.
     * Integer fields must fit signed int64 and be exactly representable as float64, because the server
     * parses integer line-protocol fields through that representation. Decimal-field validation is inherited.
     *
     * @param database target database; null or blank selects the configured default
     * @param records common records; null or empty is an empty success after checking adapter state
     * @return confirmed commit boundary and physical request counts
     */
    @Override
    public BatchWriteResult batchWriteDetailed(String database, Collection<TSDBRecord> records) {
        return withReadLock(() -> {
            // Preserve the delegate's state checks and empty-batch behavior before local validation.
            delegate.getNativeClient();
            if (records == null || records.isEmpty()) return delegate.batchWriteDetailed(database, records);
            List<TSDBRecord> snapshot;
            try {
                snapshot = List.copyOf(records);
                for (TSDBRecord record : snapshot) {
                    validateMeasurement(record.measurement());
                    for (Object value : record.fields().values()) validateIntegerField(value);
                }
            } catch (RuntimeException error) {
                BatchWriteResult result = new BatchWriteResult(records.size(), 0, 0, 0, 0, null, null,
                        BatchCommitStateEnum.NOT_COMMITTED, false,
                        "openGemini batch validation failed before HTTP I/O: " + error.getMessage());
                throw new TSDBBatchWriteException(result, error);
            }
            return delegate.batchWriteDetailed(database, snapshot);
        });
    }

    /**
     * Returns the input-record limit before physical HTTP request splitting.
     *
     * @return configured maximum batch records
     */
    @Override
    public int getMaxBatchRecords() {
        return delegate.getMaxBatchRecords();
    }

    /**
     * Executes the common query model with time-only ordering and bounded results.
     * Strict composite cursors and field ordering are unsupported for this default-engine subset.
     * Time-window aggregations require explicit bounds and UTC or a fixed-offset calendar alignment.
     * The exact missing-measurement backend error returns no rows. Integer-valued Number predicates
     * must fit signed int64 and be exactly representable as float64; Float/Double retain floating-point semantics.
     * Exponent-form integer literals are range-checked before expanding them into an integer.
     *
     * @param database target database; null or blank selects the configured default
     * @param query common query model
     * @return query rows with global slicing for grouped aggregations
     */
    @Override
    public QueryResult query(String database, TSDBQuery query) {
        return withReadLock(() -> {
            try {
                delegate.getNativeClient();
                validateIntegerFilters(query);
                return delegate.query(database, query);
            } catch (TSDBException error) {
                if (isMissingMeasurement(error)) return emptyResult();
                throw error;
            }
        });
    }

    /**
     * Counts logical result rows using a bounded streaming scan, including sparse-field rows.
     * Pagination and cursors are ignored in a private query copy. The response-byte limit applies;
     * the result-row cap does not apply to count scans. The exact missing-measurement error returns zero.
     * Integer-valued Number filters follow the same precision validation as common queries.
     *
     * @param database target database; null or blank selects the configured default
     * @param query common query model
     * @return result rows before pagination
     */
    @Override
    public long count(String database, TSDBQuery query) {
        return withReadLock(() -> {
            try {
                delegate.getNativeClient();
                validateIntegerFilters(query);
                return delegate.count(database, query);
            } catch (TSDBException error) {
                if (isMissingMeasurement(error)) return 0L;
                throw error;
            }
        });
    }

    // Guards local preflight, response normalization, and proxy creation as well as delegate I/O.
    // The delegate remains the sole owner of lifecycle state and client resources.
    private <T> T withReadLock(Supplier<T> operation) {
        operationLock.readLock().lock();
        try {
            return operation.get();
        } finally {
            operationLock.readLock().unlock();
        }
    }

    private static boolean isMissingMeasurement(TSDBException error) {
        return error.getErrorCode() == TSDBErrorCodeEnum.QUERY_ERROR
                && "InfluxDB1 query error: measurement not found".equals(error.getMessage());
    }

    private static QueryResult emptyResult() {
        QueryResult result = new QueryResult();
        result.setSuccess(true);
        result.setMessage("openGemini measurement does not exist; no rows returned");
        return result;
    }

    private static void validateIntegerField(Object value) {
        if (value instanceof BigInteger integer) validateExactInteger(integer);
        else if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)
            validateExactInteger(BigInteger.valueOf(((Number) value).longValue()));
    }

    private static void validateIntegerFilters(TSDBQuery query) {
        if (query == null || query.getFilters() == null) return;
        for (QueryFilter filter : query.getFilters()) {
            if (filter == null) continue; // Leave ordinary model validation to the delegate.
            for (Object value : filter.values()) {
                if (!(value instanceof Number number) || value instanceof Float || value instanceof Double) continue;
                BigDecimal decimal;
                try {
                    decimal = new BigDecimal(number.toString());
                } catch (NumberFormatException invalidNumber) {
                    continue; // Leave invalid-number handling to the existing query validation.
                }
                if (decimal.scale() > 0 && decimal.stripTrailingZeros().scale() > 0) {
                    continue; // Fractional predicates retain the existing floating-point semantics.
                }
                // Compare decimal bounds before expanding an exponent into a potentially huge integer.
                if (decimal.compareTo(MIN_INTEGER_LITERAL) < 0 || decimal.compareTo(MAX_INTEGER_LITERAL) > 0) {
                    throw integerPrecisionError();
                }
                validateExactInteger(decimal.toBigIntegerExact());
            }
        }
    }

    // These server releases parse line-protocol integer fields and SQL number literals through float64.
    // Inspect the exact binary double value rather than its shortest decimal representation.
    private static void validateExactInteger(BigInteger integer) {
        if (integer.compareTo(MIN_INTEGER) < 0 || integer.compareTo(MAX_INTEGER) > 0
                || !new BigDecimal(integer.doubleValue()).toBigIntegerExact().equals(integer)) {
            throw integerPrecisionError();
        }
    }

    private static TSDBException integerPrecisionError() {
        return new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                "openGemini integer fields and common integer predicates must fit signed int64 and be exactly representable as float64");
    }

    private static void validateMeasurement(String name) {
        if (name == null || name.isEmpty() || name.equals(".") || name.equals("..")
                || name.codePoints().anyMatch(character -> ",;/\\".indexOf(character) >= 0 || !isPrintable(character))) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "openGemini measurement must be printable and must not contain comma, semicolon, slash or backslash, or equal '.' or '..'");
        }
    }

    // Mirrors Go unicode.IsPrint: letters, marks, numbers, punctuation, symbols, and ASCII space.
    private static boolean isPrintable(int character) {
        if (character == ' ') return true;
        return switch (Character.getType(character)) {
            case Character.UPPERCASE_LETTER, Character.LOWERCASE_LETTER, Character.TITLECASE_LETTER,
                    Character.MODIFIER_LETTER, Character.OTHER_LETTER, Character.NON_SPACING_MARK,
                    Character.COMBINING_SPACING_MARK, Character.ENCLOSING_MARK, Character.DECIMAL_DIGIT_NUMBER,
                    Character.LETTER_NUMBER, Character.OTHER_NUMBER, Character.CONNECTOR_PUNCTUATION,
                    Character.DASH_PUNCTUATION, Character.START_PUNCTUATION, Character.END_PUNCTUATION,
                    Character.INITIAL_QUOTE_PUNCTUATION, Character.FINAL_QUOTE_PUNCTUATION,
                    Character.OTHER_PUNCTUATION, Character.MATH_SYMBOL, Character.CURRENCY_SYMBOL,
                    Character.MODIFIER_SYMBOL, Character.OTHER_SYMBOL -> true;
            default -> false;
        };
    }

    /**
     * Returns the backend name used by the shared template and application diagnostics.
     *
     * @return {@code openGemini}
     */
    @Override
    public String getAdapterName() {
        return "openGemini";
    }
}
