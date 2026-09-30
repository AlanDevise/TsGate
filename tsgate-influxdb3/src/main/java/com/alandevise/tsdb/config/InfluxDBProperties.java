package com.alandevise.tsdb.config;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Connection properties for the InfluxDB adapter.
 * <p>The InfluxDB starter binds these settings through {@code @ConfigurationProperties(prefix = "tsdb.influxdb")}.
 * They configure the InfluxDB connection, HTTP client, resource limits, and initialization failure policy.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-08
 */
@Data
@ConfigurationProperties(prefix = "tsdb.influxdb")
public class InfluxDBProperties {

    /**
     * Enables Spring Boot auto-configuration for this backend; defaults to false.
     * Connection settings alone do not enable it. Configure enable=true explicitly to activate the starter.
     * This flag does not control adapters constructed and initialized directly by application code.
     */
    private boolean enable = false;

    /**
     * Whether client resource initialization failure aborts application startup.
     */
    private boolean failFast = true;
    /**
     * InfluxDB server URL.
     */
    private String url = "http://localhost:8181";
    /**
     * InfluxDB authentication token; may be blank when server authentication is disabled.
     */
    @ToString.Exclude
    private String token = "";
    /**
     * Default database, tsdb, for writes and queries that do not explicitly specify one.
     */
    private String database = "tsdb";
    /**
     * SQL strategy for strict composite cursor continuation pages: {@code or} (default) or
     * {@code union-all}. UNION ALL avoids older planners' mixed time/tag OR limitation, but may
     * scan the measurement once per cursor column. The adapter captures this setting at construction.
     */
    private StrictCursorSqlStrategyEnum strictCursorSql = StrictCursorSqlStrategyEnum.OR;
    /**
     * Maximum records per business batch; larger batches are rejected before any HTTP request.
     */
    private int maxBatchRecords = 10_000;
    /**
     * Maximum result rows for native SQL and ordinary structured queries. An explicit pagination
     * probe may read one extra sentinel. Exceeding the limit fails rather than returning a partial result.
     */
    private int maxQueryRows = 10_000;
    /**
     * Maximum decompressed bytes read from one successful query response, including JSON syntax.
     * Enforced while streaming, even when the server does not supply Content-Length.
     */
    private long maxQueryResponseBytes = 16L * 1024 * 1024;
    /**
     * InfluxDB HTTP client settings.
     */
    private HttpClientConfig httpClient = new HttpClientConfig();

    /**
     * InfluxDB HTTP client connection settings.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    @Data
    public static class HttpClientConfig {
        /**
         * Maximum number of idle connections in the HTTP connection pool.
         */
        private int maxIdleConnections = 8;
        /**
         * HTTP idle connection keep-alive duration in milliseconds.
         */
        private long keepAliveDurationMs = 300000L;
        /**
         * HTTP connection establishment timeout in milliseconds.
         */
        private long connectTimeoutMs = 3000L;
        /**
         * HTTP response read timeout in milliseconds.
         */
        private long readTimeoutMs = 60000L;
        /**
         * HTTP request write timeout in milliseconds.
         */
        private long writeTimeoutMs = 60000L;
        /**
         * Total timeout for an HTTP call in milliseconds; 0 disables the overall deadline.
         */
        private long callTimeoutMs = 0L;
        /**
         * Whether the HTTP client automatically retries connection failures.
         */
        private boolean retryOnConnectionFailure = true;
    }
}
