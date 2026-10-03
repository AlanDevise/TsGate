package com.alandevise.tsgate.config;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Connection settings for the IoTDB adapter.
 * <p>The IoTDB starter binds these settings with {@code @ConfigurationProperties(prefix = "tsdb.iotdb")}.
 * They configure table-model connections, pooling, Tablet batch sizes and RPC compression, query limits,
 * and initialization failure handling.</p>
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-08
 */
@Data
@ConfigurationProperties(prefix = "tsdb.iotdb")
public class IoTDBProperties {

    /**
     * Enables Spring Boot auto-configuration for this backend; defaults to false.
     * Connection settings alone do not enable it. Configure enable=true explicitly to activate the starter.
     * This flag does not control adapters constructed and initialized directly by application code.
     */
    private boolean enable = false;

    private static final int DEFAULT_MAX_BATCH_RECORDS = 10_000;

    /**
     * Whether client-resource initialization failure aborts application startup.
     */
    private boolean failFast = true;
    /**
     * Node discovery and redirection policy; the default depends on the number of endpoints.
     */
    private IoTDBNodeDiscoveryModeEnum discoveryMode = IoTDBNodeDiscoveryModeEnum.AUTO;
    /**
     * IoTDB login username, which the application must explicitly configure.
     */
    private String username;
    /**
     * IoTDB login password, which must be explicitly configured and is excluded from string representations.
     */
    @ToString.Exclude
    private String password;
    /**
     * Default database, tsdb. Create it before deployment and do not drop it while the application runs.
     * The pool binds this database. Writes and structured queries use it unless another database is explicitly requested.
     * Only cross-database operations issue USE; the client restores the default when returning sessions. Native SQL is not rewritten.
     */
    private String database = "tsdb";
    /**
     * Maximum records per application batch; oversized batches are rejected before any write occurs.
     */
    private int maxBatchRecords = DEFAULT_MAX_BATCH_RECORDS;
    /**
     * Maximum materialized rows per query, from 1 to 2147483646.
     * Only queries marked as pagination probes may read one additional sentinel row.
     */
    private int maxQueryRows = 10_000;
    /**
     * Batch-write settings for the IoTDB table model.
     */
    private IoTDBConnectionConfig table = new IoTDBConnectionConfig();
    /**
     * SessionPool settings for the IoTDB table model.
     */
    private IoTDBPoolConfig pool = new IoTDBPoolConfig();

    /**
     * Write settings for the IoTDB table model.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    @Data
    public static class IoTDBConnectionConfig {

        private static final int DEFAULT_TABLET_MAX_ROW_SIZE = 1_024;

        /**
         * Maximum rows per Tablet; defaults to the TSFile Tablet batch size.
         */
        private int tabletMaxRowSize = DEFAULT_TABLET_MAX_ROW_SIZE;

        /**
         * Whether the official client may encode and compress Tablet RPC payloads; enabled by default.
         * Disable this when connecting to servers without Tablet RPC encoding support.
         * This setting is independent of Thrift transport compaction and on-disk compression.
         * It applies whenever the adapter creates or replaces its physical session pool.
         */
        private boolean rpcCompressionEnabled = true;
    }

    /**
     * SessionPool settings for the IoTDB table model.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    @Data
    public static class IoTDBPoolConfig {

        private static final int DEFAULT_MAX_SIZE = 8;
        private static final long DEFAULT_WAIT_TO_GET_SESSION_TIMEOUT_IN_MS = 3_000L;
        private static final int DEFAULT_CONNECTION_TIMEOUT_IN_MS = 3_000;
        private static final long DEFAULT_QUERY_TIMEOUT_IN_MS = 60_000L;
        private static final int DEFAULT_MAX_RETRY_COUNT = 3;
        private static final long DEFAULT_RETRY_INTERVAL_IN_MS = 1_000L;
        private static final int DEFAULT_FETCH_SIZE = 10_000;

        /**
         * Whether to use the IoTDB SessionPool.
         * The non-thread-safe single-session mode is disabled; false causes application startup to fail.
         */
        private boolean enabled = true;
        /**
         * Initial IoTDB endpoints. With discovery-mode=AUTO, use one fixed endpoint for standalone or VIP deployments,
         * and multiple available DataNodes for a cluster.
         */
        private List<String> nodeUrls = new ArrayList<>();
        /**
         * Maximum connections in the SessionPool.
         */
        private int maxSize = DEFAULT_MAX_SIZE;
        /**
         * Maximum wait in milliseconds when no session is available in the pool.
         */
        private long waitToGetSessionTimeoutInMs = DEFAULT_WAIT_TO_GET_SESSION_TIMEOUT_IN_MS;
        /**
         * Connection establishment timeout in milliseconds.
         */
        private int connectionTimeoutInMs = DEFAULT_CONNECTION_TIMEOUT_IN_MS;
        /**
         * Query timeout in milliseconds.
         */
        private long queryTimeoutInMs = DEFAULT_QUERY_TIMEOUT_IN_MS;
        /**
         * Maximum retries after an operation fails.
         */
        private int maxRetryCount = DEFAULT_MAX_RETRY_COUNT;
        /**
         * Delay between retries in milliseconds.
         */
        private long retryIntervalInMs = DEFAULT_RETRY_INTERVAL_IN_MS;
        /**
         * Maximum result rows fetched per client batch; this does not replace the overall query row limit.
         */
        private int fetchSize = DEFAULT_FETCH_SIZE;
    }
}
