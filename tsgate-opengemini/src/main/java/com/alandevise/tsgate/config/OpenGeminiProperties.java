package com.alandevise.tsgate.config;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Connection and resource limits for the openGemini default-engine InfluxQL adapter.
 */
@Data
@ConfigurationProperties(prefix = "tsdb.opengemini")
public class OpenGeminiProperties {
    /**
     * Enables Spring Boot auto-configuration for this backend; defaults to false.
     * Connection settings alone do not enable it. Configure enable=true explicitly to activate the starter.
     * This flag does not control adapters constructed and initialized directly by application code.
     */
    private boolean enable = false;
    /**
     * Whether resource initialization failure aborts application startup; defaults to true.
     * Initialization does not probe server connectivity or database existence.
     */
    private boolean failFast = true;
    /**
     * HTTP(S) ts-sql entry point or external load-balancer URL; defaults to http://localhost:8086.
     */
    private String url = "http://localhost:8086";
    /**
     * Default database, tsdb, for writes and queries that do not explicitly specify one; must already exist.
     */
    private String database = "tsdb";
    /**
     * Blank credentials select unauthenticated access.
     */
    private String username = "";
    /**
     * Password for the configured username; defaults to blank and is excluded from toString output.
     */
    @ToString.Exclude
    private String password = "";
    /**
     * Blank selects the database's default retention policy.
     */
    private String retentionPolicy = "";
    /**
     * Maximum records per adapter batch operation before request splitting; defaults to 10,000.
     */
    private int maxBatchRecords = 10_000;
    /**
     * Maximum combined UTF-8 line-protocol payload bytes per application batch; defaults to 64 MiB.
     * Includes newlines within each physical request, with no separator between requests.
     * The entire batch is validated and encoded before I/O; oversized batches are rejected without writes.
     */
    private long maxBatchBytes = 64L * 1024 * 1024;
    /**
     * Ordinary and native queries share this row cap, defaulting to 10,000; pagination probes may read one extra row.
     * Count scans are exempt from the row cap but retain the response-byte limit.
     */
    private int maxQueryRows = 10_000;
    /**
     * Maximum decompressed JSON bytes per query, including count scans; defaults to 16 MiB.
     */
    private long maxQueryResponseBytes = 16L * 1024 * 1024;
}
