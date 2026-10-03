package com.alandevise.tsgate.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * HTTP pool and timeout settings; all duration values are milliseconds.
 */
@Data
@ConfigurationProperties(prefix = "tsdb.influxdb1.http-client")
public class InfluxDB1HttpClientProperties {
    /**
     * Maximum idle connections retained by each HTTP pool; defaults to 8.
     */
    private int maxIdleConnections = 8;
    /**
     * Idle connection lifetime in milliseconds; defaults to 300,000 and must be positive.
     */
    private long keepAliveDurationMs = 300_000L;
    /**
     * Connection timeout in milliseconds; defaults to 3,000. Zero disables this timeout.
     */
    private long connectTimeoutMs = 3_000L;
    /**
     * Read timeout in milliseconds; defaults to 60,000. Zero disables this timeout.
     */
    private long readTimeoutMs = 60_000L;
    /**
     * Write timeout in milliseconds; defaults to 60,000. Zero disables this timeout.
     */
    private long writeTimeoutMs = 60_000L;
    /**
     * Total HTTP call deadline in milliseconds; defaults to zero (disabled).
     * A finite value also bounds graceful shutdown waiting for each in-flight HTTP call.
     */
    private long callTimeoutMs = 0L;
    /**
     * Enables OkHttp recovery from eligible connection failures; defaults to true, without guaranteeing write idempotency.
     */
    private boolean retryOnConnectionFailure = true;
}
