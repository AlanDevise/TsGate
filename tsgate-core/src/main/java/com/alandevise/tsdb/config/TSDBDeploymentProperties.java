package com.alandevise.tsdb.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Common deployment settings for TSDB adapters.
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-14
 */
@Data
@ConfigurationProperties(prefix = "tsdb")
public class TSDBDeploymentProperties {

    /**
     * Whether to log executed query SQL at DEBUG level; enabled by default.
     */
    private boolean queryLogEnabled = true;
}
