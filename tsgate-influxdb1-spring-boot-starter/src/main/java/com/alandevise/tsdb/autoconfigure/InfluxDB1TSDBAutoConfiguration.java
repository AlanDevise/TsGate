package com.alandevise.tsdb.autoconfigure;

import com.alandevise.tsdb.adapter.impl.InfluxDB1Adapter;
import com.alandevise.tsdb.config.InfluxDB1HttpClientProperties;
import com.alandevise.tsdb.config.InfluxDB1Properties;
import com.alandevise.tsdb.config.TSDBAdapterEnabledCondition;
import com.alandevise.tsdb.config.TSDBDeploymentProperties;
import com.alandevise.tsdb.core.TGTemplate;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.metadata.DefaultTSDBMetadataResolver;
import com.alandevise.tsdb.metadata.TSDBMetadataResolver;
import com.alandevise.tsdb.util.TsGateBanner;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for InfluxDB 1.x, the shared template, and the official native client.
 * Initialization creates local client resources without connecting to or creating a database.
 * Only the selected backend is configured when multiple TSDB starters are present.
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({TGTemplate.class, InfluxDB1Adapter.class, InfluxDB.class})
@Conditional(TSDBAdapterEnabledCondition.InfluxDB1.class)
@EnableConfigurationProperties({InfluxDB1Properties.class, InfluxDB1HttpClientProperties.class})
public class InfluxDB1TSDBAutoConfiguration {

    /**
     * Binds shared deployment settings when the application does not supply them.
     *
     * @return shared TSDB deployment settings
     */
    @Bean
    @ConditionalOnMissingBean
    public TSDBDeploymentProperties tsdbDeploymentProperties() {
        return new TSDBDeploymentProperties();
    }

    /**
     * Supplies the default annotation mapper, allowing applications to replace it.
     *
     * @return annotation metadata resolver
     */
    @Bean
    @ConditionalOnMissingBean
    public TSDBMetadataResolver tsdbMetadataResolver() {
        return new DefaultTSDBMetadataResolver();
    }

    /**
     * Validates configuration and initializes the selected InfluxDB 1.x adapter.
     *
     * @param properties           connection and operation settings
     * @param httpProperties       HTTP resource and timeout settings
     * @param deploymentProperties shared SQL logging settings
     * @return the adapter whose lifecycle is owned by the Spring context
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public InfluxDB1Adapter influxDB1Adapter(InfluxDB1Properties properties,
                                             InfluxDB1HttpClientProperties httpProperties,
                                             TSDBDeploymentProperties deploymentProperties) {
        validateConfiguration(properties);
        InfluxDB1Adapter adapter = new InfluxDB1Adapter(properties, httpProperties,
                deploymentProperties.isQueryLogEnabled());
        try {
            adapter.init();
            log.info("InfluxDB 1.x adapter registered");
            TsGateBanner.print(log, "InfluxDB1");
        } catch (RuntimeException failure) {
            if (properties.isFailFast()) {
                adapter.close();
                throw failure;
            }
            log.error("Failed to initialize InfluxDB 1.x adapter", failure);
        }
        return adapter;
    }

    /**
     * Creates the shared template for application writes and queries.
     *
     * @param adapter          selected InfluxDB 1.x adapter
     * @param metadataResolver annotation metadata resolver
     * @return application template
     */
    @Bean
    @ConditionalOnMissingBean
    public TGTemplate tgTemplate(InfluxDB1Adapter adapter, TSDBMetadataResolver metadataResolver) {
        return new TGTemplate(adapter, metadataResolver);
    }

    /**
     * Exposes a borrowed official client without registering a second resource destroy method.
     * When fail-fast is disabled and initialization failed, the native client is unavailable;
     * applications should use optional injection and obtain a client directly from the adapter after a manual retry.
     * An unavailable native-client bean is not automatically recreated after retrying initialization.
     *
     * @param adapter    adapter that owns the native client
     * @param properties startup failure policy
     * @return borrowed native client, or null after a tolerated initialization failure
     */
    @Bean(destroyMethod = "")
    @ConditionalOnMissingBean
    public InfluxDB influxDB1Client(InfluxDB1Adapter adapter, InfluxDB1Properties properties) {
        try {
            InfluxDB client = adapter.getNativeClient();
            if (client == null) {
                throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                        "InfluxDB 1.x native client is not available");
            }
            return client;
        } catch (TSDBException failure) {
            if (properties.isFailFast() || failure.getErrorCode() != TSDBErrorCodeEnum.ADAPTER_STATE_ERROR) {
                throw failure;
            }
            log.warn("InfluxDB 1.x native client is unavailable after initialization failure");
            return null;
        }
    }

    /**
     * Rejects missing required settings regardless of the initialization failure policy.
     */
    private static void validateConfiguration(InfluxDB1Properties properties) {
        if (properties == null) {
            throw configurationError("tsdb.influxdb1 must not be null");
        }
        requireText(properties.getUrl(), "tsdb.influxdb1.url");
        requireText(properties.getDatabase(), "tsdb.influxdb1.database");
        if (properties.getMaxBatchRecords() <= 0) {
            throw configurationError("tsdb.influxdb1.max-batch-records must be greater than 0");
        }
        if (properties.getMaxQueryRows() <= 0 || properties.getMaxQueryRows() == Integer.MAX_VALUE) {
            throw configurationError("tsdb.influxdb1.max-query-rows must be between 1 and 2147483646");
        }
        if (properties.getMaxQueryResponseBytes() <= 0) {
            throw configurationError("tsdb.influxdb1.max-query-response-bytes must be greater than 0");
        }
    }

    private static void requireText(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            throw configurationError("Required TSDB configuration is empty: " + propertyName);
        }
    }

    private static TSDBException configurationError(String message) {
        return new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR, message);
    }
}
