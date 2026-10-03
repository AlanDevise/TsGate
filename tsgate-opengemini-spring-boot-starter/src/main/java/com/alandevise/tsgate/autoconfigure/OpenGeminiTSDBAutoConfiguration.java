package com.alandevise.tsgate.autoconfigure;

import com.alandevise.tsgate.adapter.impl.OpenGeminiAdapter;
import com.alandevise.tsgate.config.OpenGeminiHttpClientProperties;
import com.alandevise.tsgate.config.OpenGeminiProperties;
import com.alandevise.tsgate.config.TSDBAdapterEnabledCondition;
import com.alandevise.tsgate.config.TSDBDeploymentProperties;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.metadata.DefaultTSDBMetadataResolver;
import com.alandevise.tsgate.metadata.TSDBMetadataResolver;
import com.alandevise.tsgate.util.TsGateBanner;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for openGemini, the shared template, and the InfluxDB-compatible native client.
 * Initialization creates local client resources without connecting to or creating a database.
 * Only the selected backend is configured when multiple TSDB starters are present.
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({TGTemplate.class, OpenGeminiAdapter.class, InfluxDB.class})
@Conditional(TSDBAdapterEnabledCondition.OpenGemini.class)
@EnableConfigurationProperties({OpenGeminiProperties.class, OpenGeminiHttpClientProperties.class})
public class OpenGeminiTSDBAutoConfiguration {

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
     * Validates configuration and initializes the selected openGemini adapter.
     *
     * @param properties           connection and operation settings
     * @param httpProperties       HTTP resource and timeout settings
     * @param deploymentProperties shared SQL logging settings
     * @return the adapter whose lifecycle is owned by the Spring context
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public OpenGeminiAdapter openGeminiAdapter(OpenGeminiProperties properties,
                                              OpenGeminiHttpClientProperties httpProperties,
                                              TSDBDeploymentProperties deploymentProperties) {
        validateConfiguration(properties);
        OpenGeminiAdapter adapter = new OpenGeminiAdapter(properties, httpProperties,
                deploymentProperties.isQueryLogEnabled());
        try {
            adapter.init();
            log.info("openGemini adapter registered");
            TsGateBanner.print(log, "OpenGemini");
        } catch (RuntimeException failure) {
            if (properties.isFailFast()) {
                adapter.close();
                throw failure;
            }
            log.error("Failed to initialize openGemini adapter", failure);
        }
        return adapter;
    }

    /**
     * Creates the shared template for application writes and queries.
     *
     * @param adapter          selected openGemini adapter
     * @param metadataResolver annotation metadata resolver
     * @return application template
     */
    @Bean
    @ConditionalOnMissingBean
    public TGTemplate tgTemplate(OpenGeminiAdapter adapter, TSDBMetadataResolver metadataResolver) {
        return new TGTemplate(adapter, metadataResolver);
    }

    /**
     * Exposes a borrowed InfluxDB-compatible client without registering a second resource destroy method.
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
    public InfluxDB openGeminiClient(OpenGeminiAdapter adapter, OpenGeminiProperties properties) {
        try {
            InfluxDB client = adapter.getNativeClient();
            if (client == null) {
                throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                        "openGemini native client is not available");
            }
            return client;
        } catch (TSDBException failure) {
            if (properties.isFailFast() || failure.getErrorCode() != TSDBErrorCodeEnum.ADAPTER_STATE_ERROR) {
                throw failure;
            }
            log.warn("openGemini native client is unavailable after initialization failure");
            return null;
        }
    }

    /**
     * Rejects missing required settings regardless of the initialization failure policy.
     */
    private static void validateConfiguration(OpenGeminiProperties properties) {
        if (properties == null) {
            throw configurationError("tsdb.opengemini must not be null");
        }
        requireText(properties.getUrl(), "tsdb.opengemini.url");
        requireText(properties.getDatabase(), "tsdb.opengemini.database");
        if (properties.getMaxBatchRecords() <= 0) {
            throw configurationError("tsdb.opengemini.max-batch-records must be greater than 0");
        }
        if (properties.getMaxQueryRows() <= 0 || properties.getMaxQueryRows() == Integer.MAX_VALUE) {
            throw configurationError("tsdb.opengemini.max-query-rows must be between 1 and 2147483646");
        }
        if (properties.getMaxQueryResponseBytes() <= 0) {
            throw configurationError("tsdb.opengemini.max-query-response-bytes must be greater than 0");
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
