package com.alandevise.tsdb.autoconfigure;

import com.influxdb.v3.client.InfluxDBClient;
import com.alandevise.tsdb.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsdb.config.InfluxDBProperties;
import com.alandevise.tsdb.config.TSDBDeploymentProperties;
import com.alandevise.tsdb.config.TSDBAdapterEnabledCondition;
import com.alandevise.tsdb.core.TGTemplate;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.metadata.DefaultTSDBMetadataResolver;
import com.alandevise.tsdb.metadata.TSDBMetadataResolver;
import com.alandevise.tsdb.util.TsGateBanner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for the InfluxDB TSDB adapter starter.
 * <p>This starter configures the InfluxDB adapter, unified template, and official InfluxDB client bean.
 * Applications requiring manual configuration can depend directly on {@code tsgate-influxdb3}.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-07
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({TGTemplate.class, InfluxDBAdapter.class})
@Conditional(TSDBAdapterEnabledCondition.InfluxDB.class)
public class InfluxDBTSDBAutoConfiguration {

    /**
     * Binds common TSDB deployment properties.
     * <p>InfluxDB always uses the configured URL without IoTDB-style node discovery; these properties keep the configuration model consistent.</p>
     *
     * @return common TSDB deployment properties, for example {@code tsdb.query-log-enabled=true}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-14
     */
    @Bean
    @ConditionalOnMissingBean
    public TSDBDeploymentProperties tsdbDeploymentProperties() {
        return new TSDBDeploymentProperties();
    }

    /**
     * Binds the InfluxDB adapter configuration properties.
     *
     * @return InfluxDB configuration properties, for example {@code tsdb.influxdb.database=tsdb}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    @Bean
    @ConditionalOnMissingBean
    public InfluxDBProperties influxDBProperties() {
        return new InfluxDBProperties();
    }

    /**
     * Creates the default annotation metadata resolver, which applications can replace with their own bean.
     *
     * @return default annotation metadata resolver
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    @Bean
    @ConditionalOnMissingBean
    public TSDBMetadataResolver tsdbMetadataResolver() {
        return new DefaultTSDBMetadataResolver();
    }

    /**
     * Creates and initializes the InfluxDB adapter.
     *
     * @param properties           InfluxDB configuration properties, for example {@code tsdb.influxdb.url=http://localhost:8181}
     * @param deploymentProperties common TSDB deployment properties, for example {@code tsdb.query-log-enabled=true}
     * @return InfluxDB adapter
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public InfluxDBAdapter influxDBAdapter(InfluxDBProperties properties,
                                           TSDBDeploymentProperties deploymentProperties) {
        validateInfluxDBConfig(properties);
        InfluxDBAdapter adapter = new InfluxDBAdapter(properties, deploymentProperties.isQueryLogEnabled());
        initializeAdapter(adapter, properties);
        return adapter;
    }

    /**
     * Creates the unified TSDB template for application code.
     *
     * @param adapter          InfluxDB adapter, for example the auto-configured {@code InfluxDBAdapter}
     * @param metadataResolver metadata resolver, for example {@code DefaultTSDBMetadataResolver}
     * @return unified application template
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    @Bean
    @ConditionalOnMissingBean
    public TGTemplate tgTemplate(InfluxDBAdapter adapter,
                                     TSDBMetadataResolver metadataResolver) {
        return new TGTemplate(adapter, metadataResolver);
    }

    /**
     * Exposes the official InfluxDB Java client for direct native access through {@code @Autowired InfluxDBClient}.
     *
     * @param adapter InfluxDB adapter, for example the auto-configured {@code InfluxDBAdapter}
     * @return official InfluxDB Java client
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    @Bean(destroyMethod = "")
    @ConditionalOnMissingBean
    public InfluxDBClient influxDBClient(InfluxDBAdapter adapter) {
        InfluxDBClient nativeClient = adapter.getNativeClient();
        if (nativeClient == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                    "InfluxDB native client is not available");
        }
        return nativeClient;
    }

    /**
     * Initializes adapter resources and applies the fail-fast policy to initialization failures.
     * Initialization does not access the default database.
     *
     * @param adapter    InfluxDB adapter, for example {@code new InfluxDBAdapter(config)}
     * @param properties InfluxDB configuration properties, for example {@code failFast=true}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    private static void initializeAdapter(InfluxDBAdapter adapter,
                                          InfluxDBProperties properties) {
        try {
            adapter.init();
            log.info("InfluxDB adapter registered");
            TsGateBanner.print(log, "InfluxDB");
        } catch (RuntimeException e) {
            if (properties.isFailFast()) {
                adapter.close();
                throw e;
            }
            log.error("Failed to initialize InfluxDB adapter", e);
        }
    }

    /**
     * Validates required InfluxDB adapter settings.
     *
     * @param properties InfluxDB configuration properties, for example {@code tsdb.influxdb.database=tsdb}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    private static void validateInfluxDBConfig(InfluxDBProperties properties) {
        if (properties == null) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Required TSDB configuration is empty: tsdb.influxdb");
        }
        requireText(properties.getUrl(), "tsdb.influxdb.url");
        requireText(properties.getDatabase(), "tsdb.influxdb.database");
        if (properties.getMaxBatchRecords() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.influxdb.max-batch-records must be greater than 0");
        }
    }

    /**
     * Validates that a required string setting is not blank.
     *
     * @param value        configuration value, for example {@code "http://localhost:8181"}
     * @param propertyName configuration property name, for example {@code "tsdb.influxdb.url"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static void requireText(String value,
                                    String propertyName) {
        if (value == null || value.trim().isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Required TSDB configuration is empty: " + propertyName);
        }
    }

}
