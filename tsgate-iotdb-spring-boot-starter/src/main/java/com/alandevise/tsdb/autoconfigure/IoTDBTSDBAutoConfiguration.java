package com.alandevise.tsdb.autoconfigure;

import com.alandevise.tsdb.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.config.TSDBDeploymentProperties;
import com.alandevise.tsdb.config.TSDBAdapterEnabledCondition;
import com.alandevise.tsdb.core.TGTemplate;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.metadata.DefaultTSDBMetadataResolver;
import com.alandevise.tsdb.metadata.TSDBMetadataResolver;
import com.alandevise.tsdb.util.TsGateBanner;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for the IoTDB TSDB adapter starter.
 * <p>Registers the IoTDB table-model adapter, shared template, and official client bean.
 * Applications that manage their own configuration may depend on {@code tsgate-iotdb} alone.</p>
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-07
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({TGTemplate.class, IoTDBTableAdapter.class})
@Conditional(TSDBAdapterEnabledCondition.IoTDB.class)
public class IoTDBTSDBAutoConfiguration {

    /**
     * Bind shared TSDB deployment settings.
     * @return shared TSDB settings, for example {@code tsdb.query-log-enabled=true}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-14
     */
    @Bean
    @ConditionalOnMissingBean
    public TSDBDeploymentProperties tsdbDeploymentProperties() {
        return new TSDBDeploymentProperties();
    }

    /**
     * Bind IoTDB adapter settings.
     * @return IoTDB settings, for example {@code tsdb.iotdb.database=tsdb}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    @Bean
    @ConditionalOnMissingBean
    public IoTDBProperties iotdbProperties() {
        return new IoTDBProperties();
    }

    /**
     * Create the default annotation metadata resolver, which applications can replace with a custom bean.
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
     * Create and initialize the IoTDB table-model adapter.
     * @param properties IoTDB adapter settings; for example {@code tsdb.iotdb.pool.node-urls[0]=127.0.0.1:16669}
     * @param deploymentProperties shared TSDB deployment settings; for example {@code tsdb.query-log-enabled=true}
     * @return IoTDB table-model adapter
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public IoTDBTableAdapter iotdbTableAdapter(IoTDBProperties properties,
                                               TSDBDeploymentProperties deploymentProperties) {
        validateIoTDBConfig(properties);
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(properties, properties.getPool(),
                deploymentProperties.isQueryLogEnabled());
        initializeAdapter(adapter, properties);
        return adapter;
    }

    /**
     * Create the application's shared TSDB template.
     * @param adapter IoTDB table-model adapter; for example {@code IoTDBTableAdapter}
     * @param metadataResolver annotation metadata resolver; for example {@code DefaultTSDBMetadataResolver}
     * @return shared application template
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    @Bean
    @ConditionalOnMissingBean
    public TGTemplate tgTemplate(IoTDBTableAdapter adapter,
                                     TSDBMetadataResolver metadataResolver) {
        return new TGTemplate(adapter, metadataResolver);
    }

    /**
     * Expose the official IoTDB table SessionPool for native operations through {@code @Autowired ITableSessionPool}.
     * When fail-fast is disabled and initialization failed, the native pool is unavailable;
     * applications should use optional injection and obtain a pool directly from the adapter after a manual retry.
     * An unavailable native-pool bean is not automatically recreated after retrying initialization.
     * @param adapter IoTDB table-model adapter; for example {@code IoTDBTableAdapter}
     * @param properties startup failure policy
     * @return borrowed official IoTDB table-model pool, or null after a tolerated initialization failure
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    @Bean(destroyMethod = "")
    @ConditionalOnMissingBean
    public ITableSessionPool iotdbTableSessionPool(IoTDBTableAdapter adapter, IoTDBProperties properties) {
        try {
            return iotdbTableSessionPool(adapter);
        } catch (TSDBException failure) {
            if (properties.isFailFast() || failure.getErrorCode() != TSDBErrorCodeEnum.ADAPTER_STATE_ERROR) {
                throw failure;
            }
            log.warn("IoTDB table SessionPool is unavailable after initialization failure");
            return null;
        }
    }

    /**
     * Obtains the borrowed official pool with the original strict availability contract.
     * Spring uses the overload accepting properties to apply its configured startup failure policy.
     *
     * @param adapter adapter that owns the native pool
     * @return borrowed official IoTDB table-model pool
     * @throws TSDBException if the native pool is unavailable
     */
    public ITableSessionPool iotdbTableSessionPool(IoTDBTableAdapter adapter) {
        ITableSessionPool sessionPool = adapter.getSessionPool();
        if (sessionPool == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                    "IoTDB table SessionPool is not available");
        }
        return sessionPool;
    }

    /**
     * Initialize client resources, honoring fail-fast when resource creation fails.
     * The pool binds the default database; physical sessions connect and validate it only on first borrow.
     * @param adapter IoTDB table-model adapter; for example {@code new IoTDBTableAdapter(config, pool)}
     * @param properties IoTDB adapter settings; for example {@code failFast=true}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    private static void initializeAdapter(IoTDBTableAdapter adapter,
                                          IoTDBProperties properties) {
        try {
            adapter.init();
            log.info("IoTDB table adapter registered");
            TsGateBanner.print(log, "IoTDB");
        } catch (RuntimeException e) {
            if (properties.isFailFast()) {
                adapter.close();
                throw e;
            }
            log.error("Failed to initialize IoTDB table adapter", e);
        }
    }

    /**
     * Validate the required IoTDB adapter configuration.
     * @param properties IoTDB adapter settings; for example {@code tsdb.iotdb.username=root}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-08
     */
    private static void validateIoTDBConfig(IoTDBProperties properties) {
        if (properties == null) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Required TSDB configuration is empty: tsdb.iotdb");
        }
        if (properties.getTable() == null) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Required TSDB configuration is empty: tsdb.iotdb.table");
        }
        if (properties.getTable().getTabletMaxRowSize() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.iotdb.table.tablet-max-row-size must be greater than 0");
        }
        if (properties.getMaxBatchRecords() <= 0) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.iotdb.max-batch-records must be greater than 0");
        }
        if (properties.getMaxQueryRows() <= 0 || properties.getMaxQueryRows() == Integer.MAX_VALUE) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "tsdb.iotdb.max-query-rows must be between 1 and 2147483646");
        }
        IoTDBProperties.IoTDBPoolConfig pool = properties.getPool();
        if (pool == null || pool.getNodeUrls() == null || pool.getNodeUrls().isEmpty()) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Required TSDB configuration is empty: tsdb.iotdb.pool.node-urls");
        }
        if (!pool.isEnabled()) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "IoTDB single Session mode is disabled; tsdb.iotdb.pool.enabled must be true");
        }
        requireText(properties.getUsername(), "tsdb.iotdb.username");
        requireText(properties.getPassword(), "tsdb.iotdb.password");
        // Create the default database before deployment and retain it as the pool's stable baseline during operation.
        requireText(properties.getDatabase(), "tsdb.iotdb.database");
    }

    /**
     * Validate that a string setting is not blank.
     * @param value source value; for example {@code "root"}
     * @param propertyName configuration property name; for example {@code "tsdb.iotdb.username"}
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
