package com.alandevise.tsdb.config;

import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Allows multiple adapter dependencies while enabling at most one per Spring context.
 * Only an explicit {@code tsdb.<backend>.enable=true} activates a backend.
 * Adapters absent from the classpath and backends with missing or false flags remain inactive,
 * regardless of connection properties. Ordinary application.yml configuration is sufficient.
 * Validates during configuration parsing to prevent bean-name conflicts or premature client initialization.
 */
public class TSDBAdapterEnabledCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return !enabledAdapters(context).isEmpty();
    }

    private static List<String> enabledAdapters(ConditionContext context) {
        List<String> enabled = new ArrayList<>(3);
        addIfEnabled(context, enabled, "iotdb", "com.alandevise.tsdb.adapter.impl.IoTDBTableAdapter");
        addIfEnabled(context, enabled, "influxdb", "com.alandevise.tsdb.adapter.impl.InfluxDBAdapter");
        addIfEnabled(context, enabled, "influxdb1", "com.alandevise.tsdb.adapter.impl.InfluxDB1Adapter");
        if (enabled.size() > 1) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Only one TSDB adapter may be enabled; active adapters: " + String.join(", ", enabled)
                            + ". Set tsdb.<adapter>.enable=true for only one backend.");
        }
        return enabled;
    }

    private static void addIfEnabled(ConditionContext context, List<String> enabled,
                                     String type, String adapterClass) {
        if (!ClassUtils.isPresent(adapterClass, context.getClassLoader())) {
            return;
        }
        String prefix = "tsdb." + type;
        ConfigurationPropertyName enableName = ConfigurationPropertyName.of(prefix + ".enable");
        Iterable<ConfigurationPropertySource> sources = ConfigurationPropertySources.get(context.getEnvironment());
        for (ConfigurationPropertySource source : sources) {
            if (source.getConfigurationProperty(enableName) != null) {
                if (bindEnable(context, enableName.toString())) {
                    enabled.add(type);
                }
                return;
            }
        }
    }

    private static boolean bindEnable(ConditionContext context, String propertyName) {
        Boolean enabled;
        try {
            enabled = Binder.get(context.getEnvironment()).bind(propertyName, Boolean.class).orElse(null);
        } catch (RuntimeException ex) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Invalid " + propertyName + ": expected a boolean value", ex);
        }
        if (enabled == null) {
            throw new TSDBException(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    "Invalid " + propertyName + ": expected a boolean value");
        }
        return enabled;
    }

    /**
     * Shared enablement condition for the IoTDB starter and business components.
     */
    public static final class IoTDB implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return enabledAdapters(context).contains("iotdb");
        }
    }

    /**
     * Shared enablement condition for the InfluxDB starter and business components.
     */
    public static final class InfluxDB implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return enabledAdapters(context).contains("influxdb");
        }
    }
    /**
     * Shared enablement condition for the InfluxDB 1.x starter and business components.
     */
    public static final class InfluxDB1 implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return enabledAdapters(context).contains("influxdb1");
        }
    }

}
