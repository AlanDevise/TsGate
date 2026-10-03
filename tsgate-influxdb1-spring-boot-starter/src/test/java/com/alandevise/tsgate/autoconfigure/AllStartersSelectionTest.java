package com.alandevise.tsgate.autoconfigure;

import com.alandevise.tsgate.adapter.impl.InfluxDB1Adapter;
import com.alandevise.tsgate.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.influxdb.InfluxDB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockConstruction;

class AllStartersSelectionTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    IoTDBTSDBAutoConfiguration.class, InfluxDBTSDBAutoConfiguration.class,
                    InfluxDB1TSDBAutoConfiguration.class));

    @ParameterizedTest
    @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void oneExplicitlyEnabledBackendWorksWithoutOtherEnableFlags(String selected) {
        configured(selected).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(TGTemplate.class).hasSingleBean(TSDBAdapter.class);
            assertThat(context.getBeansOfType(TGTemplate.class)).containsOnlyKeys("tgTemplate");
            assertThat(context.getBean("tgTemplate", TGTemplate.class)).isSameAs(context.getBean(TGTemplate.class));
            assertThat(context.getBeansOfType(IoTDBTableAdapter.class)).hasSize("iotdb".equals(selected) ? 1 : 0);
            assertThat(context.getBeansOfType(InfluxDBAdapter.class)).hasSize("influxdb".equals(selected) ? 1 : 0);
            assertThat(context.getBeansOfType(InfluxDB1Adapter.class)).hasSize("influxdb1".equals(selected) ? 1 : 0);
            assertThat(context.getBeansOfType(ITableSessionPool.class)).hasSize("iotdb".equals(selected) ? 1 : 0);
            assertThat(context.getBeansOfType(InfluxDBClient.class)).hasSize("influxdb".equals(selected) ? 1 : 0);
            assertThat(context.getBeansOfType(InfluxDB.class)).hasSize("influxdb1".equals(selected) ? 1 : 0);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void customTgTemplateMakesEachStarterBackOffByType(String selected) {
        TGTemplate businessTemplate = new TGTemplate(null);
        configured(selected).withBean("businessTemplate", TGTemplate.class, () -> businessTemplate)
                .run(context -> {
                    assertThat(context).hasNotFailed().hasSingleBean(TGTemplate.class)
                            .hasSingleBean(TSDBAdapter.class).doesNotHaveBean("tgTemplate");
                    assertThat(context.getBeansOfType(TGTemplate.class)).containsOnlyKeys("businessTemplate");
                    assertThat(context.getBean(TGTemplate.class)).isSameAs(businessTemplate);
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void explicitlyDisabledBackendIgnoresItsInvalidProperties(String selected) {
        runner.withPropertyValues("tsdb." + selected + ".enable=false", "tsdb." + selected + ".database=",
                        "tsdb." + selected + ".max-query-rows=not-an-integer")
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(TSDBAdapter.class)
                        .doesNotHaveBean(TGTemplate.class).doesNotHaveBean(ITableSessionPool.class)
                        .doesNotHaveBean(InfluxDBClient.class).doesNotHaveBean(InfluxDB.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void explicitTruePreservesNormalConfigurationValidation(String selected) {
        runner.withPropertyValues("tsdb." + selected + ".enable=true", "tsdb." + selected + ".database=")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test void multipleExplicitlyEnabledBackendsFailBeforeAnyAdapterConstructorRuns() {
        try (MockedConstruction<IoTDBTableAdapter> iotdb = mockConstruction(IoTDBTableAdapter.class);
             MockedConstruction<InfluxDBAdapter> influx3 = mockConstruction(InfluxDBAdapter.class);
             MockedConstruction<InfluxDB1Adapter> influx1 = mockConstruction(InfluxDB1Adapter.class)) {
            runner.withPropertyValues("tsdb.iotdb.enable=true", "tsdb.influxdb.enable=true",
                    "tsdb.influxdb1.enable=true").run(context -> {
                assertThat(context).hasFailed();
                Throwable cause = context.getStartupFailure();
                while (cause != null && !(cause instanceof TSDBException)) cause = cause.getCause();
                assertThat(cause).isInstanceOf(TSDBException.class);
                assertThat(((TSDBException) cause).getErrorCode()).isEqualTo(TSDBErrorCodeEnum.CONFIGURATION_ERROR);
                assertThat(cause).hasMessageContaining("iotdb").hasMessageContaining("influxdb").hasMessageContaining("influxdb1");
            });
            assertThat(iotdb.constructed()).isEmpty();
            assertThat(influx3.constructed()).isEmpty();
            assertThat(influx1.constructed()).isEmpty();
        }
    }

    @Test void disablingOneConfiguredBackendLeavesTheOtherBackendSelected() {
        configured("influxdb1").withPropertyValues("tsdb.iotdb.enable=false", "tsdb.iotdb.database=unused_db",
                        "tsdb.iotdb.pool.node-urls[0]=${MISSING_DISABLED_ADDRESS}")
                .run(context -> assertThat(context).hasNotFailed().hasSingleBean(InfluxDB1Adapter.class)
                        .hasSingleBean(TGTemplate.class).doesNotHaveBean(IoTDBTableAdapter.class));
    }

    @Test void noBackendSettingsOrSharedSettingsCreateNoAdapterOrClient() {
        runner.withPropertyValues("tsdb.query-log-enabled=true").run(context ->
                assertThat(context).hasNotFailed().doesNotHaveBean(TSDBAdapter.class)
                        .doesNotHaveBean(TGTemplate.class).doesNotHaveBean(ITableSessionPool.class)
                        .doesNotHaveBean(InfluxDBClient.class).doesNotHaveBean(InfluxDB.class));
    }

    @Test void configuredBackendsWithoutFlagsNeverBindOrConstructClients() {
        assertThat(new com.alandevise.tsgate.config.IoTDBProperties().isEnable()).isFalse();
        assertThat(new com.alandevise.tsgate.config.InfluxDBProperties().isEnable()).isFalse();
        assertThat(new com.alandevise.tsgate.config.InfluxDB1Properties().isEnable()).isFalse();
        try (MockedConstruction<IoTDBTableAdapter> iotdb = mockConstruction(IoTDBTableAdapter.class);
             MockedConstruction<InfluxDBAdapter> influx3 = mockConstruction(InfluxDBAdapter.class);
             MockedConstruction<InfluxDB1Adapter> influx1 = mockConstruction(InfluxDB1Adapter.class)) {
            runner.withPropertyValues("tsdb.iotdb.username=${UNRESOLVED_INACTIVE_USERNAME}",
                    "tsdb.iotdb.pool.max-size=not-an-integer", "tsdb.influxdb.url=",
                    "tsdb.influxdb1.max-query-rows=not-an-integer").run(context -> {
                assertThat(context).hasNotFailed().doesNotHaveBean(TSDBAdapter.class)
                        .doesNotHaveBean(TGTemplate.class).doesNotHaveBean(ITableSessionPool.class)
                        .doesNotHaveBean(InfluxDBClient.class).doesNotHaveBean(InfluxDB.class)
                        .doesNotHaveBean(com.alandevise.tsgate.config.IoTDBProperties.class)
                        .doesNotHaveBean(com.alandevise.tsgate.config.InfluxDBProperties.class)
                        .doesNotHaveBean(com.alandevise.tsgate.config.InfluxDB1Properties.class);
            });
            assertThat(iotdb.constructed()).isEmpty();
            assertThat(influx3.constructed()).isEmpty();
            assertThat(influx1.constructed()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void plainApplicationYamlEnablesOnlyTheSelectedBackendWithoutProfiles(String selected, @TempDir Path directory) throws Exception {
        Path configuration = directory.resolve("application.yml");
        String connection = "iotdb".equals(selected)
                ? "    username: root\n    password: test-password\n    pool:\n      node-urls:\n        - 127.0.0.1:1\n"
                : "    url: http://127.0.0.1:1\n";
        Files.writeString(configuration, "tsdb:\n  " + selected + ":\n    enable: true\n" + connection);
        // Load ordinary application YAML before evaluating auto-configuration conditions.
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class))
                .withUserConfiguration(ConfiguredApplication.class)
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("spring.config.location=" + configuration.toUri(), "spring.profiles.active=")
                .run(context -> {
                    assertThat(context).hasNotFailed().hasSingleBean(TGTemplate.class).hasSingleBean(TSDBAdapter.class);
                    assertThat(context.getEnvironment().getActiveProfiles()).isEmpty();
                    assertThat(context.getBeansOfType(IoTDBTableAdapter.class)).hasSize("iotdb".equals(selected) ? 1 : 0);
                    assertThat(context.getBeansOfType(InfluxDBAdapter.class)).hasSize("influxdb".equals(selected) ? 1 : 0);
                    assertThat(context.getBeansOfType(InfluxDB1Adapter.class)).hasSize("influxdb1".equals(selected) ? 1 : 0);
                });
    }

    @Configuration(proxyBeanMethods = false)
    @Import({IoTDBTSDBAutoConfiguration.class, InfluxDBTSDBAutoConfiguration.class, InfluxDB1TSDBAutoConfiguration.class})
    static class ConfiguredApplication { }

    private ApplicationContextRunner configured(String selected) {
        if ("iotdb".equals(selected)) {
            return runner.withPropertyValues("tsdb.iotdb.enable=true", "tsdb.iotdb.username=root", "tsdb.iotdb.password=test-password",
                    "tsdb.iotdb.pool.node-urls[0]=127.0.0.1:1");
        }
        return runner.withPropertyValues("tsdb." + selected + ".enable=true", "tsdb." + selected + ".url=http://127.0.0.1:1");
    }
}
