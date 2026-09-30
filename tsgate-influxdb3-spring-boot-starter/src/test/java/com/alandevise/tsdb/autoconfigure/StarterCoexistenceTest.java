package com.alandevise.tsdb.autoconfigure;

import com.alandevise.tsdb.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsdb.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsdb.config.InfluxDBProperties;
import com.alandevise.tsdb.core.TGTemplate;
import com.influxdb.v3.client.InfluxDBClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class StarterCoexistenceTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    IoTDBTSDBAutoConfiguration.class, InfluxDBTSDBAutoConfiguration.class));
    @Test void bothStartersRemainInactiveWithoutBackendConfiguration() {
        runner.run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(TGTemplate.class)
                .doesNotHaveBean(InfluxDBClient.class).doesNotHaveBean(IoTDBTableAdapter.class));
    }
    @Test void twoExplicitlyEnabledBackendsFailBeforeClientCreation() {
        runner.withPropertyValues("tsdb.iotdb.enable=true", "tsdb.influxdb.enable=true")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("Only one TSDB adapter may be enabled");
                });
    }
    @Test void bothDisabledCreatesNoBackend() {
        runner.withPropertyValues("tsdb.iotdb.enable=false", "tsdb.influxdb.enable=false")
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(TGTemplate.class)
                        .doesNotHaveBean(InfluxDBClient.class).doesNotHaveBean(IoTDBTableAdapter.class));
    }
    private ApplicationContextRunner influx() {
        return runner.withPropertyValues("tsdb.influxdb.enable=true", "tsdb.influxdb.url=http://127.0.0.1:1",
                "tsdb.influxdb.database=test_db");
    }
    @Test void omittedDatabaseBindsTsdbAndCreatesTheNativeClient() {
        runner.withPropertyValues("tsdb.influxdb.enable=true", "tsdb.influxdb.url=http://127.0.0.1:1")
                .run(context -> {
                    assertThat(context).hasNotFailed().hasSingleBean(InfluxDBClient.class);
                    assertThat(context.getBean(InfluxDBProperties.class).getDatabase()).isEqualTo("tsdb");
                });
    }
    @Test void selectsInfluxBindsAndExposesSameNativeClient() {
        influx().withPropertyValues("tsdb.influxdb.max-batch-records=31",
                "tsdb.influxdb.http-client.connect-timeout-ms=1234").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(TGTemplate.class)
                    .hasSingleBean(InfluxDBAdapter.class).hasSingleBean(InfluxDBClient.class)
                    .doesNotHaveBean(IoTDBTableAdapter.class);
            assertThat(context.getBean(InfluxDBProperties.class).getDatabase()).isEqualTo("test_db");
            assertThat(context.getBean(InfluxDBProperties.class).getMaxBatchRecords()).isEqualTo(31);
            assertThat(context.getBean(InfluxDBClient.class)).isSameAs(context.getBean(InfluxDBAdapter.class).getNativeClient());
        });
    }
    @ParameterizedTest @ValueSource(strings = {"tsdb.influxdb.url=", "tsdb.influxdb.database=", "tsdb.influxdb.max-batch-records=0"})
    void invalidConfigurationStillFailsWithoutFailFast(String property) {
        influx().withPropertyValues("tsdb.influxdb.fail-fast=false", property)
                .run(context -> assertThat(context).hasFailed());
    }
    @Test void contextsHaveIndependentBackendSelection() {
        influx().run(influxContext -> {
            assertThat(influxContext).hasNotFailed().hasSingleBean(InfluxDBAdapter.class);
            runner.withPropertyValues("tsdb.iotdb.enable=true", "tsdb.iotdb.username=root",
                    "tsdb.iotdb.password=test-password", "tsdb.iotdb.database=test_db",
                    "tsdb.iotdb.pool.node-urls[0]=127.0.0.1:1").run(iotdbContext -> {
                assertThat(iotdbContext).hasNotFailed().hasSingleBean(IoTDBTableAdapter.class)
                        .doesNotHaveBean(InfluxDBAdapter.class);
            });
        });
    }
}
