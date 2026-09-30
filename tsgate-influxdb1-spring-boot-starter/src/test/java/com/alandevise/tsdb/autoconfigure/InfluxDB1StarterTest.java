package com.alandevise.tsdb.autoconfigure;

import com.alandevise.tsdb.adapter.impl.InfluxDB1Adapter;
import com.alandevise.tsdb.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsdb.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsdb.config.InfluxDB1HttpClientProperties;
import com.alandevise.tsdb.config.InfluxDB1Properties;
import com.alandevise.tsdb.config.TSDBDeploymentProperties;
import com.alandevise.tsdb.core.TGTemplate;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.metadata.TSDBMetadataResolver;
import org.influxdb.InfluxDB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class InfluxDB1StarterTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    IoTDBTSDBAutoConfiguration.class, InfluxDBTSDBAutoConfiguration.class,
                    InfluxDB1TSDBAutoConfiguration.class));

    private ApplicationContextRunner influx1() {
        return runner.withPropertyValues("tsdb.influxdb1.enable=true", "tsdb.influxdb1.url=http://127.0.0.1:1", "tsdb.influxdb1.database=test_db");
    }

    @Test void omittedDatabaseBindsTsdbAndCreatesTheNativeClient() {
        runner.withPropertyValues("tsdb.influxdb1.enable=true", "tsdb.influxdb1.url=http://127.0.0.1:1").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(InfluxDB.class);
            assertThat(context.getBean(InfluxDB1Properties.class).getDatabase()).isEqualTo("tsdb");
        });
    }

    @Test void allThreeStartersRemainInactiveWithoutBackendConfiguration() {
        runner.withPropertyValues("tsdb.query-log-enabled=false").run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(TGTemplate.class)
                    .doesNotHaveBean(IoTDBTableAdapter.class).doesNotHaveBean(InfluxDBAdapter.class)
                    .doesNotHaveBean(InfluxDB1Adapter.class).doesNotHaveBean(InfluxDB.class);
        });
    }

    @ParameterizedTest @ValueSource(strings = {"iotdb", "influxdb"})
    void influx1ConflictsWithEitherOtherEnabledBackend(String backend) {
        influx1().withPropertyValues("tsdb." + backend + ".enable=true").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("Only one TSDB adapter may be enabled");
        });
    }

    @Test void allDisabledIgnoresInvalidConnectionSettingsAndCreatesNoBackend() {
        runner.withPropertyValues("tsdb.iotdb.enable=false", "tsdb.influxdb.enable=false", "tsdb.influxdb1.enable=false",
                        "tsdb.influxdb1.url=", "tsdb.influxdb1.database=", "tsdb.influxdb1.max-query-rows=0")
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(TGTemplate.class)
                        .doesNotHaveBean(InfluxDB1Adapter.class).doesNotHaveBean(InfluxDB.class)
                        .doesNotHaveBean(InfluxDB1Properties.class));
    }

    @Test void selectedBackendBindsAllSettingsAndExposesTheOwnedNativeClient() {
        influx1().withPropertyValues("tsdb.query-log-enabled=false", "tsdb.influxdb1.username=reader",
                "tsdb.influxdb1.password=test-password", "tsdb.influxdb1.retention-policy=archive",
                "tsdb.influxdb1.max-batch-records=31", "tsdb.influxdb1.max-query-rows=19",
                "tsdb.influxdb1.max-query-response-bytes=4096",
                "tsdb.influxdb1.http-client.connect-timeout-ms=1234").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(TGTemplate.class)
                    .hasSingleBean(InfluxDB1Adapter.class).hasSingleBean(InfluxDB.class)
                    .doesNotHaveBean(InfluxDBAdapter.class).doesNotHaveBean(IoTDBTableAdapter.class);
            InfluxDB1Properties properties = context.getBean(InfluxDB1Properties.class);
            assertThat(properties.isEnable()).isTrue();
            assertThat(properties.isFailFast()).isTrue();
            assertThat(properties.getDatabase()).isEqualTo("test_db");
            assertThat(properties.getUsername()).isEqualTo("reader");
            assertThat(properties.getPassword()).isEqualTo("test-password");
            assertThat(properties.getRetentionPolicy()).isEqualTo("archive");
            assertThat(properties.getMaxBatchRecords()).isEqualTo(31);
            assertThat(properties.getMaxQueryRows()).isEqualTo(19);
            assertThat(properties.getMaxQueryResponseBytes()).isEqualTo(4096L);
            assertThat(context.getBean(InfluxDB1HttpClientProperties.class).getConnectTimeoutMs()).isEqualTo(1234L);
            assertThat(context.getBean(TSDBDeploymentProperties.class).isQueryLogEnabled()).isFalse();
            assertThat(context.getBean(InfluxDB.class)).isSameAs(context.getBean(InfluxDB1Adapter.class).getNativeClient());
        });
    }

    @ParameterizedTest @ValueSource(strings = {"tsdb.influxdb1.url=", "tsdb.influxdb1.database=",
            "tsdb.influxdb1.max-batch-records=0", "tsdb.influxdb1.max-query-rows=0",
            "tsdb.influxdb1.max-query-rows=2147483647", "tsdb.influxdb1.max-query-response-bytes=0"})
    void invalidSettingsStillFailWhenFailFastIsDisabled(String property) {
        influx1().withPropertyValues("tsdb.influxdb1.fail-fast=false", property)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test void absentUnselectedBackendClassesDoNotRequireExplicitDisableFlags() throws Exception {
        try (FilteredClassLoader loader = new FilteredClassLoader(IoTDBTableAdapter.class, InfluxDBAdapter.class)) {
            runner.withClassLoader(loader).withPropertyValues("tsdb.influxdb1.enable=true", "tsdb.influxdb1.url=http://127.0.0.1:1")
                    .run(context -> assertThat(context).hasNotFailed().hasSingleBean(InfluxDB1Adapter.class));
        }
    }

    @Test void applicationMetadataResolverOverridesTheDefault() {
        TSDBMetadataResolver resolver = mock(TSDBMetadataResolver.class);
        influx1().withBean(TSDBMetadataResolver.class, () -> resolver)
                .run(context -> assertThat(context.getBean(TSDBMetadataResolver.class)).isSameAs(resolver));
    }

    @Test void closingTheSpringContextClosesTheAdapterAndTheBorrowedClientOnlyOnce() {
        InfluxDB nativeClient = mock(InfluxDB.class);
        try (MockedConstruction<InfluxDB1Adapter> construction = mockConstruction(InfluxDB1Adapter.class, (adapter, ignored) ->
                when(adapter.getNativeClient()).thenReturn(nativeClient))) {
            influx1().run(context -> assertThat(context.getBean(InfluxDB.class)).isSameAs(nativeClient));
            verify(construction.constructed().get(0), times(1)).close();
            verify(nativeClient, never()).close();
        }
    }

    @Test void failedInitializationWithFailFastDisabledLeavesOptionalNativeClientInjectionUnavailable() {
        try (MockedConstruction<InfluxDB1Adapter> construction = mockConstruction(InfluxDB1Adapter.class, (adapter, ignored) -> {
            doThrow(new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR, "cannot initialize")).when(adapter).init();
            when(adapter.getNativeClient()).thenThrow(new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, "not initialized"));
        })) {
            influx1().withBean(OptionalNativeClientConsumer.class)
                    .withPropertyValues("tsdb.influxdb1.fail-fast=false").run(context -> {
                assertThat(context).hasNotFailed().hasSingleBean(InfluxDB1Adapter.class)
                        .hasSingleBean(TGTemplate.class);
                OptionalNativeClientConsumer consumer = context.getBean(OptionalNativeClientConsumer.class);
                // A null-returning factory keeps its declared bean type but supplies no injectable native client.
                assertThat(consumer.clientProvider).isNotNull();
                assertThat(consumer.clientProvider.getIfAvailable()).isNull();
                assertThat(consumer.optionalClient).isEmpty();
                assertThat(consumer.autowiredClient).isNull();
                assertThat(context.getBeanProvider(InfluxDB.class).getIfAvailable()).isNull();

                InfluxDB recoveredClient = mock(InfluxDB.class);
                InfluxDB1Adapter adapter = context.getBean(InfluxDB1Adapter.class);
                doNothing().when(adapter).init();
                doReturn(recoveredClient).when(adapter).getNativeClient();
                adapter.init();
                assertThat(adapter.getNativeClient()).isSameAs(recoveredClient);
                assertThat(consumer.clientProvider.getIfAvailable()).isNull();
            });
            assertThat(construction.constructed()).hasSize(1);
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @Test void successfulInitializationSuppliesTheSameNativeClientToEveryOptionalInjectionForm() {
        InfluxDB nativeClient = mock(InfluxDB.class);
        try (MockedConstruction<InfluxDB1Adapter> construction = mockConstruction(InfluxDB1Adapter.class,
                (adapter, ignored) -> when(adapter.getNativeClient()).thenReturn(nativeClient))) {
            influx1().withBean(OptionalNativeClientConsumer.class).run(context -> {
                assertThat(context).hasNotFailed().hasSingleBean(InfluxDB.class);
                OptionalNativeClientConsumer consumer = context.getBean(OptionalNativeClientConsumer.class);
                assertThat(consumer.clientProvider.getIfAvailable()).isSameAs(nativeClient);
                assertThat(consumer.optionalClient).containsSame(nativeClient);
                assertThat(consumer.autowiredClient).isSameAs(nativeClient);
            });
            verify(construction.constructed().get(0), times(1)).close();
            verify(nativeClient, never()).close();
        }
    }

    static class OptionalNativeClientConsumer {
        @Autowired
        ObjectProvider<InfluxDB> clientProvider;

        @Autowired
        Optional<InfluxDB> optionalClient;

        @Autowired(required = false)
        InfluxDB autowiredClient;
    }

    @Test void failedInitializationWithFailFastEnabledCleansUpAndStopsStartup() {
        try (MockedConstruction<InfluxDB1Adapter> construction = mockConstruction(InfluxDB1Adapter.class, (adapter, ignored) ->
                doThrow(new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR, "cannot initialize")).when(adapter).init())) {
            influx1().run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasStackTraceContaining("cannot initialize");
            });
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @Test void contextsKeepIndependentSelections() {
        influx1().run(first -> {
            assertThat(first).hasNotFailed().hasSingleBean(InfluxDB1Adapter.class);
            runner.withPropertyValues("tsdb.influxdb.enable=true", "tsdb.influxdb.url=http://127.0.0.1:1").run(second -> {
                assertThat(second).hasNotFailed().hasSingleBean(InfluxDBAdapter.class).doesNotHaveBean(InfluxDB1Adapter.class);
            });
            assertThat(first).hasSingleBean(InfluxDB1Adapter.class);
        });
    }
}
