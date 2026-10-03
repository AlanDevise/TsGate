package com.alandevise.tsgate.autoconfigure;

import com.alandevise.tsgate.adapter.impl.OpenGeminiAdapter;
import com.alandevise.tsgate.adapter.impl.InfluxDB1Adapter;
import com.alandevise.tsgate.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsgate.config.OpenGeminiHttpClientProperties;
import com.alandevise.tsgate.config.OpenGeminiProperties;
import com.alandevise.tsgate.config.TSDBDeploymentProperties;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.metadata.TSDBMetadataResolver;
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

class OpenGeminiStarterTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    IoTDBTSDBAutoConfiguration.class, InfluxDBTSDBAutoConfiguration.class,
                    OpenGeminiTSDBAutoConfiguration.class, InfluxDB1TSDBAutoConfiguration.class));

    private ApplicationContextRunner openGemini() {
        return runner.withPropertyValues("tsdb.opengemini.enable=true", "tsdb.opengemini.url=http://127.0.0.1:1", "tsdb.opengemini.database=test_db");
    }

    @Test void defaultSettingsRemainInactiveAndDoNotEnableTransportReplay() {
        assertThat(new OpenGeminiProperties().isEnable()).isFalse();
        assertThat(new OpenGeminiHttpClientProperties().isRetryOnConnectionFailure()).isFalse();
    }

    @Test void omittedDatabaseBindsTsdbAndCreatesTheNativeClient() {
        runner.withPropertyValues("tsdb.opengemini.enable=true", "tsdb.opengemini.url=http://127.0.0.1:1").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(InfluxDB.class);
            assertThat(context.getBean(OpenGeminiProperties.class).getDatabase()).isEqualTo("tsdb");
        });
    }

    @Test void allFourStartersRemainInactiveWithoutBackendConfiguration() {
        runner.withPropertyValues("tsdb.query-log-enabled=false").run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(TGTemplate.class)
                    .doesNotHaveBean(IoTDBTableAdapter.class).doesNotHaveBean(InfluxDBAdapter.class)
                    .doesNotHaveBean(OpenGeminiAdapter.class).doesNotHaveBean(InfluxDB1Adapter.class)
                    .doesNotHaveBean(InfluxDB.class);
        });
    }

    @ParameterizedTest @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void openGeminiConflictsWithEveryOtherEnabledBackend(String backend) {
        openGemini().withPropertyValues("tsdb." + backend + ".enable=true").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("Only one TSDB adapter may be enabled");
        });
    }

    @Test void allDisabledIgnoresInvalidConnectionSettingsAndCreatesNoBackend() {
        runner.withPropertyValues("tsdb.iotdb.enable=false", "tsdb.influxdb.enable=false", "tsdb.opengemini.enable=false", "tsdb.influxdb1.enable=false",
                        "tsdb.opengemini.url=", "tsdb.opengemini.database=", "tsdb.opengemini.max-query-rows=0")
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(TGTemplate.class)
                        .doesNotHaveBean(OpenGeminiAdapter.class).doesNotHaveBean(InfluxDB.class)
                        .doesNotHaveBean(OpenGeminiProperties.class));
    }

    @Test void selectedBackendBindsAllSettingsAndExposesTheOwnedNativeClient() {
        openGemini().withPropertyValues("tsdb.query-log-enabled=false", "tsdb.opengemini.username=reader",
                "tsdb.opengemini.password=test-password", "tsdb.opengemini.retention-policy=archive",
                "tsdb.opengemini.max-batch-records=31", "tsdb.opengemini.max-query-rows=19",
                "tsdb.opengemini.max-query-response-bytes=4096",
                "tsdb.opengemini.http-client.connect-timeout-ms=1234",
                "tsdb.opengemini.http-client.retry-on-connection-failure=true").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(TGTemplate.class)
                    .hasSingleBean(OpenGeminiAdapter.class).hasSingleBean(InfluxDB.class)
                    .doesNotHaveBean(InfluxDBAdapter.class).doesNotHaveBean(IoTDBTableAdapter.class);
            OpenGeminiProperties properties = context.getBean(OpenGeminiProperties.class);
            assertThat(properties.isEnable()).isTrue();
            assertThat(properties.isFailFast()).isTrue();
            assertThat(properties.getDatabase()).isEqualTo("test_db");
            assertThat(properties.getUsername()).isEqualTo("reader");
            assertThat(properties.getPassword()).isEqualTo("test-password");
            assertThat(properties.getRetentionPolicy()).isEqualTo("archive");
            assertThat(properties.getMaxBatchRecords()).isEqualTo(31);
            assertThat(properties.getMaxQueryRows()).isEqualTo(19);
            assertThat(properties.getMaxQueryResponseBytes()).isEqualTo(4096L);
            assertThat(context.getBean(OpenGeminiHttpClientProperties.class).getConnectTimeoutMs()).isEqualTo(1234L);
            assertThat(context.getBean(OpenGeminiHttpClientProperties.class).isRetryOnConnectionFailure()).isTrue();
            assertThat(context).doesNotHaveBean(InfluxDB1Adapter.class);
            assertThat(context.getBean(TSDBDeploymentProperties.class).isQueryLogEnabled()).isFalse();
            assertThat(context.getBean(InfluxDB.class)).isSameAs(context.getBean(OpenGeminiAdapter.class).getNativeClient());
        });
    }

    @ParameterizedTest @ValueSource(strings = {"tsdb.opengemini.url=", "tsdb.opengemini.database=",
            "tsdb.opengemini.max-batch-records=0", "tsdb.opengemini.max-query-rows=0",
            "tsdb.opengemini.max-query-rows=2147483647", "tsdb.opengemini.max-query-response-bytes=0"})
    void invalidSettingsStillFailWhenFailFastIsDisabled(String property) {
        openGemini().withPropertyValues("tsdb.opengemini.fail-fast=false", property)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test void absentUnselectedBackendClassesDoNotRequireExplicitDisableFlags() throws Exception {
        try (FilteredClassLoader loader = new FilteredClassLoader(IoTDBTableAdapter.class, InfluxDBAdapter.class)) {
            runner.withClassLoader(loader).withPropertyValues("tsdb.opengemini.enable=true", "tsdb.opengemini.url=http://127.0.0.1:1")
                    .run(context -> assertThat(context).hasNotFailed().hasSingleBean(OpenGeminiAdapter.class));
        }
    }

    @Test void applicationMetadataResolverOverridesTheDefault() {
        TSDBMetadataResolver resolver = mock(TSDBMetadataResolver.class);
        openGemini().withBean(TSDBMetadataResolver.class, () -> resolver)
                .run(context -> assertThat(context.getBean(TSDBMetadataResolver.class)).isSameAs(resolver));
    }

    @Test void closingTheSpringContextClosesTheAdapterAndTheBorrowedClientOnlyOnce() {
        InfluxDB nativeClient = mock(InfluxDB.class);
        try (MockedConstruction<OpenGeminiAdapter> construction = mockConstruction(OpenGeminiAdapter.class, (adapter, ignored) ->
                when(adapter.getNativeClient()).thenReturn(nativeClient))) {
            openGemini().run(context -> assertThat(context.getBean(InfluxDB.class)).isSameAs(nativeClient));
            verify(construction.constructed().get(0), times(1)).close();
            verify(nativeClient, never()).close();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void failedInitializationWithFailFastDisabledLeavesOptionalNativeClientInjectionUnavailable(boolean getterThrows) {
        try (MockedConstruction<OpenGeminiAdapter> construction = mockConstruction(OpenGeminiAdapter.class, (adapter, ignored) -> {
            doThrow(new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR, "cannot initialize")).when(adapter).init();
            if (getterThrows) {
                when(adapter.getNativeClient()).thenThrow(new TSDBException(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, "not initialized"));
            }
        })) {
            openGemini().withBean(OptionalNativeClientConsumer.class)
                    .withPropertyValues("tsdb.opengemini.fail-fast=false").run(context -> {
                assertThat(context).hasNotFailed().hasSingleBean(OpenGeminiAdapter.class)
                        .hasSingleBean(TGTemplate.class);
                OptionalNativeClientConsumer consumer = context.getBean(OptionalNativeClientConsumer.class);
                // A null-returning factory keeps its declared bean type but supplies no injectable native client.
                assertThat(consumer.clientProvider).isNotNull();
                assertThat(consumer.clientProvider.getIfAvailable()).isNull();
                assertThat(consumer.optionalClient).isEmpty();
                assertThat(consumer.autowiredClient).isNull();
                assertThat(context.getBeanProvider(InfluxDB.class).getIfAvailable()).isNull();

                InfluxDB recoveredClient = mock(InfluxDB.class);
                OpenGeminiAdapter adapter = context.getBean(OpenGeminiAdapter.class);
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

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void successfulInitializationSuppliesTheSameNativeClientToEveryOptionalInjectionForm(boolean failFast) {
        InfluxDB nativeClient = mock(InfluxDB.class);
        try (MockedConstruction<OpenGeminiAdapter> construction = mockConstruction(OpenGeminiAdapter.class,
                (adapter, ignored) -> when(adapter.getNativeClient()).thenReturn(nativeClient))) {
            openGemini().withBean(OptionalNativeClientConsumer.class)
                    .withPropertyValues("tsdb.opengemini.fail-fast=" + failFast).run(context -> {
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
        try (MockedConstruction<OpenGeminiAdapter> construction = mockConstruction(OpenGeminiAdapter.class, (adapter, ignored) ->
                doThrow(new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR, "cannot initialize")).when(adapter).init())) {
            openGemini().run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasStackTraceContaining("cannot initialize");
            });
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @Test void nativeClientErrorsOtherThanUnavailableStateStillFailStartup() {
        try (MockedConstruction<OpenGeminiAdapter> construction = mockConstruction(OpenGeminiAdapter.class,
                (adapter, ignored) -> when(adapter.getNativeClient()).thenThrow(new TSDBException(
                        TSDBErrorCodeEnum.CONNECTION_ERROR, "native client failure")))) {
            openGemini().withPropertyValues("tsdb.opengemini.fail-fast=false").run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasStackTraceContaining("native client failure");
            });
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @Test void contextsKeepIndependentSelections() {
        openGemini().run(first -> {
            assertThat(first).hasNotFailed().hasSingleBean(OpenGeminiAdapter.class);
            runner.withPropertyValues("tsdb.influxdb.enable=true", "tsdb.influxdb.url=http://127.0.0.1:1").run(second -> {
                assertThat(second).hasNotFailed().hasSingleBean(InfluxDBAdapter.class).doesNotHaveBean(OpenGeminiAdapter.class);
            });
            assertThat(first).hasSingleBean(OpenGeminiAdapter.class);
        });
    }
}
