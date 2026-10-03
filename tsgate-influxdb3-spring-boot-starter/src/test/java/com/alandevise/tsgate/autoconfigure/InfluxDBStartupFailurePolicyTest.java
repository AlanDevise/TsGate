package com.alandevise.tsgate.autoconfigure;

import com.alandevise.tsgate.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.influxdb.v3.client.InfluxDBClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/** Verifies startup failure policy using a real Spring context without database connections. */
class InfluxDBStartupFailurePolicyTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    InfluxDBTSDBAutoConfiguration.class))
            .withPropertyValues("tsdb.influxdb.enable=true", "tsdb.influxdb.url=http://127.0.0.1:1",
                    "tsdb.influxdb.database=test_db");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void toleratedInitializationFailureLeavesOptionalClientUnavailableUntilDirectManualRetry(boolean getterThrows) {
        try (MockedConstruction<InfluxDBAdapter> construction = mockConstruction(InfluxDBAdapter.class,
                (adapter, ignored) -> {
                    doThrow(new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR, "cannot initialize"))
                            .when(adapter).init();
                    if (getterThrows) {
                        when(adapter.getNativeClient()).thenThrow(new TSDBException(
                                TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, "not initialized"));
                    }
                })) {
            runner.withBean(OptionalNativeClientConsumer.class)
                    .withPropertyValues("tsdb.influxdb.fail-fast=false").run(context -> {
                        assertThat(context).hasNotFailed().hasSingleBean(InfluxDBAdapter.class)
                                .hasSingleBean(TGTemplate.class);
                        OptionalNativeClientConsumer consumer = context.getBean(OptionalNativeClientConsumer.class);
                        assertThat(consumer.clientProvider.getIfAvailable()).isNull();
                        assertThat(consumer.optionalClient).isEmpty();
                        assertThat(consumer.autowiredClient).isNull();
                        assertThat(context.getBeanProvider(InfluxDBClient.class).getIfAvailable()).isNull();

                        InfluxDBAdapter adapter = context.getBean(InfluxDBAdapter.class);
                        verify(adapter, never()).close();
                        InfluxDBClient recoveredClient = mock(InfluxDBClient.class);
                        doNothing().when(adapter).init();
                        doReturn(recoveredClient).when(adapter).getNativeClient();
                        adapter.init();
                        assertThat(adapter.getNativeClient()).isSameAs(recoveredClient);
                        // Spring retains the null-returning factory result after the adapter's manual retry.
                        assertThat(consumer.clientProvider.getIfAvailable()).isNull();
                        assertThat(context.getBeanProvider(InfluxDBClient.class).getIfAvailable()).isNull();
                        verify(adapter, times(2)).init();
                    });
            assertThat(construction.constructed()).hasSize(1);
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void healthyInitializationSuppliesTheBorrowedClientRegardlessOfFailurePolicy(boolean failFast) throws Exception {
        InfluxDBClient nativeClient = mock(InfluxDBClient.class);
        try (MockedConstruction<InfluxDBAdapter> construction = mockConstruction(InfluxDBAdapter.class,
                (adapter, ignored) -> when(adapter.getNativeClient()).thenReturn(nativeClient))) {
            runner.withBean(OptionalNativeClientConsumer.class)
                    .withPropertyValues("tsdb.influxdb.fail-fast=" + failFast).run(context -> {
                        assertThat(context).hasNotFailed().hasSingleBean(InfluxDBClient.class)
                                .hasSingleBean(TGTemplate.class);
                        OptionalNativeClientConsumer consumer = context.getBean(OptionalNativeClientConsumer.class);
                        assertThat(consumer.clientProvider.getIfAvailable()).isSameAs(nativeClient);
                        assertThat(consumer.optionalClient).containsSame(nativeClient);
                        assertThat(consumer.autowiredClient).isSameAs(nativeClient);
                    });
            verify(construction.constructed().get(0), times(1)).init();
            verify(construction.constructed().get(0), times(1)).close();
            verify(nativeClient, never()).close();
        }
    }

    @Test
    void failFastInitializationFailureCleansUpAndStopsStartup() {
        try (MockedConstruction<InfluxDBAdapter> construction = mockConstruction(InfluxDBAdapter.class,
                (adapter, ignored) -> doThrow(new TSDBException(
                        TSDBErrorCodeEnum.CONNECTION_ERROR, "cannot initialize")).when(adapter).init())) {
            runner.run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasStackTraceContaining("cannot initialize");
            });
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @Test
    void nonStateNativeClientFailureStillStopsStartupWhenFailFastIsDisabled() {
        try (MockedConstruction<InfluxDBAdapter> construction = mockConstruction(InfluxDBAdapter.class,
                (adapter, ignored) -> when(adapter.getNativeClient()).thenThrow(new TSDBException(
                        TSDBErrorCodeEnum.CONNECTION_ERROR, "native client failure")))) {
            runner.withPropertyValues("tsdb.influxdb.fail-fast=false").run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasStackTraceContaining("native client failure");
            });
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @Test
    void singleArgumentFactoryRetainsItsStrictAvailabilityContract() {
        InfluxDBAdapter adapter = mock(InfluxDBAdapter.class);
        assertThatThrownBy(() -> new InfluxDBTSDBAutoConfiguration().influxDBClient(adapter))
                .isInstanceOfSatisfying(TSDBException.class,
                        failure -> assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR));
    }

    static class OptionalNativeClientConsumer {
        @Autowired
        ObjectProvider<InfluxDBClient> clientProvider;

        @Autowired
        Optional<InfluxDBClient> optionalClient;

        @Autowired(required = false)
        InfluxDBClient autowiredClient;
    }
}
