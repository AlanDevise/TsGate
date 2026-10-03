package com.alandevise.tsgate.autoconfigure;

import com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import org.apache.iotdb.isession.pool.ITableSessionPool;
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
class IoTDBStartupFailurePolicyTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    IoTDBTSDBAutoConfiguration.class))
            .withPropertyValues("tsdb.iotdb.enable=true", "tsdb.iotdb.username=root",
                    "tsdb.iotdb.password=test-password", "tsdb.iotdb.database=test_db",
                    "tsdb.iotdb.pool.node-urls[0]=127.0.0.1:1");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void toleratedInitializationFailureLeavesOptionalPoolUnavailableUntilDirectManualRetry(boolean getterThrows) {
        try (MockedConstruction<IoTDBTableAdapter> construction = mockConstruction(IoTDBTableAdapter.class,
                (adapter, ignored) -> {
                    doThrow(new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR, "cannot initialize"))
                            .when(adapter).init();
                    if (getterThrows) {
                        when(adapter.getSessionPool()).thenThrow(new TSDBException(
                                TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, "not initialized"));
                    }
                })) {
            runner.withBean(OptionalNativePoolConsumer.class)
                    .withPropertyValues("tsdb.iotdb.fail-fast=false").run(context -> {
                        assertThat(context).hasNotFailed().hasSingleBean(IoTDBTableAdapter.class)
                                .hasSingleBean(TGTemplate.class);
                        OptionalNativePoolConsumer consumer = context.getBean(OptionalNativePoolConsumer.class);
                        assertThat(consumer.poolProvider.getIfAvailable()).isNull();
                        assertThat(consumer.optionalPool).isEmpty();
                        assertThat(consumer.autowiredPool).isNull();
                        assertThat(context.getBeanProvider(ITableSessionPool.class).getIfAvailable()).isNull();

                        IoTDBTableAdapter adapter = context.getBean(IoTDBTableAdapter.class);
                        verify(adapter, never()).close();
                        ITableSessionPool recoveredPool = mock(ITableSessionPool.class);
                        doNothing().when(adapter).init();
                        doReturn(recoveredPool).when(adapter).getSessionPool();
                        adapter.init();
                        assertThat(adapter.getSessionPool()).isSameAs(recoveredPool);
                        // Spring retains the null-returning factory result after the adapter's manual retry.
                        assertThat(consumer.poolProvider.getIfAvailable()).isNull();
                        assertThat(context.getBeanProvider(ITableSessionPool.class).getIfAvailable()).isNull();
                        verify(adapter, times(2)).init();
                    });
            assertThat(construction.constructed()).hasSize(1);
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void healthyInitializationSuppliesTheBorrowedPoolRegardlessOfFailurePolicy(boolean failFast) {
        ITableSessionPool sessionPool = mock(ITableSessionPool.class);
        try (MockedConstruction<IoTDBTableAdapter> construction = mockConstruction(IoTDBTableAdapter.class,
                (adapter, ignored) -> when(adapter.getSessionPool()).thenReturn(sessionPool))) {
            runner.withBean(OptionalNativePoolConsumer.class)
                    .withPropertyValues("tsdb.iotdb.fail-fast=" + failFast).run(context -> {
                        assertThat(context).hasNotFailed().hasSingleBean(ITableSessionPool.class)
                                .hasSingleBean(TGTemplate.class);
                        OptionalNativePoolConsumer consumer = context.getBean(OptionalNativePoolConsumer.class);
                        assertThat(consumer.poolProvider.getIfAvailable()).isSameAs(sessionPool);
                        assertThat(consumer.optionalPool).containsSame(sessionPool);
                        assertThat(consumer.autowiredPool).isSameAs(sessionPool);
                    });
            verify(construction.constructed().get(0), times(1)).init();
            verify(construction.constructed().get(0), times(1)).close();
            verify(sessionPool, never()).close();
        }
    }

    @Test
    void failFastInitializationFailureCleansUpAndStopsStartup() {
        try (MockedConstruction<IoTDBTableAdapter> construction = mockConstruction(IoTDBTableAdapter.class,
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
    void nonStateNativePoolFailureStillStopsStartupWhenFailFastIsDisabled() {
        try (MockedConstruction<IoTDBTableAdapter> construction = mockConstruction(IoTDBTableAdapter.class,
                (adapter, ignored) -> when(adapter.getSessionPool()).thenThrow(new TSDBException(
                        TSDBErrorCodeEnum.CONNECTION_ERROR, "native pool failure")))) {
            runner.withPropertyValues("tsdb.iotdb.fail-fast=false").run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasStackTraceContaining("native pool failure");
            });
            verify(construction.constructed().get(0), times(1)).close();
        }
    }

    @Test
    void singleArgumentFactoryRetainsItsStrictAvailabilityContract() {
        IoTDBTableAdapter adapter = mock(IoTDBTableAdapter.class);
        assertThatThrownBy(() -> new IoTDBTSDBAutoConfiguration().iotdbTableSessionPool(adapter))
                .isInstanceOfSatisfying(TSDBException.class,
                        failure -> assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR));
    }

    static class OptionalNativePoolConsumer {
        @Autowired
        ObjectProvider<ITableSessionPool> poolProvider;

        @Autowired
        Optional<ITableSessionPool> optionalPool;

        @Autowired(required = false)
        ITableSessionPool autowiredPool;
    }
}
