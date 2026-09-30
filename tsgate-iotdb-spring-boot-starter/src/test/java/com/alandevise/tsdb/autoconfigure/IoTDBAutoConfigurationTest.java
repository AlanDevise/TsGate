package com.alandevise.tsdb.autoconfigure;

import com.alandevise.tsdb.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.config.TSDBDeploymentProperties;
import com.alandevise.tsdb.core.TGTemplate;
import com.alandevise.tsdb.metadata.TSDBMetadataResolver;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class IoTDBAutoConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    IoTDBTSDBAutoConfiguration.class));
    private ApplicationContextRunner configured() {
        return runner.withPropertyValues("tsdb.iotdb.enable=true", "tsdb.iotdb.username=root", "tsdb.iotdb.password=test-password",
                "tsdb.iotdb.database=test_db", "tsdb.iotdb.pool.node-urls[0]=127.0.0.1:1");
    }
    @Test void omittedDatabaseBindsTsdbAndCreatesThePool() {
        runner.withPropertyValues("tsdb.iotdb.enable=true", "tsdb.iotdb.username=root", "tsdb.iotdb.password=test-password",
                "tsdb.iotdb.pool.node-urls[0]=127.0.0.1:1").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(ITableSessionPool.class);
            assertThat(context.getBean(IoTDBProperties.class).getDatabase()).isEqualTo("tsdb");
            assertThat(context.getBean(IoTDBProperties.class).getTable().isRpcCompressionEnabled()).isTrue();
        });
    }
    @ParameterizedTest @ValueSource(booleans = {true, false})
    void bindsExplicitTabletRpcCompressionWithoutOpeningConnections(boolean enabled) {
        configured().withPropertyValues("tsdb.iotdb.table.rpc-compression-enabled=" + enabled)
                .run(context -> {
                    assertThat(context).hasNotFailed().hasSingleBean(ITableSessionPool.class);
                    assertThat(context.getBean(IoTDBProperties.class).getTable().isRpcCompressionEnabled())
                            .isEqualTo(enabled);
                });
    }
    @Test void disabledBackendDoesNotValidateOrCreateClients() {
        runner.withPropertyValues("tsdb.iotdb.enable=false", "tsdb.iotdb.password=${UNRESOLVED_DISABLED_PASSWORD}",
                "tsdb.iotdb.pool.max-size=invalid").run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(TGTemplate.class)
                    .doesNotHaveBean(IoTDBTableAdapter.class).doesNotHaveBean(ITableSessionPool.class);
        });
    }
    @Test void bindsPropertiesAndExposesOneLazilyConnectedClient() {
        configured().withPropertyValues("tsdb.query-log-enabled=false", "tsdb.iotdb.pool.max-size=3",
                "tsdb.iotdb.table.tablet-max-row-size=17", "tsdb.iotdb.max-query-rows=123").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(TGTemplate.class)
                    .hasSingleBean(IoTDBTableAdapter.class).hasSingleBean(ITableSessionPool.class);
            assertThat(context.getBean(IoTDBProperties.class).getDatabase()).isEqualTo("test_db");
            assertThat(context.getBean(IoTDBProperties.class).getPool().getMaxSize()).isEqualTo(3);
            assertThat(context.getBean(IoTDBProperties.class).getTable().getTabletMaxRowSize()).isEqualTo(17);
            assertThat(context.getBean(IoTDBProperties.class).getMaxQueryRows()).isEqualTo(123);
            assertThat(context.getBean(TSDBDeploymentProperties.class).isQueryLogEnabled()).isFalse();
            assertThat(context.getBean(ITableSessionPool.class)).isSameAs(context.getBean(IoTDBTableAdapter.class).getSessionPool());
        });
    }
    @Test void repeatedInitKeepsTheInjectedNativePoolBeanValid() {
        configured().run(context -> {
            assertThat(context).hasNotFailed();
            IoTDBTableAdapter adapter = context.getBean(IoTDBTableAdapter.class);
            ITableSessionPool injected = context.getBean(ITableSessionPool.class);
            adapter.init();
            adapter.init();
            assertThat(adapter.getSessionPool()).isSameAs(injected);
            assertThat(context.getBean(ITableSessionPool.class)).isSameAs(injected);
        });
    }
    @ParameterizedTest @ValueSource(strings = {
            "tsdb.iotdb.username=", "tsdb.iotdb.password=", "tsdb.iotdb.database=",
            "tsdb.iotdb.table.rpc-compression-enabled=invalid",
            "tsdb.iotdb.pool.enabled=false", "tsdb.iotdb.table.tablet-max-row-size=0", "tsdb.iotdb.max-batch-records=0",
            "tsdb.iotdb.max-query-rows=0", "tsdb.iotdb.max-query-rows=-1", "tsdb.iotdb.max-query-rows=2147483647"})
    void invalidConfigFailsEvenWhenFailFastIsDisabled(String property) {
        configured().withPropertyValues("tsdb.iotdb.fail-fast=false", property)
                .run(context -> assertThat(context).hasFailed());
    }
    @Test void absentConnectionConfigCreatesNoBackend() {
        runner.run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(TGTemplate.class)
                .doesNotHaveBean(IoTDBTableAdapter.class).doesNotHaveBean(ITableSessionPool.class));
    }
    @Test void explicitEnableWithoutRequiredConnectionSettingsFailsValidation() {
        runner.withPropertyValues("tsdb.iotdb.enable=true").run(context -> assertThat(context).hasFailed());
    }
    @Test void customAdapterResolverAndTemplateAreHonored() {
        IoTDBTableAdapter adapter = mock(IoTDBTableAdapter.class);
        ITableSessionPool pool = mock(ITableSessionPool.class);
        TSDBMetadataResolver resolver = mock(TSDBMetadataResolver.class);
        TGTemplate template = mock(TGTemplate.class);
        when(adapter.getSessionPool()).thenReturn(pool);
        runner.withPropertyValues("tsdb.iotdb.enable=true").withBean(IoTDBTableAdapter.class, () -> adapter)
                .withBean(TSDBMetadataResolver.class, () -> resolver)
                .withBean(TGTemplate.class, () -> template).run(context -> {
                    assertThat(context).hasNotFailed().hasSingleBean(TGTemplate.class);
                    assertThat(context.getBean(TGTemplate.class)).isSameAs(template);
                    assertThat(context.getBean(TSDBMetadataResolver.class)).isSameAs(resolver);
                    verify(adapter, never()).init();
                });
        verify(pool, never()).close();
    }
}
