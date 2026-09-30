package com.alandevise.tsdb.autoconfigure;

import com.alandevise.tsdb.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsdb.config.InfluxDBProperties;
import com.alandevise.tsdb.config.StrictCursorSqlStrategyEnum;
import com.alandevise.tsdb.core.TGTemplate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks application-facing strategy binding without opening database connections. */
class InfluxDBStrictCursorStrategyBindingTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    InfluxDBTSDBAutoConfiguration.class));

    private ApplicationContextRunner configured() {
        return runner.withPropertyValues("tsdb.influxdb.enable=true", "tsdb.influxdb.url=http://127.0.0.1:1",
                "tsdb.influxdb.database=strategy_test");
    }

    @Test
    void omittedStrategyBindsOrWithoutRequiringOtherBackendFlags() {
        configured().run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(InfluxDBAdapter.class)
                    .hasSingleBean(TGTemplate.class);
            assertThat(context.getBean(InfluxDBProperties.class).getStrictCursorSql())
                    .isEqualTo(StrictCursorSqlStrategyEnum.OR);
        });
    }

    @ParameterizedTest
    @CsvSource({"or,OR", "union-all,UNION_ALL"})
    void bindsDocumentedYamlValues(String property, StrictCursorSqlStrategyEnum expected) {
        configured().withPropertyValues("tsdb.influxdb.strict-cursor-sql=" + property).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(InfluxDBAdapter.class);
            assertThat(context.getBean(InfluxDBProperties.class).getStrictCursorSql()).isEqualTo(expected);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "unsupported-mode"})
    void invalidStrategyFailsEvenWhenFailFastIsDisabled(String property) {
        configured().withPropertyValues("tsdb.influxdb.fail-fast=false",
                "tsdb.influxdb.strict-cursor-sql=" + property)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void blankValueRetainsSpringBindingDefault() {
        configured().withPropertyValues("tsdb.influxdb.strict-cursor-sql=").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(InfluxDBAdapter.class);
            assertThat(context.getBean(InfluxDBProperties.class).getStrictCursorSql())
                    .isEqualTo(StrictCursorSqlStrategyEnum.OR);
        });
    }

    @Test
    void disabledBackendDoesNotBindInvalidStrategy() {
        runner.withPropertyValues("tsdb.influxdb.enable=false", "tsdb.influxdb.strict-cursor-sql=invalid")
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(InfluxDBAdapter.class)
                        .doesNotHaveBean(TGTemplate.class));
    }
}
