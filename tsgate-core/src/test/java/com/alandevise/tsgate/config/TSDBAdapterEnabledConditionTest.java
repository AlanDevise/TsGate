package com.alandevise.tsgate.config;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mock.env.MockEnvironment;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TSDBAdapterEnabledConditionTest {
    private final TSDBAdapterEnabledCondition common = new TSDBAdapterEnabledCondition();

    @Test void allPresentAdaptersRemainDisabledWithoutApplicationConfiguration() {
        assertSelection(new MockEnvironment(), null);
    }

    @Test void sharedPropertiesAndNeighboringPrefixesDoNotSelectAnAdapter() {
        assertSelection(new MockEnvironment().withProperty("tsdb.query-log-enabled", "false")
                .withProperty("tsdb.iotdbx.url", "http://unused")
                .withProperty("tsdb.influxdb-extra.url", "http://unused")
                .withProperty("unrelated.tsdb.influxdb1.url", "http://unused"), null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void connectionSettingsDoNotEnableAnyBackend(String selected) {
        assertSelection(new MockEnvironment().withProperty("tsdb." + selected + ".database", "business_metrics"), null);
    }

    @ParameterizedTest
    @CsvSource({"iotdb,pool.node-urls[0]", "iotdb,pool.max-size", "influxdb,http-client.connect-timeout-ms",
            "influxdb1,http-client.connect-timeout-ms"})
    void nestedAndIndexedConfigurationDoesNotEnableItsBackend(String selected, String property) {
        assertSelection(new MockEnvironment().withProperty("tsdb." + selected + "." + property, "1"), null);
    }

    @Test void realYamlLoadingFlattensNestedAndIndexedConnectionProperties() throws Exception {
        MockEnvironment environment = yaml("""
                tsdb:
                  query-log-enabled: false
                  iotdb:
                    pool:
                      node-urls:
                        - 127.0.0.1:6667
                        - 127.0.0.2:6667
                """);
        assertThat(environment.getProperty("tsdb.iotdb.pool.node-urls[0]")).isEqualTo("127.0.0.1:6667");
        assertSelection(environment, null);
        assertSelection(environment.withProperty("tsdb.iotdb.enable", "true"), "iotdb");
    }

    @Test void yamlDisabledInvalidBackendDoesNotCompeteWithTheConfiguredBackend() throws Exception {
        assertSelection(yaml("""
                tsdb:
                  iotdb:
                    enable: false
                    username: "${MISSING_DISABLED_USERNAME}"
                    pool:
                      max-size: not-a-number
                  influxdb1:
                    enable: true
                    url: http://127.0.0.1:8086
                """), "influxdb1");
    }

    @ParameterizedTest
    @CsvSource({"iotdb,influxdb", "iotdb,influxdb1", "influxdb,influxdb1"})
    void everyExplicitlyEnabledPairConflicts(String first, String second) {
        assertConfigurationFailure(new MockEnvironment().withProperty("tsdb." + first + ".enable", "true")
                .withProperty("tsdb." + second + ".enable", "true"));
    }

    @ParameterizedTest
    @CsvSource({"iotdb,influxdb", "iotdb,influxdb1", "influxdb,influxdb1"})
    void configuredPairsRemainInactiveWithoutEnableFlags(String first, String second) {
        assertSelection(new MockEnvironment().withProperty("tsdb." + first + ".database", "first_db")
                .withProperty("tsdb." + second + ".database", "second_db"), null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void explicitTrueSelectsABackendWithoutConnectionSettings(String selected) {
        assertSelection(new MockEnvironment().withProperty("tsdb." + selected + ".enable", "true"), selected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"iotdb", "influxdb", "influxdb1"})
    void explicitFalseSuppressesEvenInvalidAndUnresolvedBackendConfiguration(String selected) {
        assertSelection(new MockEnvironment().withProperty("tsdb." + selected + ".enable", "false")
                .withProperty("tsdb." + selected + ".database", "${MISSING_DISABLED_DATABASE}")
                .withProperty("tsdb." + selected + ".max-query-rows", "invalid"), null);
    }

    @Test void disabledBackendDoesNotCompeteWithExplicitSelection() {
        assertSelection(new MockEnvironment().withProperty("tsdb.iotdb.enable", "false")
                .withProperty("tsdb.iotdb.database", "unused_db")
                .withProperty("tsdb.influxdb1.url", "http://127.0.0.1:8086")
                .withProperty("tsdb.influxdb1.enable", "true"), "influxdb1");
    }

    @Test void invalidConnectionPropertiesDoNotEnableTheBackend() {
        assertSelection(new MockEnvironment().withProperty("tsdb.influxdb.url", ""), null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "", "   ", "${MISSING_ENABLE_FLAG}"})
    void invalidExplicitFlagsFailWithConfigurationError(String flag) {
        TSDBException failure = assertConfigurationFailure(new MockEnvironment()
                .withProperty("tsdb.influxdb1.enable", flag));
        assertThat(failure).hasMessageContaining("tsdb.influxdb1.enable");
    }

    @Test void resolvedEnablePlaceholderCanDisableConfiguredBackend() {
        assertSelection(new MockEnvironment().withProperty("selected-backend-enabled", "false")
                .withProperty("tsdb.influxdb.enable", "${selected-backend-enabled}")
                .withProperty("tsdb.influxdb.url", "http://127.0.0.1:8181"), null);
    }

    @Test void enablePlaceholderFallbackCanSelectBackend() {
        assertSelection(new MockEnvironment().withProperty("tsdb.influxdb1.enable", "${OPTIONAL_BACKEND_FLAG:true}"), "influxdb1");
    }

    @ParameterizedTest
    @CsvSource({"TSDB_IOTDB_USERNAME,iotdb", "TSDB_INFLUXDB_URL,influxdb", "TSDB_INFLUXDB1_URL,influxdb1"})
    void relaxedEnvironmentConnectionNamesNeedAnExplicitEnableFlag(String variable, String selected) {
        MockEnvironment environment = new MockEnvironment();
        environment.getPropertySources().addFirst(new SystemEnvironmentPropertySource("test-systemEnvironment", Map.of(variable, "value")));
        assertSelection(environment, null);
        environment.getPropertySources().addFirst(new SystemEnvironmentPropertySource("test-enable",
                Map.of("TSDB_" + selected.toUpperCase(java.util.Locale.ROOT) + "_ENABLE", "true")));
        assertSelection(environment, selected);
    }

    @Test void relaxedEnvironmentEnableFlagOverridesLowerPriorityFileConfiguration() {
        MockEnvironment environment = new MockEnvironment().withProperty("tsdb.influxdb1.url", "http://127.0.0.1:8086")
                .withProperty("tsdb.influxdb1.enable", "true");
        environment.getPropertySources().addFirst(new SystemEnvironmentPropertySource("test-systemEnvironment",
                Map.of("TSDB_INFLUXDB1_ENABLE", "false")));
        assertSelection(environment, null);
    }

    @Test void higherPriorityApplicationProfileCanDisableAnInheritedBackendAndSelectAnother() {
        MockEnvironment environment = new MockEnvironment().withProperty("tsdb.iotdb.database", "inherited_db")
                .withProperty("tsdb.iotdb.enable", "true");
        environment.setActiveProfiles("test");
        environment.getPropertySources().addFirst(new MapPropertySource("application-test.yml",
                Map.of("tsdb.iotdb.enable", false, "tsdb.influxdb1.enable", true)));
        assertSelection(environment, "influxdb1");
    }

    @Test void higherPriorityTrueOverridesLowerPriorityDisableFlag() {
        MockEnvironment environment = new MockEnvironment().withProperty("tsdb.influxdb.enable", "false");
        environment.getPropertySources().addFirst(new MapPropertySource("command-line",
                Map.of("tsdb.influxdb.enable", "true")));
        assertSelection(environment, "influxdb");
    }

    @Test void absentAdapterClassesIgnoreTheirConfigurationAndMalformedEnableFlags() throws Exception {
        try (FilteredClassLoader loader = new FilteredClassLoader(
                com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter.class,
                com.alandevise.tsgate.adapter.impl.InfluxDBAdapter.class)) {
            ConditionContext context = context(new MockEnvironment().withProperty("tsdb.iotdb.enable", "invalid")
                    .withProperty("tsdb.influxdb.enable", "true")
                    .withProperty("tsdb.influxdb1.enable", "true"), loader);
            assertThat(common.matches(context, null)).isTrue();
            assertThat(new TSDBAdapterEnabledCondition.InfluxDB1().matches(context, null)).isTrue();
            assertThat(new TSDBAdapterEnabledCondition.IoTDB().matches(context, null)).isFalse();
        }
    }

    @Test void noClasspathAdaptersMeansNoBackendEvenWithExplicitFlags() throws Exception {
        try (FilteredClassLoader loader = new FilteredClassLoader("com.alandevise.tsgate.adapter.impl")) {
            assertThat(common.matches(context(new MockEnvironment().withProperty("tsdb.iotdb.enable", "true"), loader), null)).isFalse();
        }
    }

    @Test void separateContextsNeverShareTheirSelection() {
        MockEnvironment first = new MockEnvironment().withProperty("tsdb.influxdb1.enable", "true");
        MockEnvironment second = new MockEnvironment().withProperty("tsdb.iotdb.enable", "true");
        assertSelection(first, "influxdb1");
        assertSelection(second, "iotdb");
        assertSelection(first, "influxdb1");
    }

    @Test void nonEnumerablePropertySourcesCanExplicitlyEnableAnAdapter() {
        MockEnvironment environment = new MockEnvironment();
        environment.getPropertySources().addFirst(new PropertySource<Object>("custom") {
            @Override public Object getProperty(String name) {
                return "tsdb.influxdb1.enable".equals(name) ? "true" : null;
            }
        });
        assertSelection(environment, "influxdb1");
    }

    @Test void blankYamlEnableFailsInsteadOfSilentlyDisabling() throws Exception {
        assertConfigurationFailure(yaml("tsdb:\n  influxdb1:\n    enable:\n"));
    }

    private TSDBException assertConfigurationFailure(Environment environment) {
        TSDBException failure = assertThrows(TSDBException.class,
                () -> common.matches(context(environment, getClass().getClassLoader()), null));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.CONFIGURATION_ERROR);
        return failure;
    }

    private void assertSelection(Environment environment, String selected) {
        ConditionContext context = context(environment, getClass().getClassLoader());
        assertThat(common.matches(context, null)).isEqualTo(selected != null);
        assertThat(new TSDBAdapterEnabledCondition.IoTDB().matches(context, null)).isEqualTo("iotdb".equals(selected));
        assertThat(new TSDBAdapterEnabledCondition.InfluxDB().matches(context, null)).isEqualTo("influxdb".equals(selected));
        assertThat(new TSDBAdapterEnabledCondition.InfluxDB1().matches(context, null)).isEqualTo("influxdb1".equals(selected));
    }

    private MockEnvironment yaml(String text) throws Exception {
        MockEnvironment environment = new MockEnvironment();
        for (PropertySource<?> source : new YamlPropertySourceLoader().load("test-yaml",
                new ByteArrayResource(text.getBytes(StandardCharsets.UTF_8)))) {
            environment.getPropertySources().addFirst(source);
        }
        return environment;
    }

    private ConditionContext context(Environment environment, ClassLoader loader) {
        ConditionContext context = mock(ConditionContext.class);
        when(context.getEnvironment()).thenReturn(environment);
        when(context.getClassLoader()).thenReturn(loader);
        return context;
    }
}
