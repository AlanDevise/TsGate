package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.InfluxDBProperties;
import com.alandevise.tsgate.config.StrictCursorSqlStrategyEnum;
import org.springframework.test.util.ReflectionTestUtils;
import com.alandevise.tsgate.contract.HttpSharedAdapterFixture;
import com.alandevise.tsgate.contract.SharedAdapterContract;
import com.alandevise.tsgate.contract.SharedAdapterFixture;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/** Executes shared semantics through the InfluxDB 3 JSON query and line-protocol paths. */
class InfluxDB3SharedAdapterContractTest implements SharedAdapterContract {
    @Override
    public String backendId() { return "influxdb3"; }

    @Override
    public SharedAdapterFixture createFixture() throws IOException { return new Fixture(); }

    private static final class Fixture extends HttpSharedAdapterFixture {
        private final InfluxDBProperties properties = new InfluxDBProperties();
        private final ObjectMapper mapper = new ObjectMapper();

        private Fixture() throws IOException {
            super(false);
            try {
                properties.setUrl(peer.url());
                properties.setDatabase(ORIGINAL_DATABASE);
                properties.setMaxBatchRecords(ORIGINAL_MAX_BATCH_RECORDS);
                properties.setMaxQueryRows(ORIGINAL_MAX_QUERY_ROWS);
                properties.setStrictCursorSql(StrictCursorSqlStrategyEnum.UNION_ALL);
                properties.getHttpClient().setRetryOnConnectionFailure(false);
                adapter = new InfluxDBAdapter(properties, false);
            } catch (RuntimeException | Error failure) {
                close();
                throw failure;
            }
        }

        @Override
        public Object nativeResource() { return ((InfluxDBAdapter) adapter).getNativeClient(); }

        @Override
        public void mutateOriginalConfiguration() {
            properties.setUrl("http://127.0.0.1:1");
            properties.setDatabase("changed_database");
            properties.setMaxBatchRecords(1);
            properties.setMaxQueryRows(100);
            properties.getHttpClient().setReadTimeoutMs(-1);
            properties.setHttpClient(null);
            properties.setStrictCursorSql(StrictCursorSqlStrategyEnum.OR);
        }

        @Override
        public java.util.Map<String, String> configurationValues() {
            StrictCursorSqlStrategyEnum actual = (StrictCursorSqlStrategyEnum) ReflectionTestUtils.getField(adapter, "strictCursorSql");
            return java.util.Map.of("tsdb.influxdb.strict-cursor-sql", actual.name().toLowerCase(java.util.Locale.ROOT).replace('_', '-'));
        }

        @Override
        public String lastQuerySql() { return requestValue("q"); }

        @Override
        public String lastDatabase() { return requestValue("db"); }

        private String requestValue(String key) {
            try {
                return mapper.readTree(peer.lastRequest().body()).get(key).asText();
            } catch (IOException error) {
                throw new AssertionError("Invalid captured query request", error);
            }
        }
    }
}
