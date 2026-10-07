package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.InfluxDB1HttpClientProperties;
import com.alandevise.tsgate.config.InfluxDB1Properties;
import com.alandevise.tsgate.contract.HttpSharedAdapterFixture;
import com.alandevise.tsgate.contract.InfluxQlAggregateOutputContract;
import com.alandevise.tsgate.contract.SharedAdapterFixture;

import java.io.IOException;

/** Executes the same public contract as every other backend, through its real HTTP adapter. */
class InfluxDB1SharedAdapterContractTest implements InfluxQlAggregateOutputContract {
    @Override
    public String backendId() { return "influxdb1"; }

    @Override
    public SharedAdapterFixture createFixture() throws IOException { return new Fixture(); }

    private static final class Fixture extends HttpSharedAdapterFixture {
        private final InfluxDB1Properties properties = new InfluxDB1Properties();
        private final InfluxDB1HttpClientProperties http = new InfluxDB1HttpClientProperties();

        private Fixture() throws IOException {
            super(true);
            try {
                properties.setUrl(peer.url());
                properties.setDatabase(ORIGINAL_DATABASE);
                properties.setMaxBatchRecords(ORIGINAL_MAX_BATCH_RECORDS);
                properties.setMaxQueryRows(ORIGINAL_MAX_QUERY_ROWS);
                http.setRetryOnConnectionFailure(false);
                adapter = new InfluxDB1Adapter(properties, http, false);
            } catch (RuntimeException | Error failure) {
                close();
                throw failure;
            }
        }

        @Override
        public Object nativeResource() { return ((InfluxDB1Adapter) adapter).getNativeClient(); }

        @Override
        public void mutateOriginalConfiguration() {
            properties.setUrl("http://127.0.0.1:1");
            properties.setDatabase("changed_database");
            properties.setMaxBatchRecords(1);
            properties.setMaxQueryRows(100);
            http.setReadTimeoutMs(-1);
        }
    }
}
