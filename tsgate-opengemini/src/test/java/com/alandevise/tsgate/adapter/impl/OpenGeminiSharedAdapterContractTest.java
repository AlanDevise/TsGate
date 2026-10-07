package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.OpenGeminiHttpClientProperties;
import com.alandevise.tsgate.config.OpenGeminiProperties;
import com.alandevise.tsgate.contract.HttpSharedAdapterFixture;
import com.alandevise.tsgate.contract.InfluxQlAggregateOutputContract;
import com.alandevise.tsgate.contract.SharedAdapterFixture;
import com.alandevise.tsgate.contract.SharedAdapterContract;

import java.io.IOException;

/** Exercises openGemini's actual wrapper and its explicitly unsupported optional operations. */
class OpenGeminiSharedAdapterContractTest implements SharedAdapterContract, InfluxQlAggregateOutputContract {
    @Override
    public String backendId() { return "opengemini"; }

    @Override
    public SharedAdapterFixture createFixture() throws IOException { return new Fixture(); }

    private static final class Fixture extends HttpSharedAdapterFixture {
        private final OpenGeminiProperties properties = new OpenGeminiProperties();
        private final OpenGeminiHttpClientProperties http = new OpenGeminiHttpClientProperties();

        private Fixture() throws IOException {
            super(true);
            try {
                properties.setUrl(peer.url());
                properties.setDatabase(ORIGINAL_DATABASE);
                properties.setMaxBatchRecords(ORIGINAL_MAX_BATCH_RECORDS);
                properties.setMaxQueryRows(ORIGINAL_MAX_QUERY_ROWS);
                http.setRetryOnConnectionFailure(false);
                adapter = new OpenGeminiAdapter(properties, http, false);
            } catch (RuntimeException | Error failure) {
                close();
                throw failure;
            }
        }

        @Override
        public Object nativeResource() { return ((OpenGeminiAdapter) adapter).getNativeClient(); }

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
