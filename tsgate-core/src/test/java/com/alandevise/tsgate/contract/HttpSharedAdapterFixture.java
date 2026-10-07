package com.alandevise.tsgate.contract;

import com.alandevise.tsgate.adapter.TSDBAdapter;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Shared HTTP test setup for JSON-row and InfluxQL responses, independent of backend implementation types. */
public abstract class HttpSharedAdapterFixture implements SharedAdapterFixture {
    protected final HttpContractPeer peer;
    protected TSDBAdapter adapter;
    private final boolean influxQl;

    protected HttpSharedAdapterFixture(boolean influxQl) throws IOException {
        this.influxQl = influxQl;
        peer = new HttpContractPeer(influxQl ? "{\"results\":[{}]}" : "[]");
    }

    @Override
    public TSDBAdapter adapter() { return adapter; }

    @Override
    public void initialize() { adapter.init(); }

    @Override
    public int ioCount() { return peer.requestCount(); }

    @Override
    public String lastQuerySql() { return formValue("q"); }

    @Override
    public String lastDatabase() { return formValue("db"); }

    private String formValue(String key) {
        return Arrays.stream(peer.lastRequest().body().split("&"))
                .filter(value -> value.startsWith(key + "="))
                .map(value -> URLDecoder.decode(value.substring(key.length() + 1), StandardCharsets.UTF_8))
                .findFirst().orElseThrow();
    }

    @Override
    public void enqueueRows(long... timestamps) {
        String rows = Arrays.stream(timestamps).mapToObj(time -> influxQl
                        ? "[" + time + ",\"a\",1]"
                        : "{\"time\":" + time + ",\"device\":\"a\",\"value\":1}")
                .collect(Collectors.joining(","));
        String body = influxQl ? "{\"results\":[{\"series\":[{\"name\":\"points\","
                + "\"columns\":[\"time\",\"device\",\"value\"],\"values\":[" + rows + "]}]}]}"
                : "[" + rows + "]";
        peer.reply(200, body);
    }

    @Override
    public void enqueueCount(int total) {
        if (influxQl) {
            long[] timestamps = new long[total];
            Arrays.setAll(timestamps, index -> index + 1L);
            enqueueRows(timestamps);
        } else {
            peer.reply(200, "[{\"total\":" + total + "}]");
        }
    }

    @Override
    public void enqueueWriteSuccess() { peer.reply(204, ""); }

    @Override
    public void enqueueUnknownWriteFailure() { peer.reply(500, "server unavailable"); }

    @Override
    public int physicalBatchSize() { return 5_000; }

    @Override
    public void close() {
        try {
            if (adapter != null) adapter.close();
        } finally {
            peer.close();
        }
    }
}
