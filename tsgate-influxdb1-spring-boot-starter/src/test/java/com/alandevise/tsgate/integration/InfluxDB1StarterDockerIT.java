package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.InfluxDB1Adapter;
import com.alandevise.tsgate.annotation.TGField;
import com.alandevise.tsgate.annotation.TGMeasurement;
import com.alandevise.tsgate.annotation.TGTag;
import com.alandevise.tsgate.annotation.TGTime;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.config.InfluxDB1Properties;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class InfluxDB1StarterDockerIT {
    @ParameterizedTest @ValueSource(booleans = {true, false})
    void autoDiscoveredStarterProvidesWorkingTemplateAndBorrowedOfficialClient(boolean omitDatabase) throws Exception {
        String url = System.getProperty("tsdb.it.influxdb1.url", "http://127.0.0.1:18086");
        String database = omitDatabase ? "tsdb" : "it_spring_influx1_" + Long.toUnsignedString(System.nanoTime(), 36);
        Map<String, Object> settings = new HashMap<>(Map.of("spring.main.banner-mode", "off",
                "tsdb.influxdb1.url", url, "tsdb.query-log-enabled", "false"));
        settings.put("tsdb.influxdb1.enable", "true");
        settings.put("tsdb.iotdb.username", "${UNUSED_BACKEND_SETTING}");
        settings.put("tsdb.influxdb.url", "${UNUSED_BACKEND_SETTING}");
        if (!omitDatabase) settings.put("tsdb.influxdb1.database", database);
        HttpClient admin = HttpClient.newHttpClient();
        executeAdmin(admin, url, "CREATE DATABASE " + database);
        InfluxDB1Adapter adapter;
        try {
            try (var context = new SpringApplicationBuilder(App.class).web(WebApplicationType.NONE)
                    .properties(settings).run()) {
                assertEquals(1, context.getBeansOfType(TGTemplate.class).size());
                assertTrue(context.getBean(InfluxDB1Properties.class).isEnable());
                assertEquals(1, context.getBeansOfType(com.alandevise.tsgate.adapter.TSDBAdapter.class).size());
                assertTrue(context.getBeansOfType(com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter.class).isEmpty());
                assertTrue(context.getBeansOfType(com.alandevise.tsgate.adapter.impl.InfluxDBAdapter.class).isEmpty());
                assertEquals(database, context.getBean(InfluxDB1Properties.class).getDatabase());
                adapter = context.getBean(InfluxDB1Adapter.class);
                assertSame(adapter.getNativeClient(), context.getBean(InfluxDB.class));
                TGTemplate template = context.getBean(TGTemplate.class);
                Point point = new Point();
                point.time = System.currentTimeMillis();
                point.device = "spring";
                point.value = 42.0;
                assertTrue(template.write(point));
                assertEquals(42.0, template.query(Point.class).list().get(0).value);
                var nativeResult = context.getBean(InfluxDB.class).query(new Query("SELECT value FROM spring_metrics", database));
                assertFalse(nativeResult.hasError());
                assertEquals(1, nativeResult.getResults().get(0).getSeries().get(0).getValues().size());
            }
            TSDBException closed = assertThrows(TSDBException.class, adapter::getNativeClient);
            assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, closed.getErrorCode());
        } finally {
            executeAdmin(admin, url, "DROP DATABASE " + database);
        }
    }

    private static void executeAdmin(HttpClient client, String url, String sql) throws Exception {
        HttpResponse<String> result = client.send(HttpRequest.newBuilder(URI.create(url + "/query"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8)))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, result.statusCode(), result.body());
        assertFalse(result.body().contains("\"error\""), result.body());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class App { }

    @TGMeasurement("spring_metrics")
    public static class Point {
        @TGTime public Long time;
        @TGTag public String device;
        @TGField public Double value;
        public Point() { }
    }
}
