package com.alandevise.tsgate.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.alandevise.tsgate.adapter.impl.OpenGeminiAdapter;
import com.alandevise.tsgate.annotation.TGField;
import com.alandevise.tsgate.annotation.TGMeasurement;
import com.alandevise.tsgate.annotation.TGTag;
import com.alandevise.tsgate.annotation.TGTime;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.config.OpenGeminiProperties;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class OpenGeminiStarterDockerIT {
    @ParameterizedTest @ValueSource(booleans = {true, false})
    void autoDiscoveredStarterProvidesWorkingTemplateAndBorrowedOfficialClient(boolean omitDatabase) throws Exception {
        String url = System.getProperty("tsdb.it.opengemini.url", "http://127.0.0.1:18086");
        String database = omitDatabase ? "tsdb" : "it_spring_opengemini_" + Long.toUnsignedString(System.nanoTime(), 36);
        Map<String, Object> settings = new HashMap<>(Map.of("spring.main.banner-mode", "off",
                "tsdb.opengemini.url", url, "tsdb.query-log-enabled", "false"));
        settings.put("tsdb.opengemini.enable", "true");
        settings.put("tsdb.iotdb.username", "${UNUSED_BACKEND_SETTING}");
        settings.put("tsdb.influxdb.url", "${UNUSED_BACKEND_SETTING}");
        settings.put("tsdb.influxdb1.url", "${UNUSED_BACKEND_SETTING}");
        if (!omitDatabase) settings.put("tsdb.opengemini.database", database);
        HttpClient admin = HttpClient.newHttpClient();
        executeAdmin(admin, url, "CREATE DATABASE " + database + " REPLICAS "
                + Integer.getInteger("tsdb.it.opengemini.replicas", 1));
        OpenGeminiAdapter adapter;
        try {
            awaitDatabaseRaft(database);
            try (var context = new SpringApplicationBuilder(App.class).web(WebApplicationType.NONE)
                    .properties(settings).run()) {
                assertEquals(1, context.getBeansOfType(TGTemplate.class).size());
                assertTrue(context.getBean(OpenGeminiProperties.class).isEnable());
                assertEquals(1, context.getBeansOfType(com.alandevise.tsgate.adapter.TSDBAdapter.class).size());
                assertTrue(context.getBeansOfType(com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter.class).isEmpty());
                assertTrue(context.getBeansOfType(com.alandevise.tsgate.adapter.impl.InfluxDBAdapter.class).isEmpty());
                assertTrue(context.getBeansOfType(com.alandevise.tsgate.adapter.impl.InfluxDB1Adapter.class).isEmpty());
                assertEquals(database, context.getBean(OpenGeminiProperties.class).getDatabase());
                adapter = context.getBean(OpenGeminiAdapter.class);
                assertSame(adapter.getNativeClient(), context.getBean(InfluxDB.class));
                TGTemplate template = context.getBean(TGTemplate.class);
                Point point = new Point();
                point.time = System.currentTimeMillis();
                point.device = "spring";
                point.value = 42.0;
                assertTrue(template.write(point));
                awaitRawPoint(admin, url, database, point);
                assertEquals(42.0, template.query(Point.class).list().get(0).value);
                var nativeResult = context.getBean(InfluxDB.class).query(new Query("SELECT value FROM spring_opengemini_metrics", database));
                assertFalse(nativeResult.hasError());
                assertEquals(1, nativeResult.getResults().get(0).getSeries().get(0).getValues().size());
            }
            TSDBException closed = assertThrows(TSDBException.class, adapter::getNativeClient);
            assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, closed.getErrorCode());
        } finally {
            executeAdmin(admin, url, "DROP DATABASE " + database);
        }
    }

    /** Waits for this owned test database's final data-Raft leader without writing any points. */
    private static void awaitDatabaseRaft(String database) throws Exception {
        int replicas = Integer.parseInt(System.getProperty("tsdb.it.opengemini.replicas", "1"));
        String mode = System.getProperty("tsdb.it.opengemini.mode", replicas == 3 ? "cluster" : "single");
        if ("single".equals(mode) && replicas == 1) return;
        if (!"cluster".equals(mode) || replicas != 3) {
            throw new IllegalStateException("The data-Raft gate requires a three-replica cluster fixture");
        }
        String script = System.getProperty("tsdb.it.opengemini.fixture.script");
        String state = System.getProperty("tsdb.it.opengemini.fixture.state");
        String python = System.getProperty("tsdb.it.opengemini.fixture.python", "python3");
        if (script == null || state == null || python.isBlank()
                || !Path.of(script).isAbsolute() || !Path.of(state).isAbsolute()
                || !Files.isRegularFile(Path.of(script)) || !Files.isRegularFile(Path.of(state))) {
            throw new IllegalStateException("Cluster integration tests require absolute fixture script and state properties");
        }
        Path output = Files.createTempFile("tsgate-database-raft-", ".log");
        Process process = null;
        try {
            process = new ProcessBuilder(python, script, "wait-database", "--state", state, "--database", database)
                    .redirectErrorStream(true).redirectOutput(output.toFile()).start();
            if (!process.waitFor(70, TimeUnit.SECONDS)) {
                throw new AssertionError("Database data-Raft gate exceeded 70 seconds: " + database);
            }
            assertEquals(0, process.exitValue(), Files.readString(output));
        } finally {
            if (process != null && process.isAlive()) {
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
            Files.deleteIfExists(output);
        }
    }

    /** Waits for the server's query index, independently of the adapter under test. */
    private static void awaitRawPoint(HttpClient client, String url, String database, Point expected) throws Exception {
        String endpoint = url + "/query?db=" + URLEncoder.encode(database, StandardCharsets.UTF_8)
                + "&epoch=ms&q=" + URLEncoder.encode("SELECT * FROM \"spring_opengemini_metrics\"", StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        String lastBody = "No response";
        while (System.nanoTime() < deadline) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            lastBody = response.body();
            assertEquals(200, response.statusCode(), lastBody);
            JsonNode envelope = mapper.readTree(lastBody);
            int count = 0;
            boolean matched = false;
            boolean pending = false;
            if (envelope.has("error")) {
                assertEquals("measurement not found", envelope.get("error").asText(), lastBody);
                pending = true;
            }
            for (JsonNode result : envelope.path("results")) {
                if (result.has("error")) {
                    assertEquals("measurement not found", result.get("error").asText(), lastBody);
                    pending = true;
                }
                for (JsonNode series : result.path("series")) {
                    JsonNode columns = series.path("columns");
                    int time = -1, device = -1, value = -1;
                    for (int index = 0; index < columns.size(); index++) {
                        switch (columns.get(index).asText()) {
                            case "time" -> time = index;
                            case "device" -> device = index;
                            case "value" -> value = index;
                        }
                    }
                    for (JsonNode row : series.path("values")) {
                        count++;
                        JsonNode tag = device < 0 ? series.path("tags").path("device") : row.path(device);
                        matched |= time >= 0 && value >= 0 && row.path(time).isNumber() && row.path(value).isNumber()
                                && row.path(time).decimalValue().compareTo(BigDecimal.valueOf(expected.time)) == 0
                                && expected.device.equals(tag.asText())
                                && Double.compare(expected.value, row.path(value).doubleValue()) == 0;
                    }
                }
            }
            if (!pending && count == 1 && matched) return;
            Thread.sleep(50);
        }
        fail("Raw HTTP oracle did not observe the exact timestamp/device/value within 30 seconds: " + lastBody);
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

    @TGMeasurement("spring_opengemini_metrics")
    public static class Point {
        @TGTime public Long time;
        @TGTag public String device;
        @TGField public Double value;
        public Point() { }
    }
}
