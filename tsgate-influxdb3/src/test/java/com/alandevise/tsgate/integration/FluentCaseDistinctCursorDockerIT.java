package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsgate.annotation.TGField;
import com.alandevise.tsgate.annotation.TGMeasurement;
import com.alandevise.tsgate.annotation.TGTag;
import com.alandevise.tsgate.annotation.TGTime;
import com.alandevise.tsgate.config.InfluxDBProperties;
import com.alandevise.tsgate.config.StrictCursorSqlStrategyEnum;
import com.alandevise.tsgate.core.TGQueryBuilder;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.model.PageResult;
import com.alandevise.tsgate.model.TSDBRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the complete fluent cursor path against case-sensitive physical columns. */
class FluentCaseDistinctCursorDockerIT {
    private static final String URL = System.getProperty("tsdb.it.influxdb.url", "http://127.0.0.1:18181");
    private static final String TABLE = "fluent_case_cursor";
    private final HttpClient http = HttpClient.newHttpClient();
    private final List<Sample> samples = new ArrayList<>();
    private String database;
    private InfluxDBAdapter adapter;

    static Stream<StrictCursorSqlStrategyEnum> sqlStrategies() {
        return Boolean.getBoolean("tsdb.it.influxdb.only-union")
                ? Stream.of(StrictCursorSqlStrategyEnum.UNION_ALL)
                : Stream.of(StrictCursorSqlStrategyEnum.OR, StrictCursorSqlStrategyEnum.UNION_ALL);
    }

    static Stream<Arguments> cursorOrders() {
        return sqlStrategies().flatMap(strategy -> Stream.of(false, true).flatMap(valueDescending ->
                Stream.of(false, true).map(timeDescending -> Arguments.of(strategy, valueDescending, timeDescending))));
    }

    @BeforeEach
    void createDatabaseAndFixture() throws Exception {
        database = "case_cursor_" + Long.toUnsignedString(System.nanoTime(), 36);
        request("POST", "/api/v3/configure/database", "{\"db\":\"" + database + "\"}");
        long base = System.currentTimeMillis() - 3_600_000;
        for (int time = 0; time < 4; time++) {
            for (int device = 0; device < 3; device++) {
                int marker = time * 3 + device;
                samples.add(new Sample(base + time, "device_" + device, (time + device) % 3, 1000 + marker));
            }
        }
    }

    @AfterEach
    void cleanup() throws Exception {
        if (adapter != null) adapter.close();
        if (database != null) request("DELETE", "/api/v3/configure/database?db=" + database, null);
    }

    @ParameterizedTest(name = "strategy={0}, valueDescending={1}, timeDescending={2}")
    @MethodSource("cursorOrders")
    void projectingUppercaseValueMustNotSupplyTheLowercaseCursor(
            StrictCursorSqlStrategyEnum strategy, boolean valueDescending, boolean timeDescending) {
        InfluxDBProperties properties = new InfluxDBProperties();
        properties.setUrl(URL);
        properties.setDatabase(database);
        properties.setStrictCursorSql(strategy);
        properties.setMaxQueryRows(2);
        adapter = new InfluxDBAdapter(properties, false);
        adapter.init();
        assertTrue(adapter.batchWrite(null, samples.stream().map(sample -> new TSDBRecord(TABLE,
                sample.time(), Map.of("device", sample.device()),
                Map.of("value", sample.value(), "VALUE", sample.uppercaseValue()))).toList()));

        Comparator<Sample> valueOrder = Comparator.comparingDouble(Sample::value);
        Comparator<Sample> timeOrder = Comparator.comparingLong(Sample::time);
        if (valueDescending) valueOrder = valueOrder.reversed();
        if (timeDescending) timeOrder = timeOrder.reversed();
        List<Sample> expected = samples.stream().sorted(valueOrder.thenComparing(timeOrder)
                .thenComparing(Sample::device)).toList();

        TGTemplate template = new TGTemplate(adapter);
        List<Sample> actual = new ArrayList<>();
        Map<String, Object> cursor = null;
        int pages = 0;
        while (true) {
            TGQueryBuilder<Point> query = template.query(Point.class).select("VALUE").limit(2).cursor(cursor);
            if (valueDescending) query.orderByFieldDesc("value"); else query.orderByFieldAsc("value");
            if (timeDescending) query.thenByFieldDesc("time"); else query.thenByFieldAsc("time");
            query.thenByFieldAsc("device");
            PageResult<Map> page = query.strictCursorPage(Map.class);
            assertFalse(page.getRows().isEmpty(), "An advertised next page must contain rows");
            for (Map row : page.getRows()) {
                assertTrue(row.containsKey("VALUE"), "The requested uppercase field must remain projected");
                assertTrue(row.containsKey("value"), "The exact lowercase sort field must be projected independently");
                actual.add(new Sample(((Number) row.get("time")).longValue(), (String) row.get("device"),
                        ((Number) row.get("value")).doubleValue(), ((Number) row.get("VALUE")).doubleValue()));
            }
            pages++;
            assertTrue(pages <= 6, "Pagination must terminate after every unique row is returned");
            if (!page.isHasNext()) {
                assertTrue(page.getNextCursor().isEmpty());
                break;
            }
            cursor = page.getNextCursor();
            Sample last = actual.get(actual.size() - 1);
            assertEquals(last.value(), ((Number) cursor.get("value")).doubleValue());
            assertNotEquals(last.uppercaseValue(), ((Number) cursor.get("value")).doubleValue());
            assertEquals(last.time(), ((Number) cursor.get("time")).longValue());
            assertEquals(last.device(), cursor.get("device"));
        }
        assertEquals(6, pages);
        assertEquals(expected, actual, "All rows and both case-distinct values must survive ordered traversal");
        assertEquals(12, new HashSet<>(actual).size(), "No row may repeat across pages");
    }

    @ParameterizedTest(name = "physical time-like columns, strategy={0}")
    @MethodSource("sqlStrategies")
    void physicalTimeLikeFieldsMustNotBeReplacedByTimestampAliases(StrictCursorSqlStrategyEnum strategy) {
        InfluxDBProperties properties = new InfluxDBProperties();
        properties.setUrl(URL);
        properties.setDatabase(database);
        properties.setStrictCursorSql(strategy);
        adapter = new InfluxDBAdapter(properties, false);
        adapter.init();
        List<TSDBRecord> records = new ArrayList<>();
        for (int index = 0; index < samples.size(); index++) {
            Sample sample = samples.get(index);
            records.add(new TSDBRecord(TABLE, sample.time(), Map.of("device", sample.device()),
                    Map.of("value", sample.value(), "_time", 2000d + index, "TIME", 3000d + index)));
        }
        assertTrue(adapter.batchWrite(null, records));
        TGTemplate template = new TGTemplate(adapter);
        Map<String, Object> cursor = null;
        int seen = 0;
        int pages = 0;
        while (true) {
            PageResult<Map> page = template.query(Point.class).select("_time", "TIME")
                    .orderByTimeAsc().thenByFieldAsc("device").limit(2).cursor(cursor).strictCursorPage(Map.class);
            assertFalse(page.getRows().isEmpty());
            for (Map row : page.getRows()) {
                assertTrue(seen < samples.size(), "Rows must not repeat indefinitely");
                Sample expected = samples.get(seen);
                assertEquals(expected.time(), ((Number) row.get("time")).longValue());
                assertEquals(expected.device(), row.get("device"));
                assertEquals(2000d + seen, ((Number) row.get("_time")).doubleValue());
                assertEquals(3000d + seen, ((Number) row.get("TIME")).doubleValue());
                seen++;
            }
            assertTrue(++pages <= 6);
            if (!page.isHasNext()) break;
            cursor = page.getNextCursor();
            assertEquals(samples.get(seen - 1).time(), ((Number) cursor.get("time")).longValue());
        }
        assertEquals(12, seen);
        assertEquals(6, pages);
    }

    private void request(String method, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(URL + path)).header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() / 100 == 2, response.statusCode() + " " + response.body());
    }

    private record Sample(long time, String device, double value, double uppercaseValue) { }

    @TGMeasurement(TABLE)
    public static class Point {
        @TGTime public Long time;
        @TGTag public String device;
        @TGField public Double value;
        public Point() { }
    }
}
