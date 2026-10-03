package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsgate.annotation.*;
import com.alandevise.tsgate.config.InfluxDBProperties;
import com.alandevise.tsgate.config.StrictCursorSqlStrategyEnum;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.*;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.http.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** Local real-server comparisons of composite cursor strategies and their boundary cases. */
class StrictCursorUnionDockerIT {
    static final String URL = System.getProperty("tsdb.it.influxdb.url", "http://127.0.0.1:18184");
    static final boolean LEGACY = Boolean.getBoolean("tsdb.it.influxdb.legacy-or-failure");
    static final long BASE = System.currentTimeMillis() - 3_600_000;
    static final String TABLE = "strict_cursor_union";
    final HttpClient http = HttpClient.newHttpClient();
    String database;
    InfluxDBAdapter adapter;

    @BeforeEach void setUp() throws Exception {
        database = "union_it_" + Long.toUnsignedString(System.nanoTime(), 36);
        request("POST", "/api/v3/configure/database", "{\"db\":\"" + database + "\"}");
        adapter = adapter(StrictCursorSqlStrategyEnum.UNION_ALL, 1000);
        List<TSDBRecord> records = new ArrayList<>();
        for (String tenant : List.of("wanted", "excluded")) {
            for (int time = 0; time < 5; time++) {
                List<String> devices = List.of("a", "b", "c'quote", "d");
                for (int device = 0; device < devices.size(); device++) {
                    records.add(new TSDBRecord(TABLE, BASE + time,
                            Map.of("tenant", tenant, "device", devices.get(device)),
                            Map.of("value", (double) ((time + device) % 3 + 1),
                                    "Value", (double) (100 + time * 10 + device), "marker", time * 10 + device)));
                }
            }
        }
        assertTrue(adapter.batchWrite(null, records));
    }

    @AfterEach void tearDown() throws Exception {
        if (adapter != null) adapter.close();
        if (database != null) request("DELETE", "/api/v3/configure/database?db=" + database, null);
    }

    @Test void ascendingTimeKeepsEveryTagAtInclusiveEndBoundary() {
        assertAllPages(List.of(asc("time"), asc("device"), asc("tenant")));
    }

    @Test void descendingTimeKeepsEveryTagAtInclusiveStartBoundary() {
        assertAllPages(List.of(desc("time"), desc("device"), desc("tenant")));
    }

    @Test void fieldFirstMixedDirectionsKeepEqualFieldRowsAcrossEveryPage() {
        assertAllPages(List.of(desc("value"), asc("device"), desc("time"), asc("tenant")));
    }

    @Test void fieldFirstWithTimeInMiddleKeepsLexicographicOrdering() {
        assertAllPages(List.of(asc("value"), desc("time"), asc("device"), desc("tenant")));
    }

    @Test void directProjectionHidesSortKeysAndOffsetAppliesAfterUnion() {
        List<SortSpec> order = List.of(asc("time"), asc("device"), asc("tenant"));
        List<Map<String, Object>> reference = rows(adapter, query(order));
        TSDBQuery page = query(order);
        page.setStrictCursor(true);
        page.setCursorValues(cursor(reference.get(1), order));
        page.setSelectColumns(List.of("value"));
        page.setOffset(2);
        page.setLimit(3);
        List<Map<String, Object>> result = rows(adapter, page);
        assertEquals(reference.subList(4, 7).stream().map(row -> Map.of("time", row.get("time"), "_time", row.get("time"), "value", row.get("value"))).toList(), result);
        for (Map<String, Object> row : result) assertEquals(Set.of("time", "_time", "value"), row.keySet());
    }

    @Test void fluentProjectionAppendsKeysAndAllowsOnlyOneProbeRow() {
        adapter.close();
        adapter = adapter(StrictCursorSqlStrategyEnum.UNION_ALL, 2);
        TGTemplate template = new TGTemplate(adapter);
        List<Long> markers = new ArrayList<>();
        Map<String, Object> cursor = null;
        for (int pageNumber = 0; pageNumber < 10; pageNumber++) {
            PageResult<Point> page = template.query(Point.class).select("marker")
                    .whereTag("tenant", "wanted").timeRange(BASE + 1, BASE + 3)
                    .orderByFieldDesc("value").thenByFieldAsc("device").thenByFieldDesc("time")
                    .cursor(cursor).limit(2).strictCursorPage();
            for (Point row : page.getRows()) {
                assertNotNull(row.time); assertNotNull(row.device); assertNotNull(row.tenant); assertNotNull(row.value);
                markers.add(row.marker);
            }
            if (!page.isHasNext()) break;
            cursor = page.getNextCursor();
            assertTrue(pageNumber < 9, "cursor must terminate");
        }
        assertEquals(List.of(20L, 11L, 32L, 23L, 10L, 31L, 22L, 13L, 30L, 21L, 12L, 33L), markers);
        assertEquals(12, new HashSet<>(markers).size());
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class,
                () -> template.query(Point.class).limit(3).list()).getErrorCode());
    }

    @Test void differentlyCasedProjectedAndHiddenSortColumnsRemainDistinct() {
        List<SortSpec> order = List.of(asc("value"), asc("time"), asc("device"), asc("tenant"));
        InfluxDBAdapter original = adapter(null, 1000);
        try {
            List<Map<String, Object>> reference = rows(original, query(order));
            TSDBQuery page = query(order);
            page.setStrictCursor(true);
            page.setCursorValues(cursor(reference.get(1), order));
            page.setSelectColumns(List.of("Value"));
            page.setLimit(3);
            List<Map<String, Object>> actual = rows(adapter, page);
            assertEquals(reference.subList(2, 5).stream().map(row -> Map.of("time", row.get("time"),
                    "_time", row.get("time"), "Value", row.get("Value"))).toList(), actual);
            assertTrue(actual.stream().allMatch(row -> row.keySet().equals(Set.of("time", "_time", "Value"))));
            if (!LEGACY) assertEquals(actual, rows(original, page));
        } finally { original.close(); }
    }

    @Test void cursorOutsideTimeRangeReturnsEmptyWithoutContradictoryScan() {
        for (SortOrderEnum direction : SortOrderEnum.values()) {
            List<SortSpec> order = List.of(new SortSpec("time", direction), asc("device"), asc("tenant"));
            TSDBQuery page = query(order);
            page.setStrictCursor(true);
            page.setCursorValues(Map.of("time", direction == SortOrderEnum.ASC ? BASE + 10 : BASE - 10,
                    "device", "a", "tenant", "wanted"));
            assertTrue(rows(adapter, page).isEmpty());
        }
    }

    @Test void singleTimeKeyAtEndpointReturnsEmpty() {
        for (SortOrderEnum direction : SortOrderEnum.values()) {
            TSDBQuery page = query(List.of(new SortSpec("time", direction)));
            page.setStrictCursor(true);
            page.setCursorValues(Map.of("time", direction == SortOrderEnum.ASC ? BASE + 3 : BASE + 1));
            assertTrue(rows(adapter, page).isEmpty());
        }
    }

    @Test void impossibleTagFilterCannotLeakRowsFromAnotherBranch() {
        TSDBQuery page = query(List.of(asc("time"), asc("device"), asc("tenant")));
        page.setStrictCursor(true);
        page.setCursorValues(Map.of("time", BASE + 1, "device", "a", "tenant", "wanted"));
        List<QueryFilter> filters = new ArrayList<>(page.getFilters());
        filters.add(new QueryFilter("device", OperatorEnum.EQ, List.of("does-not-exist")));
        page.setFilters(filters);
        assertTrue(rows(adapter, page).isEmpty());
    }

    @Test void defaultOrIsPreservedAndModernResultMatchesUnion() {
        InfluxDBAdapter original = adapter(null, 1000);
        try {
            for (List<SortSpec> order : List.of(
                    List.of(asc("time"), asc("device"), asc("tenant")),
                    List.of(desc("time"), desc("device"), desc("tenant")),
                    List.of(desc("value"), asc("device"), desc("time"), asc("tenant")),
                    List.of(asc("value"), desc("time"), asc("device"), desc("tenant")))) {
                TSDBQuery page = query(order);
                page.setStartTime(null); page.setEndTime(null);
                Map<String, Object> middle = rows(adapter, page).get(6);
                page.setStrictCursor(true);
                page.setCursorValues(cursor(middle, order));
                page.setLimit(4);
                if (LEGACY) {
                    TSDBException error = assertThrows(TSDBException.class, () -> rows(original, page));
                    assertEquals(TSDBErrorCodeEnum.CONNECTION_ERROR, error.getErrorCode());
                    assertTrue(error.toString().contains("500"), error.toString());
                } else {
                    assertEquals(rows(adapter, page), rows(original, page));
                }
            }
        } finally { original.close(); }
    }

    private void assertAllPages(List<SortSpec> order) {
        List<Map<String, Object>> reference = rows(adapter, query(order));
        assertEquals(12, reference.size());
        List<Map<String, Object>> collected = new ArrayList<>();
        Map<String, Object> previous = null;
        for (int pageNumber = 0; pageNumber < 10; pageNumber++) {
            TSDBQuery page = query(order); page.setStrictCursor(true); page.setLimit(4);
            if (previous != null) page.setCursorValues(previous);
            List<Map<String, Object>> result = rows(adapter, page);
            List<Map<String, Object>> visible = result.subList(0, Math.min(result.size(), 3));
            collected.addAll(visible);
            if (result.size() <= 3) break;
            previous = cursor(visible.get(visible.size() - 1), order);
            assertTrue(pageNumber < 9, "cursor must terminate");
        }
        assertEquals(reference, collected);
        assertEquals(12, new HashSet<>(collected).size());
        assertTrue(collected.stream().allMatch(row -> "wanted".equals(row.get("tenant"))));
    }

    private TSDBQuery query(List<SortSpec> order) {
        TSDBQuery query = new TSDBQuery(); query.setMeasurement(TABLE);
        query.setStartTime(BASE + 1); query.setEndTime(BASE + 3);
        query.setSortSpecs(order); query.setCursorColumns(order.stream().map(SortSpec::column).toList());
        query.setFilters(List.of(new QueryFilter("tenant", OperatorEnum.EQ, List.of("wanted")),
                new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1d, 3d)),
                new QueryFilter("device", OperatorEnum.IN, List.of("a", "b", "c'quote", "d"))));
        return query;
    }

    private InfluxDBAdapter adapter(StrictCursorSqlStrategyEnum strategy, int maxRows) {
        InfluxDBProperties properties = new InfluxDBProperties(); properties.setUrl(URL); properties.setDatabase(database);
        properties.setMaxQueryRows(maxRows);
        if (strategy != null) properties.setStrictCursorSql(strategy);
        InfluxDBAdapter created = new InfluxDBAdapter(properties, false); created.init(); return created;
    }

    private static List<Map<String, Object>> rows(InfluxDBAdapter selected, TSDBQuery query) {
        QueryResult result = selected.query(null, query); assertTrue(result.isSuccess(), result.getMessage()); return result.getRows();
    }
    private static Map<String, Object> cursor(Map<String, Object> row, List<SortSpec> order) {
        Map<String, Object> values = new LinkedHashMap<>(); order.forEach(sort -> values.put(sort.column(), row.get(sort.column()))); return values;
    }
    private static SortSpec asc(String key) { return new SortSpec(key, SortOrderEnum.ASC); }
    private static SortSpec desc(String key) { return new SortSpec(key, SortOrderEnum.DESC); }
    private void request(String method, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(URL + path)).header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() / 100 == 2, response.statusCode() + " " + response.body());
    }

    @TGMeasurement(TABLE)
    public static class Point {
        @TGTime public Long time;
        @TGTag public String device;
        @TGTag public String tenant;
        @TGField public Double value;
        @TGField public Long marker;
        public Point() { }
    }
}
