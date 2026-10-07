package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.OpenGeminiAdapter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.alandevise.tsgate.annotation.*;
import com.alandevise.tsgate.config.*;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.*;
import com.alandevise.tsgate.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.influxdb.dto.Query;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.math.BigDecimal;
import java.util.concurrent.locks.LockSupport;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class OpenGeminiDockerIT {
    static final String URL = System.getProperty("tsdb.it.opengemini.url", "http://127.0.0.1:28086");
    static final String VERSION = System.getProperty("tsdb.it.opengemini.version", "1.5.2");
    static final int REPLICAS = Integer.getInteger("tsdb.it.opengemini.replicas", 1);
    static final List<String> URLS = Arrays.stream(System.getProperty("tsdb.it.opengemini.urls", URL).split(","))
            .map(String::trim).filter(value -> !value.isEmpty()).distinct().toList();
    OpenGeminiAdapter adapter;
    TGTemplate template;
    String database;
    long base;
    HttpClient admin;
    private static final ObjectMapper ORACLE_JSON = new ObjectMapper();

    @BeforeEach
    void setup() throws Exception {
        database = "opengemini_" + Long.toUnsignedString(System.nanoTime(), 36);
        base = System.currentTimeMillis() - 60_000;
        admin = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        executeAdmin("CREATE DATABASE \"" + database + "\" REPLICAS " + REPLICAS);
        initialize(10000, 16L * 1024 * 1024);
    }

    void initialize(int rows, long bytes) {
        initialize(rows, bytes, 64L * 1024 * 1024);
    }

    void initialize(int rows, long bytes, long batchBytes) {
        if (adapter != null) adapter.close();
        OpenGeminiProperties config = new OpenGeminiProperties();
        config.setUrl(URL);
        config.setDatabase(database);
        config.setMaxQueryRows(rows);
        config.setMaxQueryResponseBytes(bytes);
        config.setMaxBatchBytes(batchBytes);
        OpenGeminiHttpClientProperties http = new OpenGeminiHttpClientProperties();
        http.setCallTimeoutMs(15000);
        adapter = new OpenGeminiAdapter(config, http, false);
        adapter.init();
        template = new TGTemplate(adapter);
    }

    @AfterEach
    void cleanup() throws Exception {
        if (adapter != null) adapter.close();
        if (admin != null && database != null) executeAdmin("DROP DATABASE \"" + database + "\"");
    }

    void executeAdmin(String sql) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(URL + "/query"))
                .timeout(Duration.ofSeconds(15)).header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8))).build();
        HttpResponse<String> response = admin.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        assertFalse(response.body().contains("\"error\""), response.body());
    }

    TSDBRecord point(long time, String device, Object value) {
        return new TSDBRecord("points", time, Map.of("device", device), Map.of("value", value));
    }

    TSDBQuery detail() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        query.setOrder(SortOrderEnum.ASC);
        return query;
    }

    void seed() {
        assertTrue(adapter.batchWrite(null, List.of(point(base, "a", 1d), point(base + 1000, "b", 2d), point(base + 2000, "a", 3d), point(base + 3000, "b", 4d))));
        awaitVisible(List.of(point(base, "a", 1d), point(base + 1000, "b", 2d), point(base + 2000, "a", 3d), point(base + 3000, "b", 4d)));
    }

    @Test
    void compatibleNativeHealthReportsTheExactOpenGeminiVersion() throws Exception {
        // openGemini identifies its release independently of InfluxDB-compatible client headers.
        HttpResponse<Void> ping = admin.send(HttpRequest.newBuilder(URI.create(URL + "/ping"))
                .timeout(Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.discarding());
        assertEquals(204, ping.statusCode());
        assertEquals(VERSION, ping.headers().firstValue("X-Geminidb-Version").orElseThrow());
        assertTrue(adapter.getNativeClient().ping().isGood(), "Compatible health request failed for fixture " + VERSION);
        assertEquals("openGemini", adapter.getAdapterName());
    }

    @Test
    void encodedBatchBudgetRejectsBeforeWritingAndAllowsConfiguredReadback() {
        TSDBRecord first = point(base, "existing", 1d);
        TSDBRecord second = point(base + 1, "next", 2d);
        assertTrue(adapter.write(null, first));
        awaitVisible(List.of(first));
        initialize(10000, 16L * 1024 * 1024, 1);
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(second)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(0, failure.getResult().committedRecords());
        assertEquals(1, adapter.count(null, detail()));
        initialize(10000, 16L * 1024 * 1024, 4096);
        assertTrue(adapter.write(null, second));
        awaitVisible(List.of(first, second));
        assertEquals(2, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void aggregateOutputCollisionsFailWhileDistinctAliasesRoundTrip() {
        seed();
        TSDBQuery query = detail();
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.MAX, "duplicate"),
                new AggregationSpec("value", AggregationFunctionEnum.MIN, "duplicate")));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.count(null, query)).getErrorCode());
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.MAX, "maximum"),
                new AggregationSpec("value", AggregationFunctionEnum.MIN, "minimum")));
        Map<String, Object> row = adapter.query(null, query).getRows().get(0);
        assertEquals(4d, ((Number) row.get("maximum")).doubleValue());
        assertEquals(1d, ((Number) row.get("minimum")).doubleValue());
        assertEquals(1, adapter.count(null, query));
        assertEquals(4, adapter.count(null, detail()));
    }

    @Test
    void nativeClientAndTemplateReadIdenticalData() {
        assertTrue(template.batchWrite(List.of(new Point(base, "a", 1.25), new Point(base + 1000, "b", 2.5))));
        awaitVisible(List.of(point(base, "a", 1.25), point(base + 1000, "b", 2.5)));
        List<Point> rows = template.query(Point.class).orderByTimeAsc().list();
        assertEquals(2, rows.size());
        assertEquals(base, rows.get(0).time);
        assertEquals(1.25, rows.get(0).value);
        org.influxdb.dto.QueryResult nativeRows = adapter.getNativeClient().query(new Query("SELECT value FROM points ORDER BY time", database));
        assertNativeSuccess(nativeRows);
        assertEquals(2, nativeRows.getResults().get(0).getSeries().get(0).getValues().size());
    }

    @Test
    void propertyChangesCannotRedirectAdapterOrNativeWrites() {
        adapter.close();
        OpenGeminiProperties properties = new OpenGeminiProperties();
        properties.setUrl(URL);
        properties.setDatabase(database);
        OpenGeminiHttpClientProperties http = new OpenGeminiHttpClientProperties();
        http.setCallTimeoutMs(15000);
        adapter = new OpenGeminiAdapter(properties, http, false);
        properties.setUrl("http://127.0.0.1:1");
        properties.setDatabase("missing_changed_database");
        properties.setUsername("unused_changed_user");
        properties.setPassword("unused_changed_password");
        properties.setRetentionPolicy("missing_changed_policy");
        http.setReadTimeoutMs(-1);
        adapter.init();
        properties.setDatabase("another_missing_database");
        properties.setRetentionPolicy("another_missing_policy");
        assertTrue(adapter.write(null, point(base, "adapter", 1d)));
        adapter.getNativeClient().write("points,device=native value=2.0 " + ((base + 1) * 1_000_000L));
        awaitVisible(List.of(point(base, "adapter", 1d), point(base + 1, "native", 2d)));
        assertEquals(2, adapter.query(null, detail()).getRowCount());
        org.influxdb.dto.QueryResult nativeRows = adapter.getNativeClient().query(new Query("SELECT value FROM points", database));
        assertNativeSuccess(nativeRows);
        assertEquals(2, nativeRows.getResults().get(0).getSeries().get(0).getValues().size());
        TSDBQuery invalid = detail();
        invalid.setLimit(0);
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, invalid)).getErrorCode());
        assertEquals(2, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void timeCursorAndOffsetPagesRespectRowCountSemantics() {
        seed();
        PageResult<Point> first = template.query(Point.class).orderByTimeAsc().limit(2).page();
        assertTrue(first.isHasNext());
        assertEquals(2, first.getRows().size());
        PageResult<Point> second = template.query(Point.class).orderByTimeAsc().cursorTime(first.getNextCursorTime()).limit(2).page();
        assertFalse(second.isHasNext());
        assertEquals(3d, second.getRows().get(0).value);
        PageResult<Point> offset = template.query(Point.class).orderByTimeAsc().page(2, 2);
        assertEquals(4L, offset.getTotal());
        assertEquals(2, offset.getRows().size());
        assertEquals(3d, offset.getRows().get(0).value);
    }

    @Test
    void sparseFieldsCountLogicalRowsRatherThanNonNullFields() {
        assertTrue(adapter.batchWrite(null, List.of(new TSDBRecord("points", base, Map.of(), Map.of("a", 1)),
                new TSDBRecord("points", base + 1000, Map.of(), Map.of("b", 2)),
                new TSDBRecord("points", base + 2000, Map.of(), Map.of("a", 3, "b", 4)))));
        awaitVisible(List.of(new TSDBRecord("points", base, Map.of(), Map.of("a", 1)),
                new TSDBRecord("points", base + 1000, Map.of(), Map.of("b", 2)),
                new TSDBRecord("points", base + 2000, Map.of(), Map.of("a", 3, "b", 4))));
        assertEquals(3, adapter.count(null, detail()));
    }

    @Test
    void groupingTagsAndMeanUseInfluxQlSeriesTags() {
        seed();
        TSDBQuery query = detail();
        query.setGroupByTags(List.of("device"));
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.AVG, "mean")));
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(0).get("device"));
        assertEquals(2d, ((Number) rows.get(0).get("mean")).doubleValue());
        assertEquals(3d, ((Number) rows.get(1).get("mean")).doubleValue());
        assertEquals(2, adapter.count(null, query));
    }

    @Test
    void fixedOffsetTimeWindowAndCountWork() {
        seed();
        TSDBQuery query = detail();
        query.setStartTime(base - 3600000);
        query.setEndTime(base + 10000);
        query.setGroupByTime("1h");
        query.setTimeZone("+08:00");
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "sum")));
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(10d, rows.stream().mapToDouble(row -> ((Number) row.get("sum")).doubleValue()).sum());
        assertTrue(rows.stream().allMatch(row -> row.get("window_start") instanceof Long));
        assertEquals(rows.size(), adapter.count(null, query));
    }

    @Test
    void inBetweenAndEscapedStringFiltersRoundTrip() {
        assertTrue(adapter.write(null, point(base, "O'Brien", 1d)));
        assertTrue(adapter.write(null, point(base + 1000, "other", 2d)));
        awaitVisible(List.of(point(base, "O'Brien", 1d), point(base + 1000, "other", 2d)));
        TSDBQuery query = detail();
        query.getFilters().add(new QueryFilter("device", OperatorEnum.IN, List.of("O'Brien", "absent")));
        query.getFilters().add(new QueryFilter("value", OperatorEnum.BETWEEN, List.of(0, 3)));
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(1, rows.size());
        assertEquals("O'Brien", rows.get(0).get("device"));
    }

    @Test
    void actualPartialWriteReportsUnknownInsteadOfFalseZeroCommit() {
        assertTrue(adapter.write(null, point(base, "a", 1d)));
        awaitVisible(List.of(point(base, "a", 1d)));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class, () -> adapter.batchWriteDetailed(null,
                List.of(point(base + 1000, "a", 2d), point(base + 2000, "a", "conflicting-type"))));
        assertEquals(BatchCommitStateEnum.UNKNOWN, error.getResult().commitState());
        assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, error.getErrorCode());
        assertEquals(0, error.getResult().committedRecords());
        awaitVisible(List.of(point(base, "a", 1d), point(base + 1000, "a", 2d)));
        assertEquals(2, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void rawAndOrdinaryQueriesHaveStrictCapWhilePagesCanProbe() {
        seed();
        initialize(2, 4096);
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points LIMIT 3")).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class, () -> template.query(Point.class).limit(3).list()).getErrorCode());
        PageResult<Point> page = template.query(Point.class).orderByTimeAsc().limit(2).page();
        assertEquals(2, page.getRows().size());
        assertTrue(page.isHasNext());
        assertEquals(4, adapter.count(null, detail()));
    }

    @Test
    void responseByteLimitFailsWithoutPartialSuccess() {
        seed();
        initialize(100, 40);
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class, () -> adapter.query(null, detail())).getErrorCode());
    }

    @Test
    void unsupportedDialectContractsAreExplicit() {
        seed();
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class,
                () -> template.query(Point.class).orderByFieldAsc("value").list()).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class,
                () -> template.query(Point.class).cursorColumns("time", "device").limit(2).strictCursorPage()).getErrorCode());
    }

    @Test
    void emptyWritesAndPreflightFailuresDoNotCommit() {
        assertTrue(adapter.batchWriteDetailed(null, List.of()).isSuccess());
        TSDBRecord invalid = new TSDBRecord("bad\nmeasurement", base, Map.of(), Map.of("value", 1));
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(base, "a", 1d), invalid)));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(0, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void measurementEqualsAndBackslashesInTagAndFieldNamesRoundTripWithoutRenamingData() {
        String measurement = "room=west";
        String tag = "C:\\path";
        TSDBRecord point = new TSDBRecord(measurement, base, Map.of("device\\path", tag), Map.of("field\\name", 7));
        assertTrue(adapter.write(null, point));
        awaitVisible(List.of(point));
        TSDBQuery query = detail();
        query.setMeasurement(measurement);
        query.getFilters().add(new QueryFilter("device\\path", OperatorEnum.EQ, List.of(tag)));
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(1, rows.size());
        assertEquals(tag, rows.get(0).get("device\\path"));
        assertEquals(7, ((Number) rows.get(0).get("field\\name")).intValue());
    }

    @Test
    void exactIntegersRoundTripAndUnsafeValuesOrPredicatesFailPreflight() {
        java.math.BigInteger safe = new java.math.BigInteger("9007199254740994");
        List<TSDBRecord> expected = List.of(point(base, "a", safe), point(base + 1000, "a", Long.MIN_VALUE));
        assertTrue(adapter.batchWrite(null, expected));
        awaitVisible(expected);
        List<Map<String, Object>> rows = adapter.query(null, detail()).getRows();
        assertEquals(2, rows.size());
        assertEquals(9007199254740994L, ((Number) rows.get(0).get("value")).longValue());
        assertEquals(Long.MIN_VALUE, ((Number) rows.get(1).get("value")).longValue());
        for (Number invalid : List.of(new java.math.BigInteger("9007199254740993"), -9007199254740993L,
                Long.MAX_VALUE, Long.MIN_VALUE + 1, new java.math.BigInteger("9223372036854775808"),
                new java.math.BigDecimal("0.1"))) {
            TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                    () -> adapter.batchWriteDetailed(null, List.of(point(base + 2000, "a", 2L), point(base + 3000, "a", invalid))));
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
            assertEquals(0, error.getResult().totalBatches());
        }
        TSDBQuery invalidQuery = detail();
        invalidQuery.getFilters().add(new QueryFilter("value", OperatorEnum.EQ, List.of(9007199254740993L)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, invalidQuery)).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.count(null, invalidQuery)).getErrorCode());
        assertEquals(2, adapter.count(null, detail()));
    }

    @Test
    void selectorGroupingUsesTagsForGlobalPagingDespiteDifferentSourceTimes() {
        assertTrue(adapter.batchWrite(null, List.of(point(base + 1000, "a", 2d), point(base, "b", 1d))));
        awaitVisible(List.of(point(base + 1000, "a", 2d), point(base, "b", 1d)));
        TSDBQuery query = detail();
        query.setGroupByTags(List.of("device"));
        query.setLimit(1);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.MIN, "minimum")));
        assertEquals("a", adapter.query(null, query).getRows().get(0).get("device"));
        query.setOffset(1);
        assertEquals("b", adapter.query(null, query).getRows().get(0).get("device"));
    }

    @Test
    void actualTimeAliasFieldSurvivesQueryNormalization() {
        assertTrue(adapter.write(null, new TSDBRecord("points", base, Map.of(), Map.of("value", 1d, "_time", 123L))));
        awaitVisible(List.of(new TSDBRecord("points", base, Map.of(), Map.of("value", 1d, "_time", 123L))));
        Map<String, Object> row = adapter.query(null, detail()).getRows().get(0);
        assertEquals(base, row.get("time"));
        assertEquals(123L, ((Number) row.get("_time")).longValue());
    }

    @Test
    void futureRowsAreIncludedOnlyWithExplicitWindowBounds() {
        long future = System.currentTimeMillis() + 7200000;
        assertTrue(adapter.write(null, point(future, "a", 7d)));
        awaitVisible(List.of(point(future, "a", 7d)));
        TSDBQuery query = detail();
        query.setGroupByTime("1h");
        query.setStartTime(future - 1000);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        query.setEndTime(future + 1000);
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(1, rows.size());
        assertEquals(7d, ((Number) rows.get(0).get("total")).doubleValue());
    }

    @Test
    void unsafeNamesAndTagOnlyProjectionAreRejectedBeforeDataLoss() {
        assertTrue(adapter.write(null, point(base, "a", 1d)));
        awaitVisible(List.of(point(base, "a", 1d)));
        TSDBRecord comment = new TSDBRecord("#sensor", base, Map.of(), Map.of("value", 1d));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(base + 1000, "b", 2d), comment))).getResult().commitState());
        for (String key : List.of("_field", "_measurement", "time")) {
            TSDBRecord invalid = new TSDBRecord("points", base + 1000, Map.of(key, "reserved"), Map.of("value", 2d));
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                    () -> adapter.batchWriteDetailed(null, List.of(invalid))).getResult().commitState());
        }
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class,
                () -> template.query(Point.class).select("device").list()).getErrorCode());
        assertEquals(1, adapter.count(null, detail()));
        assertEquals("a", template.query(Point.class).select("device", "value").list().get(0).device);
    }

    @Test
    void nativeMutationsAndMultipleStatementsAreRejectedWithoutChangingData() {
        assertTrue(adapter.write(null, point(base, "a", 1d)));
        awaitVisible(List.of(point(base, "a", 1d)));
        for (String sql : List.of("DROP DATABASE \"" + database + "\"", "DROP MEASUREMENT points", "DELETE FROM points",
                "SELECT * INTO copied FROM points", "SELECT * FROM points; DROP MEASUREMENT points",
                "SELECT * FROM points; SELECT * INTO copied FROM points")) {
            assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                    assertThrows(TSDBException.class, () -> adapter.executeQuery(sql)).getErrorCode());
            assertEquals(1, adapter.executeQuery("SELECT * FROM points").getRowCount());
        }
        List<Map<String, Object>> measurements = adapter.executeQuery("SHOW MEASUREMENTS").getRows();
        assertEquals(1, measurements.size());
        assertEquals("points", measurements.get(0).get("name"));
    }

    @Test
    void nativeReadOnlyQueryAllowsQuotedIntoAndSemicolons() {
        String value = "INTO; O'Brien";
        assertTrue(adapter.write(null, new TSDBRecord("points", base, Map.of("device", "a"), Map.of("value", 1d, "INTO", value))));
        awaitVisible(List.of(new TSDBRecord("points", base, Map.of("device", "a"), Map.of("value", 1d, "INTO", value))));
        List<Map<String, Object>> rows = adapter.executeQuery("SELECT \"INTO\" FROM \"points\" WHERE \"INTO\" = 'INTO; O\\'Brien';").getRows();
        assertEquals(1, rows.size());
        assertEquals(value, rows.get(0).get("INTO"));
    }

    @ParameterizedTest
    @EnumSource(AggregationFunctionEnum.class)
    void allCommonAggregationsReturnTheirInfluxQlValues(AggregationFunctionEnum function) {
        seed();
        TSDBQuery query = detail();
        query.setAggregations(List.of(new AggregationSpec("value", function, "result")));
        double expected = switch (function) {
            case COUNT, MAX, LAST -> 4d;
            case SUM -> 10d;
            case AVG -> 2.5d;
            case MIN, FIRST -> 1d;
        };
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(1, rows.size());
        assertEquals(expected, ((Number) rows.get(0).get("result")).doubleValue());
    }

    @ParameterizedTest
    @EnumSource(OperatorEnum.class)
    void allCommonFieldFiltersSelectExpectedRows(OperatorEnum operator) {
        seed();
        TSDBQuery query = detail();
        List<Object> values = switch (operator) {
            case LT, LE -> List.of(3);
            case IN -> List.of(2, 4);
            case BETWEEN -> List.of(2, 3);
            default -> List.of(2);
        };
        List<Double> expected = switch (operator) {
            case EQ -> List.of(2d);
            case NE -> List.of(1d, 3d, 4d);
            case GT -> List.of(3d, 4d);
            case GE -> List.of(2d, 3d, 4d);
            case LT -> List.of(1d, 2d);
            case LE -> List.of(1d, 2d, 3d);
            case IN -> List.of(2d, 4d);
            case BETWEEN -> List.of(2d, 3d);
        };
        query.getFilters().add(new QueryFilter("value", operator, values));
        List<Double> actual = adapter.query(null, query).getRows().stream()
                .map(row -> ((Number) row.get("value")).doubleValue()).toList();
        assertEquals(expected, actual);
    }

    @Test
    void descendingTimeCursorPagesFollowReverseChronologicalOrder() {
        seed();
        PageResult<Point> first = template.query(Point.class).orderByTimeDesc().limit(2).page();
        assertTrue(first.isHasNext());
        assertEquals(List.of(4d, 3d), first.getRows().stream().map(row -> row.value).toList());
        PageResult<Point> second = template.query(Point.class).orderByTimeDesc()
                .cursorTime(first.getNextCursorTime()).limit(2).page();
        assertFalse(second.isHasNext());
        assertEquals(List.of(2d, 1d), second.getRows().stream().map(row -> row.value).toList());
    }

    @Test
    void everyConfiguredSqlEntrySharesTemplateAndNativeWrites() {
        assertFalse(URLS.isEmpty(), "At least one SQL entry point is required");
        List<OpenGeminiAdapter> entries = new ArrayList<>();
        Map<String, Double> expected = new LinkedHashMap<>();
        List<TSDBRecord> expectedRecords = new ArrayList<>();
        try {
            for (int index = 0; index < URLS.size(); index++) {
                OpenGeminiProperties properties = new OpenGeminiProperties();
                properties.setUrl(URLS.get(index));
                properties.setDatabase(database);
                OpenGeminiHttpClientProperties http = new OpenGeminiHttpClientProperties();
                http.setCallTimeoutMs(15000);
                OpenGeminiAdapter entry = new OpenGeminiAdapter(properties, http, false);
                entries.add(entry);
                entry.init();
                String templateDevice = "template_" + index;
                String nativeDevice = "native_" + index;
                double templateValue = index + 0.25;
                double nativeValue = index + 0.75;
                long time = base + index * 1000L;
                assertTrue(new TGTemplate(entry).write(new Point(time, templateDevice, templateValue)));
                entry.getNativeClient().write("points,device=" + nativeDevice + " value=" + nativeValue
                        + " " + ((time + 1) * 1_000_000L));
                expected.put(templateDevice, templateValue);
                expected.put(nativeDevice, nativeValue);
                expectedRecords.add(point(time, templateDevice, templateValue));
                expectedRecords.add(point(time + 1, nativeDevice, nativeValue));
            }
            for (String url : URLS) awaitVisible(url, expectedRecords);
            for (OpenGeminiAdapter entry : entries) {
                List<Point> rows = new TGTemplate(entry).query(Point.class).orderByTimeAsc().list();
                assertEquals(expected.size(), rows.size());
                Map<String, Double> actual = new LinkedHashMap<>();
                for (Point row : rows) actual.put(row.device, row.value);
                assertEquals(expected, actual);
                org.influxdb.dto.QueryResult nativeRows = entry.getNativeClient()
                        .query(new Query("SELECT value, device FROM points ORDER BY time", database));
                assertNativeSuccess(nativeRows);
                Map<String, Double> nativeValues = new LinkedHashMap<>();
                for (org.influxdb.dto.QueryResult.Result result : nativeRows.getResults()) {
                    for (org.influxdb.dto.QueryResult.Series series : result.getSeries()) {
                        int deviceIndex = series.getColumns().indexOf("device");
                        int valueIndex = series.getColumns().indexOf("value");
                        assertTrue(deviceIndex >= 0 && valueIndex >= 0, series.getColumns().toString());
                        for (List<Object> row : series.getValues()) {
                            assertNull(nativeValues.put(String.valueOf(row.get(deviceIndex)),
                                    ((Number) row.get(valueIndex)).doubleValue()), "Duplicate native row");
                        }
                    }
                }
                assertEquals(expected, nativeValues);
            }
        } finally {
            for (OpenGeminiAdapter entry : entries) entry.close();
        }
    }

    @Test
    void largeWritesSplitRequestsAndRetainAllRows() {
        List<TSDBRecord> records = new ArrayList<>();
        for (int index = 0; index < 5001; index++) records.add(point(base + index, "large", (long) index));
        BatchWriteResult result = adapter.batchWriteDetailed(null, records);
        assertEquals(BatchCommitStateEnum.SUCCESS, result.commitState());
        assertEquals(5001, result.committedRecords());
        assertEquals(2, result.committedBatches());
        awaitVisible(records);
        assertEquals(5001, adapter.count(null, detail()));
    }

    @Test
    void closedAdapterRejectsOperationsAndCannotBeReinitialized() {
        adapter.close();
        adapter.close();
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, adapter::init).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, adapter::getNativeClient).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, detail())).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.batchWriteDetailed(null, List.of())).getErrorCode());
    }


    // The server acknowledges new series before its periodically merged index is query-visible.
    // This independent oracle establishes fixture data, before any adapter feature assertions.
    void awaitVisible(List<TSDBRecord> expected) {
        awaitVisible(URL, expected);
    }

    void awaitVisible(String url, List<TSDBRecord> expected) {
        Map<String, List<TSDBRecord>> measurements = new LinkedHashMap<>();
        for (TSDBRecord record : expected)
            measurements.computeIfAbsent(record.measurement(), ignored -> new ArrayList<>()).add(record);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        String last = "No response";
        try {
            while (System.nanoTime() < deadline) {
                boolean complete = true;
                for (Map.Entry<String, List<TSDBRecord>> entry : measurements.entrySet()) {
                    List<TSDBRecord> records = entry.getValue();
                    long minimum = records.stream().mapToLong(TSDBRecord::timestamp).min().orElseThrow();
                    long maximum = records.stream().mapToLong(TSDBRecord::timestamp).max().orElseThrow();
                    String measurement = entry.getKey().replace("\\", "\\\\").replace("\"", "\\\"");
                    String sql = "SELECT * FROM \"" + measurement + "\" WHERE time >= '"
                            + Instant.ofEpochMilli(minimum) + "' AND time <= '" + Instant.ofEpochMilli(maximum) + "'";
                    String parameters = "db=" + URLEncoder.encode(database, StandardCharsets.UTF_8)
                            + "&epoch=ms&q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8);
                    HttpResponse<String> response = admin.send(HttpRequest.newBuilder(URI.create(url + "/query?" + parameters))
                            .timeout(Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString());
                    assertEquals(200, response.statusCode(), response.body());
                    last = response.body();
                    JsonNode envelope = ORACLE_JSON.readTree(last);
                    assertFalse(envelope.has("error"), last);
                    assertTrue(envelope.path("results").isArray(), last);
                    Map<Long, List<Map<String, JsonNode>>> byTime = new HashMap<>();
                    int rows = 0;
                    for (JsonNode result : envelope.path("results")) {
                        if (result.has("error")) {
                            assertEquals("measurement not found", result.path("error").asText(), last);
                            complete = false;
                        }
                        for (JsonNode series : result.path("series")) {
                            List<String> columns = new ArrayList<>();
                            for (JsonNode column : series.path("columns")) columns.add(column.asText());
                            for (JsonNode values : series.path("values")) {
                                Map<String, JsonNode> row = new LinkedHashMap<>();
                                series.path("tags").fields().forEachRemaining(field -> row.put(field.getKey(), field.getValue()));
                                assertEquals(columns.size(), values.size(), last);
                                for (int index = 0; index < columns.size(); index++) row.put(columns.get(index), values.get(index));
                                assertNotNull(row.get("time"), last);
                                byTime.computeIfAbsent(row.get("time").longValue(), ignored -> new ArrayList<>()).add(row);
                                rows++;
                            }
                        }
                    }
                    if (rows != records.size()) complete = false;
                    for (TSDBRecord record : records) {
                        List<Map<String, JsonNode>> candidates = byTime.getOrDefault(record.timestamp(), List.of());
                        if (candidates.stream().noneMatch(row -> matchesRecord(row, record))) complete = false;
                    }
                }
                if (complete) return;
                if (Thread.currentThread().isInterrupted()) fail("Interrupted while awaiting fixture visibility");
                LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
            }
        } catch (Exception error) {
            throw new AssertionError("Independent openGemini fixture oracle failed", error);
        }
        fail("Complete fixture points did not become visible within 30 seconds at " + url + ": " + last);
    }

    static boolean matchesRecord(Map<String, JsonNode> row, TSDBRecord record) {
        for (Map.Entry<String, String> tag : record.tags().entrySet()) {
            JsonNode actual = row.get(tag.getKey());
            if (actual == null || !Objects.equals(tag.getValue(), actual.asText())) return false;
        }
        for (Map.Entry<String, Object> field : record.fields().entrySet()) {
            JsonNode actual = row.get(field.getKey());
            Object expected = field.getValue();
            if (actual == null) return false;
            if (expected instanceof Number number) {
                if (!actual.isNumber() || new BigDecimal(number.toString()).compareTo(actual.decimalValue()) != 0) return false;
            } else if (expected instanceof Boolean bool) {
                if (!actual.isBoolean() || actual.booleanValue() != bool) return false;
            } else if (!Objects.equals(String.valueOf(expected), actual.asText())) return false;
        }
        return true;
    }

    static void assertNativeSuccess(org.influxdb.dto.QueryResult result) {
        assertNotNull(result);
        assertFalse(result.hasError(), result.getError());
        assertNotNull(result.getResults(), "Native result statements must be present");
        for (org.influxdb.dto.QueryResult.Result statement : result.getResults())
            assertNull(statement.getError(), "Native statement must not contain a backend error");
    }

    @Test
    void structuredMissingMeasurementReadsAreEmptyWhileNativeErrorsRemainVisible() {
        assertEquals(0, adapter.query(null, detail()).getRowCount());
        assertEquals(0, adapter.count(null, detail()));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points")).getErrorCode());
        org.influxdb.dto.QueryResult nativeResult = adapter.getNativeClient().query(new Query("SELECT * FROM points", database));
        assertNotNull(nativeResult.getResults());
        assertEquals(1, nativeResult.getResults().size());
        assertEquals("measurement not found", nativeResult.getResults().get(0).getError(),
                "Borrowed native query must preserve the statement-level backend error");
    }

    @Test
    void invalidServerMeasurementNamesAreRejectedBeforeAnyBatchCanCommit() {
        for (String name : List.of("bad,measurement", "bad;measurement", "bad/measurement", "bad\\measurement", ".", "..")) {
            TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class, () -> adapter.batchWriteDetailed(null,
                    List.of(point(base, "a", 1d), new TSDBRecord(name, base, Map.of(), Map.of("value", 1)))));
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
            assertEquals(0, failure.getResult().totalBatches());
        }
        assertEquals(0, adapter.query(null, detail()).getRowCount());
    }

    @TGMeasurement("points")
    public static class Point {
        @TGTime
        public long time;
        @TGTag
        public String device;
        @TGField
        public double value;

        public Point() {
        }

        Point(long time, String device, double value) {
            this.time = time;
            this.device = device;
            this.value = value;
        }
    }
}
