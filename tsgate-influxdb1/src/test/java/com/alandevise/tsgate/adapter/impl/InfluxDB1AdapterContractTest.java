package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.*;
import com.alandevise.tsgate.exception.*;
import com.alandevise.tsgate.model.*;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class InfluxDB1AdapterContractTest {
    HttpServer server;
    InfluxDB1Adapter adapter;
    InfluxDB1Properties config;
    InfluxDB1HttpClientProperties http;
    final Queue<Reply> replies = new ConcurrentLinkedQueue<>();
    final List<Request> requests = Collections.synchronizedList(new ArrayList<>());

    record Reply(int status, String body) {
    }

    record Request(String path, String body, String authorization) {
    }

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(new Request(exchange.getRequestURI().toString(),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
                    exchange.getRequestHeaders().getFirst("Authorization")));
            Reply reply = replies.poll();
            if (reply == null) reply = new Reply(200, "{\"results\":[{}]}");
            byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(reply.status(), reply.status() == 204 ? -1 : bytes.length);
            if (reply.status() != 204) exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        config = new InfluxDB1Properties();
        config.setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        config.setDatabase("contract");
        http = new InfluxDB1HttpClientProperties();
        http.setRetryOnConnectionFailure(false);
        adapter = new InfluxDB1Adapter(config, http, false);
        adapter.init();
    }

    @AfterEach
    void cleanup() {
        if (adapter != null) adapter.close();
        if (server != null) server.stop(0);
    }

    static String response(String columns, String values) {
        return "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"points\",\"columns\":" + columns + ",\"values\":" + values + "}]}]}";
    }

    TSDBQuery detail() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        return query;
    }

    TSDBRecord point(long time, Object value) {
        return new TSDBRecord("points", time, Map.of("device", "a"), Map.of("value", value));
    }

    String sql() {
        String body = requests.get(requests.size() - 1).body();
        return Arrays.stream(body.split("&")).filter(v -> v.startsWith("q=")).map(v -> URLDecoder.decode(v.substring(2), StandardCharsets.UTF_8)).findFirst().orElseThrow();
    }

    void resetLimits(int rows, long bytes) {
        adapter.close();
        config.setMaxQueryRows(rows);
        config.setMaxQueryResponseBytes(bytes);
        adapter = new InfluxDB1Adapter(config, http, false);
        adapter.init();
    }

    @Test
    void normalizesMillisecondsAndPreservesDecimalPrecision() {
        replies.add(new Reply(200, response("[\"time\",\"value\"]", "[[9007199254740993.0,0.1234567890123456789]]")));
        Map<String, Object> row = adapter.executeQuery("SELECT * FROM points").getRows().get(0);
        assertEquals(9007199254740993L, row.get("time"));
        assertEquals(9007199254740993L, row.get("_time"));
        assertEquals(new java.math.BigDecimal("0.1234567890123456789"), row.get("value"));
        assertTrue(requests.get(0).body().contains("epoch=ms"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"bad\"", "1.5", "9223372036854775808", "true", "\"\""})
    void invalidTimesAreQueryErrors(String time) {
        replies.add(new Reply(200, response("[\"time\",\"value\"]", "[[" + time + ",1]]")));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points")).getErrorCode());
    }

    @Test
    void translatesFiltersAndEscapesInfluxQlQuotes() {
        TSDBQuery query = detail();
        query.setMeasurement("quoted\"measurement");
        query.setStartTime(1000L);
        query.setEndTime(2000L);
        query.setLimit(2);
        query.setOffset(1);
        query.getFilters().add(new QueryFilter("device", OperatorEnum.IN, List.of("a'b", "c")));
        query.getFilters().add(new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1, 9)));
        adapter.query(null, query);
        assertTrue(sql().contains("FROM \"quoted\\\"measurement\""), sql());
        assertTrue(sql().contains("\"device\" = 'a\\'b' OR \"device\" = 'c'"), sql());
        assertTrue(sql().contains("\"value\" >= 1 AND \"value\" <= 9"), sql());
        assertTrue(sql().contains("time >= '1970-01-01T00:00:01Z'"), sql());
        assertTrue(sql().contains("LIMIT 2 OFFSET 1"), sql());
    }

    @Test
    void fixedOffsetWindowUsesMeanAndNormalizedWindowStart() {
        TSDBQuery query = detail();
        query.setGroupByTime("1d");
        query.setTimeZone("+08:00");
        query.setStartTime(0L);
        query.setEndTime(100000L);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.AVG, "mean")));
        replies.add(new Reply(200, response("[\"time\",\"mean\"]", "[[1000,2.5]]")));
        assertEquals(1000L, adapter.query(null, query).getRows().get(0).get("window_start"));
        assertTrue(sql().contains("MEAN(\"value\") AS \"mean\""), sql());
        assertTrue(sql().contains("GROUP BY time(1d, -28800000ms) fill(none)"), sql());
    }

    @Test
    void refusesUnsupportedSortCursorAndDstCalendar() {
        TSDBQuery query = detail();
        query.setSortSpecs(List.of(new SortSpec("value", SortOrderEnum.ASC)));
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        query.setSortSpecs(List.of());
        query.setStrictCursor(true);
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        query.setStrictCursor(false);
        query.setGroupByTime("1d");
        query.setTimeZone("America/New_York");
        query.setStartTime(java.time.Instant.parse("2026-03-08T05:00:00Z").toEpochMilli());
        query.setEndTime(java.time.Instant.parse("2026-03-09T04:00:00Z").toEpochMilli());
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "sum")));
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void globalCapCoversAllSeriesAndPaginationRequiresExplicitProbe() {
        resetLimits(2, 4096);
        String json = response("[\"time\",\"value\"]", "[[1,1],[2,2],[3,3]]");
        replies.add(new Reply(200, json));
        assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points"));
        replies.add(new Reply(200, json));
        assertThrows(TSDBException.class, () -> adapter.query(null, detail()));
        TSDBQuery page = detail();
        page.setPaginationProbe(true);
        replies.add(new Reply(200, json));
        assertEquals(3, adapter.query(null, page).getRowCount());
        replies.add(new Reply(200, response("[\"time\",\"value\"]", "[[1,1],[2,2],[3,3],[4,4]]")));
        assertThrows(TSDBException.class, () -> adapter.query(null, page));
        String multiple = "{\"results\":[{\"series\":[{\"tags\":{\"device\":\"a\"},\"columns\":[\"time\",\"value\"],\"values\":[[1,1],[2,2]]},{\"tags\":{\"device\":\"b\"},\"columns\":[\"time\",\"value\"],\"values\":[[3,3]]}]}]}";
        replies.add(new Reply(200, multiple));
        assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points GROUP BY device"));
    }

    @Test
    void countScansActualRowsInsteadOfCountingNullableFields() {
        resetLimits(2, 4096);
        replies.add(new Reply(200, response("[\"time\",\"a\",\"b\"]", "[[1,1,null],[2,null,2],[3,3,3]]")));
        TSDBQuery query = detail();
        query.setLimit(0);
        query.setOffset(-1);
        query.setCursorTime(999L);
        assertEquals(3, adapter.count(null, query));
        assertFalse(sql().contains("COUNT("));
        assertFalse(sql().contains("LIMIT"));
        assertFalse(sql().contains("OFFSET"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "null", "{\"error\":\"bad\"}", "{\"results\":[{\"error\":\"bad\"}]}", "{\"results\":[{\"partial\":true}]}", "{\"results\":[{\"series\":[{\"columns\":[\"time\"],\"values\":[[1]],\"partial\":true}]}]}", "{\"results\":[{\"series\":[{\"columns\":[\"time\"],\"values\":[[1,2]]}]}]}", "{\"results\":[{}]} []"})
    void malformedErrorOrPartialResponsesNeverBecomeSuccess(String json) {
        replies.add(new Reply(200, json));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT 1")).getErrorCode());
    }

    @Test
    void byteLimitAppliesToStreamingAndCount() {
        resetLimits(10, 50);
        replies.add(new Reply(200, response("[\"time\",\"value\"]", "[[1,1]]")));
        assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points"));
        replies.add(new Reply(200, response("[\"time\",\"value\"]", "[[1,1]]")));
        assertThrows(TSDBException.class, () -> adapter.count(null, detail()));
    }

    @Test
    void partialWriteResponseIsUnknownWhileAuthenticationRejectionIsDefinite() {
        replies.add(new Reply(400, "{\"error\":\"partial write: field type conflict dropped=1\"}"));
        TSDBBatchWriteException partial = assertThrows(TSDBBatchWriteException.class, () -> adapter.batchWriteDetailed(null, List.of(point(1, 1), point(2, "conflict"))));
        assertEquals(BatchCommitStateEnum.UNKNOWN, partial.getResult().commitState());
        assertEquals(0, partial.getResult().committedRecords());
        assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, partial.getErrorCode());
        replies.add(new Reply(401, "unauthorized"));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class, () -> adapter.batchWriteDetailed(null, List.of(point(1, 1)))).getResult().commitState());
    }

    @Test
    void lineProtocolUsesV1EndpointPrecisionAndBasicCredentials() {
        adapter.close();
        config.setUsername("user");
        config.setPassword("secret");
        config.setRetentionPolicy("autogen");
        adapter = new InfluxDB1Adapter(config, http, false);
        adapter.init();
        replies.add(new Reply(204, ""));
        assertTrue(adapter.write(null, point(123, 7)));
        Request request = requests.get(0);
        assertEquals("/write?db=contract&precision=ms&rp=autogen", request.path());
        assertTrue(request.body().endsWith(" 123"));
        assertEquals("Basic " + Base64.getEncoder().encodeToString("user:secret".getBytes(StandardCharsets.ISO_8859_1)), request.authorization());
        assertFalse(config.toString().contains("secret"));
    }

    @Test
    void constructionSnapshotPreservesCredentialsRetentionLimitsAndHttpSettings() {
        adapter.close();
        config.setUsername("user");
        config.setPassword("secret");
        config.setRetentionPolicy("autogen");
        config.setMaxBatchRecords(1);
        config.setMaxQueryRows(1);
        config.setMaxQueryResponseBytes(1024);
        http.setConnectTimeoutMs(1234);
        http.setReadTimeoutMs(2345);
        http.setWriteTimeoutMs(3456);
        http.setCallTimeoutMs(4567);
        adapter = new InfluxDB1Adapter(config, http, false);
        config.setUrl("http://127.0.0.1:1");
        config.setDatabase("changed_database");
        config.setUsername("changed_user");
        config.setPassword("changed_password");
        config.setRetentionPolicy("changed_policy");
        config.setMaxBatchRecords(100);
        config.setMaxQueryRows(100);
        config.setMaxQueryResponseBytes(1);
        http.setReadTimeoutMs(-1);
        adapter.init();
        okhttp3.OkHttpClient actualHttp = (okhttp3.OkHttpClient) ReflectionTestUtils.getField(adapter, "client");
        assertNotNull(actualHttp);
        assertEquals(1234, actualHttp.connectTimeoutMillis());
        assertEquals(2345, actualHttp.readTimeoutMillis());
        assertEquals(3456, actualHttp.writeTimeoutMillis());
        assertEquals(4567, actualHttp.callTimeoutMillis());
        replies.add(new Reply(204, ""));
        assertTrue(adapter.write(null, point(1, 1)));
        replies.add(new Reply(204, ""));
        adapter.getNativeClient().write("points value=2i 2");
        String authorization = "Basic " + Base64.getEncoder().encodeToString("user:secret".getBytes(StandardCharsets.ISO_8859_1));
        assertEquals(2, requests.size());
        for (Request request : requests) {
            assertTrue(request.path().contains("db=contract"));
            assertTrue(request.path().contains("rp=autogen"));
            assertEquals(authorization, request.authorization());
        }
        replies.add(new Reply(200, response("[\"time\",\"value\"]", "[[1,1]]")));
        assertEquals(1, adapter.query(null, detail()).getRowCount());
        replies.add(new Reply(200, response("[\"time\",\"value\"]", "[[1,1],[2,2]]")));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, detail())).getErrorCode());
        assertEquals(1, adapter.getMaxBatchRecords());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(3, 3), point(4, 4)))).getResult().commitState());
    }

    @Test
    void failedNativeInitializationRetriesTheConstructionSnapshot() {
        adapter.close();
        adapter = new InfluxDB1Adapter(config, http, false);
        try (var factory = org.mockito.Mockito.mockStatic(org.influxdb.InfluxDBFactory.class)) {
            factory.when(() -> org.influxdb.InfluxDBFactory.connect(
                    org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any(okhttp3.OkHttpClient.Builder.class)))
                    .thenThrow(new IllegalStateException("synthetic resource failure"));
            assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    assertThrows(TSDBException.class, adapter::init).getErrorCode());
        }
        config.setUrl("http://127.0.0.1:1");
        config.setDatabase("changed_after_failure");
        http.setReadTimeoutMs(-1);
        adapter.init();
        assertTrue(adapter.executeQuery("SHOW MEASUREMENTS").isSuccess());
        assertTrue(requests.get(0).body().contains("db=contract"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "zero-limit", "negative-limit", "negative-offset", "reversed-time"})
    void rejectsInvalidDirectQueryBeforeHttp(String invalid) {
        TSDBQuery query = detail();
        switch (invalid) {
            case "null" -> query = null;
            case "zero-limit" -> query.setLimit(0);
            case "negative-limit" -> query.setLimit(-1);
            case "negative-offset" -> query.setOffset(-1);
            case "reversed-time" -> { query.setStartTime(2L); query.setEndTime(1L); }
        }
        TSDBQuery argument = query;
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, argument)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void countRejectsReversedBoundsAfterIgnoringPaging() {
        TSDBQuery query = detail();
        query.setLimit(0);
        query.setOffset(-1);
        query.setStartTime(2L);
        query.setEndTime(1L);
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.count(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void preflightFailureAndNonFiniteValueSendNoRequests() {
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(1, 1), point(2, Double.NaN)))).getResult().commitState());
        assertTrue(requests.isEmpty());
    }

    @Test
    void initializationIsIdempotentAndCloseIsTerminal() {
        Object nativeClient = adapter.getNativeClient();
        Object client = ReflectionTestUtils.getField(adapter, "client");
        adapter.init();
        assertSame(nativeClient, adapter.getNativeClient());
        assertSame(client, ReflectionTestUtils.getField(adapter, "client"));
        adapter.close();
        adapter.close();
        assertNull(ReflectionTestUtils.getField(adapter, "client"));
        assertNull(ReflectionTestUtils.getField(adapter, "nativeClient"));
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, assertThrows(TSDBException.class, adapter::init).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, assertThrows(TSDBException.class, () -> adapter.executeQuery("SHOW MEASUREMENTS")).getErrorCode());
    }

    @Test
    void invalidConfigurationFailsBeforeInitialization() {
        http.setReadTimeoutMs(-1);
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR, assertThrows(TSDBException.class, () -> new InfluxDB1Adapter(config, http, false)).getErrorCode());
    }

    @Test
    void lineProtocolPreservesMeasurementEqualsAndLiteralBackslashes() {
        replies.add(new Reply(204, ""));
        TSDBRecord record = new TSDBRecord("room=west\\zone", 123L,
                Map.of("device\\path", "C:\\path"), Map.of("field\\name", 7));
        assertTrue(adapter.write(null, record));
        assertEquals("room=west\\zone,device\\path=C:\\path field\\name=7i 123", requests.get(0).body());
    }

    @ParameterizedTest
    @ValueSource(strings = {"_field", "_measurement", "time"})
    void reservedKeysFailBeforeSendingAnyRecord(String key) {
        TSDBRecord badTag = new TSDBRecord("points", 1L, Map.of(key, "x"), Map.of("value", 1));
        TSDBRecord badField = new TSDBRecord("points", 1L, Map.of(), Map.of(key, 1));
        for (TSDBRecord bad : List.of(badTag, badField)) {
            TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                    () -> adapter.batchWriteDetailed(null, List.of(point(0, 1), bad)));
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        }
        assertTrue(requests.isEmpty());
    }

    @Test
    void commentMeasurementsFailBeforeSendingAnyRecord() {
        TSDBRecord comment = new TSDBRecord("#sensor", 1L, Map.of(), Map.of("value", 1));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(0, 1), comment))).getResult().commitState());
        assertTrue(requests.isEmpty());
    }

    @Test
    void arbitraryPrecisionNumbersAreNeverSilentlyWrittenAsRoundedFloat64() {
        replies.add(new Reply(204, ""));
        assertTrue(adapter.write(null, point(1, new java.math.BigInteger("9007199254740993"))));
        assertTrue(requests.get(0).body().contains("value=9007199254740993i"));
        for (Number bad : List.of(new java.math.BigInteger("9223372036854775808"),
                new java.math.BigDecimal("0.1"), new java.math.BigDecimal("9007199254740993.0"))) {
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                    () -> adapter.batchWriteDetailed(null, List.of(point(2, 1), point(3, bad)))).getResult().commitState());
        }
        assertEquals(1, requests.size());
        replies.add(new Reply(204, ""));
        assertTrue(adapter.write(null, point(4, new java.math.BigDecimal("1.25"))));
        assertTrue(requests.get(1).body().contains("value=1.25"));
    }

    @Test
    void groupedSelectorsSortByTagsRatherThanTheSelectedPointsTime() {
        String json = "{\"results\":[{\"series\":[{\"tags\":{\"device\":\"b\"},\"columns\":[\"time\",\"minimum\"],\"values\":[[100,1]]},{\"tags\":{\"device\":\"a\"},\"columns\":[\"time\",\"minimum\"],\"values\":[[200,2]]}]}]}";
        TSDBQuery query = detail();
        query.setGroupByTags(List.of("device"));
        query.setOrder(SortOrderEnum.ASC);
        query.setLimit(1);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.MIN, "minimum")));
        replies.add(new Reply(200, json));
        assertEquals("a", adapter.query(null, query).getRows().get(0).get("device"));
        query.setOffset(1);
        replies.add(new Reply(200, json));
        assertEquals("b", adapter.query(null, query).getRows().get(0).get("device"));
    }

    @Test
    void actualTimeAliasFieldIsPreservedIncludingNull() {
        replies.add(new Reply(200, response("[\"time\",\"_time\"]", "[[100,12345],[200,null]]")));
        List<Map<String, Object>> rows = adapter.executeQuery("SELECT _time FROM points").getRows();
        assertEquals(12345, rows.get(0).get("_time"));
        assertNull(rows.get(1).get("_time"));
        assertEquals(100L, rows.get(0).get("time"));
    }

    @Test
    void generatedWindowAliasCannotOverwriteAnAggregationOrTag() {
        TSDBQuery query = detail();
        query.setGroupByTime("1h");
        query.setStartTime(0L);
        query.setEndTime(1000L);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "window_start")));
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        query.setGroupByTags(List.of("window_start"));
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void tagOnlyAndUnknownRoleProjectionsFailInsteadOfReturningMisleadingEmptyResults() {
        var template = new com.alandevise.tsgate.core.TGTemplate(adapter);
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class,
                () -> template.query(ProjectedPoint.class).select("device").list()).getErrorCode());
        TSDBQuery unknown = detail();
        unknown.setSelectColumns(List.of("value"));
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class,
                () -> adapter.query(null, unknown)).getErrorCode());
        assertTrue(requests.isEmpty());
        replies.add(new Reply(200, response("[\"time\",\"device\",\"value\"]", "[[1,\"a\",2.5]]")));
        List<ProjectedPoint> rows = template.query(ProjectedPoint.class).select("device", "value").list();
        assertEquals(1, rows.size());
        assertEquals("a", rows.get(0).device);
    }

    @Test
    void timeWindowsRequireAnExplicitEndInsteadOfTheImplicitNowCutoff() {
        TSDBQuery query = detail();
        query.setGroupByTime("1h");
        query.setStartTime(0L);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, assertThrows(TSDBException.class,
                () -> adapter.query(null, query)).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, assertThrows(TSDBException.class,
                () -> adapter.count(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "DROP DATABASE contract", "DELETE FROM points", "CREATE DATABASE forbidden", "KILL QUERY 1",
            "SELECT * INTO copied FROM points", "select * iNtO copied from points",
            "SELECT * FROM points; DROP MEASUREMENT points", "SHOW MEASUREMENTS; SHOW DATABASES", "SELECT 1;;",
            "EXPLAIN DROP DATABASE contract", "EXPLAIN ANALYZE SELECT * INTO copied FROM points",
            "SELECT * FROM points -- comment", "SELECT /* comment */ * FROM points", "SELECT * FROM /points/",
            "SELECT value / 2 FROM points", "SELECT * FROM `points`", "'SELECT' * FROM points",
            "SELECT 'unterminated", "SELECT \"unterminated", "SELECT 'unfinished\\"
    })
    void nativeMutationMultiStatementAndUnsupportedSyntaxFailBeforeHttp(String sql) {
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                assertThrows(TSDBException.class, () -> adapter.executeQuery(sql)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT * FROM points", "  select * from points; \n", "SHOW MEASUREMENTS", "EXPLAIN SELECT value FROM points",
            "EXPLAIN ANALYZE SELECT value FROM points",
            "SELECT \"INTO\" FROM \"p;oints\" WHERE note = 'INTO; DROP DATABASE points' ;",
            "SELECT \"escaped\\\"INTO;name\" FROM points WHERE note = 'O\\'Brien; INTO'",
            "SELECT * FROM points WHERE path = 'C:\\\\path' AND note = '/*comment*/ -- /regex/ `name`'"
    })
    void nativeReadOnlyQueriesPreserveQuotedKeywordsAndSemicolons(String sql) {
        assertEquals(0, adapter.executeQuery(sql).getRowCount());
        assertEquals(1, requests.size());
        assertEquals(sql.trim(), sql());
    }

    @com.alandevise.tsgate.annotation.TGMeasurement("points")
    public static class ProjectedPoint {
        @com.alandevise.tsgate.annotation.TGTime
        public long time;
        @com.alandevise.tsgate.annotation.TGTag
        public String device;
        @com.alandevise.tsgate.annotation.TGField
        public double value;
    }
}
