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

class OpenGeminiAdapterContractTest {
    HttpServer server;
    OpenGeminiAdapter adapter;
    OpenGeminiProperties config;
    OpenGeminiHttpClientProperties http;
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
            if (reply.status() == -1) { exchange.close(); return; }
            byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(reply.status(), reply.status() == 204 ? -1 : bytes.length);
            if (reply.status() != 204) exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        config = new OpenGeminiProperties();
        config.setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        config.setDatabase("contract");
        http = new OpenGeminiHttpClientProperties();
        http.setRetryOnConnectionFailure(false);
        adapter = new OpenGeminiAdapter(config, http, false);
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
        adapter = new OpenGeminiAdapter(config, http, false);
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
        adapter = new OpenGeminiAdapter(config, http, false);
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
        adapter = new OpenGeminiAdapter(config, http, false);
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
        okhttp3.OkHttpClient actualHttp = (okhttp3.OkHttpClient) ReflectionTestUtils.getField(delegate(), "client");
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
        adapter = new OpenGeminiAdapter(config, http, false);
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
        Object client = ReflectionTestUtils.getField(delegate(), "client");
        adapter.init();
        assertSame(nativeClient, adapter.getNativeClient());
        assertSame(client, ReflectionTestUtils.getField(delegate(), "client"));
        adapter.close();
        adapter.close();
        assertNull(ReflectionTestUtils.getField(delegate(), "client"));
        assertNull(ReflectionTestUtils.getField(delegate(), "nativeClient"));
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, assertThrows(TSDBException.class, adapter::init).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, assertThrows(TSDBException.class, () -> adapter.executeQuery("SHOW MEASUREMENTS")).getErrorCode());
    }

    @Test
    void invalidConfigurationFailsBeforeInitialization() {
        http.setReadTimeoutMs(-1);
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR, assertThrows(TSDBException.class, () -> new OpenGeminiAdapter(config, http, false)).getErrorCode());
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

    private Object delegate() {
        return ReflectionTestUtils.getField(adapter, "delegate");
    }

    @Test
    void independentConfigurationDefaultsAndBackendIdentity() {
        assertEquals("openGemini", adapter.getAdapterName());
        assertFalse(new OpenGeminiProperties().isEnable());
        assertTrue(new OpenGeminiProperties().isFailFast());
        assertEquals("http://localhost:8086", new OpenGeminiProperties().getUrl());
        assertEquals("tsdb", new OpenGeminiProperties().getDatabase());
        assertFalse(new OpenGeminiHttpClientProperties().isRetryOnConnectionFailure());
        assertEquals("tsdb.opengemini", OpenGeminiProperties.class
                .getAnnotation(org.springframework.boot.context.properties.ConfigurationProperties.class).prefix());
        assertEquals("tsdb.opengemini.http-client", OpenGeminiHttpClientProperties.class
                .getAnnotation(org.springframework.boot.context.properties.ConfigurationProperties.class).prefix());
        assertTrue(requests.isEmpty(), "Initialization must not probe or create server resources");
    }

    @ParameterizedTest
    @ValueSource(strings = {"null-config", "null-http", "empty-url", "empty-database", "password-without-user",
            "zero-batch", "zero-rows", "maximum-rows", "zero-bytes", "negative-idle", "zero-keepalive"})
    void rejectsInvalidOpenGeminiSettingsBeforeCreatingResources(String invalid) {
        OpenGeminiProperties properties = new OpenGeminiProperties();
        OpenGeminiHttpClientProperties settings = new OpenGeminiHttpClientProperties();
        switch (invalid) {
            case "null-config" -> properties = null;
            case "null-http" -> settings = null;
            case "empty-url" -> properties.setUrl("");
            case "empty-database" -> properties.setDatabase("");
            case "password-without-user" -> properties.setPassword("secret");
            case "zero-batch" -> properties.setMaxBatchRecords(0);
            case "zero-rows" -> properties.setMaxQueryRows(0);
            case "maximum-rows" -> properties.setMaxQueryRows(Integer.MAX_VALUE);
            case "zero-bytes" -> properties.setMaxQueryResponseBytes(0);
            case "negative-idle" -> settings.setMaxIdleConnections(-1);
            case "zero-keepalive" -> settings.setKeepAliveDurationMs(0);
        }
        OpenGeminiProperties argument = properties;
        OpenGeminiHttpClientProperties httpArgument = settings;
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR, assertThrows(TSDBException.class,
                () -> new OpenGeminiAdapter(argument, httpArgument, false)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void explicitDatabaseAndRetentionPolicyReachBothCompatibleEndpoints() {
        adapter.close();
        config.setRetentionPolicy("custom");
        adapter = new OpenGeminiAdapter(config, http, false);
        adapter.init();
        replies.add(new Reply(204, ""));
        assertTrue(adapter.write("other", point(1, 1)));
        adapter.query("other", detail());
        assertTrue(requests.get(0).path().contains("db=other"));
        assertTrue(requests.get(0).path().contains("rp=custom"));
        assertTrue(requests.get(1).body().contains("db=other"));
        assertTrue(requests.get(1).body().contains("rp=custom"));
    }

    @Test
    void physicalBatchesRetainOnlyConfirmedCommitCounts() {
        List<TSDBRecord> records = new ArrayList<>();
        for (int index = 0; index < 5001; index++) records.add(point(index, index));
        replies.add(new Reply(204, ""));
        replies.add(new Reply(401, "unauthorized"));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, records));
        assertEquals(BatchCommitStateEnum.PARTIALLY_COMMITTED, error.getResult().commitState());
        assertEquals(5000, error.getResult().committedRecords());
        assertEquals(1, error.getResult().committedBatches());
        assertEquals(2, error.getResult().totalBatches());
        assertEquals(1, error.getResult().failedBatchIndex());
        assertEquals(2, requests.size());
    }

    @Test
    void connectionFailureDoesNotAutomaticallyReplayAWritingRequest() {
        replies.add(new Reply(-1, ""));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(1, 1))));
        assertEquals(BatchCommitStateEnum.UNKNOWN, error.getResult().commitState());
        assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, error.getErrorCode());
        assertEquals(1, requests.size());
        okhttp3.OkHttpClient actualHttp = (okhttp3.OkHttpClient) ReflectionTestUtils.getField(delegate(), "client");
        assertNotNull(actualHttp);
        assertFalse(actualHttp.retryOnConnectionFailure());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ".", "..", "a,b", "a;b", "a/b", "a\\b", "a\nmeasurement", "a\u0000b", "a\u200Bb", "a\u00A0b"})
    void serverMeasurementRestrictionsRejectTheWholeBatchBeforeIo(String measurement) {
        TSDBRecord invalid = new TSDBRecord(measurement, 2L, Map.of(), Map.of("value", 2));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(1, 1), invalid)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(0, error.getResult().totalBatches());
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"room=west", "a b", "测量😀", "a.b", "a:b"})
    void printableMeasurementNamesAreSentWithoutRenaming(String measurement) {
        replies.add(new Reply(204, ""));
        assertTrue(adapter.write(null, new TSDBRecord(measurement, 1L, Map.of(), Map.of("value", 1))));
        assertTrue(requests.get(0).body().startsWith(measurement.replace(" ", "\\ ") + " "));
    }

    @Test
    void onlyTheExactMissingMeasurementErrorBecomesAnEmptyCommonResult() {
        String missing = "{\"results\":[{\"statement_id\":0,\"error\":\"measurement not found\"}]}";
        replies.add(new Reply(200, missing));
        QueryResult common = adapter.query(null, detail());
        assertTrue(common.isSuccess());
        assertEquals(0, common.getRowCount());
        replies.add(new Reply(200, missing));
        assertEquals(0, adapter.count(null, detail()));
        replies.add(new Reply(200, missing));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points")).getErrorCode());
        for (String message : List.of("measurement not found: points", "database not found", "retention policy not found", "permission denied")) {
            replies.add(new Reply(200, "{\"results\":[{\"error\":\"" + message + "\"}]}"));
            assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                    assertThrows(TSDBException.class, () -> adapter.query(null, detail())).getErrorCode());
        }
        replies.add(new Reply(503, missing));
        assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points"));
        assertEquals(8, requests.size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"query", "count", "native-sql"})
    void missingMeasurementNormalizationNeverHidesUnrelatedErrors(String operation) {
        replies.add(new Reply(200, "{\"results\":[{\"error\":\"database not found\"}]}"));
        TSDBException error = assertThrows(TSDBException.class, () -> {
            switch (operation) {
                case "query" -> adapter.query(null, detail());
                case "count" -> adapter.count(null, detail());
                case "native-sql" -> adapter.executeQuery("SELECT * FROM points");
                default -> throw new AssertionError(operation);
            }
        });
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, error.getErrorCode());
        assertEquals(1, requests.size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"SELECT * FROM points, absent", "SELECT * FROM (SELECT * FROM absent)"})
    void nativeQueriesPreserveMissingMeasurementErrorsAfterParsingSeries(String sql) {
        replies.add(new Reply(200, "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"points\","
                + "\"columns\":[\"time\",\"value\"],\"values\":[[1,1]]}],\"error\":\"measurement not found\"}]}"));
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.executeQuery(sql));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, error.getErrorCode());
        assertEquals("InfluxDB1 query error: measurement not found", error.getMessage());
        assertEquals(1, requests.size());
    }

    @Test
    void aSuccessfulEmptyServerResultIsReturnedWithoutQueryRetries() {
        replies.add(new Reply(200, "{\"results\":[{\"statement_id\":0}]}"));
        replies.add(new Reply(200, response("[\"time\",\"value\"]", "[[1,1]]")));
        assertEquals(0, adapter.query(null, detail()).getRowCount());
        assertEquals(1, requests.size());
        assertEquals(1, replies.size());
        assertEquals(1, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void borrowedCompatibilityClientHasStableIdentityAndStillChecksState() {
        assertSame(adapter.getNativeClient(), adapter.getNativeClient());
        adapter.close();
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, adapter::getNativeClient).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.batchWriteDetailed(null, List.of())).getErrorCode());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeWaitsForLocalBatchSnapshotAndValidation(boolean invalidMeasurement) throws Exception {
        CountDownLatch copying = new CountDownLatch(1);
        CountDownLatch releaseCopy = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        TSDBRecord record = invalidMeasurement
                ? new TSDBRecord("bad\\measurement", 1L, Map.of(), Map.of("value", 1)) : point(1, 1);
        Collection<TSDBRecord> blockedRecords = new AbstractCollection<>() {
            @Override
            public Iterator<TSDBRecord> iterator() { return List.of(record).iterator(); }
            @Override
            public int size() { return 1; }
            @Override
            public Object[] toArray() {
                copying.countDown();
                try {
                    if (!releaseCopy.await(5, TimeUnit.SECONDS)) throw new AssertionError("Batch copy was never released");
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(error);
                }
                return new Object[]{record};
            }
        };
        ExecutorService operations = Executors.newFixedThreadPool(2);
        try {
            if (!invalidMeasurement) replies.add(new Reply(204, ""));
            Future<BatchWriteResult> write = operations.submit(() -> adapter.batchWriteDetailed(null, blockedRecords));
            assertTrue(copying.await(2, TimeUnit.SECONDS), "Write must reach the local snapshot step");
            Future<?> close = operations.submit(() -> { closing.countDown(); adapter.close(); });
            assertTrue(closing.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> close.get(150, TimeUnit.MILLISECONDS),
                    "Close must wait while the local batch snapshot holds an operation open");
            releaseCopy.countDown();
            if (invalidMeasurement) {
                ExecutionException failure = assertThrows(ExecutionException.class, () -> write.get(2, TimeUnit.SECONDS));
                TSDBBatchWriteException validation = assertInstanceOf(TSDBBatchWriteException.class, failure.getCause());
                assertEquals(BatchCommitStateEnum.NOT_COMMITTED, validation.getResult().commitState());
                assertTrue(requests.isEmpty());
            } else {
                assertTrue(write.get(2, TimeUnit.SECONDS).isSuccess());
                assertEquals(1, requests.size());
            }
            close.get(2, TimeUnit.SECONDS);
            assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                    assertThrows(TSDBException.class, adapter::getNativeClient).getErrorCode());
            assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                    assertThrows(TSDBException.class, () -> adapter.batchWriteDetailed(null, List.of(record))).getErrorCode());
        } finally {
            releaseCopy.countDown();
            operations.shutdownNow();
            assertTrue(operations.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"9007199254740993", "-9007199254740993", "9223372036854775807",
            "-9223372036854775807", "9223372036854775808", "-9223372036854775809"})
    void unsafeIntegerFieldsRejectTheEntireBatchBeforeIo(String literal) {
        java.math.BigInteger integer = new java.math.BigInteger(literal);
        List<Number> numbers = new ArrayList<>();
        numbers.add(integer);
        if (integer.bitLength() <= 63) numbers.add(integer.longValueExact());
        for (Number number : numbers) {
            TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                    () -> adapter.batchWriteDetailed(null, List.of(point(1, 1), point(2, number))));
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
            assertEquals(0, failure.getResult().committedRecords());
            assertEquals(0, failure.getResult().totalBatches());
            assertTrue(requests.isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"9007199254740992", "9007199254740994", "-9007199254740994", "-9223372036854775808"})
    void exactIntegerFieldsRetainEveryBitInTheProtocol(String literal) {
        java.math.BigInteger integer = new java.math.BigInteger(literal);
        for (Number number : List.of(integer, integer.longValueExact())) {
            replies.add(new Reply(204, ""));
            assertTrue(adapter.write(null, point(1, number)));
            assertTrue(requests.get(requests.size() - 1).body().contains("value=" + literal + "i "));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"EQ", "IN", "BETWEEN"})
    void commonQueryAndCountRejectUnsafeIntegerPredicatesBeforeIo(String operatorName) {
        OperatorEnum operator = OperatorEnum.valueOf(operatorName);
        List<Number> invalidNumbers = List.of(9007199254740993L, -9007199254740993L, Long.MAX_VALUE, Long.MIN_VALUE + 1,
                new java.math.BigInteger("9223372036854775808"), new java.math.BigInteger("-9223372036854775809"),
                new java.math.BigDecimal("9007199254740993.0"), new java.util.concurrent.atomic.AtomicLong(9007199254740993L),
                new java.math.BigDecimal("1E+2147483647"), new java.math.BigDecimal("-1E+2147483647"));
        for (Number number : invalidNumbers) {
            TSDBQuery query = detail();
            List<Object> values = operator == OperatorEnum.EQ ? List.of(number) : List.of(1, number);
            query.getFilters().add(new QueryFilter("value", operator, values));
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    assertThrows(TSDBException.class, () -> adapter.count(null, query)).getErrorCode());
            assertTrue(requests.isEmpty());
        }
    }

    @Test
    void safeIntegerFractionalAndFloatingPredicatesKeepTheirExistingSemantics() {
        for (Object value : List.of(9007199254740994L, Long.MIN_VALUE, new java.math.BigInteger("9007199254740994"),
                new java.math.BigDecimal("9007199254740994.0"), new java.math.BigDecimal("0.1"),
                new java.math.BigDecimal("1E-2147483647"), new java.math.BigDecimal("0E+2147483647"),
                (double) 9007199254740993L, (float) 9007199254740993L, "9007199254740993")) {
            TSDBQuery query = detail();
            query.getFilters().add(new QueryFilter("value", OperatorEnum.EQ, List.of(value)));
            assertTrue(adapter.query(null, query).isSuccess());
            assertEquals(0, adapter.count(null, query));
        }
        assertEquals(20, requests.size());
    }

    @Test
    void nativeSqlRetainsBackendNumericLiteralSemantics() {
        assertTrue(adapter.executeQuery("SELECT * FROM points WHERE value = 9007199254740993").isSuccess());
        assertTrue(sql().contains("9007199254740993"));
        assertEquals(1, requests.size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"new", "closed"})
    void commonNumericPreflightRetainsStateErrorsBeforeInspectingFilters(String state) {
        adapter.close();
        adapter = new OpenGeminiAdapter(config, http, false);
        if (state.equals("closed")) {
            adapter.init();
            adapter.close();
        }
        TSDBQuery query = detail();
        query.getFilters().add(new QueryFilter("value", OperatorEnum.EQ, List.of(9007199254740993L)));
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.count(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }
}
