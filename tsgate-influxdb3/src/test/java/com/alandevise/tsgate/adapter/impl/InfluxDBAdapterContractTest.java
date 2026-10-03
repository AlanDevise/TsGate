package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.InfluxDBProperties;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.InetSocketAddress;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/** Public API contract tests against an in-process HTTP peer; no database or token is required. */
class InfluxDBAdapterContractTest {
    private HttpServer server;
    private InfluxDBAdapter adapter;
    private InfluxDBProperties properties;
    private final List<CapturedRequest> requests = Collections.synchronizedList(new ArrayList<>());
    private final Queue<Reply> replies = new ConcurrentLinkedQueue<>();
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(new CapturedRequest(exchange.getRequestURI().toString(),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
                    exchange.getRequestHeaders().getFirst("Authorization")));
            Reply reply = replies.poll();
            if (reply == null) reply = new Reply(200, "[]");
            byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(reply.status(), reply.status() == 204 ? -1 : bytes.length);
            if (reply.status() != 204) exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        properties = new InfluxDBProperties();
        properties.setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        properties.setDatabase("contract");
        properties.getHttpClient().setRetryOnConnectionFailure(false);
        adapter = new InfluxDBAdapter(properties, false);
        adapter.init();
    }

    @AfterEach
    void cleanup() {
        adapter.close();
        server.stop(0);
    }

    @ParameterizedTest
    @CsvSource({"1ms,1 milliseconds", "5s,5 seconds", "10m,10 minutes", "2h,2 hours", "1d,1 days", "300000,300000 milliseconds", "5 M,5 minutes"})
    void acceptsOnlyUnambiguousPositiveWindows(String window, String sqlInterval) throws Exception {
        adapter.query(null, aggregate(window));
        assertTrue(sql().contains("date_bin(interval '" + sqlInterval + "'"), sql());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "0ms", "-1h", "1.5h", "1h30m", "+2s", "1w", "1e3ms", "9223372036854775808ms", "999999999999999999999d", "1 second',time)--", "1m);SELECT 1--", "5m/*comment*/"})
    void rejectsInvalidOrInjectedWindowsBeforeHttp(String window) {
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.query(null, aggregate(window)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void escapesIdentifiersAndStringFiltersWithoutChangingValues() throws Exception {
        TSDBQuery query = detail();
        query.setMeasurement("meter\"archive");
        query.getFilters().add(new QueryFilter("device", OperatorEnum.EQ, List.of("O'Brien; --")));
        adapter.query("another", query);
        assertTrue(sql().contains("FROM \"meter\"\"archive\""));
        assertTrue(sql().contains("\"device\" = 'O''Brien; --'"));
        assertEquals("another", mapper.readTree(requests.get(0).body()).get("db").asText());
    }

    @Test
    void countIgnoresPagingAndCursorWhileRetainingFilters() throws Exception {
        TSDBQuery query = detail();
        query.setCursorTime(999L);
        query.setLimit(0);
        query.setOffset(-1);
        query.getFilters().add(new QueryFilter("device", OperatorEnum.EQ, List.of("a")));
        replies.add(new Reply(200, "[{\"total\":42}]"));
        assertEquals(42, adapter.count(null, query));
        assertTrue(sql().contains("COUNT(*)"));
        assertTrue(sql().contains("\"device\" = 'a'"));
        assertFalse(sql().contains("LIMIT"));
        assertFalse(sql().contains("OFFSET"));
        assertFalse(sql().contains("ORDER BY"));
        assertFalse(sql().contains("1970-01-01"));
    }

    @Test
    void normalizesUtcTimestampsWithoutJvmTimezoneDependence() {
        replies.add(new Reply(200, "[{\"time\":\"2026-01-01T00:00:00\",\"value\":2}]"));
        QueryResult result = adapter.executeQuery("SELECT * FROM points");
        assertEquals(1767225600000L, result.getRows().get(0).get("time"));
        assertEquals(1767225600000L, result.getRows().get(0).get("_time"));
        assertTrue(result.isSuccess());
    }

    @ParameterizedTest
    @CsvSource({"400,QUERY_ERROR", "401,PERMISSION_ERROR", "403,PERMISSION_ERROR", "404,RESOURCE_NOT_FOUND", "405,UNSUPPORTED_OPERATION", "408,CONNECTION_ERROR", "429,CONNECTION_ERROR", "500,CONNECTION_ERROR", "501,UNSUPPORTED_OPERATION"})
    void mapsHttpQueryErrorsToStableCodes(int status, TSDBErrorCodeEnum expected) {
        replies.add(new Reply(status, "rejected"));
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT 1"));
        assertEquals(expected, error.getErrorCode());
    }

    @Test
    void malformedResponseIsQueryError() {
        replies.add(new Reply(200, "not-json"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT 1")).getErrorCode());
    }

    @Test
    void lineProtocolEscapesValuesAndUsesMillisecondsAndAtomicRequestValidation() {
        adapter.close();
        properties.setToken("local-test-token");
        adapter = new InfluxDBAdapter(properties, false);
        adapter.init();
        replies.add(new Reply(204, ""));
        TSDBRecord point = new TSDBRecord("room temp", 1000L, Map.of("device", "A,B"),
                Map.of("note", "say \"yes\"", "count", 7));
        BatchWriteResult result = adapter.batchWriteDetailed(null, List.of(point));
        assertTrue(result.isSuccess());
        CapturedRequest request = requests.get(0);
        assertTrue(request.path().contains("precision=millisecond"));
        assertTrue(request.path().contains("accept_partial=false"));
        assertTrue(request.body().contains("room\\ temp,device=A\\,B "));
        assertTrue(request.body().contains("count=7i"));
        assertTrue(request.body().contains("note=\"say \\\"yes\\\"\""));
        assertTrue(request.body().endsWith(" 1000"));
        assertNotNull(request.authorization());
    }

    @Test
    void lateValidationFailureSendsNothing() {
        TSDBRecord invalid = new TSDBRecord("bad\nmeasurement", 1L, Map.of(), Map.of("value", 1));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(0), invalid)));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        assertEquals(0, error.getResult().committedRecords());
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @CsvSource({"400,NOT_COMMITTED,false", "401,NOT_COMMITTED,false", "429,NOT_COMMITTED,true", "408,UNKNOWN,true", "503,UNKNOWN,true", "501,UNKNOWN,false"})
    void distinguishesDefiniteRejectionFromUnknownCommit(int status, BatchCommitStateEnum expected, boolean retryable) {
        replies.add(new Reply(status, "rejected"));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(0))));
        assertEquals(expected, error.getResult().commitState());
        assertEquals(retryable, error.getResult().retryable());
        assertEquals(0, error.getResult().committedRecords());
    }

    @ParameterizedTest
    @CsvSource({"400,PARTIALLY_COMMITTED", "503,UNKNOWN"})
    void preservesCommittedLowerBoundWhenLaterRequestFails(int status, BatchCommitStateEnum expected) {
        replies.add(new Reply(204, ""));
        replies.add(new Reply(status, "second batch failed"));
        List<TSDBRecord> points = IntStream.range(0, 5001).mapToObj(this::point).toList();
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, points));
        assertEquals(expected, error.getResult().commitState());
        assertEquals(5000, error.getResult().committedRecords());
        assertEquals(1, error.getResult().committedBatches());
        assertEquals(1, error.getResult().failedBatchIndex());
        assertEquals(2, requests.size());
    }

    @Test
    void invalidBetweenFilterAndNonNativeTimeColumnFailLocally() {
        TSDBQuery query = detail();
        query.getFilters().add(new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        TSDBQuery other = detail();
        other.setTimeColumn("event_time");
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                assertThrows(TSDBException.class, () -> adapter.query(null, other)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void configurationStringNeverContainsToken() {
        properties.setToken("must-not-appear-in-logs");
        assertFalse(properties.toString().contains("must-not-appear-in-logs"));
    }

    @Test
    void repeatedInitKeepsExistingResourcesAndCloseIsTerminal() {
        Object http = ReflectionTestUtils.getField(adapter, "client");
        Object nativeClient = adapter.getNativeClient();
        adapter.init();
        assertSame(http, ReflectionTestUtils.getField(adapter, "client"));
        assertSame(nativeClient, adapter.getNativeClient());
        adapter.close();
        adapter.close();
        assertNull(ReflectionTestUtils.getField(adapter, "client"));
        assertNull(ReflectionTestUtils.getField(adapter, "nativeClient"));
        assertTrue(((OkHttpClient) http).dispatcher().executorService().isShutdown());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT 1")).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.batchWriteDetailed(null, List.of(point(1)))).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, adapter::init).getErrorCode());
        assertThrows(TSDBException.class, adapter::getNativeClient);
        assertTrue(requests.isEmpty());
    }

    @Test
    void failedNativeInitializationReleasesReferencesAndCanRetry() {
        adapter.close();
        adapter = new InfluxDBAdapter(properties, false);
        try (var nativeFactory = org.mockito.Mockito.mockStatic(com.influxdb.v3.client.InfluxDBClient.class)) {
            nativeFactory.when(() -> com.influxdb.v3.client.InfluxDBClient.getInstance(
                    org.mockito.ArgumentMatchers.any(com.influxdb.v3.client.config.ClientConfig.class)))
                    .thenThrow(new IllegalStateException("native factory failure"));
            assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    assertThrows(TSDBException.class, adapter::init).getErrorCode());
        }
        assertNull(ReflectionTestUtils.getField(adapter, "client"));
        assertNull(ReflectionTestUtils.getField(adapter, "nativeClient"));
        assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT 1"));
        properties.setUrl("http://127.0.0.1:1");
        properties.setDatabase("changed_after_failure");
        properties.getHttpClient().setReadTimeoutMs(-1);
        adapter.init();
        assertNotNull(adapter.getNativeClient());
        assertTrue(adapter.executeQuery("SELECT 1").isSuccess());
        assertTrue(requests.get(0).body().contains("\"db\":\"contract\""));
    }

    @Test
    void constructionSnapshotKeepsAdapterAndNativeWritesOnTheSameConnection() {
        adapter.close();
        properties.setToken("original-token");
        properties.setMaxBatchRecords(1);
        properties.setMaxQueryRows(1);
        properties.setMaxQueryResponseBytes(1024);
        InfluxDBProperties.HttpClientConfig originalHttp = properties.getHttpClient();
        originalHttp.setConnectTimeoutMs(1234);
        originalHttp.setReadTimeoutMs(2345);
        originalHttp.setWriteTimeoutMs(3456);
        originalHttp.setCallTimeoutMs(4567);
        adapter = new InfluxDBAdapter(properties, false);
        properties.setUrl("http://127.0.0.1:1");
        properties.setToken("changed-token");
        properties.setDatabase("changed_database");
        properties.setMaxBatchRecords(100);
        properties.setMaxQueryRows(100);
        properties.setMaxQueryResponseBytes(1);
        originalHttp.setConnectTimeoutMs(-1);
        originalHttp.setReadTimeoutMs(-1);
        originalHttp.setWriteTimeoutMs(-1);
        originalHttp.setCallTimeoutMs(-1);
        properties.setHttpClient(null);
        adapter.init();
        OkHttpClient http = (OkHttpClient) ReflectionTestUtils.getField(adapter, "client");
        assertNotNull(http);
        assertEquals(1234, http.connectTimeoutMillis());
        assertEquals(2345, http.readTimeoutMillis());
        assertEquals(3456, http.writeTimeoutMillis());
        assertEquals(4567, http.callTimeoutMillis());
        replies.add(new Reply(204, ""));
        assertTrue(adapter.write(null, point(1)));
        replies.add(new Reply(204, ""));
        adapter.getNativeClient().writeRecord("points value=2i 2");
        assertEquals(2, requests.size());
        assertTrue(requests.get(0).path().contains("db=contract"));
        assertEquals("Bearer original-token", requests.get(0).authorization());
        assertTrue(requests.get(1).path().contains("bucket=contract"));
        assertEquals("Token original-token", requests.get(1).authorization());
        replies.add(new Reply(200, "[{\"value\":1}]"));
        assertEquals(1, adapter.query(null, detail()).getRowCount());
        replies.add(new Reply(200, "[{\"value\":1},{\"value\":2}]"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, detail())).getErrorCode());
        assertEquals(1, adapter.getMaxBatchRecords());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(3), point(4)))).getResult().commitState());
    }

    @Test
    void nullHttpSettingsAreNotReplacedByDefaultsOrLaterPropertyChanges() {
        adapter.close();
        properties.setHttpClient(null);
        adapter = new InfluxDBAdapter(properties, false);
        properties.setHttpClient(new InfluxDBProperties.HttpClientConfig());
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                assertThrows(TSDBException.class, adapter::init).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                assertThrows(TSDBException.class, adapter::init).getErrorCode());
        assertThrows(TSDBException.class, () -> new InfluxDBAdapter(null));
        assertTrue(requests.isEmpty());
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
    void closeWaitsForActiveQueryThenRejectsFutureRequests() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.removeContext("/");
        server.createContext("/", exchange -> {
            entered.countDown();
            try {
                assertTrue(release.await(10, TimeUnit.SECONDS));
                byte[] bytes = "[{\"value\":1}]".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        var executor = Executors.newFixedThreadPool(2);
        try {
            var query = executor.submit(() -> adapter.executeQuery("SELECT value FROM points"));
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            CountDownLatch closing = new CountDownLatch(1);
            var close = executor.submit(() -> { closing.countDown(); adapter.close(); });
            assertTrue(closing.await(10, TimeUnit.SECONDS));
            assertThrows(java.util.concurrent.TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            release.countDown();
            assertEquals(1, query.get(10, TimeUnit.SECONDS).getRowCount());
            close.get(10, TimeUnit.SECONDS);
            assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT 1"));
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void nativeRowsAreCappedAndOnlyExplicitPaginationAllowsOneLookahead() {
        resetLimits(2, 1024);
        replies.add(new Reply(200, "[{\"v\":1},{\"v\":2}]"));
        assertEquals(2, adapter.executeQuery("SELECT * FROM points").getRowCount());
        replies.add(new Reply(200, "[{\"v\":1},{\"v\":2},{\"v\":3}]"));
        TSDBException nativeError = assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, nativeError.getErrorCode());
        assertTrue(nativeError.getMessage().contains("maximum rows: 2"));
        replies.add(new Reply(200, "[{\"v\":1},{\"v\":2},{\"v\":3}]"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, detail())).getErrorCode());
        TSDBQuery probe = detail();
        probe.setPaginationProbe(true);
        replies.add(new Reply(200, "[{\"v\":1},{\"v\":2},{\"v\":3}]"));
        assertEquals(3, adapter.query(null, probe).getRowCount());
        replies.add(new Reply(200, "[{\"v\":1},{\"v\":2},{\"v\":3},{\"v\":4}]"));
        assertThrows(TSDBException.class, () -> adapter.query(null, probe));
        replies.add(new Reply(200, "[{\"v\":1}]"));
        assertEquals(1, adapter.executeQuery("SELECT * FROM points LIMIT 1").getRowCount());
    }

    @Test
    void ordinaryTemplateListUsesExactConfiguredRowLimit() {
        resetLimits(2, 1024);
        var template = new com.alandevise.tsgate.core.TGTemplate(adapter);
        replies.add(new Reply(200, "[{\"time\":1,\"value\":1},{\"time\":2,\"value\":2}]"));
        assertEquals(2, template.query(LimitedPoint.class).limit(2).list().size());
        replies.add(new Reply(200, "[{\"time\":1,\"value\":1},{\"time\":2,\"value\":2},{\"time\":3,\"value\":3}]"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> template.query(LimitedPoint.class).limit(3).list()).getErrorCode());
    }

    @com.alandevise.tsgate.annotation.TGMeasurement("points")
    public static class LimitedPoint {
        @com.alandevise.tsgate.annotation.TGTime public long time;
        @com.alandevise.tsgate.annotation.TGField public long value;
    }

    @Test
    void knownLengthResponseHonorsExactByteBudget() {
        String json = "[{\"v\":\"é\"}]";
        resetLimits(10, json.getBytes(StandardCharsets.UTF_8).length);
        replies.add(new Reply(200, json));
        assertEquals(1, adapter.executeQuery("SELECT v FROM points").getRowCount());
        replies.add(new Reply(200, json + " "));
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT v FROM points"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, error.getErrorCode());
        assertTrue(error.getMessage().contains("response bytes"));
    }

    @Test
    void chunkedResponsesEnforceByteBudgetDuringParsing() {
        resetLimits(10, 64);
        server.removeContext("/");
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            try {
                exchange.getResponseBody().write(("[{\"v\":\"" + "x".repeat(1000) + "\"}]").getBytes(StandardCharsets.UTF_8));
            } finally {
                exchange.close();
            }
        });
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT v FROM points")).getErrorCode());
    }

    @Test
    void compressedResponseBudgetAppliesToDecompressedBytes() throws Exception {
        resetLimits(10, 128);
        var compressed = new java.io.ByteArrayOutputStream();
        try (var gzip = new java.util.zip.GZIPOutputStream(compressed)) {
            gzip.write(("[{\"v\":\"" + "x".repeat(10000) + "\"}]").getBytes(StandardCharsets.UTF_8));
        }
        assertTrue(compressed.size() < 128);
        server.removeContext("/");
        server.createContext("/", exchange -> {
            exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            exchange.sendResponseHeaders(200, compressed.size());
            try {
                exchange.getResponseBody().write(compressed.toByteArray());
            } finally {
                exchange.close();
            }
        });
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT v FROM points")).getErrorCode());
    }

    @Test
    void concurrentInitializationPublishesOneClient() throws Exception {
        adapter.close();
        adapter = new InfluxDBAdapter(properties, false);
        var executor = Executors.newFixedThreadPool(4);
        try {
            List<java.util.concurrent.Callable<Object>> calls = Collections.nCopies(8, () -> {
                adapter.init();
                return adapter.getNativeClient();
            });
            var results = executor.invokeAll(calls);
            Object client = results.get(0).get(10, TimeUnit.SECONDS);
            for (var result : results) {
                assertSame(client, result.get(10, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "null", "[null]", "[1]", "[{}", "[{}] []", "[{},]"})
    void rejectsInvalidJsonShapeAndTrailingContent(String json) {
        replies.add(new Reply(200, json));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT 1")).getErrorCode());
    }

    @Test
    void invalidResourceLimitsFailAtConstruction() {
        properties.setMaxQueryRows(0);
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                assertThrows(TSDBException.class, () -> new InfluxDBAdapter(properties)).getErrorCode());
        properties.setMaxQueryRows(Integer.MAX_VALUE);
        assertThrows(TSDBException.class, () -> new InfluxDBAdapter(properties));
        properties.setMaxQueryRows(10000);
        properties.setMaxQueryResponseBytes(0);
        assertThrows(TSDBException.class, () -> new InfluxDBAdapter(properties));
    }

    @ParameterizedTest
    @CsvSource({
            "2026-03-08T05:00:00Z,2026-03-09T04:00:00Z",
            "2026-11-01T04:00:00Z,2026-11-02T05:00:00Z"
    })
    void newYorkDaysUseRealDstMidnights(String start, String end) throws Exception {
        TSDBQuery query = aggregate("1d");
        query.setTimeZone("America/New_York");
        query.setStartTime(java.time.Instant.parse(start).toEpochMilli());
        query.setEndTime(java.time.Instant.parse(end).toEpochMilli() - 1);
        adapter.query(null, query);
        assertTrue(sql().contains("CASE WHEN \"time\" >= timestamp '" + start + "'"), sql());
        assertTrue(sql().contains("AND \"time\" < timestamp '" + end + "' THEN timestamp '" + start + "'"), sql());
        assertFalse(sql().contains("date_bin"), sql());
    }

    @Test
    void summerSubDayUsesSummerOffsetAndCrossTransitionFails() throws Exception {
        TSDBQuery query = aggregate("1h");
        query.setTimeZone("America/New_York");
        query.setStartTime(java.time.Instant.parse("2026-07-01T00:00:00Z").toEpochMilli());
        query.setEndTime(java.time.Instant.parse("2026-07-02T00:00:00Z").toEpochMilli());
        adapter.query(null, query);
        assertTrue(sql().contains("1970-01-01T00:00:00-04:00"), sql());
        query.setStartTime(java.time.Instant.parse("2026-03-08T05:00:00Z").toEpochMilli());
        query.setEndTime(java.time.Instant.parse("2026-03-09T04:00:00Z").toEpochMilli());
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(1, requests.size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"9223372036854775808", "1.5", "\"invalid\""})
    void invalidCountNeverTruncatesOrReturnsZero(String count) {
        replies.add(new Reply(200, "[{\"total\":" + count + "}]"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.count(null, detail())).getErrorCode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"9223372036854775808", "1.5", "\"invalid\"", "\"\"", "\" \"", "true", "\"+999999999-01-01T00:00:00Z\""})
    void invalidTimeNeverTruncatesOrSilentlySurvivesNormalization(String value) {
        replies.add(new Reply(200, "[{\"time\":" + value + "}]"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT time FROM points")).getErrorCode());
    }

    @Test
    void timeNormalizationPreservesAnActualUnderscoreTimeColumn() {
        for (Object value : java.util.Arrays.asList(99, null)) {
            Map<String, Object> row = new java.util.LinkedHashMap<>();
            row.put("time", 1234L);
            row.put("_time", value);
            org.springframework.test.util.ReflectionTestUtils.invokeMethod(InfluxDBAdapter.class, "normalizeRows", List.of(row));
            assertEquals(1234L, row.get("time"));
            assertTrue(row.containsKey("_time"));
            assertEquals(value, row.get("_time"));
        }
    }

    @Test
    void nullTimeRemainsNullWithoutInventingAnEpoch() {
        replies.add(new Reply(200, "[{\"time\":null,\"value\":1}]"));
        Map<String, Object> row = adapter.executeQuery("SELECT time, value FROM points").getRows().get(0);
        assertNull(row.get("time"));
        assertFalse(row.containsKey("_time"));
    }

    @Test
    void jsonDecimalIntegersRetainTheirOriginalPrecision() {
        replies.add(new Reply(200, "[{\"time\":9007199254740993.0}]"));
        assertEquals(9007199254740993L, adapter.executeQuery("SELECT time FROM points").getRows().get(0).get("time"));
        replies.add(new Reply(200, "[{\"total\":9007199254740993.0}]"));
        assertEquals(9007199254740993L, adapter.count(null, detail()));
    }

    @Test
    void binaryFloatingPointIntegersUseActualValueInsteadOfRoundedDisplayText() {
        double binaryInteger = Math.nextUp(1e18d);
        assertEquals(1000000000000000128L, (Long) ReflectionTestUtils.invokeMethod(
                InfluxDBAdapter.class, "parseTimeMillis", binaryInteger));
        assertEquals(1000000000000000128L, (Long) ReflectionTestUtils.invokeMethod(
                InfluxDBAdapter.class, "parseCursorTimeMillis", binaryInteger));
        float floatInteger = 1_000_000_000_000f;
        assertEquals(999999995904L, (Long) ReflectionTestUtils.invokeMethod(
                InfluxDBAdapter.class, "parseTimeMillis", floatInteger));
    }

    @Test
    void commentMeasurementRejectsTheWholeBatchEvenAfterACompletePreparedHttpPayload() {
        List<TSDBRecord> points = new ArrayList<>(IntStream.range(0, 5001).mapToObj(this::point).toList());
        points.add(new TSDBRecord("#ignored", 6000L, Map.of(), Map.of("value", 1)));
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, points));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(0, failure.getResult().committedRecords());
        assertEquals(0, failure.getResult().committedBatches());
        assertTrue(requests.isEmpty());
    }

    @Test
    void measurementEncodingKeepsEqualsLiteralAndPreservesInflux3BackslashEscaping() {
        replies.add(new Reply(204, ""));
        TSDBRecord record = new TSDBRecord("path\\name=room temp,west", 1000L,
                Map.of("tag=key", "tag=value"), Map.of("field=key", 1));
        assertTrue(adapter.write(null, record));
        String payload = requests.get(0).body();
        assertTrue(payload.startsWith("path\\\\name=room\\ temp\\,west,"), payload);
        assertTrue(payload.contains("tag\\=key=tag\\=value"), payload);
        assertTrue(payload.contains("field\\=key=1i"), payload);
    }

    @ParameterizedTest
    @ValueSource(strings = {"9007199254740993", "9223372036854775807", "-9223372036854775808"})
    void bigIntegersUseExactSignedIntegerLineProtocol(String text) {
        replies.add(new Reply(204, ""));
        assertTrue(adapter.write(null, new TSDBRecord("precise", 1L, Map.of(),
                Map.of("value", new BigInteger(text)))));
        assertEquals("precise value=" + text + "i 1", requests.get(0).body());
    }

    @ParameterizedTest
    @ValueSource(strings = {"9223372036854775808", "-9223372036854775809"})
    void outOfRangeBigIntegerRejectsMixedBatchBeforeHttp(String text) {
        assertInvalidNumericFieldIsPreflightFailure(new BigInteger(text), TSDBErrorCodeEnum.ARGUMENT_ERROR);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1E-400", "-1E-400", "1E400", "-1E400"})
    void decimalUnderflowAndOverflowRejectMixedBatchBeforeHttp(String text) {
        assertInvalidNumericFieldIsPreflightFailure(new BigDecimal(text), TSDBErrorCodeEnum.ARGUMENT_ERROR);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "0E-400", "1E-320", "0.12345678901234567890123456789"})
    void supportedDecimalsAreExplicitlyEncodedAtDoublePrecision(String text) {
        replies.add(new Reply(204, ""));
        BigDecimal decimal = new BigDecimal(text);
        assertTrue(adapter.write(null, new TSDBRecord("decimal", 1L, Map.of(), Map.of("value", decimal))));
        assertEquals("decimal value=" + Double.toString(decimal.doubleValue()) + " 1", requests.get(0).body());
    }

    @Test
    void nonFiniteAndUnspecifiedNumericTypesCannotReachLineProtocol() {
        for (Number invalid : List.<Number>of(Double.NaN, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY)) {
            assertInvalidNumericFieldIsPreflightFailure(invalid, TSDBErrorCodeEnum.ARGUMENT_ERROR);
        }
        assertInvalidNumericFieldIsPreflightFailure(new java.util.concurrent.atomic.AtomicLong(9007199254740993L),
                TSDBErrorCodeEnum.METADATA_ERROR);
    }

    private void assertInvalidNumericFieldIsPreflightFailure(Number value, TSDBErrorCodeEnum expected) {
        TSDBRecord invalid = new TSDBRecord("invalid_numeric", 1L, Map.of(), Map.of("value", value));
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(0), invalid)));
        assertEquals(expected, failure.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(0, failure.getResult().committedRecords());
        assertTrue(requests.isEmpty());
    }

    private void resetLimits(int rows, long bytes) {
        adapter.close();
        properties.setMaxQueryRows(rows);
        properties.setMaxQueryResponseBytes(bytes);
        adapter = new InfluxDBAdapter(properties, false);
        adapter.init();
    }

    private String sql() throws Exception {
        return mapper.readTree(requests.get(requests.size() - 1).body()).get("q").asText();
    }

    private TSDBQuery detail() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        query.setLimit(10);
        return query;
    }

    private TSDBQuery aggregate(String window) {
        TSDBQuery query = detail();
        query.setGroupByTime(window);
        query.getAggregations().add(new AggregationSpec("value", AggregationFunctionEnum.AVG, "mean"));
        return query;
    }

    private TSDBRecord point(int index) {
        return new TSDBRecord("points", (long) index, Map.of("device", "a"), Map.of("value", index));
    }

    private record Reply(int status, String body) {}
    private record CapturedRequest(String path, String body, String authorization) {}
}
