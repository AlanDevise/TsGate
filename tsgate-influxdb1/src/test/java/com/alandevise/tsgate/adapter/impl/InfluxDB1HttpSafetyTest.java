package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.*;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.OperatorEnum;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.TSDBRecord;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises transport, strict response parsing, and literal encoding through a local HTTP peer. */
class InfluxDB1HttpSafetyTest {
    private HttpServer server;
    private InfluxDB1Adapter adapter;
    private InfluxDB1Properties properties;
    private InfluxDB1HttpClientProperties http;
    private final List<CapturedRequest> requests = Collections.synchronizedList(new ArrayList<>());
    private final Queue<Reply> replies = new ConcurrentLinkedQueue<>();

    private record CapturedRequest(String method, String path, byte[] body) { }
    private record Reply(int status, String body, String location, String retryAfter) {
        private Reply(int status, String body, String location) {
            this(status, body, location, null);
        }
    }

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] requestBody = exchange.getRequestBody().readAllBytes();
            requests.add(new CapturedRequest(exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(), requestBody));
            Reply reply = "/redirected".equals(exchange.getRequestURI().getPath())
                    ? new Reply(200, "<html>Not a write acknowledgement</html>", null) : replies.poll();
            if (reply == null) reply = new Reply(204, "", null);
            if (reply.status() == -1) {
                exchange.close();
                return;
            }
            if (reply.retryAfter() != null) exchange.getResponseHeaders().set("Retry-After", reply.retryAfter());
            if (reply.location() != null) exchange.getResponseHeaders().set("Location", reply.location());
            byte[] body = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(reply.status(), reply.status() == 204 || body.length == 0 ? -1 : body.length);
            if (reply.status() != 204 && body.length > 0) exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        properties = new InfluxDB1Properties();
        properties.setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        http = new InfluxDB1HttpClientProperties();
        adapter = newAdapter();
        adapter.init();
    }

    @AfterEach
    void cleanup() {
        if (adapter != null) adapter.close();
        if (server != null) server.stop(0);
    }

    private InfluxDB1Adapter newAdapter() {
        return new InfluxDB1Adapter(properties, http, false);
    }

    private void setBudget(long bytes) {
        adapter.close();
        properties.setMaxBatchBytes(bytes);
        adapter = newAdapter();
        adapter.init();
    }

    private void setReadRecovery(boolean enabled) {
        adapter.close();
        http.setRetryOnConnectionFailure(enabled);
        adapter = newAdapter();
        adapter.init();
    }

    private static TSDBRecord point(long time, Object value) {
        return new TSDBRecord("points", time, Map.of(), Map.of("v", value));
    }

    private static long utf8Bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8).length;
    }

    private void assertUncommittedBeforeIo(List<TSDBRecord> records) {
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, records));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        assertEquals(records.size(), error.getResult().requestedRecords());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(0, error.getResult().totalBatches());
        assertEquals(0, error.getResult().committedBatches());
        assertTrue(requests.isEmpty(), "The entire batch must fail before the first HTTP write");
    }

    @ParameterizedTest
    @ValueSource(ints = {408, 503, -1})
    void writeFailureNeverReplaysWithDefaultRecoverySettings(int status) {
        replies.add(new Reply(status, "write failed", null, status == 503 ? "0" : null));
        replies.add(new Reply(204, "", null));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(1, 1))));
        assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, error.getErrorCode());
        assertEquals(BatchCommitStateEnum.UNKNOWN, error.getResult().commitState());
        assertTrue(error.getResult().retryable());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(0, error.getResult().committedBatches());
        assertEquals(1, error.getResult().totalBatches());
        assertEquals(1, requests.size());
        assertEquals(1, replies.size(), "The write must not consume the response reserved for a replay");
    }

    @ParameterizedTest
    @ValueSource(ints = {408, 503, -1})
    void laterWriteFailurePreservesConfirmedPrefixAndStopsRemainingBatches(int status) {
        properties.setMaxBatchRecords(10_001);
        setReadRecovery(true);
        replies.add(new Reply(204, "", null));
        replies.add(new Reply(status, "write failed", null, status == 503 ? "0" : null));
        replies.add(new Reply(204, "", null));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, Collections.nCopies(10_001, point(1, 1))));
        assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, error.getErrorCode());
        assertEquals(BatchCommitStateEnum.UNKNOWN, error.getResult().commitState());
        assertTrue(error.getResult().retryable());
        assertEquals(5_000, error.getResult().committedRecords());
        assertEquals(1, error.getResult().committedBatches());
        assertEquals(3, error.getResult().totalBatches());
        assertEquals(1, error.getResult().failedBatchIndex());
        assertEquals(2, requests.size());
        assertEquals(1, replies.size(), "No replay or remaining batch may be sent after the failure");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void queriesKeepConfiguredConnectionRecovery(boolean enabled) {
        setReadRecovery(enabled);
        replies.add(new Reply(408, "query timed out", null));
        replies.add(new Reply(200, "{\"results\":[{}]}", null));
        if (enabled) {
            assertEquals(0, adapter.executeQuery("SELECT * FROM points").getRowCount());
        } else {
            TSDBException error = assertThrows(TSDBException.class,
                    () -> adapter.executeQuery("SELECT * FROM points"));
            assertEquals(TSDBErrorCodeEnum.CONNECTION_ERROR, error.getErrorCode());
        }
        assertEquals(enabled ? 2 : 1, requests.size());
    }

    @Test
    void writeClientSharesOwnedResourcesAndKeepsIdentityUntilTerminalClose() {
        setReadRecovery(true);
        Object httpAdapter = adapter;
        okhttp3.OkHttpClient readClient = (okhttp3.OkHttpClient) ReflectionTestUtils.getField(httpAdapter, "client");
        okhttp3.OkHttpClient writeClient = (okhttp3.OkHttpClient) ReflectionTestUtils.getField(httpAdapter, "writeClient");
        assertNotNull(readClient);
        assertNotNull(writeClient);
        assertTrue(readClient.retryOnConnectionFailure());
        assertFalse(writeClient.retryOnConnectionFailure());
        assertSame(readClient.connectionPool(), writeClient.connectionPool());
        assertSame(readClient.dispatcher(), writeClient.dispatcher());
        assertEquals(readClient.connectTimeoutMillis(), writeClient.connectTimeoutMillis());
        assertEquals(readClient.readTimeoutMillis(), writeClient.readTimeoutMillis());
        assertEquals(readClient.writeTimeoutMillis(), writeClient.writeTimeoutMillis());
        assertEquals(readClient.callTimeoutMillis(), writeClient.callTimeoutMillis());
        Object nativeClient = adapter.getNativeClient();
        adapter.init();
        assertSame(writeClient, ReflectionTestUtils.getField(httpAdapter, "writeClient"));
        assertSame(nativeClient, adapter.getNativeClient());
        adapter.close();
        adapter.close();
        assertNull(ReflectionTestUtils.getField(httpAdapter, "client"));
        assertNull(ReflectionTestUtils.getField(httpAdapter, "writeClient"));
        assertTrue(readClient.dispatcher().executorService().isShutdown());
        assertEquals(0, readClient.connectionPool().connectionCount());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.batchWriteDetailed(null, List.of(point(1, 1))))
                        .getErrorCode());
        assertThrows(TSDBException.class, adapter::init);
    }

    @Test
    void closeWaitsForInFlightWriteBeforeClosingSharedResources() throws Exception {
        CountDownLatch arrived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        server.removeContext("/");
        server.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            arrived.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new java.io.IOException("Write release timed out");
                exchange.sendResponseHeaders(204, -1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException(interrupted);
            } finally {
                exchange.close();
            }
        });
        Object httpAdapter = adapter;
        okhttp3.OkHttpClient readClient = (okhttp3.OkHttpClient) ReflectionTestUtils.getField(httpAdapter, "client");
        assertNotNull(readClient);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var write = executor.submit(() -> adapter.batchWriteDetailed(null, List.of(point(1, 1))));
            assertTrue(arrived.await(2, TimeUnit.SECONDS));
            var close = executor.submit(() -> {
                closing.countDown();
                adapter.close();
            });
            assertTrue(closing.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            assertFalse(readClient.dispatcher().executorService().isShutdown());
            release.countDown();
            assertTrue(write.get(2, TimeUnit.SECONDS).isSuccess());
            close.get(2, TimeUnit.SECONDS);
            assertTrue(readClient.dispatcher().executorService().isShutdown());
            assertNull(ReflectionTestUtils.getField(httpAdapter, "writeClient"));
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 302, 303, 307, 308})
    void redirectIsNeverFollowedOrReportedAsCommitted(int status) {
        replies.add(new Reply(status, "", "/redirected"));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(1, 1))));
        assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, error.getErrorCode());
        assertEquals(BatchCommitStateEnum.UNKNOWN, error.getResult().commitState());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(0, error.getResult().committedBatches());
        assertEquals(1, requests.size());
        assertEquals("POST", requests.get(0).method());
        assertNotEquals("/redirected", requests.get(0).path());
    }

    @Test
    void redirectAfterConfirmedFirstRequestPreservesItsCommitBoundaryWithoutReplay() {
        replies.add(new Reply(204, "", null));
        replies.add(new Reply(302, "", "/redirected"));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, Collections.nCopies(5_001, point(1, 1))));
        assertEquals(BatchCommitStateEnum.UNKNOWN, error.getResult().commitState());
        assertEquals(5_000, error.getResult().committedRecords());
        assertEquals(1, error.getResult().committedBatches());
        assertEquals(2, error.getResult().totalBatches());
        assertEquals(1, error.getResult().failedBatchIndex());
        assertEquals(2, requests.size());
        assertTrue(requests.stream().allMatch(request -> "POST".equals(request.method())));
    }

    @Test
    void compatibleNativeClientAlsoDoesNotFollowRedirects() {
        replies.add(new Reply(302, "", "/redirected"));
        assertThrows(RuntimeException.class, () -> adapter.getNativeClient().write("points v=1i 1"));
        assertEquals(1, requests.size());
        assertEquals("POST", requests.get(0).method());
        assertNotEquals("/redirected", requests.get(0).path());
    }

    @Test
    void acceptsExactUtf8BudgetIncludingInRequestNewline() {
        String value = "温度😀";
        String expected = "points v=\"" + value + "\" 1\npoints v=\"" + value + "\" 2";
        long bytes = utf8Bytes(expected);
        assertTrue(bytes > expected.length());
        setBudget(bytes);
        BatchWriteResult result = adapter.batchWriteDetailed(null, List.of(point(1, value), point(2, value)));
        assertTrue(result.isSuccess());
        assertEquals(2, result.committedRecords());
        assertEquals(1, requests.size());
        assertEquals(expected, new String(requests.get(0).body(), StandardCharsets.UTF_8));
        assertEquals(bytes, requests.get(0).body().length);
    }

    @Test
    void rejectsOneByteOverUtf8BudgetBeforeAnyRequest() {
        String value = "温度😀";
        String payload = "points v=\"" + value + "\" 1\npoints v=\"" + value + "\" 2";
        setBudget(utf8Bytes(payload) - 1);
        assertUncommittedBeforeIo(List.of(point(1, value), point(2, value)));
    }

    @Test
    void exactBudgetDoesNotAddASeparatorBetweenRequestsSplitByLineCount() {
        int count = 5_001;
        long bytes = utf8Bytes("points v=1i 1") * count + count - 2;
        setBudget(bytes);
        BatchWriteResult result = adapter.batchWriteDetailed(null, Collections.nCopies(count, point(1, 1)));
        assertEquals(2, result.committedBatches());
        assertEquals(count, result.committedRecords());
        assertEquals(2, requests.size());
        assertEquals(bytes, requests.stream().mapToLong(request -> request.body().length).sum());
    }

    @Test
    void exactBudgetDoesNotAddASeparatorBetweenRequestsSplitByByteCount() {
        String value = "x".repeat(60 * 1024);
        int count = 20;
        long lineBytes = utf8Bytes("points v=\"" + value + "\" 1");
        long bytes = lineBytes * count + count - 2;
        setBudget(bytes);
        BatchWriteResult result = adapter.batchWriteDetailed(null, Collections.nCopies(count, point(1, value)));
        assertEquals(2, result.committedBatches());
        assertEquals(2, requests.size());
        assertEquals(bytes, requests.stream().mapToLong(request -> request.body().length).sum());
        assertTrue(requests.stream().allMatch(request -> request.body().length <= 1024 * 1024));
    }

    @Test
    void budgetExceededInASecondPreparedRequestStillSendsNothing() {
        String value = "x".repeat(60 * 1024);
        int count = 20;
        long bytes = utf8Bytes("points v=\"" + value + "\" 1") * count + count - 2;
        setBudget(bytes - 1);
        assertUncommittedBeforeIo(Collections.nCopies(count, point(1, value)));
    }

    @Test
    void capturesBudgetAtConstructionBeforeInitializationAndLaterPropertyChanges() {
        adapter.close();
        long twoRecordBytes = utf8Bytes("points v=1i 1\npoints v=2i 2");
        properties.setMaxBatchBytes(twoRecordBytes - 1);
        adapter = newAdapter();
        properties.setMaxBatchBytes(Long.MAX_VALUE);
        adapter.init();
        assertUncommittedBeforeIo(List.of(point(1, 1), point(2, 2)));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, Long.MIN_VALUE})
    void rejectsNonpositiveBudgetAtConstruction(long bytes) {
        properties.setMaxBatchBytes(bytes);
        TSDBException error = assertThrows(TSDBException.class, this::newAdapter);
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR, error.getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void defaultsTo64MiBAndAllowsALongValuedBudgetWithoutOverflow() {
        assertEquals(64L * 1024 * 1024, new InfluxDB1Properties().getMaxBatchBytes());
        setBudget(Long.MAX_VALUE);
        assertTrue(adapter.batchWriteDetailed(null, List.of(point(1, 1))).isSuccess());
        assertEquals(1, requests.size());
    }

    @Test
    void largeWholeBatchBudgetDoesNotDisableTheSingleLineLimit() {
        setBudget(Long.MAX_VALUE);
        Map<String, Object> fields = new LinkedHashMap<>();
        String shared = "x".repeat(60 * 1024);
        for (int index = 0; index < 18; index++) fields.put("field" + index, shared);
        assertUncommittedBeforeIo(List.of(new TSDBRecord("points", 1L, Map.of(), fields)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void timeFiltersUseMillisecondsAndPreserveCaseDistinctFields(boolean countOnly) throws Exception {
        TSDBQuery query = filteredQuery("time", 1783000000000L);
        query.getFilters().add(new QueryFilter("VALUE", OperatorEnum.EQ, List.of(7)));
        query.getFilters().add(new QueryFilter("value", OperatorEnum.EQ, List.of(8)));
        completeEmptyRead(query, countOnly);
        String sql = lastQuery();
        assertTrue(sql.contains("\"time\" = '2026-07-02T13:46:40Z'"), sql);
        assertTrue(sql.contains("\"VALUE\" = 7"), sql);
        assertTrue(sql.contains("\"value\" = 8"), sql);

        int beforeInvalidTime = requests.size();
        TSDBQuery textTimeQuery = filteredQuery("time", "1783000000000");
        TSDBException error = assertThrows(TSDBException.class, () -> {
            if (countOnly) adapter.count(null, textTimeQuery);
            else adapter.query(null, textTimeQuery);
        });
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        assertEquals(beforeInvalidTime, requests.size());

        TSDBQuery textFieldQuery = filteredQuery("v", "1783000000000");
        completeEmptyRead(textFieldQuery, countOnly);
        assertTrue(lastQuery().contains("\"v\" = '1783000000000'"), lastQuery());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"results\":[{}],\"results\":[{}]}",
            "{\"results\":[{\"series\":[],\"series\":[]}]}",
            "{\"results\":[{\"series\":[{\"columns\":[\"time\",\"v\"],\"tags\":{\"device\":\"a\",\"device\":\"b\"},\"values\":[[1,2]]}]}]}"
    })
    void duplicateJsonObjectKeysFailForEveryReadMode(String responseBody) {
        for (int invocation = 0; invocation < 3; invocation++)
            replies.add(new Reply(200, responseBody, null));
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        List<Runnable> operations = List.of(
                () -> adapter.query(null, query),
                () -> adapter.count(null, query),
                () -> adapter.executeQuery("SELECT * FROM points"));
        for (Runnable operation : operations) {
            TSDBException error = assertThrows(TSDBException.class, operation::run);
            assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, error.getErrorCode());
            assertEquals("Invalid InfluxDB1 JSON response", error.getMessage());
            assertInstanceOf(com.fasterxml.jackson.core.JsonProcessingException.class, error.getCause());
        }
        assertEquals(3, requests.size());
    }

    @Test
    void equalTagNamesInSeparateSeriesRemainValid() {
        String responseBody = "{\"results\":[{\"series\":["
                + "{\"columns\":[\"time\",\"v\"],\"tags\":{\"device\":\"a\"},\"values\":[[1,2]]},"
                + "{\"columns\":[\"time\",\"v\"],\"tags\":{\"device\":\"b\"},\"values\":[[2,3]]}]}]}";
        replies.add(new Reply(200, responseBody, null));
        replies.add(new Reply(200, responseBody, null));
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        assertEquals(2, adapter.query(null, query).getRowCount());
        assertEquals(2L, adapter.count(null, query));
    }

    @ParameterizedTest
    @ValueSource(strings = {"NE", "IN"})
    void unsupportedPrimaryTimeFiltersFailBeforeQueryOrCountIo(String operatorName) {
        OperatorEnum operator = OperatorEnum.valueOf(operatorName);
        for (String column : List.of("time", "TIME", "Time")) {
            for (boolean instant : List.of(false, true)) {
                List<Object> values = operator == OperatorEnum.IN
                        ? List.of(1783000000000L, 1783000000002L) : List.of(1783000000001L);
                if (instant) values = values.stream().map(value -> (Object) Instant.ofEpochMilli((Long) value)).toList();
                TSDBQuery query = new TSDBQuery();
                query.setMeasurement("points");
                query.setTimeColumn(column);
                query.setFilters(List.of(new QueryFilter(column, operator, values)));
                for (boolean countOnly : List.of(false, true)) {
                    TSDBException error = assertThrows(TSDBException.class, () -> {
                        if (countOnly) adapter.count(null, query);
                        else adapter.query(null, query);
                    });
                    assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, error.getErrorCode());
                    assertTrue(error.getMessage().contains("primary-time NE and IN"), error.getMessage());
                    assertTrue(requests.isEmpty());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"NE", "IN"})
    void ordinaryFieldsKeepExistingNotEqualAndMembershipSql(String operatorName) throws Exception {
        OperatorEnum operator = OperatorEnum.valueOf(operatorName);
        for (String column : List.of("v", "VALUE")) {
            for (boolean countOnly : List.of(false, true)) {
                TSDBQuery query = new TSDBQuery();
                query.setMeasurement("points");
                query.setFilters(List.of(new QueryFilter(column, operator,
                        operator == OperatorEnum.IN ? List.of(1, 3) : List.of(1))));
                completeEmptyRead(query, countOnly);
                String expected = operator == OperatorEnum.NE ? "\"" + column + "\" != 1"
                        : "(\"" + column + "\" = 1 OR \"" + column + "\" = 3)";
                assertTrue(lastQuery().contains(expected), lastQuery());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"time", "TIME", "Time"})
    void primaryTimeCaseVariantsEncodeLongAndInstantIdentically(String column) {
        long millis = 1783000000000L;
        for (Object value : List.of(millis, Instant.ofEpochMilli(millis))) {
            for (boolean countOnly : List.of(false, true)) {
                TSDBQuery query = filteredQuery(column, value);
                query.setTimeColumn(column);
                completeEmptyRead(query, countOnly);
                String expected = "\"" + column + "\" = '2026-07-02T13:46:40Z'";
                assertTrue(lastQuery().contains(expected), lastQuery());
                assertEquals(column, query.getTimeColumn());
                assertSame(value, query.getFilters().get(0).values().get(0));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"time", "TIME", "Time"})
    void invalidPrimaryTimeOperandsFailBeforeIoForEveryCaseVariant(String column) {
        for (Object value : List.of(new java.math.BigDecimal("1.5"), "1783000000000", "2026-07-02T13:46:40Z", true)) {
            TSDBQuery query = filteredQuery(column, value);
            query.setTimeColumn(column);
            for (boolean countOnly : List.of(false, true)) {
                TSDBException error = assertThrows(TSDBException.class, () -> {
                    if (countOnly) adapter.count(null, query);
                    else adapter.query(null, query);
                });
                assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
                assertTrue(requests.isEmpty());
            }
        }
    }

    private static TSDBQuery filteredQuery(String column, Object value) {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        query.setFilters(List.of(new QueryFilter(column, OperatorEnum.EQ, List.of(value))));
        return query;
    }

    private void completeEmptyRead(TSDBQuery query, boolean countOnly) {
        replies.add(new Reply(200, "{\"results\":[{}]}", null));
        if (countOnly) assertEquals(0L, adapter.count(null, query));
        else assertTrue(adapter.query(null, query).isSuccess());
    }

    private String lastQuery() {
        String form = new String(requests.get(requests.size() - 1).body(), StandardCharsets.UTF_8);
        for (String field : form.split("&")) {
            if (field.startsWith("q=")) return URLDecoder.decode(field.substring(2), StandardCharsets.UTF_8);
        }
        fail("Missing query form parameter: " + form);
        return "";
    }
}
