package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.*;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises redirect handling and complete-batch byte budgets through a local HTTP peer. */
class InfluxDBHttpSafetyTest {
    private HttpServer server;
    private InfluxDBAdapter adapter;
    private InfluxDBProperties properties;

    private final List<CapturedRequest> requests = Collections.synchronizedList(new ArrayList<>());
    private final Queue<Reply> replies = new ConcurrentLinkedQueue<>();

    private record CapturedRequest(String method, String path, byte[] body) { }
    private record Reply(int status, String body, String location) { }

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
            if (reply.location() != null) exchange.getResponseHeaders().set("Location", reply.location());
            byte[] body = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(reply.status(), reply.status() == 204 || body.length == 0 ? -1 : body.length);
            if (reply.status() != 204 && body.length > 0) exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        properties = new InfluxDBProperties();
        properties.setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        properties.getHttpClient().setRetryOnConnectionFailure(false);
        adapter = newAdapter();
        adapter.init();
    }

    @AfterEach
    void cleanup() {
        if (adapter != null) adapter.close();
        if (server != null) server.stop(0);
    }

    private InfluxDBAdapter newAdapter() {
        return new InfluxDBAdapter(properties, false);
    }

    private void setBudget(long bytes) {
        adapter.close();
        properties.setMaxBatchBytes(bytes);
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
        assertEquals(64L * 1024 * 1024, new InfluxDBProperties().getMaxBatchBytes());
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
    @ValueSource(strings = {"", " \n\t ", "{}"})
    void missingOrInvalidJsonIsAQueryErrorForReadAndCount(String body) {
        for (int invocation = 0; invocation < 2; invocation++) replies.add(new Reply(200, body, null));
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points")).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.count(null, query)).getErrorCode());
    }

    @Test
    void noContentStatusIsAQueryErrorForReadAndCount() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.count(null, query)).getErrorCode());
    }

    @Test
    void anExplicitEmptyJsonArrayRemainsASuccessfulEmptyReadAndCount() {
        replies.add(new Reply(200, "[]", null));
        replies.add(new Reply(200, "[]", null));
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        var result = adapter.query(null, query);
        assertTrue(result.isSuccess());
        assertEquals(0, result.getRowCount());
        assertEquals(0L, adapter.count(null, query));
    }
}
