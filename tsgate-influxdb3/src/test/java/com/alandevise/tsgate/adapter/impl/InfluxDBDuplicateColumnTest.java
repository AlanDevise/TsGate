package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.InfluxDBProperties;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.QueryResult;
import com.alandevise.tsgate.model.TSDBQuery;
import com.sun.net.httpserver.HttpServer;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;
import okio.BufferedSource;
import okio.ForwardingSource;
import okio.Okio;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies that successful HTTP responses cannot silently overwrite duplicate result columns. */
class InfluxDBDuplicateColumnTest {
    private HttpServer server;
    private InfluxDBAdapter adapter;
    private OkHttpClient http;
    private final Queue<String> replies = new ConcurrentLinkedQueue<>();
    private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
    private final List<TrackedBody> bodies = Collections.synchronizedList(new ArrayList<>());

    private enum QueryPath { STRUCTURED, NATIVE, COUNT }

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                requests.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                String reply = replies.poll();
                if (reply == null) reply = "[]";
                byte[] bytes = reply.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally {
                exchange.close();
            }
        });
        server.start();
        InfluxDBProperties properties = new InfluxDBProperties();
        properties.setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        properties.setDatabase("contract");
        properties.getHttpClient().setRetryOnConnectionFailure(false);
        adapter = new InfluxDBAdapter(properties, false);
        adapter.init();
        OkHttpClient original = (OkHttpClient) ReflectionTestUtils.getField(adapter, "client");
        assertNotNull(original);
        http = original.newBuilder().addNetworkInterceptor(chain -> {
            var response = chain.proceed(chain.request());
            ResponseBody body = response.body();
            if (body == null) return response;
            TrackedBody tracked = new TrackedBody(body);
            bodies.add(tracked);
            return response.newBuilder().body(tracked).build();
        }).build();
        ReflectionTestUtils.setField(adapter, "client", http);
    }

    @AfterEach
    void cleanup() {
        if (adapter != null) adapter.close();
        if (server != null) server.stop(0);
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("duplicateResponses")
    void duplicateKeysFailTheWholeQueryAndReleaseItsResponse(QueryPath path, String body) {
        replies.add(body);
        TSDBException failure = assertThrows(TSDBException.class, () -> execute(path));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
        assertNotNull(failure.getCause());
        assertTrue(failure.getCause().getMessage().contains("Duplicate field"));
        assertEquals(1, requests.size(), "The malformed response must not be retried");
        assertResponsesClosed(1);

        // A valid first row in a malformed response must not become a successful partial result.
        // A following valid request also verifies that the HTTP call and lifecycle lock were released.
        replies.add("[{\"value\":3.25}]");
        QueryResult next = adapter.executeQuery("SELECT value FROM points");
        assertTrue(next.isSuccess());
        assertEquals(1, next.getRowCount());
        assertEquals(new BigDecimal("3.25"), next.getRows().get(0).get("value"));
        assertEquals(2, requests.size());
        assertResponsesClosed(2);
    }

    private static Stream<Arguments> duplicateResponses() {
        List<String> bodies = List.of(
                "[{\"total\":1,\"total\":2}]",
                "[{\"total\":1,\"total\":1}]",
                "[{\"total\":null,\"total\":2}]",
                "[{\"total\":null,\"total\":null}]",
                "[{\"total\":7},{\"total\":null,\"total\":2}]");
        return Stream.of(QueryPath.values()).flatMap(path -> bodies.stream()
                .map(body -> Arguments.of(path, body)));
    }

    @ParameterizedTest
    @EnumSource(value = QueryPath.class, names = {"STRUCTURED", "NATIVE"})
    void caseDistinctColumnsAndRepeatedKeysInDifferentRowsRetainExactValues(QueryPath path) {
        replies.add("[{\"value\":null,\"VALUE\":0.12345678901234567890123456789,"
                + "\"integer\":9223372036854775808},{\"value\":2,\"VALUE\":3.50,\"integer\":4}]");
        QueryResult result = (QueryResult) execute(path);
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRowCount());
        assertEquals(List.of("value", "VALUE", "integer"), result.getColumns());
        Map<String, Object> first = result.getRows().get(0);
        assertTrue(first.containsKey("value"));
        assertNull(first.get("value"));
        assertEquals(new BigDecimal("0.12345678901234567890123456789"), first.get("VALUE"));
        assertEquals(new BigInteger("9223372036854775808"), first.get("integer"));
        assertEquals(2, result.getRows().get(1).get("value"));
        assertEquals(new BigDecimal("3.50"), result.getRows().get(1).get("VALUE"));
        assertResponsesClosed(1);
    }

    @Test
    void countPreservesCaseDistinctKeysAndAnEmptyArrayRemainsAValidResponse() {
        replies.add("[{\"total\":2,\"TOTAL\":99}]");
        assertEquals(2L, execute(QueryPath.COUNT));
        for (QueryPath path : QueryPath.values()) {
            replies.add("[]");
            Object result = execute(path);
            if (result instanceof QueryResult queryResult) {
                assertTrue(queryResult.isSuccess());
                assertTrue(queryResult.getRows().isEmpty());
            } else {
                assertEquals(0L, result);
            }
        }
        assertResponsesClosed(4);
        adapter.close();
        assertTrue(http.dispatcher().executorService().isShutdown());
    }

    private Object execute(QueryPath path) {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        return switch (path) {
            case STRUCTURED -> adapter.query(null, query);
            case NATIVE -> adapter.executeQuery("SELECT * FROM points");
            case COUNT -> adapter.count(null, query);
        };
    }

    private void assertResponsesClosed(int expected) {
        assertEquals(expected, bodies.size());
        assertTrue(bodies.stream().allMatch(body -> body.closed), "Every response source must close");
        assertEquals(0, http.dispatcher().runningCallsCount());
    }

    /** Observes source closure while retaining OkHttp's real HTTP request and response handling. */
    private static final class TrackedBody extends ResponseBody {
        private final ResponseBody delegate;
        private final BufferedSource source;
        private volatile boolean closed;

        private TrackedBody(ResponseBody delegate) {
            this.delegate = delegate;
            this.source = Okio.buffer(new ForwardingSource(delegate.source()) {
                @Override
                public void close() throws IOException {
                    closed = true;
                    super.close();
                }
            });
        }

        @Override public MediaType contentType() { return delegate.contentType(); }
        @Override public long contentLength() { return delegate.contentLength(); }
        @Override public BufferedSource source() { return source; }
    }
}
