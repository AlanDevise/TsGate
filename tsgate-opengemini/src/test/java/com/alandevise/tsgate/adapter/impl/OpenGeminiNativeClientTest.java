package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.InfluxDB1HttpClientProperties;
import com.alandevise.tsgate.config.InfluxDB1Properties;
import okhttp3.Credentials;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OpenGeminiNativeClientTest {
    private MockWebServer server;
    private InfluxDB raw;
    private InfluxDB1Properties properties;
    private InfluxDB1HttpClientProperties http;

    @BeforeEach
    void setup() throws Exception {
        server = new MockWebServer();
        server.start();
        raw = mock(InfluxDB.class);
        properties = new InfluxDB1Properties();
        properties.setUrl(server.url("/").toString());
        http = new InfluxDB1HttpClientProperties();
        http.setRetryOnConnectionFailure(false);
        http.setCallTimeoutMs(2000);
    }

    @AfterEach
    void cleanup() throws Exception {
        server.shutdown();
    }

    private InfluxDB wrap() {
        return OpenGeminiNativeClient.wrap(raw, properties, http);
    }

    private MockResponse successfulHealth(String version) {
        return new MockResponse().setResponseCode(204).addHeader("X-Geminidb-Version", version);
    }

    private RecordedRequest request() throws Exception {
        RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        assertNotNull(request, "Expected an actual health HTTP request");
        return request;
    }

    @Test
    void pingUsesTheActualGeminiHeaderAndRecordsElapsedTimeWithoutSdkHealthCalls() throws Exception {
        server.enqueue(successfulHealth("1.5.2").setHeadersDelay(50, TimeUnit.MILLISECONDS));
        InfluxDB client = wrap();
        assertEquals(0, server.getRequestCount(), "Wrapping must not probe connectivity");
        Pong pong = client.ping();
        assertTrue(pong.isGood());
        assertEquals("1.5.2", pong.getVersion());
        assertTrue(pong.getResponseTime() >= 30, "The server deliberately delayed its response headers");
        RecordedRequest request = request();
        assertEquals("GET", request.getMethod());
        assertEquals("/ping", request.getPath());
        verifyNoInteractions(raw);
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 404, 500})
    void anErrorStatusCannotLookHealthyEvenWithAGeminiVersionHeader(int status) {
        server.enqueue(successfulHealth("1.4.1").setResponseCode(status).setBody("rejected"));
        Pong pong = wrap().ping();
        assertFalse(pong.isGood());
        assertEquals("unknown", pong.getVersion());
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "empty", "blank", "unknown", "influx-only"})
    void anUnconfirmedVersionNeverClaimsGoodHealth(String kind) {
        MockResponse response = new MockResponse().setResponseCode(204);
        switch (kind) {
            case "empty" -> response.addHeader("X-Geminidb-Version", "");
            case "blank" -> response.addHeader("X-Geminidb-Version", "  ");
            case "unknown" -> response.addHeader("X-Geminidb-Version", "unknown");
            case "influx-only" -> response.addHeader("X-Influxdb-Version", "1.8.10");
        }
        server.enqueue(response);
        Pong pong = wrap().ping();
        assertEquals("unknown", pong.getVersion());
        assertFalse(pong.isGood());
    }

    @Test
    void versionRetriesUnconfirmedHealthAndCachesOnlyTheActualServerVersion() {
        server.enqueue(new MockResponse().setResponseCode(204));
        server.enqueue(successfulHealth("1.4.1"));
        InfluxDB client = wrap();
        assertEquals("unknown", client.version());
        assertEquals("1.4.1", client.version());
        assertEquals("1.4.1", client.version());
        assertEquals(2, server.getRequestCount());
        verifyNoInteractions(raw);
    }

    @Test
    void healthAuthenticationUrlAndSettingsUseAnImmutableSnapshot() throws Exception {
        properties.setUrl(server.url("/gateway/").toString());
        properties.setUsername("user");
        properties.setPassword("original-secret");
        InfluxDB client = wrap();
        properties.setUrl("http://127.0.0.1:1");
        properties.setUsername("changed-user");
        properties.setPassword("changed-secret");
        http.setReadTimeoutMs(-1);
        http.setKeepAliveDurationMs(-1);
        server.enqueue(successfulHealth("1.5.2"));
        assertTrue(client.ping().isGood());
        RecordedRequest request = request();
        assertEquals("/gateway/ping", request.getPath());
        assertEquals(Credentials.basic("user", "original-secret"), request.getHeader("Authorization"));
        assertFalse(client.toString().contains("original-secret"));
        assertFalse(client.toString().contains("changed-secret"));
    }

    @Test
    void capturedCallDeadlineBoundsAnUnresponsiveHealthEndpoint() {
        http.setReadTimeoutMs(0);
        http.setCallTimeoutMs(200);
        InfluxDB client = wrap();
        http.setCallTimeoutMs(0);
        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            InfluxDBIOException error = assertThrows(InfluxDBIOException.class, client::ping);
            assertNotNull(error.getCause());
        });
    }

    @Test
    void fluentCallsKeepProxyIdentityAndDelegateOrdinaryOperations() {
        when(raw.setDatabase("business")).thenReturn(raw);
        when(raw.setRetentionPolicy("custom")).thenReturn(raw);
        InfluxDB client = wrap();
        assertSame(client, client.setDatabase("business").setRetentionPolicy("custom"));
        server.enqueue(successfulHealth("1.5.2"));
        assertTrue(client.setDatabase("business").ping().isGood());
        verify(raw, times(2)).setDatabase("business");
        verify(raw).setRetentionPolicy("custom");
        verify(raw, never()).ping();
    }

    @Test
    void objectIdentityWorksAsAMapKeyAndDoesNotExposeConfiguration() {
        properties.setPassword("sensitive-secret");
        InfluxDB client = wrap();
        assertEquals(client, client);
        assertNotEquals(client, raw);
        assertNotEquals(client, wrap());
        assertNotEquals(client, null);
        int identity = client.hashCode();
        assertEquals(identity, client.hashCode());
        Map<InfluxDB, String> clients = new HashMap<>();
        clients.put(client, "present");
        assertEquals("present", clients.get(client));
        assertFalse(client.toString().contains("sensitive-secret"));
        verifyNoInteractions(raw);
    }

    @Test
    void ordinaryQueryErrorsAreReturnedWithoutNormalization() {
        Query query = new Query("SELECT * FROM missing", "business");
        QueryResult error = new QueryResult();
        error.setError("measurement not found");
        when(raw.query(query)).thenReturn(error);
        QueryResult result = wrap().query(query);
        assertSame(error, result);
        assertTrue(result.hasError());
        assertEquals("measurement not found", result.getError());
        verify(raw).query(query);
        assertEquals(0, server.getRequestCount());
    }

    @Test
    void ordinaryQueryExceptionsKeepTheirOriginalTypeAndInstance() {
        Query query = new Query("SELECT * FROM points", "business");
        IllegalArgumentException original = new IllegalArgumentException("native query failed");
        when(raw.query(query)).thenThrow(original);
        assertSame(original, assertThrows(IllegalArgumentException.class, () -> wrap().query(query)));
    }

    @Test
    void closeStillDelegatesToTheOriginalClientWithoutAdditionalNetworkCalls() {
        wrap().close();
        verify(raw, times(1)).close();
        assertEquals(0, server.getRequestCount());
    }

    @Test
    void successiveHealthCallsDoNotRetainAnIdleConnectionPool() throws Exception {
        InfluxDB client = wrap();
        server.enqueue(successfulHealth("1.4.1"));
        server.enqueue(successfulHealth("1.4.1"));
        assertTrue(client.ping().isGood());
        assertTrue(client.ping().isGood());
        assertEquals(0, request().getSequenceNumber());
        assertEquals(0, request().getSequenceNumber());
    }
}
