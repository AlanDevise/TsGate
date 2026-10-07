package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.OpenGeminiProperties;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies that native health checks cannot silently follow an ingress redirect. */
class OpenGeminiHealthRedirectTest {
    private HttpServer server;
    private OpenGeminiAdapter adapter;
    private final AtomicInteger status = new AtomicInteger(302);
    private final List<String> paths = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            paths.add(path);
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().set("X-Geminidb-Version", "1.5.2");
            int reply = "/healthy".equals(path) ? 204 : status.get();
            if (reply >= 300 && reply < 400) exchange.getResponseHeaders().set("Location", "/healthy");
            exchange.sendResponseHeaders(reply, -1);
            exchange.close();
        });
        server.start();
        OpenGeminiProperties properties = new OpenGeminiProperties();
        properties.setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        adapter = new OpenGeminiAdapter(properties);
        adapter.init();
    }

    @AfterEach
    void cleanup() {
        if (adapter != null) adapter.close();
        if (server != null) server.stop(0);
    }

    @ParameterizedTest
    @ValueSource(ints = {301, 302, 303, 307, 308})
    void redirectedPingIsUnhealthyWithoutRequestingTheRedirectTarget(int responseStatus) {
        status.set(responseStatus);
        var pong = adapter.getNativeClient().ping();
        assertFalse(pong.isGood());
        assertEquals("unknown", pong.getVersion());
        assertEquals(List.of("/ping"), paths);
    }

    @Test
    void versionDoesNotCacheRedirectsAndCanConfirmALaterDirectHealthResponse() {
        var client = adapter.getNativeClient();
        assertEquals("unknown", client.version());
        status.set(204);
        assertEquals("1.5.2", client.version());
        assertEquals("1.5.2", client.version());
        assertEquals(List.of("/ping", "/ping"), paths);
    }
}
