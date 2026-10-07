package com.alandevise.tsgate.contract;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/** A loopback protocol peer; it does not claim database SQL or storage compatibility. */
public final class HttpContractPeer implements AutoCloseable {
    public record Request(String path, String body) { }
    private record Reply(int status, String body) { }

    private final HttpServer server;
    private final Queue<Reply> replies = new ConcurrentLinkedQueue<>();
    private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
    private final String defaultResponse;

    public HttpContractPeer(String defaultResponse) throws IOException {
        this.defaultResponse = defaultResponse;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                requests.add(new Request(exchange.getRequestURI().toString(),
                        new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
                Reply reply = replies.poll();
                if (reply == null) reply = new Reply(200, this.defaultResponse);
                byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(reply.status(), reply.status() == 204 ? -1 : bytes.length);
                if (reply.status() != 204) exchange.getResponseBody().write(bytes);
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public void reply(int status, String body) {
        replies.add(new Reply(status, body));
    }

    public int requestCount() {
        return requests.size();
    }

    public Request lastRequest() {
        return requests.get(requests.size() - 1);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
