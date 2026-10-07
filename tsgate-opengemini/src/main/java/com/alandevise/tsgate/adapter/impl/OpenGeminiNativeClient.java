package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.InfluxDB1HttpClientProperties;
import com.alandevise.tsgate.config.InfluxDB1Properties;
import okhttp3.ConnectionPool;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.Pong;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Adapts the borrowed compatibility client's health methods to openGemini's version header.
 * All database operations and close calls continue to use the original client.
 */
final class OpenGeminiNativeClient implements InvocationHandler {
    private static final String UNKNOWN_VERSION = "unknown";
    private final InfluxDB raw;
    private final Snapshot configuration;
    private volatile String confirmedVersion;

    private OpenGeminiNativeClient(InfluxDB raw, InfluxDB1Properties snapshot,
                                   InfluxDB1HttpClientProperties httpSnapshot) {
        this.raw = Objects.requireNonNull(raw, "native client");
        this.configuration = new Snapshot(snapshot, httpSnapshot);
    }

    /**
     * Wraps a client without performing I/O. The caller retains and returns the same wrapper,
     * and must supply its private constructor-time configuration copies.
     */
    static InfluxDB wrap(InfluxDB raw, InfluxDB1Properties snapshot,
                        InfluxDB1HttpClientProperties httpSnapshot) {
        OpenGeminiNativeClient handler = new OpenGeminiNativeClient(raw, snapshot, httpSnapshot);
        return (InfluxDB) Proxy.newProxyInstance(InfluxDB.class.getClassLoader(),
                new Class<?>[]{InfluxDB.class}, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "equals" -> proxy == arguments[0];
                case "hashCode" -> System.identityHashCode(proxy);
                case "toString" -> "OpenGeminiNativeClient@" + Integer.toHexString(System.identityHashCode(proxy));
                default -> throw new UnsupportedOperationException(method.getName());
            };
        }
        if (method.getParameterCount() == 0) {
            if (method.getName().equals("ping")) return ping();
            if (method.getName().equals("version")) return version();
        }
        try {
            Object result = method.invoke(raw, arguments);
            // Fluent configuration must retain this wrapper for subsequent health calls.
            return result == raw ? proxy : result;
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private synchronized String version() {
        if (confirmedVersion != null) return confirmedVersion;
        Pong pong = ping();
        if (pong.isGood()) confirmedVersion = pong.getVersion();
        // A failed or incomplete health response must remain retryable.
        return pong.getVersion();
    }

    private Pong ping() {
        long started = System.nanoTime();
        OkHttpClient client = new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(0, configuration.keepAliveDurationMs, TimeUnit.MILLISECONDS))
                .connectTimeout(configuration.connectTimeoutMs, TimeUnit.MILLISECONDS)
                .readTimeout(configuration.readTimeoutMs, TimeUnit.MILLISECONDS)
                .writeTimeout(configuration.writeTimeoutMs, TimeUnit.MILLISECONDS)
                .callTimeout(configuration.callTimeoutMs, TimeUnit.MILLISECONDS)
                .followRedirects(false)
                .followSslRedirects(false)
                .retryOnConnectionFailure(configuration.retryOnConnectionFailure)
                .build();
        try {
            Request.Builder request = new Request.Builder().url(configuration.pingUrl).get();
            if (configuration.authorization != null) request.header("Authorization", configuration.authorization);
            try (Response response = client.newCall(request.build()).execute()) {
                String version = response.isSuccessful() ? response.header("X-Geminidb-Version") : null;
                Pong pong = new Pong();
                pong.setVersion(version == null || version.isBlank() ? UNKNOWN_VERSION : version.trim());
                pong.setResponseTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
                return pong;
            }
        } catch (IOException failure) {
            throw new InfluxDBIOException(failure);
        } finally {
            // Health calls hold no persistent pool; the borrowed native client's resources
            // remain owned by the original adapter and are closed through that adapter.
            try {
                client.dispatcher().executorService().shutdown();
            } finally {
                client.connectionPool().evictAll();
            }
        }
    }

    /** Copies only health request inputs; neither configuration object is retained. */
    private static final class Snapshot {
        private final HttpUrl pingUrl;
        private final String authorization;
        private final long keepAliveDurationMs;
        private final long connectTimeoutMs;
        private final long readTimeoutMs;
        private final long writeTimeoutMs;
        private final long callTimeoutMs;
        private final boolean retryOnConnectionFailure;

        private Snapshot(InfluxDB1Properties source, InfluxDB1HttpClientProperties http) {
            Objects.requireNonNull(source, "connection snapshot");
            Objects.requireNonNull(http, "HTTP snapshot");
            HttpUrl base = Objects.requireNonNull(HttpUrl.parse(source.getUrl()), "snapshot URL");
            pingUrl = base.newBuilder().addPathSegment("ping").build();
            authorization = source.getUsername() == null || source.getUsername().isBlank() ? null
                    : Credentials.basic(source.getUsername(), source.getPassword() == null ? "" : source.getPassword());
            keepAliveDurationMs = http.getKeepAliveDurationMs();
            connectTimeoutMs = http.getConnectTimeoutMs();
            readTimeoutMs = http.getReadTimeoutMs();
            writeTimeoutMs = http.getWriteTimeoutMs();
            callTimeoutMs = http.getCallTimeoutMs();
            retryOnConnectionFailure = http.isRetryOnConnectionFailure();
        }
    }
}
