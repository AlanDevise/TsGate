package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.annotation.TGField;
import com.alandevise.tsgate.annotation.TGMeasurement;
import com.alandevise.tsgate.annotation.TGTag;
import com.alandevise.tsgate.annotation.TGTime;
import com.alandevise.tsgate.config.InfluxDBProperties;
import com.alandevise.tsgate.config.StrictCursorSqlStrategyEnum;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.OperatorEnum;
import com.alandevise.tsgate.model.PageResult;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.SortOrderEnum;
import com.alandevise.tsgate.model.SortSpec;
import com.alandevise.tsgate.model.TSDBQuery;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises strategy selection through real HTTP requests without a database process. */
class InfluxDBStrictCursorStrategyTest {
    private static final String CURSOR_TIME = "timestamp '1970-01-01T00:00:01Z'";
    private HttpServer server;
    private InfluxDBProperties properties;
    private InfluxDBAdapter adapter;
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
    private final Queue<Reply> replies = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            Reply reply = replies.poll();
            if (reply == null) reply = new Reply(200, "[]", false);
            byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(reply.status(), reply.chunked() ? 0 : bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        properties = new InfluxDBProperties();
        properties.setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        properties.setDatabase("cursor_contract");
        properties.getHttpClient().setCallTimeoutMs(3000);
    }

    @AfterEach
    void cleanup() {
        if (adapter != null) adapter.close();
        server.stop(0);
    }

    @Test
    void omissionPreservesOriginalOrSql() throws Exception {
        assertEquals(StrictCursorSqlStrategyEnum.OR, properties.getStrictCursorSql());
        open(StrictCursorSqlStrategyEnum.OR);
        adapter.query(null, cursor());
        assertTrue(sql(0).contains(" OR "));
        assertFalse(sql(0).contains("UNION"));
        assertTrue(sql(0).endsWith("ORDER BY time ASC, \"device\" ASC LIMIT 3"), sql(0));
    }

    @Test
    void nullStrategyFailsBeforeCreatingResources() {
        properties.setStrictCursorSql(null);
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                assertThrows(TSDBException.class, () -> new InfluxDBAdapter(properties, false)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void strategyIsFrozenAtConstruction() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        properties.setStrictCursorSql(StrictCursorSqlStrategyEnum.OR);
        adapter.query(null, cursor());
        assertTrue(sql(0).contains(" UNION ALL "));
    }

    @ParameterizedTest
    @EnumSource(SortOrderEnum.class)
    void unionKeepsDisjointTimeAndTagBranchesWithGlobalOrdering(SortOrderEnum order) throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setOrder(order);
        query.setOffset(4);
        adapter.query("another_database", query);
        String sql = sql(0);
        String compare = order == SortOrderEnum.ASC ? ">" : "<";
        assertEquals(1, occurrences(sql, " UNION ALL "), sql);
        assertTrue(sql.contains("time " + compare + " " + CURSOR_TIME), sql);
        assertTrue(sql.contains("time = " + CURSOR_TIME + " AND \"device\" " + compare + " 'b'"), sql);
        assertFalse(sql.contains(" OR "), sql);
        assertEquals(1, occurrences(sql, " ORDER BY "), sql);
        assertEquals(1, occurrences(sql, " LIMIT "), sql);
        assertEquals(1, occurrences(sql, " OFFSET "), sql);
        assertTrue(sql.endsWith("ORDER BY time " + order + ", \"device\" " + order + " LIMIT 3 OFFSET 4"), sql);
        assertEquals("another_database", mapper.readTree(requests.get(0)).get("db").asText());
    }

    @Test
    void fieldFirstMixedDirectionCursorHasOneBranchPerLexicographicPosition() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setCursorColumns(List.of("value", "time", "device"));
        query.setSortSpecs(List.of(new SortSpec("value", SortOrderEnum.DESC),
                new SortSpec("time", SortOrderEnum.ASC), new SortSpec("device", SortOrderEnum.DESC)));
        query.setCursorValues(Map.of("value", 7, "time", 1000L, "device", "b"));
        adapter.query(null, query);
        String sql = sql(0);
        assertEquals(2, occurrences(sql, " UNION ALL "), sql);
        assertTrue(sql.contains("\"value\" < 7"), sql);
        assertTrue(sql.contains("\"value\" = 7 AND time > " + CURSOR_TIME), sql);
        assertTrue(sql.contains("\"value\" = 7 AND time = " + CURSOR_TIME + " AND \"device\" < 'b'"), sql);
        assertTrue(sql.endsWith("ORDER BY \"value\" DESC, time ASC, \"device\" DESC LIMIT 3"), sql);
        assertFalse(sql.contains(" OR "), sql);
    }

    @Test
    void timeBoundsAndEscapedFiltersApplyToEveryUnionBranch() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setMeasurement("meter\"archive");
        query.setStartTime(0L);
        query.setEndTime(2000L);
        query.setCursorValues(Map.of("time", 1000L, "device", "O'Brien"));
        query.setFilters(List.of(new QueryFilter("region", OperatorEnum.EQ, List.of("O'Brien; --")),
                new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1, 9))));
        adapter.query(null, query);
        String sql = sql(0);
        assertEquals(2, occurrences(sql, "FROM \"meter\"\"archive\""), sql);
        assertEquals(2, occurrences(sql, "\"time\" >= timestamp '1970-01-01T00:00:00Z'"), sql);
        assertEquals(2, occurrences(sql, "\"time\" <= timestamp '1970-01-01T00:00:02Z'"), sql);
        assertEquals(2, occurrences(sql, "\"region\" = 'O''Brien; --'"), sql);
        assertEquals(2, occurrences(sql, "\"value\" BETWEEN 1 AND 9"), sql);
        assertTrue(sql.contains("\"device\" > 'O''Brien'"), sql);
    }

    @ParameterizedTest
    @EnumSource(SortOrderEnum.class)
    void cursorAtInclusiveRangeEdgeDropsOnlyImpossibleTimeBranch(SortOrderEnum order) throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setOrder(order);
        query.setStartTime(order == SortOrderEnum.DESC ? 1000L : 0L);
        query.setEndTime(order == SortOrderEnum.ASC ? 1000L : 2000L);
        adapter.query(null, query);
        String sql = sql(0);
        String compare = order == SortOrderEnum.ASC ? ">" : "<";
        assertFalse(sql.contains(" UNION ALL "), sql);
        assertFalse(sql.contains("time " + compare + " " + CURSOR_TIME), sql);
        assertTrue(sql.contains("time = " + CURSOR_TIME + " AND \"device\" " + compare + " 'b'"), sql);
    }

    @ParameterizedTest
    @EnumSource(SortOrderEnum.class)
    void cursorBeforeTraversalRangeDropsOnlyImpossibleEqualityBranch(SortOrderEnum order) throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setOrder(order);
        query.setStartTime(order == SortOrderEnum.ASC ? 1500L : 0L);
        query.setEndTime(order == SortOrderEnum.ASC ? 2000L : 500L);
        adapter.query(null, query);
        String sql = sql(0);
        String compare = order == SortOrderEnum.ASC ? ">" : "<";
        assertFalse(sql.contains(" UNION ALL "), sql);
        assertTrue(sql.contains("time " + compare + " " + CURSOR_TIME), sql);
        assertFalse(sql.contains("time = " + CURSOR_TIME), sql);
    }

    @ParameterizedTest
    @EnumSource(SortOrderEnum.class)
    void fieldFirstCursorAtRangeEdgeKeepsFieldBranchAndEqualTimeTagBranch(SortOrderEnum timeOrder) throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = fieldFirstCursor(timeOrder);
        query.setStartTime(timeOrder == SortOrderEnum.DESC ? 1000L : 0L);
        query.setEndTime(timeOrder == SortOrderEnum.ASC ? 1000L : 2000L);
        adapter.query(null, query);
        String sql = sql(0);
        String compare = timeOrder == SortOrderEnum.ASC ? ">" : "<";
        assertEquals(1, occurrences(sql, " UNION ALL "), sql);
        assertTrue(sql.contains("\"value\" < 7"), sql);
        assertFalse(sql.contains("\"value\" = 7 AND time " + compare + " " + CURSOR_TIME), sql);
        assertTrue(sql.contains("\"value\" = 7 AND time = " + CURSOR_TIME + " AND \"device\" > 'b'"), sql);
    }

    @ParameterizedTest
    @EnumSource(SortOrderEnum.class)
    void fieldFirstCursorBeyondRangeKeepsEarlierFieldBranch(SortOrderEnum timeOrder) throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = fieldFirstCursor(timeOrder);
        query.setStartTime(timeOrder == SortOrderEnum.DESC ? 1500L : 0L);
        query.setEndTime(timeOrder == SortOrderEnum.ASC ? 500L : 2000L);
        adapter.query(null, query);
        String sql = sql(0);
        String compare = timeOrder == SortOrderEnum.ASC ? ">" : "<";
        assertFalse(sql.contains(" UNION ALL "), sql);
        assertTrue(sql.contains("\"value\" < 7"), sql);
        assertFalse(sql.contains("time " + compare + " " + CURSOR_TIME), sql);
        assertFalse(sql.contains("time = " + CURSOR_TIME), sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-tag", "invalid-time", "null-field"})
    void pruningCannotHideInvalidCursorValues(String invalid) {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = fieldFirstCursor(SortOrderEnum.ASC);
        query.setStartTime(0L);
        query.setEndTime(500L);
        switch (invalid) {
            case "missing-tag" -> query.getCursorValues().remove("device");
            case "invalid-time" -> query.getCursorValues().put("time", "invalid timestamp");
            case "null-field" -> query.getCursorValues().put("value", null);
            default -> fail("Unknown case");
        }
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @EnumSource(SortOrderEnum.class)
    void allPrunedBranchesSendBoundedFalseQueryInsteadOfReturningLocally(SortOrderEnum order) throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setOrder(order);
        query.setStartTime(order == SortOrderEnum.DESC ? 1500L : 0L);
        query.setEndTime(order == SortOrderEnum.ASC ? 500L : 2000L);
        query.setOffset(2);
        query.setFilters(List.of(new QueryFilter("region", OperatorEnum.EQ, List.of("west"))));
        assertEquals(0, adapter.query(null, query).getRowCount());
        assertEquals(1, requests.size());
        String sql = sql(0);
        assertFalse(sql.contains("UNION"), sql);
        assertTrue(sql.contains("\"region\" = 'west' AND FALSE"), sql);
        assertTrue(sql.endsWith("ORDER BY time " + order + ", \"device\" " + order + " LIMIT 3 OFFSET 2"), sql);
    }

    @Test
    void reversedTimeBoundsAreRejectedBeforeCursorSqlGeneration() {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setStartTime(2000L);
        query.setEndTime(0L);
        query.getCursorValues().remove("device");
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
        query.getCursorValues().put("device", "b");
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void falseQueryStillChecksLifecycleAndPropagatesServerErrors() {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setEndTime(500L);
        replies.add(new Reply(403, "database access denied", false));
        assertEquals(TSDBErrorCodeEnum.PERMISSION_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(1, requests.size());
        adapter.close();
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(1, requests.size());
    }

    @Test
    void directProjectionKeepsSortKeysInternalAndDoesNotMutateQuery() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setSelectColumns(List.of("value"));
        String before = mapper.writeValueAsString(query);
        replies.add(new Reply(200, "[{\"time\":1001,\"value\":2}]", false));
        var result = adapter.query(null, query);
        String sql = sql(0);
        assertTrue(sql.startsWith("SELECT \"time\", \"value\" FROM ("), sql);
        assertTrue(sql.contains("\"device\""), sql);
        assertTrue(sql.endsWith("ORDER BY time ASC, \"device\" ASC LIMIT 3"), sql);
        assertEquals(before, mapper.writeValueAsString(query));
        assertFalse(result.getColumns().contains("device"));
        assertEquals(2, result.getRows().get(0).get("value"));
    }

    @Test
    void quotedProjectionAndSortKeysWithDifferentCaseRemainDistinct() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = fieldFirstCursor(SortOrderEnum.ASC);
        query.setSelectColumns(List.of("VALUE"));
        replies.add(new Reply(200, "[{\"time\":1001,\"VALUE\":99}]", false));
        var result = adapter.query(null, query);
        String sql = sql(0);
        assertTrue(sql.startsWith("SELECT \"time\", \"VALUE\" FROM ("), sql);
        assertEquals(3, occurrences(sql,
                "SELECT \"time\", \"VALUE\", \"value\", \"device\" FROM \"telemetry\""), sql);
        assertTrue(sql.endsWith("ORDER BY \"value\" DESC, time ASC, \"device\" ASC LIMIT 3"), sql);
        assertEquals(99, result.getRows().get(0).get("VALUE"));
        assertFalse(result.getColumns().contains("value"));
        assertEquals(List.of("VALUE"), query.getSelectColumns());
    }

    @ParameterizedTest
    @EnumSource(StrictCursorSqlStrategyEnum.class)
    void cursorKeysMustMatchExactPhysicalNamesBeforeHttp(StrictCursorSqlStrategyEnum strategy) {
        open(strategy);
        TSDBQuery query = cursor();
        query.setCursorValues(Map.of("_time", "1970-01-01T00:00:01Z", "DEVICE", "b"));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("cursorColumnModes")
    void paddedCursorKeysMatchCanonicalSqlWithoutChangingTheCaller(StrictCursorSqlStrategyEnum strategy,
                                                                 boolean explicitColumns) throws Exception {
        open(strategy);
        TSDBQuery query = cursor();
        if (!explicitColumns) query.setCursorColumns(List.of());
        TSDBQuery canonical = query.copy();
        query.setCursorValues(Map.of(" time ", 1000L, " device ", "b"));
        String before = mapper.writeValueAsString(query);
        adapter.query(null, canonical);
        adapter.query(null, query);
        assertEquals(sql(0), sql(1));
        assertEquals(before, mapper.writeValueAsString(query));
        assertEquals(Map.of(" time ", 1000L, " device ", "b"), query.getCursorValues());
        assertEquals(2, requests.size());
    }

    private static Stream<Arguments> cursorColumnModes() {
        return Stream.of(StrictCursorSqlStrategyEnum.values())
                .flatMap(strategy -> Stream.of(true, false).map(explicit -> Arguments.of(strategy, explicit)));
    }

    @ParameterizedTest
    @MethodSource("malformedCursorCases")
    void malformedRawCursorEntriesFailBeforeHttpWithoutChangingTheCaller(StrictCursorSqlStrategyEnum strategy,
                                                                       String invalid) {
        open(strategy);
        TSDBQuery query = cursor();
        Map<String, Object> values = new LinkedHashMap<>(query.getCursorValues());
        switch (invalid) {
            case "null-key" -> values.put(null, 1);
            case "blank-key" -> values.put(" \t ", 1);
            case "unicode-blank-key" -> values.put("\u2003", 1);
            case "null-value" -> values.put("device", null);
            case "trim-collision" -> values.put(" time ", 2000L);
            default -> fail("Unknown malformed cursor case");
        }
        query.setCursorValues(values);
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(values, query.getCursorValues());
        assertEquals(new ArrayList<>(values.keySet()), new ArrayList<>(query.getCursorValues().keySet()));
        assertTrue(requests.isEmpty());
        query.setStrictCursor(false);
        assertTrue(adapter.query(null, query).isSuccess());
        assertEquals(values, query.getCursorValues());
        assertEquals(1, requests.size());
    }

    private static Stream<Arguments> malformedCursorCases() {
        return Stream.of(StrictCursorSqlStrategyEnum.values())
                .flatMap(strategy -> Stream.of("null-key", "blank-key", "unicode-blank-key", "null-value", "trim-collision")
                        .map(invalid -> Arguments.of(strategy, invalid)));
    }

    @ParameterizedTest
    @EnumSource(StrictCursorSqlStrategyEnum.class)
    void caseDistinctCursorKeysKeepTheirOwnDirectionsAndValues(StrictCursorSqlStrategyEnum strategy) throws Exception {
        open(strategy);
        TSDBQuery query = cursor();
        query.setCursorColumns(List.of("value", "VALUE", "TIME", "time", "device"));
        query.setSortSpecs(List.of(new SortSpec("value", SortOrderEnum.DESC), new SortSpec("VALUE", SortOrderEnum.ASC),
                new SortSpec("TIME", SortOrderEnum.DESC), new SortSpec("time", SortOrderEnum.ASC),
                new SortSpec("device", SortOrderEnum.ASC)));
        query.setCursorValues(Map.of(" value ", 7, " VALUE ", 99, " TIME ", 42.5, " time ", 1000L, " device ", "b"));
        String before = mapper.writeValueAsString(query);
        adapter.query(null, query);
        String sql = sql(0);
        assertTrue(sql.contains("\"value\" < 7"), sql);
        assertTrue(sql.contains("\"VALUE\" > 99"), sql);
        assertTrue(sql.contains("\"TIME\" < 42.5"), sql);
        assertTrue(sql.contains("time > " + CURSOR_TIME), sql);
        assertTrue(sql.endsWith("ORDER BY \"value\" DESC, \"VALUE\" ASC, \"TIME\" DESC, time ASC, \"device\" ASC LIMIT 3"), sql);
        assertEquals(before, mapper.writeValueAsString(query));
    }

    @ParameterizedTest
    @EnumSource(StrictCursorSqlStrategyEnum.class)
    void directCaseMismatchedSortIsRejectedBeforeHttp(StrictCursorSqlStrategyEnum strategy) {
        open(strategy);
        TSDBQuery query = cursor();
        query.setSortSpecs(List.of(new SortSpec("TIME", SortOrderEnum.ASC)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @EnumSource(StrictCursorSqlStrategyEnum.class)
    void conflictingDuplicateSortDirectionsAreRejectedBeforeHttp(StrictCursorSqlStrategyEnum strategy) {
        open(strategy);
        TSDBQuery query = cursor();
        query.setSortSpecs(List.of(new SortSpec("device", SortOrderEnum.ASC), new SortSpec("device", SortOrderEnum.DESC)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void initialPageAndLegacyTimeFallbackAvoidUnion() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setCursorValues(Map.of());
        adapter.query(null, query);
        assertFalse(sql(0).contains("UNION"));
        assertFalse(sql(0).contains(" WHERE "));
        query.setCursorTime(1000L);
        adapter.query(null, query);
        assertFalse(sql(1).contains("UNION"));
        assertTrue(sql(1).contains("\"time\" > " + CURSOR_TIME), sql(1));
    }

    @Test
    void ordinaryQueryIgnoresCompositeValuesAndKeepsOriginalSql() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setStrictCursor(false);
        query.setCursorTime(1000L);
        adapter.query(null, query);
        assertEquals("SELECT * FROM \"telemetry\" WHERE \"time\" > " + CURSOR_TIME
                + " ORDER BY \"time\" ASC LIMIT 3", sql(0));
    }

    @Test
    void countStillIgnoresCursorOrderingAndPaging() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setOffset(99);
        query.setFilters(List.of(new QueryFilter("region", OperatorEnum.EQ, List.of("west"))));
        replies.add(new Reply(200, "[{\"total\":42}]", false));
        assertEquals(42, adapter.count(null, query));
        assertEquals("SELECT COUNT(*) AS total FROM \"telemetry\" WHERE \"region\" = 'west'", sql(0));
    }

    @Test
    void nativeSqlIsNeverRewritten() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        String nativeSql = "SELECT * FROM telemetry WHERE time > timestamp '2026-01-01' OR value > 2 LIMIT 3";
        adapter.executeQuery(nativeSql);
        assertEquals(nativeSql, sql(0));
    }

    @ParameterizedTest
    @EnumSource(StrictCursorSqlStrategyEnum.class)
    void http500DoesNotRetryOrSwitchStrategy(StrictCursorSqlStrategyEnum strategy) throws Exception {
        open(strategy);
        replies.add(new Reply(500, "unable to analyze provided filters for a boundary on the time column", false));
        replies.add(new Reply(200, "[]", false));
        assertEquals(TSDBErrorCodeEnum.CONNECTION_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, cursor())).getErrorCode());
        assertEquals(1, requests.size());
        assertEquals(1, replies.size());
        assertEquals(strategy == StrictCursorSqlStrategyEnum.UNION_ALL, sql(0).contains("UNION ALL"));
    }

    @Test
    void globalRowLimitStillRejectsOversizedUnionResult() {
        properties.setMaxQueryRows(2);
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        replies.add(new Reply(200, "[{\"value\":1},{\"value\":2},{\"value\":3}]", false));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, cursor())).getErrorCode());
    }

    @Test
    void paginationProbeAllowsExactlyOneExtraRow() {
        properties.setMaxQueryRows(2);
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setPaginationProbe(true);
        replies.add(new Reply(200, "[{\"value\":1},{\"value\":2},{\"value\":3}]", false));
        assertEquals(3, adapter.query(null, query).getRowCount());
        replies.add(new Reply(200, "[{\"value\":1},{\"value\":2},{\"value\":3},{\"value\":4}]", false));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void responseByteLimitStillBoundsUnionResults(boolean chunked) {
        properties.setMaxQueryResponseBytes(32);
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        replies.add(new Reply(200, "[{\"label\":\"" + "x".repeat(80) + "\"}]", chunked));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, cursor())).getErrorCode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "null", "time-text", "time-fraction", "sort-outside-cursor"})
    void invalidCursorsFailBeforeHttp(String invalid) {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        switch (invalid) {
            case "missing" -> query.getCursorValues().remove("device");
            case "null" -> query.getCursorValues().put("device", null);
            case "time-text" -> query.getCursorValues().put("time", "invalid timestamp");
            case "time-fraction" -> query.getCursorValues().put("time", new BigDecimal("1000.5"));
            case "sort-outside-cursor" -> query.setSortSpecs(List.of(new SortSpec("value", SortOrderEnum.ASC)));
            default -> fail("Unknown case");
        }
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unionStrategyRejectsStrictAggregatesOnInitialAndLaterPages(boolean hasCursor) {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        if (!hasCursor) query.setCursorValues(Map.of());
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertTrue(requests.isEmpty());
    }

    @Test
    void ordinaryAggregateQueryStillUsesSingleSelect() throws Exception {
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TSDBQuery query = cursor();
        query.setStrictCursor(false);
        query.setGroupByTags(List.of("device"));
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        adapter.query(null, query);
        assertTrue(sql(0).contains("SUM(\"value\") AS \"total\""), sql(0));
        assertTrue(sql(0).contains("GROUP BY \"device\""), sql(0));
        assertFalse(sql(0).contains("UNION"));
    }

    @Test
    void templatePreservesPageProbeProjectionAndSameTimeNextCursor() throws Exception {
        properties.setMaxQueryRows(2);
        open(StrictCursorSqlStrategyEnum.UNION_ALL);
        TGTemplate template = new TGTemplate(adapter);
        replies.add(new Reply(200, "[{\"time\":1000,\"device\":\"a\",\"value\":1},"
                + "{\"time\":1000,\"device\":\"b\",\"value\":2},"
                + "{\"time\":1000,\"device\":\"c\",\"value\":3}]", false));
        PageResult<Telemetry> first = template.query(Telemetry.class).select("value")
                .orderByTimeAsc().limit(2).strictCursorPage();
        assertEquals(List.of("a", "b"), first.getRows().stream().map(row -> row.device).toList());
        assertTrue(first.isHasNext());
        assertEquals(Map.of("time", 1000L, "device", "b"), first.getNextCursor());
        assertFalse(sql(0).contains("UNION"));
        assertTrue(sql(0).contains("\"device\""));
        assertTrue(sql(0).endsWith("LIMIT 3"));

        replies.add(new Reply(200, "[{\"time\":1000,\"device\":\"c\",\"value\":3}]", false));
        PageResult<Telemetry> next = template.query(Telemetry.class).select("value")
                .orderByTimeAsc().cursor(first.getNextCursor()).limit(2).strictCursorPage();
        assertEquals(List.of("c"), next.getRows().stream().map(row -> row.device).toList());
        assertFalse(next.isHasNext());
        assertTrue(next.getNextCursor().isEmpty());
        assertTrue(sql(1).contains(" UNION ALL "));
        assertTrue(sql(1).endsWith("LIMIT 3"));
        assertEquals(2, requests.size());
    }

    private void open(StrictCursorSqlStrategyEnum strategy) {
        properties.setStrictCursorSql(strategy);
        adapter = new InfluxDBAdapter(properties, false);
        adapter.init();
    }

    private TSDBQuery cursor() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("telemetry");
        query.setStrictCursor(true);
        query.setCursorColumns(List.of("time", "device"));
        query.setCursorValues(Map.of("time", 1000L, "device", "b"));
        query.setOrder(SortOrderEnum.ASC);
        query.setLimit(3);
        return query;
    }

    private TSDBQuery fieldFirstCursor(SortOrderEnum timeOrder) {
        TSDBQuery query = cursor();
        query.setCursorColumns(List.of("value", "time", "device"));
        query.setSortSpecs(List.of(new SortSpec("value", SortOrderEnum.DESC),
                new SortSpec("time", timeOrder), new SortSpec("device", SortOrderEnum.ASC)));
        query.setCursorValues(Map.of("value", 7, "time", 1000L, "device", "b"));
        return query;
    }

    private String sql(int index) throws Exception {
        return mapper.readTree(requests.get(index)).get("q").asText();
    }

    private static int occurrences(String text, String fragment) {
        return (text.length() - text.replace(fragment, "").length()) / fragment.length();
    }

    private record Reply(int status, String body, boolean chunked) { }

    @TGMeasurement("telemetry")
    public static class Telemetry {
        @TGTime public Long time;
        @TGTag public String device;
        @TGField public Double value;
        public Telemetry() { }
    }
}
