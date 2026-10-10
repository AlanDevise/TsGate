package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.InfluxDB1Adapter;
import com.alandevise.tsgate.annotation.*;
import com.alandevise.tsgate.config.*;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.*;
import com.alandevise.tsgate.model.*;
import org.junit.jupiter.api.*;
import org.influxdb.dto.Query;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class InfluxDB1DockerIT implements com.alandevise.tsgate.contract.BackendSemanticsContract {
    @Override public com.alandevise.tsgate.adapter.TSDBAdapter semanticsAdapter() { return adapter; }
    @Override public com.alandevise.tsgate.core.TGQueryBuilder<?> semanticsQuery() { return template.query(Point.class); }
    @Override public String semanticsMeasurement() { return "points"; }
    @Override public long semanticsTime() { return base; }
    @Override public void awaitSemanticsVisibility(List<TSDBRecord> expected) {
        // This HTTP fixture tests acknowledged writes against the configured InfluxDB 1.x version.
    }
    @Override public void configureSemanticsRowLimit(int rows) { initialize(rows, 16L * 1024 * 1024); }

    static final String URL = System.getProperty("tsdb.it.influxdb1.url", "http://127.0.0.1:18086");
    InfluxDB1Adapter adapter;
    TGTemplate template;
    String database;
    long base;
    HttpClient admin;

    @BeforeEach
    void setup() throws Exception {
        database = "influx1_" + Long.toUnsignedString(System.nanoTime(), 36);
        base = System.currentTimeMillis() - 60_000;
        admin = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        executeAdmin("CREATE DATABASE \"" + database + "\"");
        initialize(10000, 16L * 1024 * 1024);
    }

    void initialize(int rows, long bytes) {
        initialize(rows, bytes, 64L * 1024 * 1024);
    }

    void initialize(int rows, long bytes, long batchBytes) {
        initialize(rows, bytes, batchBytes, 10_000);
    }

    private void initialize(int rows, long bytes, long batchBytes, int maxBatchRecords) {
        if (adapter != null) adapter.close();
        InfluxDB1Properties config = new InfluxDB1Properties();
        config.setUrl(URL);
        config.setDatabase(database);
        config.setMaxQueryRows(rows);
        config.setMaxQueryResponseBytes(bytes);
        config.setMaxBatchBytes(batchBytes);
        config.setMaxBatchRecords(maxBatchRecords);
        InfluxDB1HttpClientProperties http = new InfluxDB1HttpClientProperties();
        http.setCallTimeoutMs(15000);
        adapter = new InfluxDB1Adapter(config, http, false);
        adapter.init();
        template = new TGTemplate(adapter);
    }

    @AfterEach
    void cleanup() throws Exception {
        if (adapter != null) adapter.close();
        if (admin != null && database != null) executeAdmin("DROP DATABASE \"" + database + "\"");
    }

    void executeAdmin(String sql) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(URL + "/query"))
                .timeout(Duration.ofSeconds(15)).header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8))).build();
        HttpResponse<String> response = admin.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        assertFalse(response.body().contains("\"error\""), response.body());
    }

    TSDBRecord point(long time, String device, Object value) {
        return new TSDBRecord("points", time, Map.of("device", device), Map.of("value", value));
    }

    TSDBQuery detail() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        query.setOrder(SortOrderEnum.ASC);
        return query;
    }

    void seed() {
        assertTrue(adapter.batchWrite(null, List.of(point(base, "a", 1d), point(base + 1000, "b", 2d), point(base + 2000, "a", 3d), point(base + 3000, "b", 4d))));
    }

    @Test
    void directBatchRecordLimitRejectsBeforeCopyAndPreservesReadback() {
        initialize(10000, 16L * 1024 * 1024, 64L * 1024 * 1024, 3);
        TSDBRecord existing = point(base, "existing", 0d);
        assertTrue(adapter.write(null, existing));
        Collection<TSDBRecord> oversized = new AbstractCollection<>() {
            @Override public int size() { return adapter.getMaxBatchRecords() + 1; }
            @Override public Iterator<TSDBRecord> iterator() {
                throw new AssertionError("Oversized batch must be rejected before traversal");
            }
        };
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, oversized));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(4, failure.getResult().requestedRecords());
        assertEquals(0, failure.getResult().validatedRecords());
        assertEquals(0, failure.getResult().committedRecords());
        assertEquals(0, failure.getResult().totalBatches());
        assertEquals(List.of(0d), adapter.query(null, detail()).getRows().stream()
                .map(row -> ((Number) row.get("value")).doubleValue()).toList());
        assertEquals(1, adapter.count(null, detail()));

        List<TSDBRecord> boundary = List.of(point(base + 1, "next", 1d),
                point(base + 2, "next", 2d), point(base + 3, "next", 3d));
        assertEquals(adapter.getMaxBatchRecords(), boundary.size());
        BatchWriteResult result = adapter.batchWriteDetailed(null, boundary);
        assertTrue(result.isSuccess());
        assertEquals(3, result.committedRecords());
        assertEquals(List.of(0d, 1d, 2d, 3d), adapter.query(null, detail()).getRows().stream()
                .map(row -> ((Number) row.get("value")).doubleValue()).toList());
        assertEquals(4, adapter.count(null, detail()));
    }

    @Test
    void sharedFilterRulesRejectMalformedInputsAndPreserveReadback() {
        seed();
        List<QueryFilter> valid = List.of(
                new QueryFilter("value", OperatorEnum.BETWEEN, List.of(2d, 3d)),
                new QueryFilter("value", OperatorEnum.EQ, List.of(3d)),
                new QueryFilter("value", OperatorEnum.IN, List.of(1d, 4d)));
        List<List<Double>> expected = List.of(List.of(2d, 3d), List.of(3d), List.of(1d, 4d));
        for (int index = 0; index < valid.size(); index++) {
            TSDBQuery filtered = detail();
            filtered.setFilters(List.of(valid.get(index)));
            List<Double> values = adapter.query(null, filtered).getRows().stream()
                    .map(row -> ((Number) row.get("value")).doubleValue()).toList();
            assertEquals(expected.get(index), values);
            assertEquals(expected.get(index).size(), adapter.count(null, filtered));
        }
        List<QueryFilter> invalid = List.of(
                new QueryFilter("value", OperatorEnum.EQ, List.of(1d, 2d)),
                new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1d)),
                new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1d, 2d, 3d)),
                new QueryFilter("value", OperatorEnum.IN, List.of()),
                new QueryFilter("value", OperatorEnum.EQ, Collections.singletonList(null)),
                new QueryFilter("value", OperatorEnum.IN, Arrays.asList(1d, null, 2d)),
                new QueryFilter("value", OperatorEnum.GT, List.of(Double.NaN)),
                new QueryFilter("value", OperatorEnum.LE, List.of(Double.POSITIVE_INFINITY)));
        for (QueryFilter filter : invalid) {
            TSDBQuery filtered = detail();
            filtered.setFilters(List.of(filter));
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    assertThrows(TSDBException.class, () -> adapter.query(null, filtered)).getErrorCode());
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    assertThrows(TSDBException.class, () -> adapter.count(null, filtered)).getErrorCode());
        }
        assertEquals(4, adapter.query(null, detail()).getRowCount());
        assertEquals(4, adapter.count(null, detail()));
    }

    @Test
    void officialLatestServerVersionIsV113() {
        assertTrue(adapter.getNativeClient().version().startsWith("1.13."), adapter.getNativeClient().version());
    }

    @Test
    void encodedBatchBudgetRejectsBeforeWritingAndAllowsConfiguredReadback() {
        TSDBRecord first = point(base, "existing", 1d);
        TSDBRecord second = point(base + 1, "next", 2d);
        assertTrue(adapter.write(null, first));
        initialize(10000, 16L * 1024 * 1024, 1);
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(second)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(0, failure.getResult().committedRecords());
        assertEquals(1, adapter.count(null, detail()));
        initialize(10000, 16L * 1024 * 1024, 4096);
        assertTrue(adapter.write(null, second));
        assertEquals(2, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void aggregateOutputCollisionsFailWhileDistinctAliasesRoundTrip() {
        seed();
        TSDBQuery query = detail();
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.MAX, "duplicate"),
                new AggregationSpec("value", AggregationFunctionEnum.MIN, "duplicate")));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.count(null, query)).getErrorCode());
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.MAX, "maximum"),
                new AggregationSpec("value", AggregationFunctionEnum.MIN, "minimum")));
        Map<String, Object> row = adapter.query(null, query).getRows().get(0);
        assertEquals(4d, ((Number) row.get("maximum")).doubleValue());
        assertEquals(1d, ((Number) row.get("minimum")).doubleValue());
        assertEquals(1, adapter.count(null, query));
        assertEquals(4, adapter.count(null, detail()));
    }

    @Test
    void nativeClientAndTemplateReadIdenticalData() {
        assertTrue(template.batchWrite(List.of(new Point(base, "a", 1.25), new Point(base + 1000, "b", 2.5))));
        List<Point> rows = template.query(Point.class).orderByTimeAsc().list();
        assertEquals(2, rows.size());
        assertEquals(base, rows.get(0).time);
        assertEquals(1.25, rows.get(0).value);
        org.influxdb.dto.QueryResult nativeRows = adapter.getNativeClient().query(new Query("SELECT value FROM points ORDER BY time", database));
        assertFalse(nativeRows.hasError(), nativeRows.getError());
        assertEquals(2, nativeRows.getResults().get(0).getSeries().get(0).getValues().size());
    }

    @Test
    void propertyChangesCannotRedirectAdapterOrNativeWrites() {
        adapter.close();
        InfluxDB1Properties properties = new InfluxDB1Properties();
        properties.setUrl(URL);
        properties.setDatabase(database);
        InfluxDB1HttpClientProperties http = new InfluxDB1HttpClientProperties();
        http.setCallTimeoutMs(15000);
        adapter = new InfluxDB1Adapter(properties, http, false);
        properties.setUrl("http://127.0.0.1:1");
        properties.setDatabase("missing_changed_database");
        properties.setUsername("unused_changed_user");
        properties.setPassword("unused_changed_password");
        properties.setRetentionPolicy("missing_changed_policy");
        http.setReadTimeoutMs(-1);
        adapter.init();
        properties.setDatabase("another_missing_database");
        properties.setRetentionPolicy("another_missing_policy");
        assertTrue(adapter.write(null, point(base, "adapter", 1d)));
        adapter.getNativeClient().write("points,device=native value=2.0 " + ((base + 1) * 1_000_000L));
        assertEquals(2, adapter.query(null, detail()).getRowCount());
        org.influxdb.dto.QueryResult nativeRows = adapter.getNativeClient().query(new Query("SELECT value FROM points", database));
        assertFalse(nativeRows.hasError(), nativeRows.getError());
        assertEquals(2, nativeRows.getResults().get(0).getSeries().get(0).getValues().size());
        TSDBQuery invalid = detail();
        invalid.setLimit(0);
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, invalid)).getErrorCode());
        assertEquals(2, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void timeCursorAndOffsetPagesRespectRowCountSemantics() {
        seed();
        PageResult<Point> first = template.query(Point.class).orderByTimeAsc().limit(2).page();
        assertTrue(first.isHasNext());
        assertEquals(2, first.getRows().size());
        PageResult<Point> second = template.query(Point.class).orderByTimeAsc().cursorTime(first.getNextCursorTime()).limit(2).page();
        assertFalse(second.isHasNext());
        assertEquals(3d, second.getRows().get(0).value);
        PageResult<Point> offset = template.query(Point.class).orderByTimeAsc().page(2, 2);
        assertEquals(4L, offset.getTotal());
        assertEquals(2, offset.getRows().size());
        assertEquals(3d, offset.getRows().get(0).value);
    }

    @Test
    void sparseFieldsCountLogicalRowsRatherThanNonNullFields() {
        assertTrue(adapter.batchWrite(null, List.of(new TSDBRecord("points", base, Map.of(), Map.of("a", 1)),
                new TSDBRecord("points", base + 1000, Map.of(), Map.of("b", 2)),
                new TSDBRecord("points", base + 2000, Map.of(), Map.of("a", 3, "b", 4)))));
        assertEquals(3, adapter.count(null, detail()));
    }

    @Test
    void groupingTagsAndMeanUseInfluxQlSeriesTags() {
        seed();
        TSDBQuery query = detail();
        query.setGroupByTags(List.of("device"));
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.AVG, "mean")));
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(0).get("device"));
        assertEquals(2d, ((Number) rows.get(0).get("mean")).doubleValue());
        assertEquals(3d, ((Number) rows.get(1).get("mean")).doubleValue());
        assertEquals(2, adapter.count(null, query));
    }

    @Test
    void fixedOffsetTimeWindowAndCountWork() {
        seed();
        TSDBQuery query = detail();
        query.setStartTime(base - 3600000);
        query.setEndTime(base + 10000);
        query.setGroupByTime("1h");
        query.setTimeZone("+08:00");
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "sum")));
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(10d, rows.stream().mapToDouble(row -> ((Number) row.get("sum")).doubleValue()).sum());
        assertTrue(rows.stream().allMatch(row -> row.get("window_start") instanceof Long));
        assertEquals(rows.size(), adapter.count(null, query));
    }

    @Test
    void inBetweenAndEscapedStringFiltersRoundTrip() {
        assertTrue(adapter.write(null, point(base, "O'Brien", 1d)));
        assertTrue(adapter.write(null, point(base + 1000, "other", 2d)));
        TSDBQuery query = detail();
        query.getFilters().add(new QueryFilter("device", OperatorEnum.IN, List.of("O'Brien", "absent")));
        query.getFilters().add(new QueryFilter("value", OperatorEnum.BETWEEN, List.of(0, 3)));
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(1, rows.size());
        assertEquals("O'Brien", rows.get(0).get("device"));
    }

    @Test
    void actualPartialWriteReportsUnknownInsteadOfFalseZeroCommit() {
        assertTrue(adapter.write(null, point(base, "a", 1d)));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class, () -> adapter.batchWriteDetailed(null,
                List.of(point(base + 1000, "a", 2d), point(base + 2000, "a", "conflicting-type"))));
        assertEquals(BatchCommitStateEnum.UNKNOWN, error.getResult().commitState());
        assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, error.getErrorCode());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(2, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void rawAndOrdinaryQueriesHaveStrictCapWhilePagesCanProbe() {
        seed();
        initialize(2, 4096);
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM points LIMIT 3")).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class, () -> template.query(Point.class).limit(3).list()).getErrorCode());
        PageResult<Point> page = template.query(Point.class).orderByTimeAsc().limit(2).page();
        assertEquals(2, page.getRows().size());
        assertTrue(page.isHasNext());
        assertEquals(4, adapter.count(null, detail()));
    }

    @Test
    void responseByteLimitFailsWithoutPartialSuccess() {
        seed();
        initialize(100, 40);
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, assertThrows(TSDBException.class, () -> adapter.query(null, detail())).getErrorCode());
    }

    @Test
    void unsupportedDialectContractsAreExplicit() {
        seed();
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class,
                () -> template.query(Point.class).orderByFieldAsc("value").list()).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class,
                () -> template.query(Point.class).cursorColumns("time", "device").limit(2).strictCursorPage()).getErrorCode());
    }

    @Test
    void emptyWritesAndPreflightFailuresDoNotCommit() {
        assertTrue(adapter.batchWriteDetailed(null, List.of()).isSuccess());
        TSDBRecord invalid = new TSDBRecord("bad\nmeasurement", base, Map.of(), Map.of("value", 1));
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(base, "a", 1d), invalid)));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(0, adapter.query(null, detail()).getRowCount());
    }

    @Test
    void measurementEqualsAndBackslashesRoundTripWithoutRenamingData() {
        String measurement = "room=west\\zone";
        String tag = "C:\\path";
        TSDBRecord point = new TSDBRecord(measurement, base, Map.of("device\\path", tag), Map.of("field\\name", 7));
        assertTrue(adapter.write(null, point));
        TSDBQuery query = detail();
        query.setMeasurement(measurement);
        query.getFilters().add(new QueryFilter("device\\path", OperatorEnum.EQ, List.of(tag)));
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(1, rows.size());
        assertEquals(tag, rows.get(0).get("device\\path"));
        assertEquals(7, ((Number) rows.get(0).get("field\\name")).intValue());
    }

    @Test
    void bigIntegerRetainsEveryBitAndUnrepresentableNumbersFailPreflight() {
        assertTrue(adapter.write(null, point(base, "a", new java.math.BigInteger("9007199254740993"))));
        assertEquals(9007199254740993L, ((Number) adapter.query(null, detail()).getRows().get(0).get("value")).longValue());
        for (Number invalid : List.of(new java.math.BigInteger("9223372036854775808"), new java.math.BigDecimal("0.1"))) {
            TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                    () -> adapter.batchWriteDetailed(null, List.of(point(base + 1000, "a", 2L), point(base + 2000, "a", invalid))));
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        }
        assertEquals(1, adapter.count(null, detail()));
    }

    @Test
    void selectorGroupingUsesTagsForGlobalPagingDespiteDifferentSourceTimes() {
        assertTrue(adapter.batchWrite(null, List.of(point(base + 1000, "a", 2d), point(base, "b", 1d))));
        TSDBQuery query = detail();
        query.setGroupByTags(List.of("device"));
        query.setLimit(1);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.MIN, "minimum")));
        assertEquals("a", adapter.query(null, query).getRows().get(0).get("device"));
        query.setOffset(1);
        assertEquals("b", adapter.query(null, query).getRows().get(0).get("device"));
    }

    @Test
    void actualTimeAliasFieldSurvivesQueryNormalization() {
        assertTrue(adapter.write(null, new TSDBRecord("points", base, Map.of(), Map.of("value", 1d, "_time", 123L))));
        Map<String, Object> row = adapter.query(null, detail()).getRows().get(0);
        assertEquals(base, row.get("time"));
        assertEquals(123L, ((Number) row.get("_time")).longValue());
    }

    @Test
    void futureRowsAreIncludedOnlyWithExplicitWindowBounds() {
        long future = System.currentTimeMillis() + 7200000;
        assertTrue(adapter.write(null, point(future, "a", 7d)));
        TSDBQuery query = detail();
        query.setGroupByTime("1h");
        query.setStartTime(future - 1000);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        query.setEndTime(future + 1000);
        List<Map<String, Object>> rows = adapter.query(null, query).getRows();
        assertEquals(1, rows.size());
        assertEquals(7d, ((Number) rows.get(0).get("total")).doubleValue());
    }

    @Test
    void unsafeNamesAndTagOnlyProjectionAreRejectedBeforeDataLoss() {
        assertTrue(adapter.write(null, point(base, "a", 1d)));
        TSDBRecord comment = new TSDBRecord("#sensor", base, Map.of(), Map.of("value", 1d));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(base + 1000, "b", 2d), comment))).getResult().commitState());
        for (String key : List.of("_field", "_measurement", "time")) {
            TSDBRecord invalid = new TSDBRecord("points", base + 1000, Map.of(key, "reserved"), Map.of("value", 2d));
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                    () -> adapter.batchWriteDetailed(null, List.of(invalid))).getResult().commitState());
        }
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, assertThrows(TSDBException.class,
                () -> template.query(Point.class).select("device").list()).getErrorCode());
        assertEquals(1, adapter.count(null, detail()));
        assertEquals("a", template.query(Point.class).select("device", "value").list().get(0).device);
    }

    @Test
    void nativeMutationsAndMultipleStatementsAreRejectedWithoutChangingData() {
        assertTrue(adapter.write(null, point(base, "a", 1d)));
        for (String sql : List.of("DROP DATABASE \"" + database + "\"", "DROP MEASUREMENT points", "DELETE FROM points",
                "SELECT * INTO copied FROM points", "SELECT * FROM points; DROP MEASUREMENT points",
                "SELECT * FROM points; SELECT * INTO copied FROM points")) {
            assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                    assertThrows(TSDBException.class, () -> adapter.executeQuery(sql)).getErrorCode());
            assertEquals(1, adapter.executeQuery("SELECT * FROM points").getRowCount());
        }
        List<Map<String, Object>> measurements = adapter.executeQuery("SHOW MEASUREMENTS").getRows();
        assertEquals(1, measurements.size());
        assertEquals("points", measurements.get(0).get("name"));
    }

    @Test
    void nativeReadOnlyQueryAllowsQuotedIntoAndSemicolons() {
        String value = "INTO; O'Brien";
        assertTrue(adapter.write(null, new TSDBRecord("points", base, Map.of("device", "a"), Map.of("value", 1d, "INTO", value))));
        List<Map<String, Object>> rows = adapter.executeQuery("SELECT \"INTO\" FROM \"points\" WHERE \"INTO\" = 'INTO; O\\'Brien';").getRows();
        assertEquals(1, rows.size());
        assertEquals(value, rows.get(0).get("INTO"));
    }

    @TGMeasurement("points")
    public static class Point {
        @TGTime
        public long time;
        @TGTag
        public String device;
        @TGField
        public double value;

        public Point() {
        }

        Point(long time, String device, double value) {
            this.time = time;
            this.device = device;
            this.value = value;
        }
    }
}
