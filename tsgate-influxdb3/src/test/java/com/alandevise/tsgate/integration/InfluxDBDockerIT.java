package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsgate.config.InfluxDBProperties;
import com.alandevise.tsgate.config.StrictCursorSqlStrategyEnum;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.*;
import com.alandevise.tsgate.model.*;
import org.junit.jupiter.api.*;
import java.net.URI;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.http.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class InfluxDBDockerIT extends DatabaseContractIT {
    static final String URL=System.getProperty("tsdb.it.influxdb.url","http://127.0.0.1:18181");
    HttpClient admin;
    @BeforeEach void setup() throws Exception {
        db="it_influx_"+Long.toUnsignedString(System.nanoTime(),36); admin=HttpClient.newHttpClient();
        request("POST","/api/v3/configure/database","{\"db\":\""+db+"\"}");
        InfluxDBProperties config=new InfluxDBProperties(); config.setUrl(URL); config.setDatabase(db); config.setMaxBatchRecords(100);
        config.setStrictCursorSql(StrictCursorSqlStrategyEnum.valueOf(System.getProperty("tsdb.it.influxdb.strict-cursor-sql", "OR").replace('-', '_').toUpperCase(Locale.ROOT)));
        adapter=new InfluxDBAdapter(config,false); adapter.init(); template=new TGTemplate(adapter);
    }
    @AfterEach void cleanup() throws Exception { if(adapter!=null) adapter.close(); if(admin!=null) { request("DELETE","/api/v3/configure/database?db="+db,null); } }
    void request(String method,String path,String body) throws Exception {
        HttpRequest r=HttpRequest.newBuilder(URI.create(URL+path)).header("Content-Type","application/json").method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response=admin.send(r,HttpResponse.BodyHandlers.ofString()); assertTrue(response.statusCode()/100==2,response.statusCode()+" "+response.body());
    }
    @Test void encodedBatchBudgetRejectsBeforeWritingAndAllowsConfiguredReadback() {
        assertTrue(adapter.write(null, record(BASE, "existing", 1)));
        initializeBatchBudget(1);
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(record(BASE + 1, "next", 2))));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(0, failure.getResult().committedRecords());
        assertEquals(1, adapter.count(null, query()));
        initializeBatchBudget(4096);
        assertTrue(adapter.write(null, record(BASE + 1, "next", 2)));
        assertEquals(2, rows(query()).size());
    }

    private void initializeBatchBudget(long bytes) {
        adapter.close();
        InfluxDBProperties config = new InfluxDBProperties();
        config.setUrl(URL);
        config.setDatabase(db);
        config.setMaxBatchBytes(bytes);
        config.setStrictCursorSql(StrictCursorSqlStrategyEnum.valueOf(System.getProperty(
                "tsdb.it.influxdb.strict-cursor-sql", "OR").replace('-', '_').toUpperCase(Locale.ROOT)));
        adapter = new InfluxDBAdapter(config, false);
        adapter.init();
        template = new TGTemplate(adapter);
    }

    @Test void aggregateOutputCollisionsFailWhileDistinctAliasesRoundTrip() {
        assertTrue(adapter.batchWrite(null, List.of(record(BASE, "a", 1), record(BASE + 1, "b", 4))));
        TSDBQuery query = query();
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
        assertEquals(2, adapter.count(null, query()));
    }

    @Test void lineProtocolRejectsNewlineBeforeCommittingAnyRow() {
        TSDBRecord invalid=new TSDBRecord("telemetry",BASE+1,Map.of("device","bad\ntag"),Map.of("value",1d));
        TSDBBatchWriteException error=assertThrows(TSDBBatchWriteException.class,()->adapter.batchWriteDetailed(null,List.of(record(BASE,"a",1),invalid)));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED,error.getResult().commitState()); assertEquals(0,error.getResult().committedRecords());
    }
    @Test void nativeArrowClientQueriesSameData() {
        assertTrue(adapter.write(null,record(BASE,"a",1)));
        try(var stream=((InfluxDBAdapter)adapter).getNativeClient().query("SELECT value FROM telemetry")) { assertEquals(1,stream.count()); }
    }

    @Test void propertyChangesCannotRedirectAdapterOrNativeWrites() {
        adapter.close();
        InfluxDBProperties properties = new InfluxDBProperties();
        properties.setUrl(URL);
        properties.setDatabase(db);
        properties.setStrictCursorSql(StrictCursorSqlStrategyEnum.valueOf(System.getProperty(
                "tsdb.it.influxdb.strict-cursor-sql", "OR").replace('-', '_').toUpperCase(Locale.ROOT)));
        InfluxDBAdapter snapshot = new InfluxDBAdapter(properties, false);
        adapter = snapshot;
        properties.setUrl("http://127.0.0.1:1");
        properties.setDatabase("missing_changed_database");
        properties.setToken("unused_changed_token");
        properties.getHttpClient().setReadTimeoutMs(-1);
        snapshot.init();
        properties.setDatabase("another_missing_database");
        properties.setHttpClient(null);
        assertTrue(snapshot.write(null, record(BASE, "adapter", 1)));
        snapshot.getNativeClient().writeRecord("telemetry,device=native value=2.0 " + (BASE + 1));
        assertEquals(2, rows(query()).size());
        try (var stream = snapshot.getNativeClient().query("SELECT value FROM telemetry")) {
            assertEquals(2, stream.count());
        }
        TSDBQuery invalid = query();
        invalid.setLimit(0);
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> snapshot.query(null, invalid)).getErrorCode());
        assertEquals(2, rows(query()).size());
        TSDBQuery strict = query();
        strict.setStrictCursor(true);
        strict.setCursorColumns(List.of("time", "device"));
        strict.setCursorValues(Map.of(" time ", BASE, " device ", "adapter"));
        assertEquals("native", snapshot.query(null, strict).getRows().get(0).get("device"));
        assertEquals(Map.of(" time ", BASE, " device ", "adapter"), strict.getCursorValues());
    }
    @Test void schemaConflictIsRejectedWithZeroConfirmedCommits() {
        assertTrue(adapter.write(null,record(BASE,"a",1)));
        TSDBRecord invalid=new TSDBRecord("telemetry",BASE+1,Map.of("device","a"),Map.of("value","wrong-type"));
        TSDBBatchWriteException error=assertThrows(TSDBBatchWriteException.class,()->adapter.batchWriteDetailed(null,List.of(invalid)));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED,error.getResult().commitState()); assertEquals(0,error.getResult().committedRecords()); assertEquals(1,rows(query()).size());
    }

    @Test void springDstDayGroupsTwentyThreeRealHours() {
        calendarDays("2026-03-08T05:00:00Z", "2026-03-09T04:00:00Z", "2026-03-10T04:00:00Z",
                List.of("2026-03-08T05:30:00Z", "2026-03-08T07:30:00Z", "2026-03-09T04:30:00Z"),
                3d, 3d);
    }

    @Test void fallDstDayKeepsBothRepeatedHours() {
        calendarDays("2026-11-01T04:00:00Z", "2026-11-02T05:00:00Z", "2026-11-03T05:00:00Z",
                List.of("2026-11-01T04:30:00Z", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z", "2026-11-02T05:30:00Z"),
                6d, 4d);
    }

    @Test void configuredRowLimitProtectsListsWithoutBreakingPagination() {
        seed();
        adapter.close();
        InfluxDBProperties configuration = new InfluxDBProperties();
        configuration.setUrl(URL);
        configuration.setDatabase(db);
        configuration.setMaxQueryRows(2);
        adapter = new InfluxDBAdapter(configuration, false);
        adapter.init();
        template = new TGTemplate(adapter);
        assertEquals(2, template.query(Telemetry.class).orderByTimeAsc().limit(2).list().size());
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> template.query(Telemetry.class).limit(3).list()).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM telemetry LIMIT 3")).getErrorCode());
        PageResult<Telemetry> page = template.query(Telemetry.class).orderByTimeAsc().limit(2).page();
        assertEquals(2, page.getRows().size());
        assertTrue(page.isHasNext());
        PageResult<Telemetry> next = template.query(Telemetry.class).orderByTimeAsc()
                .cursorTime(page.getNextCursorTime()).limit(2).page();
        assertEquals(2, next.getRows().size());
        assertFalse(next.isHasNext());
    }

    @Test void commentMeasurementCannotSilentlyDisappearFromAMixedBatch() {
        assertTrue(adapter.write(null, record(BASE, "existing", 1)));
        TSDBRecord comment = new TSDBRecord("#discarded", BASE + 2, Map.of(), Map.of("value", 3d));
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(record(BASE + 1, "must_not_commit", 2), comment)));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
        assertEquals(0, failure.getResult().committedRecords());
        assertEquals(1, adapter.count(null, query()));
    }

    @Test void measurementEqualsAndInflux3BackslashesRoundTripWithoutRenamingTheTable() {
        for (String measurement : List.of("measure=equals", "path\\measurement", "space ,=measurement")) {
            assertTrue(adapter.write(null, new TSDBRecord(measurement, BASE, Map.of("device", "name"), Map.of("value", 7L))));
            TSDBQuery query = query();
            query.setMeasurement(measurement);
            List<Map<String, Object>> result = rows(query);
            assertEquals(1, result.size());
            assertEquals(7L, ((Number) result.get(0).get("value")).longValue());
        }
    }

    @Test void largeBigIntegersRoundTripExactlyThroughSignedIntegerFields() {
        List<BigInteger> values = List.of(new BigInteger("9007199254740993"),
                BigInteger.valueOf(Long.MAX_VALUE), BigInteger.valueOf(Long.MIN_VALUE));
        List<TSDBRecord> records = new ArrayList<>();
        for (int index = 0; index < values.size(); index++) {
            records.add(new TSDBRecord("precise_integers", BASE + index, Map.of(), Map.of("value", values.get(index))));
        }
        assertTrue(adapter.batchWrite(null, records));
        TSDBQuery query = query();
        query.setMeasurement("precise_integers");
        List<Map<String, Object>> result = rows(query);
        for (int index = 0; index < values.size(); index++) {
            assertInstanceOf(Long.class, result.get(index).get("value"));
            assertEquals(values.get(index).longValueExact(), result.get(index).get("value"));
        }
    }

    @Test void decimalUnderflowAndIntegerOverflowCannotPartiallyCommitAMixedBatch() {
        assertTrue(adapter.write(null, record(BASE, "existing", 1)));
        for (Number invalid : List.<Number>of(new BigDecimal("1E-400"), new BigDecimal("-1E-400"),
                new BigDecimal("1E400"), BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE))) {
            TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                    () -> adapter.batchWriteDetailed(null, List.of(record(BASE + 1, "must_not_commit", 2),
                            new TSDBRecord("invalid_numeric", BASE + 2, Map.of(), Map.of("value", invalid)))));
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
            assertEquals(0, failure.getResult().committedRecords());
            assertEquals(1, adapter.count(null, query()));
        }
    }

    @Test void decimalValuesUseDocumentedDoublePrecisionWithoutDroppingNonzeroValues() {
        BigDecimal original = new BigDecimal("0.12345678901234567890123456789");
        assertTrue(adapter.write(null, new TSDBRecord("decimal_values", BASE, Map.of(), Map.of("value", original))));
        TSDBQuery query = query();
        query.setMeasurement("decimal_values");
        assertEquals(original.doubleValue(), ((Number) rows(query).get(0).get("value")).doubleValue());
    }

    private void calendarDays(String first, String second, String end, List<String> times, double sumFirst, double sumSecond) {
        List<TSDBRecord> points = new ArrayList<>();
        for (int i = 0; i < times.size(); i++) {
            points.add(record(java.time.Instant.parse(times.get(i)).toEpochMilli(), "dst", i + 1));
        }
        assertTrue(adapter.batchWrite(null, points));
        TSDBQuery query = query();
        query.setGroupByTime("1d");
        query.setTimeZone("America/New_York");
        query.setStartTime(java.time.Instant.parse(first).toEpochMilli());
        query.setEndTime(java.time.Instant.parse(end).toEpochMilli() - 1);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        List<Map<String, Object>> result = rows(query);
        assertEquals(2, result.size());
        assertEquals(java.time.Instant.parse(first).toEpochMilli(), result.get(0).get("window_start"));
        assertEquals(java.time.Instant.parse(second).toEpochMilli(), result.get(1).get("window_start"));
        assertEquals(sumFirst, ((Number) result.get(0).get("total")).doubleValue());
        assertEquals(sumSecond, ((Number) result.get(1).get("total")).doubleValue());
        assertEquals(2, adapter.count(null, query));
    }
}
