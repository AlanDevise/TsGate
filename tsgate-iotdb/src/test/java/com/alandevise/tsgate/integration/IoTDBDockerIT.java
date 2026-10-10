package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsgate.config.IoTDBProperties;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.*;
import com.alandevise.tsgate.model.*;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class IoTDBDockerIT extends DatabaseContractIT implements com.alandevise.tsgate.contract.BackendSemanticsContract {
    @Override public com.alandevise.tsgate.adapter.TSDBAdapter semanticsAdapter() { return adapter; }
    @Override public com.alandevise.tsgate.core.TGQueryBuilder<?> semanticsQuery() { return template.query(Telemetry.class); }
    @Override public String semanticsMeasurement() { return "telemetry"; }
    @Override public long semanticsTime() { return BASE; }
    @Override public void awaitSemanticsVisibility(List<TSDBRecord> expected) {
        // This RPC fixture tests acknowledged writes against the configured IoTDB server version.
    }
    @Override public void configureSemanticsRowLimit(int rows) {
        adapter.close();
        IoTDBProperties properties = config(db);
        properties.setMaxQueryRows(rows);
        adapter = new IoTDBTableAdapter(properties, properties.getPool(), false);
        adapter.init();
        template = new TGTemplate(adapter);
    }

    ITableSession admin;
    static final String ENDPOINT=System.getProperty("tsdb.it.iotdb.endpoint","127.0.0.1:16667");
    @BeforeEach void setup() throws Exception {
        db="it_iot_"+Long.toUnsignedString(System.nanoTime(),36);
        admin=new TableSessionBuilder().nodeUrls(List.of(ENDPOINT)).username("root").password("root").enableAutoFetch(false).enableRedirection(false).connectionTimeoutInMs(30000).queryTimeoutInMs(30000).build();
        admin.executeNonQueryStatement("CREATE DATABASE "+db);
        admin.executeNonQueryStatement("USE "+db);
        admin.executeNonQueryStatement("CREATE TABLE telemetry (device STRING TAG, value DOUBLE FIELD, ival INT32 FIELD, lval INT64 FIELD, fval FLOAT FIELD, active BOOLEAN FIELD, label STRING FIELD)");
        IoTDBProperties config=config(db); adapter=new IoTDBTableAdapter(config,config.getPool(),false); adapter.init(); template=new TGTemplate(adapter);
    }
    static IoTDBProperties config(String db) {
        IoTDBProperties c=new IoTDBProperties(); c.setUsername("root"); c.setPassword("root"); c.setDatabase(db); c.setMaxBatchRecords(100);
        c.getPool().setConnectionTimeoutInMs(30000); c.getPool().setQueryTimeoutInMs(30000); c.getPool().setWaitToGetSessionTimeoutInMs(30000);
        c.getTable().setRpcCompressionEnabled(Boolean.parseBoolean(System.getProperty("tsdb.it.iotdb.rpc-compression-enabled", "true")));
        c.getPool().setNodeUrls(List.of(ENDPOINT)); c.getPool().setMaxRetryCount(0); c.getPool().setRetryIntervalInMs(10); c.getTable().setTabletMaxRowSize(3); return c;
    }
    @AfterEach void cleanup() throws Exception {
        if(adapter!=null) adapter.close();
        if(admin!=null) { try { admin.executeNonQueryStatement("DROP DATABASE "+db); } finally { admin.close(); } }
    }
    @Test void directBatchRecordLimitRejectsBeforeCopyAndPreservesReadback() {
        adapter.close();
        IoTDBProperties config = config(db);
        config.setMaxBatchRecords(3);
        adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        adapter.init();
        template = new TGTemplate(adapter);
        assertTrue(adapter.write(null, record(BASE, "existing", 0d)));
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
        assertEquals(List.of(0d), rows(query()).stream()
                .map(row -> ((Number) row.get("value")).doubleValue()).toList());
        assertEquals(1, adapter.count(null, query()));

        List<TSDBRecord> boundary = List.of(record(BASE + 1, "next", 1d),
                record(BASE + 2, "next", 2d), record(BASE + 3, "next", 3d));
        assertEquals(adapter.getMaxBatchRecords(), boundary.size());
        BatchWriteResult result = adapter.batchWriteDetailed(null, boundary);
        assertTrue(result.isSuccess());
        assertEquals(3, result.committedRecords());
        assertEquals(List.of(0d, 1d, 2d, 3d), rows(query()).stream()
                .map(row -> ((Number) row.get("value")).doubleValue()).toList());
        assertEquals(4, adapter.count(null, query()));
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

    @Test void tabletChunkingReportsPhysicalBatches() {
        var result=adapter.batchWriteDetailed(null,java.util.stream.IntStream.range(0,7).mapToObj(i->record(BASE+i,"a",i)).toList());
        assertEquals(3,result.totalBatches()); assertEquals(7,result.committedRecords());
    }
    @Test void mixedCaseColumnsRoundTripWithinOneTablet() {
        adapter.close();
        IoTDBProperties config = config(db);
        config.getTable().setTabletMaxRowSize(20);
        adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        adapter.init();
        template = new TGTemplate(adapter);
        String[] tagNames = {"DEVICE", "device", "DeViCe"};
        String[] valueNames = {"value", "VALUE", "VaLuE"};
        String[] integerNames = {"IVAL", "Ival", "ival"};
        List<TSDBRecord> records = new ArrayList<>();
        for (int row = 0; row < 12; row++) {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put(valueNames[row % 3], row + 0.25d);
            fields.put(integerNames[row % 3], row % 3 == 0 ? null : row);
            fields.put(row % 2 == 0 ? "LABEL" : "label", "row-" + row);
            records.add(new TSDBRecord("telemetry", BASE + row,
                    Map.of(tagNames[row % 3], "sensor-" + row), fields));
        }
        BatchWriteResult result = adapter.batchWriteDetailed(null, records);
        assertTrue(result.isSuccess());
        assertEquals(12, result.committedRecords());
        assertEquals(1, result.committedBatches());
        List<Map<String, Object>> readback = rows(query());
        assertEquals(12, readback.size());
        for (int row = 0; row < readback.size(); row++) {
            Map<String, Object> actual = readback.get(row);
            assertEquals(BASE + row, ((Number) actual.get("time")).longValue());
            assertEquals("sensor-" + row, actual.get("device"));
            assertEquals(row + 0.25d, actual.get("value"));
            assertEquals(row % 3 == 0 ? null : row, actual.get("ival"));
            assertEquals("row-" + row, actual.get("label"));
            assertTrue(records.get(row).tags().containsKey(tagNames[row % 3]));
            assertTrue(records.get(row).fields().containsKey(valueNames[row % 3]));
        }
    }
    @Test void mixedCaseTableNamesSharePhysicalTabletsAndRoundTrip() {
        String[] tableNames = {"TELEMETRY", "telemetry", "TeLeMeTrY"};
        List<TSDBRecord> records = new ArrayList<>();
        for (int row = 0; row < 6; row++) {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("value", row + 0.25d);
            fields.put("ival", row % 2 == 0 ? null : row);
            fields.put("label", "row-" + row);
            records.add(new TSDBRecord(tableNames[row % tableNames.length], BASE + row,
                    Map.of("device", "sensor-" + row), fields));
        }
        BatchWriteResult result = adapter.batchWriteDetailed(null, records);
        assertTrue(result.isSuccess());
        assertEquals(6, result.committedRecords());
        assertEquals(2, result.totalBatches());
        assertEquals(2, result.committedBatches());
        TSDBQuery query = query();
        query.setMeasurement("TELEMETRY");
        List<Map<String, Object>> readback = rows(query);
        assertEquals(6, readback.size());
        for (int row = 0; row < readback.size(); row++) {
            Map<String, Object> actual = readback.get(row);
            assertEquals(BASE + row, ((Number) actual.get("time")).longValue());
            assertEquals("sensor-" + row, actual.get("device"));
            assertEquals(row + 0.25d, actual.get("value"));
            assertEquals(row % 2 == 0 ? null : row, actual.get("ival"));
            assertEquals("row-" + row, actual.get("label"));
            assertEquals(tableNames[row % tableNames.length], records.get(row).measurement());
        }
        assertEquals(6, adapter.count(null, query));
    }

    @ParameterizedTest
    @ValueSource(strings = {"type", "role"})
    void mixedCaseTableConflictsRejectTheWholeBatchWithoutChangingStoredRows(String conflict) throws Exception {
        admin.executeNonQueryStatement("CREATE TABLE earlier_table (device STRING TAG, value DOUBLE FIELD)");
        assertTrue(adapter.batchWrite(null, List.of(record(BASE, "existing", 100),
                new TSDBRecord("earlier_table", BASE, Map.of("device", "existing"), Map.of("value", 200d)))));
        List<TSDBRecord> records = new ArrayList<>();
        for (int row = 1; row <= 4; row++) {
            records.add(new TSDBRecord("earlier_table", BASE + row, Map.of("device", "new"),
                    Map.of("value", (double) row)));
        }
        records.add(record(BASE + 1, "new", 1));
        records.add(record(BASE + 2, "new", 2));
        records.add(conflict.equals("type")
                ? new TSDBRecord("TELEMETRY", BASE + 3, Map.of("device", "new"), Map.of("value", 3L))
                : new TSDBRecord("TeLeMeTrY", BASE + 3, Map.of("other", "new"),
                        Map.of("value", 3d, "device", "field")));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, records));
        assertEquals(TSDBErrorCodeEnum.METADATA_ERROR, error.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(0, error.getResult().committedBatches());
        List<Map<String, Object>> telemetry = rows(query());
        assertEquals(1, telemetry.size());
        assertEquals(BASE, ((Number) telemetry.get(0).get("time")).longValue());
        assertEquals("existing", telemetry.get(0).get("device"));
        assertEquals(100d, telemetry.get(0).get("value"));
        TSDBQuery earlier = query();
        earlier.setMeasurement("earlier_table");
        List<Map<String, Object>> earlierRows = rows(earlier);
        assertEquals(1, earlierRows.size());
        assertEquals(BASE, ((Number) earlierRows.get(0).get("time")).longValue());
        assertEquals("existing", earlierRows.get(0).get("device"));
        assertEquals(200d, earlierRows.get(0).get("value"));
    }

    @Test void filterOperandValidationPreservesValidQueriesAndExistingRows() {
        seed();
        TSDBQuery query = query();
        query.setFilters(List.of(new QueryFilter("value", OperatorEnum.BETWEEN, List.of(2d, 3d))));
        assertEquals(List.of(2d, 3d), rows(query).stream().map(row -> row.get("value")).toList());
        assertEquals(2, adapter.count(null, query));
        query.setFilters(List.of(new QueryFilter("value", OperatorEnum.EQ, List.of(3d))));
        assertEquals(List.of(3d), rows(query).stream().map(row -> row.get("value")).toList());
        assertEquals(1, adapter.count(null, query));
        query.setFilters(List.of(new QueryFilter("value", OperatorEnum.IN, List.of(1d, 4d))));
        assertEquals(List.of(1d, 4d), rows(query).stream().map(row -> row.get("value")).toList());
        assertEquals(2, adapter.count(null, query));

        List<QueryFilter> invalidFilters = List.of(
                new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1d, 2d, 3d)),
                new QueryFilter("value", OperatorEnum.EQ, List.of(1d, 2d)),
                new QueryFilter("value", OperatorEnum.EQ, List.of(Double.NaN)),
                new QueryFilter("value", OperatorEnum.EQ, List.of(Double.POSITIVE_INFINITY)),
                new QueryFilter("value", OperatorEnum.EQ, List.of(Float.NEGATIVE_INFINITY)),
                new QueryFilter("value", OperatorEnum.EQ, Arrays.asList((Object) null)),
                new QueryFilter("value", OperatorEnum.IN, Arrays.asList(1d, null)));
        for (QueryFilter filter : invalidFilters) {
            query.setFilters(List.of(filter));
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    assertThrows(TSDBException.class, () -> adapter.count(null, query)).getErrorCode());
            assertEquals(List.of(1d, 2d, 3d, 4d),
                    rows(query()).stream().map(row -> row.get("value")).toList());
            assertEquals(4, adapter.count(null, query()));
        }
    }

    @Test void explicitDatabaseDoesNotLeakIntoDefaultSession() throws Exception {
        String other=db+"_other"; admin.executeNonQueryStatement("CREATE DATABASE "+other);
        try {
            admin.executeNonQueryStatement("CREATE TABLE "+other+".telemetry (device STRING TAG, value DOUBLE FIELD)");
            assertTrue(adapter.write(other,record(BASE,"other",9))); assertTrue(adapter.write(null,record(BASE,"default",3)));
            assertEquals("default",rows(query()).get(0).get("device"));
            assertEquals("other",adapter.query(other,query()).getRows().get(0).get("device"));
            assertEquals("default",adapter.executeQuery("SELECT * FROM telemetry").getRows().get(0).get("device"));
        } finally { admin.executeNonQueryStatement("DROP DATABASE "+other); }
    }
    @Test void nativeDuplicateOutputColumnsFailWithoutPoisoningTheOnlyPooledSession() throws Exception {
        seed();
        adapter.close();
        IoTDBProperties config = config(db);
        config.getPool().setMaxSize(1);
        adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        adapter.init();
        String duplicateSql = "SELECT ival AS duplicate, value AS duplicate FROM telemetry";
        for (boolean empty : List.of(false, true)) {
            String sql = duplicateSql + (empty ? " WHERE time < " + BASE : "") + " ORDER BY time";
            try (var nativeResult = admin.executeQueryStatement(sql)) {
                assertEquals(List.of("duplicate", "duplicate"), nativeResult.getColumnNames());
                assertEquals(!empty, nativeResult.hasNext());
                if (!empty) {
                    var fields = nativeResult.next().getFields();
                    assertEquals(2, fields.size());
                    assertTrue(fields.get(0) == null || fields.get(0).getDataType() == null);
                    assertEquals(1d, fields.get(1).getDoubleV());
                }
            }
            TSDBException failure = assertThrows(TSDBException.class, () -> adapter.executeQuery(sql));
            assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
            assertTrue(failure.getMessage().contains("duplicate output column: duplicate"));
            assertEquals(List.of(1d, 2d, 3d, 4d),
                    rows(query()).stream().map(row -> row.get("value")).toList());
            assertEquals(4, adapter.count(null, query()));
        }
        QueryResult caseDistinct = adapter.executeQuery(
                "SELECT value AS \"Upper\", value AS \"upper\" FROM telemetry ORDER BY time");
        assertEquals(List.of("Upper", "upper"), caseDistinct.getColumns());
        assertEquals(4, caseDistinct.getRowCount());
        for (int index = 0; index < caseDistinct.getRowCount(); index++) {
            assertEquals(index + 1d, caseDistinct.getRows().get(index).get("Upper"));
            assertEquals(index + 1d, caseDistinct.getRows().get(index).get("upper"));
        }
    }

    @Test void queryErrorDoesNotPoisonPool() {
        assertTrue(adapter.write(null,record(BASE,"a",1)));
        assertThrows(TSDBException.class,()->adapter.executeQuery("SELECT unknown_field FROM telemetry"));
        assertEquals(1,rows(query()).size());
    }

    @Test void propertyChangesCannotRedirectTheSessionPoolOrItsDefaultDatabase() throws Exception {
        adapter.close();
        IoTDBProperties properties = config(db);
        List<String> endpoints = new ArrayList<>(properties.getPool().getNodeUrls());
        properties.getPool().setNodeUrls(endpoints);
        IoTDBTableAdapter snapshot = new IoTDBTableAdapter(properties, properties.getPool(), false);
        adapter = snapshot;
        properties.setDatabase("missing_changed_database");
        properties.setUsername("unused_changed_user");
        properties.setPassword("unused_changed_password");
        properties.getTable().setTabletMaxRowSize(0);
        properties.getPool().setMaxSize(0);
        endpoints.clear();
        endpoints.add("127.0.0.1:1");
        snapshot.init();
        properties.setDatabase("another_missing_database");
        properties.setPool(null);
        properties.setTable(null);
        assertTrue(snapshot.write(null, record(BASE, "snapshot", 1)));
        assertEquals(1, rows(query()).size());
        try (ITableSession session = snapshot.getSessionPool().getSession();
             var result = session.executeQueryStatement("SELECT value FROM telemetry")) {
            assertTrue(result.hasNext());
            assertEquals(1d, result.next().getFields().get(0).getDoubleV());
            assertFalse(result.hasNext());
        }
        TSDBQuery invalid = query();
        invalid.setLimit(0);
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> snapshot.query(null, invalid)).getErrorCode());
        assertEquals(1, rows(query()).size());
    }
    @Test void dateBlobAndNullRoundTrip() throws Exception {
        admin.executeNonQueryStatement("CREATE TABLE binary_data (device STRING TAG, payload BLOB FIELD, event_date DATE FIELD, optional STRING FIELD)");
        byte[] payload=new byte[]{0,1,-1,64};
        Map<String,Object> fields=new LinkedHashMap<>(); fields.put("payload",payload); fields.put("event_date",java.time.LocalDate.of(2026,9,24)); fields.put("optional",null);
        assertTrue(adapter.write(null,new TSDBRecord("binary_data",BASE,Map.of("device","binary"),fields)));
        Map<String,Object> row=adapter.executeQuery("SELECT * FROM binary_data").getRows().get(0);
        assertArrayEquals(payload,(byte[])row.get("payload")); assertEquals(java.time.LocalDate.of(2026,9,24),row.get("event_date")); assertNull(row.get("optional"));
    }
    @Test void nullableNumericFieldsInferFromLaterRowsAndPreserveNulls() {
        Map<String,Object> first=new LinkedHashMap<>(); first.put("value",1d); first.put("ival",null);
        Map<String,Object> second=new LinkedHashMap<>(); second.put("value",2d); second.put("ival",7);
        Map<String,Object> third=new LinkedHashMap<>(); third.put("value",3d); third.put("ival",null);
        assertTrue(adapter.batchWrite(null,List.of(new TSDBRecord("telemetry",BASE,Map.of("device","a"),first),new TSDBRecord("telemetry",BASE+1,Map.of("device","a"),second),new TSDBRecord("telemetry",BASE+2,Map.of("device","a"),third))));
        List<Map<String,Object>> result=rows(query()); assertEquals(3,result.size()); assertNull(result.get(0).get("ival")); assertEquals(7,result.get(1).get("ival")); assertNull(result.get(2).get("ival"));
    }
    @Test void invalidCredentialsFailOnFirstUse() {
        IoTDBProperties c=config(db); c.setPassword("incorrect-it-password"); IoTDBTableAdapter wrong=new IoTDBTableAdapter(c,c.getPool(),false);
        try { wrong.init(); assertThrows(TSDBException.class,()->wrong.executeQuery("SELECT * FROM telemetry")); } finally { wrong.close(); }
    }

    @Test void nativeAndStructuredRowLimitsReleaseTheOnlyPooledSession() {
        seed();
        IoTDBProperties limitedConfig = config(db);
        limitedConfig.setMaxQueryRows(2);
        limitedConfig.getPool().setMaxSize(1);
        IoTDBTableAdapter limited = new IoTDBTableAdapter(limitedConfig, limitedConfig.getPool(), false);
        try {
            limited.init();
            for (int attempt = 0; attempt < 3; attempt++) {
                TSDBException failure = assertThrows(TSDBException.class,
                        () -> limited.executeQuery("SELECT * FROM telemetry"));
                assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
                assertTrue(failure.getMessage().contains("max-query-rows"));
                assertEquals(2, limited.executeQuery("SELECT * FROM telemetry LIMIT 2").getRowCount());
                TSDBQuery structured = query();
                assertThrows(TSDBException.class, () -> limited.query(null, structured));
                structured.setLimit(3);
                assertThrows(TSDBException.class, () -> limited.query(null, structured));
                structured.setPaginationProbe(true);
                assertEquals(3, limited.query(null, structured).getRowCount());
            }
        } finally {
            limited.close();
        }
    }

    @Test void springDstDayUsesTwentyThreeHoursAndLocalMidnightBoundaries() {
        assertCalendarDays("2026-03-07", 23);
    }

    @Test void autumnDstDayUsesTwentyFiveHoursAndLocalMidnightBoundaries() {
        assertCalendarDays("2026-10-31", 25);
    }

    @Test void nearEpochCalendarDaysKeepInt64ResultType() {
        assertCalendarDays("1970-01-01", 24);
    }

    @Test void calendarDaysCrossingInt32LiteralRangeKeepTheSameResultType() {
        assertCalendarDays("1970-01-24", 24);
    }

    private void assertCalendarDays(String firstDate, int transitionDayHours) {
        java.time.ZoneId zone = java.time.ZoneId.of("America/New_York");
        java.time.LocalDate date = java.time.LocalDate.parse(firstDate);
        List<Long> boundaries = java.util.stream.IntStream.rangeClosed(0, 3)
                .mapToObj(i -> date.plusDays(i).atStartOfDay(zone).toInstant().toEpochMilli()).toList();
        assertEquals(transitionDayHours * 3_600_000L, boundaries.get(2) - boundaries.get(1));
        List<TSDBRecord> records = new ArrayList<>();
        for (int day = 0; day < 3; day++) {
            records.add(record(boundaries.get(day), "calendar", 1));
            records.add(record(boundaries.get(day + 1) - 1, "calendar", 1));
        }
        assertTrue(adapter.batchWrite(null, records));
        TSDBQuery query = query();
        query.setStartTime(boundaries.get(0));
        query.setEndTime(boundaries.get(3) - 1);
        query.setTimeZone("America/New_York");
        query.setGroupByTime("1d");
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.COUNT, "samples")));
        List<Map<String, Object>> results = rows(query);
        assertEquals(3, results.size());
        for (int day = 0; day < 3; day++) {
            assertInstanceOf(Long.class, results.get(day).get("window_start"));
            assertEquals(boundaries.get(day).longValue(), ((Number) results.get(day).get("window_start")).longValue());
            assertEquals(2, ((Number) results.get(day).get("samples")).longValue());
        }
        assertEquals(3, adapter.count(null, query));
    }
}
