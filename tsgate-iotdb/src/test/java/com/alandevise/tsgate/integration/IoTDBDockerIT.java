package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsgate.config.IoTDBProperties;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.*;
import com.alandevise.tsgate.model.*;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.junit.jupiter.api.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class IoTDBDockerIT extends DatabaseContractIT {
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
    @Test void tabletChunkingReportsPhysicalBatches() {
        var result=adapter.batchWriteDetailed(null,java.util.stream.IntStream.range(0,7).mapToObj(i->record(BASE+i,"a",i)).toList());
        assertEquals(3,result.totalBatches()); assertEquals(7,result.committedRecords());
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
