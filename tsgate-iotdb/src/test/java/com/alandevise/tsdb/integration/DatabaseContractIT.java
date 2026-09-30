package com.alandevise.tsdb.integration;

import com.alandevise.tsdb.adapter.TSDBAdapter;
import com.alandevise.tsdb.annotation.*;
import com.alandevise.tsdb.core.TGTemplate;
import com.alandevise.tsdb.exception.*;
import com.alandevise.tsdb.model.*;
import org.junit.jupiter.api.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real database contract tests, intentionally local and gitignored. */
abstract class DatabaseContractIT {
    TSDBAdapter adapter;
    TGTemplate template;
    String db;
    static final long BASE = System.currentTimeMillis() - 3600000;
    static TSDBRecord record(long time, String device, double value) {
        return new TSDBRecord("telemetry", time, Map.of("device", device), Map.of("value", value));
    }
    TSDBQuery query() { TSDBQuery q = new TSDBQuery(); q.setMeasurement("telemetry"); q.setOrder(SortOrderEnum.ASC); return q; }
    List<Map<String,Object>> rows(TSDBQuery q) { QueryResult r=adapter.query(null,q); assertTrue(r.isSuccess(),r.getMessage()); return r.getRows(); }
    void seed() { assertTrue(adapter.batchWrite(null,List.of(record(BASE,"a",1),record(BASE+1000,"b",2),record(BASE+2000,"a",3),record(BASE+3000,"b",4)))); }
    @Test void singleRecordRoundTripsScalarTypesUnicodeAndEscaping() {
        Map<String,Object> fields=new LinkedHashMap<>(); fields.put("ival",12); fields.put("lval",12345678901L); fields.put("fval",1.25f); fields.put("value",2.5d); fields.put("active",true); fields.put("label","设备'\"\\value");
        assertTrue(adapter.write(null,new TSDBRecord("telemetry",BASE,Map.of("device","a,b = '华东'"),fields)));
        Map<String,Object> row=rows(query()).get(0);
        assertEquals(BASE,((Number)row.get("time")).longValue()); assertEquals(12,((Number)row.get("ival")).intValue()); assertEquals(12345678901L,((Number)row.get("lval")).longValue());
        assertEquals(1.25d,((Number)row.get("fval")).doubleValue()); assertEquals(2.5d,((Number)row.get("value")).doubleValue()); assertEquals(true,row.get("active"));
        assertEquals("设备'\"\\value",row.get("label")); assertEquals("a,b = '华东'",row.get("device"));
    }
    @Test void batchPreservesOutOfOrderRowsAndCommitCounts() {
        List<TSDBRecord> input=IntStream.range(0,7).mapToObj(i->record(BASE+(6-i)*1000,"a",6-i)).toList();
        BatchWriteResult result=adapter.batchWriteDetailed(null,input); assertTrue(result.isSuccess()); assertEquals(7,result.committedRecords()); assertEquals(result.totalBatches(),result.committedBatches());
        List<Map<String,Object>> rows=rows(query()); assertEquals(7,rows.size());
        for(int i=0;i<7;i++) assertEquals(i,((Number)rows.get(i).get("value")).intValue());
    }
    @Test void nullAndEmptyBatchesAreNoOps() {
        assertEquals(0,adapter.batchWriteDetailed(null,null).committedRecords()); assertEquals(0,adapter.batchWriteDetailed(null,List.of()).committedRecords());
    }
    @Test void validatesWholeBatchBeforeAnyCommit() {
        TSDBBatchWriteException error=assertThrows(TSDBBatchWriteException.class,()->adapter.batchWriteDetailed(null,Arrays.asList(record(BASE,"a",1),null)));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED,error.getResult().commitState()); assertEquals(0,error.getResult().committedRecords());
        assertTrue(adapter.write(null,record(BASE+1000,"a",2))); assertEquals(1,rows(query()).size());
    }
    @Test void validatesOversizeBatchBeforeAnyCommit() {
        TSDBBatchWriteException error=assertThrows(TSDBBatchWriteException.class,()->adapter.batchWriteDetailed(null,Collections.nCopies(adapter.getMaxBatchRecords()+1,record(BASE,"a",1))));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED,error.getResult().commitState()); assertEquals(0,error.getResult().committedRecords());
    }
    @Test void rejectsBlankSqlMissingMeasurementAndMissingTable() {
        assertThrows(TSDBException.class,()->adapter.executeQuery(" ")); assertThrows(TSDBException.class,()->adapter.executeQuery(null));
        assertThrows(TSDBException.class,()->adapter.query(null,new TSDBQuery()));
        assertThrows(TSDBException.class,()->adapter.executeQuery("SELECT * FROM definitely_absent"));
    }
    @Test void nativeSqlAndInclusiveRangeFiltersWork() {
        seed(); TSDBQuery q=query(); q.setStartTime(BASE+1000); q.setEndTime(BASE+2000); assertEquals(2,rows(q).size());
        q.setFilters(List.of(new QueryFilter("device",OperatorEnum.EQ,List.of("a")))); assertEquals(1,rows(q).size());
        assertEquals(4,adapter.executeQuery("SELECT * FROM telemetry ORDER BY time").getRows().size());
        assertEquals(1,template.query(Telemetry.class).where("value",OperatorEnum.GT,2).whereTag("device","a").list().size());
    }
    @Test void offsetPaginationCountsAllRows() {
        seed(); PageResult<Telemetry> page=template.query(Telemetry.class).orderByTimeAsc().page(2,2);
        assertEquals(4L,page.getTotal()); assertEquals(2L,page.getTotalPages()); assertEquals(2,page.getRows().size()); assertFalse(page.isHasNext()); assertEquals(3d,page.getRows().get(0).value);
    }
    @Test void timeCursorPaginationResumesWithoutDuplicates() {
        seed(); PageResult<Telemetry> first=template.query(Telemetry.class).orderByTimeAsc().limit(2).page(); assertTrue(first.isHasNext());
        PageResult<Telemetry> next=template.query(Telemetry.class).orderByTimeAsc().cursorTime(first.getNextCursorTime()).limit(2).page();
        assertFalse(next.isHasNext()); assertEquals(3d,next.getRows().get(0).value); assertEquals(2,next.getRows().size());
    }
    @Test void strictCursorRetainsSameTimestampDifferentTags() {
        assertTrue(adapter.batchWrite(null,List.of(record(BASE,"a",1),record(BASE,"b",2),record(BASE,"c",3),record(BASE+1,"a",4))));
        PageResult<Telemetry> first=template.query(Telemetry.class).orderByTimeAsc().cursorColumns("time","device").limit(2).strictCursorPage();
        assertTrue(first.isHasNext());
        PageResult<Telemetry> next=template.query(Telemetry.class).orderByTimeAsc().cursorColumns("time","device").cursor(first.getNextCursor()).limit(2).strictCursorPage();
        assertFalse(next.isHasNext()); assertEquals(List.of("c","a"),next.getRows().stream().map(t->t.device).toList());
    }
    @Test void groupedAggregationAndCountWork() {
        seed(); TSDBQuery q=query(); q.setGroupByTags(List.of("device")); q.setAggregations(List.of(new AggregationSpec("value",AggregationFunctionEnum.SUM,"sum_value")));
        List<Map<String,Object>> rows=rows(q); assertEquals(2,rows.size()); assertEquals(2,adapter.count(null,q));
        Map<String,Double> sums=new HashMap<>(); rows.forEach(r->sums.put((String)r.get("device"),((Number)r.get("sum_value")).doubleValue())); assertEquals(Map.of("a",4d,"b",6d),sums);
    }
    @Test void timeWindowAggregationWorks() {
        seed(); TSDBQuery q=query(); q.setGroupByTime("1h"); q.setAggregations(List.of(new AggregationSpec("value",AggregationFunctionEnum.SUM,"sum_value")));
        double total=rows(q).stream().mapToDouble(r->((Number)r.get("sum_value")).doubleValue()).sum(); assertEquals(10d,total);
    }
    @Test void annotatedPojoWriteAndMapping() {
        Telemetry input=new Telemetry(); input.time=BASE; input.device="pojo"; input.value=8.5;
        assertTrue(template.write(input)); List<Telemetry> rows=template.query(Telemetry.class).whereTag("device","pojo").list();
        assertEquals(1,rows.size()); assertEquals(BASE,rows.get(0).time); assertEquals(8.5,rows.get(0).value);
    }
    @Test void concurrentWritesAndQueriesKeepAllRows() throws Exception {
        ExecutorService executor=Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> tasks=new ArrayList<>();
            for(int i=0;i<16;i++) { final int index=i; tasks.add(executor.submit(()->assertTrue(adapter.write(null,record(BASE+index,"worker"+(index%4),index))))); }
            for(Future<?> task:tasks) task.get(30,TimeUnit.SECONDS);
            List<Future<Long>> reads=new ArrayList<>(); for(int i=0;i<8;i++) reads.add(executor.submit(()->adapter.count(null,query())));
            for(Future<Long> read:reads) assertEquals(16L,read.get(30,TimeUnit.SECONDS));
        } finally { executor.shutdownNow(); }
    }
    @TGMeasurement("telemetry")
    public static class Telemetry {
        @TGTime public Long time;
        @TGTag public String device;
        @TGField public Double value;
        public Telemetry() { }
    }
}
