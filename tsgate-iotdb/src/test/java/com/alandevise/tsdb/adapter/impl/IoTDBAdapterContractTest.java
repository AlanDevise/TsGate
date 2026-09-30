package com.alandevise.tsdb.adapter.impl;

import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.exception.TSDBBatchWriteException;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.model.*;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.tsfile.write.record.Tablet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class IoTDBAdapterContractTest {
    private IoTDBTableAdapter adapter;
    private ITableSessionPool physicalPool;
    private ITableSession session;
    private SessionDataSet dataSet;

    @BeforeEach
    void setup() throws Exception {
        IoTDBProperties properties = new IoTDBProperties();
        properties.setDatabase("contract");
        properties.setUsername("test");
        properties.setPassword("test");
        properties.getTable().setTabletMaxRowSize(2);
        properties.getPool().setNodeUrls(List.of("127.0.0.1:6667"));
        adapter = new IoTDBTableAdapter(properties, properties.getPool(), false);
        physicalPool = mock(ITableSessionPool.class);
        session = mock(ITableSession.class);
        dataSet = mock(SessionDataSet.class);
        when(physicalPool.getSession()).thenReturn(session);
        when(session.executeQueryStatement(anyString())).thenReturn(dataSet);
        when(dataSet.getColumnNames()).thenReturn(List.of());
        when(dataSet.hasNext()).thenReturn(false);
        IoTDBTestPools.ready(adapter, physicalPool, "contract");
    }

    @ParameterizedTest
    @CsvSource({"1ms,1ms", "5s,5s", "10m,10m", "2h,2h", "1d,1d", "300000,300000ms", "5 M,5m"})
    void acceptsOnlyUnambiguousPositiveWindows(String window, String expected) throws Exception {
        adapter.query(null, aggregate(window));
        assertTrue(sql().contains("date_bin(" + expected + ", time"), sql());
        verify(dataSet).closeOperationHandle();
        verify(session).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "0ms", "-1h", "1.5h", "1h30m", "+2s", "1w", "1e3ms", "9223372036854775808ms", "999999999999999999999d", "1m,time)/*", "1m);SELECT 1--", "5m/*comment*/"})
    void rejectsInvalidOrInjectedWindowsBeforeBorrowing(String window) {
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.query(null, aggregate(window)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        verifyNoInteractions(physicalPool);
    }

    @Test
    void quotesStringFilterAndBorrowsIndependentDatabaseContext() throws Exception {
        TSDBQuery query = detail();
        query.getFilters().add(new QueryFilter("device", OperatorEnum.EQ, List.of("O'Brien; --")));
        adapter.query("other_database", query);
        assertTrue(sql().contains("device = 'O''Brien; --'"));
        verify(session).executeNonQueryStatement("USE other_database");
        verify(session).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"points;DELETE", "db.points", "meter name", "points--"})
    void rejectsUnsafeTableIdentifiersBeforeBorrowing(String table) {
        TSDBQuery query = detail();
        query.setMeasurement(table);
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        verifyNoInteractions(physicalPool);
    }

    @Test
    void rawQueryPreservesSqlAndDoesNotExecuteUse() throws Exception {
        String raw = "SELECT * FROM other.points LIMIT 3";
        adapter.executeQuery(raw);
        verify(session).executeQueryStatement(raw);
        verify(session, never()).executeNonQueryStatement(anyString());
    }

    @ParameterizedTest
    @CsvSource({"NO_PERMISSION,PERMISSION_ERROR", "TABLE_NOT_EXISTS,RESOURCE_NOT_FOUND", "ILLEGAL_PARAMETER,ARGUMENT_ERROR", "UNSUPPORTED_OPERATION,UNSUPPORTED_OPERATION", "CONFIGURATION_ERROR,CONFIGURATION_ERROR", "CAN_NOT_CONNECT_DATANODE,CONNECTION_ERROR"})
    void mapsStatusErrorsWithoutRelyingOnEnglishMessages(TSStatusCode status, TSDBErrorCodeEnum expected) throws Exception {
        when(session.executeQueryStatement(anyString())).thenThrow(new StatementExecutionException(
                new TSStatus(status.getStatusCode()).setMessage("localized-message")));
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.query(null, detail()));
        assertEquals(expected, error.getErrorCode());
        verify(session).close();
    }

    @Test
    void datasetIsClosedWhenReadingFailsAndCleanupFailureIsSuppressed() throws Exception {
        StatementExecutionException failure = new StatementExecutionException("read failed");
        StatementExecutionException closeFailure = new StatementExecutionException("close failed");
        when(dataSet.hasNext()).thenThrow(failure);
        doThrow(closeFailure).when(dataSet).closeOperationHandle();
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.query(null, detail()));
        assertSame(failure, error.getCause());
        assertArrayEquals(new Throwable[]{closeFailure}, failure.getSuppressed());
        verify(session).close();
    }

    @Test
    void completesValidationBeforeAnyIo() {
        TSDBRecord invalid = new TSDBRecord("points", 3L, Map.of(), Map.of("value", new Object()));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(1), invalid)));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        assertEquals(TSDBErrorCodeEnum.METADATA_ERROR, error.getErrorCode());
        verifyNoInteractions(physicalPool);
    }

    @Test
    void splitsTabletsByConfiguredRowsAndReportsConfirmedCounts() throws Exception {
        List<Integer> rowSizes = new ArrayList<>();
        doAnswer(call -> { rowSizes.add(((Tablet) call.getArgument(0)).getRowSize()); return null; })
                .when(session).insert(any(Tablet.class));
        BatchWriteResult result = adapter.batchWriteDetailed(null, List.of(point(1), point(2), point(3)));
        assertEquals(List.of(2, 1), rowSizes);
        assertEquals(3, result.committedRecords());
        assertEquals(2, result.committedBatches());
        assertTrue(result.isSuccess());
    }

    @Test
    void laterTabletFailurePreservesKnownCommitLowerBoundAndDoesNotReplay() throws Exception {
        doNothing().doThrow(new IoTDBConnectionException("connection lost"))
                .when(session).insert(any(Tablet.class));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(1), point(2), point(3))));
        assertEquals(BatchCommitStateEnum.UNKNOWN, error.getResult().commitState());
        assertEquals(2, error.getResult().committedRecords());
        assertEquals(1, error.getResult().failedBatchIndex());
        assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, error.getErrorCode());
        verify(session, times(2)).insert(any(Tablet.class));
        verify(physicalPool, times(1)).getSession();
    }

    @Test
    void cleanupFailureAfterConfirmedWritesDoesNotReportUncertainCommit() throws Exception {
        doThrow(new IoTDBConnectionException("close lost")).when(session).close();
        BatchWriteResult result = adapter.batchWriteDetailed(null, List.of(point(1)));
        assertTrue(result.isSuccess());
        assertEquals(1, result.committedRecords());
    }

    @Test
    void poolBorrowFailureIsKnownUncommittedAndNeverChangesPool() throws Exception {
        when(physicalPool.getSession()).thenThrow(new IoTDBConnectionException("pool timeout"));
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, List.of(point(1))));
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(TSDBErrorCodeEnum.CONNECTION_ERROR, error.getErrorCode());
        verify(physicalPool, never()).close();
    }

    @Test
    void closeIsIdempotentAndRejectsLaterQueries() {
        adapter.close();
        adapter.close();
        verify(physicalPool, times(1)).close();
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, detail())).getErrorCode());
    }

    @Test
    void recoveryRetiresOldPoolOnlyAfterOutstandingBorrowReturns() throws Exception {
        ITableSessionPool first = mock(ITableSessionPool.class);
        ITableSessionPool second = mock(ITableSessionPool.class);
        ITableSession delegate = mock(ITableSession.class);
        when(first.getSession()).thenReturn(delegate);
        RecoverableTableSessionPool pool = new RecoverableTableSessionPool(first);
        ITableSession borrowed = pool.getSession();
        assertTrue(pool.replaceIfCurrent(0, () -> second));
        assertFalse(pool.replaceIfCurrent(0, () -> { throw new AssertionError("stale recovery"); }));
        verify(first, never()).close();
        borrowed.close();
        borrowed.close();
        verify(delegate, times(1)).close();
        verify(first, times(1)).close();
        pool.close();
        verify(second, times(1)).close();
        assertThrows(IllegalStateException.class, pool::getSession);
    }

    @Test
    void calendarDayQueryUsesPhysicalTimeColumnInExplicitDstBoundaries() throws Exception {
        TSDBQuery query = aggregate("1d");
        query.setTimeColumn("event_time");
        query.setTimeZone("America/New_York");
        long start = java.time.Instant.parse("2026-03-08T05:00:00Z").toEpochMilli();
        long end = java.time.Instant.parse("2026-03-09T04:00:00Z").toEpochMilli();
        query.setStartTime(start);
        query.setEndTime(end - 1);
        adapter.query(null, query);
        String sql = sql();
        assertTrue(sql.contains("CASE WHEN event_time >= " + start + " AND event_time < " + end
                + " THEN CAST(" + start + " AS TIMESTAMP) ELSE CAST(NULL AS TIMESTAMP) END AS window_start"), sql);
        assertFalse(sql.contains("date_bin"), sql);
    }

    @Test
    void regionTimeZoneWithoutBoundedRangeFailsBeforeBorrow() {
        TSDBQuery query = aggregate("1d");
        query.setTimeZone("America/New_York");
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        verifyNoInteractions(physicalPool);
    }

    @Test
    void subdayRegionWindowAcrossDstFailsBeforeBorrow() {
        TSDBQuery query = aggregate("1h");
        query.setTimeZone("America/New_York");
        query.setStartTime(java.time.Instant.parse("2026-03-08T05:00:00Z").toEpochMilli());
        query.setEndTime(java.time.Instant.parse("2026-03-09T04:00:00Z").toEpochMilli());
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        verifyNoInteractions(physicalPool);
    }

    @Test
    void subdayRegionWindowUsesTheOffsetOfItsActualSummerRange() throws Exception {
        TSDBQuery query = aggregate("1h");
        query.setTimeZone("America/New_York");
        query.setStartTime(java.time.Instant.parse("2026-07-01T00:00:00Z").toEpochMilli());
        query.setEndTime(java.time.Instant.parse("2026-07-02T00:00:00Z").toEpochMilli());
        adapter.query(null, query);
        assertTrue(sql().contains("1970-01-01T00:00:00-04:00"));
    }

    private String sql() throws Exception {
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(session).executeQueryStatement(sql.capture());
        return sql.getValue();
    }

    private TSDBQuery detail() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        query.setLimit(10);
        return query;
    }

    private TSDBQuery aggregate(String window) {
        TSDBQuery query = detail();
        query.setGroupByTime(window);
        query.getAggregations().add(new AggregationSpec("value", AggregationFunctionEnum.AVG, "mean"));
        return query;
    }

    private TSDBRecord point(int index) {
        return new TSDBRecord("points", (long) index, Map.of("device", "a"), Map.of("value", index));
    }
}
