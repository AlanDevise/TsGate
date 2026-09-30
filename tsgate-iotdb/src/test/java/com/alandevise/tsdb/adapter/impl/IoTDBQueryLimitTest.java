package com.alandevise.tsdb.adapter.impl;

import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.model.TSDBQuery;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class IoTDBQueryLimitTest {
    private final ITableSessionPool physical = mock(ITableSessionPool.class);
    private final ITableSession session = mock(ITableSession.class);
    private final SessionDataSet data = mock(SessionDataSet.class);

    private IoTDBTableAdapter adapter(int limit, int rows) throws Exception {
        IoTDBProperties config = new IoTDBProperties();
        config.setMaxQueryRows(limit);
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        IoTDBTestPools.ready(adapter, physical, config.getDatabase());
        when(physical.getSession()).thenReturn(session);
        when(session.executeQueryStatement(anyString())).thenReturn(data);
        when(data.getColumnNames()).thenReturn(List.of());
        AtomicInteger fetched = new AtomicInteger();
        when(data.hasNext()).thenAnswer(call -> fetched.get() < rows);
        when(data.next()).thenAnswer(call -> new RowRecord(fetched.getAndIncrement()));
        return adapter;
    }

    @Test
    void nativeQueryStopsBeforeReadingExcessRowsAndClosesEveryResource() throws Exception {
        IoTDBTableAdapter adapter = adapter(2, 100);
        TSDBException failure = assertThrows(TSDBException.class,
                () -> adapter.executeQuery("SELECT * FROM points"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
        assertTrue(failure.getMessage().contains("tsdb.iotdb.max-query-rows: 2"));
        verify(data, times(2)).next();
        verify(data).closeOperationHandle();
        verify(session).close();
        verify(physical, times(1)).getSession();
    }

    @Test
    void nativeQueryAcceptsExactlyTheConfiguredLimit() throws Exception {
        assertEquals(2, adapter(2, 2).executeQuery("SELECT * FROM points").getRowCount());
        verify(data).closeOperationHandle();
        verify(session).close();
    }

    @Test
    void paginationProbeAllowsOneSentinelAtTheLargestPageBoundary() throws Exception {
        IoTDBTableAdapter adapter = adapter(10_000, 10_001);
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        query.setLimit(10_001);
        query.setPaginationProbe(true);
        assertEquals(10_001, adapter.query(null, query).getRowCount());
        verify(data).closeOperationHandle();
        verify(session).close();
    }

    @Test
    void paginationProbeCannotReadBeyondTheSingleSentinelAllowance() throws Exception {
        IoTDBTableAdapter adapter = adapter(2, 100);
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        query.setPaginationProbe(true);
        assertThrows(TSDBException.class, () -> adapter.query(null, query));
        verify(data, times(3)).next();
        verify(data).closeOperationHandle();
        verify(session).close();
    }

    @Test
    void ordinaryStructuredQueryCannotUseThePaginationSentinelAllowance() throws Exception {
        IoTDBTableAdapter adapter = adapter(2, 3);
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        query.setLimit(3);
        assertFalse(query.isPaginationProbe());
        TSDBException failure = assertThrows(TSDBException.class, () -> adapter.query(null, query));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
        verify(data, times(2)).next();
        verify(data).closeOperationHandle();
        verify(session).close();
    }

    @Test
    void ordinaryStructuredQueryAcceptsExactlyTheConfiguredLimit() throws Exception {
        IoTDBTableAdapter adapter = adapter(2, 2);
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        assertEquals(2, adapter.query(null, query).getRowCount());
        verify(data).closeOperationHandle();
        verify(session).close();
    }

    @Test
    void limitFailureRemainsPrimaryWhenCleanupAlsoFails() throws Exception {
        IoTDBTableAdapter adapter = adapter(1, 2);
        StatementExecutionException cleanupFailure = new StatementExecutionException("cleanup failed");
        doThrow(cleanupFailure).when(data).closeOperationHandle();
        TSDBException failure = assertThrows(TSDBException.class,
                () -> adapter.executeQuery("SELECT * FROM points"));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
        assertArrayEquals(new Throwable[]{cleanupFailure}, failure.getSuppressed());
        verify(session).close();
        verify(physical, never()).close();
    }

    @Test
    void rowLimitMustBePositiveAndDefaultsToTenThousand() {
        IoTDBProperties config = new IoTDBProperties();
        assertEquals(10_000, config.getMaxQueryRows());
        config.setMaxQueryRows(0);
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                assertThrows(TSDBException.class, () -> new IoTDBTableAdapter(config, config.getPool())).getErrorCode());
        config.setMaxQueryRows(-1);
        assertThrows(TSDBException.class, () -> new IoTDBTableAdapter(config, config.getPool()));
        config.setMaxQueryRows(Integer.MAX_VALUE);
        assertThrows(TSDBException.class, () -> new IoTDBTableAdapter(config, config.getPool()));
        config.setMaxQueryRows(Integer.MAX_VALUE - 1);
        assertDoesNotThrow(() -> new IoTDBTableAdapter(config, config.getPool()));
    }
}
