package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.IoTDBProperties;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.QueryResult;
import com.alandevise.tsgate.model.TSDBQuery;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class IoTDBResultColumnsTest {
    private IoTDBTableAdapter adapter;
    private ITableSessionPool physicalPool;
    private ITableSession session;
    private SessionDataSet dataSet;

    @BeforeEach
    void setup() throws Exception {
        IoTDBProperties config = new IoTDBProperties();
        config.setDatabase("result_columns");
        adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        physicalPool = mock(ITableSessionPool.class);
        session = mock(ITableSession.class);
        dataSet = mock(SessionDataSet.class);
        when(physicalPool.getSession()).thenReturn(session);
        when(session.executeQueryStatement(anyString())).thenReturn(dataSet);
        IoTDBTestPools.ready(adapter, physicalPool, config.getDatabase());
    }

    @AfterEach
    void cleanup() {
        adapter.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectsDuplicateColumnsBeforeReadingRowsIncludingNullFirstValues(boolean nullFirst) throws Exception {
        when(dataSet.getColumnNames()).thenReturn(List.of("duplicate", "duplicate"));
        when(dataSet.hasNext()).thenReturn(true, false);
        when(dataSet.next()).thenReturn(row(nullFirst ? null : 1d, 2d));

        TSDBException failure = assertThrows(TSDBException.class,
                () -> adapter.executeQuery("SELECT optional AS duplicate, value AS duplicate FROM points"));

        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
        assertTrue(failure.getMessage().contains("duplicate output column: duplicate"));
        verify(dataSet, never()).hasNext();
        verify(dataSet, never()).next();
        verify(dataSet).closeOperationHandle();
        verify(session).close();
        verify(physicalPool, times(1)).getSession();
    }

    @ParameterizedTest
    @ValueSource(strings = {"native", "query", "count"})
    void rejectsDuplicateMetadataForEmptyResultsOnEveryQueryPath(String operation) throws Exception {
        when(dataSet.getColumnNames()).thenReturn(List.of("duplicate", "duplicate"));
        when(dataSet.hasNext()).thenReturn(false);

        TSDBException failure = assertThrows(TSDBException.class, () -> execute(operation));

        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
        verify(dataSet, never()).hasNext();
        verify(dataSet, never()).next();
        verify(dataSet).closeOperationHandle();
        verify(session).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"native", "query"})
    void preservesCaseDistinctKeysNullsMissingFieldsAndRowOrder(String operation) throws Exception {
        List<String> columns = List.of("Upper", "upper", "nullable", "missing");
        when(dataSet.getColumnNames()).thenReturn(columns);
        when(dataSet.hasNext()).thenReturn(true, true, false);
        when(dataSet.next()).thenReturn(row(1d, 2d, null), row(3d, 4d, 5d, 6d));

        QueryResult result = execute(operation);

        assertTrue(result.isSuccess());
        assertEquals(columns, result.getColumns());
        assertEquals(2, result.getRowCount());
        assertEquals(columns, new ArrayList<>(result.getRows().get(0).keySet()));
        assertEquals(1d, result.getRows().get(0).get("Upper"));
        assertEquals(2d, result.getRows().get(0).get("upper"));
        assertNull(result.getRows().get(0).get("nullable"));
        assertNull(result.getRows().get(0).get("missing"));
        assertEquals(3d, result.getRows().get(1).get("Upper"));
        assertEquals(4d, result.getRows().get(1).get("upper"));
        assertEquals(5d, result.getRows().get(1).get("nullable"));
        assertEquals(6d, result.getRows().get(1).get("missing"));
        verify(dataSet).closeOperationHandle();
        verify(session).close();
    }

    @Test
    void duplicateColumnErrorRemainsPrimaryWhenBothCleanupStepsFail() throws Exception {
        when(dataSet.getColumnNames()).thenReturn(List.of("duplicate", "duplicate"));
        StatementExecutionException dataSetFailure = new StatementExecutionException("dataset cleanup failed");
        IoTDBConnectionException sessionFailure = new IoTDBConnectionException("session cleanup failed");
        doThrow(dataSetFailure).when(dataSet).closeOperationHandle();
        doThrow(sessionFailure).when(session).close();

        TSDBException failure = assertThrows(TSDBException.class,
                () -> adapter.executeQuery("SELECT value AS duplicate, optional AS duplicate FROM points"));

        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR, failure.getErrorCode());
        assertTrue(failure.getMessage().contains("duplicate output column: duplicate"));
        assertArrayEquals(new Throwable[]{dataSetFailure, sessionFailure}, failure.getSuppressed());
        verify(dataSet, never()).hasNext();
        verify(dataSet).closeOperationHandle();
        verify(session).close();
        verify(physicalPool, times(1)).getSession();
    }

    private QueryResult execute(String operation) {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        return switch (operation) {
            case "native" -> adapter.executeQuery("SELECT * FROM points");
            case "query" -> adapter.query(null, query);
            case "count" -> {
                adapter.count(null, query);
                yield null;
            }
            default -> throw new IllegalArgumentException(operation);
        };
    }

    private static RowRecord row(Double... values) {
        RowRecord row = new RowRecord(0);
        for (Double value : values) {
            row.addField(value, TSDataType.DOUBLE);
        }
        return row;
    }
}
