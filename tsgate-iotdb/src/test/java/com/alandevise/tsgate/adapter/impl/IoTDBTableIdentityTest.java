package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.IoTDBProperties;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.TSDBRecord;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IoTDBTableIdentityTest {
    private IoTDBTableAdapter adapter;
    private ITableSessionPool pool;
    private ITableSession session;

    @BeforeEach
    void setup() throws Exception {
        pool = mock(ITableSessionPool.class);
        session = mock(ITableSession.class);
        when(pool.getSession()).thenReturn(session);
        readyAdapter(2);
    }

    private void readyAdapter(int tabletRows) {
        IoTDBProperties config = new IoTDBProperties();
        config.setDatabase("table_contract");
        config.getTable().setTabletMaxRowSize(tabletRows);
        adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        IoTDBTestPools.ready(adapter, pool, "table_contract");
    }

    @Test
    void mergesTableNameVariantsPreservingRowOrderNullsAndCallerNames() throws Exception {
        readyAdapter(20);
        String[] tableNames = {"METRICS", "metrics", "MeTrIcS"};
        long[] timestamps = {9L, 3L, 8L, 2L, 7L, 1L};
        List<TSDBRecord> records = new ArrayList<>();
        for (int row = 0; row < timestamps.length; row++) {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("value", row + 0.25d);
            fields.put("optional", row % 2 == 0 ? null : (long) row);
            records.add(new TSDBRecord(tableNames[row % tableNames.length], timestamps[row],
                    Map.of("device", "sensor-" + row), fields));
        }

        BatchWriteResult result = adapter.batchWriteDetailed(null, records);
        assertTrue(result.isSuccess());
        assertEquals(6, result.requestedRecords());
        assertEquals(6, result.committedRecords());
        assertEquals(1, result.totalBatches());
        assertEquals(1, result.committedBatches());
        ArgumentCaptor<Tablet> captor = ArgumentCaptor.forClass(Tablet.class);
        verify(session).insert(captor.capture());
        Tablet tablet = captor.getValue();
        assertEquals("metrics", tablet.getDeviceId());
        assertEquals(20, tablet.getMaxRowNumber());
        assertEquals(6, tablet.getRowSize());
        for (int row = 0; row < timestamps.length; row++) {
            assertEquals(timestamps[row], tablet.getTimestamp(row));
            assertEquals("sensor-" + row, ((Binary) tablet.getValue(row, 0)).toString());
            assertEquals(row + 0.25d, tablet.getValue(row, 1));
            if (row % 2 == 0) {
                assertTrue(tablet.isNull(row, 2));
            } else {
                assertEquals((long) row, tablet.getValue(row, 2));
            }
            assertEquals(tableNames[row % tableNames.length], records.get(row).measurement());
            assertEquals(Map.of("device", "sensor-" + row), records.get(row).tags());
            assertEquals(List.of("value", "optional"), List.copyOf(records.get(row).fields().keySet()));
            assertEquals(row % 2 == 0 ? null : (long) row, records.get(row).fields().get("optional"));
        }
    }

    @Test
    void chunksTheMergedPhysicalTableAndConfirmsEveryInputRecord() throws Exception {
        List<String> physicalTables = new ArrayList<>();
        List<Integer> rowCounts = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        doAnswer(invocation -> {
            Tablet tablet = invocation.getArgument(0);
            physicalTables.add(tablet.getDeviceId());
            rowCounts.add(tablet.getRowSize());
            assertEquals(2, tablet.getMaxRowNumber());
            for (int row = 0; row < tablet.getRowSize(); row++) {
                timestamps.add(tablet.getTimestamp(row));
            }
            return null;
        }).when(session).insert(any(Tablet.class));
        List<TSDBRecord> records = new ArrayList<>();
        for (int row = 0; row < 7; row++) {
            records.add(point(row % 2 == 0 ? "METRICS" : "metrics", row));
        }

        BatchWriteResult result = adapter.batchWriteDetailed(null, records);
        assertTrue(result.isSuccess());
        assertEquals(7, result.committedRecords());
        assertEquals(4, result.totalBatches());
        assertEquals(4, result.committedBatches());
        assertEquals(List.of("metrics", "metrics", "metrics", "metrics"), physicalTables);
        assertEquals(List.of(2, 2, 2, 1), rowCounts);
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L), timestamps);
    }

    @ParameterizedTest
    @ValueSource(strings = {"role", "type", "null-then-role", "null-then-type"})
    void rejectsConflictsAcrossTableNameVariantsBeforeAnySessionIsBorrowed(String conflict) {
        List<TSDBRecord> records = new ArrayList<>();
        records.add(point("earlier_table", 1));
        records.add(point("earlier_table", 2));
        records.add(point("earlier_table", 3));
        records.add(point("metrics", 4));
        records.add(point("METRICS", 5));
        if (conflict.startsWith("null-then")) {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("value", 6d);
            fields.put("optional", null);
            records.add(new TSDBRecord("MeTrIcS", 6L, Map.of("device", "a"), fields));
        }
        TSDBRecord invalid = switch (conflict) {
            case "role" -> new TSDBRecord("METRICS", 7L, Map.of("other", "b"),
                    Map.of("value", 7d, "device", "field"));
            case "type" -> new TSDBRecord("MeTrIcS", 7L, Map.of("device", "b"),
                    Map.of("value", 7L));
            case "null-then-role" -> new TSDBRecord("METRICS", 7L,
                    Map.of("device", "b", "optional", "tag"), Map.of("value", 7d));
            case "null-then-type" -> {
                records.add(new TSDBRecord("METRICS", 7L, Map.of("device", "b"),
                        Map.of("value", 7d, "optional", 7L)));
                yield new TSDBRecord("MeTrIcS", 8L, Map.of("device", "c"),
                        Map.of("value", 8d, "optional", "string"));
            }
            default -> throw new AssertionError(conflict);
        };
        records.add(invalid);

        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, records));
        assertEquals(TSDBErrorCodeEnum.METADATA_ERROR, error.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        assertEquals(records.size(), error.getResult().requestedRecords());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(0, error.getResult().committedBatches());
        assertFalse(error.getResult().retryable());
        verifyNoInteractions(pool, session);
    }

    @Test
    void keepsDifferentPhysicalTablesAndTheirSchemasIndependent() throws Exception {
        List<TSDBRecord> records = List.of(point("Metrics", 1),
                new TSDBRecord("Meters", 2L, Map.of("device", "b"), Map.of("value", "text")),
                point("METRICS", 3));
        BatchWriteResult result = adapter.batchWriteDetailed(null, records);
        assertEquals(3, result.committedRecords());
        assertEquals(2, result.totalBatches());
        ArgumentCaptor<Tablet> captor = ArgumentCaptor.forClass(Tablet.class);
        verify(session, times(2)).insert(captor.capture());
        List<Tablet> tablets = captor.getAllValues();
        assertEquals(List.of("metrics", "meters"), tablets.stream().map(Tablet::getDeviceId).toList());
        assertEquals(2, tablets.get(0).getRowSize());
        assertEquals(TSDataType.DOUBLE, tablets.get(0).getSchemas().get(1).getType());
        assertEquals(1L, tablets.get(0).getTimestamp(0));
        assertEquals(3L, tablets.get(0).getTimestamp(1));
        assertEquals(1, tablets.get(1).getRowSize());
        assertEquals(TSDataType.STRING, tablets.get(1).getSchemas().get(1).getType());
        assertEquals("text", ((Binary) tablets.get(1).getValue(0, 1)).toString());
        assertEquals(List.of("Metrics", "Meters", "METRICS"),
                records.stream().map(TSDBRecord::measurement).toList());
    }

    private static TSDBRecord point(String table, long timestamp) {
        return new TSDBRecord(table, timestamp, Map.of("device", "a"), Map.of("value", 1d));
    }
}
