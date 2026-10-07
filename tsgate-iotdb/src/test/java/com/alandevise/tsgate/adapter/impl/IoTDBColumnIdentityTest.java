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

class IoTDBColumnIdentityTest {
    private IoTDBTableAdapter adapter;
    private ITableSessionPool pool;
    private ITableSession session;

    @BeforeEach
    void setup() throws Exception {
        IoTDBProperties config = new IoTDBProperties();
        config.setDatabase("column_contract");
        config.getTable().setTabletMaxRowSize(2);
        adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        pool = mock(ITableSessionPool.class);
        session = mock(ITableSession.class);
        when(pool.getSession()).thenReturn(session);
        IoTDBTestPools.ready(adapter, pool, "column_contract");
    }

    @ParameterizedTest
    @ValueSource(strings = {"tags", "fields", "tag-field", "tag-null-field"})
    void rejectsCaseDuplicatesWithinOneRowBeforeBorrowingAnySession(String duplicate) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("device", "a");
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("value", 1d);
        switch (duplicate) {
            case "tags" -> tags.put("DEVICE", "b");
            case "fields" -> fields.put("VALUE", 2d);
            case "tag-field" -> fields.put("DEVICE", "b");
            case "tag-null-field" -> fields.put("DEVICE", null);
            default -> throw new AssertionError(duplicate);
        }
        TSDBRecord invalid = new TSDBRecord("metrics", 4L, tags, fields);
        assertPreflightFailure(List.of(point("earlier_table", 1), point("metrics", 2),
                point("metrics", 3), invalid));
        assertEquals(tags, invalid.tags());
        assertEquals(fields, invalid.fields());
    }

    @ParameterizedTest
    @ValueSource(strings = {"role", "type", "null-then-role", "null-then-type"})
    void rejectsConflictsAcrossRowsBeforeAnyMeasurementOrTabletIsWritten(String conflict) {
        List<TSDBRecord> records = new ArrayList<>();
        records.add(point("earlier_table", 1));
        records.add(point("metrics", 2));
        records.add(point("metrics", 3));
        if (conflict.startsWith("null-then")) {
            Map<String, Object> nullFields = new LinkedHashMap<>();
            nullFields.put("value", 4d);
            nullFields.put("OPTIONAL", null);
            records.add(new TSDBRecord("metrics", 4L, Map.of("device", "a"), nullFields));
        }
        TSDBRecord invalid = switch (conflict) {
            case "role" -> new TSDBRecord("metrics", 5L, Map.of("other", "b"),
                    Map.of("value", 5d, "DEVICE", "field"));
            case "type" -> new TSDBRecord("metrics", 5L, Map.of("DEVICE", "b"),
                    Map.of("VALUE", 5L));
            case "null-then-role" -> new TSDBRecord("metrics", 5L,
                    Map.of("device", "b", "optional", "tag"), Map.of("value", 5d));
            case "null-then-type" -> {
                records.add(new TSDBRecord("metrics", 5L, Map.of("device", "b"),
                        Map.of("value", 5d, "optional", 5L)));
                yield new TSDBRecord("metrics", 6L, Map.of("device", "c"),
                        Map.of("value", 6d, "Optional", "string"));
            }
            default -> throw new AssertionError(conflict);
        };
        records.add(invalid);
        assertPreflightFailure(records);
    }

    @Test
    void mergesCompatibleCaseVariantsAndPreservesAllValuesNullsAndCallerKeys() throws Exception {
        IoTDBProperties config = new IoTDBProperties();
        config.setDatabase("column_contract");
        config.getTable().setTabletMaxRowSize(20);
        adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        IoTDBTestPools.ready(adapter, pool, "column_contract");
        String[] tagNames = {"DEVICE", "device", "DeViCe"};
        String[] valueNames = {"value", "VALUE", "VaLuE"};
        String[] optionalNames = {"OPTIONAL", "Optional", "optional"};
        List<TSDBRecord> records = new ArrayList<>();
        for (int row = 0; row < 12; row++) {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put(valueNames[row % 3], row + 0.25d);
            if (row % 3 != 2) {
                fields.put(optionalNames[row % 3], row % 3 == 0 ? null : (long) row);
            }
            records.add(new TSDBRecord("metrics", (long) row,
                    Map.of(tagNames[row % 3], "sensor-" + row), fields));
        }
        BatchWriteResult result = adapter.batchWriteDetailed(null, records);
        assertEquals(12, result.committedRecords());
        assertEquals(1, result.committedBatches());
        ArgumentCaptor<Tablet> captor = ArgumentCaptor.forClass(Tablet.class);
        verify(session).insert(captor.capture());
        Tablet tablet = captor.getValue();
        assertEquals(List.of("device", "value", "optional"), tablet.getSchemas().stream()
                .map(schema -> schema.getMeasurementName()).toList());
        assertEquals(TSDataType.INT64, tablet.getSchemas().get(2).getType());
        assertEquals(12, tablet.getRowSize());
        for (int row = 0; row < records.size(); row++) {
            assertEquals("sensor-" + row, ((Binary) tablet.getValue(row, 0)).toString());
            assertEquals(row + 0.25d, tablet.getValue(row, 1));
            if (row % 3 == 1) {
                assertFalse(tablet.isNull(row, 2));
                assertEquals((long) row, tablet.getValue(row, 2));
            } else {
                assertTrue(tablet.isNull(row, 2));
            }
            assertEquals(List.of(tagNames[row % 3]), List.copyOf(records.get(row).tags().keySet()));
            assertEquals(valueNames[row % 3], records.get(row).fields().keySet().iterator().next());
        }
    }

    private void assertPreflightFailure(List<TSDBRecord> records) {
        TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                () -> adapter.batchWriteDetailed(null, records));
        assertEquals(TSDBErrorCodeEnum.METADATA_ERROR, error.getErrorCode());
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, error.getResult().commitState());
        assertEquals(0, error.getResult().committedRecords());
        assertEquals(0, error.getResult().committedBatches());
        assertFalse(error.getResult().retryable());
        verifyNoInteractions(pool, session);
    }

    private static TSDBRecord point(String table, long timestamp) {
        return new TSDBRecord(table, timestamp, Map.of("device", "a"), Map.of("value", 1d));
    }
}
