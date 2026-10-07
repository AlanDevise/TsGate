package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.config.IoTDBProperties;
import com.alandevise.tsgate.contract.SharedAdapterContract;
import com.alandevise.tsgate.contract.SharedAdapterFixture;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** Uses controlled SDK sessions while exercising the real IoTDB translation, validation and commit handling. */
class IoTDBSharedAdapterContractTest implements SharedAdapterContract {
    @Override
    public String backendId() { return "iotdb"; }

    @Override
    public SharedAdapterFixture createFixture() throws Exception { return new Fixture(); }

    private static final class Fixture implements SharedAdapterFixture {
        private final IoTDBProperties properties = new IoTDBProperties();
        private final ITableSessionPool physicalPool = mock(ITableSessionPool.class);
        private final ITableSession session = mock(ITableSession.class);
        private final Queue<SessionDataSet> queryReplies = new ArrayDeque<>();
        private final Queue<Boolean> writeFailures = new ArrayDeque<>();
        private final IoTDBTableAdapter adapter;
        private int ioCount;
        private String lastQuerySql;
        private String database = ORIGINAL_DATABASE;

        private Fixture() throws Exception {
            properties.setDatabase(ORIGINAL_DATABASE);
            properties.setMaxBatchRecords(ORIGINAL_MAX_BATCH_RECORDS);
            properties.setMaxQueryRows(ORIGINAL_MAX_QUERY_ROWS);
            properties.getPool().setNodeUrls(new ArrayList<>(List.of("127.0.0.1:6667")));
            properties.getTable().setTabletMaxRowSize(2);
            properties.getTable().setRpcCompressionEnabled(false);
            adapter = new IoTDBTableAdapter(properties, properties.getPool(), false);
            when(physicalPool.getSession()).thenReturn(session);
            when(session.executeQueryStatement(anyString())).thenAnswer(call -> {
                ioCount++;
                lastQuerySql = call.getArgument(0);
                SessionDataSet reply = queryReplies.poll();
                return reply == null ? dataSet(List.of(), List.of()) : reply;
            });
            doAnswer(call -> {
                database = ((String) call.getArgument(0)).substring("USE ".length());
                return null;
            }).when(session).executeNonQueryStatement(anyString());
            doAnswer(call -> {
                ioCount++;
                if (Boolean.TRUE.equals(writeFailures.poll())) throw new IoTDBConnectionException("connection lost after submission");
                return null;
            }).when(session).insert(any(Tablet.class));
        }

        @Override
        public TSDBAdapter adapter() { return adapter; }

        @Override
        public void initialize() { IoTDBTestPools.ready(adapter, physicalPool, ORIGINAL_DATABASE); }

        @Override
        public Object nativeResource() { return adapter.getSessionPool(); }

        @Override
        public void mutateOriginalConfiguration() {
            properties.setDatabase("changed_database");
            properties.setMaxBatchRecords(1);
            properties.setMaxQueryRows(100);
            properties.getPool().getNodeUrls().clear();
            properties.getPool().setConnectionTimeoutInMs(-1);
            properties.getTable().setTabletMaxRowSize(100);
            properties.getTable().setRpcCompressionEnabled(true);
        }

        @Override
        public int ioCount() { return ioCount; }

        @Override
        public String lastQuerySql() { return lastQuerySql; }

        @Override
        public String lastDatabase() { return database; }

        @Override
        public void enqueueRows(long... timestamps) {
            List<RowRecord> rows = new ArrayList<>();
            for (long timestamp : timestamps) {
                RowRecord row = mock(RowRecord.class);
                List<Field> values = List.of(field(TSDataType.TIMESTAMP, timestamp),
                        field(TSDataType.STRING, "a"), field(TSDataType.INT32, 1));
                when(row.getFields()).thenReturn(values);
                rows.add(row);
            }
            queryReplies.add(dataSet(List.of("time", "device", "value"), rows));
        }

        @Override
        public void enqueueCount(int total) {
            RowRecord row = mock(RowRecord.class);
            Field totalField = field(TSDataType.INT64, (long) total);
            when(row.getFields()).thenReturn(List.of(totalField));
            queryReplies.add(dataSet(List.of("total"), List.of(row)));
        }

        @Override
        public void enqueueWriteSuccess() { writeFailures.add(false); }

        @Override
        public void enqueueUnknownWriteFailure() { writeFailures.add(true); }

        @Override
        public java.util.Map<String, String> configurationValues() {
            IoTDBProperties captured = (IoTDBProperties) ReflectionTestUtils.getField(adapter, "config");
            return java.util.Map.of("tsdb.iotdb.table.rpc-compression-enabled", Boolean.toString(captured.getTable().isRpcCompressionEnabled()));
        }

        @Override
        public int physicalBatchSize() { return 2; }

        @Override
        public void close() { adapter.close(); }

        private static Field field(TSDataType type, Object value) {
            Field field = mock(Field.class);
            when(field.getDataType()).thenReturn(type);
            when(field.getObjectValue(type)).thenReturn(value);
            return field;
        }

        private static SessionDataSet dataSet(List<String> columns, List<RowRecord> rows) {
            SessionDataSet result = mock(SessionDataSet.class);
            when(result.getColumnNames()).thenReturn(columns);
            AtomicInteger cursor = new AtomicInteger();
            try {
                when(result.hasNext()).thenAnswer(call -> cursor.get() < rows.size());
                when(result.next()).thenAnswer(call -> rows.get(cursor.getAndIncrement()));
            } catch (Exception error) {
                throw new AssertionError("Cannot stub a local SDK result", error);
            }
            return result;
        }
    }
}
