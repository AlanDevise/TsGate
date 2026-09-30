package com.alandevise.tsdb.adapter.impl;

import com.alandevise.tsdb.config.IoTDBProperties;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IoTDBRpcCompressionTest {

    @Test
    void omittedSettingExplicitlyPreservesCompressionEnabledByDefault() {
        IoTDBProperties properties = configuredProperties();
        assertTrue(properties.getTable().isRpcCompressionEnabled());
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(properties, properties.getPool(), false);
        try {
            adapter.init();
            assertEncoding(physicalPool(adapter), true, 10);
        } finally {
            adapter.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"true,9", "true,10", "true,12", "true,1024",
            "false,9", "false,10", "false,12", "false,1024"})
    void settingControlsRealSdkTabletEncodingWithoutChangingThriftTransport(boolean enabled, int rowCount) {
        IoTDBProperties properties = configuredProperties();
        properties.getTable().setRpcCompressionEnabled(enabled);
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(properties, properties.getPool(), false);
        try {
            adapter.init();
            assertEncoding(physicalPool(adapter), enabled, rowCount);
        } finally {
            adapter.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void recoveryFactoryPreservesCompressionWhenReplacingThePhysicalPool(boolean enabled) {
        IoTDBProperties properties = configuredProperties();
        properties.getTable().setRpcCompressionEnabled(enabled);
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(properties, properties.getPool(), false);
        try {
            adapter.init();
            RecoverableTableSessionPool managed = (RecoverableTableSessionPool) adapter.getSessionPool();
            SessionPool original = physicalPool(adapter);
            assertTrue(managed.replaceIfCurrent(managed.currentVersion(),
                    () -> ReflectionTestUtils.invokeMethod(adapter, "buildPhysicalSessionPool")));
            assertSame(managed, adapter.getSessionPool());
            SessionPool replacement = physicalPool(adapter);
            assertNotSame(original, replacement);
            assertEquals(Boolean.TRUE, ReflectionTestUtils.getField(original, "closed"));
            assertEncoding(replacement, enabled, 12);
        } finally {
            adapter.close();
        }
    }

    private static IoTDBProperties configuredProperties() {
        IoTDBProperties properties = new IoTDBProperties();
        properties.setUsername("test");
        properties.setPassword("test");
        properties.getPool().setNodeUrls(List.of("127.0.0.1:1"));
        return properties;
    }

    private static SessionPool physicalPool(IoTDBTableAdapter adapter) {
        Object version = ReflectionTestUtils.getField(adapter.getSessionPool(), "current");
        ITableSessionPool tablePool = (ITableSessionPool) ReflectionTestUtils.getField(version, "pool");
        return (SessionPool) ReflectionTestUtils.getField(tablePool, "sessionPool");
    }

    private static void assertEncoding(SessionPool pool, boolean enabled, int rowCount) {
        assertEquals(enabled, ReflectionTestUtils.getField(pool, "enableIoTDBRpcCompression"));
        assertEquals(Boolean.FALSE, ReflectionTestUtils.getField(pool, "enableThriftCompression"));
        // Construct an official Session and request without opening a network connection.
        Session session = ReflectionTestUtils.invokeMethod(pool, "constructNewSession");
        assertNotNull(session);
        Tablet tablet = new Tablet("telemetry", List.of("device", "value"),
                List.of(TSDataType.STRING, TSDataType.DOUBLE),
                List.of(ColumnCategory.TAG, ColumnCategory.FIELD), rowCount);
        for (int row = 0; row < rowCount; row++) {
            tablet.addTimestamp(row, 1_700_000_000_000L + row);
            tablet.addValue("device", row, "test");
            tablet.addValue("value", row, row + 0.25);
        }
        tablet.setRowSize(rowCount);
        TSInsertTabletReq request = ReflectionTestUtils.invokeMethod(
                session, "genTSInsertTabletReq", tablet, true, true);
        assertNotNull(request);
        boolean encoded = enabled && rowCount >= 10;
        assertEquals(encoded, request.isIsCompressed());
        assertEquals(encoded, request.isSetEncodingTypes());
        assertEquals(encoded, request.isSetCompressType());
        assertEquals(rowCount, request.getSize());
        if (!encoded) {
            ByteBuffer times = request.bufferForTimestamps().duplicate();
            assertEquals(rowCount * Long.BYTES, times.remaining());
            for (int row = 0; row < rowCount; row++) {
                assertEquals(1_700_000_000_000L + row, times.getLong());
            }
        }
    }
}
