package com.alandevise.tsgate.integration;

import com.alandevise.tsgate.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsgate.config.IoTDBProperties;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.TableSessionBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies adapter lifecycle promises through the actual native IoTDB pool. */
class IoTDBLifecycleDockerIT {
    private static final String ENDPOINT = System.getProperty("tsdb.it.iotdb.endpoint", "127.0.0.1:16667");
    private String database;
    private ITableSession admin;
    private IoTDBTableAdapter adapter;
    private TSDBRecord record;

    @BeforeEach
    void setup() throws Exception {
        database = "lifecycle_" + Long.toUnsignedString(System.nanoTime(), 36);
        admin = new TableSessionBuilder().nodeUrls(List.of(ENDPOINT)).username("root").password("root")
                .enableAutoFetch(false).enableRedirection(false).connectionTimeoutInMs(30000)
                .queryTimeoutInMs(30000).build();
        admin.executeNonQueryStatement("CREATE DATABASE " + database);
        admin.executeNonQueryStatement("CREATE TABLE " + database + ".samples (device STRING TAG, value DOUBLE FIELD)");
        IoTDBProperties properties = new IoTDBProperties();
        properties.setUsername("root");
        properties.setPassword("root");
        properties.setDatabase(database);
        properties.getPool().setNodeUrls(List.of(ENDPOINT));
        properties.getPool().setMaxSize(1);
        properties.getPool().setConnectionTimeoutInMs(30000);
        properties.getPool().setQueryTimeoutInMs(30000);
        properties.getPool().setWaitToGetSessionTimeoutInMs(30000);
        properties.getPool().setMaxRetryCount(0);
        adapter = new IoTDBTableAdapter(properties, properties.getPool(), false);
        adapter.init();
        record = new TSDBRecord("samples", System.currentTimeMillis() - 3600000,
                Map.of("device", "lifecycle"), Map.of("value", 7d));
        assertTrue(adapter.write(null, record));
    }

    @AfterEach
    void cleanup() throws Exception {
        if (adapter != null) adapter.close();
        if (admin != null) {
            try {
                if (database != null) admin.executeNonQueryStatement("DROP DATABASE " + database);
            } finally {
                admin.close();
            }
        }
    }

    @Test
    void repeatedInitializationKeepsTheExposedNativePoolUsable() throws Exception {
        ITableSessionPool injectedPool = adapter.getSessionPool();
        for (int attempt = 0; attempt < 3; attempt++) {
            adapter.init();
            assertSame(injectedPool, adapter.getSessionPool());
            try (ITableSession session = injectedPool.getSession();
                 var rows = session.executeQueryStatement("SELECT value FROM samples")) {
                assertTrue(rows.hasNext());
                assertEquals(7d, rows.next().getFields().get(0).getDoubleV());
                assertFalse(rows.hasNext());
            }
            assertEquals(1, adapter.executeQuery("SELECT * FROM samples").getRowCount());
        }
    }

    @Test
    void closeIsTerminalAndRejectsNewAdapterOrNativeWork() {
        ITableSessionPool injectedPool = adapter.getSessionPool();
        adapter.close();
        assertDoesNotThrow(adapter::close);
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, adapter::init).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, adapter::getSessionPool).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.executeQuery("SELECT * FROM samples")).getErrorCode());
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("samples");
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.query(null, query)).getErrorCode());
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, () -> adapter.write(null, record)).getErrorCode());
        assertThrows(IllegalStateException.class, injectedPool::getSession);
    }
}
