package com.alandevise.tsdb.adapter.impl;

import com.alandevise.tsdb.config.IoTDBNodeDiscoveryModeEnum;
import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.exception.TSDBBatchWriteException;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.model.BatchCommitStateEnum;
import com.alandevise.tsdb.model.TSDBRecord;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IoTDBConfigurationSnapshotTest {
    @Test
    void initializationRetryAndPoolRecoveryPreserveTheConstructionSettings() throws Exception {
        IoTDBProperties properties = properties();
        properties.setMaxBatchRecords(2);
        properties.getTable().setTabletMaxRowSize(2);
        properties.getTable().setRpcCompressionEnabled(false);
        IoTDBProperties.IoTDBPoolConfig suppliedPool = properties.getPool();
        suppliedPool.setMaxSize(3);
        suppliedPool.setFetchSize(17);
        suppliedPool.setWaitToGetSessionTimeoutInMs(101L);
        suppliedPool.setConnectionTimeoutInMs(102);
        suppliedPool.setQueryTimeoutInMs(103L);
        suppliedPool.setMaxRetryCount(1);
        suppliedPool.setRetryIntervalInMs(104L);
        List<String> endpoints = suppliedPool.getNodeUrls();
        ITableSessionPool original = mock(ITableSessionPool.class);
        ITableSessionPool replacement = mock(ITableSessionPool.class);
        ITableSession session = mock(ITableSession.class);
        when(original.getSession()).thenReturn(session);
        when(replacement.getSession()).thenReturn(session);
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(properties, suppliedPool, false);
        properties.setUsername("changed_user");
        properties.setPassword("changed_password");
        properties.setDatabase("changed_database");
        properties.setDiscoveryMode(IoTDBNodeDiscoveryModeEnum.ENABLED);
        properties.setMaxBatchRecords(100);
        properties.setMaxQueryRows(100);
        properties.getTable().setTabletMaxRowSize(100);
        properties.getTable().setRpcCompressionEnabled(true);
        properties.setTable(null);
        endpoints.clear();
        endpoints.add("127.0.0.1:2");
        suppliedPool.setMaxSize(100);
        suppliedPool.setFetchSize(100);
        suppliedPool.setWaitToGetSessionTimeoutInMs(-1);
        suppliedPool.setConnectionTimeoutInMs(-1);
        suppliedPool.setQueryTimeoutInMs(-1);
        suppliedPool.setMaxRetryCount(100);
        suppliedPool.setRetryIntervalInMs(-1);
        suppliedPool.setEnabled(false);
        properties.setPool(null);
        try (MockedConstruction<TableSessionPoolBuilder> builders = mockConstruction(
                TableSessionPoolBuilder.class, withSettings().defaultAnswer(RETURNS_SELF),
                (builder, context) -> {
                    if (context.getCount() == 1) {
                        when(builder.build()).thenThrow(new IllegalStateException("synthetic resource failure"));
                    } else {
                        when(builder.build()).thenReturn(context.getCount() == 2 ? original : replacement);
                    }
                })) {
            try {
                assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                        assertThrows(TSDBException.class, adapter::init).getErrorCode());
                endpoints.clear();
                adapter.init();
                RecoverableTableSessionPool nativePool = (RecoverableTableSessionPool) adapter.getSessionPool();
                assertTrue(adapter.write(null, point(1)));
                ArgumentCaptor<Tablet> tablet = ArgumentCaptor.forClass(Tablet.class);
                verify(session).insert(tablet.capture());
                assertEquals(2, tablet.getValue().getMaxRowNumber());
                assertEquals(2, adapter.getMaxBatchRecords());
                assertEquals(BatchCommitStateEnum.NOT_COMMITTED, assertThrows(TSDBBatchWriteException.class,
                        () -> adapter.batchWriteDetailed(null, List.of(point(2), point(3), point(4)))).getResult().commitState());
                assertTrue(nativePool.replaceIfCurrent(nativePool.currentVersion(),
                        () -> ReflectionTestUtils.invokeMethod(adapter, "buildPhysicalSessionPool")));
                assertSame(nativePool, adapter.getSessionPool());
                assertEquals(3, builders.constructed().size());
                for (TableSessionPoolBuilder builder : builders.constructed()) {
                    verify(builder).nodeUrls(List.of("127.0.0.1:1"));
                    verify(builder).user("original_user");
                    verify(builder).password("original_password");
                    verify(builder).database("original_database");
                    verify(builder).maxSize(3);
                    verify(builder).fetchSize(17);
                    verify(builder).waitToGetSessionTimeoutInMs(101L);
                    verify(builder).connectionTimeoutInMs(102);
                    verify(builder).queryTimeoutInMs(103L);
                    verify(builder).maxRetryCount(1);
                    verify(builder).retryIntervalInMs(104L);
                    verify(builder).enableIoTDBRpcCompression(false);
                    verify(builder).enableAutoFetch(false);
                    verify(builder).enableRedirection(false);
                }
            } finally {
                adapter.close();
            }
        }
    }

    @Test
    void explicitlySuppliedPoolKeepsItsOwnEndpointsWhenPropertiesContainAnotherPool() {
        IoTDBProperties properties = properties();
        IoTDBProperties.IoTDBPoolConfig supplied = new IoTDBProperties.IoTDBPoolConfig();
        supplied.setNodeUrls(new ArrayList<>(List.of("127.0.0.1:3")));
        supplied.setMaxSize(4);
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(properties, supplied, false);
        supplied.getNodeUrls().clear();
        properties.getPool().getNodeUrls().clear();
        ITableSessionPool physical = mock(ITableSessionPool.class);
        try (MockedConstruction<TableSessionPoolBuilder> builders = mockConstruction(
                TableSessionPoolBuilder.class, withSettings().defaultAnswer(RETURNS_SELF),
                (builder, context) -> when(builder.build()).thenReturn(physical))) {
            try {
                adapter.init();
                verify(builders.constructed().get(0)).nodeUrls(List.of("127.0.0.1:3"));
                verify(builders.constructed().get(0)).maxSize(4);
            } finally {
                adapter.close();
            }
        }
    }

    @Test
    void nullSettingsRetainTheirExistingValidationErrors() {
        assertConfigurationError(() -> new IoTDBTableAdapter(null, null));
        IoTDBProperties properties = properties();
        properties.setTable(null);
        assertConfigurationError(() -> new IoTDBTableAdapter(properties, properties.getPool()));
        IoTDBProperties valid = properties();
        IoTDBTableAdapter missingPool = new IoTDBTableAdapter(valid, null);
        try {
            assertConfigurationError(missingPool::init);
        } finally {
            missingPool.close();
        }
        valid.getPool().setNodeUrls(null);
        IoTDBTableAdapter missingNodes = new IoTDBTableAdapter(valid, valid.getPool());
        try {
            valid.getPool().setNodeUrls(List.of("127.0.0.1:1"));
            assertConfigurationError(missingNodes::init);
        } finally {
            missingNodes.close();
        }
    }

    private static IoTDBProperties properties() {
        IoTDBProperties properties = new IoTDBProperties();
        properties.setUsername("original_user");
        properties.setPassword("original_password");
        properties.setDatabase("original_database");
        properties.setMaxQueryRows(2);
        properties.getPool().setNodeUrls(new ArrayList<>(List.of("127.0.0.1:1")));
        return properties;
    }

    private static TSDBRecord point(long time) {
        return new TSDBRecord("telemetry", time, Map.of("device", "a"), Map.of("value", 1d));
    }

    private static void assertConfigurationError(org.junit.jupiter.api.function.Executable action) {
        assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                assertThrows(TSDBException.class, action).getErrorCode());
    }
}
