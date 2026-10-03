package com.alandevise.tsgate.adapter.impl;

import com.alandevise.tsgate.config.IoTDBProperties;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class IoTDBLifecycleTest {
    @Test
    void repeatedInitPreservesExposedPoolAndDoesNotBuildOrCloseResourcesAgain() throws Exception {
        ITableSessionPool physical = mock(ITableSessionPool.class);
        ITableSession session = mock(ITableSession.class);
        when(physical.getSession()).thenReturn(session);
        IoTDBProperties config = config();
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        try (MockedConstruction<TableSessionPoolBuilder> builders = mockConstruction(
                TableSessionPoolBuilder.class, withSettings().defaultAnswer(RETURNS_SELF),
                (builder, context) -> when(builder.build()).thenReturn(physical))) {
            try {
                adapter.init();
                ITableSessionPool injected = adapter.getSessionPool();
                config.setDatabase("");
                adapter.init();
                adapter.init();
                assertSame(injected, adapter.getSessionPool());
                assertEquals(1, builders.constructed().size());
                try (ITableSession borrowed = injected.getSession()) {
                    borrowed.executeNonQueryStatement("SELECT 1");
                }
                verify(session).executeNonQueryStatement("SELECT 1");
                verify(physical, never()).close();
            } finally {
                adapter.close();
            }
            adapter.close();
            verify(physical, times(1)).close();
        }
    }

    @Test
    void initializationFailureCanRetryWithoutPublishingAnInvalidPool() {
        ITableSessionPool physical = mock(ITableSessionPool.class);
        IoTDBProperties config = config();
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        try (MockedConstruction<TableSessionPoolBuilder> builders = mockConstruction(
                TableSessionPoolBuilder.class, withSettings().defaultAnswer(RETURNS_SELF),
                (builder, context) -> {
                    if (context.getCount() == 1) {
                        when(builder.build()).thenThrow(new IllegalStateException("pool construction failed"));
                    } else {
                        when(builder.build()).thenReturn(physical);
                    }
                })) {
            assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    assertThrows(TSDBException.class, adapter::init).getErrorCode());
            assertNull(ReflectionTestUtils.getField(adapter, "sessionPool"));
            assertNull(ReflectionTestUtils.getField(adapter, "recoverableSessionPool"));
            assertNull(ReflectionTestUtils.getField(adapter, "poolDatabase"));
            assertStateError(adapter::getSessionPool);
            adapter.init();
            assertNotNull(adapter.getSessionPool());
            assertEquals(2, builders.constructed().size());
            adapter.close();
            verify(physical).close();
        }
    }

    @Test
    void concurrentInitializationPublishesOneStablePoolProxy() throws Exception {
        IoTDBProperties config = config();
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<ITableSessionPool>> initialized = new java.util.ArrayList<>();
            for (int i = 0; i < 8; i++) {
                initialized.add(executor.submit(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    adapter.init();
                    return adapter.getSessionPool();
                }));
            }
            start.countDown();
            ITableSessionPool expected = initialized.get(0).get(5, TimeUnit.SECONDS);
            for (Future<ITableSessionPool> result : initialized) {
                assertSame(expected, result.get(5, TimeUnit.SECONDS));
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            adapter.close();
        }
    }

    @Test
    void correctingInvalidConfigurationRequiresANewAdapter() {
        IoTDBProperties config = config();
        config.setDatabase("");
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        try {
            assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    assertThrows(TSDBException.class, adapter::init).getErrorCode());
            config.setDatabase("tsdb");
            assertEquals(TSDBErrorCodeEnum.CONFIGURATION_ERROR,
                    assertThrows(TSDBException.class, adapter::init).getErrorCode());
            IoTDBTableAdapter corrected = new IoTDBTableAdapter(config, config.getPool(), false);
            try {
                corrected.init();
                assertNotNull(corrected.getSessionPool());
            } finally {
                corrected.close();
            }
        } finally {
            adapter.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closedIsTerminalEvenWithoutPriorInitialization(boolean initialize) {
        IoTDBProperties config = config();
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        assertDataOperationsRejected(adapter);
        ITableSessionPool exposed = null;
        if (initialize) {
            adapter.init();
            exposed = adapter.getSessionPool();
        }
        adapter.close();
        adapter.close();
        assertStateError(adapter::init);
        assertDataOperationsRejected(adapter);
        assertNull(ReflectionTestUtils.getField(adapter, "sessionPool"));
        assertNull(ReflectionTestUtils.getField(adapter, "recoverableSessionPool"));
        if (exposed != null) {
            ITableSessionPool closed = exposed;
            assertThrows(IllegalStateException.class, closed::getSession);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeWaitsForInFlightAdapterOperationAndSessionReturn(boolean writing) throws Exception {
        IoTDBProperties config = config();
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        ITableSessionPool physical = mock(ITableSessionPool.class);
        ITableSession session = mock(ITableSession.class);
        SessionDataSet result = mock(SessionDataSet.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        when(physical.getSession()).thenReturn(session);
        when(session.executeQueryStatement(anyString())).thenReturn(result);
        when(result.getColumnNames()).thenReturn(List.of());
        if (writing) {
            doAnswer(call -> { entered.countDown(); assertTrue(finish.await(5, TimeUnit.SECONDS)); return null; })
                    .when(session).insert(any(Tablet.class));
        } else {
            when(result.hasNext()).thenAnswer(call -> {
                entered.countDown(); assertTrue(finish.await(5, TimeUnit.SECONDS)); return false;
            });
        }
        IoTDBTestPools.ready(adapter, physical, config.getDatabase());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> operation = executor.submit(() -> {
                if (writing) adapter.batchWriteDetailed(null, List.of(point()));
                else adapter.executeQuery("SELECT * FROM telemetry");
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Future<?> close = executor.submit(() -> { closing.countDown(); adapter.close(); });
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            verify(physical, never()).close();
            finish.countDown();
            operation.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            var ordered = inOrder(session, physical);
            ordered.verify(session).close();
            ordered.verify(physical).close();
            assertDataOperationsRejected(adapter);
        } finally {
            finish.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            adapter.close();
        }
    }

    @Test
    void nativeLeaseDefersPhysicalCloseButCannotPermitNewBorrowsAfterAdapterClose() throws Exception {
        IoTDBProperties config = config();
        IoTDBTableAdapter adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        ITableSessionPool physical = mock(ITableSessionPool.class);
        ITableSession session = mock(ITableSession.class);
        when(physical.getSession()).thenReturn(session);
        IoTDBTestPools.ready(adapter, physical, config.getDatabase());
        ITableSessionPool exposed = adapter.getSessionPool();
        ITableSession lease = exposed.getSession();
        adapter.close();
        verify(physical, never()).close();
        assertThrows(IllegalStateException.class, exposed::getSession);
        lease.close();
        verify(session).close();
        verify(physical).close();
    }

    private static void assertDataOperationsRejected(IoTDBTableAdapter adapter) {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("telemetry");
        assertStateError(adapter::getSessionPool);
        assertStateError(() -> adapter.query(null, query));
        assertStateError(() -> adapter.count(null, query));
        assertStateError(() -> adapter.executeQuery("SELECT * FROM telemetry"));
        assertStateError(() -> adapter.batchWriteDetailed(null, List.of(point())));
        assertStateError(() -> adapter.batchWriteDetailed(null, List.of()));
        assertStateError(() -> adapter.batchWriteDetailed(null, null));
        assertStateError(() -> adapter.batchWrite(null, List.of()));
    }

    private static void assertStateError(Executable action) {
        assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                assertThrows(TSDBException.class, action).getErrorCode());
    }

    private static TSDBRecord point() {
        return new TSDBRecord("telemetry", 1L, Map.of("device", "test"), Map.of("value", 1.0));
    }

    private static IoTDBProperties config() {
        IoTDBProperties config = new IoTDBProperties();
        config.setUsername("root");
        config.setPassword("test");
        config.getPool().setNodeUrls(List.of("127.0.0.1:1"));
        return config;
    }
}
