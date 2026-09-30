package com.alandevise.tsdb.adapter.impl;

import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.springframework.test.util.ReflectionTestUtils;

/** Installs a controlled pool for tests that exercise operations after initialization. */
final class IoTDBTestPools {
    private IoTDBTestPools() { }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static void ready(IoTDBTableAdapter adapter, ITableSessionPool physical, String database) {
        RecoverableTableSessionPool proxy = new RecoverableTableSessionPool(physical);
        ReflectionTestUtils.setField(adapter, "sessionPool", proxy);
        ReflectionTestUtils.setField(adapter, "recoverableSessionPool", proxy);
        ReflectionTestUtils.setField(adapter, "poolDatabase", database);
        Enum<?> current = (Enum<?>) ReflectionTestUtils.getField(adapter, "state");
        ReflectionTestUtils.setField(adapter, "state", Enum.valueOf((Class) current.getDeclaringClass(), "READY"));
    }
}
