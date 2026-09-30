package com.alandevise.tsdb.integration;

import com.alandevise.tsdb.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.exception.TSDBBatchWriteException;
import com.alandevise.tsdb.model.BatchCommitStateEnum;
import com.alandevise.tsdb.model.TSDBRecord;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies compression thresholds and full-size Tablets against explicitly selected real servers. */
class TabletCompressionDockerIT {
    static Stream<Arguments> tabletCases() {
        return Stream.of(false, true).flatMap(enabled -> Stream.of(9, 10, 12, 1024, 1025)
                .map(size -> Arguments.of(enabled, size)));
    }

    @ParameterizedTest(name = "compression={0}, rows={1}")
    @MethodSource("tabletCases")
    void roundTripOrExplicitLegacyRejection(boolean compression, int size) throws Exception {
        String endpoint = System.getProperty("tsdb.it.iotdb.endpoint", "127.0.0.1:17677");
        boolean legacyServer = Boolean.getBoolean("tsdb.it.iotdb.legacy-compression-rejection");
        String database = "it_compress_" + Long.toUnsignedString(System.nanoTime(), 36);
        try (ITableSession admin = new TableSessionBuilder().nodeUrls(List.of(endpoint))
                .username("root").password("root").enableAutoFetch(false).enableRedirection(false)
                .connectionTimeoutInMs(30000).queryTimeoutInMs(30000).build()) {
            admin.executeNonQueryStatement("CREATE DATABASE " + database);
            try {
                admin.executeNonQueryStatement("CREATE TABLE " + database
                        + ".samples (device STRING TAG, value DOUBLE FIELD, ival INT32 FIELD, lval INT64 FIELD,"
                        + " fval FLOAT FIELD, active BOOLEAN FIELD, label STRING FIELD, optional STRING FIELD)");
                IoTDBProperties settings = new IoTDBProperties();
                settings.setUsername("root");
                settings.setPassword("root");
                settings.setDatabase(database);
                settings.setMaxBatchRecords(2048);
                settings.getTable().setRpcCompressionEnabled(compression);
                assertEquals(1024, settings.getTable().getTabletMaxRowSize());
                settings.getPool().setNodeUrls(List.of(endpoint));
                settings.getPool().setConnectionTimeoutInMs(30000);
                settings.getPool().setQueryTimeoutInMs(30000);
                settings.getPool().setWaitToGetSessionTimeoutInMs(30000);
                settings.getPool().setMaxRetryCount(0);
                settings.getPool().setRetryIntervalInMs(10);
                IoTDBTableAdapter adapter = new IoTDBTableAdapter(settings, settings.getPool(), false);
                try {
                    adapter.init();
                    long start = System.currentTimeMillis() - 3600000;
                    List<TSDBRecord> input = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        Map<String, Object> fields = new LinkedHashMap<>();
                        fields.put("value", i + 0.25d);
                        fields.put("ival", i);
                        fields.put("lval", 12345678901L + i);
                        fields.put("fval", i + 0.5f);
                        fields.put("active", i % 2 == 0);
                        fields.put("label", "sensor-'\\-" + i);
                        fields.put("optional", i % 3 == 0 ? null : "present-" + i);
                        input.add(new TSDBRecord("samples", start + i, Map.of("device", "unit" + i % 3), fields));
                    }
                    if (legacyServer && compression && size >= 10) {
                        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                                () -> adapter.batchWriteDetailed(null, input));
                        assertEquals(BatchCommitStateEnum.UNKNOWN, failure.getResult().commitState());
                        assertEquals(0, failure.getResult().committedRecords());
                        assertEquals(0, failure.getResult().failedBatchIndex());
                        assertTrue(failure.getMessage().contains("301"), failure.getMessage());
                        assertEquals(0, adapter.executeQuery("SELECT * FROM samples").getRowCount());
                        System.out.println("EXPECTED_LEGACY_COMPRESSION_REJECTION size=" + size + " observedRows=0");
                        return;
                    }
                    var result = adapter.batchWriteDetailed(null, input);
                    assertTrue(result.isSuccess());
                    assertEquals(size, result.committedRecords());
                    assertEquals((size + 1023) / 1024, result.totalBatches());
                    assertEquals(result.totalBatches(), result.committedBatches());
                    var rows = adapter.executeQuery("SELECT * FROM samples ORDER BY time ASC").getRows();
                    assertEquals(size, rows.size());
                    for (int i = 0; i < size; i++) {
                        Map<String, Object> row = rows.get(i);
                        assertEquals(start + i, ((Number) row.get("time")).longValue());
                        assertEquals("unit" + i % 3, row.get("device"));
                        assertEquals(i + 0.25d, ((Number) row.get("value")).doubleValue());
                        assertEquals(i, ((Number) row.get("ival")).intValue());
                        assertEquals(12345678901L + i, ((Number) row.get("lval")).longValue());
                        assertEquals(i + 0.5f, ((Number) row.get("fval")).floatValue());
                        assertEquals(i % 2 == 0, row.get("active"));
                        assertEquals("sensor-'\\-" + i, row.get("label"));
                        assertEquals(i % 3 == 0 ? null : "present-" + i, row.get("optional"));
                    }
                    System.out.println("COMPRESSION_ROUND_TRIP_PASS enabled=" + compression + " rows=" + size);
                } finally {
                    adapter.close();
                }
            } finally {
                admin.executeNonQueryStatement("DROP DATABASE " + database);
            }
        }
    }
}
