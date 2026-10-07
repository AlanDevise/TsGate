package com.alandevise.tsgate.contract;

import com.alandevise.tsgate.annotation.TGField;
import com.alandevise.tsgate.annotation.TGMeasurement;
import com.alandevise.tsgate.annotation.TGTag;
import com.alandevise.tsgate.annotation.TGTime;
import com.alandevise.tsgate.core.TGQueryBuilder;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.OperatorEnum;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.QueryResult;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Reusable behavioral invariants; dialect, protocol and error details remain in each backend's own tests. */
public interface SharedAdapterContract {
    String backendId();
    SharedAdapterFixture createFixture() throws Exception;

    @ParameterizedTest
    @ValueSource(strings = {"null", "zero-limit", "negative-limit", "negative-offset", "reversed-time"})
    default void sharedInvalidQueryFailsBeforeIo(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = detail();
            switch (scenario) {
                case "null" -> query = null;
                case "zero-limit" -> query.setLimit(0);
                case "negative-limit" -> query.setLimit(-1);
                case "negative-offset" -> query.setOffset(-1);
                case "reversed-time" -> { query.setStartTime(2L); query.setEndTime(1L); }
                default -> throw new AssertionError(scenario);
            }
            TSDBQuery argument = query;
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> fixture.adapter().query(null, argument));
            assertEquals(0, fixture.ioCount());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "reversed-time"})
    default void sharedCountRejectsInvalidBoundsBeforeIo(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = detail();
            query.setLimit(0);
            query.setOffset(-1);
            query.setStartTime(2L);
            query.setEndTime(1L);
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    () -> fixture.adapter().count(null, scenario.equals("null") ? null : query));
            assertEquals(0, fixture.ioCount());
        }
    }

    @Test
    default void sharedCountClearsPagingAndCursorsWithoutMutatingInput() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = detail();
            query.setLimit(0);
            query.setOffset(-1);
            query.setCursorTime(999L);
            query.setStrictCursor(true);
            query.setCursorValues(Map.of("unexpected", "cursor-only"));
            query.getFilters().add(new QueryFilter("device", OperatorEnum.EQ, List.of("a")));
            fixture.enqueueCount(2);
            assertEquals(2L, fixture.adapter().count(null, query));
            String sql = fixture.lastQuerySql();
            assertFalse(sql.contains("LIMIT"), sql);
            assertFalse(sql.contains("OFFSET"), sql);
            assertFalse(sql.contains("999"), sql);
            assertFalse(sql.contains("cursor-only"), sql);
            assertTrue(sql.contains("device"), sql);
            assertTrue(sql.contains("'a'"), sql);
            assertEquals(0, query.getLimit());
            assertEquals(-1, query.getOffset());
            assertEquals(999L, query.getCursorTime());
            assertTrue(query.isStrictCursor());
            assertEquals(Map.of("unexpected", "cursor-only"), query.getCursorValues());
            assertEquals(1, fixture.ioCount());
        }
    }

    @Test
    default void sharedConstructionSnapshotKeepsEndpointDatabaseAndLimits() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.mutateOriginalConfiguration();
            fixture.initialize();
            assertEquals(SharedAdapterFixture.ORIGINAL_MAX_BATCH_RECORDS, fixture.adapter().getMaxBatchRecords());
            fixture.enqueueRows(1L, 2L);
            assertEquals(2, fixture.adapter().query(null, detail()).getRowCount());
            assertEquals(SharedAdapterFixture.ORIGINAL_DATABASE, fixture.lastDatabase());
            fixture.enqueueRows(1L, 2L, 3L);
            assertError(TSDBErrorCodeEnum.QUERY_ERROR, () -> fixture.adapter().query(null, detail()));
            fixture.enqueueWriteSuccess();
            assertTrue(fixture.adapter().batchWriteDetailed(null, List.of(point(1), point(2))).isSuccess());
            assertEquals(3, fixture.ioCount());
        }
    }

    @Test
    default void sharedInitializationKeepsNativeIdentityAndCloseIsTerminal() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            Object nativeResource = fixture.nativeResource();
            assertNotNull(nativeResource);
            fixture.adapter().init();
            assertSame(nativeResource, fixture.nativeResource());
            fixture.adapter().close();
            fixture.adapter().close();
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, fixture.adapter()::init);
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, () -> fixture.adapter().query(null, detail()));
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, () -> fixture.adapter().count(null, detail()));
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, () -> fixture.adapter().batchWriteDetailed(null, List.of()));
            assertEquals(0, fixture.ioCount());
        }
    }

    @Test
    default void sharedOperationsRejectUninitializedStateAndClosingBeforeInitIsTerminal() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, () -> fixture.adapter().query(null, detail()));
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, () -> fixture.adapter().count(null, detail()));
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, () -> fixture.adapter().batchWriteDetailed(null, List.of()));
            fixture.adapter().close();
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR, fixture.adapter()::init);
            assertEquals(0, fixture.ioCount());
        }
    }

    @Test
    default void sharedLateBatchValidationConfirmsNoSubmission() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBRecord invalid = new TSDBRecord("points", null, Map.of("device", "a"), Map.of("value", 3));
            TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                    () -> fixture.adapter().batchWriteDetailed(null, List.of(point(1), invalid)));
            assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
            assertEquals(BatchCommitStateEnum.NOT_COMMITTED, failure.getResult().commitState());
            assertEquals(0, failure.getResult().committedRecords());
            assertEquals(0, failure.getResult().committedBatches());
            assertEquals(0, failure.getResult().totalBatches());
            assertEquals(0, fixture.ioCount());
        }
    }

    @Test
    default void sharedPhysicalBatchesReportAllConfirmedRecords() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.mutateOriginalConfiguration();
            fixture.initialize();
            int size = fixture.physicalBatchSize() + 1;
            fixture.enqueueWriteSuccess();
            fixture.enqueueWriteSuccess();
            BatchWriteResult result = fixture.adapter().batchWriteDetailed(null, points(size));
            assertTrue(result.isSuccess());
            assertEquals(size, result.requestedRecords());
            assertEquals(size, result.validatedRecords());
            assertEquals(size, result.committedRecords());
            assertEquals(2, result.totalBatches());
            assertEquals(2, result.committedBatches());
            assertNull(result.failedBatchIndex());
            assertEquals(2, fixture.ioCount());
        }
    }

    @Test
    default void sharedUncertainSecondBatchRetainsConfirmedLowerBoundWithoutReplay() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            int confirmed = fixture.physicalBatchSize();
            fixture.enqueueWriteSuccess();
            fixture.enqueueUnknownWriteFailure();
            TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                    () -> fixture.adapter().batchWriteDetailed(null, points(confirmed + 1)));
            assertEquals(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN, failure.getErrorCode());
            assertEquals(BatchCommitStateEnum.UNKNOWN, failure.getResult().commitState());
            assertEquals(confirmed, failure.getResult().committedRecords());
            assertEquals(1, failure.getResult().committedBatches());
            assertEquals(2, failure.getResult().totalBatches());
            assertEquals(1, failure.getResult().failedBatchIndex());
            assertEquals(2, fixture.ioCount(), "An uncertain batch must not be replayed implicitly");
        }
    }

    @Test
    default void sharedPaginationAllowsExactlyOneExplicitLookaheadRow() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = detail();
            query.setLimit(3);
            fixture.enqueueRows(1L, 2L, 3L);
            assertError(TSDBErrorCodeEnum.QUERY_ERROR, () -> fixture.adapter().query(null, query));
            query.setPaginationProbe(true);
            fixture.enqueueRows(1L, 2L, 3L);
            assertEquals(3, fixture.adapter().query(null, query).getRowCount());
            fixture.enqueueRows(1L, 2L, 3L, 4L);
            assertError(TSDBErrorCodeEnum.QUERY_ERROR, () -> fixture.adapter().query(null, query));
            assertEquals(3, fixture.ioCount());
        }
    }

    @Test
    default void sharedResultTimePreservesAnExactEpochMillisecondLong() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            long timestamp = 9_007_199_254_740_993L;
            fixture.enqueueRows(timestamp);
            QueryResult result = fixture.adapter().query(null, detail());
            assertTrue(result.isSuccess());
            assertEquals(timestamp, assertInstanceOf(Long.class, result.getRows().get(0).get("time")));
            assertEquals(1, fixture.ioCount());
        }
    }

    @ParameterizedTest
    @EnumSource(BackendCapability.class)
    default void sharedCapabilityMatrixIsExercisedInsteadOfSkipped(BackendCapability capability) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            for (BackendContractRegistry.Condition condition : BackendContractRegistry.conditions(backendId(), capability)) {
                assertEquals(condition.requiredValue(), fixture.configurationValues().get(condition.property()),
                        "Fixture must apply the declared prerequisite for servers " + condition.serverVersions());
            }
            if (capability == BackendCapability.BATCH_WRITE) {
                assertTrue(BackendContractRegistry.profile(backendId()).supports(capability));
                int batches = (11 + fixture.physicalBatchSize() - 1) / fixture.physicalBatchSize();
                for (int index = 0; index < batches; index++) fixture.enqueueWriteSuccess();
                assertEquals(11, fixture.adapter().batchWriteDetailed(null, points(11)).committedRecords());
                assertEquals(batches, fixture.ioCount());
                return;
            }
            TSDBQuery query = capability.query();
            if (BackendContractRegistry.profile(backendId()).supports(capability)) {
                fixture.enqueueRows();
                assertTrue(fixture.adapter().query(null, query).isSuccess());
                assertEquals(1, fixture.ioCount());
                assertTrue(fixture.lastQuerySql().contains("ORDER BY"), fixture.lastQuerySql());
                if (capability == BackendCapability.STRICT_COMPOSITE_CURSOR
                        && BackendContractRegistry.profile(backendId()).capabilities().get(capability)
                        == BackendContractRegistry.Status.CONFIG_REQUIRED)
                    assertTrue(fixture.lastQuerySql().contains("UNION ALL"), fixture.lastQuerySql());
            } else {
                assertError(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION, () -> fixture.adapter().query(null, query));
                assertEquals(0, fixture.ioCount());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"aliases", "tag-alias", "tags", "window-tag", "window-alias"})
    default void sharedAggregateOutputCollisionsFailBeforeQueryCountOrTemplateIo(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = aggregateOutputQuery(scenario, false);
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> fixture.adapter().query(null, query));
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> fixture.adapter().count(null, query));
            TGQueryBuilder<AggregateContractPoint> builder = aggregateTemplateQuery(fixture, query);
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> builder.list(Map.class));
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> builder.page(1, 2, Map.class));
            assertEquals(0, fixture.ioCount());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"aliases", "tag-alias", "tags", "window-tag", "window-alias"})
    default void sharedAggregateOutputCaseUsesTheBackendColumnIdentity(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = aggregateOutputQuery(scenario, true);
            TGQueryBuilder<AggregateContractPoint> builder = aggregateTemplateQuery(fixture, query);
            boolean foldsCase = fixture.adapter().normalizeColumnIdentifier("total")
                    .equals(fixture.adapter().normalizeColumnIdentifier("TOTAL"));
            if (foldsCase) {
                assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> fixture.adapter().query(null, query));
                assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> fixture.adapter().count(null, query));
                assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> builder.list(Map.class));
                assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> builder.page(1, 2, Map.class));
                assertEquals(0, fixture.ioCount());
            } else {
                fixture.enqueueRows();
                assertTrue(fixture.adapter().query(null, query).isSuccess());
                fixture.enqueueCount(0);
                assertEquals(0, fixture.adapter().count(null, query));
                fixture.enqueueRows();
                assertTrue(builder.list(Map.class).isEmpty());
                fixture.enqueueCount(0);
                assertEquals(0L, builder.page(1, 2, Map.class).getTotal().longValue());
                assertEquals(4, fixture.ioCount());
            }
        }
    }

    @Test
    default void sharedWindowStartAliasIsAvailableWithoutAGeneratedWindow() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = detail();
            query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "window_start")));
            fixture.enqueueRows();
            assertTrue(fixture.adapter().query(null, query).isSuccess());
            fixture.enqueueCount(0);
            assertEquals(0, fixture.adapter().count(null, query));
            assertEquals(2, fixture.ioCount());
        }
    }

    private static TSDBQuery aggregateOutputQuery(String scenario, boolean caseDistinct) {
        TSDBQuery query = detail();
        query.setStartTime(0L);
        query.setEndTime(3_600_000L);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        String total = caseDistinct ? "TOTAL" : "total";
        String window = caseDistinct ? "WINDOW_START" : "window_start";
        switch (scenario) {
            case "aliases" -> query.getAggregations().add(new AggregationSpec("value", AggregationFunctionEnum.MAX, total));
            case "tag-alias" -> query.setGroupByTags(List.of(total));
            case "tags" -> query.setGroupByTags(List.of("device", caseDistinct ? "DEVICE" : "device"));
            case "window-tag" -> { query.setGroupByTime("1h"); query.setGroupByTags(List.of(window)); }
            case "window-alias" -> {
                query.setGroupByTime("1h");
                query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, window)));
            }
            default -> throw new AssertionError(scenario);
        }
        return query;
    }

    private static TGQueryBuilder<AggregateContractPoint> aggregateTemplateQuery(SharedAdapterFixture fixture,
                                                                                TSDBQuery query) {
        TGQueryBuilder<AggregateContractPoint> builder = new TGTemplate(fixture.adapter()).query(AggregateContractPoint.class)
                .groupByTags(query.getGroupByTags());
        if (query.getStartTime() != null && query.getEndTime() != null) {
            builder.timeRange(query.getStartTime(), query.getEndTime());
        }
        if (query.getGroupByTime() != null) {
            builder.groupByTime(query.getGroupByTime());
        }
        for (AggregationSpec aggregation : query.getAggregations()) {
            builder.aggregate(aggregation.field(), aggregation.function(), aggregation.alias());
        }
        return builder;
    }

    @TGMeasurement("points")
    class AggregateContractPoint {
        @TGTime public Long time;
        @TGTag public String device;
        @TGField public Integer value;
    }

    static TSDBQuery detail() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("points");
        return query;
    }

    private static TSDBRecord point(int time) {
        return new TSDBRecord("points", (long) time, Map.of("device", "a"), Map.of("value", time));
    }

    private static List<TSDBRecord> points(int count) {
        List<TSDBRecord> records = new ArrayList<>(count);
        for (int index = 0; index < count; index++) records.add(point(index));
        return records;
    }

    private static void assertError(TSDBErrorCodeEnum expected, org.junit.jupiter.api.function.Executable operation) {
        assertEquals(expected, assertThrows(TSDBException.class, operation).getErrorCode());
    }
}
