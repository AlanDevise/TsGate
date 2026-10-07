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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

    @ParameterizedTest
    @ValueSource(strings = {"missing-filter", "blank-column", "missing-operator", "no-values", "scalar-many",
            "between-one", "between-many", "null-scalar", "null-in", "null-between", "double-nan",
            "double-positive-infinity", "double-negative-infinity", "float-nan",
            "float-positive-infinity", "float-negative-infinity"})
    default void sharedInvalidFiltersFailBeforeQueryAndCountIo(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            QueryFilter filter = invalidFilter(scenario);
            TSDBQuery query = detail();
            query.setFilters(Arrays.asList(filter));
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> fixture.adapter().query(null, query));
            query.setLimit(0);
            query.setOffset(-1);
            query.setStrictCursor(true);
            query.setCursorValues(Map.of("unexpected", "ignored-count-cursor"));
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> fixture.adapter().count(null, query));
            assertEquals(Arrays.asList(filter), query.getFilters());
            assertEquals(0, query.getLimit());
            assertEquals(-1, query.getOffset());
            assertTrue(query.isStrictCursor());
            assertEquals(Map.of("unexpected", "ignored-count-cursor"), query.getCursorValues());
            assertEquals(0, fixture.ioCount());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"scalar-many", "between-one", "between-many", "double-nan", "double-positive-infinity",
            "double-negative-infinity", "float-nan", "float-positive-infinity", "float-negative-infinity"})
    default void sharedInvalidBuilderFiltersFailBeforeListAndOffsetPageIo(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            QueryFilter filter = invalidFilter(scenario);
            TGQueryBuilder<AggregateContractPoint> builder = new TGTemplate(fixture.adapter())
                    .query(AggregateContractPoint.class).where(filter.column(), filter.operator(), filter.values());
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> builder.list(Map.class));
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> builder.page(1, 2, Map.class));
            assertEquals(List.of(filter), builder.build().getFilters());
            assertEquals(0, fixture.ioCount());
        }
    }

    @Test
    default void sharedInvalidFiltersPrecedeOptionalCapabilityChecks() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = BackendCapability.STRICT_COMPOSITE_CURSOR.query();
            query.getFilters().add(invalidFilter("between-many"));
            assertError(TSDBErrorCodeEnum.ARGUMENT_ERROR, () -> fixture.adapter().query(null, query));
            assertEquals(0, fixture.ioCount());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"scalar", "between", "in", "literal-null"})
    default void sharedValidFiltersReachQueryCountAndTemplateUnchanged(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            QueryFilter filter = switch (scenario) {
                case "scalar" -> new QueryFilter("value", OperatorEnum.GE, List.of(1.5));
                case "between" -> new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1, 2));
                case "in" -> new QueryFilter("value", OperatorEnum.IN, List.of(1, 2));
                case "literal-null" -> new QueryFilter("device", OperatorEnum.EQ, List.of("null"));
                default -> throw new AssertionError(scenario);
            };
            TSDBQuery query = detail();
            query.setFilters(List.of(filter));
            fixture.enqueueRows();
            assertTrue(fixture.adapter().query(null, query).isSuccess());
            fixture.enqueueCount(0);
            assertEquals(0, fixture.adapter().count(null, query));
            TGQueryBuilder<AggregateContractPoint> builder = new TGTemplate(fixture.adapter())
                    .query(AggregateContractPoint.class).where(filter.column(), filter.operator(), filter.values());
            fixture.enqueueRows();
            assertTrue(builder.list(Map.class).isEmpty());
            fixture.enqueueCount(0);
            assertEquals(0L, builder.page(1, 2, Map.class).getTotal().longValue());
            assertEquals(List.of(filter), query.getFilters());
            assertEquals(List.of(filter), builder.build().getFilters());
            if (scenario.equals("literal-null")) {
                assertTrue(fixture.lastQuerySql().contains("'null'"), fixture.lastQuerySql());
            }
            assertEquals(4, fixture.ioCount());
        }
    }

    @Test
    default void sharedBuilderStillSkipsOptionalEmptyAndNullFilters() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TGQueryBuilder<AggregateContractPoint> builder = new TGTemplate(fixture.adapter())
                    .query(AggregateContractPoint.class)
                    .whereTag("device", "a")
                    .where(" ", OperatorEnum.EQ, 1)
                    .where("value", null, 1)
                    .where("value", OperatorEnum.EQ, (Object) null)
                    .where("value", OperatorEnum.IN, (List<?>) null)
                    .where("value", OperatorEnum.BETWEEN, List.of())
                    .where("value", OperatorEnum.IN, Arrays.asList((Object) null));
            assertEquals(List.of(new QueryFilter("device", OperatorEnum.EQ, List.of("a"))),
                    builder.build().getFilters());
            fixture.enqueueRows();
            assertTrue(builder.list(Map.class).isEmpty());
            fixture.enqueueCount(0);
            assertEquals(0L, builder.page(1, 2, Map.class).getTotal().longValue());
            assertTrue(fixture.lastQuerySql().contains("'a'"), fixture.lastQuerySql());
            assertFalse(fixture.lastQuerySql().contains("BETWEEN"), fixture.lastQuerySql());
            assertEquals(2, fixture.ioCount());
        }
    }

    private static QueryFilter invalidFilter(String scenario) {
        return switch (scenario) {
            case "missing-filter" -> null;
            case "blank-column" -> new QueryFilter(" ", OperatorEnum.EQ, List.of(1));
            case "missing-operator" -> new QueryFilter("value", null, List.of(1));
            case "no-values" -> new QueryFilter("value", OperatorEnum.IN, List.of());
            case "scalar-many" -> new QueryFilter("value", OperatorEnum.EQ, List.of(1, 2));
            case "between-one" -> new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1));
            case "between-many" -> new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1, 2, 3));
            case "null-scalar" -> new QueryFilter("value", OperatorEnum.EQ, Arrays.asList((Object) null));
            case "null-in" -> new QueryFilter("value", OperatorEnum.IN, Arrays.asList(1, null, 2));
            case "null-between" -> new QueryFilter("value", OperatorEnum.BETWEEN, Arrays.asList(1, null));
            case "double-nan" -> new QueryFilter("value", OperatorEnum.GT, List.of(Double.NaN));
            case "double-positive-infinity" -> new QueryFilter("value", OperatorEnum.GT, List.of(Double.POSITIVE_INFINITY));
            case "double-negative-infinity" -> new QueryFilter("value", OperatorEnum.GT, List.of(Double.NEGATIVE_INFINITY));
            case "float-nan" -> new QueryFilter("value", OperatorEnum.GT, List.of(Float.NaN));
            case "float-positive-infinity" -> new QueryFilter("value", OperatorEnum.GT, List.of(Float.POSITIVE_INFINITY));
            case "float-negative-infinity" -> new QueryFilter("value", OperatorEnum.GT, List.of(Float.NEGATIVE_INFINITY));
            default -> throw new AssertionError(scenario);
        };
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    default void sharedOversizedBatchFailsBeforeTraversalOrSnapshotAllocation(boolean huge) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            int requested = huge ? Integer.MAX_VALUE : fixture.adapter().getMaxBatchRecords() + 1;
            AtomicInteger sizeReads = new AtomicInteger();
            Collection<TSDBRecord> records = nonTraversableRecords(requested, sizeReads);
            TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                    () -> fixture.adapter().batchWriteDetailed(null, records));
            assertZeroCommitSizeFailure(failure, requested);
            assertTrue(sizeReads.get() > 0, "Reject the reported size without accessing record elements");
            assertEquals(0, fixture.ioCount());
        }
    }

    @Test
    default void sharedSnapshotGrowthStillFailsThePostCopyRecordLimit() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            int limit = fixture.adapter().getMaxBatchRecords();
            AtomicBoolean copied = new AtomicBoolean();
            Collection<TSDBRecord> records = new AbstractCollection<>() {
                private List<TSDBRecord> grownRecords() {
                    copied.set(true);
                    return Collections.nCopies(limit + 1, point(1));
                }

                @Override public int size() { return copied.get() ? limit + 1 : limit; }
                @Override public Iterator<TSDBRecord> iterator() { return grownRecords().iterator(); }
                @Override public Object[] toArray() { return grownRecords().toArray(); }
                @Override public <T> T[] toArray(T[] array) { return grownRecords().toArray(array); }
            };
            TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                    () -> fixture.adapter().batchWriteDetailed(null, records));
            assertTrue(copied.get(), "The original size is permitted; the oversized snapshot must be rejected");
            assertZeroCommitSizeFailure(failure, limit + 1);
            assertEquals(0, fixture.ioCount());
        }
    }

    @Test
    default void sharedExactBatchRecordLimitSucceedsAndOrdinaryWritesRemainUsable() throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            int limit = fixture.adapter().getMaxBatchRecords();
            int batches = (limit - 1) / fixture.physicalBatchSize() + 1;
            for (int batch = 0; batch < batches; batch++) fixture.enqueueWriteSuccess();
            BatchWriteResult result = fixture.adapter().batchWriteDetailed(null,
                    Collections.nCopies(limit, point(1)));
            assertTrue(result.isSuccess());
            assertEquals(limit, result.requestedRecords());
            assertEquals(limit, result.validatedRecords());
            assertEquals(limit, result.committedRecords());
            assertEquals(batches, result.totalBatches());
            assertEquals(batches, result.committedBatches());
            assertNull(result.failedBatchIndex());
            assertEquals(batches, fixture.ioCount());
            fixture.enqueueWriteSuccess();
            assertTrue(fixture.adapter().write(null, point(2)));
            assertEquals(batches + 1, fixture.ioCount());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    default void sharedBatchStateChecksPrecedeOversizedCollectionAccess(boolean closed) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            if (closed) {
                fixture.initialize();
                fixture.adapter().close();
            }
            AtomicInteger sizeReads = new AtomicInteger();
            Collection<TSDBRecord> records = nonTraversableRecords(Integer.MAX_VALUE, sizeReads);
            assertError(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                    () -> fixture.adapter().batchWriteDetailed(null, records));
            assertEquals(0, sizeReads.get(), "Adapter state must be checked before inspecting the collection");
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

    /** Reports an oversized batch without allocating elements; copying or traversal is a test failure. */
    private static Collection<TSDBRecord> nonTraversableRecords(int count, AtomicInteger sizeReads) {
        return new AbstractCollection<>() {
            @Override public int size() { sizeReads.incrementAndGet(); return count; }
            @Override public Iterator<TSDBRecord> iterator() { throw new AssertionError("Oversized batch must not be traversed"); }
            @Override public Object[] toArray() { throw new AssertionError("Oversized batch must not be copied"); }
            @Override public <T> T[] toArray(T[] array) { throw new AssertionError("Oversized batch must not be copied"); }
        };
    }

    private static void assertZeroCommitSizeFailure(TSDBBatchWriteException failure, int requested) {
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, failure.getErrorCode());
        BatchWriteResult result = failure.getResult();
        assertEquals(BatchCommitStateEnum.NOT_COMMITTED, result.commitState());
        assertEquals(requested, result.requestedRecords());
        assertEquals(0, result.validatedRecords());
        assertEquals(0, result.committedRecords());
        assertEquals(0, result.totalBatches());
        assertEquals(0, result.committedBatches());
        assertNull(result.failedBatchIndex());
        assertNull(result.failedMeasurement());
        assertFalse(result.retryable());
    }

    private static void assertError(TSDBErrorCodeEnum expected, org.junit.jupiter.api.function.Executable operation) {
        assertEquals(expected, assertThrows(TSDBException.class, operation).getErrorCode());
    }
}
