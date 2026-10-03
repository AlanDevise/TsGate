package com.alandevise.tsdb.model;

import com.alandevise.tsdb.exception.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

class ModelContractTest {
    @Test void querySnapshotPreservesItsProbeFlagWithoutMutatingTheSource() {
        TSDBQuery original = new TSDBQuery();
        assertThat(original.isPaginationProbe()).isFalse();
        original.setPaginationProbe(true);
        TSDBQuery copy = original.copy();
        assertThat(copy.isPaginationProbe()).isTrue();
        copy.setPaginationProbe(false);
        assertThat(original.isPaginationProbe()).isTrue();
    }

    @Test void recordMapsAreDefensiveImmutableCopiesThatPreserveOrder() {
        Map<String, String> tags = new LinkedHashMap<>(); tags.put("device", "a"); tags.put("region", "east");
        Map<String, Object> fields = new LinkedHashMap<>(); fields.put("value", 2.5); fields.put("quality", true);
        TSDBRecord record = new TSDBRecord("metrics", 123L, tags, fields);
        tags.clear(); fields.clear();
        assertThat(record.tags().keySet()).containsExactly("device", "region");
        assertThat(record.fields().keySet()).containsExactly("value", "quality");
        assertThatThrownBy(() -> record.tags().put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> record.fields().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(new TSDBRecord("m", 1L, null, null).tags()).isEmpty();
        assertThat(new TSDBRecord("m", 1L, null, null).fields()).isEmpty();
    }

    @Test void queryFilterDefensivelyCopiesValues() {
        List<Object> values = new ArrayList<>(List.of(1, 2));
        QueryFilter filter = new QueryFilter("value", OperatorEnum.IN, values);
        values.clear();
        assertThat(filter.values()).containsExactly(1, 2);
        assertThatThrownBy(() -> filter.values().add(3)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(new QueryFilter("value", OperatorEnum.IN, null).values()).isEmpty();
    }

    @Test void sortingRequiresANonblankColumnAndDefaultsToDescending() {
        assertThat(new SortSpec(" value ", null)).isEqualTo(new SortSpec("value", SortOrderEnum.DESC));
        assertThatThrownBy(() -> new SortSpec(null, SortOrderEnum.ASC)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SortSpec(" \t", SortOrderEnum.ASC)).isInstanceOf(IllegalArgumentException.class);
        assertThat(SortOrderEnum.normalize(null)).isEqualTo(SortOrderEnum.DESC);
        assertThat(SortOrderEnum.normalize(SortOrderEnum.ASC)).isEqualTo(SortOrderEnum.ASC);
    }

    @Test void querySnapshotPreservesAllScalarsAndIsolatesMutableCollections() {
        TSDBQuery original = new TSDBQuery();
        original.setMeasurement("metrics"); original.setRecordType(String.class); original.setTimeColumn(" event_time ");
        original.setStartTime(1L); original.setEndTime(9L); original.setCursorTime(3L); original.setStrictCursor(true);
        original.setLimit(20); original.setOffset(40); original.setGroupByTime("1m"); original.setTimeZone("UTC");
        original.setCursorColumns(Arrays.asList("event_time", null, " ", "device"));
        original.setCursorValues(Map.of("event_time", 3L, "device", "a"));
        original.setSelectColumns(Arrays.asList(" value ", null, ""));
        original.setFilters(List.of(new QueryFilter("value", OperatorEnum.GT, List.of(1))));
        original.setGroupByTags(List.of("device"));
        original.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.AVG, "mean")));
        original.setSortSpecs(Arrays.asList(null, new SortSpec("value", SortOrderEnum.ASC)));
        TSDBQuery copy = original.copy();
        assertThat(copy).usingRecursiveComparison().isEqualTo(original);
        copy.getCursorColumns().clear(); copy.getCursorValues().clear(); copy.getSelectColumns().clear();
        copy.getFilters().clear(); copy.getGroupByTags().clear(); copy.getAggregations().clear(); copy.getSortSpecs().clear();
        assertThat(original.getCursorColumns()).containsExactly("event_time", "device");
        assertThat(original.getCursorValues()).hasSize(2);
        assertThat(original.getSelectColumns()).containsExactly("value");
        assertThat(original.getFilters()).hasSize(1);
        assertThat(original.getGroupByTags()).containsExactly("device");
        assertThat(original.getAggregations()).hasSize(1);
        assertThat(original.getSortSpecs()).containsExactly(new SortSpec("value", SortOrderEnum.ASC));
        assertThat(original.getOrder()).isEqualTo(SortOrderEnum.ASC);
    }

    @Test void queryCursorCopiesRetainRawEntriesForStrictValidation() {
        TSDBQuery query = new TSDBQuery();
        Map<String, Object> cursor = new LinkedHashMap<>();
        cursor.put("time", 1L); cursor.put(" time ", 2L); cursor.put("TIME", 3L);
        cursor.put("device", "a"); cursor.put(null, 5); cursor.put(" ", 5); cursor.put("missing", null);
        Map<String, Object> expected = new LinkedHashMap<>(cursor);
        query.setCursorValues(cursor); cursor.clear();
        assertThat(query.getCursorValues()).containsExactlyEntriesOf(expected);
        TSDBQuery copy = query.copy();
        assertThat(copy.getCursorValues()).containsExactlyEntriesOf(expected).isNotSameAs(query.getCursorValues());
        copy.getCursorValues().clear();
        assertThat(query.getCursorValues()).containsExactlyEntriesOf(expected);
    }

    @Test void querySettersHandleNullCollections() {
        TSDBQuery query = new TSDBQuery();
        query.setCursorValues(null); query.setCursorColumns(null); query.setSelectColumns(null); query.setFilters(null);
        query.setGroupByTags(null); query.setAggregations(null); query.setSortSpecs(null); query.setTimeColumn(" "); query.setOrder(null);
        assertThat(query.getCursorValues()).isEmpty(); assertThat(query.getCursorColumns()).isEmpty();
        assertThat(query.getSelectColumns()).isEmpty(); assertThat(query.getFilters()).isEmpty();
        assertThat(query.getGroupByTags()).isEmpty(); assertThat(query.getAggregations()).isEmpty(); assertThat(query.getSortSpecs()).isEmpty();
        assertThat(query.getTimeColumn()).isEqualTo("time"); assertThat(query.getOrder()).isEqualTo(SortOrderEnum.DESC);
        assertThat(query.hasAggregations()).isFalse();
    }

    @Test void failedQueryResultStartsWithIndependentEmptyPayloads() {
        QueryResult failed = QueryResult.failure("offline");
        QueryResult another = new QueryResult();
        another.getColumns().add("value"); another.getRows().add(Map.of("value", 1));
        assertThat(failed.isSuccess()).isFalse(); assertThat(failed.getMessage()).isEqualTo("offline");
        assertThat(failed.getRows()).isEmpty(); assertThat(failed.getColumns()).isEmpty(); assertThat(failed.getRowCount()).isZero();
    }

    @Test void cursorPageProtectsRowsAndCursorAgainstCallerMutation() {
        List<String> rows = new ArrayList<>(List.of("a"));
        Map<String, Object> cursor = new LinkedHashMap<>(Map.of("time", 12L, "device", "a"));
        PageResult<String> page = new PageResult<>(rows, cursor, true, 1, null);
        rows.clear(); cursor.clear();
        assertThat(page.getRows()).containsExactly("a");
        assertThat(page.getNextCursorTime()).isEqualTo(12L); assertThat(page.getNextCursor()).hasSize(2);
        assertThat(page.getOrder()).isEqualTo(SortOrderEnum.DESC);
        assertThat(page.getPageNum()).isNull(); assertThat(page.getTotal()).isNull();
        assertThatThrownBy(() -> page.getRows().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> page.getNextCursor().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void simpleTimeCursorAndEmptyPageExposeConsistentMetadata() {
        PageResult<String> page = new PageResult<>(List.of("a"), 23L, true, 1, SortOrderEnum.ASC);
        assertThat(page.getNextCursor()).containsExactlyEntriesOf(Map.of("time", 23L));
        PageResult<String> empty = new PageResult<>(null, (Long) null, false, 1, null);
        assertThat(empty.getRows()).isEmpty(); assertThat(empty.getNextCursor()).isEmpty();
        assertThat(empty.isHasNext()).isFalse(); assertThat(empty.getNextCursorTime()).isNull();
        PageResult<String> legacy = new PageResult<>(List.of("a"), false, 1, 2, 0, null);
        assertThat(legacy.getTotal()).isNull(); assertThat(legacy.getTotalPages()).isNull();
    }

    @Test void pageTotalsRoundUpWithoutOverflowAtLongMaxValue() {
        PageResult<String> page = new PageResult<>(List.of("a"), true, 2, 2, 2, SortOrderEnum.DESC, 5L);
        assertThat(page.getTotalPages()).isEqualTo(3L); assertThat(page.getTotal()).isEqualTo(5L);
        assertThat(page.getOffset()).isEqualTo(2); assertThat(page.getPageSize()).isEqualTo(2); assertThat(page.getLimit()).isEqualTo(2);
        PageResult<String> enormous = new PageResult<>(List.of(), false, 1, 2, 0, null, Long.MAX_VALUE);
        assertThat(enormous.getTotalPages()).isEqualTo(Long.MAX_VALUE / 2 + 1);
        assertThat(new PageResult<>(List.of(), false, 1, 2, 0, null, 0).getTotalPages()).isZero();
    }

    @Test void batchSuccessAccountsForEveryRequestedRecordAndBatch() throws Exception {
        BatchWriteResult result = BatchWriteResult.success(7, 3);
        assertThat(result.requestedRecords()).isEqualTo(7); assertThat(result.validatedRecords()).isEqualTo(7);
        assertThat(result.committedRecords()).isEqualTo(7); assertThat(result.totalBatches()).isEqualTo(3);
        assertThat(result.committedBatches()).isEqualTo(3); assertThat(result.failedBatchIndex()).isNull();
        assertThat(result.failedMeasurement()).isNull(); assertThat(result.retryable()).isFalse(); assertThat(result.isSuccess()).isTrue();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream stream = new ObjectOutputStream(output)) { stream.writeObject(result); }
        try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(output.toByteArray()))) {
            assertThat(stream.readObject()).isEqualTo(result);
        }
        assertThat(BatchWriteResult.emptySuccess().committedRecords()).isZero();
        assertThat(BatchWriteResult.emptySuccess().totalBatches()).isZero();
    }

    @ParameterizedTest @EnumSource(BatchCommitStateEnum.class)
    void batchSuccessMeansConfirmedSuccessStateOnly(BatchCommitStateEnum state) {
        BatchWriteResult result = new BatchWriteResult(1, 1, 0, 1, 0, 0, "m", state, false, "status");
        assertThat(result.isSuccess()).isEqualTo(state == BatchCommitStateEnum.SUCCESS);
    }

    @Test void exceptionsPreserveStableCodesAndOriginalCauses() {
        TSDBException root = new TSDBException(TSDBErrorCodeEnum.PERMISSION_ERROR, "denied");
        RuntimeException wrapper = new RuntimeException("wrapped", root);
        assertThat(root.getCode()).isEqualTo(TSDBErrorCodeEnum.PERMISSION_ERROR.getCode());
        assertThat(TSDBException.resolveErrorCode(wrapper, TSDBErrorCodeEnum.INTERNAL_ERROR)).isEqualTo(TSDBErrorCodeEnum.PERMISSION_ERROR);
        assertThat(TSDBException.resolveErrorCode(new RuntimeException(), null)).isEqualTo(TSDBErrorCodeEnum.INTERNAL_ERROR);
        assertThat(TSDBException.resolveErrorCode(null, TSDBErrorCodeEnum.WRITE_ERROR)).isEqualTo(TSDBErrorCodeEnum.WRITE_ERROR);
        assertThat(new TSDBException("legacy").getErrorCode()).isEqualTo(TSDBErrorCodeEnum.INTERNAL_ERROR);
        assertThat(new TSDBException("legacy", wrapper)).hasCause(wrapper);
        assertThat(new TSDBException(null, "fallback").getErrorCode()).isEqualTo(TSDBErrorCodeEnum.INTERNAL_ERROR);
    }

    @Test void batchExceptionPrioritizesUnknownCommitOverUnderlyingClassification() {
        BatchWriteResult unknown = new BatchWriteResult(1, 1, 0, 1, 0, 0, "m", BatchCommitStateEnum.UNKNOWN, true, "unknown");
        TSDBException cause = new TSDBException(TSDBErrorCodeEnum.CONNECTION_ERROR, "offline");
        TSDBBatchWriteException error = new TSDBBatchWriteException(unknown, cause);
        assertThat(error.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN);
        assertThat(error.getResult()).isSameAs(unknown); assertThat(error).hasMessage("unknown").hasCause(cause);
        assertThat(new TSDBBatchWriteException(null, cause).getErrorCode()).isEqualTo(TSDBErrorCodeEnum.CONNECTION_ERROR);
        assertThat(new TSDBBatchWriteException(null, new RuntimeException()).getErrorCode()).isEqualTo(TSDBErrorCodeEnum.WRITE_ERROR);
        assertThat(new TSDBBatchWriteException(TSDBErrorCodeEnum.ARGUMENT_ERROR, null, null))
                .hasMessage("Batch write failed");
    }

    @Test void publicErrorCodesAreDistinctSixDigitValues() {
        assertThat(Arrays.stream(TSDBErrorCodeEnum.values()).mapToInt(TSDBErrorCodeEnum::getCode).distinct().count())
                .isEqualTo(TSDBErrorCodeEnum.values().length);
        for (TSDBErrorCodeEnum code : TSDBErrorCodeEnum.values()) {
            assertThat(code.getCode()).isBetween(100300, 100399);
            assertThat(code.getMessage()).isNotBlank();
        }
    }
}
