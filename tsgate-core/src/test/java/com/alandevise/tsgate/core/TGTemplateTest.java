package com.alandevise.tsgate.core;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.annotation.*;
import com.alandevise.tsgate.exception.*;
import com.alandevise.tsgate.metadata.DefaultTSDBMetadataResolver;
import com.alandevise.tsgate.metadata.TSDBMetadataResolver;
import com.alandevise.tsgate.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TGTemplateTest {
    TSDBAdapter adapter;
    TGTemplate template;

    @BeforeEach void setUp() {
        adapter = mock(TSDBAdapter.class);
        when(adapter.getMaxBatchRecords()).thenReturn(100);
        when(adapter.normalizeColumnIdentifier(any())).thenCallRealMethod();
        template = new TGTemplate(adapter);
    }

    @Test void emptyBatchesAreSuccessfulWithoutAnAdapterOrDatabaseIo() {
        TGTemplate unconfigured = new TGTemplate(null);
        assertThat(unconfigured.batchWrite((Collection<?>) null)).isTrue();
        assertThat(unconfigured.batchWriteDetailed(List.of())).isEqualTo(BatchWriteResult.emptySuccess());
        assertThat(unconfigured.batchWrite(Point.class, List.of())).isTrue();
        verify(adapter, never()).batchWriteDetailed(any(), any());
    }

    @Test void writeConvertsAnnotationsAndUsesRequestedDatabase() {
        when(adapter.batchWriteDetailed(eq("db"), any())).thenReturn(BatchWriteResult.success(1, 1));
        assertThat(template.write("db", new Point(5L, "meter", 2.5))).isTrue();
        @SuppressWarnings("unchecked") ArgumentCaptor<Collection<TSDBRecord>> captured = ArgumentCaptor.forClass(Collection.class);
        verify(adapter).batchWriteDetailed(eq("db"), captured.capture());
        TSDBRecord record = captured.getValue().iterator().next();
        assertThat(record.measurement()).isEqualTo("metrics");
        assertThat(record.timestamp()).isEqualTo(5L);
        assertThat(record.tags()).containsExactlyEntriesOf(Map.of("device_code", "meter"));
        assertThat(record.fields()).containsExactlyEntriesOf(Map.of("value", 2.5));
        assertThatThrownBy(() -> captured.getValue().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void writeAndBatchOverloadsUseAdapterDefaultDatabase() {
        when(adapter.batchWriteDetailed(isNull(), any())).thenAnswer(invocation ->
                BatchWriteResult.success(((Collection<?>) invocation.getArgument(1)).size(), 1));
        assertThat(template.write(new Point(1L, "a", 1.0))).isTrue();
        assertThat(template.batchWrite(List.of(new Point(2L, "b", 2.0)))).isTrue();
        assertThat(template.batchWrite(Point.class, List.of(new Point(3L, "c", 3.0)))).isTrue();
        assertThat(template.batchWriteDetailed(List.of(new Point(4L, "d", 4.0))).committedRecords()).isEqualTo(1);
    }

    @Test void allRecordsAreValidatedBeforeTheFirstDatabaseWrite() {
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class, () ->
                template.batchWriteDetailed(List.of(new Point(1L, "a", 1.0), new Point(2L, "b", null))));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure.getResult().requestedRecords()).isEqualTo(2);
        assertThat(failure.getResult().validatedRecords()).isEqualTo(1);
        assertThat(failure.getResult().committedRecords()).isZero();
        assertThat(failure.getResult().commitState()).isEqualTo(BatchCommitStateEnum.NOT_COMMITTED);
        assertThat(failure.getResult().retryable()).isFalse();
        verify(adapter, never()).batchWriteDetailed(any(), any());
    }

    @Test void nullRecordReportsZeroCommittedWithoutAnAccidentalPartialBatch() {
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class, () ->
                template.batchWrite(Arrays.asList(new Point(1L, "a", 1.0), null)));
        assertThat(failure.getResult().requestedRecords()).isEqualTo(2);
        assertThat(failure.getResult().committedRecords()).isZero();
        assertThat(failure.getResult().commitState()).isEqualTo(BatchCommitStateEnum.NOT_COMMITTED);
        verify(adapter, never()).batchWriteDetailed(any(), any());
    }

    @Test void oversizeBatchIsRejectedBeforeMetadataConversion() {
        TSDBMetadataResolver resolver = mock(TSDBMetadataResolver.class);
        when(adapter.getMaxBatchRecords()).thenReturn(1);
        TGTemplate bounded = new TGTemplate(adapter, resolver);
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class, () ->
                bounded.batchWriteDetailed(List.of(new Object(), new Object())));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure.getResult().validatedRecords()).isZero();
        verifyNoInteractions(resolver);
        verify(adapter, never()).batchWriteDetailed(any(), any());
    }

    @ParameterizedTest @ValueSource(ints = {0, -1})
    void invalidBackendBatchLimitIsAConfigurationFailure(int limit) {
        when(adapter.getMaxBatchRecords()).thenReturn(limit);
        TSDBBatchWriteException failure = assertThrows(TSDBBatchWriteException.class,
                () -> template.write(new Point(1L, "a", 1.0)));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.CONFIGURATION_ERROR);
        assertThat(failure.getResult().committedRecords()).isZero();
    }

    @Test void backendPartialCommitAndOriginalCauseRemainVisibleToCaller() {
        BatchWriteResult partial = new BatchWriteResult(2, 2, 1, 2, 1, 1, "metrics",
                BatchCommitStateEnum.UNKNOWN, true, "connection closed");
        TSDBBatchWriteException failure = new TSDBBatchWriteException(partial, new IllegalStateException("offline"));
        when(adapter.batchWriteDetailed(any(), any())).thenThrow(failure);
        assertThatThrownBy(() -> template.batchWrite(List.of(new Point(1L, "a", 1.0), new Point(2L, "a", 1.0))))
                .isSameAs(failure);
        assertThat(failure.getResult()).isSameAs(partial);
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN);
    }

    @Test void rawSqlIsPassedUnchangedAndReturnedRowsAreDefensiveCopies() {
        Map<String, Object> original = new LinkedHashMap<>(Map.of("value", 3.5));
        when(adapter.executeQuery("select * from metrics")).thenReturn(rows(original));
        List<Map<String, Object>> returned = template.executeQuery("select * from metrics");
        returned.get(0).put("value", 99);
        returned.clear();
        assertThat(original).containsEntry("value", 3.5);
    }

    @Test void rawQueryMapsPlainDtoUsingPhysicalColumnNames() {
        when(adapter.executeQuery("sql")).thenReturn(rows(row(1L, "meter", 3.5)));
        Point actual = template.executeQuery("sql", Point.class).get(0);
        assertThat(actual.time).isEqualTo(1L);
        assertThat(actual.device).isEqualTo("meter");
        assertThat(actual.value).isEqualTo(3.5);
    }

    @Test void failedOrNullQueryResponsesBecomeCategorizedErrors() {
        when(adapter.executeQuery("failed")).thenReturn(QueryResult.failure("permission denied"));
        TSDBException failure = assertThrows(TSDBException.class, () -> template.executeQuery("failed"));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
        assertThat(failure).hasMessage("permission denied");
        assertThat(assertThrows(TSDBException.class, () -> template.executeQuery("null", Point.class)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
    }

    @Test void missingBackendFailsClearly() {
        assertThatThrownBy(() -> new TGTemplate(null).executeQuery("select 1"))
                .isInstanceOf(TSDBException.class).hasMessageContaining("No TSDB adapter available");
    }

    @Test void nullAndNullElementRawPayloadsProduceSafeEmptyValues() {
        QueryResult result = rows();
        result.setRows(null);
        when(adapter.executeQuery("empty")).thenReturn(result);
        assertThat(template.executeQuery("empty")).isEmpty();
        result.setRows(Arrays.asList((Map<String, Object>) null));
        assertThat(template.executeQuery("empty")).containsExactly(Map.of());
    }

    @Test void normalQueryNormalizesLimitAndMetadataWithoutMutatingBuilderSnapshot() {
        when(adapter.query(eq("archive"), any())).thenReturn(rows(row(1L, "a", 2.0)));
        TGQueryBuilder<Point> builder = template.query(Point.class).database("archive").offset(0);
        TSDBQuery original = builder.build();
        assertThat(builder.list()).hasSize(1);
        TSDBQuery actual = capturedQuery();
        assertThat(actual.getMeasurement()).isEqualTo("metrics");
        assertThat(actual.getTimeColumn()).isEqualTo("time");
        assertThat(actual.getLimit()).isEqualTo(1000);
        assertThat(actual.getOffset()).isZero();
        assertThat(actual.getOrder()).isEqualTo(SortOrderEnum.DESC);
        assertThat(actual.isPaginationProbe()).isFalse();
        assertThat(original.isPaginationProbe()).isFalse();
        assertThat(original.getLimit()).isNull();
        assertThat(builder.build().getLimit()).isNull();
    }

    @ParameterizedTest @ValueSource(ints = {0, -1, 10001, Integer.MAX_VALUE})
    void invalidQueryLimitsAreRejectedBeforeDatabaseIo(int limit) {
        TSDBException failure = assertThrows(TSDBException.class, () -> template.query(Point.class).limit(limit).list());
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        verify(adapter, never()).query(any(), any());
    }

    @Test void invalidOffsetAndMissingRecordTypeAreRejected() {
        assertThatThrownBy(() -> template.query(Point.class).offset(-1).list()).isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> template.queryRaw(null, new TSDBQuery())).isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> template.queryRaw(null, null)).isInstanceOf(TSDBException.class);
        verify(adapter, never()).query(any(), any());
    }

    @Test void cursorPageFetchesOneExtraRowAndReturnsOnlyTheRequestedPage() {
        when(adapter.query(isNull(), any())).thenReturn(rows(row(1L, "a", 1.0), row(2L, "b", 2.0), row(3L, "c", 3.0)));
        PageResult<Point> page = template.query(Point.class).limit(2).orderByTimeAsc().page();
        assertThat(page.getRows()).hasSize(2);
        assertThat(page.isHasNext()).isTrue();
        assertThat(page.getNextCursorTime()).isEqualTo(2L);
        assertThat(page.getNextCursor()).containsEntry("time", 2L);
        assertThat(page.getOrder()).isEqualTo(SortOrderEnum.ASC);
        TSDBQuery actual = capturedQuery();
        assertThat(actual.getLimit()).isEqualTo(3);
        assertThat(actual.isPaginationProbe()).isTrue();
    }

    @Test void lastCursorPageHasNoNextCursor() {
        when(adapter.query(isNull(), any())).thenReturn(rows(row(1L, "a", 1.0)));
        PageResult<Map> page = template.query(Point.class).limit(2).page(Map.class);
        assertThat(page.getRows()).hasSize(1);
        assertThat(page.isHasNext()).isFalse();
        assertThat(page.getNextCursorTime()).isNull();
        assertThat(page.getNextCursor()).isEmpty();
    }

    @ParameterizedTest @ValueSource(strings = {"time", "_time", "timestamp", "Time"})
    void cursorRecognizesBackendTimestampColumnAliases(String column) {
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of(column, Instant.ofEpochMilli(1234L)), row(2L, "a", 2.0)));
        assertThat(template.query(Point.class).limit(1).page(Map.class).getNextCursorTime()).isEqualTo(1234L);
    }

    @Test void cursorPrefersAnExactPhysicalTimeColumnOverAliasesAndCaseVariants() {
        when(adapter.query(isNull(), any())).thenReturn(rows(
                Map.of("event_time", 12L, "EVENT_TIME", 90L, "time", 88L), Map.of("event_time", 13L)));
        assertThat(template.query(CustomTimePoint.class).limit(1).page(Map.class).getNextCursorTime()).isEqualTo(12L);
    }

    @Test void cursorFindsMixedCasePhysicalColumnBeforeDefaultTimeAlias() {
        when(adapter.query(isNull(), any())).thenReturn(rows(
                Map.of("EVENT_TIME", 12L, "time", 88L), Map.of("EVENT_TIME", 13L)));
        assertThat(template.query(CustomTimePoint.class).limit(1).page(Map.class).getNextCursorTime()).isEqualTo(12L);
    }

    @Test void cursorFallsBackToUsableAliasesWhenEarlierCandidatesCannotBeParsed() {
        Map<String, Object> first = new LinkedHashMap<>();
        first.put(null, "ignored"); first.put("time", "invalid timestamp");
        first.put("timestamp", Instant.ofEpochMilli(1234L));
        when(adapter.query(isNull(), any())).thenReturn(rows(first, row(2L, "a", 2.0)));
        assertThat(template.query(Point.class).limit(1).page(Map.class).getNextCursorTime()).isEqualTo(1234L);
    }

    @Test void strictCursorUsesBackendCanonicalizationForTimeAndTagColumns() {
        when(adapter.normalizeColumnIdentifier(any())).thenAnswer(call -> {
            String column = call.getArgument(0);
            return column == null ? null : column.trim().toLowerCase(Locale.ROOT);
        });
        when(adapter.query(isNull(), any())).thenReturn(rows(
                Map.of("Time", 1L, "DEVICE_CODE", "a"), Map.of("Time", 1L, "DEVICE_CODE", "b")));
        assertThat(template.query(Point.class).limit(1).strictCursorPage(Map.class).getNextCursor())
                .containsExactlyInAnyOrderEntriesOf(Map.of("time", 1L, "device_code", "a"));
    }

    @Test void cursorSupportsDateAndIsoTimeValues() {
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of("time", new Date(1234L)), row(2L, "a", 2.0)));
        assertThat(template.query(Point.class).limit(1).page(Map.class).getNextCursorTime()).isEqualTo(1234L);
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of("time", Instant.ofEpochMilli(1234L).toString()), row(2L, "a", 2.0)));
        assertThat(template.query(Point.class).limit(1).page(Map.class).getNextCursorTime()).isEqualTo(1234L);
    }

    @ParameterizedTest @MethodSource("exactCursorTimes")
    void cursorTimesPreserveExactNumericBoundariesAndTemporalResolution(Object time, long expected) {
        when(adapter.query(isNull(), any())).thenReturn(rows(
                Map.of("time", time, "device_code", "a"), row(2L, "b", 2.0)));
        assertThat(template.query(Point.class).limit(1).page(Map.class).getNextCursorTime()).isEqualTo(expected);
        assertThat(template.query(Point.class).limit(1).strictCursorPage(Map.class).getNextCursor())
                .containsEntry("time", expected).containsEntry("device_code", "a");
    }

    static Stream<Arguments> exactCursorTimes() {
        return Stream.of(
                Arguments.of((byte) 12, 12L),
                Arguments.of((short) -12, -12L),
                Arguments.of(1234, 1234L),
                Arguments.of(Long.MIN_VALUE, Long.MIN_VALUE),
                Arguments.of(Long.MAX_VALUE, Long.MAX_VALUE),
                Arguments.of(BigInteger.valueOf(Long.MIN_VALUE), Long.MIN_VALUE),
                Arguments.of(BigInteger.valueOf(Long.MAX_VALUE), Long.MAX_VALUE),
                Arguments.of(new BigDecimal("9223372036854775807.000"), Long.MAX_VALUE),
                Arguments.of(new BigDecimal("-9223372036854775808.000"), Long.MIN_VALUE),
                Arguments.of(1234.0D, 1234L),
                Arguments.of(-1234.0F, -1234L),
                Arguments.of(-0.0D, 0L),
                Arguments.of((double) Long.MIN_VALUE, Long.MIN_VALUE),
                Arguments.of((float) Long.MIN_VALUE, Long.MIN_VALUE),
                Arguments.of(Math.nextDown(Math.scalb(1.0D, 63)), 9223372036854774784L),
                Arguments.of(new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE), Long.MAX_VALUE),
                Arguments.of(new Date(Long.MIN_VALUE), Long.MIN_VALUE),
                Arguments.of(new Date(Long.MAX_VALUE), Long.MAX_VALUE),
                Arguments.of(Instant.ofEpochMilli(Long.MIN_VALUE), Long.MIN_VALUE),
                Arguments.of(Instant.ofEpochMilli(Long.MAX_VALUE).toString(), Long.MAX_VALUE),
                Arguments.of(Instant.parse("1970-01-01T00:00:00.123999999Z"), 123L),
                Arguments.of("1969-12-31T23:59:59.999999999Z", -1L));
    }

    @ParameterizedTest @MethodSource("invalidCursorTimes")
    void timePageRejectsAnUnusableNextCursorInsteadOfReturningTheFirstPageAgain(Object time) {
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of("time", time), row(2L, "b", 2.0)));
        TSDBException failure = assertThrows(TSDBException.class,
                () -> template.query(Point.class).limit(1).page(Map.class));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
        assertThat(failure).hasMessageContaining("time cursor");
    }

    @ParameterizedTest @MethodSource("invalidCursorTimes")
    void strictCursorRejectsInvalidTimeEvenOnTheLastPage(Object time) {
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of("time", time, "device_code", "a")));
        TSDBException failure = assertThrows(TSDBException.class,
                () -> template.query(Point.class).limit(1).strictCursorPage(Map.class));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
        assertThat(failure).hasMessageContaining("time");
    }

    @ParameterizedTest @MethodSource("invalidCursorTimes")
    void invalidNumericOrOverflowingPhysicalTimeStillFallsBackToUsableAliases(Object time) {
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("event_time", time);
        first.put("EVENT_TIME", time);
        first.put("time", time);
        first.put("Time", time);
        first.put("_time", time);
        first.put("timestamp", Instant.ofEpochMilli(1234L));
        when(adapter.query(isNull(), any())).thenReturn(rows(first, row(2L, "b", 2.0)));
        assertThat(template.query(CustomTimePoint.class).limit(1).page(Map.class).getNextCursorTime())
                .isEqualTo(1234L);
    }

    static Stream<Object> invalidCursorTimes() {
        return Stream.of(
                1.9D, -1.9F, new BigDecimal("0.1"),
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
                BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
                BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
                new BigDecimal("9223372036854775808"), new BigDecimal("-9223372036854775809"),
                (double) Long.MAX_VALUE, Math.nextDown((double) Long.MIN_VALUE),
                new BigDecimal("1E+2147483647"), new BigDecimal("1E-2147483647"),
                Instant.MAX, Instant.MIN, Instant.MAX.toString(), Instant.MIN.toString(),
                "invalid time", " ", new Object());
    }

    @Test void timePageRejectsMissingAndNullTimeWhenAnotherPageExists() {
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of("value", 1), row(2L, "b", 2.0)));
        assertThat(assertThrows(TSDBException.class,
                () -> template.query(Point.class).limit(1).page(Map.class)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
        when(adapter.query(isNull(), any())).thenReturn(rows(cursorEntries("time", null), row(2L, "b", 2.0)));
        assertThat(assertThrows(TSDBException.class,
                () -> template.query(Point.class).limit(1).page(Map.class)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
    }

    @Test void finalTimePageDoesNotRequireACursorAndEmptyPagesStaySuccessful() {
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of("time", 1.9D)));
        PageResult<Map> last = template.query(Point.class).limit(1).page(Map.class);
        assertThat(last.getRows()).containsExactly(Map.of("time", 1.9D));
        assertThat(last.isHasNext()).isFalse();
        assertThat(last.getNextCursorTime()).isNull();
        assertThat(last.getNextCursor()).isEmpty();
        when(adapter.query(isNull(), any())).thenReturn(rows());
        PageResult<Map> empty = template.query(Point.class).limit(1).page(Map.class);
        assertThat(empty.getRows()).isEmpty();
        assertThat(empty.isHasNext()).isFalse();
        assertThat(empty.getNextCursorTime()).isNull();
        assertThat(template.query(Point.class).limit(1).strictCursorPage(Map.class).getRows()).isEmpty();
    }

    @Test void strictCursorValidatesEveryReturnedTimeInsteadOfOnlyTheBoundaryRow() {
        when(adapter.query(isNull(), any())).thenReturn(rows(
                Map.of("time", Double.NaN, "device_code", "a"), row(2L, "b", 2.0), row(3L, "c", 3.0)));
        assertThat(assertThrows(TSDBException.class,
                () -> template.query(Point.class).limit(2).strictCursorPage(Map.class)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
    }

    @Test void unsupportedCursorAggregationOrCustomSortFailsBeforeIo() {
        assertThatThrownBy(() -> template.query(Point.class).aggregate("value", AggregationFunctionEnum.AVG, null).page())
                .isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> template.query(Point.class).aggregate("value", AggregationFunctionEnum.AVG, null).strictCursorPage())
                .isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> template.query(Point.class).orderByFieldAsc("value").page())
                .isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> template.query(Point.class).aggregate("value", null, null).orderByFieldAsc("value").list())
                .isInstanceOf(TSDBException.class);
        verify(adapter, never()).query(any(), any());
    }

    @Test void strictCursorAddsTimeAndTagsToSelectionAndPreservesDuplicateTimestampRows() {
        when(adapter.query(isNull(), any())).thenReturn(rows(row(1L, "a", 1.0), row(1L, "b", 2.0), row(1L, "c", 3.0)));
        TGQueryBuilder<Point> builder = template.query(Point.class).select("value").limit(2).orderByTimeAsc();
        PageResult<Map> page = builder.strictCursorPage(Map.class);
        assertThat(page.getRows()).hasSize(2);
        assertThat(page.getNextCursor()).containsExactlyInAnyOrderEntriesOf(Map.of("time", 1L, "device_code", "b"));
        TSDBQuery actual = capturedQuery();
        assertThat(actual.isStrictCursor()).isTrue();
        assertThat(actual.isPaginationProbe()).isTrue();
        assertThat(actual.getCursorColumns()).containsExactly("time", "device_code");
        assertThat(actual.getSelectColumns()).containsExactly("value", "time", "device_code");
        assertThat(actual.getLimit()).isEqualTo(3);
        assertThat(builder.build().getSelectColumns()).containsExactly("value");
        assertThat(builder.build().isPaginationProbe()).isFalse();
    }

    @Test void strictCursorKeepsMixedSortAndAddsStableTieBreakers() {
        when(adapter.query(isNull(), any())).thenReturn(rows(row(1L, "a", 2.0), row(1L, "b", 2.0)));
        PageResult<Map> page = template.query(Point.class).limit(1).orderByFieldDesc("value")
                .thenByFieldAsc("device_code").strictCursorPage(Map.class);
        assertThat(page.getNextCursor()).containsExactlyInAnyOrderEntriesOf(Map.of("value", 2.0, "device_code", "a", "time", 1L));
        assertThat(capturedQuery().getSortSpecs()).containsExactly(new SortSpec("value", SortOrderEnum.DESC),
                new SortSpec("device_code", SortOrderEnum.ASC), new SortSpec("time", SortOrderEnum.DESC));
    }

    @Test void strictCursorRejectsColumnsThatDoNotMatchCustomSortOrder() {
        assertThatThrownBy(() -> template.query(Point.class).orderByFieldAsc("value").cursorColumns("time").strictCursorPage())
                .isInstanceOf(TSDBException.class).hasMessageContaining("must match");
        assertThatThrownBy(() -> template.query(Point.class).orderByFieldAsc("value").cursorColumns("value", "time").strictCursorPage())
                .isInstanceOf(TSDBException.class).hasMessageContaining("must match");
        verify(adapter, never()).query(any(), any());
    }

    @Test void strictCursorRejectsMissingTieBreakerInsteadOfReturningAnUnusableCursor() {
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of("time", 1L), Map.of("time", 2L)));
        TSDBException failure = assertThrows(TSDBException.class,
                () -> template.query(Point.class).limit(1).strictCursorPage(Map.class));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
        assertThat(failure).hasMessageContaining("device_code");
    }

    @Test void offsetPageCountsBeforeReadingAndReturnsAccuratePageMetadata() {
        when(adapter.count(eq("db"), any())).thenReturn(5L);
        when(adapter.query(eq("db"), any())).thenReturn(rows(row(3L, "a", 3.0), row(4L, "b", 4.0)));
        PageResult<Point> page = template.query(Point.class).database("db").cursorTime(99L).cursor(Map.of("time", 99L)).page(2, 2);
        assertThat(page.getRows()).hasSize(2);
        assertThat(page.getTotal()).isEqualTo(5L);
        assertThat(page.getTotalPages()).isEqualTo(3L);
        assertThat(page.getPageNum()).isEqualTo(2);
        assertThat(page.getPageSize()).isEqualTo(2);
        assertThat(page.getOffset()).isEqualTo(2);
        assertThat(page.isHasNext()).isTrue();
        TSDBQuery actual = capturedQuery();
        assertThat(actual.getLimit()).isEqualTo(2);
        assertThat(actual.getCursorTime()).isNull();
        assertThat(actual.getCursorValues()).isEmpty();
        assertThat(actual.isPaginationProbe()).isFalse();
        ArgumentCaptor<TSDBQuery> counted = ArgumentCaptor.forClass(TSDBQuery.class);
        verify(adapter).count(eq("db"), counted.capture());
        assertThat(counted.getValue().isPaginationProbe()).isFalse();
        var ordered = inOrder(adapter);
        ordered.verify(adapter).count(eq("db"), any());
        ordered.verify(adapter).query(eq("db"), any());
    }

    @ParameterizedTest @ValueSource(longs = {0, 1, 2})
    void offsetPageBeyondTotalDoesNotPerformUnnecessaryQuery(long total) {
        when(adapter.count(isNull(), any())).thenReturn(total);
        PageResult<Map> page = template.query(Point.class).page(2, 2, Map.class);
        assertThat(page.getRows()).isEmpty();
        assertThat(page.isHasNext()).isFalse();
        assertThat(page.getTotal()).isEqualTo(total);
        verify(adapter, never()).query(any(), any());
    }

    @Test void offsetPageRejectsNegativeBackendTotal() {
        when(adapter.count(isNull(), any())).thenReturn(-1L);
        TSDBException failure = assertThrows(TSDBException.class, () -> template.query(Point.class).page(1, 2));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
        verify(adapter, never()).query(any(), any());
    }

    @Test void offsetPageRejectsBadPageNumbersAndOverflow() {
        assertThatThrownBy(() -> template.query(Point.class).page(0, 1)).isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> template.query(Point.class).page(1, 0)).isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> template.query(Point.class).page(Integer.MAX_VALUE, 2)).isInstanceOf(TSDBException.class);
        verify(adapter, never()).count(any(), any());
    }

    @Test void cursorProbeDoesNotLeakWhenAQueryBuilderIsReusedForAList() {
        when(adapter.query(isNull(), any())).thenReturn(rows());
        TGQueryBuilder<Point> builder = template.query(Point.class).limit(1);
        builder.page();
        builder.strictCursorPage();
        builder.list();
        ArgumentCaptor<TSDBQuery> captured = ArgumentCaptor.forClass(TSDBQuery.class);
        verify(adapter, times(3)).query(isNull(), captured.capture());
        assertThat(captured.getAllValues()).extracting(TSDBQuery::isPaginationProbe)
                .containsExactly(true, true, false);
        assertThat(captured.getAllValues()).extracting(TSDBQuery::getLimit).containsExactly(2, 2, 1);
        assertThat(builder.build().isPaginationProbe()).isFalse();
    }

    @Test void ordinaryQueryClearsCallerSuppliedProbeWithoutMutatingTheInput() {
        when(adapter.query(isNull(), any())).thenReturn(rows());
        TSDBQuery query = template.query(Point.class).limit(1).build();
        query.setPaginationProbe(true);
        template.queryRaw(null, query);
        assertThat(capturedQuery().isPaginationProbe()).isFalse();
        assertThat(query.isPaginationProbe()).isTrue();
    }

    @Test void strictCursorKeepsCaseDistinctProjectionAndSortColumns() {
        when(adapter.query(isNull(), any())).thenReturn(rows(
                Map.of("time", 1L, "device_code", "a", "value", 7.0, "VALUE", 99.0),
                Map.of("time", 2L, "device_code", "b", "value", 8.0, "VALUE", 98.0)));
        PageResult<Map> page = template.query(Point.class).select("VALUE").limit(1)
                .orderByFieldDesc("value").thenByFieldAsc("VALUE").strictCursorPage(Map.class);
        assertThat(capturedQuery().getSelectColumns()).containsExactly("VALUE", "value", "time", "device_code");
        assertThat(capturedQuery().getSortSpecs()).containsExactly(
                new SortSpec("value", SortOrderEnum.DESC), new SortSpec("VALUE", SortOrderEnum.ASC),
                new SortSpec("time", SortOrderEnum.DESC), new SortSpec("device_code", SortOrderEnum.DESC));
        assertThat(page.getNextCursor()).containsEntry("value", 7.0).containsEntry("VALUE", 99.0);
    }

    @ParameterizedTest @ValueSource(ints = {1, 2})
    void strictCursorDoesNotGuessMissingPhysicalColumnsEvenOnTheLastPage(int rowCount) {
        List<Map<String, Object>> returned = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) returned.add(Map.of("time", 1L, "device_code", "a", "VALUE", 99.0));
        when(adapter.query(isNull(), any())).thenReturn(rows(returned.toArray(Map[]::new)));
        TSDBException failure = assertThrows(TSDBException.class, () -> template.query(Point.class)
                .select("VALUE").orderByFieldDesc("value").limit(1).strictCursorPage(Map.class));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
        assertThat(failure).hasMessageContaining("value");
    }

    @Test void strictCursorRejectsInputCaseMismatchBeforeQuery() {
        assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class)
                .orderByFieldDesc("value").cursorColumns("VALUE").strictCursorPage()).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class)
                .orderByFieldDesc("value").cursor(Map.of("VALUE", 99, "time", 1L, "device_code", "a"))
                .strictCursorPage()).getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        verify(adapter, never()).query(any(), any());
    }

    @ParameterizedTest @MethodSource("malformedStrictCursors")
    void malformedStrictCursorEntriesFailBeforeDatabaseIo(String scenario, Map<String, Object> cursor) {
        TSDBException failure = assertThrows(TSDBException.class,
                () -> template.query(Point.class).cursor(cursor).strictCursorPage(Map.class), scenario);
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        verify(adapter, never()).query(any(), any());
        verify(adapter, never()).count(any(), any());
        verify(adapter, never()).executeQuery(any());
    }

    static Stream<Arguments> malformedStrictCursors() {
        return Stream.of(
                Arguments.of("all-null cursor", cursorEntries("time", null, "device_code", null)),
                Arguments.of("null time", cursorEntries("time", null, "device_code", "a")),
                Arguments.of("null tag", cursorEntries("time", 1L, "device_code", null)),
                Arguments.of("extra null value", cursorEntries("time", 1L, "device_code", "a", "unexpected", null)),
                Arguments.of("null key", cursorEntries("time", 1L, "device_code", "a", null, 2L)),
                Arguments.of("blank key", cursorEntries("time", 1L, "device_code", "a", " \t", 2L)),
                Arguments.of("only null key", cursorEntries(null, 1L)),
                Arguments.of("only blank key", cursorEntries(" \t", 1L)),
                Arguments.of("trimmed time collision", cursorEntries("time", 1L, " time ", 2L, "device_code", "a")),
                Arguments.of("trimmed tag collision", cursorEntries("time", 1L, "device_code", "a", " device_code ", "b")));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void nullAndEmptyStrictCursorsStillRequestTheFirstPage(boolean nullCursor) {
        when(adapter.query(isNull(), any())).thenReturn(rows(row(1L, "a", 1.0)));
        PageResult<Map> page = template.query(Point.class)
                .cursor(nullCursor ? null : Collections.emptyMap()).strictCursorPage(Map.class);
        assertThat(page.getRows()).hasSize(1);
        assertThat(page.isHasNext()).isFalse();
        assertThat(capturedQuery().getCursorValues()).isEmpty();
    }

    @Test void validWhitespaceCursorKeysNormalizeWithoutMutatingTheBuilderSnapshot() {
        when(adapter.query(isNull(), any())).thenReturn(rows(row(2L, "b", 2.0)));
        Map<String, Object> cursor = cursorEntries(" time ", 1L, " device_code ", "a");
        TGQueryBuilder<Point> builder = template.query(Point.class).cursor(cursor);
        cursor.clear();
        builder.strictCursorPage(Map.class);
        assertThat(capturedQuery().getCursorValues()).containsExactlyEntriesOf(cursorEntries("time", 1L, "device_code", "a"));
        assertThat(builder.build().getCursorValues()).containsExactlyEntriesOf(
                cursorEntries(" time ", 1L, " device_code ", "a"));
    }

    @Test void backendFoldedCursorCollisionRetainsNullEntryUntilValidation() {
        when(adapter.normalizeColumnIdentifier(any())).thenAnswer(call -> {
            String column = call.getArgument(0);
            return column == null ? null : column.trim().toLowerCase(Locale.ROOT);
        });
        Map<String, Object> cursor = cursorEntries("time", 1L, "Time", null, "device_code", "a");
        TSDBException failure = assertThrows(TSDBException.class,
                () -> template.query(Point.class).cursor(cursor).strictCursorPage(Map.class));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure).hasMessageContaining("ambiguous");
        verify(adapter, never()).query(any(), any());
    }

    @Test void strictCursorTreatsTimeVariantsAsOrdinaryPhysicalFields() {
        when(adapter.query(isNull(), any())).thenReturn(rows(
                Map.of("time", 1L, "device_code", "a", "TIME", 99.5, "timestamp", "ordinary", "_time", 71),
                Map.of("time", 2L, "device_code", "b", "TIME", 98.5, "timestamp", "other", "_time", 72)));
        PageResult<Map> page = template.query(Point.class).limit(1).orderByFieldDesc("TIME")
                .thenByFieldAsc("timestamp").thenByFieldAsc("_time").strictCursorPage(Map.class);
        assertThat(page.getNextCursor()).containsEntry("TIME", 99.5).containsEntry("timestamp", "ordinary")
                .containsEntry("_time", 71).containsEntry("time", 1L);
    }

    @Test void backendFoldedDuplicateCursorKeysAndConflictingSortsAreRejected() {
        when(adapter.normalizeColumnIdentifier(any())).thenAnswer(call -> {
            String column = call.getArgument(0);
            return column == null ? null : column.trim().toLowerCase(Locale.ROOT);
        });
        assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class)
                .cursor(Map.of("time", 1L, "Time", 1L, "device_code", "a")).strictCursorPage()).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class)
                .orderByFieldAsc("value").thenByFieldDesc("VALUE").strictCursorPage()).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        verify(adapter, never()).query(any(), any());
    }

    @Test void backendAmbiguousResultKeysAreRejectedRatherThanChoosingAValue() {
        when(adapter.normalizeColumnIdentifier(any())).thenAnswer(call -> {
            String column = call.getArgument(0);
            return column == null ? null : column.trim().toLowerCase(Locale.ROOT);
        });
        when(adapter.query(isNull(), any())).thenReturn(rows(Map.of("time", 1L, "Time", 2L, "device_code", "a")));
        assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class)
                .strictCursorPage(Map.class)).getErrorCode()).isEqualTo(TSDBErrorCodeEnum.QUERY_ERROR);
    }

    @Test void supportedMapResultsHaveTheRequestedConcreteTypeAndAreIndependentCopies() {
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("z", 2); original.put("a", 1);
        when(adapter.executeQuery("sql")).thenReturn(rows(original));
        assertThat(template.executeQuery("sql", Map.class).get(0)).isExactlyInstanceOf(LinkedHashMap.class);
        assertThat(template.executeQuery("sql", LinkedHashMap.class).get(0)).isExactlyInstanceOf(LinkedHashMap.class);
        assertThat(template.executeQuery("sql", HashMap.class).get(0)).isExactlyInstanceOf(HashMap.class);
        TreeMap result = template.executeQuery("sql", TreeMap.class).get(0);
        assertThat(result.keySet()).containsExactly("a", "z");
        result.put("z", 99);
        assertThat(original).containsEntry("z", 2);
    }

    @Test void unsupportedMapResultsAndNullResultTypesFailBeforeAnyDatabaseIo() {
        for (Class<?> type : List.of(SortedMap.class, java.util.concurrent.ConcurrentHashMap.class, CustomMap.class)) {
            assertThat(assertThrows(TSDBException.class, () -> template.executeQuery("sql", type)).getErrorCode())
                    .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
            assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class).list(type)).getErrorCode())
                    .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
            assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class).page(type)).getErrorCode())
                    .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
            assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class).strictCursorPage(type)).getErrorCode())
                    .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
            assertThat(assertThrows(TSDBException.class, () -> template.query(Point.class).page(1, 5, type)).getErrorCode())
                    .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        }
        assertThat(assertThrows(TSDBException.class, () -> template.executeQuery("sql", null)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        verify(adapter, never()).executeQuery(any());
        verify(adapter, never()).query(any(), any());
        verify(adapter, never()).count(any(), any());
    }

    static class CustomMap extends LinkedHashMap<String, Object> {}

    private static Map<String, Object> cursorEntries(Object... entries) {
        Map<String, Object> cursor = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            cursor.put((String) entries[i], entries[i + 1]);
        }
        return cursor;
    }

    private TSDBQuery capturedQuery() {
        ArgumentCaptor<TSDBQuery> query = ArgumentCaptor.forClass(TSDBQuery.class);
        verify(adapter).query(any(), query.capture());
        return query.getValue();
    }

    @SafeVarargs static QueryResult rows(Map<String, Object>... rows) {
        QueryResult result = new QueryResult();
        result.setSuccess(true);
        result.setRows(new ArrayList<>(Arrays.asList(rows)));
        result.setRowCount(rows.length);
        return result;
    }

    static Map<String, Object> row(long time, String device, double value) {
        return new LinkedHashMap<>(Map.of("time", time, "device_code", device, "value", value));
    }

    @TGMeasurement("metrics")
    static class Point {
        @TGTime Long time;
        @TGTag("device_code") String device;
        @TGField Double value;
        Point() {}
        Point(Long time, String device, Double value) { this.time = time; this.device = device; this.value = value; }
    }

    @TGMeasurement("custom_metrics")
    static class CustomTimePoint {
        @TGTime("event_time") Long time;
        @TGField Double value;
    }
}
