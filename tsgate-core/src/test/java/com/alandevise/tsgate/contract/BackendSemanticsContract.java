package com.alandevise.tsgate.contract;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.core.TGQueryBuilder;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.SortOrderEnum;
import com.alandevise.tsgate.model.SortSpec;
import com.alandevise.tsgate.model.PageResult;
import com.alandevise.tsgate.model.OperatorEnum;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real-server acceptance for the currently registered table and Influx-compatible deployments.
 * The fixtures establish query visibility according to their backend, version, protocol and topology.
 * Same-point update cases describe these deployments, not universal retry idempotency or atomicity.
 * Backends with different point identity or update semantics must override the corresponding test
 * and assert their explicitly declared behavior, including append-only behavior where applicable.
 */
public interface BackendSemanticsContract {
    String semanticsBackendId();
    String semanticsServerVersion();
    String semanticsProtocol();
    TSDBAdapter semanticsAdapter();
    TGQueryBuilder<?> semanticsQuery();
    String semanticsMeasurement();
    long semanticsTime();
    void configureSemanticsRowLimit(int rows);

    /** Establishes the exact expected points without replaying writes or changing adapter query behavior. */
    void awaitSemanticsVisibility(List<TSDBRecord> expected);

    /** Reports only the settings used to construct this test fixture's adapter. */
    default Map<String, String> semanticsConfiguration() { return Map.of(); }

    /** Applies version-specific prerequisites locally; modern fixtures retain their normal settings. */
    default void prepareSemanticsCapability(BackendCapability capability) { }

    @Test
    default void capabilityBatchWriteRoundTripsRows() {
        if (!supportsSemanticsCapability(BackendCapability.BATCH_WRITE)) {
            assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                    assertThrows(TSDBException.class, () -> semanticsAdapter().batchWrite(null, groupedPoints())).getErrorCode());
            return;
        }
        List<TSDBRecord> expected = groupedPoints();
        BatchWriteResult result = semanticsAdapter().batchWriteDetailed(null, expected);
        assertTrue(result.isSuccess());
        assertEquals(expected.size(), result.requestedRecords());
        assertEquals(expected.size(), result.validatedRecords());
        assertEquals(expected.size(), result.committedRecords());
        assertEquals(result.totalBatches(), result.committedBatches());
        awaitSemanticsVisibility(expected);
        List<Map<String, Object>> actual = semanticsAdapter().query(null, detailQuery()).getRows();
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.get(index).timestamp(), actual.get(index).get("time"));
            assertEquals(expected.get(index).tags().get("device"), actual.get(index).get("device"));
            assertEquals(((Number) expected.get(index).fields().get("value")).doubleValue(),
                    ((Number) actual.get(index).get("value")).doubleValue());
        }
    }

    @Test
    default void capabilityFieldSortReturnsExpectedRows() {
        if (!supportsSemanticsCapability(BackendCapability.FIELD_SORT)) {
            assertUnsupportedSemanticsCapability(BackendCapability.FIELD_SORT);
            return;
        }
        List<TSDBRecord> expected = List.of(record(semanticsTime(), "a", Map.of("value", 3d)),
                record(semanticsTime() + 1, "b", Map.of("value", 1d)),
                record(semanticsTime() + 2, "c", Map.of("value", 2d)));
        assertTrue(semanticsAdapter().batchWrite(null, expected));
        awaitSemanticsVisibility(expected);
        for (SortOrderEnum direction : SortOrderEnum.values()) {
            TSDBQuery query = detailQuery();
            query.setSortSpecs(List.of(new SortSpec("value", direction)));
            List<Map<String, Object>> rows = semanticsAdapter().query(null, query).getRows();
            assertEquals(direction == SortOrderEnum.ASC ? List.of(1d, 2d, 3d) : List.of(3d, 2d, 1d),
                    rows.stream().map(row -> ((Number) row.get("value")).doubleValue()).toList());
        }
    }

    @Test
    default void capabilityStrictCompositeCursorReturnsEveryPoint() {
        if (!supportsSemanticsCapability(BackendCapability.STRICT_COMPOSITE_CURSOR)) {
            assertUnsupportedSemanticsCapability(BackendCapability.STRICT_COMPOSITE_CURSOR);
            return;
        }
        List<TSDBRecord> points = new ArrayList<>();
        for (int time = 0; time < 2; time++) {
            for (String device : List.of("a", "b", "c"))
                points.add(record(semanticsTime() + time, device, Map.of("value", time + 1d)));
        }
        assertTrue(semanticsAdapter().batchWrite(null, points));
        awaitSemanticsVisibility(points);
        for (SortOrderEnum timeOrder : SortOrderEnum.values()) {
            for (SortOrderEnum tagOrder : SortOrderEnum.values()) {
                List<String> expected = new ArrayList<>();
                for (int time : timeOrder == SortOrderEnum.ASC ? List.of(0, 1) : List.of(1, 0)) {
                    for (String device : tagOrder == SortOrderEnum.ASC ? List.of("a", "b", "c") : List.of("c", "b", "a"))
                        expected.add((semanticsTime() + time) + "/" + device);
                }
                List<String> collected = new ArrayList<>();
                Map<String, Object> cursor = null;
                for (int page = 0; page < points.size() + 1; page++) {
                    TSDBQuery query = detailQuery();
                    query.setStrictCursor(true);
                    query.setCursorColumns(List.of("time", "device"));
                    query.setSortSpecs(List.of(new SortSpec("time", timeOrder), new SortSpec("device", tagOrder)));
                    query.setCursorValues(cursor);
                    query.setLimit(2);
                    List<Map<String, Object>> rows = semanticsAdapter().query(null, query).getRows();
                    if (rows.isEmpty()) break;
                    assertTrue(page < points.size(), "Composite cursor must terminate");
                    for (Map<String, Object> row : rows) collected.add(row.get("time") + "/" + row.get("device"));
                    Map<String, Object> last = rows.get(rows.size() - 1);
                    cursor = Map.of("time", last.get("time"), "device", last.get("device"));
                }
                assertEquals(expected, collected, "The complete unique key must preserve every tied timestamp");
            }
        }
    }

    @Test
    default void capabilityRegionalCalendarDayReturnsExpectedWindows() {
        if (!supportsSemanticsCapability(BackendCapability.REGIONAL_CALENDAR_DAY)) {
            assertUnsupportedSemanticsCapability(BackendCapability.REGIONAL_CALENDAR_DAY);
            return;
        }
        java.time.ZoneId zone = java.time.ZoneId.of("America/New_York");
        java.time.LocalDate first = java.time.LocalDate.of(2026, 3, 7);
        List<Long> boundaries = java.util.stream.IntStream.rangeClosed(0, 3)
                .mapToObj(day -> first.plusDays(day).atStartOfDay(zone).toInstant().toEpochMilli()).toList();
        assertEquals(23 * 3_600_000L, boundaries.get(2) - boundaries.get(1));
        List<TSDBRecord> points = new ArrayList<>();
        for (int day = 0; day < 3; day++) {
            points.add(record(boundaries.get(day), "calendar", Map.of("value", 1d)));
            points.add(record(boundaries.get(day + 1) - 1, "calendar", Map.of("value", 1d)));
        }
        assertTrue(semanticsAdapter().batchWrite(null, points));
        awaitSemanticsVisibility(points);
        TSDBQuery query = detailQuery();
        query.setStartTime(boundaries.get(0));
        query.setEndTime(boundaries.get(3) - 1);
        query.setTimeZone(zone.getId());
        query.setGroupByTime("1d");
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        List<Map<String, Object>> rows = semanticsAdapter().query(null, query).getRows();
        assertEquals(3, rows.size());
        for (int day = 0; day < rows.size(); day++) {
            assertEquals(boundaries.get(day), assertInstanceOf(Long.class, rows.get(day).get("window_start")));
            assertEquals(2d, ((Number) rows.get(day).get("total")).doubleValue());
        }
        assertEquals(3L, semanticsAdapter().count(null, query));
    }

    @Test
    default void capabilityTimeNotEqualReturnsExpectedRows() {
        assertTimeFilterCapability(BackendCapability.TIME_FILTER_NE, OperatorEnum.NE);
    }

    @Test
    default void capabilityTimeInReturnsExpectedRows() {
        assertTimeFilterCapability(BackendCapability.TIME_FILTER_IN, OperatorEnum.IN);
    }

    private void assertTimeFilterCapability(BackendCapability capability, OperatorEnum operator) {
        boolean supported = supportsSemanticsCapability(capability);
        long base = semanticsTime();
        List<TSDBRecord> points = new ArrayList<>();
        for (int index = 0; index < 4; index++) points.add(record(base + index, "time-capability", Map.of("value", index + 1d)));
        assertTrue(semanticsAdapter().batchWrite(null, points));
        awaitSemanticsVisibility(points);
        List<Long> operands = operator == OperatorEnum.NE ? List.of(base + 1) : List.of(base, base + 2);
        for (boolean instant : List.of(false, true)) {
            List<Object> values = operands.stream().map(value -> instant
                    ? (Object) java.time.Instant.ofEpochMilli(value) : (Object) value).toList();
            TSDBQuery query = detailQuery();
            query.setStartTime(base + 1);
            query.setEndTime(base + 2);
            query.setFilters(List.of(new QueryFilter("time", operator, values)));
            if (supported) {
                List<Map<String, Object>> rows = semanticsAdapter().query(null, query).getRows();
                assertEquals(List.of(base + 2), rows.stream().map(row -> assertInstanceOf(Long.class, row.get("time"))).toList());
                assertEquals(1L, semanticsAdapter().count(null, query));
            } else {
                assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                        assertThrows(TSDBException.class, () -> semanticsAdapter().query(null, query)).getErrorCode());
                assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                        assertThrows(TSDBException.class, () -> semanticsAdapter().count(null, query)).getErrorCode());
            }
            assertEquals(values, query.getFilters().get(0).values());
        }
        assertEquals(4L, semanticsAdapter().count(null, detailQuery()));
    }

    @Test
    default void everyPrimaryTimeFilterUsesEpochMillisecondsAndIntersectsTheTimeRange() {
        long base = semanticsTime();
        List<TSDBRecord> points = new ArrayList<>();
        for (int index = 0; index < 4; index++) points.add(record(base + index, "time-filter", Map.of("value", index + 1d)));
        assertTrue(semanticsAdapter().batchWrite(null, points));
        awaitSemanticsVisibility(points);
        for (OperatorEnum operator : OperatorEnum.values()) {
            List<Long> operands = switch (operator) {
                case IN -> List.of(base, base + 2);
                case BETWEEN -> List.of(base, base + 2);
                case LT -> List.of(base + 2);
                default -> List.of(base + 1);
            };
            List<Long> expected = switch (operator) {
                case EQ, LE -> List.of(base + 1);
                case NE, GT, IN -> List.of(base + 2);
                case GE, BETWEEN -> List.of(base + 1, base + 2);
                case LT -> List.of(base + 1);
            };
            for (boolean instant : List.of(false, true)) {
                List<Object> values = operands.stream().map(value -> instant
                        ? (Object) java.time.Instant.ofEpochMilli(value) : (Object) value).toList();
                TSDBQuery query = detailQuery();
                query.setStartTime(base + 1);
                query.setEndTime(base + 2);
                query.setFilters(List.of(new QueryFilter("time", operator, values)));
                BackendCapability capability = switch (operator) {
                    case NE -> BackendCapability.TIME_FILTER_NE;
                    case IN -> BackendCapability.TIME_FILTER_IN;
                    default -> null;
                };
                if (capability != null && !BackendContractRegistry.profile(semanticsBackendId()).supports(capability)) {
                    assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                            assertThrows(TSDBException.class, () -> semanticsAdapter().query(null, query)).getErrorCode());
                    assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                            assertThrows(TSDBException.class, () -> semanticsAdapter().count(null, query)).getErrorCode());
                    assertEquals(values, query.getFilters().get(0).values());
                    continue;
                }
                List<Map<String, Object>> rows = semanticsAdapter().query(null, query).getRows();
                assertEquals(expected, rows.stream().map(row -> assertInstanceOf(Long.class, row.get("time"))).toList(),
                        operator + " with " + (instant ? "Instant" : "Long"));
                assertEquals(expected.size(), semanticsAdapter().count(null, query), operator.toString());
                assertEquals(values, query.getFilters().get(0).values());
            }
        }
    }

    @Test
    default void numericAndBooleanTagFiltersMatchTheWrittenStringTags() {
        List<TSDBRecord> points = List.of(record(semanticsTime(), "42", Map.of("value", 1d)),
                record(semanticsTime() + 1, "true", Map.of("value", 2d)),
                record(semanticsTime() + 2, "false", Map.of("value", 3d)),
                record(semanticsTime() + 3, "other", Map.of("value", 4d)));
        assertTrue(semanticsAdapter().batchWrite(null, points));
        awaitSemanticsVisibility(points);
        for (Object value : List.of(42L, true, false)) {
            TGQueryBuilder<?> builder = semanticsQuery().whereTag("device", value).orderByTimeAsc();
            List<Map> rows = builder.list(Map.class);
            assertEquals(1, rows.size());
            assertEquals(String.valueOf(value), rows.get(0).get("device"));
            assertEquals(1L, semanticsAdapter().count(null, builder.build()));
        }
        TGQueryBuilder<?> in = semanticsQuery().whereTagIn("device", java.util.Arrays.asList(42, true, null, false)).orderByTimeAsc();
        assertEquals(List.of("42", "true", "false"), in.list(Map.class).stream().map(row -> row.get("device")).toList());
        assertEquals(3L, semanticsAdapter().count(null, in.build()));
    }

    private boolean supportsSemanticsCapability(BackendCapability capability) {
        prepareSemanticsCapability(capability);
        assertNotNull(semanticsProtocol());
        assertFalse(semanticsProtocol().isBlank());
        assertTrue(semanticsServerVersion().matches("[0-9]+\\.[0-9]+\\.[0-9]+"),
                "The fixture must identify the exact server version");
        BackendContractRegistry.Status status = BackendContractRegistry.profile(semanticsBackendId()).capabilities().get(capability);
        assertNotNull(status);
        for (BackendContractRegistry.Condition condition : BackendContractRegistry.conditions(semanticsBackendId(), capability)) {
            if (condition.serverVersions().contains(semanticsServerVersion())) {
                assertEquals(condition.requiredValue(), semanticsConfiguration().get(condition.property()),
                        "The real fixture must apply its version-specific prerequisite for " + semanticsProtocol());
            }
        }
        System.out.println("CAPABILITY backend=" + semanticsBackendId() + " capability=" + capability
                + " status=" + status + " server=" + semanticsServerVersion() + " protocol=" + semanticsProtocol()
                + " settings=" + semanticsConfiguration());
        return status != BackendContractRegistry.Status.UNSUPPORTED;
    }

    private void assertUnsupportedSemanticsCapability(BackendCapability capability) {
        TSDBRecord existing = record(semanticsTime(), "unsupported", Map.of("value", 1d));
        assertTrue(semanticsAdapter().write(null, existing));
        awaitSemanticsVisibility(List.of(existing));
        TSDBQuery query = capability.query();
        query.setMeasurement(semanticsMeasurement());
        assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                assertThrows(TSDBException.class, () -> semanticsAdapter().query(null, query)).getErrorCode());
        assertEquals(1L, semanticsAdapter().count(null, detailQuery()));
    }

    @Test
    default void countBeyondQueryRowLimitCountsDetailsAndGroupsBeforePagination() {
        configureSemanticsRowLimit(2);
        List<TSDBRecord> records = groupedPoints();
        assertTrue(semanticsAdapter().batchWrite(null, records));
        awaitSemanticsVisibility(records);

        TSDBQuery detail = detailQuery();
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> semanticsAdapter().query(null, detail)).getErrorCode());
        detail.setLimit(1);
        detail.setOffset(1);
        assertEquals(6L, semanticsAdapter().count(null, detail));
        assertEquals(1, detail.getLimit());
        assertEquals(1, detail.getOffset());

        TSDBQuery grouped = detailQuery();
        grouped.setGroupByTags(List.of("device"));
        grouped.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "sum_value")));
        assertEquals(TSDBErrorCodeEnum.QUERY_ERROR,
                assertThrows(TSDBException.class, () -> semanticsAdapter().query(null, grouped)).getErrorCode());
        grouped.setLimit(1);
        grouped.setOffset(1);
        assertEquals(3L, semanticsAdapter().count(null, grouped),
                "Aggregate pagination totals count emitted groups, not contributing detail points");
        assertEquals(1, grouped.getLimit());
        assertEquals(1, grouped.getOffset());

        grouped.setGroupByTags(List.of());
        assertEquals(1L, semanticsAdapter().count(null, grouped),
                "An ungrouped aggregate emits one result row for this populated fixture");
    }

    @Test
    default void countedAndUncountedOffsetPagesAgreeAcrossGroupedSeriesAndBothDirections() {
        configureSemanticsRowLimit(20);
        List<TSDBRecord> records = groupedPoints();
        assertTrue(semanticsAdapter().batchWrite(null, records));
        awaitSemanticsVisibility(records);

        for (boolean grouped : List.of(false, true)) {
            for (boolean descending : List.of(false, true)) {
                List<String> devices = grouped ? List.of("a", "b", "c") : List.of("a", "a", "b", "b", "c", "c");
                if (descending) {
                    devices = new ArrayList<>(devices);
                    java.util.Collections.reverse(devices);
                }
                int total = devices.size();
                for (int pageNumber = 1; pageNumber <= total + 1; pageNumber++) {
                    TGQueryBuilder<?> builder = semanticsQuery();
                    if (grouped) builder.groupByTag("device").aggregate("value", AggregationFunctionEnum.SUM, "sum_value");
                    if (descending) builder.orderByTimeDesc(); else builder.orderByTimeAsc();
                    PageResult<Map> counted = builder.page(pageNumber, 1, Map.class);
                    PageResult<Map> uncounted = builder.totalPageCount(false).page(pageNumber, 1, Map.class);
                    assertEquals((long) total, counted.getTotal());
                    assertEquals((long) total, counted.getTotalPages());
                    assertNull(uncounted.getTotal());
                    assertNull(uncounted.getTotalPages());
                    assertEquals(counted.getRows(), uncounted.getRows());
                    assertEquals(pageNumber < total, counted.isHasNext());
                    assertEquals(counted.isHasNext(), uncounted.isHasNext());
                    assertEquals(pageNumber, uncounted.getPageNum());
                    assertEquals(pageNumber - 1, uncounted.getOffset());
                    if (pageNumber <= total) {
                        assertEquals(1, uncounted.getRows().size(), "The lookahead row must be cropped before mapping");
                        Map row = uncounted.getRows().get(0);
                        String device = devices.get(pageNumber - 1);
                        assertEquals(device, row.get("device"));
                        if (grouped) assertEquals(11d * (device.charAt(0) - 'a' + 1),
                                ((Number) row.get("sum_value")).doubleValue());
                    } else {
                        assertTrue(uncounted.getRows().isEmpty());
                    }
                }
            }
        }
    }

    @Test
    default void samePointUpdatesRetainOmittedFieldsWhileTagsAndMillisecondTimeDistinguishPoints() {
        long time = semanticsTime();
        TSDBRecord original = record(time, "a", Map.of("value", 1d, "label", "retained"));
        assertTrue(semanticsAdapter().write(null, original));
        awaitSemanticsVisibility(List.of(original));
        assertTrue(semanticsAdapter().write(null, original));
        awaitSemanticsVisibility(List.of(original));
        assertEquals(1L, semanticsAdapter().count(null, detailQuery()));

        assertTrue(semanticsAdapter().write(null, record(time, "a", Map.of("value", 2d))));
        TSDBRecord updated = record(time, "a", Map.of("value", 2d, "label", "retained"));
        awaitSemanticsVisibility(List.of(updated));
        assertSamePoint(updated);

        Map<String, Object> nullField = new LinkedHashMap<>();
        nullField.put("value", 3d);
        nullField.put("label", null);
        assertTrue(semanticsAdapter().write(null, record(time, "a", nullField)));
        updated = record(time, "a", Map.of("value", 3d, "label", "retained"));
        awaitSemanticsVisibility(List.of(updated));
        assertSamePoint(updated);

        TSDBRecord otherTag = record(time, "b", Map.of("value", 4d, "label", "other"));
        TSDBRecord otherTime = record(time + 1, "a", Map.of("value", 5d, "label", "later"));
        assertTrue(semanticsAdapter().batchWrite(null, List.of(otherTag, otherTime)));
        awaitSemanticsVisibility(List.of(updated, otherTag, otherTime));
        assertEquals(3L, semanticsAdapter().count(null, detailQuery()));
        List<Map<String, Object>> rows = semanticsAdapter().query(null, detailQuery()).getRows();
        assertEquals(3, rows.size());
        assertEquals(2, rows.stream().filter(row -> Long.valueOf(time).equals(row.get("time"))).count());
        assertEquals(1, rows.stream().filter(row -> Long.valueOf(time + 1).equals(row.get("time"))).count());
    }

    @Test
    default void malformedAggregateFailsForQueryAndCountWithoutChangingValidData() {
        TSDBRecord point = record(semanticsTime(), "a", Map.of("value", 1d));
        assertTrue(semanticsAdapter().write(null, point));
        awaitSemanticsVisibility(List.of(point));
        TSDBQuery malformed = detailQuery();
        malformed.setAggregations(java.util.Arrays.asList((AggregationSpec) null));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> semanticsAdapter().query(null, malformed)).getErrorCode());
        malformed.setLimit(0);
        malformed.setOffset(-1);
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, () -> semanticsAdapter().count(null, malformed)).getErrorCode());
        assertEquals(java.util.Arrays.asList((AggregationSpec) null), malformed.getAggregations());
        assertEquals(1L, semanticsAdapter().count(null, detailQuery()));
    }

    private void assertSamePoint(TSDBRecord expected) {
        List<Map<String, Object>> rows = semanticsAdapter().query(null, detailQuery()).getRows();
        assertEquals(1, rows.size());
        assertEquals(expected.timestamp(), assertInstanceOf(Long.class, rows.get(0).get("time")));
        assertEquals(expected.tags().get("device"), rows.get(0).get("device"));
        assertEquals(((Number) expected.fields().get("value")).doubleValue(),
                ((Number) rows.get(0).get("value")).doubleValue());
        assertEquals(expected.fields().get("label"), rows.get(0).get("label"));
    }

    private List<TSDBRecord> groupedPoints() {
        List<TSDBRecord> points = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            String device = String.valueOf((char) ('a' + index));
            points.add(record(semanticsTime() + index * 2L, device, Map.of("value", index + 1d)));
            points.add(record(semanticsTime() + index * 2L + 1, device, Map.of("value", 10d * (index + 1))));
        }
        return points;
    }

    private TSDBRecord record(long time, String device, Map<String, Object> fields) {
        return new TSDBRecord(semanticsMeasurement(), time, Map.of("device", device), fields);
    }

    private TSDBQuery detailQuery() {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement(semanticsMeasurement());
        query.setOrder(SortOrderEnum.ASC);
        return query;
    }
}
