package com.alandevise.tsgate.contract;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.core.TGQueryBuilder;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.PageResult;
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
    TSDBAdapter semanticsAdapter();
    TGQueryBuilder<?> semanticsQuery();
    String semanticsMeasurement();
    long semanticsTime();
    void configureSemanticsRowLimit(int rows);

    /** Establishes the exact expected points without replaying writes or changing adapter query behavior. */
    void awaitSemanticsVisibility(List<TSDBRecord> expected);

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
        return query;
    }
}
