package com.alandevise.tsgate.contract;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.core.TGQueryBuilder;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.metadata.DefaultTSDBMetadataResolver;
import com.alandevise.tsgate.metadata.TSDBColumnMetadata;
import com.alandevise.tsgate.metadata.TSDBEntityMetadata;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.BatchWriteResult;
import com.alandevise.tsgate.model.SortOrderEnum;
import com.alandevise.tsgate.model.SortSpec;
import com.alandevise.tsgate.model.PageResult;
import com.alandevise.tsgate.model.OperatorEnum;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.QueryResult;
import com.alandevise.tsgate.model.TSDBQuery;
import com.alandevise.tsgate.model.TSDBRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
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

    /** Fixed native empty-input SUM/COUNT shape for the fixture's server, protocol and schema. */
    EmptyAggregateExpectation semanticsEmptyAggregateExpectation(AggregationFunctionEnum function);

    record EmptyAggregateValue(boolean present, Number value) { }

    record EmptyAggregateExpectation(List<String> groupedDevices, EmptyAggregateValue groupedValue,
                                     int ungroupedRows, EmptyAggregateValue ungroupedValue) {
        public EmptyAggregateExpectation {
            groupedDevices = List.copyOf(groupedDevices);
            if (ungroupedRows < 0 || ungroupedRows > 1)
                throw new IllegalArgumentException("An empty ungrouped aggregate must declare zero or one result row");
        }
    }
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

    @Test
    default void nonEmptyWhitespaceTagsKeepTheirValuesAndSameTimePointIdentityThroughPojoWrites() throws Exception {
        long time = semanticsTime();
        // These fixed values are distinct identities; empty strings and null tags retain their existing semantics.
        List<String> devices = List.of(" ", "  ", " a ", "\u2003", "\u00a0");
        DefaultTSDBMetadataResolver resolver = new DefaultTSDBMetadataResolver();
        TSDBEntityMetadata metadata = resolver.resolve(semanticsQuery().build().getRecordType());
        assertEquals(semanticsMeasurement(), metadata.measurement());
        TSDBColumnMetadata deviceColumn = metadata.tagColumns().stream()
                .filter(column -> column.getColumnName().equals("device")).findFirst().orElseThrow();
        TSDBColumnMetadata valueColumn = metadata.fieldColumns().stream()
                .filter(column -> column.getColumnName().equals("value")).findFirst().orElseThrow();
        Constructor<?> constructor = metadata.entityType().getDeclaredConstructor();
        constructor.setAccessible(true);
        List<Object> pojos = new ArrayList<>();
        List<TSDBRecord> expectedPoints = new ArrayList<>();
        Map<String, Double> expectedValues = new LinkedHashMap<>();
        for (int index = 0; index < devices.size(); index++) {
            String device = devices.get(index);
            double value = index + 1d;
            Object pojo = constructor.newInstance();
            metadata.timeColumn().write(pojo, time);
            deviceColumn.write(pojo, device);
            valueColumn.write(pojo, value);
            pojos.add(pojo);
            expectedPoints.add(record(time, device, Map.of("value", value)));
            expectedValues.put(device, value);
        }
        BatchWriteResult written = new TGTemplate(semanticsAdapter()).batchWriteDetailed(pojos);
        assertTrue(written.isSuccess());
        assertEquals(devices.size(), written.requestedRecords());
        assertEquals(devices.size(), written.validatedRecords());
        assertEquals(devices.size(), written.committedRecords());
        awaitSemanticsVisibility(expectedPoints);

        TSDBQuery all = detailQuery();
        all.setStartTime(time);
        all.setEndTime(time);
        QueryResult result = semanticsAdapter().query(null, all);
        assertTrue(result.isSuccess());
        assertEquals(devices.size(), result.getRowCount());
        assertWhitespaceTagRows(result.getRows(), time, expectedValues);
        assertEquals(devices.size(), semanticsAdapter().count(null, all));

        Map<String, Double> typedValues = new LinkedHashMap<>();
        List<?> restored = semanticsQuery().timeRange(time, time).list();
        assertEquals(devices.size(), restored.size());
        for (Object pojo : restored) {
            TSDBRecord point = resolver.toRecord(pojo);
            assertEquals(time, point.timestamp());
            String device = point.tags().get("device");
            double value = assertInstanceOf(Number.class, point.fields().get("value")).doubleValue();
            assertNull(typedValues.put(device, value), "Typed results must preserve each distinct tag identity");
        }
        assertEquals(expectedValues, typedValues);

        for (String device : devices) {
            TGQueryBuilder<?> equality = semanticsQuery().timeRange(time, time).whereTag("device", device);
            assertWhitespaceTagRows(equality.list(Map.class), time, Map.of(device, expectedValues.get(device)));
            assertEquals(1L, semanticsAdapter().count(null, equality.build()));
        }
        for (List<String> selected : List.of(devices, devices.subList(0, 2), devices.subList(2, devices.size()))) {
            Map<String, Double> expectedSelection = new LinkedHashMap<>();
            for (String device : selected) expectedSelection.put(device, expectedValues.get(device));
            TGQueryBuilder<?> membership = semanticsQuery().timeRange(time, time).whereTagIn("device", selected);
            assertWhitespaceTagRows(membership.list(Map.class), time, expectedSelection);
            assertEquals(selected.size(), semanticsAdapter().count(null, membership.build()));
        }
        assertEquals(devices.size(), semanticsAdapter().count(null, detailQuery()));
    }

    private static void assertWhitespaceTagRows(List<?> rows, long time, Map<String, Double> expectedValues) {
        assertEquals(expectedValues.size(), rows.size());
        Map<String, Double> actualValues = new LinkedHashMap<>();
        for (Object raw : rows) {
            Map<?, ?> row = assertInstanceOf(Map.class, raw);
            assertEquals(time, assertInstanceOf(Long.class, row.get("time")));
            String device = assertInstanceOf(String.class, row.get("device"));
            double value = assertInstanceOf(Number.class, row.get("value")).doubleValue();
            assertNull(actualValues.put(device, value), "Distinct tag values at one timestamp must not collapse");
        }
        assertEquals(expectedValues, actualValues);
    }

    @Test
    default void primaryTimeCaseVariantsPreserveMillisecondsAndOperatorCapabilities() {
        long time = semanticsTime();
        List<TSDBRecord> points = List.of(record(time, "case-a", Map.of("value", 1d)),
                record(time + 1, "case-b", Map.of("value", 2d)));
        assertTrue(semanticsAdapter().batchWrite(null, points));
        awaitSemanticsVisibility(points);
        // InfluxDB 3 quoted names remain case-sensitive; InfluxQL and IoTDB recognize time variants.
        List<String> columns = semanticsBackendId().equals("influxdb3")
                ? List.of("time") : List.of("time", "TIME", "Time");
        for (String column : columns) {
            for (Object operand : List.of(time, java.time.Instant.ofEpochMilli(time),
                    java.time.Instant.ofEpochMilli(time).plusNanos(999_999))) {
                TSDBQuery query = detailQuery();
                query.setTimeColumn(column);
                query.setFilters(List.of(new QueryFilter(column, OperatorEnum.EQ, List.of(operand))));
                List<Map<String, Object>> rows = semanticsAdapter().query(null, query).getRows();
                assertEquals(1, rows.size(), column + " / " + operand);
                assertEquals(time, rows.get(0).get("time"));
                assertEquals(1L, semanticsAdapter().count(null, query));
                PageResult<Map> page = semanticsQuery().where(column, OperatorEnum.EQ, operand).page(1, 1, Map.class);
                assertEquals(1L, page.getTotal());
                assertEquals(1L, page.getTotalPages());
                assertEquals(1, page.getRows().size());
                assertEquals(time, page.getRows().get(0).get("time"));
                assertFalse(page.isHasNext());
            }
            for (OperatorEnum operator : List.of(OperatorEnum.NE, OperatorEnum.IN)) {
                BackendCapability capability = operator == OperatorEnum.NE
                        ? BackendCapability.TIME_FILTER_NE : BackendCapability.TIME_FILTER_IN;
                TSDBQuery query = detailQuery();
                query.setTimeColumn(column);
                query.setFilters(List.of(new QueryFilter(column, operator,
                        operator == OperatorEnum.NE ? List.of(time) : List.of(time, time + 1))));
                if (BackendContractRegistry.profile(semanticsBackendId()).supports(capability)) {
                    List<Map<String, Object>> rows = semanticsAdapter().query(null, query).getRows();
                    List<Long> expected = operator == OperatorEnum.NE ? List.of(time + 1) : List.of(time, time + 1);
                    assertEquals(expected, rows.stream().map(row -> (Long) row.get("time")).toList());
                    assertEquals(expected.size(), semanticsAdapter().count(null, query));
                } else {
                    assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                            assertThrows(TSDBException.class, () -> semanticsAdapter().query(null, query)).getErrorCode());
                    assertEquals(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                            assertThrows(TSDBException.class, () -> semanticsAdapter().count(null, query)).getErrorCode());
                }
            }
        }
        assertEquals(2L, semanticsAdapter().count(null, detailQuery()));
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
    default void filteredEmptyResultsKeepQueryCountAndOffsetPagesConsistent() {
        List<TSDBRecord> points = List.of(record(semanticsTime(), "a", Map.of("value", 1d)),
                record(semanticsTime() + 1, "b", Map.of("value", 2d)));
        assertTrue(semanticsAdapter().batchWrite(null, points));
        awaitSemanticsVisibility(points);
        assertFilteredEmptyResultShape(null, false, 0, null, List.of());
        for (AggregationFunctionEnum function : List.of(AggregationFunctionEnum.SUM, AggregationFunctionEnum.COUNT)) {
            EmptyAggregateExpectation expected = semanticsEmptyAggregateExpectation(function);
            assertNotNull(expected, "The fixture must explicitly declare each function's native empty-input shape");
            assertNotNull(expected.groupedValue());
            assertNotNull(expected.ungroupedValue());
            assertFilteredEmptyResultShape(function, true, expected.groupedDevices().size(),
                    expected.groupedValue(), expected.groupedDevices());
            assertFilteredEmptyResultShape(function, false, expected.ungroupedRows(),
                    expected.ungroupedValue(), List.of());
        }
        assertEquals(points.size(), semanticsAdapter().count(null, detailQuery()));
    }

    private void assertFilteredEmptyResultShape(AggregationFunctionEnum function, boolean grouped,
                                                int expectedRows, EmptyAggregateValue expectedValue, List<String> expectedDevices) {
        TSDBQuery query = detailQuery();
        query.setFilters(List.of(new QueryFilter("value", OperatorEnum.GT, List.of(1_000_000d))));
        if (function != null) query.setAggregations(List.of(new AggregationSpec("value", function, "empty_value")));
        if (grouped) query.setGroupByTags(List.of("device"));
        com.alandevise.tsgate.model.QueryResult result = semanticsAdapter().query(null, query);
        assertTrue(result.isSuccess());
        List<Map<String, Object>> rows = result.getRows();
        assertEquals(expectedRows, result.getRowCount());
        assertEquals(expectedRows, rows.size());
        if (grouped) assertEquals(expectedDevices, rows.stream().map(row -> row.get("device")).toList());
        for (Map<String, Object> row : rows) {
            assertNotNull(function);
            assertEquals(expectedValue.present(), row.containsKey("empty_value"));
            if (expectedValue.present()) {
                if (expectedValue.value() == null) assertNull(row.get("empty_value"));
                else assertEquals(expectedValue.value().doubleValue(),
                        assertInstanceOf(Number.class, row.get("empty_value")).doubleValue());
            }
        }
        assertEquals(expectedRows, semanticsAdapter().count(null, query),
                "Count must measure emitted result rows rather than an aggregate value or contributing points");
        for (int pageNumber : List.of(1, Math.max(2, expectedRows + 1))) {
            List<Map<String, Object>> expectedPageRows = pageNumber == 1
                    ? rows.subList(0, Math.min(1, rows.size())) : List.of();
            for (boolean counted : List.of(true, false)) {
                TGQueryBuilder<?> builder = semanticsQuery().where("value", OperatorEnum.GT, 1_000_000d).orderByTimeAsc();
                if (function != null) builder.aggregate("value", function, "empty_value");
                if (grouped) builder.groupByTag("device");
                PageResult<Map> page = builder.totalPageCount(counted).page(pageNumber, 1, Map.class);
                assertEquals(expectedPageRows, page.getRows());
                assertEquals(pageNumber < expectedRows, page.isHasNext());
                assertEquals(pageNumber, page.getPageNum());
                assertEquals(1, page.getPageSize());
                assertEquals(pageNumber - 1, page.getOffset());
                if (counted) {
                    assertEquals((long) expectedRows, page.getTotal());
                    assertEquals((long) expectedRows, page.getTotalPages());
                } else {
                    assertNull(page.getTotal());
                    assertNull(page.getTotalPages());
                }
            }
        }
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
