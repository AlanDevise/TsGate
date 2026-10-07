package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.TSDBQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TSDBQueryValidatorTest {

    @Test void rejectsNullQueryWithArgumentError() {
        TSDBException failure = assertThrows(TSDBException.class, () -> TSDBQueryValidator.validate(null));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure).hasMessageContaining("query");
    }

    @ParameterizedTest @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void rejectsNonPositiveExplicitLimit(int limit) {
        TSDBQuery query = new TSDBQuery();
        query.setLimit(limit);
        TSDBException failure = assertThrows(TSDBException.class, () -> TSDBQueryValidator.validate(query));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure).hasMessageContaining("limit");
    }

    @ParameterizedTest @ValueSource(ints = {-1, Integer.MIN_VALUE})
    void rejectsNegativeOffset(int offset) {
        TSDBQuery query = new TSDBQuery();
        query.setOffset(offset);
        TSDBException failure = assertThrows(TSDBException.class, () -> TSDBQueryValidator.validate(query));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure).hasMessageContaining("offset");
    }

    @ParameterizedTest @MethodSource("reversedBounds")
    void rejectsReversedTimeBoundsWithoutSubtractingThem(long start, long end) {
        TSDBQuery query = new TSDBQuery();
        query.setStartTime(start);
        query.setEndTime(end);
        TSDBException failure = assertThrows(TSDBException.class, () -> TSDBQueryValidator.validate(query));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure).hasMessageContaining("startTime").hasMessageContaining("endTime");
    }

    static Stream<Arguments> reversedBounds() {
        return Stream.of(Arguments.of(2L, 1L), Arguments.of(0L, -1L),
                Arguments.of(Long.MAX_VALUE, Long.MIN_VALUE));
    }

    @ParameterizedTest @MethodSource("validQueries")
    void preservesLegalArgumentsAndLeavesBackendLimitsToTheAdapter(Integer limit, Integer offset,
                                                                   Long start, Long end, boolean probe) {
        TSDBQuery query = new TSDBQuery();
        query.setLimit(limit);
        query.setOffset(offset);
        query.setStartTime(start);
        query.setEndTime(end);
        query.setPaginationProbe(probe);
        TSDBQuery snapshot = query.copy();
        TSDBQueryValidator.validate(query);
        assertThat(query).usingRecursiveComparison().isEqualTo(snapshot);
    }

    static Stream<Arguments> validQueries() {
        return Stream.of(
                Arguments.of(null, null, null, null, false),
                Arguments.of(1, 0, 0L, 0L, false),
                Arguments.of(null, 0, Long.MIN_VALUE, Long.MAX_VALUE, false),
                Arguments.of(100, Integer.MAX_VALUE, null, 1L, false),
                Arguments.of(100, null, 1L, null, false),
                Arguments.of(10_001, 0, 1L, 2L, true),
                Arguments.of(Integer.MAX_VALUE, null, null, null, false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"aliases", "tag-alias", "tags", "window-tag", "window-alias"})
    void rejectsCollidingAggregateOutputNames(String scenario) {
        TSDBQuery query = aggregateQuery();
        switch (scenario) {
            case "aliases" -> query.getAggregations().add(new AggregationSpec("other", AggregationFunctionEnum.MAX, "total"));
            case "tag-alias" -> query.setGroupByTags(List.of("total"));
            case "tags" -> query.setGroupByTags(List.of("device", "device"));
            case "window-tag" -> { query.setGroupByTime("1h"); query.setGroupByTags(List.of("window_start")); }
            case "window-alias" -> {
                query.setGroupByTime("1h");
                query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "window_start")));
            }
            default -> throw new AssertionError(scenario);
        }
        TSDBQuery original = query.copy();
        TSDBException failure = assertThrows(TSDBException.class,
                () -> TSDBQueryValidator.validateAggregationOutputNames(query, String::trim));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure).hasMessageContaining("Duplicate aggregation output name");
        assertThat(query).usingRecursiveComparison().isEqualTo(original);
    }

    @Test void physicalColumnIdentityControlsCaseSensitiveAggregateCollisions() {
        TSDBQuery query = aggregateQuery();
        query.getAggregations().add(new AggregationSpec("other", AggregationFunctionEnum.MAX, "TOTAL"));
        TSDBQueryValidator.validateAggregationOutputNames(query, String::trim);
        TSDBException failure = assertThrows(TSDBException.class, () -> TSDBQueryValidator.validateAggregationOutputNames(
                query, column -> column.trim().toLowerCase(Locale.ROOT)));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
    }

    @Test void windowStartIsAvailableWhenNoWindowIsGeneratedAndDetailGroupingIsUnchanged() {
        TSDBQuery query = aggregateQuery();
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "window_start")));
        TSDBQueryValidator.validateAggregationOutputNames(query, String::trim);
        query.setAggregations(List.of());
        query.setGroupByTime("1h");
        query.setGroupByTags(List.of("window_start", "window_start"));
        TSDBQueryValidator.validateAggregationOutputNames(query, String::trim);
    }

    @Test void caseDistinctWindowAndTagOutputsRemainValidForQuotedIdentifiers() {
        TSDBQuery query = aggregateQuery();
        query.setGroupByTime("1h");
        query.setGroupByTags(List.of("WINDOW_START", "TOTAL"));
        TSDBQueryValidator.validateAggregationOutputNames(query, String::trim);
    }

    private static TSDBQuery aggregateQuery() {
        TSDBQuery query = new TSDBQuery();
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
        return query;
    }
}
