package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.OperatorEnum;
import com.alandevise.tsgate.model.QueryFilter;
import com.alandevise.tsgate.model.TSDBQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
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

    @ParameterizedTest @MethodSource("malformedFilters")
    void rejectsMalformedFiltersWithoutChangingCallerState(QueryFilter filter) {
        TSDBQuery query = new TSDBQuery();
        query.setFilters(Arrays.asList(filter));
        TSDBQuery snapshot = query.copy();
        TSDBException failure = assertThrows(TSDBException.class, () -> TSDBQueryValidator.validate(query));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(failure.getMessage()).containsIgnoringCase("filter");
        assertThat(query).usingRecursiveComparison().isEqualTo(snapshot);
    }

    static Stream<Arguments> malformedFilters() {
        Stream<Arguments> malformed = Stream.of(
                Arguments.of((Object) null),
                Arguments.of(new QueryFilter(null, OperatorEnum.EQ, List.of(1))),
                Arguments.of(new QueryFilter(" ", OperatorEnum.EQ, List.of(1))),
                Arguments.of(new QueryFilter("value", null, List.of(1))),
                Arguments.of(new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1))),
                Arguments.of(new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1, 2, 3))),
                Arguments.of(new QueryFilter("value", OperatorEnum.EQ, Arrays.asList((Object) null))),
                Arguments.of(new QueryFilter("value", OperatorEnum.BETWEEN, Arrays.asList(1, null))),
                Arguments.of(new QueryFilter("value", OperatorEnum.IN, Arrays.asList(1, null, 2))));
        Stream<Arguments> empty = Arrays.stream(OperatorEnum.values())
                .map(operator -> Arguments.of(new QueryFilter("value", operator, List.of())));
        Stream<Arguments> extraScalars = Arrays.stream(OperatorEnum.values())
                .filter(operator -> operator != OperatorEnum.IN && operator != OperatorEnum.BETWEEN)
                .map(operator -> Arguments.of(new QueryFilter("value", operator, List.of(1, 2))));
        Stream<Arguments> nonFinite = Stream.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                        Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY)
                .flatMap(number -> Stream.of(
                        Arguments.of(new QueryFilter("value", OperatorEnum.GT, List.of(number))),
                        Arguments.of(new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1, number))),
                        Arguments.of(new QueryFilter("value", OperatorEnum.IN, List.of(1, number)))));
        return Stream.of(malformed, empty, extraScalars, nonFinite).flatMap(stream -> stream);
    }

    @ParameterizedTest @MethodSource("validFilters")
    void preservesValidOperandsAndLeavesPrecisionRulesToBackends(QueryFilter filter) {
        TSDBQuery query = new TSDBQuery();
        query.setFilters(List.of(filter));
        TSDBQuery snapshot = query.copy();
        TSDBQueryValidator.validate(query);
        assertThat(query).usingRecursiveComparison().isEqualTo(snapshot);
    }

    static Stream<Arguments> validFilters() {
        Stream<Arguments> operators = Arrays.stream(OperatorEnum.values()).map(operator -> Arguments.of(
                new QueryFilter("value", operator, operator == OperatorEnum.BETWEEN ? List.of(1, 2) : List.of(1))));
        Stream<Arguments> literals = Stream.of("null", "NaN", "Infinity", true, 0.0d, -0.0f,
                        Double.MIN_VALUE, Double.MAX_VALUE, Float.MIN_VALUE, Float.MAX_VALUE,
                        new BigInteger("999999999999999999999999999999999999"), new BigDecimal("1E+10000"))
                .map(value -> Arguments.of(new QueryFilter("value", OperatorEnum.EQ, List.of(value))));
        return Stream.concat(operators, Stream.concat(literals,
                Stream.of(Arguments.of(new QueryFilter("value", OperatorEnum.IN, List.of(1, 2, 3))))));
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
