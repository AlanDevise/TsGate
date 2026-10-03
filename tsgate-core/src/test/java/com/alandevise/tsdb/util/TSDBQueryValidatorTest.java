package com.alandevise.tsdb.util;

import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.model.TSDBQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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
}
