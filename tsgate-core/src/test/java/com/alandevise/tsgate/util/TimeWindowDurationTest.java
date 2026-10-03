package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimeWindowDurationTest {
    @ParameterizedTest
    @CsvSource({"1, 1ms, 1 milliseconds", "250ms, 250ms, 250 milliseconds", "5s, 5s, 5 seconds",
            "2m, 2m, 2 minutes", "3h, 3h, 3 hours", "7d, 7d, 7 days", "' 02 H ', 2h, 2 hours"})
    void validWindowsHaveEquivalentNormalizedDialectRepresentations(String input, String iotdb, String sql) {
        TimeWindowDuration duration = TimeWindowDuration.parse(input);
        assertThat(duration.toIoTDBInterval()).isEqualTo(iotdb);
        assertThat(duration.toSqlInterval()).isEqualTo(sql);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "0", "0h", "-1h", "1.5h", "+1m", "1e3s", "1w", "1month", "1h30m",
            "NaN", "9223372036854775808ms", "9223372036854775807s", "9223372036854775807m",
            "9223372036854775807h", "9223372036854775807d",
            "1h') OR 1=1 --", "1h;DROP TABLE metrics", "1 h extra"})
    void rejectsInvalidOrUnsafeWindowsWithoutChangingTheirMeaning(String input) {
        TSDBException error = assertThrows(TSDBException.class, () -> TimeWindowDuration.parse(input));
        assertThat(error.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(error.getMessage()).contains("expected a positive integer");
    }

    @ParameterizedTest
    @CsvSource({"ms,1", "s,1000", "m,60000", "h,3600000", "d,86400000"})
    void acceptsTheLargestAmountWhoseMillisecondsFitInLong(String unit, long multiplier) {
        long maximum = Long.MAX_VALUE / multiplier;
        TimeWindowDuration duration = TimeWindowDuration.parse(maximum + unit);
        assertThat(duration.amount()).isEqualTo(maximum);
        assertThat(duration.unit()).isEqualTo(unit);
        assertThat(duration.toIoTDBInterval()).isEqualTo(maximum + unit);
    }

    @ParameterizedTest
    @CsvSource({"s,1000", "m,60000", "h,3600000", "d,86400000"})
    void rejectsTheFirstAmountWhoseMillisecondsOverflowLong(String unit, long multiplier) {
        long overflowingAmount = Long.MAX_VALUE / multiplier + 1;
        TSDBException error = assertThrows(TSDBException.class,
                () -> TimeWindowDuration.parse(overflowingAmount + unit));
        assertThat(error.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(error.getMessage()).contains("signed long milliseconds");
    }
}
