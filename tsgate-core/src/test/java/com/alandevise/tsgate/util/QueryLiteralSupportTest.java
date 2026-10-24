package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class QueryLiteralSupportTest {
    @Test
    void validatesAndOutputsTheSameSingleRead() {
        AtomicInteger reads = new AtomicInteger();
        Number changing = number(() -> reads.incrementAndGet() == 1 ? "17" : "0 OR true --");
        assertEquals("17", QueryLiteralSupport.numericLiteral(changing));
        assertEquals(1, reads.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0 OR true --", "NaN", "Infinity", "", " 17 ", "1; DROP TABLE points"})
    void rejectsNonNumericText(String text) {
        assertArgument(() -> QueryLiteralSupport.numericLiteral(number(() -> text)));
    }

    @Test
    void rejectsNullOrThrowingNumberTextWithoutReadingItAgain() {
        assertArgument(() -> QueryLiteralSupport.numericLiteral(null));
        assertArgument(() -> QueryLiteralSupport.numericLiteral(number(() -> null)));
        assertArgument(() -> QueryLiteralSupport.numericLiteral(number(() -> { throw new IllegalStateException(); })));
    }

    @Test
    void preservesPrecisionAndKeepsExponentsCompact() {
        assertEquals("9223372036854775808", QueryLiteralSupport.numericLiteral(new BigInteger("9223372036854775808")));
        assertEquals("0.1234567890123456789", QueryLiteralSupport.numericLiteral(new BigDecimal("0.1234567890123456789")));
        assertEquals("1E+100000", QueryLiteralSupport.numericLiteral(new BigDecimal("1e100000")));
    }

    @Test
    void timeOperandsUseExactNumericValuesAndTheExistingInstantMillisecondConvention() {
        assertEquals(Long.MIN_VALUE, QueryLiteralSupport.exactEpochMillis(Long.MIN_VALUE));
        assertEquals(Long.MAX_VALUE, QueryLiteralSupport.exactEpochMillis(BigInteger.valueOf(Long.MAX_VALUE)));
        assertEquals(-5L, QueryLiteralSupport.exactEpochMillis(new BigDecimal("-5.000")));
        assertEquals(Long.MIN_VALUE, QueryLiteralSupport.exactEpochMillis(-0x1.0p63));
        assertEquals(9223372036854774784L, QueryLiteralSupport.exactEpochMillis(Math.nextDown(0x1.0p63)));
        assertEquals(-1L, QueryLiteralSupport.exactEpochMillis(Instant.ofEpochSecond(-1, 999_999_999)));
    }

    @Test
    void rejectsTimeRoundingOverflowAndStringInference() {
        for (Object value : new Object[]{new BigDecimal("1.1"), 1.1d, Float.NaN, Double.POSITIVE_INFINITY,
                new BigInteger("9223372036854775808"), 0x1.0p63, Instant.MAX, "17", true, null}) {
            assertArgument(() -> QueryLiteralSupport.exactEpochMillis(value));
        }
    }

    private static Number number(java.util.function.Supplier<String> text) {
        return new Number() {
            @Override public int intValue() { return 17; }
            @Override public long longValue() { return 17; }
            @Override public float floatValue() { return 17; }
            @Override public double doubleValue() { return 17; }
            @Override public String toString() { return text.get(); }
        };
    }

    private static void assertArgument(org.junit.jupiter.api.function.Executable executable) {
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                assertThrows(TSDBException.class, executable).getErrorCode());
    }
}
