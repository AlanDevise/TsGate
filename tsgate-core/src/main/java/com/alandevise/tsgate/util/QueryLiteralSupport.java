package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;

import java.math.BigDecimal;
import java.time.Instant;

/** Safe numeric literals and epoch-millisecond operands shared by SQL translators. */
public final class QueryLiteralSupport {
    private QueryLiteralSupport() {
    }

    /**
     * Reads a number's text once and returns an independent, validated decimal value.
     * This also prevents mutable or custom Number implementations from changing a checked literal.
     *
     * @param number numeric query operand
     * @return a new plain BigDecimal containing the checked value
     * @throws TSDBException when the operand has no valid finite decimal representation
     */
    public static BigDecimal decimalValue(Number number) {
        try {
            return new BigDecimal(number.toString());
        } catch (RuntimeException exception) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Query number must have a valid finite decimal representation", exception);
        }
    }

    /**
     * Encodes the checked decimal rather than reading the original operand again.
     * Exponents are preserved compactly instead of being expanded into arbitrarily long strings.
     *
     * @param number numeric query operand
     * @return a safe SQL numeric literal
     */
    public static String numericLiteral(Number number) {
        return decimalValue(number).toString();
    }

    /**
     * Converts a time-filter operand to epoch milliseconds without rounding numeric values.
     * Instant operands follow the existing millisecond time-range API; strings are not inferred.
     *
     * @param value a Number representing whole epoch milliseconds, or an Instant
     * @return signed epoch milliseconds
     * @throws TSDBException when the type, range, or fractional precision is invalid
     */
    public static long exactEpochMillis(Object value) {
        try {
            if (value instanceof Instant instant) {
                return instant.toEpochMilli();
            }
            if (value instanceof Double || value instanceof Float) {
                return new BigDecimal(((Number) value).doubleValue()).longValueExact();
            }
            if (value instanceof Number number) {
                return decimalValue(number).longValueExact();
            }
        } catch (ArithmeticException | NumberFormatException exception) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Time filter must fit signed 64-bit whole epoch milliseconds", exception);
        }
        throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                "Time filter requires a whole epoch-millisecond Number or Instant");
    }
}
