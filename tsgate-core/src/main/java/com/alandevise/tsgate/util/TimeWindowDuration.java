package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Validated fixed-duration window shared by the two SQL dialects. */
public final class TimeWindowDuration {
    private static final Pattern FORMAT = Pattern.compile("([0-9]+)\\s*(ms|s|m|h|d)?");
    private final long amount;
    private final String unit;

    private TimeWindowDuration(long amount, String unit) {
        this.amount = amount;
        this.unit = unit;
    }

    /**
     * Accepts a positive integer with ms/s/m/h/d; a missing unit means milliseconds.
     * The full duration must fit in a signed long number of milliseconds.
     */
    public static TimeWindowDuration parse(String window) {
        Matcher matcher = FORMAT.matcher(window == null ? "" : window.trim().toLowerCase(Locale.ROOT));
        if (!matcher.matches()) {
            throw invalidWindow(window);
        }
        try {
            long amount = Long.parseLong(matcher.group(1));
            if (amount <= 0) {
                throw invalidWindow(window);
            }
            String unit = matcher.group(2) == null ? "ms" : matcher.group(2);
            long unitMillis = switch (unit) {
                case "s" -> 1_000L;
                case "m" -> 60_000L;
                case "h" -> 3_600_000L;
                case "d" -> 86_400_000L;
                default -> 1L;
            };
            Math.multiplyExact(amount, unitMillis);
            return new TimeWindowDuration(amount, unit);
        } catch (NumberFormatException | ArithmeticException exception) {
            throw invalidWindow(window);
        }
    }

    public long amount() {
        return amount;
    }

    public String unit() {
        return unit;
    }

    public String toIoTDBInterval() {
        return amount + unit;
    }

    public String toSqlInterval() {
        return amount + " " + switch (unit) {
            case "s" -> "seconds";
            case "m" -> "minutes";
            case "h" -> "hours";
            case "d" -> "days";
            default -> "milliseconds";
        };
    }

    private static TSDBException invalidWindow(String window) {
        return new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                "Invalid time window: " + window
                        + "; expected a positive integer with ms/s/m/h/d whose duration fits in signed long milliseconds");
    }
}
