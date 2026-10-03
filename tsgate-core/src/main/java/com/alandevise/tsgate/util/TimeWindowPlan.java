package com.alandevise.tsgate.util;

import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Resolves calendar-day boundaries without changing a pooled session's time zone. */
public final class TimeWindowPlan {
    private static final int MAX_CALENDAR_BUCKETS = 10_000;
    private final String fixedOrigin;
    private final List<Bucket> buckets;

    private TimeWindowPlan(String fixedOrigin, List<Bucket> buckets) {
        this.fixedOrigin = fixedOrigin;
        this.buckets = Collections.unmodifiableList(new ArrayList<>(buckets));
    }

    /** A half-open bucket expressed in Unix epoch milliseconds. */
    public record Bucket(long startInclusive, long endExclusive) { }

    /**
     * Resolves fixed-offset windows or bounded calendar-day windows for a region.
     * Region-based sub-day windows must have a bounded range with a stable UTC offset.
     * Day windows follow local midnight, so a DST transition can produce a 23/25-hour day.
     */
    public static TimeWindowPlan resolve(String window, String timeZone, Long startTime, Long endTime) {
        TimeWindowDuration duration = TimeWindowDuration.parse(window);
        try {
            ZoneId zone = timeZone == null || timeZone.isBlank() ? ZoneOffset.UTC : ZoneId.of(timeZone.trim());
            if (zone.getRules().isFixedOffset()) {
                return fixed(zone.getRules().getOffset(Instant.EPOCH));
            }
            if (startTime == null || endTime == null || startTime > endTime) {
                throw argument("Region time zones require a bounded, ordered timeRange; use UTC or a fixed offset otherwise");
            }
            Instant start = Instant.ofEpochMilli(startTime);
            Instant end = Instant.ofEpochMilli(endTime);
            if (!"d".equals(duration.unit())) {
                ZoneOffsetTransition transition = zone.getRules().nextTransition(start);
                if (transition != null && !transition.getInstant().isAfter(end)) {
                    throw new TSDBException(TSDBErrorCodeEnum.UNSUPPORTED_OPERATION,
                            "A sub-day window cannot cross a region time-zone offset transition; use calendar days or a fixed offset");
                }
                return fixed(zone.getRules().getOffset(start));
            }
            long days = duration.amount();
            long firstEpochDay = start.atZone(zone).toLocalDate().toEpochDay();
            long alignedDay = Math.multiplyExact(Math.floorDiv(firstEpochDay, days), days);
            LocalDate date = LocalDate.ofEpochDay(alignedDay);
            List<Bucket> buckets = new ArrayList<>();
            long bucketStart = date.atStartOfDay(zone).toInstant().toEpochMilli();
            while (bucketStart <= endTime) {
                if (buckets.size() == MAX_CALENDAR_BUCKETS) {
                    throw argument("Calendar windows exceed 10000 buckets; narrow timeRange or increase the day interval");
                }
                LocalDate next = date.plusDays(days);
                long bucketEnd = next.atStartOfDay(zone).toInstant().toEpochMilli();
                if (bucketEnd > bucketStart) {
                    buckets.add(new Bucket(bucketStart, bucketEnd));
                }
                date = next;
                bucketStart = bucketEnd;
            }
            return new TimeWindowPlan(null, buckets);
        } catch (DateTimeException | ArithmeticException exception) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "Invalid or out-of-range window time zone, interval or range: " + timeZone, exception);
        }
    }

    public boolean isCalendar() {
        return fixedOrigin == null;
    }

    /** Returns a timestamp literal origin with an explicit offset, including +00:00 for UTC. */
    public String fixedOrigin() {
        return fixedOrigin;
    }

    public List<Bucket> buckets() {
        return buckets;
    }

    private static TimeWindowPlan fixed(ZoneOffset offset) {
        String suffix = ZoneOffset.UTC.equals(offset) ? "+00:00" : offset.getId();
        return new TimeWindowPlan("1970-01-01T00:00:00" + suffix, Collections.emptyList());
    }

    private static TSDBException argument(String message) {
        return new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, message);
    }
}
