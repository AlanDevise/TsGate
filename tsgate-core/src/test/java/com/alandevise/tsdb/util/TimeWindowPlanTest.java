package com.alandevise.tsdb.util;

import com.alandevise.tsdb.exception.TSDBException;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import static org.assertj.core.api.Assertions.*;

class TimeWindowPlanTest {
    static long midnight(String day, String zone) {
        return LocalDate.parse(day).atStartOfDay(ZoneId.of(zone)).toInstant().toEpochMilli();
    }
    @Test void springForwardDayHas23Hours() {
        var plan=TimeWindowPlan.resolve("1d","America/New_York",midnight("2026-03-07","America/New_York"),midnight("2026-03-10","America/New_York")-1);
        assertThat(plan.isCalendar()).isTrue();
        assertThat(plan.buckets()).hasSize(3);
        assertThat(plan.buckets().stream().map(b -> (b.endExclusive()-b.startInclusive())/3600000).toList()).containsExactly(24L,23L,24L);
    }
    @Test void fallBackDayHas25Hours() {
        var plan=TimeWindowPlan.resolve("1d","America/New_York",midnight("2026-10-31","America/New_York"),midnight("2026-11-03","America/New_York")-1);
        assertThat(plan.buckets().stream().map(b -> (b.endExclusive()-b.startInclusive())/3600000).toList()).containsExactly(24L,25L,24L);
    }
    @Test void southernHemisphereUsesItsOwnTransitionDate() {
        var plan=TimeWindowPlan.resolve("1d","Australia/Sydney",midnight("2026-04-05","Australia/Sydney"),midnight("2026-04-06","Australia/Sydney")-1);
        assertThat(plan.buckets().get(0).endExclusive()-plan.buckets().get(0).startInclusive()).isEqualTo(25L*3600000);
    }
    @Test void fixedOffsetsDoNotNeedBoundedRanges() {
        assertThat(TimeWindowPlan.resolve("5m","+08:00",null,null).fixedOrigin()).isEqualTo("1970-01-01T00:00:00+08:00");
        assertThat(TimeWindowPlan.resolve("1d",null,null,null).fixedOrigin()).isEqualTo("1970-01-01T00:00:00+00:00");
    }
    @Test void subDayRegionWindowUsesRangeOffsetInsteadOf1970Offset() {
        var plan=TimeWindowPlan.resolve("5m","America/New_York",Instant.parse("2026-07-01T00:00:00Z").toEpochMilli(),Instant.parse("2026-07-02T00:00:00Z").toEpochMilli());
        assertThat(plan.fixedOrigin()).endsWith("-04:00");
    }
    @Test void subDayTransitionCannotSilentlyUseFixedOffset() {
        assertThatThrownBy(() -> TimeWindowPlan.resolve("1h","America/New_York",midnight("2026-03-08","America/New_York"),midnight("2026-03-09","America/New_York")))
                .isInstanceOf(TSDBException.class).hasMessageContaining("offset transition");
    }
    @Test void invalidUnboundedAndReversedRegionRangesFail() {
        assertThatThrownBy(() -> TimeWindowPlan.resolve("1d","America/New_York",null,null)).isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> TimeWindowPlan.resolve("1d","America/New_York",2L,1L)).isInstanceOf(TSDBException.class);
        assertThatThrownBy(() -> TimeWindowPlan.resolve("1d","not_a_zone",0L,1L)).isInstanceOf(TSDBException.class);
    }
    @Test void bucketGenerationIsBoundedAndImmutable() {
        assertThatThrownBy(() -> TimeWindowPlan.resolve("1d","America/New_York",midnight("1970-01-01","America/New_York"),midnight("2026-01-01","America/New_York")))
                .isInstanceOf(TSDBException.class).hasMessageContaining("10000");
        var plan=TimeWindowPlan.resolve("1d","Asia/Shanghai",midnight("2026-01-01","Asia/Shanghai"),midnight("2026-01-02","Asia/Shanghai")-1);
        assertThatThrownBy(() -> plan.buckets().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void skippedLocalDateProducesNoZeroLengthBucket() {
        var plan=TimeWindowPlan.resolve("1d","Pacific/Apia",midnight("2011-12-29","Pacific/Apia"),midnight("2012-01-01","Pacific/Apia")-1);
        assertThat(plan.buckets()).hasSize(2).allSatisfy(b -> assertThat(b.endExclusive()).isGreaterThan(b.startInclusive()));
    }
    @Test void multiDayBucketsAreAlignedToLocalEpochDate() {
        var plan=TimeWindowPlan.resolve("2d","America/New_York",midnight("2026-03-08","America/New_York"),midnight("2026-03-09","America/New_York")-1);
        var local=Instant.ofEpochMilli(plan.buckets().get(0).startInclusive()).atZone(ZoneId.of("America/New_York")).toLocalDate();
        assertThat(Math.floorMod(local.toEpochDay(),2)).isZero();
    }
}
