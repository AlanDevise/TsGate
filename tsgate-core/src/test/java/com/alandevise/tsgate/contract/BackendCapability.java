package com.alandevise.tsgate.contract;

import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.SortOrderEnum;
import com.alandevise.tsgate.model.SortSpec;
import com.alandevise.tsgate.model.TSDBQuery;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Optional operations exercised by the test-only backend capability matrix. */
public enum BackendCapability {
    BATCH_WRITE,
    FIELD_SORT,
    STRICT_COMPOSITE_CURSOR,
    REGIONAL_CALENDAR_DAY;

    /** Builds an actual operation; an unsupported capability must reject it before I/O. */
    public TSDBQuery query() {
        TSDBQuery query = SharedAdapterContract.detail();
        switch (this) {
            case BATCH_WRITE -> throw new AssertionError("BATCH_WRITE uses the shared write fixture");
            case FIELD_SORT -> query.setSortSpecs(List.of(new SortSpec("value", SortOrderEnum.DESC)));
            case STRICT_COMPOSITE_CURSOR -> {
                query.setStrictCursor(true);
                query.setCursorColumns(List.of("time", "device"));
                query.setCursorValues(Map.of("time", 1L, "device", "a"));
                query.setSortSpecs(List.of(new SortSpec("time", SortOrderEnum.ASC),
                        new SortSpec("device", SortOrderEnum.DESC)));
            }
            case REGIONAL_CALENDAR_DAY -> {
                query.setGroupByTime("1d");
                query.setTimeZone("America/New_York");
                query.setStartTime(Instant.parse("2026-03-08T05:00:00Z").toEpochMilli());
                query.setEndTime(Instant.parse("2026-03-09T03:59:59.999Z").toEpochMilli());
                query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM, "total")));
            }
        }
        return query;
    }
}
