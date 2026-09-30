package com.alandevise.tsdb.core;

import com.alandevise.tsdb.model.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

class TGQueryBuilderTest {
    private TGQueryBuilder<TGTemplateTest.Point> builder() {
        return new TGTemplate(null).query(TGTemplateTest.Point.class);
    }

    @Test void builderResolvesMeasurementTimeAndEntityType() {
        TGQueryBuilder<TGTemplateTest.Point> initial = builder();
        TGQueryBuilder<TGTemplateTest.Point> chained = initial.select("value")
                .whereTag("device_code", "a").limit(5);
        assertThat(chained).isSameAs(initial).isExactlyInstanceOf(TGQueryBuilder.class);
        TSDBQuery query = chained.build();
        assertThat(query.getMeasurement()).isEqualTo("metrics");
        assertThat(query.getTimeColumn()).isEqualTo("time");
        assertThat(query.getRecordType()).isEqualTo(TGTemplateTest.Point.class);
        assertThat(query.getOrder()).isEqualTo(SortOrderEnum.DESC);
    }

    @Test void selectFiltersBlankNamesAndCopiesCallerCollection() {
        List<String> columns = new ArrayList<>(Arrays.asList(" value ", "", null, "device_code"));
        TGQueryBuilder<?> builder = builder().select(columns);
        columns.clear();
        assertThat(builder.build().getSelectColumns()).containsExactly("value", "device_code");
        assertThat(builder.select((String[]) null).build().getSelectColumns()).isEmpty();
    }

    @Test void filtersRetainOperatorsAndValuesWhileDiscardingNullInputs() {
        TSDBQuery query = builder().whereTag(" device_code ", "a")
                .whereTagIn("region", Arrays.asList("north", null, "south"))
                .where("value", OperatorEnum.BETWEEN, List.of(1.0, 2.0))
                .where("value", OperatorEnum.GE, 10)
                .where(" ", OperatorEnum.EQ, 4)
                .where("value", null, 4)
                .where("value", OperatorEnum.EQ, (Object) null)
                .where("value", OperatorEnum.IN, (Collection<?>) null)
                .where("value", OperatorEnum.IN, List.of())
                .where("value", OperatorEnum.IN, Arrays.asList((Object) null))
                .build();
        assertThat(query.getFilters()).containsExactly(
                new QueryFilter("device_code", OperatorEnum.EQ, List.of("a")),
                new QueryFilter("region", OperatorEnum.IN, List.of("north", "south")),
                new QueryFilter("value", OperatorEnum.BETWEEN, List.of(1.0, 2.0)),
                new QueryFilter("value", OperatorEnum.GE, List.of(10)));
    }

    @Test void timeRangesPreserveOpenBoundsAndConvertInstants() {
        TGQueryBuilder<?> builder = builder().timeRange(Instant.ofEpochMilli(12L), Instant.ofEpochMilli(25L));
        assertThat(builder.build().getStartTime()).isEqualTo(12L);
        assertThat(builder.build().getEndTime()).isEqualTo(25L);
        TSDBQuery openEnd = builder.timeRange(8L, (Long) null).build();
        assertThat(openEnd.getStartTime()).isEqualTo(8L);
        assertThat(openEnd.getEndTime()).isNull();
        TSDBQuery openBoth = builder.timeRange((Instant) null, null).build();
        assertThat(openBoth.getStartTime()).isNull();
        assertThat(openBoth.getEndTime()).isNull();
    }

    @Test void cursorAndPagingValuesAreStoredInAnIndependentSnapshot() {
        Map<String, Object> cursor = new LinkedHashMap<>(Map.of("time", 10L, "device_code", "a"));
        TGQueryBuilder<?> builder = builder().cursor(cursor).cursorColumns(" time ", "device_code", " ")
                .cursorTime(10L).limit(25).offset(50);
        cursor.clear();
        TSDBQuery snapshot = builder.build();
        assertThat(snapshot.getCursorValues()).containsEntry("time", 10L);
        assertThat(snapshot.getCursorColumns()).containsExactly("time", "device_code");
        assertThat(snapshot.getCursorTime()).isEqualTo(10L);
        assertThat(snapshot.getLimit()).isEqualTo(25);
        assertThat(snapshot.getOffset()).isEqualTo(50);
        snapshot.getCursorValues().clear();
        snapshot.getCursorColumns().clear();
        builder.limit(99);
        assertThat(builder.build().getCursorValues()).hasSize(2);
        assertThat(builder.build().getCursorColumns()).hasSize(2);
        assertThat(snapshot.getLimit()).isEqualTo(25);
    }

    @Test void sortingUpdatesDuplicateColumnsWithoutReorderingThem() {
        TSDBQuery query = builder().orderByFieldAsc("value").thenByFieldDesc("device_code")
                .thenByFieldDesc(" value ").thenByFieldAsc(" ").build();
        assertThat(query.getSortSpecs()).containsExactly(new SortSpec("value", SortOrderEnum.DESC),
                new SortSpec("device_code", SortOrderEnum.DESC));
        assertThat(query.getOrder()).isEqualTo(SortOrderEnum.DESC);
        TSDBQuery reset = builder().orderByFieldDesc("value").thenByFieldAsc("device_code").orderByTimeAsc().build();
        assertThat(reset.getSortSpecs()).containsExactly(new SortSpec("time", SortOrderEnum.ASC));
        assertThat(reset.getOrder()).isEqualTo(SortOrderEnum.ASC);
        assertThat(builder().orderByTimeDesc().build().getSortSpecs())
                .containsExactly(new SortSpec("time", SortOrderEnum.DESC));
        assertThat(builder().orderByFieldAsc(null).build().getSortSpecs()).isEmpty();
    }

    @Test void caseDistinctSortColumnsRemainSeparate() {
        assertThat(builder().orderByFieldAsc("value").thenByFieldDesc("VALUE").build().getSortSpecs())
                .containsExactly(new SortSpec("value", SortOrderEnum.ASC), new SortSpec("VALUE", SortOrderEnum.DESC));
    }

    @Test void defaultAggregationAliasesAreIndependentOfJvmLocale() {
        Locale original = Locale.getDefault();
        try {
            for (Locale locale : List.of(Locale.ENGLISH, Locale.CHINESE, Locale.forLanguageTag("tr-TR"))) {
                Locale.setDefault(locale);
                assertThat(builder().aggregate("value", AggregationFunctionEnum.MIN, null).build().getAggregations())
                        .containsExactly(new AggregationSpec("value", AggregationFunctionEnum.MIN, "value_min"));
                assertThat(builder().aggregate("value", AggregationFunctionEnum.MIN, "VALUE_MIN").build().getAggregations())
                        .containsExactly(new AggregationSpec("value", AggregationFunctionEnum.MIN, "VALUE_MIN"));
            }
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test void aggregationBuildsDefaultAliasesAndTimeWindowAndTimezone() {
        TSDBQuery query = builder().groupByTime(Duration.ofSeconds(5))
                .groupByTag(" device_code ").groupByTag(" ")
                .groupByTags(Arrays.asList("region", null, " "))
                .aggregate(" value ", null, null)
                .aggregate("value", AggregationFunctionEnum.MAX, " peak ")
                .aggregate(" ", AggregationFunctionEnum.MIN, "ignored")
                .timeZone("Asia/Shanghai").build();
        assertThat(query.getGroupByTime()).isEqualTo("5000ms");
        assertThat(query.getGroupByTags()).containsExactly("device_code", "region");
        assertThat(query.getAggregations()).containsExactly(new AggregationSpec("value", AggregationFunctionEnum.AVG, "value_avg"),
                new AggregationSpec("value", AggregationFunctionEnum.MAX, "peak"));
        assertThat(query.getTimeZone()).isEqualTo("Asia/Shanghai");
        assertThat(query.hasAggregations()).isTrue();
        assertThat(builder().groupByTime((Duration) null).build().getGroupByTime()).isNull();
        assertThat(builder().groupByTime("1h").build().getGroupByTime()).isEqualTo("1h");
    }

    @Test void futureBuilderChangesCannotMutateAlreadyBuiltQueries() {
        TGQueryBuilder<?> builder = builder().select("value").whereTag("device_code", "a");
        TSDBQuery first = builder.build();
        builder.select("device_code").where("value", OperatorEnum.GT, 3).aggregate("value", null, null);
        assertThat(first.getSelectColumns()).containsExactly("value");
        assertThat(first.getFilters()).hasSize(1);
        assertThat(first.getAggregations()).isEmpty();
    }
}
