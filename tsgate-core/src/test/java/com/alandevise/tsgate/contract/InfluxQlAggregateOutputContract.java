package com.alandevise.tsgate.contract;

import com.alandevise.tsgate.core.TGQueryBuilder;
import com.alandevise.tsgate.core.TGTemplate;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.AggregationFunctionEnum;
import com.alandevise.tsgate.model.AggregationSpec;
import com.alandevise.tsgate.model.TSDBQuery;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** InfluxQL-only aggregate invariants inherited by InfluxDB1 and openGemini's HTTP adapter. */
public interface InfluxQlAggregateOutputContract extends SharedAdapterContract {
    @ParameterizedTest
    @ValueSource(strings = {"alias", "tag", "window-alias", "window-tag"})
    default void implicitTimeOutputCollisionsFailBeforeQueryCountOrTemplateIo(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = implicitTimeQuery(scenario, "time");
            TGQueryBuilder<AggregateContractPoint> builder = templateQuery(fixture, query);
            assertArgument(() -> fixture.adapter().query(null, query));
            assertArgument(() -> fixture.adapter().count(null, query));
            assertArgument(() -> builder.list(Map.class));
            assertArgument(() -> builder.page(1, 2, Map.class));
            assertEquals(0, fixture.ioCount());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"alias", "tag", "window-alias", "window-tag"})
    default void caseDistinctTimeOutputNamesRemainAvailable(String scenario) throws Exception {
        try (SharedAdapterFixture fixture = createFixture()) {
            fixture.initialize();
            TSDBQuery query = implicitTimeQuery(scenario, "TIME");
            TGQueryBuilder<AggregateContractPoint> builder = templateQuery(fixture, query);
            fixture.enqueueRows();
            assertTrue(fixture.adapter().query(null, query).isSuccess());
            fixture.enqueueCount(0);
            assertEquals(0, fixture.adapter().count(null, query));
            fixture.enqueueRows();
            assertTrue(builder.list(Map.class).isEmpty());
            fixture.enqueueCount(0);
            assertEquals(0L, builder.page(1, 2, Map.class).getTotal().longValue());
            assertEquals(4, fixture.ioCount());
        }
    }

    private static TSDBQuery implicitTimeQuery(String scenario, String name) {
        TSDBQuery query = SharedAdapterContract.detail();
        query.setStartTime(0L);
        query.setEndTime(3_600_000L);
        query.setAggregations(List.of(new AggregationSpec("value", AggregationFunctionEnum.SUM,
                scenario.endsWith("alias") ? name : "total")));
        if (scenario.endsWith("tag")) query.setGroupByTags(List.of(name));
        if (scenario.startsWith("window-")) query.setGroupByTime("1h");
        return query;
    }

    private static TGQueryBuilder<AggregateContractPoint> templateQuery(SharedAdapterFixture fixture,
                                                                        TSDBQuery query) {
        TGQueryBuilder<AggregateContractPoint> builder = new TGTemplate(fixture.adapter())
                .query(AggregateContractPoint.class).timeRange(query.getStartTime(), query.getEndTime())
                .groupByTags(query.getGroupByTags());
        if (query.getGroupByTime() != null) builder.groupByTime(query.getGroupByTime());
        for (AggregationSpec aggregation : query.getAggregations()) {
            builder.aggregate(aggregation.field(), aggregation.function(), aggregation.alias());
        }
        return builder;
    }

    private static void assertArgument(org.junit.jupiter.api.function.Executable operation) {
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, assertThrows(TSDBException.class, operation).getErrorCode());
    }
}
