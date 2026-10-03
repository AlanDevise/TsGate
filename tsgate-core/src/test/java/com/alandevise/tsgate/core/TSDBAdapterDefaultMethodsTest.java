package com.alandevise.tsgate.core;

import com.alandevise.tsgate.adapter.TSDBAdapter;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.*;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TSDBAdapterDefaultMethodsTest {
    @Test void writeAndBatchConvenienceMethodsPreserveDetailedOutcome() {
        TSDBAdapter adapter = mock(TSDBAdapter.class, CALLS_REAL_METHODS);
        TSDBRecord record = new TSDBRecord("metrics", 1L, null, Map.of("value", 3));
        doReturn(BatchWriteResult.success(1, 1)).when(adapter).batchWriteDetailed("db", List.of(record));
        assertThat(adapter.write("db", record)).isTrue();
        assertThat(adapter.batchWrite("db", List.of(record))).isTrue();
        assertThat(adapter.getMaxBatchRecords()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test void fallbackCountRemovesPagingWithoutMutatingTheOriginalQuery() {
        TSDBAdapter adapter = mock(TSDBAdapter.class, CALLS_REAL_METHODS);
        doReturn(TGTemplateTest.rows(Map.of("value", 1), Map.of("value", 2)))
                .when(adapter).query(eq("db"), any());
        TSDBQuery original = new TSDBQuery();
        original.setMeasurement("metrics"); original.setLimit(1); original.setOffset(20);
        original.setCursorTime(12L); original.setStrictCursor(true); original.setCursorValues(Map.of("time", 12L));
        original.setPaginationProbe(true);
        assertThat(adapter.count("db", original)).isEqualTo(2L);
        ArgumentCaptor<TSDBQuery> captured = ArgumentCaptor.forClass(TSDBQuery.class);
        verify(adapter).query(eq("db"), captured.capture());
        TSDBQuery actual = captured.getValue();
        assertThat(actual.getMeasurement()).isEqualTo("metrics");
        assertThat(actual.getLimit()).isNull(); assertThat(actual.getOffset()).isNull();
        assertThat(actual.getCursorTime()).isNull(); assertThat(actual.getCursorValues()).isEmpty();
        assertThat(actual.isStrictCursor()).isFalse();
        assertThat(actual.isPaginationProbe()).isFalse();
        assertThat(original.isPaginationProbe()).isTrue();
        assertThat(original.getLimit()).isEqualTo(1); assertThat(original.getOffset()).isEqualTo(20);
        assertThat(original.isStrictCursor()).isTrue(); assertThat(original.getCursorValues()).isNotEmpty();
    }

    @Test void fallbackCountRejectsBackendFailureOrNullAndAcceptsEmptyRows() {
        TSDBAdapter adapter = mock(TSDBAdapter.class, CALLS_REAL_METHODS);
        TSDBQuery query = new TSDBQuery();
        assertThatThrownBy(() -> adapter.count("db", query)).isInstanceOf(TSDBException.class);
        doReturn(QueryResult.failure("offline")).when(adapter).query(any(), any());
        assertThatThrownBy(() -> adapter.count("db", query)).isInstanceOf(TSDBException.class).hasMessage("offline");
        QueryResult empty = new QueryResult(); empty.setSuccess(true); empty.setRows(null);
        doReturn(empty).when(adapter).query(any(), any());
        assertThat(adapter.count("db", query)).isZero();
    }
}
