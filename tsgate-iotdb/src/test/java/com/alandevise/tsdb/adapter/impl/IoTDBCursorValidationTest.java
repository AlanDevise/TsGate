package com.alandevise.tsdb.adapter.impl;

import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.model.SortOrderEnum;
import com.alandevise.tsdb.model.SortSpec;
import com.alandevise.tsdb.model.TSDBQuery;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class IoTDBCursorValidationTest {
    private IoTDBTableAdapter adapter;
    private ITableSessionPool pool;
    private ITableSession session;

    @BeforeEach
    void setUp() throws Exception {
        IoTDBProperties config = new IoTDBProperties();
        adapter = new IoTDBTableAdapter(config, config.getPool(), false);
        pool = mock(ITableSessionPool.class);
        session = mock(ITableSession.class);
        SessionDataSet data = mock(SessionDataSet.class);
        when(pool.getSession()).thenReturn(session);
        when(session.executeQueryStatement(anyString())).thenReturn(data);
        when(data.getColumnNames()).thenReturn(List.of());
        IoTDBTestPools.ready(adapter, pool, config.getDatabase());
    }

    static Stream<Arguments> exactTimes() {
        return Stream.of(
                Arguments.of(Long.MIN_VALUE, Long.toString(Long.MIN_VALUE)),
                Arguments.of(Long.MAX_VALUE, Long.toString(Long.MAX_VALUE)),
                Arguments.of(BigInteger.valueOf(Long.MAX_VALUE), Long.toString(Long.MAX_VALUE)),
                Arguments.of(new BigDecimal("1.0"), "1"),
                Arguments.of(new BigDecimal("-5.000"), "-5"),
                Arguments.of(1.0d, "1"), Arguments.of(1.0f, "1"),
                Arguments.of(-0.0d, "0"), Arguments.of(17, "17"),
                Arguments.of(new AtomicLong(42), "42"), Arguments.of("-17", "-17"),
                Arguments.of(Instant.ofEpochSecond(-1, 999_999_999), "-1"),
                Arguments.of(Instant.ofEpochMilli(Long.MAX_VALUE), Long.toString(Long.MAX_VALUE)),
                Arguments.of(Math.nextDown(0x1.0p63), "9223372036854774784"),
                Arguments.of(-0x1.0p63, Long.toString(Long.MIN_VALUE)));
    }

    @ParameterizedTest
    @MethodSource("exactTimes")
    void acceptsOnlyExactLongMilliseconds(Object value, String expected) throws Exception {
        adapter.query(null, strict(Map.of("time", value)));
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(session).executeQueryStatement(sql.capture());
        assertTrue(sql.getValue().contains("time > " + expected), sql.getValue());
    }

    static Stream<Arguments> invalidTimes() {
        return Stream.of(
                Arguments.of(new BigDecimal("1.9")), Arguments.of(1.9d), Arguments.of(-1.1f),
                Arguments.of(Double.NaN), Arguments.of(Double.POSITIVE_INFINITY),
                Arguments.of(Double.NEGATIVE_INFINITY), Arguments.of(Float.NaN),
                Arguments.of(Float.POSITIVE_INFINITY), Arguments.of(0x1.0p63),
                Arguments.of(Math.nextDown(-0x1.0p63)),
                Arguments.of(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)),
                Arguments.of(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)),
                Arguments.of(new BigDecimal("9223372036854775808")),
                Arguments.of("9223372036854775808"), Arguments.of("1.0"),
                Arguments.of("invalid"), Arguments.of(Instant.MAX), Arguments.of(Instant.MIN));
    }

    @ParameterizedTest
    @MethodSource("invalidTimes")
    void rejectsInvalidTimeBeforeBorrowingSession(Object value) {
        TSDBException error = assertThrows(TSDBException.class,
                () -> adapter.query(null, strict(Map.of("time", value))));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        verifyNoInteractions(pool);
    }

    @Test
    void normalizesUnquotedPhysicalColumnNamesWithoutGuessingTimeAliases() throws Exception {
        TSDBQuery query = strict(Map.of("Time", 12L, "VALUE", 7));
        query.setCursorColumns(List.of("TIME", "Value"));
        query.setSortSpecs(List.of(new SortSpec("TIME", SortOrderEnum.ASC),
                new SortSpec("VALUE", SortOrderEnum.DESC)));
        adapter.query(null, query);
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(session).executeQueryStatement(sql.capture());
        assertTrue(sql.getValue().contains("time > 12"), sql.getValue());
        assertTrue(sql.getValue().contains("value < 7"), sql.getValue());
        assertTrue(sql.getValue().contains("ORDER BY time ASC, value DESC"), sql.getValue());
    }

    @Test
    void rejectsAmbiguousCanonicalCursorKeysBeforeIo() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("time", 12L);
        values.put("VALUE", 99);
        values.put("value", 7);
        TSDBQuery query = strict(values);
        query.setCursorColumns(List.of("time", "value"));
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.query(null, query));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        verifyNoInteractions(pool);
    }

    @Test
    void rejectsTimestampFieldAsMissingTimeValue() {
        TSDBException error = assertThrows(TSDBException.class,
                () -> adapter.query(null, strict(Map.of("timestamp", 12L))));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        verifyNoInteractions(pool);
    }

    @Test
    void rejectsExtraCursorKeysBeforeIo() {
        TSDBException error = assertThrows(TSDBException.class,
                () -> adapter.query(null, strict(Map.of("time", 12L, "extra", 7))));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        verifyNoInteractions(pool);
    }

    @Test
    void rejectsConflictingCanonicalSortColumnsBeforeIo() {
        TSDBQuery query = strict(Map.of("time", 12L));
        query.setSortSpecs(List.of(new SortSpec("time", SortOrderEnum.ASC),
                new SortSpec("TIME", SortOrderEnum.DESC)));
        TSDBException error = assertThrows(TSDBException.class, () -> adapter.query(null, query));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR, error.getErrorCode());
        verifyNoInteractions(pool);
    }

    private static TSDBQuery strict(Map<String, Object> values) {
        TSDBQuery query = new TSDBQuery();
        query.setMeasurement("telemetry");
        query.setStrictCursor(true);
        query.setOrder(SortOrderEnum.ASC);
        query.setCursorColumns(List.of("time"));
        query.setCursorValues(values);
        return query;
    }
}
