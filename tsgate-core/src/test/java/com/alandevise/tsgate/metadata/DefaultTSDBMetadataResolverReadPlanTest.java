package com.alandevise.tsgate.metadata;

import com.alandevise.tsgate.annotation.TGField;
import com.alandevise.tsgate.annotation.TGTag;
import com.alandevise.tsgate.annotation.TGTime;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultTSDBMetadataResolverReadPlanTest {
    private final DefaultTSDBMetadataResolver resolver = new DefaultTSDBMetadataResolver();

    @Test void cachedPlansKeepRowsNullsAndConstructorDefaultsIndependent() {
        PlainDto first = resolver.toEntity(PlainDto.class,
                Map.of("_time", 11L, "sensor_value", 2.5, "base_count", "3"));
        assertThat(first.timestamp).isEqualTo(11L);
        assertThat(first.sensorValue).isEqualTo(2.5);
        assertThat(((PlainBase) first).baseCount).isEqualTo(3);

        Map<String, Object> nulls = new LinkedHashMap<>();
        nulls.put("timestamp", null);
        nulls.put("_time", 999L);
        nulls.put("sensorValue", null);
        nulls.put("sensor_value", 999.0);
        nulls.put("base_count", null);
        PlainDto second = resolver.toEntity(PlainDto.class, nulls);
        assertThat(second.timestamp).isNull();
        assertThat(second.sensorValue).isNull();
        assertThat(((PlainBase) second).baseCount).isEqualTo(5);

        PlainDto omitted = resolver.toEntity(PlainDto.class, Map.of());
        assertThat(omitted.timestamp).isEqualTo(99L);
        assertThat(omitted.sensorValue).isEqualTo(7.0);
        assertThat(((PlainBase) omitted).baseCount).isEqualTo(5);
        assertThat(second).isNotSameAs(first);
        assertThat(omitted).isNotSameAs(second);
    }

    @Test void annotatedReadPlansRemainIndependentFromWriteModelValidation() {
        ReadOnlyDto first = resolver.toEntity(ReadOnlyDto.class,
                Map.of("EVENT_TIME", 12L, "DEVICE_ID", "a", "READING", 3.5));
        assertThat(first.timestamp).isEqualTo(12L);
        assertThat(first.device).isEqualTo("a");
        assertThat(first.value).isEqualTo(3.5);
        assertThat(assertThrows(TSDBException.class, () -> resolver.resolve(ReadOnlyDto.class)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.METADATA_ERROR);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("reading", null);
        row.put("READING", 99.0);
        row.put("value", 88.0);
        assertThat(resolver.toEntity(ReadOnlyDto.class, row).value).isNull();
        assertThat(resolver.toEntity(ReadOnlyDto.class, Map.of("value", 88.0)).value).isEqualTo(4.0);
    }

    @Test void inheritedHiddenPrivateFieldsRetainSeparatePhysicalColumns() {
        ReadChild dto = resolver.toEntity(ReadChild.class, Map.of("parent_value", 1.5, "child_value", 2.5));
        assertThat(((ReadParent) dto).value).isEqualTo(1.5);
        assertThat(dto.value).isEqualTo(2.5);
        ReadChild next = resolver.toEntity(ReadChild.class, Map.of("PARENT_VALUE", 3.5, "CHILD_VALUE", 4.5));
        assertThat(((ReadParent) next).value).isEqualTo(3.5);
        assertThat(next.value).isEqualTo(4.5);
    }

    @Test void physicalFallbackPreservesUnicodeIgnoreCaseAndExactNullPrecedence() {
        assertThat(resolver.toEntity(UnicodeDto.class, Map.of("\u0130", 3L)).value).isEqualTo(3L);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("I", 4L);
        row.put("i", null);
        assertThat(resolver.toEntity(UnicodeDto.class, row).value).isNull();
    }

    @Test void sharedResolverConstructsOneFreshObjectPerConcurrentRow() {
        ConcurrentDto.constructions.set(0);
        List<CompletableFuture<Void>> requests = new ArrayList<>();
        for (int worker = 0; worker < 16; worker++) {
            int workerId = worker;
            requests.add(CompletableFuture.runAsync(() -> {
                for (int index = 0; index < 64; index++) {
                    long timestamp = workerId * 1000L + index;
                    ConcurrentDto result = resolver.toEntity(ConcurrentDto.class,
                            Map.of("timestamp", timestamp, "device_id", "device-" + workerId,
                                    "reading", timestamp + 0.5, "count", index));
                    assertThat(result.timestamp).isEqualTo(timestamp);
                    assertThat(result.deviceId).isEqualTo("device-" + workerId);
                    assertThat(result.reading).isEqualTo(timestamp + 0.5);
                    assertThat(result.count).isEqualTo(index);
                }
            }));
        }
        CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new)).join();
        assertThat(ConcurrentDto.constructions).hasValue(16 * 64);
    }

    @Test void cachedConstructorStillRunsAndPreservesInvocationFailureCauses() {
        FlakyConstructorDto.attempts.set(0);
        TSDBException failure = assertThrows(TSDBException.class,
                () -> resolver.toEntity(FlakyConstructorDto.class, Map.of("value", 1)));
        assertThat(failure.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.METADATA_ERROR);
        assertThat(failure).hasMessageContaining("no-args constructor")
                .hasCauseInstanceOf(InvocationTargetException.class);
        assertThat(failure.getCause().getCause()).isInstanceOf(IllegalStateException.class)
                .hasMessage("first construction");
        assertThat(resolver.toEntity(FlakyConstructorDto.class, Map.of("value", 9)).value).isEqualTo(9);
        assertThat(FlakyConstructorDto.attempts).hasValue(2);
    }

    @Test void warmedPlansRetainNumericErrorsAndRecoverForLaterRows() {
        assertThat(resolver.toEntity(IntegerDto.class, Map.of("value", "12")).value).isEqualTo(12);
        assertThatThrownBy(() -> resolver.toEntity(IntegerDto.class, Map.of("value", "12.5")))
                .isInstanceOfSatisfying(TSDBException.class,
                        error -> assertThat(error.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.METADATA_ERROR))
                .hasMessageContaining("IntegerDto.value")
                .hasMessageContaining("java.lang.String")
                .hasCauseInstanceOf(ArithmeticException.class);
        assertThat(resolver.toEntity(IntegerDto.class, Map.of("value", 13)).value).isEqualTo(13);
    }

    @Test void sameBinaryNameInDifferentClassLoadersGetsIndependentReadPlans() throws Exception {
        String name = ReloadableDto.class.getName();
        byte[] bytecode;
        try (InputStream input = ReloadableDto.class.getResourceAsStream("/" + name.replace('.', '/') + ".class")) {
            if (input == null) throw new IOException("Missing compiled reloadable DTO fixture");
            bytecode = input.readAllBytes();
        }
        Class<?> firstType = new ResultClassLoader(name, bytecode).loadClass(name);
        Class<?> secondType = new ResultClassLoader(name, bytecode).loadClass(name);
        Object first = resolver.toEntity(firstType, Map.of("value", 7L));
        Object second = resolver.toEntity(secondType, Map.of("value", 8L));
        assertThat(firstType).isNotSameAs(secondType);
        assertThat(first.getClass()).isSameAs(firstType);
        assertThat(second.getClass()).isSameAs(secondType);
        assertThat(firstType.getField("value").get(first)).isEqualTo(7L);
        assertThat(secondType.getField("value").get(second)).isEqualTo(8L);
        assertThatThrownBy(() -> firstType.cast(second)).isInstanceOf(ClassCastException.class);
    }

    private static class PlainBase { private int baseCount = 5; }
    private static class PlainDto extends PlainBase {
        private Long timestamp = 99L;
        private Double sensorValue = 7.0;
        private PlainDto() { }
    }
    private static class ReadOnlyDto {
        @TGTime("event_time") private Long timestamp;
        @TGTag("device_id") private String device;
        @TGField("reading") private Double value = 4.0;
    }
    private static class ReadParent { @TGField("parent_value") private Double value; }
    private static class ReadChild extends ReadParent { @TGField("child_value") private Double value; }
    private static class UnicodeDto { @TGField("i") private Long value; }
    private static class ConcurrentDto {
        private static final AtomicInteger constructions = new AtomicInteger();
        private Long timestamp;
        private String deviceId;
        private Double reading;
        private int count;
        private ConcurrentDto() { constructions.incrementAndGet(); }
    }
    private static class FlakyConstructorDto {
        private static final AtomicInteger attempts = new AtomicInteger();
        private int value;
        private FlakyConstructorDto() {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("first construction");
        }
    }
    private static class IntegerDto { private Integer value; }

    public static class ReloadableDto {
        public Long value;
        public ReloadableDto() { }
    }

    private static final class ResultClassLoader extends ClassLoader {
        private final String resultName;
        private final byte[] bytecode;
        private ResultClassLoader(String resultName, byte[] bytecode) {
            super(DefaultTSDBMetadataResolverReadPlanTest.class.getClassLoader());
            this.resultName = resultName;
            this.bytecode = bytecode;
        }
        @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                if (!name.equals(resultName)) return super.loadClass(name, resolve);
                Class<?> type = findLoadedClass(name);
                if (type == null) type = defineClass(name, bytecode, 0, bytecode.length);
                if (resolve) resolveClass(type);
                return type;
            }
        }
    }
}
