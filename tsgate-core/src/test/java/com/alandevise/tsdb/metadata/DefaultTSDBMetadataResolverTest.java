package com.alandevise.tsdb.metadata;

import com.alandevise.tsdb.annotation.*;
import com.alandevise.tsdb.exception.*;
import com.alandevise.tsdb.model.TSDBRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultTSDBMetadataResolverTest {
    private final DefaultTSDBMetadataResolver resolver = new DefaultTSDBMetadataResolver();

    @Test void resolvesPrivateAndInheritedFieldsAndCachesImmutableMetadata() {
        TSDBEntityMetadata metadata = resolver.resolve(Meter.class);
        assertThat(metadata.entityType()).isEqualTo(Meter.class);
        assertThat(metadata.measurement()).isEqualTo("meter");
        assertThat(metadata.timeColumn().getColumnName()).isEqualTo("sample_time");
        assertThat(metadata.timeColumn().getJavaType()).isEqualTo(Long.class);
        assertThat(metadata.timeColumn().getRole()).isEqualTo(TSDBColumnRoleEnum.TIME);
        assertThat(metadata.tagColumns()).extracting(TSDBColumnMetadata::getColumnName).containsExactly("device_id");
        assertThat(metadata.fieldColumns()).extracting(TSDBColumnMetadata::getColumnName).containsExactly("reading", "quality");
        assertThat(resolver.resolve(Meter.class)).isSameAs(metadata);
        assertThatThrownBy(() -> metadata.tagColumns().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.fieldColumns().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void concurrentMetadataRequestsProduceAConsistentCachedDefinition() {
        List<CompletableFuture<TSDBEntityMetadata>> requests = new ArrayList<>();
        for (int i = 0; i < 12; i++) requests.add(CompletableFuture.supplyAsync(() -> resolver.resolve(Meter.class)));
        TSDBEntityMetadata first = requests.get(0).join();
        assertThat(requests.stream().map(CompletableFuture::join)).allMatch(metadata -> metadata == first);
    }

    @ParameterizedTest @MethodSource("invalidModels")
    void invalidEntityDefinitionsFailWithMetadataError(Class<?> model) {
        TSDBException error = assertThrows(TSDBException.class, () -> resolver.resolve(model));
        assertThat(error.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.METADATA_ERROR);
        assertThat(error.getMessage()).isNotBlank();
    }

    static Stream<Class<?>> invalidModels() {
        return Stream.of(NoMeasurement.class, BlankMeasurement.class, NoTime.class, TwoTimes.class,
                WrongTimeType.class, NoFields.class, ConflictingRole.class, DuplicateColumns.class,
                DuplicateInheritedColumn.class, DuplicateTimeColumn.class);
    }

    @Test void inputNullsProduceArgumentErrors() {
        assertThat(assertThrows(TSDBException.class, () -> resolver.resolve(null)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(assertThrows(TSDBException.class, () -> resolver.toRecord(null)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
        assertThat(assertThrows(TSDBException.class, () -> resolver.toEntity(PlainDto.class, null)).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
    }

    @Test void convertsAnnotatedPojoToImmutableRecordWithStringTagsAndTypedFields() {
        Meter meter = new Meter();
        meter.time = 123L;
        meter.deviceId = 42;
        meter.value = 12.5;
        meter.ignored = "do not persist";
        TSDBRecord record = resolver.toRecord(meter);
        assertThat(record.measurement()).isEqualTo("meter");
        assertThat(record.timestamp()).isEqualTo(123L);
        assertThat(record.tags()).containsExactlyEntriesOf(Map.of("device_id", "42"));
        assertThat(record.fields()).containsExactlyEntriesOf(Map.of("reading", 12.5));
        assertThatThrownBy(() -> record.fields().put("extra", 1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void optionalTagAndFieldValuesAreOmittedButTimestampIsRequired() {
        Meter meter = new Meter();
        meter.time = 5L;
        meter.quality = true;
        TSDBRecord record = resolver.toRecord(meter);
        assertThat(record.tags()).isEmpty();
        assertThat(record.fields()).containsExactlyEntriesOf(Map.of("quality", true));
        meter.time = null;
        assertThatThrownBy(() -> resolver.toRecord(meter)).isInstanceOf(TSDBException.class)
                .hasMessageContaining("@TGTime value must not be null");
    }

    @Test void aRecordWithOnlyNullDataFieldsIsRejectedBeforeBackendIo() {
        Meter meter = new Meter();
        meter.time = 5L;
        assertThatThrownBy(() -> resolver.toRecord(meter)).isInstanceOf(TSDBException.class)
                .hasMessageContaining("At least one @TGField");
    }

    @Test void blankColumnAnnotationsFallBackToDocumentedNames() {
        TSDBEntityMetadata metadata = resolver.resolve(DefaultColumns.class);
        assertThat(metadata.timeColumn().getColumnName()).isEqualTo("time");
        assertThat(metadata.tagColumns().get(0).getColumnName()).isEqualTo("device");
        assertThat(metadata.fieldColumns().get(0).getColumnName()).isEqualTo("value");
        TSDBRecord record = resolver.toRecord(new DefaultColumns());
        assertThat(record.timestamp()).isEqualTo(0L);
        assertThat(record.fields()).containsEntry("value", 1);
    }

    @Test void annotatedReadUsesPhysicalNamesWithCaseInsensitiveCompatibility() {
        Meter restored = resolver.toEntity(Meter.class,
                Map.of("SAMPLE_TIME", 123L, "DEVICE_ID", "42", "READING", 12.5, "QUALITY", true));
        assertThat(restored.time).isEqualTo(123L);
        assertThat(restored.deviceId).isEqualTo(42);
        assertThat(restored.value).isEqualTo(12.5);
        assertThat(restored.quality).isTrue();
    }

    @Test void physicalNullDoesNotFallBackToJavaFieldNameOrCaseVariant() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("reading", null);
        row.put("READING", 9.0);
        row.put("value", 99.0);
        assertThat(resolver.toEntity(Meter.class, row).value).isNull();
    }

    @Test void annotatedFieldDoesNotUseAnUnrelatedJavaNameWhenPhysicalColumnIsMissing() {
        Meter restored = resolver.toEntity(Meter.class, Map.of("value", 99.0, "time", 4L, "deviceId", 77));
        assertThat(restored.value).isNull();
        assertThat(restored.time).isNull();
        assertThat(restored.deviceId).isNull();
    }

    @Test void plainDtoSupportsSnakeCaseCaseVariantsTimeAliasesAndInheritedFields() {
        PlainDto dto = resolver.toEntity(PlainDto.class,
                Map.of("device_code", "meter-a", "READING_VALUE", "12.5", "_time", Instant.ofEpochMilli(1234), "base_count", "4"));
        assertThat(dto.deviceCode).isEqualTo("meter-a");
        assertThat(dto.readingValue).isEqualTo(12.5);
        assertThat(dto.timestamp).isEqualTo(1234L);
        assertThat(dto.baseCount).isEqualTo(4);
    }

    @Test void exactNullWinsOverLooserDtoColumnAliasesAndPrimitivesKeepDefault() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("readingValue", null);
        row.put("reading_value", 99.0);
        row.put("base_count", null);
        row.put("constant", "changed");
        PlainDto dto = resolver.toEntity(PlainDto.class, row);
        assertThat(dto.readingValue).isNull();
        assertThat(dto.baseCount).isEqualTo(7);
        assertThat(PlainDto.constant).isEqualTo("constant");
    }

    @Test void convertsCommonNumericBooleanTextAndTimeRepresentations() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("longValue", "1234");
        row.put("intValue", 23L);
        row.put("doubleValue", "2.5");
        row.put("floatValue", 1.5d);
        row.put("booleanValue", "true");
        row.put("text", 42);
        row.put("instant", "2026-01-01T00:00:00Z");
        row.put("date", 1234L);
        TypesDto dto = resolver.toEntity(TypesDto.class, row);
        assertThat(dto.longValue).isEqualTo(1234L);
        assertThat(dto.intValue).isEqualTo(23);
        assertThat(dto.doubleValue).isEqualTo(2.5);
        assertThat(dto.floatValue).isEqualTo(1.5f);
        assertThat(dto.booleanValue).isTrue();
        assertThat(dto.text).isEqualTo("42");
        assertThat(dto.instant).isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
        assertThat(dto.date).isEqualTo(new Date(1234L));
    }

    @Test void mapsAllDocumentedTimeInputRepresentations() {
        for (Object time : List.of(1234L, Instant.ofEpochMilli(1234L), new Date(1234L), "1234",
                Instant.ofEpochMilli(1234L).toString())) {
            assertThat(resolver.toEntity(PlainDto.class, Map.of("time", time)).timestamp).isEqualTo(1234L);
        }
    }

    @Test void resultTypeWithoutNoArgsConstructorGetsActionableMetadataError() {
        TSDBException error = assertThrows(TSDBException.class,
                () -> resolver.toEntity(NoDefaultConstructor.class, Map.of("value", 1)));
        assertThat(error.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.METADATA_ERROR);
        assertThat(error).hasMessageContaining("no-args constructor").hasCauseInstanceOf(NoSuchMethodException.class);
    }

    @Test void columnMetadataCanReadAndWritePrivateMembers() {
        Meter target = new Meter();
        TSDBColumnMetadata field = resolver.resolve(Meter.class).fieldColumns().get(0);
        field.write(target, 17.5);
        assertThat(field.read(target)).isEqualTo(17.5);
        assertThat(field.getField().getName()).isEqualTo("value");
        assertThat(field.getRole()).isEqualTo(TSDBColumnRoleEnum.FIELD);
    }

    @ParameterizedTest @MethodSource("invalidConversions")
    void rejectsInvalidOrLossyConversionsWithFieldAndTypeContext(String field, Object value) {
        TSDBException error = assertThrows(TSDBException.class,
                () -> resolver.toEntity(StrictTypesDto.class, Map.of(field, value)));
        assertThat(error.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.METADATA_ERROR);
        assertThat(error).hasMessageContaining("StrictTypesDto." + field)
                .hasMessageContaining(value.getClass().getName()).hasCauseInstanceOf(RuntimeException.class);
    }

    static Stream<Arguments> invalidConversions() {
        return Stream.of(
                Arguments.of("longValue", "not-a-number"),
                Arguments.of("primitiveLong", "not-a-number"),
                Arguments.of("longValue", ""),
                Arguments.of("longValue", "9223372036854775808"),
                Arguments.of("longValue", new BigInteger("-9223372036854775809")),
                Arguments.of("longValue", new BigDecimal("1.5")),
                Arguments.of("longValue", Double.NaN),
                Arguments.of("longValue", Double.POSITIVE_INFINITY),
                Arguments.of("intValue", 2147483648L),
                Arguments.of("intValue", -2147483649L),
                Arguments.of("intValue", 1.25d),
                Arguments.of("intValue", "12.5"),
                Arguments.of("intValue", new BigDecimal("2147483648")),
                Arguments.of("shortValue", 32768),
                Arguments.of("byteValue", -129),
                Arguments.of("integer", "12.5"),
                Arguments.of("booleanValue", "yes"),
                Arguments.of("booleanValue", 1),
                Arguments.of("booleanValue", ""),
                Arguments.of("boxedBoolean", "off"),
                Arguments.of("doubleValue", "1e309"),
                Arguments.of("doubleValue", "NaN"),
                Arguments.of("doubleValue", Double.POSITIVE_INFINITY),
                Arguments.of("doubleValue", new BigDecimal("1e-400")),
                Arguments.of("floatValue", Double.MAX_VALUE),
                Arguments.of("floatValue", "1e-100"),
                Arguments.of("instant", "invalid-date"),
                Arguments.of("instant", 1.5d),
                Arguments.of("date", "invalid-date"),
                Arguments.of("date", new BigInteger("9223372036854775808")),
                Arguments.of("unsupported", "text"));
    }

    @Test void acceptsExactNumericBoundariesAndCaseInsensitiveBooleanText() {
        StrictTypesDto dto = resolver.toEntity(StrictTypesDto.class, Map.of(
                "longValue", new BigInteger("9223372036854775807"),
                "primitiveLong", "-9223372036854775808",
                "intValue", new BigDecimal("2147483647.000"),
                "shortValue", -32768L,
                "byteValue", 127L,
                "booleanValue", " TrUe ",
                "boxedBoolean", " FaLsE ",
                "integer", "123456789012345678901234567890",
                "decimal", "1234567890.123456789"));
        assertThat(dto.longValue).isEqualTo(Long.MAX_VALUE);
        assertThat(dto.primitiveLong).isEqualTo(Long.MIN_VALUE);
        assertThat(dto.intValue).isEqualTo(Integer.MAX_VALUE);
        assertThat(dto.shortValue).isEqualTo((short) -32768);
        assertThat(dto.byteValue).isEqualTo((byte) 127);
        assertThat(dto.booleanValue).isTrue();
        assertThat(dto.boxedBoolean).isFalse();
        assertThat(dto.integer).isEqualTo(new BigInteger("123456789012345678901234567890"));
        assertThat(dto.decimal).isEqualByComparingTo("1234567890.123456789");
    }

    @Test void mapsLargeIntegralFloatUsingItsExactBinaryValue() {
        StrictTypesDto dto = resolver.toEntity(StrictTypesDto.class, Map.of("longValue", 1.0e18f));
        assertThat(dto.longValue).isEqualTo(999999984306749440L);
    }

    @Test void mapsLargeIntegralDoubleUsingItsExactBinaryValue() {
        StrictTypesDto dto = resolver.toEntity(StrictTypesDto.class, Map.of("longValue", 1.2345678901234568e18));
        assertThat(dto.longValue).isEqualTo(1234567890123456768L);
    }

    @Test void preservesInstantPrecisionAndSupportsMillisecondTimeConversions() {
        Instant instant = Instant.parse("2026-01-01T00:00:00.123456789Z");
        StrictTypesDto dto = resolver.toEntity(StrictTypesDto.class, Map.of(
                "instant", instant.toString(), "date", instant, "longValue", instant));
        assertThat(dto.instant).isEqualTo(instant);
        assertThat(dto.date.getTime()).isEqualTo(instant.toEpochMilli());
        assertThat(dto.longValue).isEqualTo(instant.toEpochMilli());
    }

    @Test void preservesCompatiblePrimitiveCharacterValues() {
        assertThat(resolver.toEntity(StrictTypesDto.class, Map.of("character", 'x')).character).isEqualTo('x');
    }

    @Test void rejectsNullResultTypeAsAnArgumentError() {
        assertThat(assertThrows(TSDBException.class, () -> resolver.toEntity(null, Map.of())).getErrorCode())
                .isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
    }

    static class StrictTypesDto {
        Long longValue; long primitiveLong; int intValue; short shortValue; byte byteValue;
        boolean booleanValue; Boolean boxedBoolean; Double doubleValue; float floatValue;
        BigInteger integer; BigDecimal decimal; Instant instant; Date date; Thread unsupported; char character;
    }

    static class TimeBase { @TGTime(" sample_time ") Long time; }
    @TGMeasurement(" meter ") static class Meter extends TimeBase {
        @TGTag(" device_id ") Integer deviceId;
        @TGField(" reading ") private Double value;
        @TGField Boolean quality;
        String ignored;
    }
    @TGMeasurement("d") static class DefaultColumns {
        @TGTime(" ") long timestamp;
        @TGTag(" ") String device;
        @TGField(" ") int value = 1;
    }
    static class PlainBase { int baseCount = 7; }
    static class PlainDto extends PlainBase {
        static String constant = "constant";
        String deviceCode;
        Double readingValue = 4.0;
        Long timestamp;
    }
    static class TypesDto {
        Long longValue; int intValue; Double doubleValue; float floatValue; boolean booleanValue;
        String text; Instant instant; Date date;
    }
    record NoDefaultConstructor(int value) {}
    static class NoMeasurement { @TGTime long time; @TGField int value; }
    @TGMeasurement(" ") static class BlankMeasurement { @TGTime long time; @TGField int value; }
    @TGMeasurement("m") static class NoTime { @TGField int value; }
    @TGMeasurement("m") static class TwoTimes { @TGTime long a; @TGTime long b; @TGField int value; }
    @TGMeasurement("m") static class WrongTimeType { @TGTime Instant time; @TGField int value; }
    @TGMeasurement("m") static class NoFields { @TGTime long time; @TGTag String tag; }
    @TGMeasurement("m") static class ConflictingRole { @TGTime long time; @TGField @TGTag int value; }
    @TGMeasurement("m") static class DuplicateColumns { @TGTime long time; @TGTag("VALUE") String tag; @TGField("value") int value; }
    @TGMeasurement("m") static class DuplicateInheritedColumn extends TimeBase { @TGField("SAMPLE_TIME") int value; }
    @TGMeasurement("m") static class DuplicateTimeColumn { @TGTime long time; @TGField("TIME") int value; }
}
