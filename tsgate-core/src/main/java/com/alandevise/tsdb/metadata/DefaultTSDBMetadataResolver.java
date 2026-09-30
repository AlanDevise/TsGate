package com.alandevise.tsdb.metadata;

import com.alandevise.tsdb.annotation.TGField;
import com.alandevise.tsdb.annotation.TGMeasurement;
import com.alandevise.tsdb.annotation.TGTag;
import com.alandevise.tsdb.annotation.TGTime;
import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.model.TSDBRecord;
import lombok.NonNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * <h2>Default reflection-based annotation metadata resolver</h2>
 *
 * <p>Uses {@link ClassValue} to cache metadata by class and converts POJOs into {@link TSDBRecord} for writes.
 * Reads match annotated physical columns; unannotated DTOs also support field names and snake_case to camelCase.
 * Cache entries follow their class lifecycle, avoiding strong map references that prevent dynamic class unloading.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public class DefaultTSDBMetadataResolver implements TSDBMetadataResolver {

    /**
     * Marks a missing result column, distinct from a present column whose value is null.
     */
    private static final Object UNMAPPED = new Object();

    /**
     * Preserves explicit nulls in the loose index so computeIfAbsent cannot replace them as missing values.
     */
    private static final Object NULL_VALUE = new Object();

    private final ClassValue<TSDBEntityMetadata> cache = new ClassValue<>() {
        /**
         * Scans annotations when an entity class is first accessed and reuses the metadata on later accesses.
         * <p>Under contention, ClassValue can call computeValue more than once for the same class.
         * Scanning must therefore remain repeatable and create only local metadata snapshots without external side effects.</p>
         *
         * @param type entity class to scan, for example {@code AccruePoint.class}
         * @return resolved entity metadata
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-07-02
         */
        @Override
        protected TSDBEntityMetadata computeValue(@NonNull Class<?> type) {
            return scan(type);
        }

        /**
         * Scans a POJO's annotations and validates the time and field columns required for writing.
         * <p>Does not write to the database, register global objects, or change resolver state; repeat scans produce equivalent snapshots.
         * Calling setAccessible on the current {@link Field} is local, idempotent preparation for reflective access.</p>
         *
         * @param entityType entity class to scan, for example {@code AccruePoint.class}
         * @return resolved entity metadata
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-07-02
         */
        private TSDBEntityMetadata scan(Class<?> entityType) {
            // Write entities must declare a nonblank table name in the annotation.
            TGMeasurement measurement = entityType.getAnnotation(TGMeasurement.class);
            if (measurement == null || isBlank(measurement.value())) {
                throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                        entityType.getName() + " must declare @TGMeasurement");
            }

            TSDBColumnMetadata timeColumn = null;
            List<TSDBColumnMetadata> tagColumns = new ArrayList<>();
            List<TSDBColumnMetadata> fieldColumns = new ArrayList<>();
            // All column roles share a name registry to prevent duplicate physical columns from overwriting map entries.
            Map<String, TSDBColumnMetadata> columnsByPhysicalName = new LinkedHashMap<>();
            // Include inherited fields so base classes can provide reusable column declarations.
            for (Field field : fieldsOf(entityType)) {
                TGTime time = field.getAnnotation(TGTime.class);
                TGTag tag = field.getAnnotation(TGTag.class);
                TGField value = field.getAnnotation(TGField.class);
                // Each field can declare only one of the time, tag, and field roles.
                int annotationCount = (time == null ? 0 : 1) + (tag == null ? 0 : 1) + (value == null ? 0 : 1);
                if (annotationCount > 1) {
                    throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                            "TSDB column role annotations are mutually exclusive: "
                                    + entityType.getName() + "." + field.getName());
                }
                if (time != null) {
                    // The sole time column must be long/Long; its default physical name is time.
                    if (timeColumn != null) {
                        throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                                entityType.getName() + " must declare only one @TGTime");
                    }
                    if (field.getType() != Long.class && field.getType() != long.class) {
                        throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                                "@TGTime field type must be Long or long: "
                                        + entityType.getName() + "." + field.getName());
                    }
                    TSDBColumnMetadata column = new TSDBColumnMetadata(field, defaultIfBlank(time.value(), "time"),
                            TSDBColumnRoleEnum.TIME);
                    registerPhysicalColumn(entityType, columnsByPhysicalName, column);
                    timeColumn = column;
                } else if (tag != null) {
                    // Preserve tag discovery order for writes and default composite cursors.
                    TSDBColumnMetadata column = new TSDBColumnMetadata(field,
                            defaultIfBlank(tag.value(), field.getName()), TSDBColumnRoleEnum.TAG);
                    registerPhysicalColumn(entityType, columnsByPhysicalName, column);
                    tagColumns.add(column);
                } else if (value != null) {
                    // Prefer the annotated column name, falling back to the Java field name.
                    TSDBColumnMetadata column = new TSDBColumnMetadata(field,
                            defaultIfBlank(value.value(), field.getName()), TSDBColumnRoleEnum.FIELD);
                    registerPhysicalColumn(entityType, columnsByPhysicalName, column);
                    fieldColumns.add(column);
                } else {
                    // Fields without TSDB role annotations do not participate in time-series mapping.
                }
            }
            // Write models require one time column and at least one field column; tags are optional.
            if (timeColumn == null) {
                throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                        entityType.getName() + " must declare @TGTime");
            }
            if (fieldColumns.isEmpty()) {
                throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                        entityType.getName() + " must declare at least one @TGField");
            }
            // Cache the metadata in ClassValue only after all validation succeeds.
            return new TSDBEntityMetadata(entityType, measurement.value().trim(), timeColumn, tagColumns,
                    fieldColumns);
        }
    };

    /**
     * Resolves the TSDB annotation metadata for an entity class.
     *
     * @param entityType annotated business class, for example {@code AccruePoint.class}
     * @return resolved entity metadata
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public TSDBEntityMetadata resolve(Class<?> entityType) {
        if (entityType == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "recordType must not be null");
        }
        return cache.get(entityType);
    }

    /**
     * Reads the time, tags, and fields to construct the common write model.
     *
     * @param source annotated business object, for example {@code new AccruePoint(...)}
     * @return a common write record accepted by adapters
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public TSDBRecord toRecord(Object source) {
        if (source == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "record must not be null");
        }
        // Reuse validated metadata to locate the table and determine how to read each column.
        TSDBEntityMetadata metadata = resolve(source.getClass());
        // The time field is already restricted to long/Long; its runtime value must also be non-null.
        Object rawTimestamp = metadata.timeColumn().read(source);
        if (rawTimestamp == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "@TGTime value must not be null: "
                    + source.getClass().getName() + "." + metadata.timeColumn().getField().getName());
        }
        Long timestamp = (Long) rawTimestamp;

        // Index tags by physical column name, stringify non-null values, and omit nulls.
        Map<String, String> tags = new LinkedHashMap<>();
        for (TSDBColumnMetadata column : metadata.tagColumns()) {
            Object value = column.read(source);
            if (value != null) {
                tags.put(column.getColumnName(), String.valueOf(value));
            }
        }

        // Preserve Java field types for backend conversion and omit null values.
        Map<String, Object> fields = new LinkedHashMap<>();
        for (TSDBColumnMetadata column : metadata.fieldColumns()) {
            Object value = column.read(source);
            if (value != null) {
                fields.put(column.getColumnName(), value);
            }
        }
        if (fields.isEmpty()) {
            // Reject records with no non-null data fields instead of writing an empty measurement.
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR,
                    "At least one @TGField value must be non-null: "
                    + source.getClass().getName());
        }
        // Delegate transmission of the common write record to the selected backend.
        return new TSDBRecord(metadata.measurement(), timestamp, tags, fields);
    }

    /**
     * <h4>Maps a query result row to the requested Java object</h4>
     *
     * <p>Converts one map returned by a time-series query into a Java object using reflection.</p>
     *
     * <ul>
     *     <li>The result type requires a no-argument constructor, so ordinary Java records are generally unsuitable.</li>
     * </ul>
     *
     * <p>Annotated fields match only their physical column names, case-insensitively, consistent with write mapping.
     * Unannotated DTOs also support field names, snake_case, and common time aliases. A NULL never falls back to another column.</p>
     *
     * <p>Invalid text, integral overflow, fractional-to-integral conversion, invalid booleans, and floating-point
     * overflow or underflow to zero fail with {@code METADATA_ERROR} and field context. Floating-point targets
     * retain ordinary IEEE 754 rounding. Missing columns leave defaults unchanged; explicit null sets reference
     * fields to null and leaves primitive defaults unchanged. Time conversion to long or Date uses milliseconds.</p>
     *
     * @param entityType result class, for example {@code ValueOnlyResult.class}
     * @param row        one adapter result row, for example {@code Map.of("tag_name", "device001", "value", 12.34D)}
     * @param <T>        result object type
     * @return the mapped result object
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    @Override
    public <T> T toEntity(Class<T> entityType,
                          Map<String, Object> row) {
        if (entityType == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "entityType must not be null");
        }
        if (row == null) {
            throw new TSDBException(TSDBErrorCodeEnum.ARGUMENT_ERROR, "row must not be null");
        }
        T entity = instantiate(entityType);
        Map<String, Object> indexedRow = indexRow(row);
        for (Field field : fieldsOf(entityType)) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            Object value = findFieldValue(row, indexedRow, field);
            if (value != UNMAPPED && (value != null || !field.getType().isPrimitive())) {
                writeField(entity, field, value);
            }
        }
        return entity;
    }

    /**
     * Registers physical column names and rejects case-insensitive duplicates during metadata resolution.
     * <p>Checks all time, tag, and field roles to prevent later map conversion from silently overwriting columns.</p>
     *
     * @param entityType            current entity class
     * @param columnsByPhysicalName registered physical columns
     * @param column                column metadata to register
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-20
     */
    private static void registerPhysicalColumn(Class<?> entityType,
                                               Map<String, TSDBColumnMetadata> columnsByPhysicalName,
                                               TSDBColumnMetadata column) {
        String normalizedName = column.getColumnName().toLowerCase(Locale.ROOT);
        TSDBColumnMetadata existing = columnsByPhysicalName.putIfAbsent(normalizedName, column);
        if (existing != null) {
            throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                    "Duplicate TSDB physical column '" + column.getColumnName() + "' in "
                    + entityType.getName() + ": "
                    + existing.getField().getName() + " (" + existing.getRole() + ") and "
                    + column.getField().getName() + " (" + column.getRole() + ")");
        }
    }

    /**
     * Collects declared and inherited fields so business models can reuse TSDB columns through inheritance.
     *
     * @param type class to inspect, for example {@code AccruePoint.class}
     * @return fields declared by the class and its ancestors
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static List<Field> fieldsOf(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = type;
        while (current != null && current != Object.class) {
            Collections.addAll(fields, current.getDeclaredFields());
            current = current.getSuperclass();
        }
        return fields;
    }

    /**
     * Builds a loose result-row index for exact names and snake_case to camelCase matching.
     *
     * @param row one adapter result row, for example {@code Map.of("tag_name", "device001")}
     * @return row values indexed by original, lowercase, and normalized column names
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static Map<String, Object> indexRow(Map<String, Object> row) {
        Map<String, Object> indexed = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            for (String key : List.of(entry.getKey(), entry.getKey().toLowerCase(Locale.ROOT),
                    normalizeResultName(entry.getKey()))) {
                indexed.computeIfAbsent(key, ignored -> entry.getValue() == null ? NULL_VALUE : entry.getValue());
            }
        }
        return indexed;
    }

    /**
     * Matches annotated fields by physical column only; unannotated fields also use Java names and common aliases.
     *
     * @param row        original adapter result row, for example {@code Map.of("tag_name", "device001")}
     * @param indexedRow loose row index, for example containing {@code tagname -> device001}
     * @param field      Java field to map, for example {@code ValueResult.tagName}
     * @return the matched value, possibly null, or UNMAPPED when no column matches
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static Object findFieldValue(Map<String, Object> row,
                                         Map<String, Object> indexedRow,
                                         Field field) {
        List<String> candidates = new ArrayList<>();
        addAnnotationColumnCandidates(field, candidates);
        // Annotated fields use only physical columns so unrelated Java-name matches cannot overwrite their values.
        if (!candidates.isEmpty()) {
            for (String column : candidates) {
                // Prefer exact matches and retain explicit nulls when the column exists.
                if (row.containsKey(column)) {
                    return row.get(column);
                }
                // Physical columns allow case differences but no camelCase or other loose transformations.
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    if (column.equalsIgnoreCase(entry.getKey())) {
                        return entry.getValue();
                    }
                }
            }
            // Missing physical columns remain unmapped, without falling back to Java names or aliases.
            return UNMAPPED;
        }
        // Field names, snake_case names, and common time aliases apply only to unannotated DTO fields.
        candidates.add(field.getName());
        candidates.add(toSnakeCase(field.getName()));
        if (isTimeField(field)) {
            candidates.add("time");
            candidates.add("Time");
            candidates.add("_time");
            candidates.add("timestamp");
        }
        for (String candidate : candidates) {
            Object value = findCandidateValue(row, indexedRow, candidate);
            if (value != UNMAPPED) {
                return value;
            }
        }
        return UNMAPPED;
    }

    /**
     * Reads TSDB annotation column names, allowing write POJOs to receive query results as well.
     *
     * @param field      Java field to inspect, for example {@code AccruePoint.deviceCode}
     * @param candidates candidate column names, for example an existing {@code ["deviceCode", "device_code"]}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void addAnnotationColumnCandidates(Field field,
                                                      List<String> candidates) {
        TGTime time = field.getAnnotation(TGTime.class);
        TGTag tag = field.getAnnotation(TGTag.class);
        TGField value = field.getAnnotation(TGField.class);
        if (time != null) {
            candidates.add(defaultIfBlank(time.value(), "time"));
        }
        if (tag != null) {
            candidates.add(defaultIfBlank(tag.value(), field.getName()));
        }
        if (value != null) {
            candidates.add(defaultIfBlank(value.value(), field.getName()));
        }
    }

    /**
     * Determines whether a field can receive a query result's time column.
     *
     * @param field field to inspect, for example {@code AccruePoint.time}
     * @return whether the field represents time
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static boolean isTimeField(Field field) {
        String name = field.getName();
        return field.getAnnotation(TGTime.class) != null
                || "time".equalsIgnoreCase(name)
                || "timestamp".equalsIgnoreCase(name);
    }

    /**
     * <h3>Three-stage matching for one candidate column name</h3>
     *
     * <p>Looks up original names, case-insensitive names, and normalized names in that order.</p>
     *
     * @param row        original adapter result row, for example {@code Map.of("tag_name", "device001")}
     * @param indexedRow loose row index, for example containing {@code tagname -> device001}
     * @param candidate  current candidate name, for example {@code "tagName"}
     * @return the matched value, possibly null, or UNMAPPED when no column matches
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static Object findCandidateValue(Map<String, Object> row,
                                             Map<String, Object> indexedRow,
                                             String candidate) {
        if (candidate == null) {
            return UNMAPPED;
        }
        // Match the exact name first.
        if (row.containsKey(candidate)) {
            return row.get(candidate);
        }
        // Locale.ROOT makes case conversion independent of the server locale, including Turkish I rules.
        String lowerName = candidate.toLowerCase(Locale.ROOT);
        // Match without considering case.
        Object value = indexedRow.get(lowerName);
        if (value == null) {
            // Ignore differences in naming convention.
            value = indexedRow.getOrDefault(normalizeResultName(candidate), UNMAPPED);
        }
        // Restore the null sentinel to an actual null; missing columns remain UNMAPPED.
        return value == NULL_VALUE ? null : value;
    }

    /**
     * Converts a query result value to the Java field type and writes it to the result object.
     *
     * @param target result object, for example {@code new ValueOnlyResult()}
     * @param field  field to populate, for example {@code ValueOnlyResult.value}
     * @param value  raw adapter value, for example {@code 12.34D}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static void writeField(Object target, Field field, Object value) {
        try {
            if (!field.canAccess(target)) {
                field.setAccessible(true);
            }
            field.set(target, convertValue(value, field.getType()));
        } catch (IllegalAccessException | RuntimeException e) {
            throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                    "Failed to map query result field " + target.getClass().getName() + "." + field.getName()
                    + " from " + (value == null ? "null" : value.getClass().getName())
                    + " to " + field.getType().getName(), e);
        }
    }

    /**
     * Converts a Java field name to a snake_case database column candidate.
     *
     * @param value Java field name, for example {@code "deviceCode"}
     * @return snake_case column name, for example {@code "device_code"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String toSnakeCase(String value) {
        if (isBlank(value)) {
            return value;
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (Character.isUpperCase(ch) && i > 0) {
                result.append('_');
            }
            result.append(Character.toLowerCase(ch));
        }
        return result.toString();
    }

    /**
     * Normalizes result names by ignoring underscores, hyphens, whitespace, and case for camelCase matching.
     *
     * @param value original column name, for example {@code "tag_name"}
     * @return normalized column name, for example {@code "tagname"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String normalizeResultName(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '_' || ch == '-' || Character.isWhitespace(ch)) {
                continue;
            }
            result.append(Character.toLowerCase(ch));
        }
        return result.toString();
    }

    /**
     * Converts common numeric and temporal values, rejecting invalid or lossy integral conversions.
     *
     * @param value      raw adapter value, for example {@code "12.34"}
     * @param targetType target Java type, for example {@code Double.class}
     * @return the converted value; invalid values raise an exception wrapped with field context by the caller
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static Object convertValue(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }
        if (targetType == Long.class || targetType == long.class) {
            return toEpochMillis(value);
        }
        if (targetType == Integer.class || targetType == int.class) {
            return toDecimal(value).intValueExact();
        }
        if (targetType == Short.class || targetType == short.class) {
            return toDecimal(value).shortValueExact();
        }
        if (targetType == Byte.class || targetType == byte.class) {
            return toDecimal(value).byteValueExact();
        }
        if (targetType == BigInteger.class) {
            return toDecimal(value).toBigIntegerExact();
        }
        if (targetType == BigDecimal.class) {
            return toDecimal(value);
        }
        if (targetType == Double.class || targetType == double.class) {
            double result = value instanceof Number number
                    ? number.doubleValue() : Double.parseDouble(String.valueOf(value).trim());
            if (!Double.isFinite(result) || (result == 0.0d && toDecimal(value).signum() != 0)) {
                throw new IllegalArgumentException("Value is outside the finite double range");
            }
            return result;
        }
        if (targetType == Float.class || targetType == float.class) {
            float result = value instanceof Number number
                    ? number.floatValue() : Float.parseFloat(String.valueOf(value).trim());
            if (!Float.isFinite(result) || (result == 0.0f && toDecimal(value).signum() != 0)) {
                throw new IllegalArgumentException("Value is outside the finite float range");
            }
            return result;
        }
        if (targetType == Boolean.class || targetType == boolean.class) {
            if (value instanceof Boolean) {
                return value;
            }
            String text = String.valueOf(value).trim();
            if ("true".equalsIgnoreCase(text)) {
                return true;
            }
            if ("false".equalsIgnoreCase(text)) {
                return false;
            }
            throw new IllegalArgumentException("Boolean values must be true or false");
        }
        if (targetType.isInstance(value) || (targetType == char.class && value instanceof Character)) {
            return value;
        }
        if (targetType == String.class) {
            return String.valueOf(value);
        }
        if (targetType == Instant.class) {
            if (value instanceof CharSequence text) {
                try {
                    return Instant.parse(text.toString().trim());
                } catch (DateTimeParseException ignored) {
                    // Numeric text is interpreted as Unix epoch milliseconds below.
                }
            }
            return Instant.ofEpochMilli(toEpochMillis(value));
        }
        if (targetType == Date.class) {
            return new Date(toEpochMillis(value));
        }
        throw new IllegalArgumentException("Unsupported result conversion to " + targetType.getName());
    }

    /**
     * Parses a decimal representation without truncating integral values or accepting non-finite numbers.
     * Floating-point targets retain their ordinary IEEE 754 rounding behavior.
     *
     * @param value numeric source value or numeric text
     * @return the decimal value used for exact range and fractional-part checks
     */
    private static BigDecimal toDecimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof BigInteger integer) {
            return new BigDecimal(integer);
        }
        if (value instanceof Double || value instanceof Float) {
            // A shortest round-trip string can differ from the exact integer represented by a large float.
            // Widening Float to double preserves its binary value; BigDecimal then captures that value exactly.
            return new BigDecimal(((Number) value).doubleValue());
        }
        if (value instanceof Number || value instanceof CharSequence) {
            return new BigDecimal(value.toString().trim());
        }
        throw new IllegalArgumentException("Expected a number or numeric text");
    }

    /**
     * Converts supported temporal representations to Unix epoch milliseconds without numeric truncation.
     *
     * @param value time value, for example {@code Instant.parse("2026-07-03T00:00:00Z")} or {@code 1783000000000L}
     * @return epoch milliseconds; invalid, fractional, or out-of-range numeric values raise an exception
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static long toEpochMillis(Object value) {
        if (value instanceof Instant instant) {
            return instant.toEpochMilli();
        }
        if (value instanceof Date date) {
            return date.getTime();
        }
        if (value instanceof CharSequence text) {
            String input = text.toString().trim();
            try {
                return new BigDecimal(input).longValueExact();
            } catch (NumberFormatException invalidNumber) {
                return Instant.parse(input).toEpochMilli();
            }
        }
        return toDecimal(value).longValueExact();
    }

    /**
     * Creates a result object through its no-argument constructor.
     *
     * @param entityType result class, for example {@code ValueOnlyResult.class}
     * @param <T>        result object type
     * @return a newly constructed result object
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static <T> T instantiate(Class<T> entityType) {
        try {
            Constructor<T> constructor = entityType.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                    entityType.getName() + " must have a no-args constructor", e);
        }
    }

    /**
     * Returns the fallback for blank strings, or the trimmed original value otherwise.
     *
     * @param value    original string, for example {@code " device_code "}
     * @param fallback fallback string, for example {@code "deviceCode"}
     * @return a nonblank string
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static String defaultIfBlank(String value, String fallback) {
        return isBlank(value) ? fallback : value.trim();
    }

    /**
     * Determines whether a string is null or blank.
     *
     * @param value string to inspect, for example {@code "device_code"}
     * @return whether the string is null or blank
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
