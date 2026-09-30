package com.alandevise.tsdb.metadata;

import com.alandevise.tsdb.exception.TSDBException;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;

import java.lang.reflect.Field;

/**
 * Metadata for one annotated field.
 * <p>Stores the Java field, physical column name, role, and Java type, and encapsulates reflective reads and writes.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public class TSDBColumnMetadata {

    private final Field field;
    private final String columnName;
    private final TSDBColumnRoleEnum role;
    private final Class<?> javaType;

    /**
     * Creates column metadata and enables reflective access.
     * <p>setAccessible affects only the current Field instance and is repeatable local initialization;
     * it does not modify business objects, the database, or resolver cache state.</p>
     *
     * @param field      reflected Java field, for example {@code AccruePoint.class.getDeclaredField("deviceCode")}
     * @param columnName physical TSDB column name, for example {@code "device_code"}
     * @param role       column role, for example {@code TSDBColumnRoleEnum.TAG}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TSDBColumnMetadata(Field field,
                              String columnName,
                              TSDBColumnRoleEnum role) {
        this.field = field;
        this.field.setAccessible(true);
        this.columnName = columnName;
        this.role = role;
        this.javaType = field.getType();
    }

    /**
     * Returns the reflected Java field.
     *
     * @return reflected Java field, for example {@code AccruePoint.deviceCode}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public Field getField() {
        return field;
    }

    /**
     * Returns the physical TSDB column name.
     *
     * @return physical TSDB column name, for example {@code "device_code"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Returns the column role.
     *
     * @return column role, for example {@code TSDBColumnRoleEnum.TAG}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public TSDBColumnRoleEnum getRole() {
        return role;
    }

    /**
     * Returns the Java field type.
     *
     * @return Java field type, for example {@code String.class}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public Class<?> getJavaType() {
        return javaType;
    }

    /**
     * Reads this field's value from a business object.
     *
     * @param source business object instance, for example {@code new AccruePoint(...)}
     * @return field value, for example {@code "device001"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public Object read(Object source) {
        try {
            return field.get(source);
        } catch (IllegalAccessException e) {
            throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                    "Failed to read TSDB field: " + field.getName(), e);
        }
    }

    /**
     * Writes a query result value to the business object's field.
     *
     * @param target result object, for example {@code new ValueOnlyResult()}
     * @param value  field value to write, for example {@code 12.34D}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public void write(Object target,
                      Object value) {
        try {
            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw new TSDBException(TSDBErrorCodeEnum.METADATA_ERROR,
                    "Failed to write TSDB field: " + field.getName(), e);
        }
    }
}
