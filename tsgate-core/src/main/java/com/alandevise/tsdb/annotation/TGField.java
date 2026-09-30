package com.alandevise.tsdb.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a field or metric value in an annotated POJO.
 * <p>Maps to a regular field column in the table model or a field in InfluxDB line protocol.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TGField {

    /**
     * Physical field column name; defaults to the Java field name when blank.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    String value() default "";
}
