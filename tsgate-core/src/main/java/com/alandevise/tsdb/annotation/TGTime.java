package com.alandevise.tsdb.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the timestamp field of an annotated POJO.
 * <p>The field must be {@link Long} or {@code long}; its value is written directly as Unix epoch milliseconds.
 * Local date-time types are not accepted, preventing implicit time-zone conversions from shifting timestamps.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TGTime {

    /**
     * Physical time column name; defaults to {@code time}, as exposed by most TSDBs.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    String value() default "time";
}
