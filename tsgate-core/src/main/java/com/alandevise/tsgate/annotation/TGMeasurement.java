package com.alandevise.tsgate.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the logical TSDB table name for an annotated POJO.
 * <p>Used as the measurement in InfluxDB and as the table in the IoTDB table model.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TGMeasurement {

    /**
     * Physical measurement or table name.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    String value();
}
