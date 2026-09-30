package com.alandevise.tsdb.metadata;

import com.alandevise.tsdb.model.TSDBRecord;

import java.util.Map;

/**
 * <h2>Annotated POJO metadata resolver</h2>
 *
 * <p>Caches metadata for write models, converts business objects to common records, and maps query rows to result objects.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public interface TSDBMetadataResolver {

    /**
     * Resolves and caches the TSDB annotation metadata of a business class.
     *
     * @param entityType annotated business class, for example {@code AccruePoint.class}
     * @return resolved entity metadata
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    TSDBEntityMetadata resolve(Class<?> entityType);

    /**
     * Converts an annotated business object to a common write record accepted by adapters.
     *
     * @param source annotated business object, for example {@code new AccruePoint(...)}
     * @return a common write record accepted by adapters
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    TSDBRecord toRecord(Object source);

    /**
     * Maps one adapter result row to the requested result object.
     * <p>Result classes need not declare TSDB annotations. Annotated fields use physical column names;
     * unannotated fields also support Java names, snake_case, and common time aliases.</p>
     *
     * @param entityType result class, for example {@code ValueOnlyResult.class}
     * @param row        one adapter result row, for example {@code Map.of("value", 12.34D)}
     * @param <T>        result object type
     * @return the mapped result object
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    <T> T toEntity(Class<T> entityType,
                   Map<String, Object> row);
}
