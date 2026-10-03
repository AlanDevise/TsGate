package com.alandevise.tsgate.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * <h2>Common TSDB adapter error codes</h2>
 * <p>Codes use the six-digit {@code 1003xx} range; {@code 100300-100398} identify stable failure categories,
 * while {@code 100399} is reserved for unclassified internal errors. These are not HTTP status codes;
 * applications can map them in their global exception handlers.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-21
 */
@Getter
@AllArgsConstructor
public enum TSDBErrorCodeEnum {

    /**
     * Missing required settings, malformed configuration, or invalid connection-pool options.
     */
    CONFIGURATION_ERROR(100300, "TSDB configuration error"),

    /**
     * Invalid SQL, pagination, filters, identifiers, or write arguments supplied by the caller.
     */
    ARGUMENT_ERROR(100301, "Invalid request arguments"),

    /**
     * Invalid entity annotations, physical columns, Java conversions, or result mappings.
     */
    METADATA_ERROR(100302, "TSDB metadata or mapping error"),

    /**
     * An adapter or client that is uninitialized, closed, or otherwise unavailable.
     */
    ADAPTER_STATE_ERROR(100303, "Invalid TSDB adapter state"),

    /**
     * Network interruption, connection timeout, unreachable service, or temporary service unavailability.
     */
    CONNECTION_ERROR(100304, "TSDB connection error"),

    /**
     * Authentication failure, invalid access token, or insufficient user permissions.
     */
    PERMISSION_ERROR(100305, "TSDB authentication or permission error"),

    /**
     * A missing database, table, column, or other backend resource.
     */
    RESOURCE_NOT_FOUND(100306, "TSDB resource not found"),

    /**
     * A server-rejected write whose current batch commit boundary is known.
     */
    WRITE_ERROR(100307, "TSDB write error"),

    /**
     * Invalid query syntax, failed query execution, or an unreadable query response.
     */
    QUERY_ERROR(100308, "TSDB query error"),

    /**
     * A failure after batch network I/O begins, leaving the current batch commit status uncertain.
     */
    BATCH_COMMIT_UNKNOWN(100309, "Batch commit state is unknown"),

    /**
     * A common API operation or argument combination unsupported by the selected backend.
     */
    UNSUPPORTED_OPERATION(100310, "Unsupported TSDB operation"),

    /**
     * An unclassified adapter failure or fallback for legacy exception constructors.
     */
    INTERNAL_ERROR(100399, "Internal TSDB adapter error");

    /**
     * Six-digit application error code.
     */
    private final int code;

    /**
     * Default error message.
     */
    private final String message;
}
