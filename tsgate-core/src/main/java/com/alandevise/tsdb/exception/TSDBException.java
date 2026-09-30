package com.alandevise.tsdb.exception;

import lombok.Getter;

/**
 * Common TSDB adapter exception.
 * <p>Argument, configuration, metadata, connection, and database-execution failures use this type or a subclass,
 * preserving the original exception as the cause. Applications should prefer {@link #getErrorCode()} or {@link #getCode()}
 * for stable classification instead of messages that may change with underlying client versions.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-06
 */
@Getter
public class TSDBException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Stable adapter error category for application handling.
     */
    private final TSDBErrorCodeEnum errorCode;

    /**
     * Creates an adapter exception with a message.
     *
     * @param message error message, for example {@code "measurement must not be empty"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-06
     */
    public TSDBException(String message) {
        this(TSDBErrorCodeEnum.INTERNAL_ERROR, message);
    }

    /**
     * Creates an adapter exception with a message and original cause.
     *
     * @param message error message, for example {@code "Invalid query filter"}
     * @param cause   original cause, for example {@code NumberFormatException}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-06
     */
    public TSDBException(String message, Throwable cause) {
        this(TSDBErrorCodeEnum.INTERNAL_ERROR, message, cause);
    }

    /**
     * Creates an adapter exception with an error code and contextual message.
     *
     * @param errorCode stable error code
     * @param message   context for this failure
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    public TSDBException(TSDBErrorCodeEnum errorCode,
                         String message) {
        super(message);
        this.errorCode = normalize(errorCode);
    }

    /**
     * Creates an adapter exception with an error code, contextual message, and original cause.
     *
     * @param errorCode stable error code
     * @param message   context for this failure
     * @param cause     original underlying exception
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    public TSDBException(TSDBErrorCodeEnum errorCode,
                         String message,
                         Throwable cause) {
        super(message, cause);
        this.errorCode = normalize(errorCode);
    }

    /**
     * Returns the six-digit error code for use in global exception-handler responses.
     *
     * @return six-digit application error code
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    public int getCode() {
        return errorCode.getCode();
    }

    /**
     * Finds an existing TSDB error code in the cause chain, or returns the caller's fallback category.
     *
     * @param cause    original exception chain
     * @param fallback category used when no TSDB exception is found
     * @return the existing or fallback error code
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    public static TSDBErrorCodeEnum resolveErrorCode(Throwable cause,
                                                     TSDBErrorCodeEnum fallback) {
        Throwable current = cause;
        while (current != null) {
            if (current instanceof TSDBException tsdbException) {
                return tsdbException.getErrorCode();
            }
            current = current.getCause();
        }
        return normalize(fallback);
    }

    /**
     * Normalizes an error category to prevent exceptions from carrying a null code.
     *
     * @param errorCode error code to normalize
     * @return the original code, or {@link TSDBErrorCodeEnum#INTERNAL_ERROR} when it is {@code null}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private static TSDBErrorCodeEnum normalize(TSDBErrorCodeEnum errorCode) {
        return errorCode == null ? TSDBErrorCodeEnum.INTERNAL_ERROR : errorCode;
    }
}
