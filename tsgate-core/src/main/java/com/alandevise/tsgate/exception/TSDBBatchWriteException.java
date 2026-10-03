package com.alandevise.tsgate.exception;

import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.BatchWriteResult;
import lombok.Getter;

/**
 * Batch-write failure carrying detailed results that identify the confirmed commit boundary.
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-11
 */
@Getter
public class TSDBBatchWriteException extends TSDBException {

    /**
     * Batch commit boundary confirmed when the failure occurred.
     */
    private final BatchWriteResult result;

    /**
     * Creates an exception from a detailed result and an underlying cause.
     *
     * @param result batch-write result snapshot
     * @param cause  underlying failure
     */
    public TSDBBatchWriteException(BatchWriteResult result,
                                   Throwable cause) {
        this(resolveBatchErrorCode(result, cause), result, cause);
    }

    /**
     * Creates a batch-write exception with an explicit error code, commit boundary, and underlying cause.
     *
     * @param errorCode batch-write failure category
     * @param result    batch commit boundary
     * @param cause     underlying failure
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    public TSDBBatchWriteException(TSDBErrorCodeEnum errorCode,
                                   BatchWriteResult result,
                                   Throwable cause) {
        super(errorCode, result == null ? "Batch write failed" : result.message(), cause);
        this.result = result;
    }

    /**
     * Classifies a batch failure, prioritizing unknown commit state and existing TSDB error codes in the cause chain.
     *
     * @param result batch-write result snapshot
     * @param cause  underlying failure
     * @return the public batch-write error code
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private static TSDBErrorCodeEnum resolveBatchErrorCode(BatchWriteResult result,
                                                       Throwable cause) {
        if (result != null && result.commitState() == BatchCommitStateEnum.UNKNOWN) {
            return TSDBErrorCodeEnum.BATCH_COMMIT_UNKNOWN;
        }
        return TSDBException.resolveErrorCode(cause, TSDBErrorCodeEnum.WRITE_ERROR);
    }

}
