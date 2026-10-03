package com.alandevise.tsgate.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Snapshot of the known outcome of a batch write.
 *
 * @param requestedRecords  requested record count
 * @param validatedRecords  records validated before transmission
 * @param committedRecords  minimum number of records confirmed committed
 * @param totalBatches      physical batches created for this request
 * @param committedBatches  batches confirmed committed
 * @param failedBatchIndex  zero-based failed batch index, or {@code null} on success
 * @param failedMeasurement measurement of the failed batch, or {@code null} when unknown
 * @param commitState       commit state
 * @param retryable         whether callers can retry using an idempotent strategy
 * @param message           outcome description
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-11
 */
public record BatchWriteResult(int requestedRecords,
                               int validatedRecords,
                               int committedRecords,
                               int totalBatches,
                               int committedBatches,
                               Integer failedBatchIndex,
                               String failedMeasurement,
                               BatchCommitStateEnum commitState,
                               boolean retryable,
                               String message) implements Serializable {

    /**
     * Serialization version identifier.
     */
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates a successful result for an empty batch request.
     *
     * @return a successful result with no records or physical batches
     */
    public static BatchWriteResult emptySuccess() {
        return success(0, 0);
    }

    /**
     * Creates a successful result with all records confirmed committed.
     *
     * @param recordCount committed record count
     * @param batchCount  committed physical batch count
     * @return a successful result
     */
    public static BatchWriteResult success(int recordCount, int batchCount) {
        return new BatchWriteResult(recordCount, recordCount, recordCount,
                batchCount, batchCount, null, null, BatchCommitStateEnum.SUCCESS,
                false, "Batch write completed successfully");
    }

    /**
     * Determines whether all requested records are confirmed committed.
     *
     * @return {@code true} when the commit state is {@link BatchCommitStateEnum#SUCCESS}
     */
    public boolean isSuccess() {
        return commitState == BatchCommitStateEnum.SUCCESS;
    }
}
