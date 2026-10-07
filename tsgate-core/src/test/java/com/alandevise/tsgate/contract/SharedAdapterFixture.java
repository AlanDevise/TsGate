package com.alandevise.tsgate.contract;

import com.alandevise.tsgate.adapter.TSDBAdapter;

/** Backend-specific transport seam used only by inherited adapter contract tests. */
public interface SharedAdapterFixture extends AutoCloseable {
    int ORIGINAL_MAX_BATCH_RECORDS = 6_000;
    int ORIGINAL_MAX_QUERY_ROWS = 2;
    String ORIGINAL_DATABASE = "contract";

    TSDBAdapter adapter();
    void initialize();
    Object nativeResource();
    void mutateOriginalConfiguration();
    int ioCount();
    String lastQuerySql();
    String lastDatabase();
    void enqueueRows(long... timestamps);
    void enqueueCount(int total);
    void enqueueWriteSuccess();
    void enqueueUnknownWriteFailure();
    int physicalBatchSize();

    /** Actual captured adapter settings used to verify declared configuration prerequisites. */
    default java.util.Map<String, String> configurationValues() { return java.util.Map.of(); }

    @Override
    void close();
}
