package com.alandevise.tsgate.autoconfigure;

import com.alandevise.tsgate.adapter.impl.OpenGeminiAdapter;
import com.alandevise.tsgate.config.OpenGeminiProperties;
import com.alandevise.tsgate.exception.TSDBBatchWriteException;
import com.alandevise.tsgate.exception.TSDBErrorCodeEnum;
import com.alandevise.tsgate.exception.TSDBException;
import com.alandevise.tsgate.model.BatchCommitStateEnum;
import com.alandevise.tsgate.model.TSDBRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Ensures the starter binds the whole-batch byte budget and enforces it before network I/O. */
class OpenGeminiBatchBytesBindingTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    OpenGeminiTSDBAutoConfiguration.class))
            .withPropertyValues("tsdb.opengemini.enable=true", "tsdb.opengemini.url=http://127.0.0.1:1");

    @Test
    void defaultBudgetBindsAs64MiB() {
        runner.run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(OpenGeminiAdapter.class);
            assertThat(context.getBean(OpenGeminiProperties.class).getMaxBatchBytes()).isEqualTo(64L * 1024 * 1024);
        });
    }

    @Test
    void bindingRetainsValuesAboveTheIntegerRange() {
        runner.withPropertyValues("tsdb.opengemini.max-batch-bytes=5000000000").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(OpenGeminiProperties.class).getMaxBatchBytes()).isEqualTo(5_000_000_000L);
        });
    }

    @Test
    void boundBudgetReachesTheAdapterAndRejectsBeforeConnecting() {
        runner.withPropertyValues("tsdb.opengemini.max-batch-bytes=1").run(context -> {
            assertThat(context).hasNotFailed();
            TSDBRecord point = new TSDBRecord("points", 1L, Map.of(), Map.of("v", 1));
            TSDBBatchWriteException error = assertThrows(TSDBBatchWriteException.class,
                    () -> context.getBean(OpenGeminiAdapter.class).batchWriteDetailed(null, List.of(point)));
            assertThat(error.getErrorCode()).isEqualTo(TSDBErrorCodeEnum.ARGUMENT_ERROR);
            assertThat(error.getResult().commitState()).isEqualTo(BatchCommitStateEnum.NOT_COMMITTED);
            assertThat(error.getResult().committedRecords()).isZero();
            assertThat(error.getResult().totalBatches()).isZero();
        });
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1})
    void invalidBudgetFailsStartupEvenWithFailFastDisabled(long bytes) {
        runner.withPropertyValues("tsdb.opengemini.max-batch-bytes=" + bytes, "tsdb.opengemini.fail-fast=false")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(TSDBException.class)
                            .hasStackTraceContaining("tsdb.opengemini.max-batch-bytes");
                });
    }
}
