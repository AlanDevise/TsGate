package com.alandevise.tsgate.benchmark;

import com.alandevise.tsgate.annotation.TGField;
import com.alandevise.tsgate.annotation.TGTag;
import com.alandevise.tsgate.annotation.TGTime;
import com.alandevise.tsgate.metadata.DefaultTSDBMetadataResolver;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

/**
 * Standalone, dependency-free comparison harness for the existing resolver API; this is not a JMH benchmark.
 * Compile this exact source once against the baseline core classes with javac --release 17, then run that
 * harness with either baseline or changed core classes on the classpath, in separate fresh JVMs.
 * Example arguments: --label baseline-7c7cf761 --warmup 4 --iterations 7 --passes 2 --rows 1000,10000 --threads 1,4.
 * Keep the JDK, JVM options, arguments and machine load equal, and repeat forks in alternating version order.
 * JSON-lines report wall time and, when the JVM supports it, summed worker-thread allocation bytes per mapped row.
 * Timings include thread coordination but exclude row-fixture creation, classpath setup and warmup iterations.
 * These local measurements neither establish end-to-end database performance nor impose a test pass threshold.
 */
public final class ResultMappingBenchmark {
    private static volatile long checksumSink;

    private ResultMappingBenchmark() {
    }

    public static void main(String[] arguments) throws Exception {
        Options options = Options.parse(arguments);
        com.sun.management.ThreadMXBean allocationBean = allocationBean();
        System.out.printf(Locale.ROOT,
                "{\"kind\":\"environment\",\"label\":%s,\"java\":%s,\"vm\":%s,\"arch\":%s,"
                        + "\"warmup\":%d,\"iterations\":%d,\"passes\":%d,\"allocation_supported\":%s}%n",
                json(options.label()), json(System.getProperty("java.version")),
                json(System.getProperty("java.vm.name")), json(System.getProperty("os.arch")),
                options.warmup(), options.iterations(), options.passes(), allocationBean != null);
        for (int rowCount : options.rows()) {
            Scenario<FewFieldsDto> few = new Scenario<>("few-plain-inherited", FewFieldsDto.class,
                    plainRows(rowCount), dto -> dto.timestamp + dto.status + dto.deviceCode.length()
                            + Double.doubleToLongBits(dto.readingValue));
            Scenario<ManyFieldsDto> many = new Scenario<>("many-annotated-inherited", ManyFieldsDto.class,
                    annotatedRows(rowCount), dto -> dto.time + dto.count04 + dto.device.length()
                            + Double.doubleToLongBits(dto.value12) + (dto.enabled ? 1 : 0));
            for (int threads : options.threads()) {
                measure(few, threads, options, allocationBean);
                measure(many, threads, options, allocationBean);
            }
        }
        // Keep observable work alive without retaining a result class, row, or resolver globally.
        if (checksumSink == Long.MIN_VALUE) System.err.println("checksum=" + checksumSink);
    }

    private static <T> void measure(Scenario<T> scenario, int threads, Options options,
                                     com.sun.management.ThreadMXBean allocationBean) throws Exception {
        DefaultTSDBMetadataResolver resolver = new DefaultTSDBMetadataResolver();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Worker<T>> workers = new ArrayList<>();
        for (int index = 0; index < threads; index++) workers.add(new Worker<>(resolver, scenario));
        try {
            for (int iteration = 0; iteration < options.warmup(); iteration++) {
                runIteration(executor, workers, options.passes(), allocationBean);
            }
            long[] nanos = new long[options.iterations()];
            long[] allocations = new long[options.iterations()];
            long checksum = 0L;
            for (int iteration = 0; iteration < options.iterations(); iteration++) {
                Measurement measurement = runIteration(executor, workers, options.passes(), allocationBean);
                nanos[iteration] = measurement.nanos();
                allocations[iteration] = measurement.allocatedBytes();
                checksum ^= measurement.checksum();
            }
            Arrays.sort(nanos);
            Arrays.sort(allocations);
            long totalRows = Math.multiplyExact(Math.multiplyExact((long) scenario.rows().size(), threads), options.passes());
            long medianNanos = nanos[nanos.length / 2];
            long medianAllocation = allocations[allocations.length / 2];
            System.out.printf(Locale.ROOT,
                    "{\"kind\":\"measurement\",\"label\":%s,\"scenario\":%s,\"rows_per_thread\":%d,"
                            + "\"threads\":%d,\"mapped_rows\":%d,\"min_nanos\":%d,\"median_nanos\":%d,"
                            + "\"max_nanos\":%d,\"median_ns_per_row\":%.3f,\"median_allocated_bytes_per_row\":%s,"
                            + "\"checksum\":%d}%n",
                    json(options.label()), json(scenario.name()), scenario.rows().size(), threads, totalRows,
                    nanos[0], medianNanos, nanos[nanos.length - 1], (double) medianNanos / totalRows,
                    medianAllocation < 0 ? "null" : String.format(Locale.ROOT, "%.3f", (double) medianAllocation / totalRows),
                    checksum);
        } finally {
            executor.shutdownNow();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Benchmark workers did not terminate");
            }
        }
    }

    private static <T> Measurement runIteration(ExecutorService executor, List<Worker<T>> workers, int passes,
                                                 com.sun.management.ThreadMXBean allocationBean) throws Exception {
        CountDownLatch ready = new CountDownLatch(workers.size());
        CountDownLatch start = new CountDownLatch(1);
        List<Future<WorkerResult>> futures = new ArrayList<>();
        for (Worker<T> worker : workers) {
            futures.add(executor.submit(() -> {
                ready.countDown();
                start.await();
                long before = allocatedBytes(allocationBean);
                long checksum = worker.mapRows(passes);
                long after = allocatedBytes(allocationBean);
                return new WorkerResult(before < 0 || after < before ? -1L : after - before, checksum);
            }));
        }
        if (!ready.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Benchmark workers did not start");
        long begin = System.nanoTime();
        start.countDown();
        long allocatedBytes = 0L;
        long checksum = 0L;
        for (Future<WorkerResult> future : futures) {
            WorkerResult result = future.get();
            if (result.allocatedBytes() < 0) allocatedBytes = -1L;
            else if (allocatedBytes >= 0) allocatedBytes += result.allocatedBytes();
            checksum += result.checksum();
        }
        long nanos = System.nanoTime() - begin;
        checksumSink = checksum;
        return new Measurement(nanos, allocatedBytes, checksum);
    }

    private static com.sun.management.ThreadMXBean allocationBean() {
        if (!(ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean bean)
                || !bean.isThreadAllocatedMemorySupported()) return null;
        try {
            if (!bean.isThreadAllocatedMemoryEnabled()) bean.setThreadAllocatedMemoryEnabled(true);
            return bean.isThreadAllocatedMemoryEnabled() ? bean : null;
        } catch (UnsupportedOperationException | SecurityException ignored) {
            return null;
        }
    }

    private static long allocatedBytes(com.sun.management.ThreadMXBean bean) {
        return bean == null ? -1L : bean.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private static List<Map<String, Object>> plainRows(int count) {
        List<Map<String, Object>> rows = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("_time", 1783000000000L + index);
            row.put("device_code", "device-" + index % 64);
            row.put("reading_value", index + 0.5);
            row.put("status", index % 4);
            rows.add(row);
        }
        return rows;
    }

    private static List<Map<String, Object>> annotatedRows(int count) {
        List<Map<String, Object>> rows = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("time", 1783000000000L + index);
            row.put("device", "device-" + index % 64);
            for (int field = 1; field <= 12; field++) {
                row.put(String.format(Locale.ROOT, "value_%02d", field), index + field + 0.5);
            }
            for (int field = 1; field <= 4; field++) {
                row.put(String.format(Locale.ROOT, "count_%02d", field), (long) index + field);
            }
            row.put("note", "sample-" + index);
            row.put("enabled", (index & 1) == 0);
            rows.add(row);
        }
        return rows;
    }

    private static String json(String text) {
        return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
    }

    private record Scenario<T>(String name, Class<T> type, List<Map<String, Object>> rows,
                                ToLongFunction<T> checksum) {
    }
    private record WorkerResult(long allocatedBytes, long checksum) {
    }
    private record Measurement(long nanos, long allocatedBytes, long checksum) {
    }

    private static final class Worker<T> {
        private final DefaultTSDBMetadataResolver resolver;
        private final Scenario<T> scenario;
        private final Object[] retainedResults = new Object[256];
        private Worker(DefaultTSDBMetadataResolver resolver, Scenario<T> scenario) {
            this.resolver = resolver;
            this.scenario = scenario;
        }
        private long mapRows(int passes) {
            long checksum = 0L;
            int position = 0;
            for (int pass = 0; pass < passes; pass++) {
                for (Map<String, Object> row : scenario.rows()) {
                    T result = resolver.toEntity(scenario.type(), row);
                    retainedResults[position++ & 255] = result;
                    checksum += scenario.checksum().applyAsLong(result);
                }
            }
            return checksum;
        }
    }

    private record Options(String label, int warmup, int iterations, int passes, int[] rows, int[] threads) {
        private static Options parse(String[] arguments) {
            String label = "unspecified";
            int warmup = 3;
            int iterations = 5;
            int passes = 1;
            int[] rows = {1000, 10000};
            int[] threads = {1, 4};
            for (int index = 0; index < arguments.length; index += 2) {
                if (index + 1 >= arguments.length) throw new IllegalArgumentException("Missing option value");
                String value = arguments[index + 1];
                switch (arguments[index]) {
                    case "--label" -> label = value;
                    case "--warmup" -> warmup = positive(value);
                    case "--iterations" -> iterations = positive(value);
                    case "--passes" -> passes = positive(value);
                    case "--rows" -> rows = positiveList(value);
                    case "--threads" -> threads = positiveList(value);
                    default -> throw new IllegalArgumentException("Unknown option: " + arguments[index]);
                }
            }
            return new Options(label, warmup, iterations, passes, rows, threads);
        }
        private static int positive(String value) {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) throw new IllegalArgumentException("Benchmark counts must be positive");
            return parsed;
        }
        private static int[] positiveList(String value) {
            return Arrays.stream(value.split(",")).mapToInt(Options::positive).toArray();
        }
    }

    public static class PlainBase {
        public Long timestamp;
    }
    public static class FewFieldsDto extends PlainBase {
        public String deviceCode;
        public Double readingValue;
        public int status;
    }
    public static class AnnotatedBase {
        @TGTime public Long time;
        @TGTag public String device;
    }
    public static class ManyFieldsDto extends AnnotatedBase {
        @TGField("value_01") public Double value01;
        @TGField("value_02") public Double value02;
        @TGField("value_03") public Double value03;
        @TGField("value_04") public Double value04;
        @TGField("value_05") public Double value05;
        @TGField("value_06") public Double value06;
        @TGField("value_07") public Double value07;
        @TGField("value_08") public Double value08;
        @TGField("value_09") public Double value09;
        @TGField("value_10") public Double value10;
        @TGField("value_11") public Double value11;
        @TGField("value_12") public Double value12;
        @TGField("count_01") public Long count01;
        @TGField("count_02") public Long count02;
        @TGField("count_03") public Long count03;
        @TGField("count_04") public Long count04;
        @TGField public String note;
        @TGField public boolean enabled;
    }
}
