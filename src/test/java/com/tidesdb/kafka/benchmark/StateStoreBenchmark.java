package com.tidesdb.kafka.benchmark;

import com.tidesdb.kafka.store.TidesDBStore;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Comprehensive benchmark suite comparing TidesDB vs RocksDB.
 * Generates CSV files for graphing performance metrics.
 *
 * All parameters are configurable via system properties:
 *   -Dbenchmark.data.dir=/path          Data directory for benchmark databases
 *   -Dbenchmark.sizes=1000,5000,10000   Comma-separated operation counts for standard benchmarks
 *   -Dbenchmark.large.sizes=100000,...   Comma-separated sizes for large dataset benchmarks
 *   -Dbenchmark.threads=1,2,4,8,16      Comma-separated thread counts for concurrent benchmarks
 *   -Dbenchmark.value.size=64           Value size in bytes for standard benchmarks
 *   -Dbenchmark.large.value.size=10240  Value size in bytes for large-value benchmarks
 *   -Dbenchmark.warmup=3                Number of warmup iterations
 *   -Dbenchmark.iterations=5            Number of measurement iterations
 *   -Dbenchmark.compaction.batch=50000  Batch size for compaction pressure test
 *   -Dbenchmark.compaction.batches=5    Number of batches for compaction pressure test
 *   -Dbenchmark.range.data=50000        Data size for range scan benchmark
 *   -Dbenchmark.range.sizes=10,100,...  Comma-separated range sizes
 *   -Dbenchmark.mixed.ratio=50          Read percentage for mixed workload (0-100)
 *   -Dbenchmark.seed=42                 Random seed for reproducibility
 *   -Dbenchmark.percentiles=true        Enable per-operation latency percentile tracking
 */
public class StateStoreBenchmark {

    @TempDir
    File tempDir;

    private File dataDir;
    private StateStoreContext context;
    private Random random;
    private static final String TIMESTAMP = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    private static final String DATA_DIR_PROPERTY = System.getProperty("benchmark.data.dir");
    
    // Configurable benchmark parameters (all driven by system properties)
    private int WARMUP_ITERATIONS;
    private int MEASUREMENT_ITERATIONS;
    private int[] STANDARD_SIZES;
    private int[] LARGE_SIZES;
    private int[] CONCURRENT_THREAD_COUNTS;
    private int VALUE_SIZE;
    private int LARGE_VALUE_SIZE;
    private int COMPACTION_BATCH_SIZE;
    private int COMPACTION_NUM_BATCHES;
    private int RANGE_DATA_SIZE;
    private int[] RANGE_SIZES;
    private int MIXED_READ_PERCENT;
    private boolean ENABLE_PERCENTILES;
    
    // Memory and CPU monitoring
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

    @BeforeEach
    void setUp() {
        // Use custom data directory if specified, otherwise use JUnit's temp directory
        if (DATA_DIR_PROPERTY != null && !DATA_DIR_PROPERTY.isEmpty()) {
            dataDir = new File(DATA_DIR_PROPERTY);
            dataDir.mkdirs();
            System.out.println("Using custom data directory: " + dataDir.getAbsolutePath());
        } else {
            dataDir = tempDir;
        }
        context = mock(StateStoreContext.class);
        when(context.stateDir()).thenReturn(dataDir);

        // Parse all configurable parameters from system properties
        long seed = Long.parseLong(System.getProperty("benchmark.seed", "42"));
        random = new Random(seed);

        WARMUP_ITERATIONS = Integer.parseInt(System.getProperty("benchmark.warmup", "3"));
        MEASUREMENT_ITERATIONS = Integer.parseInt(System.getProperty("benchmark.iterations", "5"));
        STANDARD_SIZES = parseIntArray(System.getProperty("benchmark.sizes", "1000,5000,10000,50000,100000"));
        LARGE_SIZES = parseIntArray(System.getProperty("benchmark.large.sizes", "100000,500000,1000000,5000000,10000000,25000000"));
        CONCURRENT_THREAD_COUNTS = parseIntArray(System.getProperty("benchmark.threads", "1,2,4,8,16"));
        VALUE_SIZE = Integer.parseInt(System.getProperty("benchmark.value.size", "64"));
        LARGE_VALUE_SIZE = Integer.parseInt(System.getProperty("benchmark.large.value.size", "10240"));
        COMPACTION_BATCH_SIZE = Integer.parseInt(System.getProperty("benchmark.compaction.batch", "50000"));
        COMPACTION_NUM_BATCHES = Integer.parseInt(System.getProperty("benchmark.compaction.batches", "5"));
        RANGE_DATA_SIZE = Integer.parseInt(System.getProperty("benchmark.range.data", "50000"));
        RANGE_SIZES = parseIntArray(System.getProperty("benchmark.range.sizes", "10,100,1000,5000,10000"));
        MIXED_READ_PERCENT = Integer.parseInt(System.getProperty("benchmark.mixed.ratio", "50"));
        ENABLE_PERCENTILES = Boolean.parseBoolean(System.getProperty("benchmark.percentiles", "true"));
    }

    private static int[] parseIntArray(String csv) {
        String[] parts = csv.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i].trim());
        }
        return result;
    }

    @AfterEach
    void tearDown() {
        // When using a custom data dir, clean up between runs to avoid stale data
        if (DATA_DIR_PROPERTY != null && !DATA_DIR_PROPERTY.isEmpty() && dataDir != null && dataDir.exists()) {
            deleteDirectory(dataDir);
        }
        // Otherwise cleanup is handled by @TempDir
    }

    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                }
                f.delete();
            }
        }
    }

    @Test
    public void runAllBenchmarks() throws IOException, InterruptedException {
        System.out.println("Starting comprehensive benchmarks...\n");
        System.out.println("Configuration:");
        System.out.println("  Warmup iterations: " + WARMUP_ITERATIONS);
        System.out.println("  Measurement iterations: " + MEASUREMENT_ITERATIONS);
        System.out.println("  Standard sizes: " + java.util.Arrays.toString(STANDARD_SIZES));
        System.out.println("  Large dataset sizes: " + java.util.Arrays.toString(LARGE_SIZES));
        System.out.println("  Thread counts: " + java.util.Arrays.toString(CONCURRENT_THREAD_COUNTS));
        System.out.println("  Value size: " + VALUE_SIZE + " bytes");
        System.out.println("  Large value size: " + LARGE_VALUE_SIZE + " bytes");
        System.out.println("  Mixed workload read ratio: " + MIXED_READ_PERCENT + "%");
        System.out.println("  Range data size: " + RANGE_DATA_SIZE);
        System.out.println("  Range sizes: " + java.util.Arrays.toString(RANGE_SIZES));
        System.out.println("  Compaction batch: " + COMPACTION_BATCH_SIZE + " x " + COMPACTION_NUM_BATCHES);
        System.out.println("  Percentile tracking: " + ENABLE_PERCENTILES);
        System.out.println("  Data directory: " + dataDir.getAbsolutePath());
        System.out.println();

        // Run all benchmark scenarios
        benchmarkSequentialWrites();
        benchmarkRandomWrites();
        benchmarkSequentialReads();
        benchmarkRandomReads();
        benchmarkMixedWorkload();
        benchmarkRangeScans();
        benchmarkBulkWrites();
        benchmarkUpdateWorkload();
        benchmarkLargeValues();
        benchmarkIterationPerformance();
        benchmarkDeleteWorkload();
        
        // Extended benchmarks
        benchmarkLargeDatasets();
        benchmarkConcurrentAccess();
        benchmarkCompactionPressure();
        benchmarkWithMetrics();

        // Percentile latency benchmark
        if (ENABLE_PERCENTILES) {
            benchmarkLatencyPercentiles();
        }

        System.out.println("\nAll benchmarks completed!");
        System.out.println("CSV files generated in: " + new File("benchmarks").getAbsolutePath());
    }

    private void benchmarkSequentialWrites() throws IOException {
        System.out.println("Benchmarking Sequential Writes...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("seq-write-tides");
            long tidesTime = measureSequentialWrites(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("seq-write-rocks");
            long rocksTime = measureSequentialWrites(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Sequential Writes", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/sequential_writes_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkRandomWrites() throws IOException {
        System.out.println("\nBenchmarking Random Writes...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("rand-write-tides");
            long tidesTime = measureRandomWrites(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("rand-write-rocks");
            long rocksTime = measureRandomWrites(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Random Writes", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/random_writes_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkSequentialReads() throws IOException {
        System.out.println("\nBenchmarking Sequential Reads...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("seq-read-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureSequentialReads(tidesStore, size);
            tidesStore.close();

            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("seq-read-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureSequentialReads(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Sequential Reads", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/sequential_reads_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkRandomReads() throws IOException {
        System.out.println("\nBenchmarking Random Reads...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("rand-read-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureRandomReads(tidesStore, size, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("rand-read-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureRandomReads(rocksStore, size, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Random Reads", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/random_reads_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkMixedWorkload() throws IOException {
        System.out.println(String.format("\nBenchmarking Mixed Workload (%d%% read, %d%% write)...", MIXED_READ_PERCENT, 100 - MIXED_READ_PERCENT));
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("mixed-tides");
            populateSequential(tidesStore, size / 2);
            long tidesTime = measureMixedWorkload(tidesStore, size, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("mixed-rocks");
            populateSequential(rocksStore, size / 2);
            long rocksTime = measureMixedWorkload(rocksStore, size, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Mixed Workload", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d ops: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/mixed_workload_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkRangeScans() throws IOException {
        System.out.println("\nBenchmarking Range Scans...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int rangeSize : RANGE_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("range-tides");
            populateSequential(tidesStore, RANGE_DATA_SIZE);
            long tidesTime = measureRangeScan(tidesStore, rangeSize);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("range-rocks");
            populateSequential(rocksStore, RANGE_DATA_SIZE);
            long rocksTime = measureRangeScan(rocksStore, rangeSize);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Range Scan", rangeSize, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                rangeSize, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/range_scans_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkBulkWrites() throws IOException {
        System.out.println("\nBenchmarking Bulk Writes (putAll)...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("bulk-tides");
            long tidesTime = measureBulkWrites(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("bulk-rocks");
            long rocksTime = measureBulkWrites(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Bulk Writes", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/bulk_writes_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkUpdateWorkload() throws IOException {
        System.out.println("\nBenchmarking Update Workload...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("update-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureUpdates(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("update-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureUpdates(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Updates", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/updates_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkLargeValues() throws IOException {
        System.out.println(String.format("\nBenchmarking Large Values (%d bytes each)...", LARGE_VALUE_SIZE));
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("large-tides");
            long tidesTime = measureLargeValueWrites(tidesStore, size, LARGE_VALUE_SIZE);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("large-rocks");
            long rocksTime = measureLargeValueWrites(rocksStore, size, LARGE_VALUE_SIZE);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Large Values", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/large_values_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkIterationPerformance() throws IOException {
        System.out.println("\nBenchmarking Full Iteration...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("iter-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureFullIteration(tidesStore);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("iter-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureFullIteration(rocksStore);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Full Iteration", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/iteration_" + TIMESTAMP + ".csv", results);
    }

    /**
     * Benchmark delete workload
     */
    private void benchmarkDeleteWorkload() throws IOException {
        System.out.println("\nBenchmarking Delete Workload...");
        
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : STANDARD_SIZES) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("delete-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureDeletes(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("delete-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureDeletes(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Deletes", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/deletes_" + TIMESTAMP + ".csv", results);
    }

    // ==================== EXTENDED BENCHMARKS ====================

    /**
     * Benchmark with large datasets (up to 1M keys) with warmup
     */
    private void benchmarkLargeDatasets() throws IOException {
        System.out.println("\n=== EXTENDED: Large Dataset Benchmark (with warmup) ===");
        
        List<ExtendedBenchmarkResult> results = new ArrayList<>();

        for (int size : LARGE_SIZES) {
            System.out.printf("\nTesting %,d keys...%n", size);
            
            // Warmup phase
            System.out.println("  Warming up...");
            for (int w = 0; w < WARMUP_ITERATIONS; w++) {
                KeyValueStore<Bytes, byte[]> warmupStore = createTidesDBStore("warmup-tides-" + w);
                measureSequentialWrites(warmupStore, Math.min(size / 10, 10000));
                warmupStore.close();
            }
            
            // Measurement phase - multiple iterations
            long[] tidesTimes = new long[MEASUREMENT_ITERATIONS];
            long[] rocksTimes = new long[MEASUREMENT_ITERATIONS];
            
            for (int iter = 0; iter < MEASUREMENT_ITERATIONS; iter++) {
                // TidesDB
                KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("large-tides-" + iter);
                tidesTimes[iter] = measureSequentialWrites(tidesStore, size);
                tidesStore.close();

                // RocksDB
                KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("large-rocks-" + iter);
                rocksTimes[iter] = measureSequentialWrites(rocksStore, size);
                rocksStore.close();
            }
            
            // Calculate statistics
            long tidesAvg = average(tidesTimes);
            long rocksAvg = average(rocksTimes);
            long tidesStdDev = stdDev(tidesTimes);
            long rocksStdDev = stdDev(rocksTimes);
            
            results.add(new ExtendedBenchmarkResult(
                "Large Dataset Writes", size, tidesAvg, rocksAvg, tidesStdDev, rocksStdDev
            ));

            System.out.printf("  %,d keys: TidesDB=%dms (±%d), RocksDB=%dms (±%d), Speedup=%.2fx%n",
                size, tidesAvg, tidesStdDev, rocksAvg, rocksStdDev, (double) rocksAvg / tidesAvg);
        }

        writeExtendedCsv("benchmarks/large_datasets_" + TIMESTAMP + ".csv", results);
    }

    /**
     * Benchmark concurrent/multi-threaded access
     */
    private void benchmarkConcurrentAccess() throws IOException, InterruptedException {
        System.out.println("\n=== EXTENDED: Concurrent Access Benchmark ===");
        
        int dataSize = 100000;
        int opsPerThread = 10000;
        List<ConcurrentBenchmarkResult> results = new ArrayList<>();

        for (int threadCount : CONCURRENT_THREAD_COUNTS) {
            System.out.printf("\nTesting with %d threads...%n", threadCount);
            
            // TidesDB concurrent test
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("concurrent-tides");
            populateSequential(tidesStore, dataSize);
            long tidesTime = measureConcurrentMixedWorkload(tidesStore, dataSize, opsPerThread, threadCount);
            long tidesThroughput = (long) ((threadCount * opsPerThread) / (tidesTime / 1000.0));
            tidesStore.close();

            // RocksDB concurrent test
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("concurrent-rocks");
            populateSequential(rocksStore, dataSize);
            long rocksTime = measureConcurrentMixedWorkload(rocksStore, dataSize, opsPerThread, threadCount);
            long rocksThroughput = (long) ((threadCount * opsPerThread) / (rocksTime / 1000.0));
            rocksStore.close();

            results.add(new ConcurrentBenchmarkResult(
                "Concurrent Mixed", threadCount, threadCount * opsPerThread,
                tidesTime, rocksTime, tidesThroughput, rocksThroughput
            ));

            System.out.printf("  %d threads: TidesDB=%dms (%,d ops/s), RocksDB=%dms (%,d ops/s)%n",
                threadCount, tidesTime, tidesThroughput, rocksTime, rocksThroughput);
        }

        writeConcurrentCsv("benchmarks/concurrent_access_" + TIMESTAMP + ".csv", results);
    }

    /**
     * Benchmark with compaction pressure (accumulated data over time)
     */
    private void benchmarkCompactionPressure() throws IOException {
        System.out.println("\n=== EXTENDED: Compaction Pressure Benchmark ===");
        
        int batchSize = COMPACTION_BATCH_SIZE;
        int numBatches = COMPACTION_NUM_BATCHES;
        List<CompactionBenchmarkResult> results = new ArrayList<>();

        // TidesDB - accumulate data over multiple batches
        System.out.println("\nTidesDB compaction pressure test...");
        KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("compaction-tides");
        for (int batch = 1; batch <= numBatches; batch++) {
            int startKey = (batch - 1) * batchSize;
            long writeTime = measureSequentialWritesWithOffset(tidesStore, batchSize, startKey);
            long readTime = measureRandomReads(tidesStore, batch * batchSize, batchSize);
            
            results.add(new CompactionBenchmarkResult(
                "TidesDB", batch, batch * batchSize, writeTime, readTime
            ));
            
            System.out.printf("  Batch %d (%,d total keys): write=%dms, read=%dms%n",
                batch, batch * batchSize, writeTime, readTime);
        }
        tidesStore.close();

        // RocksDB - accumulate data over multiple batches
        System.out.println("\nRocksDB compaction pressure test...");
        KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("compaction-rocks");
        for (int batch = 1; batch <= numBatches; batch++) {
            int startKey = (batch - 1) * batchSize;
            long writeTime = measureSequentialWritesWithOffset(rocksStore, batchSize, startKey);
            long readTime = measureRandomReads(rocksStore, batch * batchSize, batchSize);
            
            results.add(new CompactionBenchmarkResult(
                "RocksDB", batch, batch * batchSize, writeTime, readTime
            ));
            
            System.out.printf("  Batch %d (%,d total keys): write=%dms, read=%dms%n",
                batch, batch * batchSize, writeTime, readTime);
        }
        rocksStore.close();

        writeCompactionCsv("benchmarks/compaction_pressure_" + TIMESTAMP + ".csv", results);
    }

    /**
     * Benchmark with memory and CPU metrics
     */
    private void benchmarkWithMetrics() throws IOException {
        System.out.println("\n=== EXTENDED: Benchmark with Memory/CPU Metrics ===");
        
        int[] sizes = STANDARD_SIZES;
        List<MetricsBenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            System.out.printf("\nTesting %,d keys with metrics...%n", size);
            
            // Force GC before measurement
            System.gc();
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            
            // TidesDB with metrics
            long tidesMemBefore = getUsedMemory();
            double tidesCpuBefore = getProcessCpuLoad();
            
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("metrics-tides");
            long tidesWriteTime = measureSequentialWrites(tidesStore, size);
            long tidesReadTime = measureRandomReads(tidesStore, size, size);
            
            long tidesMemAfter = getUsedMemory();
            double tidesCpuAfter = getProcessCpuLoad();
            long tidesMemUsed = tidesMemAfter - tidesMemBefore;
            tidesStore.close();
            
            // Force GC before RocksDB
            System.gc();
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            
            // RocksDB with metrics
            long rocksMemBefore = getUsedMemory();
            double rocksCpuBefore = getProcessCpuLoad();
            
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("metrics-rocks");
            long rocksWriteTime = measureSequentialWrites(rocksStore, size);
            long rocksReadTime = measureRandomReads(rocksStore, size, size);
            
            long rocksMemAfter = getUsedMemory();
            double rocksCpuAfter = getProcessCpuLoad();
            long rocksMemUsed = rocksMemAfter - rocksMemBefore;
            rocksStore.close();

            results.add(new MetricsBenchmarkResult(
                "With Metrics", size,
                tidesWriteTime, tidesReadTime, tidesMemUsed, (tidesCpuBefore + tidesCpuAfter) / 2,
                rocksWriteTime, rocksReadTime, rocksMemUsed, (rocksCpuBefore + rocksCpuAfter) / 2
            ));

            System.out.printf("  TidesDB: write=%dms, read=%dms, mem=%,dKB%n",
                tidesWriteTime, tidesReadTime, tidesMemUsed / 1024);
            System.out.printf("  RocksDB: write=%dms, read=%dms, mem=%,dKB%n",
                rocksWriteTime, rocksReadTime, rocksMemUsed / 1024);
        }

        writeMetricsCsv("benchmarks/metrics_" + TIMESTAMP + ".csv", results);
    }

    /**
     * Benchmark per-operation latency with percentile tracking (p50, p90, p95, p99, p99.9, max).
     * Measures individual operation times in nanoseconds for detailed latency analysis.
     */
    private void benchmarkLatencyPercentiles() throws IOException {
        System.out.println("\n=== EXTENDED: Latency Percentile Benchmark ===");
        
        int dataSize = STANDARD_SIZES.length > 0 ? STANDARD_SIZES[STANDARD_SIZES.length - 1] : 50000;
        int sampleOps = Math.min(dataSize, 50000); // Cap samples for memory
        List<PercentileBenchmarkResult> results = new ArrayList<>();

        String[] opTypes = {"write", "read", "mixed"};
        for (String opType : opTypes) {
            System.out.printf("\n  Profiling %s latencies (%,d ops)...%n", opType, sampleOps);

            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("pctl-tides-" + opType);
            if (!opType.equals("write")) {
                populateSequential(tidesStore, dataSize);
            }
            long[] tidesLatencies = measureLatencies(tidesStore, dataSize, sampleOps, opType);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("pctl-rocks-" + opType);
            if (!opType.equals("write")) {
                populateSequential(rocksStore, dataSize);
            }
            long[] rocksLatencies = measureLatencies(rocksStore, dataSize, sampleOps, opType);
            rocksStore.close();

            java.util.Arrays.sort(tidesLatencies);
            java.util.Arrays.sort(rocksLatencies);

            long tidesP50 = percentile(tidesLatencies, 50);
            long tidesP90 = percentile(tidesLatencies, 90);
            long tidesP95 = percentile(tidesLatencies, 95);
            long tidesP99 = percentile(tidesLatencies, 99);
            long tidesP999 = percentile(tidesLatencies, 99.9);
            long tidesMax = tidesLatencies[tidesLatencies.length - 1];

            long rocksP50 = percentile(rocksLatencies, 50);
            long rocksP90 = percentile(rocksLatencies, 90);
            long rocksP95 = percentile(rocksLatencies, 95);
            long rocksP99 = percentile(rocksLatencies, 99);
            long rocksP999 = percentile(rocksLatencies, 99.9);
            long rocksMax = rocksLatencies[rocksLatencies.length - 1];

            results.add(new PercentileBenchmarkResult(opType, sampleOps,
                tidesP50, tidesP90, tidesP95, tidesP99, tidesP999, tidesMax,
                rocksP50, rocksP90, rocksP95, rocksP99, rocksP999, rocksMax));

            System.out.printf("    TidesDB: p50=%dns p90=%dns p95=%dns p99=%dns p99.9=%dns max=%dns%n",
                tidesP50, tidesP90, tidesP95, tidesP99, tidesP999, tidesMax);
            System.out.printf("    RocksDB: p50=%dns p90=%dns p95=%dns p99=%dns p99.9=%dns max=%dns%n",
                rocksP50, rocksP90, rocksP95, rocksP99, rocksP999, rocksMax);
        }

        writePercentileCsv("benchmarks/latency_percentiles_" + TIMESTAMP + ".csv", results);
    }

    private long[] measureLatencies(KeyValueStore<Bytes, byte[]> store, int dataSize, int opCount, String opType) {
        long[] latencies = new long[opCount];
        for (int i = 0; i < opCount; i++) {
            int idx = (opType.equals("write")) ? i : random.nextInt(dataSize);
            String key = String.format("key_%08d", idx);
            Bytes keyBytes = Bytes.wrap(key.getBytes(StandardCharsets.UTF_8));

            long t0 = System.nanoTime();
            switch (opType) {
                case "write":
                    String value = String.format("value_%08d", idx);
                    store.put(keyBytes, value.getBytes(StandardCharsets.UTF_8));
                    break;
                case "read":
                    store.get(keyBytes);
                    break;
                case "mixed":
                    if (random.nextInt(100) < MIXED_READ_PERCENT) {
                        store.get(keyBytes);
                    } else {
                        String val = String.format("value_%08d", idx);
                        store.put(keyBytes, val.getBytes(StandardCharsets.UTF_8));
                    }
                    break;
            }
            latencies[i] = System.nanoTime() - t0;
        }
        return latencies;
    }

    private long percentile(long[] sorted, double pct) {
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    // ==================== EXTENDED HELPER METHODS ====================

    private long measureConcurrentMixedWorkload(KeyValueStore<Bytes, byte[]> store, int dataSize, 
                                                 int opsPerThread, int threadCount) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicLong totalOps = new AtomicLong(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Random threadRandom = new Random(42 + threadId);
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int i = 0; i < opsPerThread; i++) {
                        int idx = threadRandom.nextInt(dataSize);
                        String key = String.format("key_%08d", idx);
                        
                        if (threadRandom.nextBoolean()) {
                            store.get(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
                        } else {
                            String value = String.format("value_%08d_%d", idx, threadId);
                            store.put(
                                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                                value.getBytes(StandardCharsets.UTF_8)
                            );
                        }
                        totalOps.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long start = System.currentTimeMillis();
        startLatch.countDown(); // Start all threads
        doneLatch.await(5, TimeUnit.MINUTES); // Wait for completion
        long elapsed = System.currentTimeMillis() - start;
        
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        return elapsed;
    }

    private long measureSequentialWritesWithOffset(KeyValueStore<Bytes, byte[]> store, int count, int offset) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", offset + i);
            String value = String.format("value_%08d", offset + i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long getUsedMemory() {
        return memoryBean.getHeapMemoryUsage().getUsed() + 
               memoryBean.getNonHeapMemoryUsage().getUsed();
    }

    private double getProcessCpuLoad() {
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            return ((com.sun.management.OperatingSystemMXBean) osBean).getProcessCpuLoad() * 100;
        }
        return -1;
    }

    private long average(long[] values) {
        long sum = 0;
        for (long v : values) sum += v;
        return sum / values.length;
    }

    private long stdDev(long[] values) {
        long avg = average(values);
        long sumSquares = 0;
        for (long v : values) {
            sumSquares += (v - avg) * (v - avg);
        }
        return (long) Math.sqrt(sumSquares / values.length);
    }

    // ==================== EXTENDED RESULT CLASSES ====================

    private static class ExtendedBenchmarkResult {
        final String name;
        final int size;
        final long tidesAvg;
        final long rocksAvg;
        final long tidesStdDev;
        final long rocksStdDev;

        ExtendedBenchmarkResult(String name, int size, long tidesAvg, long rocksAvg, 
                                long tidesStdDev, long rocksStdDev) {
            this.name = name;
            this.size = size;
            this.tidesAvg = tidesAvg;
            this.rocksAvg = rocksAvg;
            this.tidesStdDev = tidesStdDev;
            this.rocksStdDev = rocksStdDev;
        }
    }

    private static class ConcurrentBenchmarkResult {
        final String name;
        final int threads;
        final int totalOps;
        final long tidesTime;
        final long rocksTime;
        final long tidesThroughput;
        final long rocksThroughput;

        ConcurrentBenchmarkResult(String name, int threads, int totalOps,
                                   long tidesTime, long rocksTime,
                                   long tidesThroughput, long rocksThroughput) {
            this.name = name;
            this.threads = threads;
            this.totalOps = totalOps;
            this.tidesTime = tidesTime;
            this.rocksTime = rocksTime;
            this.tidesThroughput = tidesThroughput;
            this.rocksThroughput = rocksThroughput;
        }
    }

    private static class CompactionBenchmarkResult {
        final String store;
        final int batch;
        final int totalKeys;
        final long writeTime;
        final long readTime;

        CompactionBenchmarkResult(String store, int batch, int totalKeys, 
                                   long writeTime, long readTime) {
            this.store = store;
            this.batch = batch;
            this.totalKeys = totalKeys;
            this.writeTime = writeTime;
            this.readTime = readTime;
        }
    }

    private static class MetricsBenchmarkResult {
        final String name;
        final int size;
        final long tidesWriteTime;
        final long tidesReadTime;
        final long tidesMemUsed;
        final double tidesCpuAvg;
        final long rocksWriteTime;
        final long rocksReadTime;
        final long rocksMemUsed;
        final double rocksCpuAvg;

        MetricsBenchmarkResult(String name, int size,
                               long tidesWriteTime, long tidesReadTime, long tidesMemUsed, double tidesCpuAvg,
                               long rocksWriteTime, long rocksReadTime, long rocksMemUsed, double rocksCpuAvg) {
            this.name = name;
            this.size = size;
            this.tidesWriteTime = tidesWriteTime;
            this.tidesReadTime = tidesReadTime;
            this.tidesMemUsed = tidesMemUsed;
            this.tidesCpuAvg = tidesCpuAvg;
            this.rocksWriteTime = rocksWriteTime;
            this.rocksReadTime = rocksReadTime;
            this.rocksMemUsed = rocksMemUsed;
            this.rocksCpuAvg = rocksCpuAvg;
        }
    }

    private static class PercentileBenchmarkResult {
        final String opType;
        final int sampleOps;
        final long tidesP50, tidesP90, tidesP95, tidesP99, tidesP999, tidesMax;
        final long rocksP50, rocksP90, rocksP95, rocksP99, rocksP999, rocksMax;

        PercentileBenchmarkResult(String opType, int sampleOps,
                                   long tidesP50, long tidesP90, long tidesP95, long tidesP99, long tidesP999, long tidesMax,
                                   long rocksP50, long rocksP90, long rocksP95, long rocksP99, long rocksP999, long rocksMax) {
            this.opType = opType;
            this.sampleOps = sampleOps;
            this.tidesP50 = tidesP50; this.tidesP90 = tidesP90; this.tidesP95 = tidesP95;
            this.tidesP99 = tidesP99; this.tidesP999 = tidesP999; this.tidesMax = tidesMax;
            this.rocksP50 = rocksP50; this.rocksP90 = rocksP90; this.rocksP95 = rocksP95;
            this.rocksP99 = rocksP99; this.rocksP999 = rocksP999; this.rocksMax = rocksMax;
        }
    }

    // ==================== EXTENDED CSV WRITERS ====================

    private void writePercentileCsv(String filename, List<PercentileBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("OpType,SampleOps,TidesDB_p50_ns,TidesDB_p90_ns,TidesDB_p95_ns,TidesDB_p99_ns,TidesDB_p999_ns,TidesDB_max_ns,RocksDB_p50_ns,RocksDB_p90_ns,RocksDB_p95_ns,RocksDB_p99_ns,RocksDB_p999_ns,RocksDB_max_ns");
            for (PercentileBenchmarkResult r : results) {
                writer.printf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                    r.opType, r.sampleOps,
                    r.tidesP50, r.tidesP90, r.tidesP95, r.tidesP99, r.tidesP999, r.tidesMax,
                    r.rocksP50, r.rocksP90, r.rocksP95, r.rocksP99, r.rocksP999, r.rocksMax);
            }
        }
    }

    private void writeExtendedCsv(String filename, List<ExtendedBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Benchmark,Size,TidesDB_avg_ms,RocksDB_avg_ms,TidesDB_stddev,RocksDB_stddev,Speedup");
            for (ExtendedBenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%d,%d,%.2f%n",
                    result.name,
                    result.size,
                    result.tidesAvg,
                    result.rocksAvg,
                    result.tidesStdDev,
                    result.rocksStdDev,
                    (double) result.rocksAvg / result.tidesAvg
                );
            }
        }
    }

    private void writeConcurrentCsv(String filename, List<ConcurrentBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Benchmark,Threads,TotalOps,TidesDB_ms,RocksDB_ms,TidesDB_ops_sec,RocksDB_ops_sec,Speedup");
            for (ConcurrentBenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%d,%d,%d,%.2f%n",
                    result.name,
                    result.threads,
                    result.totalOps,
                    result.tidesTime,
                    result.rocksTime,
                    result.tidesThroughput,
                    result.rocksThroughput,
                    (double) result.rocksTime / result.tidesTime
                );
            }
        }
    }

    private void writeCompactionCsv(String filename, List<CompactionBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Store,Batch,TotalKeys,WriteTime_ms,ReadTime_ms");
            for (CompactionBenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%d%n",
                    result.store,
                    result.batch,
                    result.totalKeys,
                    result.writeTime,
                    result.readTime
                );
            }
        }
    }

    private void writeMetricsCsv(String filename, List<MetricsBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Benchmark,Size,TidesDB_write_ms,TidesDB_read_ms,TidesDB_mem_bytes,TidesDB_cpu_pct,RocksDB_write_ms,RocksDB_read_ms,RocksDB_mem_bytes,RocksDB_cpu_pct");
            for (MetricsBenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%d,%.2f,%d,%d,%d,%.2f%n",
                    result.name,
                    result.size,
                    result.tidesWriteTime,
                    result.tidesReadTime,
                    result.tidesMemUsed,
                    result.tidesCpuAvg,
                    result.rocksWriteTime,
                    result.rocksReadTime,
                    result.rocksMemUsed,
                    result.rocksCpuAvg
                );
            }
        }
    }

    // Helper methods for measurements

    private long measureSequentialWrites(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            String value = String.format("value_%08d", i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long measureRandomWrites(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            int idx = random.nextInt(count * 2);
            String key = String.format("key_%08d", idx);
            String value = String.format("value_%08d", idx);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long measureSequentialReads(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            store.get(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
        }
        return System.currentTimeMillis() - start;
    }

    private long measureRandomReads(KeyValueStore<Bytes, byte[]> store, int dataSize, int readCount) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < readCount; i++) {
            int idx = random.nextInt(dataSize);
            String key = String.format("key_%08d", idx);
            store.get(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
        }
        return System.currentTimeMillis() - start;
    }

    private long measureMixedWorkload(KeyValueStore<Bytes, byte[]> store, int dataSize, int opCount) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < opCount; i++) {
            int idx = random.nextInt(dataSize);
            String key = String.format("key_%08d", idx);
            
            if (random.nextInt(100) < MIXED_READ_PERCENT) {
                // Read
                store.get(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
            } else {
                // Write
                String value = String.format("value_%08d", idx);
                store.put(
                    Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                    value.getBytes(StandardCharsets.UTF_8)
                );
            }
        }
        return System.currentTimeMillis() - start;
    }

    private long measureRangeScan(KeyValueStore<Bytes, byte[]> store, int rangeSize) {
        int startIdx = random.nextInt(Math.max(1, RANGE_DATA_SIZE - rangeSize));
        String fromKey = String.format("key_%08d", startIdx);
        String toKey = String.format("key_%08d", startIdx + rangeSize);

        long start = System.currentTimeMillis();
        try (KeyValueIterator<Bytes, byte[]> iter = store.range(
                Bytes.wrap(fromKey.getBytes(StandardCharsets.UTF_8)),
                Bytes.wrap(toKey.getBytes(StandardCharsets.UTF_8))
        )) {
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
        }
        return System.currentTimeMillis() - start;
    }

    private long measureBulkWrites(KeyValueStore<Bytes, byte[]> store, int count) {
        List<KeyValue<Bytes, byte[]>> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            String value = String.format("value_%08d", i);
            batch.add(KeyValue.pair(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            ));
        }

        long start = System.currentTimeMillis();
        store.putAll(batch);
        return System.currentTimeMillis() - start;
    }

    private long measureUpdates(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            String value = String.format("updated_value_%08d", i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long measureLargeValueWrites(KeyValueStore<Bytes, byte[]> store, int count, int valueSize) {
        byte[] largeValue = new byte[valueSize];
        random.nextBytes(largeValue);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                largeValue
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long measureDeletes(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            store.delete(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
        }
        return System.currentTimeMillis() - start;
    }

    private long measureFullIteration(KeyValueStore<Bytes, byte[]> store) {
        long start = System.currentTimeMillis();
        try (KeyValueIterator<Bytes, byte[]> iter = store.all()) {
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
        }
        return System.currentTimeMillis() - start;
    }

    private void populateSequential(KeyValueStore<Bytes, byte[]> store, int count) {
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            String value = String.format("value_%08d", i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    private KeyValueStore<Bytes, byte[]> createTidesDBStore(String name) {
        TidesDBStore store = new TidesDBStore(name);
        store.init(context, store);
        return store;
    }

    private RocksDBWrapper createRocksDBStore(String name) {
        try {
            RocksDB.loadLibrary();

            // Configure RocksDB to match TidesDB settings for a fair comparison:
            //   - LZ4 compression (same as TidesDB default)
            //   - Bloom filter with 10 bits/key (~1% FPR, same as TidesDB 0.01)
            //   - 64MB block cache (same as TidesDB blockCacheSize)
            //   - 64MB write buffer (same as TidesDB writeBufferSize)
            //   - Block-based table with block indexes
            org.rocksdb.BloomFilter bloomFilter = new org.rocksdb.BloomFilter(10, false);
            org.rocksdb.BlockBasedTableConfig tableConfig = new org.rocksdb.BlockBasedTableConfig()
                .setFilterPolicy(bloomFilter)
                .setBlockSize(16 * 1024)  // 16KB blocks
                .setBlockCache(new org.rocksdb.LRUCache(64 * 1024 * 1024))  // 64MB cache
                .setIndexType(org.rocksdb.IndexType.kBinarySearch);

            Options options = new Options()
                .setCreateIfMissing(true)
                .setParanoidChecks(false)
                .setCompressionType(org.rocksdb.CompressionType.LZ4_COMPRESSION)
                .setWriteBufferSize(64 * 1024 * 1024)  // 64MB write buffer
                .setMaxWriteBufferNumber(3)
                .setMinWriteBufferNumberToMerge(1)
                .setLevelCompactionDynamicLevelBytes(true)
                .setNumLevels(5)
                .setMaxBackgroundJobs(4)  // 2 flush + 2 compaction (same as TidesDB)
                .setTableFormatConfig(tableConfig);

            String dbPath = new File(dataDir, name).getAbsolutePath();
            RocksDB db = RocksDB.open(options, dbPath);
            return new RocksDBWrapper(db, options);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to create RocksDB store", e);
        }
    }

    /**
     * Simple wrapper around RocksDB to match the KeyValueStore interface for benchmarking
     */
    private static class RocksDBWrapper implements KeyValueStore<Bytes, byte[]> {
        private final RocksDB db;
        private final Options options;
        private final org.rocksdb.WriteOptions writeOptions;
        private boolean open = true;

        RocksDBWrapper(RocksDB db, Options options) {
            this.db = db;
            this.options = options;
            // Explicitly disable sync for fair comparison with TidesDB SYNC_NONE
            this.writeOptions = new org.rocksdb.WriteOptions().setSync(false).setDisableWAL(false);
        }

        @Override
        public void put(Bytes key, byte[] value) {
            try {
                if (value == null) {
                    db.delete(writeOptions, key.get());
                } else {
                    db.put(writeOptions, key.get(), value);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] putIfAbsent(Bytes key, byte[] value) {
            byte[] existing = get(key);
            if (existing == null) {
                put(key, value);
                return null;
            }
            return existing;
        }

        @Override
        public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                for (KeyValue<Bytes, byte[]> entry : entries) {
                    if (entry.value == null) {
                        batch.delete(entry.key.get());
                    } else {
                        batch.put(entry.key.get(), entry.value);
                    }
                }
                db.write(writeOptions, batch);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] delete(Bytes key) {
            byte[] old = get(key);
            try {
                db.delete(writeOptions, key.get());
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            return old;
        }

        @Override
        public byte[] get(Bytes key) {
            try {
                return db.get(key.get());
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
            return new RocksDBRangeIterator(db, from, to);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all() {
            return new RocksDBRangeIterator(db, null, null);
        }

        @Override
        public long approximateNumEntries() {
            try {
                return Long.parseLong(db.getProperty("rocksdb.estimate-num-keys"));
            } catch (RocksDBException e) {
                return 0;
            }
        }

        @Override
        public String name() {
            return "rocksdb-benchmark";
        }

        @Override
        public void init(org.apache.kafka.streams.processor.ProcessorContext context,
                         org.apache.kafka.streams.processor.StateStore root) {
        }

        @Override
        public void init(org.apache.kafka.streams.processor.StateStoreContext context,
                         org.apache.kafka.streams.processor.StateStore root) {
        }

        @Override
        public void flush() {
            try {
                db.flush(new org.rocksdb.FlushOptions());
            } catch (RocksDBException e) {
                // ignore
            }
        }

        @Override
        public void close() {
            if (open) {
                writeOptions.close();
                db.close();
                options.close();
                open = false;
            }
        }

        @Override
        public boolean persistent() {
            return true;
        }

        @Override
        public boolean isOpen() {
            return open;
        }
    }

    /**
     * Simple RocksDB range iterator for benchmarking
     */
    private static class RocksDBRangeIterator implements KeyValueIterator<Bytes, byte[]> {
        private final org.rocksdb.RocksIterator iterator;
        private final byte[] toKey;
        private boolean hasNext;
        private KeyValue<Bytes, byte[]> next;

        RocksDBRangeIterator(RocksDB db, Bytes from, Bytes to) {
            this.iterator = db.newIterator();
            this.toKey = to != null ? to.get() : null;
            
            if (from != null) {
                iterator.seek(from.get());
            } else {
                iterator.seekToFirst();
            }
            advance();
        }

        private void advance() {
            if (iterator.isValid()) {
                byte[] key = iterator.key();
                if (toKey != null && compareBytes(key, toKey) >= 0) {
                    hasNext = false;
                    next = null;
                } else {
                    next = KeyValue.pair(Bytes.wrap(key), iterator.value());
                    hasNext = true;
                    iterator.next();
                }
            } else {
                hasNext = false;
                next = null;
            }
        }

        private int compareBytes(byte[] a, byte[] b) {
            int minLen = Math.min(a.length, b.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
                if (cmp != 0) return cmp;
            }
            return a.length - b.length;
        }

        @Override
        public void close() {
            iterator.close();
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext) throw new java.util.NoSuchElementException();
            return next.key;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext) throw new java.util.NoSuchElementException();
            KeyValue<Bytes, byte[]> current = next;
            advance();
            return current;
        }
    }

    private void writeCsv(String filename, List<BenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Benchmark,Size,TidesDB_ms,RocksDB_ms,Speedup");
            for (BenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%.2f%n",
                    result.name,
                    result.size,
                    result.tidesTime,
                    result.rocksTime,
                    (double) result.rocksTime / result.tidesTime
                );
            }
        }
    }

    private static class BenchmarkResult {
        final String name;
        final int size;
        final long tidesTime;
        final long rocksTime;

        BenchmarkResult(String name, int size, long tidesTime, long rocksTime) {
            this.name = name;
            this.size = size;
            this.tidesTime = tidesTime;
            this.rocksTime = rocksTime;
        }
    }
}
