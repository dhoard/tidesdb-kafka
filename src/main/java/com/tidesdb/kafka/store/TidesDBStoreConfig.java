package com.tidesdb.kafka.store;

import com.tidesdb.*;

/**
 * Configuration for TidesDB-backed Kafka Streams state stores.
 * Allows fine-grained control over database and column family settings.
 */
public class TidesDBStoreConfig {

    // Database-level settings
    private final int numFlushThreads;
    private final int numCompactionThreads;
    private final LogLevel logLevel;
    private final long blockCacheSize;
    private final long maxOpenSSTables;
    private final long maxMemoryUsage;
    private final boolean logToFile;
    private final long logTruncationAt;

    // Column family settings
    private final long writeBufferSize;
    private final CompressionAlgorithm compressionAlgorithm;
    private final boolean enableBloomFilter;
    private final double bloomFPR;
    private final boolean enableBlockIndexes;
    private final int indexSampleRatio;
    private final int blockIndexPrefixLen;
    private final SyncMode syncMode;
    private final long syncIntervalUs;
    private final boolean useBtree;
    private final int minLevels;
    private final long levelSizeRatio;
    private final int skipListMaxLevel;
    private final float skipListProbability;
    private final IsolationLevel defaultIsolationLevel;
    private final long klogValueThreshold;
    private final int l0QueueStallThreshold;
    private final int l1FileCountTrigger;

    // Store behavior
    private final String columnFamilyName;
    private final long defaultTtlSeconds;

    private TidesDBStoreConfig(Builder builder) {
        this.numFlushThreads = builder.numFlushThreads;
        this.numCompactionThreads = builder.numCompactionThreads;
        this.logLevel = builder.logLevel;
        this.blockCacheSize = builder.blockCacheSize;
        this.maxOpenSSTables = builder.maxOpenSSTables;
        this.maxMemoryUsage = builder.maxMemoryUsage;
        this.logToFile = builder.logToFile;
        this.logTruncationAt = builder.logTruncationAt;
        this.writeBufferSize = builder.writeBufferSize;
        this.compressionAlgorithm = builder.compressionAlgorithm;
        this.enableBloomFilter = builder.enableBloomFilter;
        this.bloomFPR = builder.bloomFPR;
        this.enableBlockIndexes = builder.enableBlockIndexes;
        this.indexSampleRatio = builder.indexSampleRatio;
        this.blockIndexPrefixLen = builder.blockIndexPrefixLen;
        this.syncMode = builder.syncMode;
        this.syncIntervalUs = builder.syncIntervalUs;
        this.useBtree = builder.useBtree;
        this.minLevels = builder.minLevels;
        this.levelSizeRatio = builder.levelSizeRatio;
        this.skipListMaxLevel = builder.skipListMaxLevel;
        this.skipListProbability = builder.skipListProbability;
        this.defaultIsolationLevel = builder.defaultIsolationLevel;
        this.klogValueThreshold = builder.klogValueThreshold;
        this.l0QueueStallThreshold = builder.l0QueueStallThreshold;
        this.l1FileCountTrigger = builder.l1FileCountTrigger;
        this.columnFamilyName = builder.columnFamilyName;
        this.defaultTtlSeconds = builder.defaultTtlSeconds;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Returns a config with sensible defaults for Kafka Streams workloads */
    public static TidesDBStoreConfig defaultConfig() {
        return builder().build();
    }

    // Database-level getters
    public int getNumFlushThreads() { return numFlushThreads; }
    public int getNumCompactionThreads() { return numCompactionThreads; }
    public LogLevel getLogLevel() { return logLevel; }
    public long getBlockCacheSize() { return blockCacheSize; }
    public long getMaxOpenSSTables() { return maxOpenSSTables; }
    public long getMaxMemoryUsage() { return maxMemoryUsage; }
    public boolean isLogToFile() { return logToFile; }
    public long getLogTruncationAt() { return logTruncationAt; }

    // Column family getters
    public long getWriteBufferSize() { return writeBufferSize; }
    public CompressionAlgorithm getCompressionAlgorithm() { return compressionAlgorithm; }
    public boolean isEnableBloomFilter() { return enableBloomFilter; }
    public double getBloomFPR() { return bloomFPR; }
    public boolean isEnableBlockIndexes() { return enableBlockIndexes; }
    public int getIndexSampleRatio() { return indexSampleRatio; }
    public int getBlockIndexPrefixLen() { return blockIndexPrefixLen; }
    public SyncMode getSyncMode() { return syncMode; }
    public long getSyncIntervalUs() { return syncIntervalUs; }
    public boolean isUseBtree() { return useBtree; }
    public int getMinLevels() { return minLevels; }
    public long getLevelSizeRatio() { return levelSizeRatio; }
    public int getSkipListMaxLevel() { return skipListMaxLevel; }
    public float getSkipListProbability() { return skipListProbability; }
    public IsolationLevel getDefaultIsolationLevel() { return defaultIsolationLevel; }
    public long getKlogValueThreshold() { return klogValueThreshold; }
    public int getL0QueueStallThreshold() { return l0QueueStallThreshold; }
    public int getL1FileCountTrigger() { return l1FileCountTrigger; }

    // Store behavior getters
    public String getColumnFamilyName() { return columnFamilyName; }
    public long getDefaultTtlSeconds() { return defaultTtlSeconds; }

    public static class Builder {
        // Database defaults tuned for Kafka Streams
        private int numFlushThreads = 2;
        private int numCompactionThreads = 2;
        private LogLevel logLevel = LogLevel.INFO;
        private long blockCacheSize = 64 * 1024 * 1024; // 64MB
        private long maxOpenSSTables = 256;
        private long maxMemoryUsage = 0; // auto
        private boolean logToFile = false;
        private long logTruncationAt = 24 * 1024 * 1024; // 24MB

        // Column family defaults
        private long writeBufferSize = 64 * 1024 * 1024; // 64MB
        private CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.LZ4_COMPRESSION;
        private boolean enableBloomFilter = true;
        private double bloomFPR = 0.01;
        private boolean enableBlockIndexes = true;
        private int indexSampleRatio = 1;
        private int blockIndexPrefixLen = 16;
        private SyncMode syncMode = SyncMode.SYNC_NONE;
        private long syncIntervalUs = 128000;
        private boolean useBtree = false;
        private int minLevels = 5;
        private long levelSizeRatio = 10;
        private int skipListMaxLevel = 12;
        private float skipListProbability = 0.25f;
        private IsolationLevel defaultIsolationLevel = IsolationLevel.READ_COMMITTED;
        private long klogValueThreshold = 512;
        private int l0QueueStallThreshold = 20;
        private int l1FileCountTrigger = 4;

        // Store behavior
        private String columnFamilyName = "default";
        private long defaultTtlSeconds = -1; // no TTL

        public Builder numFlushThreads(int n) { this.numFlushThreads = n; return this; }
        public Builder numCompactionThreads(int n) { this.numCompactionThreads = n; return this; }
        public Builder logLevel(LogLevel level) { this.logLevel = level; return this; }
        public Builder blockCacheSize(long bytes) { this.blockCacheSize = bytes; return this; }
        public Builder maxOpenSSTables(long n) { this.maxOpenSSTables = n; return this; }
        public Builder maxMemoryUsage(long bytes) { this.maxMemoryUsage = bytes; return this; }
        public Builder logToFile(boolean enable) { this.logToFile = enable; return this; }
        public Builder logTruncationAt(long bytes) { this.logTruncationAt = bytes; return this; }

        public Builder writeBufferSize(long bytes) { this.writeBufferSize = bytes; return this; }
        public Builder compressionAlgorithm(CompressionAlgorithm algo) { this.compressionAlgorithm = algo; return this; }
        public Builder enableBloomFilter(boolean enable) { this.enableBloomFilter = enable; return this; }
        public Builder bloomFPR(double fpr) { this.bloomFPR = fpr; return this; }
        public Builder enableBlockIndexes(boolean enable) { this.enableBlockIndexes = enable; return this; }
        public Builder indexSampleRatio(int ratio) { this.indexSampleRatio = ratio; return this; }
        public Builder blockIndexPrefixLen(int len) { this.blockIndexPrefixLen = len; return this; }
        public Builder syncMode(SyncMode mode) { this.syncMode = mode; return this; }
        public Builder syncIntervalUs(long us) { this.syncIntervalUs = us; return this; }
        public Builder useBtree(boolean enable) { this.useBtree = enable; return this; }
        public Builder minLevels(int n) { this.minLevels = n; return this; }
        public Builder levelSizeRatio(long ratio) { this.levelSizeRatio = ratio; return this; }
        public Builder skipListMaxLevel(int n) { this.skipListMaxLevel = n; return this; }
        public Builder skipListProbability(float p) { this.skipListProbability = p; return this; }
        public Builder defaultIsolationLevel(IsolationLevel level) { this.defaultIsolationLevel = level; return this; }
        public Builder klogValueThreshold(long bytes) { this.klogValueThreshold = bytes; return this; }
        public Builder l0QueueStallThreshold(int n) { this.l0QueueStallThreshold = n; return this; }
        public Builder l1FileCountTrigger(int n) { this.l1FileCountTrigger = n; return this; }

        public Builder columnFamilyName(String name) { this.columnFamilyName = name; return this; }
        public Builder defaultTtlSeconds(long seconds) { this.defaultTtlSeconds = seconds; return this; }

        public TidesDBStoreConfig build() {
            return new TidesDBStoreConfig(this);
        }
    }
}
