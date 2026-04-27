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
    private final int maxConcurrentFlushes;

    // Unified memtable settings
    private final boolean unifiedMemtable;
    private final long unifiedMemtableWriteBufferSize;
    private final int unifiedMemtableSkipListMaxLevel;
    private final float unifiedMemtableSkipListProbability;
    private final int unifiedMemtableSyncMode;
    private final long unifiedMemtableSyncIntervalUs;

    // Object store settings
    private final String objectStoreFsPath;
    private final ObjectStoreConfig objectStoreConfig;

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
    private final int dividingLevelOffset;
    private final long minDiskSpace;
    private final String comparatorName;
    private final double tombstoneDensityTrigger;
    private final long tombstoneDensityMinEntries;
    private final boolean objectLazyCompaction;
    private final boolean objectPrefetchCompaction;

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
        this.maxConcurrentFlushes = builder.maxConcurrentFlushes;
        this.unifiedMemtable = builder.unifiedMemtable;
        this.unifiedMemtableWriteBufferSize = builder.unifiedMemtableWriteBufferSize;
        this.unifiedMemtableSkipListMaxLevel = builder.unifiedMemtableSkipListMaxLevel;
        this.unifiedMemtableSkipListProbability = builder.unifiedMemtableSkipListProbability;
        this.unifiedMemtableSyncMode = builder.unifiedMemtableSyncMode;
        this.unifiedMemtableSyncIntervalUs = builder.unifiedMemtableSyncIntervalUs;
        this.objectStoreFsPath = builder.objectStoreFsPath;
        this.objectStoreConfig = builder.objectStoreConfig;
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
        this.dividingLevelOffset = builder.dividingLevelOffset;
        this.minDiskSpace = builder.minDiskSpace;
        this.comparatorName = builder.comparatorName;
        this.tombstoneDensityTrigger = builder.tombstoneDensityTrigger;
        this.tombstoneDensityMinEntries = builder.tombstoneDensityMinEntries;
        this.objectLazyCompaction = builder.objectLazyCompaction;
        this.objectPrefetchCompaction = builder.objectPrefetchCompaction;
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
    public int getMaxConcurrentFlushes() { return maxConcurrentFlushes; }

    // Unified memtable getters
    public boolean isUnifiedMemtable() { return unifiedMemtable; }
    public long getUnifiedMemtableWriteBufferSize() { return unifiedMemtableWriteBufferSize; }
    public int getUnifiedMemtableSkipListMaxLevel() { return unifiedMemtableSkipListMaxLevel; }
    public float getUnifiedMemtableSkipListProbability() { return unifiedMemtableSkipListProbability; }
    public int getUnifiedMemtableSyncMode() { return unifiedMemtableSyncMode; }
    public long getUnifiedMemtableSyncIntervalUs() { return unifiedMemtableSyncIntervalUs; }

    // Object store getters
    public String getObjectStoreFsPath() { return objectStoreFsPath; }
    public ObjectStoreConfig getObjectStoreConfig() { return objectStoreConfig; }

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
    public int getDividingLevelOffset() { return dividingLevelOffset; }
    public long getMinDiskSpace() { return minDiskSpace; }
    public String getComparatorName() { return comparatorName; }
    public double getTombstoneDensityTrigger() { return tombstoneDensityTrigger; }
    public long getTombstoneDensityMinEntries() { return tombstoneDensityMinEntries; }
    public boolean isObjectLazyCompaction() { return objectLazyCompaction; }
    public boolean isObjectPrefetchCompaction() { return objectPrefetchCompaction; }

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
        private int maxConcurrentFlushes = 0; // 0 = library default

        // Unified memtable defaults
        private boolean unifiedMemtable = false;
        private long unifiedMemtableWriteBufferSize = 0; // auto
        private int unifiedMemtableSkipListMaxLevel = 0; // default 12
        private float unifiedMemtableSkipListProbability = 0; // default 0.25
        private int unifiedMemtableSyncMode = 0; // SYNC_NONE
        private long unifiedMemtableSyncIntervalUs = 0; // default

        // Object store defaults
        private String objectStoreFsPath = null; // null = local only
        private ObjectStoreConfig objectStoreConfig = null; // null = use defaults

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
        private int dividingLevelOffset = 2;
        private long minDiskSpace = 100 * 1024 * 1024; // 100MB
        private String comparatorName = ""; // empty = default memcmp
        private double tombstoneDensityTrigger = 0.0; // 0.0 = disabled
        private long tombstoneDensityMinEntries = 1024;
        private boolean objectLazyCompaction = false;
        private boolean objectPrefetchCompaction = true;

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
        public Builder maxConcurrentFlushes(int n) { this.maxConcurrentFlushes = n; return this; }

        public Builder unifiedMemtable(boolean enable) { this.unifiedMemtable = enable; return this; }
        public Builder unifiedMemtableWriteBufferSize(long bytes) { this.unifiedMemtableWriteBufferSize = bytes; return this; }
        public Builder unifiedMemtableSkipListMaxLevel(int n) { this.unifiedMemtableSkipListMaxLevel = n; return this; }
        public Builder unifiedMemtableSkipListProbability(float p) { this.unifiedMemtableSkipListProbability = p; return this; }
        public Builder unifiedMemtableSyncMode(int mode) { this.unifiedMemtableSyncMode = mode; return this; }
        public Builder unifiedMemtableSyncIntervalUs(long us) { this.unifiedMemtableSyncIntervalUs = us; return this; }

        public Builder objectStoreFsPath(String path) { this.objectStoreFsPath = path; return this; }
        public Builder objectStoreConfig(ObjectStoreConfig config) { this.objectStoreConfig = config; return this; }

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
        public Builder dividingLevelOffset(int n) { this.dividingLevelOffset = n; return this; }
        public Builder minDiskSpace(long bytes) { this.minDiskSpace = bytes; return this; }
        public Builder comparatorName(String name) { this.comparatorName = name; return this; }
        public Builder tombstoneDensityTrigger(double trigger) { this.tombstoneDensityTrigger = trigger; return this; }
        public Builder tombstoneDensityMinEntries(long entries) { this.tombstoneDensityMinEntries = entries; return this; }
        public Builder objectLazyCompaction(boolean enable) { this.objectLazyCompaction = enable; return this; }
        public Builder objectPrefetchCompaction(boolean enable) { this.objectPrefetchCompaction = enable; return this; }

        public Builder columnFamilyName(String name) { this.columnFamilyName = name; return this; }
        public Builder defaultTtlSeconds(long seconds) { this.defaultTtlSeconds = seconds; return this; }

        public TidesDBStoreConfig build() {
            return new TidesDBStoreConfig(this);
        }
    }
}
