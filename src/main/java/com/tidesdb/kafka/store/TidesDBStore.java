package com.tidesdb.kafka.store;

import com.tidesdb.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * TidesDB-backed implementation of Kafka Streams KeyValueStore.
 * Provides a drop-in replacement for RocksDB state stores.
 */
public class TidesDBStore implements KeyValueStore<Bytes, byte[]> {
    private static final Logger log = LoggerFactory.getLogger(TidesDBStore.class);

    private final String name;
    private final TidesDBStoreConfig storeConfig;
    private TidesDB db;
    private ColumnFamily columnFamily;
    private boolean open = false;

    // Reusable transaction for single-op read/write to reduce allocation overhead
    private Transaction reusableTxn;

    public TidesDBStore(String name) {
        this(name, TidesDBStoreConfig.defaultConfig());
    }

    public TidesDBStore(String name, TidesDBStoreConfig config) {
        this.name = name;
        this.storeConfig = config != null ? config : TidesDBStoreConfig.defaultConfig();
    }

    @Override
    public void put(Bytes key, byte[] value) {
        validateStoreOpen();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        try {
            if (value == null) {
                // Null value means delete
                delete(key);
                return;
            }

            Transaction txn = acquireReusableTxn();
            try {
                if (storeConfig.getDefaultTtlSeconds() > 0) {
                    long ttl = Instant.now().getEpochSecond() + storeConfig.getDefaultTtlSeconds();
                    txn.put(columnFamily, key.get(), value, ttl);
                } else {
                    txn.put(columnFamily, key.get(), value);
                }
                txn.commit();
            } finally {
                resetReusableTxn(txn);
            }
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to put key-value pair", e);
        }
    }

    /**
     * Put a key-value pair with an explicit TTL in seconds.
     * The entry will expire and be removed after the specified duration.
     */
    public void putWithTtl(Bytes key, byte[] value, long ttlSeconds) {
        validateStoreOpen();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        if (value == null) {
            throw new NullPointerException("Value cannot be null for TTL put");
        }

        try {
            Transaction txn = acquireReusableTxn();
            try {
                long ttl = Instant.now().getEpochSecond() + ttlSeconds;
                txn.put(columnFamily, key.get(), value, ttl);
                txn.commit();
            } finally {
                resetReusableTxn(txn);
            }
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to put key-value pair with TTL", e);
        }
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        validateStoreOpen();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        try (Transaction txn = db.beginTransaction()) {
            byte[] existing = null;
            try {
                existing = txn.get(columnFamily, key.get());
            } catch (TidesDBException e) {
                // Key not found -- treat as null
                if (!e.getMessage().contains("not found")) {
                    throw e;
                }
            }
            if (existing == null) {
                if (storeConfig.getDefaultTtlSeconds() > 0) {
                    long ttl = Instant.now().getEpochSecond() + storeConfig.getDefaultTtlSeconds();
                    txn.put(columnFamily, key.get(), value, ttl);
                } else {
                    txn.put(columnFamily, key.get(), value);
                }
                txn.commit();
                return null;
            }
            return existing;
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to putIfAbsent", e);
        }
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        validateStoreOpen();
        
        try (Transaction txn = db.beginTransaction()) {
            long ttl = -1;
            if (storeConfig.getDefaultTtlSeconds() > 0) {
                ttl = Instant.now().getEpochSecond() + storeConfig.getDefaultTtlSeconds();
            }
            for (KeyValue<Bytes, byte[]> entry : entries) {
                if (entry.key == null) {
                    throw new NullPointerException("Key cannot be null");
                }
                if (entry.value == null) {
                    txn.delete(columnFamily, entry.key.get());
                } else if (ttl > 0) {
                    txn.put(columnFamily, entry.key.get(), entry.value, ttl);
                } else {
                    txn.put(columnFamily, entry.key.get(), entry.value);
                }
            }
            txn.commit();
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to putAll", e);
        }
    }

    /**
     * Write a single-delete tombstone for a key. Same read semantics as {@link #delete(Bytes)},
     * but lets compaction drop the put and tombstone together as soon as both appear in the
     * same merge input. Caller contract: between any two single-deletes on the same key, the
     * key has been put at most once. Use only for insert-once / delete-once workloads. When
     * in doubt, prefer {@link #delete(Bytes)}.
     */
    public void singleDelete(Bytes key) {
        validateStoreOpen();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        try (Transaction txn = db.beginTransaction()) {
            txn.singleDelete(columnFamily, key.get());
            txn.commit();
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to single-delete key", e);
        }
    }

    @Override
    public byte[] delete(Bytes key) {
        validateStoreOpen();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        try (Transaction txn = db.beginTransaction()) {
            byte[] oldValue = null;
            try {
                oldValue = txn.get(columnFamily, key.get());
            } catch (TidesDBException e) {
                if (!e.getMessage().contains("not found")) {
                    throw e;
                }
            }
            if (oldValue != null) {
                txn.delete(columnFamily, key.get());
                txn.commit();
            }
            return oldValue;
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to delete key", e);
        }
    }

    @Override
    public byte[] get(Bytes key) {
        validateStoreOpen();
        if (key == null) {
            return null;
        }

        try {
            Transaction txn = acquireReusableTxn();
            try {
                return txn.get(columnFamily, key.get());
            } finally {
                resetReusableTxn(txn);
            }
        } catch (TidesDBException e) {
            // Key not found returns null
            if (e.getMessage().contains("not found")) {
                return null;
            }
            throw new RuntimeException("Failed to get value", e);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        validateStoreOpen();
        return new TidesDBRangeIterator(db, columnFamily, from, to);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        validateStoreOpen();
        return new TidesDBIteratorWrapper(db, columnFamily);
    }

    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        try {
            Stats stats = columnFamily.getStats();
            return stats.getTotalKeys();
        } catch (TidesDBException e) {
            log.warn("Failed to get approximate entry count", e);
            return 0L;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(org.apache.kafka.streams.processor.ProcessorContext context,
                     org.apache.kafka.streams.processor.StateStore root) {
        // Legacy init -- delegate to new init
    }

    @Override
    public void init(org.apache.kafka.streams.processor.StateStoreContext context,
                     org.apache.kafka.streams.processor.StateStore root) {
        try {
            String stateDir = context.stateDir().getAbsolutePath();
            String dbPath = stateDir + "/" + name;

            log.info("Initializing TidesDB store '{}' at path: {}", name, dbPath);

            // Raise the process open-file ceiling before open, if requested. The engine sizes
            // max_open_sstables to fit the ceiling at open time, so this must precede TidesDB.open().
            if (storeConfig.getRaiseOpenFileLimit() > 0) {
                long ceiling = TidesDB.raiseOpenFileLimit(storeConfig.getRaiseOpenFileLimit());
                log.info("Raised open-file ceiling toward {} for store '{}', effective ceiling: {}",
                         storeConfig.getRaiseOpenFileLimit(), name, ceiling);
            }

            Config.Builder configBuilder = Config.builder(dbPath)
                .numFlushThreads(storeConfig.getNumFlushThreads())
                .numCompactionThreads(storeConfig.getNumCompactionThreads())
                .logLevel(storeConfig.getLogLevel())
                .blockCacheSize(storeConfig.getBlockCacheSize())
                .maxOpenSSTables(storeConfig.getMaxOpenSSTables())
                .maxMemoryUsage(storeConfig.getMaxMemoryUsage())
                .logToFile(storeConfig.isLogToFile())
                .logTruncationAt(storeConfig.getLogTruncationAt())
                .maxConcurrentFlushes(storeConfig.getMaxConcurrentFlushes())
                .unifiedMemtable(storeConfig.isUnifiedMemtable())
                .unifiedMemtableWriteBufferSize(storeConfig.getUnifiedMemtableWriteBufferSize())
                .unifiedMemtableSkipListMaxLevel(storeConfig.getUnifiedMemtableSkipListMaxLevel())
                .unifiedMemtableSkipListProbability(storeConfig.getUnifiedMemtableSkipListProbability())
                .unifiedMemtableSyncMode(storeConfig.getUnifiedMemtableSyncMode())
                .unifiedMemtableSyncIntervalUs(storeConfig.getUnifiedMemtableSyncIntervalUs());

            if (storeConfig.getObjectStoreS3Config() != null) {
                configBuilder.objectStoreS3Config(storeConfig.getObjectStoreS3Config());
            } else if (storeConfig.getObjectStoreFsPath() != null) {
                configBuilder.objectStoreFsPath(storeConfig.getObjectStoreFsPath());
            }
            if (storeConfig.getObjectStoreConfig() != null) {
                configBuilder.objectStoreConfig(storeConfig.getObjectStoreConfig());
            }

            Config config = configBuilder.build();

            this.db = TidesDB.open(config);

            // Create or get column family with full configuration
            String cfName = storeConfig.getColumnFamilyName();
            // Register custom comparator if specified
            String compName = storeConfig.getComparatorName();
            if (compName != null && !compName.isEmpty()) {
                db.registerComparator(compName, null);
            }

            ColumnFamilyConfig.Builder cfBuilder = ColumnFamilyConfig.builder()
                .writeBufferSize(storeConfig.getWriteBufferSize())
                .compressionAlgorithm(storeConfig.getCompressionAlgorithm())
                .enableBloomFilter(storeConfig.isEnableBloomFilter())
                .bloomFPR(storeConfig.getBloomFPR())
                .enableBlockIndexes(storeConfig.isEnableBlockIndexes())
                .indexSampleRatio(storeConfig.getIndexSampleRatio())
                .blockIndexPrefixLen(storeConfig.getBlockIndexPrefixLen())
                .syncMode(storeConfig.getSyncMode())
                .syncIntervalUs(storeConfig.getSyncIntervalUs())
                .useBtree(storeConfig.isUseBtree())
                .minLevels(storeConfig.getMinLevels())
                .levelSizeRatio(storeConfig.getLevelSizeRatio())
                .skipListMaxLevel(storeConfig.getSkipListMaxLevel())
                .skipListProbability(storeConfig.getSkipListProbability())
                .defaultIsolationLevel(storeConfig.getDefaultIsolationLevel())
                .klogValueThreshold(storeConfig.getKlogValueThreshold())
                .l0QueueStallThreshold(storeConfig.getL0QueueStallThreshold())
                .l1FileCountTrigger(storeConfig.getL1FileCountTrigger())
                .dividingLevelOffset(storeConfig.getDividingLevelOffset())
                .minDiskSpace(storeConfig.getMinDiskSpace())
                .tombstoneDensityTrigger(storeConfig.getTombstoneDensityTrigger())
                .tombstoneDensityMinEntries(storeConfig.getTombstoneDensityMinEntries())
                .objectLazyCompaction(storeConfig.isObjectLazyCompaction())
                .objectPrefetchCompaction(storeConfig.isObjectPrefetchCompaction());

            if (compName != null && !compName.isEmpty()) {
                cfBuilder.comparatorName(compName);
            }

            ColumnFamilyConfig cfConfig = cfBuilder.build();

            try {
                this.columnFamily = db.getColumnFamily(cfName);
            } catch (TidesDBException e) {
                // Column family doesn't exist, create it
                db.createColumnFamily(cfName, cfConfig);
                this.columnFamily = db.getColumnFamily(cfName);
            }

            // Initialize reusable transaction
            this.reusableTxn = db.beginTransaction();

            this.open = true;
            log.info("TidesDB store '{}' initialized successfully (compression={}, bloom={}, syncMode={}, btree={})",
                     name, storeConfig.getCompressionAlgorithm(), storeConfig.isEnableBloomFilter(),
                     storeConfig.getSyncMode(), storeConfig.isUseBtree());

        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to initialize TidesDB store", e);
        }
    }

    @Override
    public void flush() {
        if (open) {
            try {
                columnFamily.flushMemtable();
            } catch (TidesDBException e) {
                log.warn("Failed to flush memtable", e);
            }
        }
    }

    @Override
    public void close() {
        if (open) {
            try {
                if (reusableTxn != null) {
                    reusableTxn.free();
                    reusableTxn = null;
                }
                if (db != null) {
                    if (storeConfig.isCancelBackgroundWorkOnClose()) {
                        try {
                            db.cancelBackgroundWork();
                        } catch (TidesDBException e) {
                            log.warn("Failed to cancel background work before close", e);
                        }
                    }
                    db.close();
                }
                open = false;
                log.info("TidesDB store '{}' closed", name);
            } catch (Exception e) {
                log.error("Error closing TidesDB store", e);
            }
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

    private void validateStoreOpen() {
        if (!open) {
            throw new IllegalStateException("Store is not open");
        }
    }

    // ==================== Transaction Reuse ====================

    /**
     * Acquire the reusable transaction, falling back to a new one if unavailable.
     * The reusable transaction avoids repeated allocation overhead for single-op
     * read/write calls (get, put) which are the hot path in Kafka Streams.
     */
    private synchronized Transaction acquireReusableTxn() throws TidesDBException {
        if (reusableTxn != null) {
            Transaction txn = reusableTxn;
            reusableTxn = null;
            return txn;
        }
        return db.beginTransaction();
    }

    /**
     * Return a transaction to the reusable pool after commit/rollback.
     * Uses reset() to retain internal buffers and avoid reallocation.
     */
    private synchronized void resetReusableTxn(Transaction txn) {
        try {
            txn.reset(storeConfig.getDefaultIsolationLevel());
            if (reusableTxn == null) {
                reusableTxn = txn;
            } else {
                txn.free();
            }
        } catch (TidesDBException e) {
            // If reset fails, just free it
            try { txn.free(); } catch (Exception ignored) {}
        }
    }

    // ==================== Extended TidesDB Features ====================

    /**
     * Get column family statistics
     */
    public Stats getStats() throws TidesDBException {
        validateStoreOpen();
        return columnFamily.getStats();
    }

    /**
     * Get database-level statistics
     */
    public DbStats getDbStats() throws TidesDBException {
        validateStoreOpen();
        return db.getDbStats();
    }

    /**
     * Get block cache statistics
     */
    public CacheStats getCacheStats() throws TidesDBException {
        validateStoreOpen();
        return db.getCacheStats();
    }

    /**
     * Manually trigger non-blocking compaction
     */
    public void compact() throws TidesDBException {
        validateStoreOpen();
        columnFamily.compact();
    }

    /**
     * Synchronously compact only SSTables whose key range overlaps {@code [startKey, endKey)}.
     * Bounds I/O to the affected portion of the LSM tree. A null/empty endpoint is unbounded
     * on that side; both null/empty is rejected (use {@link #compact()} for full-CF compaction).
     */
    public void compactRange(byte[] startKey, byte[] endKey) throws TidesDBException {
        validateStoreOpen();
        columnFamily.compactRange(startKey, endKey);
    }

    /**
     * Synchronous flush and aggressive compaction. Blocks until complete.
     * Use before backup, after bulk deletes, or during maintenance windows.
     */
    public void purge() throws TidesDBException {
        validateStoreOpen();
        columnFamily.purge();
    }

    /**
     * Purge all column families and drain flush/compaction queues.
     */
    public void purgeAll() throws TidesDBException {
        validateStoreOpen();
        db.purge();
    }

    /**
     * Force an immediate WAL sync for explicit durability control.
     */
    public void syncWal() throws TidesDBException {
        validateStoreOpen();
        columnFamily.syncWal();
    }

    /**
     * Create an online backup of the database.
     */
    public void backup(String backupPath) throws TidesDBException {
        validateStoreOpen();
        db.backup(backupPath);
    }

    /**
     * Create a lightweight checkpoint using hard links (near-instant, same filesystem).
     */
    public void checkpoint(String checkpointPath) throws TidesDBException {
        validateStoreOpen();
        db.checkpoint(checkpointPath);
    }

    /**
     * Estimate the cost of iterating between two keys. Useful for query planning.
     */
    public double rangeCost(byte[] fromKey, byte[] toKey) throws TidesDBException {
        validateStoreOpen();
        return columnFamily.rangeCost(fromKey, toKey);
    }

    /**
     * Register a commit hook for change data capture.
     * The hook fires synchronously after every transaction commit.
     */
    public void setCommitHook(CommitHook hook) throws TidesDBException {
        validateStoreOpen();
        columnFamily.setCommitHook(hook);
    }

    /**
     * Remove the commit hook.
     */
    public void clearCommitHook() throws TidesDBException {
        validateStoreOpen();
        columnFamily.clearCommitHook();
    }

    /**
     * Update runtime-safe column family configuration without restart.
     */
    public void updateRuntimeConfig(ColumnFamilyConfig newConfig, boolean apply) throws TidesDBException {
        validateStoreOpen();
        columnFamily.updateRuntimeConfig(newConfig, apply);
    }

    /**
     * Cancel background compaction db-wide. In-flight merges bail safely at their next
     * checkpoint and queued compaction is skipped; flushes are unaffected so durability is
     * preserved. Sticky for the session (reset on next open). Intended to be called right
     * before {@link #close()} for a fast shutdown; set
     * {@link TidesDBStoreConfig.Builder#cancelBackgroundWorkOnClose(boolean)} to have close
     * invoke it automatically.
     */
    public void cancelBackgroundWork() throws TidesDBException {
        validateStoreOpen();
        db.cancelBackgroundWork();
    }

    /**
     * Raise this process's open-file ceiling toward {@code desired} descriptors so a database
     * can keep more sstables open. Process-wide and opt-in; must be called BEFORE the store is
     * opened to take effect (the engine sizes max_open_sstables to fit the ceiling at open). A
     * failed or partial raise is non-fatal. Prefer
     * {@link TidesDBStoreConfig.Builder#raiseOpenFileLimit(long)} to apply it during init.
     * @param desired target descriptor count; {@code <= 0} just reports the current ceiling
     * @return the open-file ceiling in effect after the attempt
     */
    public static long raiseOpenFileLimit(long desired) {
        return TidesDB.raiseOpenFileLimit(desired);
    }

    /**
     * Reports whether the native TidesDB library was built with S3 object store support
     * ({@code TIDESDB_WITH_S3=ON}). When false, configuring an {@link com.tidesdb.S3Config} via
     * {@link TidesDBStoreConfig.Builder#objectStoreS3Config} and opening the store throws.
     * @return true if an S3-compatible backend can be used, false otherwise
     */
    public static boolean isS3Available() {
        return TidesDB.isS3Available();
    }

    /**
     * Check if the column family is currently flushing.
     */
    public boolean isFlushing() throws TidesDBException {
        validateStoreOpen();
        return columnFamily.isFlushing();
    }

    /**
     * Check if the column family is currently compacting.
     */
    public boolean isCompacting() throws TidesDBException {
        validateStoreOpen();
        return columnFamily.isCompacting();
    }

    // ==================== Column Family Management ====================

    /**
     * Create an additional column family in the database. The store's own column family is
     * created automatically during {@link #init}; use this to manage extra column families that
     * share the same database instance.
     */
    public void createColumnFamily(String cfName, ColumnFamilyConfig cfConfig) throws TidesDBException {
        validateStoreOpen();
        db.createColumnFamily(cfName, cfConfig);
    }

    /**
     * Fetch a column family handle by name. Use {@link #getColumnFamily()} for the store's own
     * column family.
     */
    public ColumnFamily getColumnFamily(String cfName) throws TidesDBException {
        validateStoreOpen();
        return db.getColumnFamily(cfName);
    }

    /**
     * List all column family names in the database.
     */
    public String[] listColumnFamilies() throws TidesDBException {
        validateStoreOpen();
        return db.listColumnFamilies();
    }

    /**
     * Rename a column family atomically.
     * Waits for any in-progress flush/compaction to complete before renaming.
     */
    public void renameColumnFamily(String oldName, String newName) throws TidesDBException {
        validateStoreOpen();
        db.renameColumnFamily(oldName, newName);
    }

    /**
     * Clone an existing column family to a new name.
     * The clone is completely independent of the source.
     */
    public void cloneColumnFamily(String srcName, String dstName) throws TidesDBException {
        validateStoreOpen();
        db.cloneColumnFamily(srcName, dstName);
    }

    /**
     * Drop a column family by name.
     */
    public void dropColumnFamily(String cfName) throws TidesDBException {
        validateStoreOpen();
        db.dropColumnFamily(cfName);
    }

    /**
     * Delete a column family using its handle. Alternative to {@link #dropColumnFamily(String)}.
     */
    public void deleteColumnFamily(ColumnFamily cf) throws TidesDBException {
        validateStoreOpen();
        db.deleteColumnFamily(cf);
    }

    // ==================== Comparator Operations ====================

    /**
     * Register a custom comparator for use with column families.
     * Built-in comparators: "memcmp", "lexicographic", "uint64", "int64", "reverse", "case_insensitive".
     */
    public void registerComparator(String name, String ctx) throws TidesDBException {
        validateStoreOpen();
        db.registerComparator(name, ctx);
    }

    // ==================== Replica Operations ====================

    /**
     * Switch a read-only replica database to primary mode.
     */
    public void promoteToPrimary() throws TidesDBException {
        validateStoreOpen();
        db.promoteToPrimary();
    }

    /**
     * Get the store configuration.
     */
    public TidesDBStoreConfig getStoreConfig() {
        return storeConfig;
    }

    /**
     * Get direct access to the underlying TidesDB instance.
     * Use with caution -- intended for advanced operations.
     */
    public TidesDB getDb() {
        validateStoreOpen();
        return db;
    }

    /**
     * Get direct access to the column family.
     * Use with caution -- intended for advanced operations.
     */
    public ColumnFamily getColumnFamily() {
        validateStoreOpen();
        return columnFamily;
    }
}
