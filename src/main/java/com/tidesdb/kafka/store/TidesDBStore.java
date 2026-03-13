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

            Config config = Config.builder(dbPath)
                .numFlushThreads(storeConfig.getNumFlushThreads())
                .numCompactionThreads(storeConfig.getNumCompactionThreads())
                .logLevel(storeConfig.getLogLevel())
                .blockCacheSize(storeConfig.getBlockCacheSize())
                .maxOpenSSTables(storeConfig.getMaxOpenSSTables())
                .maxMemoryUsage(storeConfig.getMaxMemoryUsage())
                .logToFile(storeConfig.isLogToFile())
                .logTruncationAt(storeConfig.getLogTruncationAt())
                .build();

            this.db = TidesDB.open(config);

            // Create or get column family with full configuration
            String cfName = storeConfig.getColumnFamilyName();
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
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
                .build();

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
