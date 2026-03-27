package com.tidesdb.kafka.store;

import com.tidesdb.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for TidesDBStore
 */
class TidesDBStoreTest {

    @TempDir
    File tempDir;

    private TidesDBStore store;
    private StateStoreContext context;

    @BeforeEach
    void setUp() {
        store = new TidesDBStore("test-store");
        context = mock(StateStoreContext.class);
        when(context.stateDir()).thenReturn(tempDir);
        
        store.init(context, store);
    }

    @AfterEach
    void tearDown() {
        if (store != null && store.isOpen()) {
            store.close();
        }
    }

    @Test
    @DisplayName("Should initialize store successfully")
    void testInitialization() {
        assertThat(store.isOpen()).isTrue();
        assertThat(store.name()).isEqualTo("test-store");
        assertThat(store.persistent()).isTrue();
    }

    @Test
    @DisplayName("Should put and get a value")
    void testPutAndGet() {
        Bytes key = Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8));
        byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

        store.put(key, value);
        byte[] retrieved = store.get(key);

        assertThat(retrieved).isEqualTo(value);
    }

    @Test
    @DisplayName("Should return null for non-existent key")
    void testGetNonExistent() {
        Bytes key = Bytes.wrap("nonexistent".getBytes(StandardCharsets.UTF_8));
        byte[] value = store.get(key);

        assertThat(value).isNull();
    }

    @Test
    @DisplayName("Should delete a key")
    void testDelete() {
        Bytes key = Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8));
        byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

        store.put(key, value);
        assertThat(store.get(key)).isNotNull();

        byte[] deleted = store.delete(key);
        assertThat(deleted).isEqualTo(value);
        assertThat(store.get(key)).isNull();
    }

    @Test
    @DisplayName("Should delete with null value in put")
    void testPutNullValue() {
        Bytes key = Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8));
        byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

        store.put(key, value);
        assertThat(store.get(key)).isNotNull();

        store.put(key, null);
        assertThat(store.get(key)).isNull();
    }

    @Test
    @DisplayName("Should handle putIfAbsent correctly")
    void testPutIfAbsent() {
        Bytes key = Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8));
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);

        // First put should succeed
        byte[] existing = store.putIfAbsent(key, value1);
        assertThat(existing).isNull();
        assertThat(store.get(key)).isEqualTo(value1);

        // Second put should fail and return existing value
        existing = store.putIfAbsent(key, value2);
        assertThat(existing).isEqualTo(value1);
        assertThat(store.get(key)).isEqualTo(value1);
    }

    @Test
    @DisplayName("Should handle putAll correctly")
    void testPutAll() {
        List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(KeyValue.pair(
            Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            "value1".getBytes(StandardCharsets.UTF_8)
        ));
        entries.add(KeyValue.pair(
            Bytes.wrap("key2".getBytes(StandardCharsets.UTF_8)),
            "value2".getBytes(StandardCharsets.UTF_8)
        ));
        entries.add(KeyValue.pair(
            Bytes.wrap("key3".getBytes(StandardCharsets.UTF_8)),
            "value3".getBytes(StandardCharsets.UTF_8)
        ));

        store.putAll(entries);

        assertThat(new String(store.get(Bytes.wrap("key1".getBytes())), StandardCharsets.UTF_8))
            .isEqualTo("value1");
        assertThat(new String(store.get(Bytes.wrap("key2".getBytes())), StandardCharsets.UTF_8))
            .isEqualTo("value2");
        assertThat(new String(store.get(Bytes.wrap("key3".getBytes())), StandardCharsets.UTF_8))
            .isEqualTo("value3");
    }

    @Test
    @DisplayName("Should iterate over all entries")
    void testAllIterator() {
        // Insert test data
        store.put(Bytes.wrap("a".getBytes()), "value_a".getBytes());
        store.put(Bytes.wrap("b".getBytes()), "value_b".getBytes());
        store.put(Bytes.wrap("c".getBytes()), "value_c".getBytes());

        List<KeyValue<String, String>> results = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<Bytes, byte[]> kv = iterator.next();
                results.add(KeyValue.pair(
                    new String(kv.key.get(), StandardCharsets.UTF_8),
                    new String(kv.value, StandardCharsets.UTF_8)
                ));
            }
        }

        assertThat(results).hasSize(3);
        assertThat(results).extracting(kv -> kv.key)
            .containsExactlyInAnyOrder("a", "b", "c");
    }

    @Test
    @DisplayName("Should iterate over range of entries")
    void testRangeIterator() {
        // Insert test data
        store.put(Bytes.wrap("a".getBytes()), "value_a".getBytes());
        store.put(Bytes.wrap("b".getBytes()), "value_b".getBytes());
        store.put(Bytes.wrap("c".getBytes()), "value_c".getBytes());
        store.put(Bytes.wrap("d".getBytes()), "value_d".getBytes());
        store.put(Bytes.wrap("e".getBytes()), "value_e".getBytes());

        List<String> keys = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> iterator = store.range(
                Bytes.wrap("b".getBytes()),
                Bytes.wrap("d".getBytes())
        )) {
            while (iterator.hasNext()) {
                KeyValue<Bytes, byte[]> kv = iterator.next();
                keys.add(new String(kv.key.get(), StandardCharsets.UTF_8));
            }
        }

        assertThat(keys).containsExactly("b", "c", "d");
    }

    @Test
    @DisplayName("Should return approximate number of entries")
    void testApproximateNumEntries() {
        assertThat(store.approximateNumEntries()).isEqualTo(0);

        store.put(Bytes.wrap("key1".getBytes()), "value1".getBytes());
        store.put(Bytes.wrap("key2".getBytes()), "value2".getBytes());
        store.put(Bytes.wrap("key3".getBytes()), "value3".getBytes());

        // After flush, stats should reflect the entries
        store.flush();
        assertThat(store.approximateNumEntries()).isGreaterThanOrEqualTo(3);
    }

    @Test
    @DisplayName("Should handle large values")
    void testLargeValues() {
        Bytes key = Bytes.wrap("large".getBytes());
        byte[] largeValue = new byte[1024 * 1024]; // 1MB
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        store.put(key, largeValue);
        byte[] retrieved = store.get(key);

        assertThat(retrieved).isEqualTo(largeValue);
    }

    @Test
    @DisplayName("Should handle many keys")
    void testManyKeys() {
        int numKeys = 10000;

        // Insert many keys
        for (int i = 0; i < numKeys; i++) {
            String keyStr = String.format("key_%06d", i);
            String valueStr = String.format("value_%06d", i);
            store.put(
                Bytes.wrap(keyStr.getBytes()),
                valueStr.getBytes()
            );
        }

        // Verify random keys
        for (int i = 0; i < 100; i++) {
            int idx = (int) (Math.random() * numKeys);
            String keyStr = String.format("key_%06d", idx);
            String expectedValue = String.format("value_%06d", idx);
            
            byte[] value = store.get(Bytes.wrap(keyStr.getBytes()));
            assertThat(new String(value)).isEqualTo(expectedValue);
        }

        store.flush();
        assertThat(store.approximateNumEntries()).isGreaterThanOrEqualTo(numKeys);
    }

    @Test
    @DisplayName("Should throw exception when accessing closed store")
    void testAccessClosedStore() {
        store.close();

        assertThatThrownBy(() -> store.put(
            Bytes.wrap("key".getBytes()),
            "value".getBytes()
        )).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("not open");
    }

    @Test
    @DisplayName("Should throw exception for null key")
    void testNullKey() {
        assertThatThrownBy(() -> store.put(null, "value".getBytes()))
            .isInstanceOf(NullPointerException.class);

        assertThatCode(() -> store.get(null))
            .doesNotThrowAnyException(); // get returns null for null key

        assertThatThrownBy(() -> store.delete(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should handle flush operation")
    void testFlush() {
        store.put(Bytes.wrap("key1".getBytes()), "value1".getBytes());
        store.put(Bytes.wrap("key2".getBytes()), "value2".getBytes());

        assertThatCode(() -> store.flush()).doesNotThrowAnyException();

        // Data should still be accessible after flush
        assertThat(store.get(Bytes.wrap("key1".getBytes()))).isNotNull();
        assertThat(store.get(Bytes.wrap("key2".getBytes()))).isNotNull();
    }

    @Test
    @DisplayName("Should update existing key")
    void testUpdateExistingKey() {
        Bytes key = Bytes.wrap("key1".getBytes());
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        store.put(key, value1);
        assertThat(store.get(key)).isEqualTo(value1);

        store.put(key, value2);
        assertThat(store.get(key)).isEqualTo(value2);
    }

    @Test
    @DisplayName("Should handle empty iterator")
    void testEmptyIterator() {
        try (KeyValueIterator<Bytes, byte[]> iterator = store.all()) {
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    // ==================== New Feature Tests ====================

    @Test
    @DisplayName("Should create store with custom config")
    void testCustomConfig() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .compressionAlgorithm(CompressionAlgorithm.ZSTD_COMPRESSION)
            .enableBloomFilter(true)
            .bloomFPR(0.001)
            .syncMode(SyncMode.SYNC_NONE)
            .writeBufferSize(32 * 1024 * 1024)
            .enableBlockIndexes(true)
            .build();

        TidesDBStore customStore = new TidesDBStore("custom-store", config);
        StateStoreContext customCtx = mock(StateStoreContext.class);
        File customDir = new File(tempDir, "custom");
        customDir.mkdirs();
        when(customCtx.stateDir()).thenReturn(customDir);

        customStore.init(customCtx, customStore);
        assertThat(customStore.isOpen()).isTrue();
        assertThat(customStore.getStoreConfig().getCompressionAlgorithm())
            .isEqualTo(CompressionAlgorithm.ZSTD_COMPRESSION);
        assertThat(customStore.getStoreConfig().getSyncMode())
            .isEqualTo(SyncMode.SYNC_NONE);

        // Verify basic operations work with custom config
        customStore.put(Bytes.wrap("k1".getBytes()), "v1".getBytes());
        assertThat(customStore.get(Bytes.wrap("k1".getBytes()))).isEqualTo("v1".getBytes());

        customStore.close();
    }

    @Test
    @DisplayName("Should put with explicit TTL")
    void testPutWithTtl() {
        Bytes key = Bytes.wrap("ttl-key".getBytes(StandardCharsets.UTF_8));
        byte[] value = "ttl-value".getBytes(StandardCharsets.UTF_8);

        // Put with long TTL (should be readable immediately)
        store.putWithTtl(key, value, 3600);
        byte[] retrieved = store.get(key);
        assertThat(retrieved).isEqualTo(value);
    }

    @Test
    @DisplayName("Should throw for null value in putWithTtl")
    void testPutWithTtlNullValue() {
        Bytes key = Bytes.wrap("key".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> store.putWithTtl(key, null, 60))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should throw for null key in putWithTtl")
    void testPutWithTtlNullKey() {
        assertThatThrownBy(() -> store.putWithTtl(null, "v".getBytes(), 60))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should create store with default TTL config")
    void testDefaultTtlConfig() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .defaultTtlSeconds(3600)
            .build();

        TidesDBStore ttlStore = new TidesDBStore("ttl-store", config);
        StateStoreContext ttlCtx = mock(StateStoreContext.class);
        File ttlDir = new File(tempDir, "ttl");
        ttlDir.mkdirs();
        when(ttlCtx.stateDir()).thenReturn(ttlDir);

        ttlStore.init(ttlCtx, ttlStore);
        assertThat(ttlStore.getStoreConfig().getDefaultTtlSeconds()).isEqualTo(3600);

        // Put should apply TTL automatically
        ttlStore.put(Bytes.wrap("k".getBytes()), "v".getBytes());
        assertThat(ttlStore.get(Bytes.wrap("k".getBytes()))).isEqualTo("v".getBytes());

        ttlStore.close();
    }

    @Test
    @DisplayName("Should build store via TidesDBStoreBuilder with config")
    void testStoreBuilder() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .compressionAlgorithm(CompressionAlgorithm.LZ4_FAST_COMPRESSION)
            .syncMode(SyncMode.SYNC_NONE)
            .build();

        TidesDBStoreBuilder builder = TidesDBStoreBuilder.create("builder-store", config);
        TidesDBStore builtStore = builder.build();
        assertThat(builtStore.name()).isEqualTo("builder-store");
        assertThat(builtStore.getStoreConfig().getCompressionAlgorithm())
            .isEqualTo(CompressionAlgorithm.LZ4_FAST_COMPRESSION);
    }

    @Test
    @DisplayName("Should build store via TidesDBStoreSupplier with config")
    void testStoreSupplier() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .enableBloomFilter(false)
            .build();

        TidesDBStoreSupplier supplier = TidesDBStoreSupplier.create("supplier-store", config);
        assertThat(supplier.name()).isEqualTo("supplier-store");
        assertThat(supplier.metricsScope()).isEqualTo("tidesdb");

        TidesDBStore suppliedStore = (TidesDBStore) supplier.get();
        assertThat(suppliedStore.getStoreConfig().isEnableBloomFilter()).isFalse();
    }

    @Test
    @DisplayName("Should expose column family stats")
    void testGetStats() throws Exception {
        store.put(Bytes.wrap("s1".getBytes()), "v1".getBytes());
        store.put(Bytes.wrap("s2".getBytes()), "v2".getBytes());
        store.flush();

        Stats stats = store.getStats();
        assertThat(stats).isNotNull();
        assertThat(stats.getTotalKeys()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("Should expose database-level stats")
    void testGetDbStats() throws Exception {
        DbStats dbStats = store.getDbStats();
        assertThat(dbStats).isNotNull();
        assertThat(dbStats.getNumColumnFamilies()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("Should expose cache stats")
    void testGetCacheStats() throws Exception {
        CacheStats cacheStats = store.getCacheStats();
        assertThat(cacheStats).isNotNull();
    }

    @Test
    @DisplayName("Should compact without error")
    void testCompact() {
        store.put(Bytes.wrap("c1".getBytes()), "v1".getBytes());
        store.flush();
        assertThatCode(() -> store.compact()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should purge without error")
    void testPurge() {
        store.put(Bytes.wrap("p1".getBytes()), "v1".getBytes());
        assertThatCode(() -> store.purge()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should sync WAL without error")
    void testSyncWal() {
        store.put(Bytes.wrap("w1".getBytes()), "v1".getBytes());
        assertThatCode(() -> store.syncWal()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should report flushing and compacting status")
    void testFlushingCompactingStatus() throws Exception {
        // Just verify the methods don't throw
        assertThat(store.isFlushing()).isIn(true, false);
        assertThat(store.isCompacting()).isIn(true, false);
    }

    @Test
    @DisplayName("Should get store config")
    void testGetStoreConfig() {
        TidesDBStoreConfig config = store.getStoreConfig();
        assertThat(config).isNotNull();
        assertThat(config.getColumnFamilyName()).isEqualTo("default");
    }

    @Test
    @DisplayName("Should create backup")
    void testBackup() {
        store.put(Bytes.wrap("bk1".getBytes()), "v1".getBytes());
        File backupDir = new File(tempDir, "backup-" + System.nanoTime());
        assertThatCode(() -> store.backup(backupDir.getAbsolutePath()))
            .doesNotThrowAnyException();
        assertThat(backupDir).exists();
    }

    @Test
    @DisplayName("Should create checkpoint")
    void testCheckpoint() {
        store.put(Bytes.wrap("cp1".getBytes()), "v1".getBytes());
        File cpDir = new File(tempDir, "checkpoint-" + System.nanoTime());
        assertThatCode(() -> store.checkpoint(cpDir.getAbsolutePath()))
            .doesNotThrowAnyException();
        assertThat(cpDir).exists();
    }

    @Test
    @DisplayName("Should create store with B+tree klog format")
    void testBtreeKlogFormat() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .useBtree(true)
            .build();

        TidesDBStore btreeStore = new TidesDBStore("btree-store", config);
        StateStoreContext btreeCtx = mock(StateStoreContext.class);
        File btreeDir = new File(tempDir, "btree");
        btreeDir.mkdirs();
        when(btreeCtx.stateDir()).thenReturn(btreeDir);

        btreeStore.init(btreeCtx, btreeStore);
        assertThat(btreeStore.isOpen()).isTrue();

        // Write and read with B+tree format
        btreeStore.put(Bytes.wrap("bt1".getBytes()), "v1".getBytes());
        assertThat(btreeStore.get(Bytes.wrap("bt1".getBytes()))).isEqualTo("v1".getBytes());

        btreeStore.close();
    }

    @Test
    @DisplayName("Should expose direct DB and CF access")
    void testDirectAccess() {
        assertThat(store.getDb()).isNotNull();
        assertThat(store.getColumnFamily()).isNotNull();
    }

    @Test
    @DisplayName("Should handle delete of non-existent key gracefully")
    void testDeleteNonExistent() {
        Bytes key = Bytes.wrap("never-existed".getBytes(StandardCharsets.UTF_8));
        byte[] result = store.delete(key);
        assertThat(result).isNull();
    }

    // ==================== Unified Memtable Config Tests ====================

    @Test
    @DisplayName("Should create store with unified memtable config")
    void testUnifiedMemtableConfig() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .unifiedMemtable(true)
            .unifiedMemtableWriteBufferSize(32 * 1024 * 1024)
            .unifiedMemtableSkipListMaxLevel(16)
            .unifiedMemtableSkipListProbability(0.5f)
            .unifiedMemtableSyncMode(0)
            .unifiedMemtableSyncIntervalUs(0)
            .build();

        TidesDBStore uniStore = new TidesDBStore("unified-store", config);
        StateStoreContext uniCtx = mock(StateStoreContext.class);
        File uniDir = new File(tempDir, "unified");
        uniDir.mkdirs();
        when(uniCtx.stateDir()).thenReturn(uniDir);

        uniStore.init(uniCtx, uniStore);
        assertThat(uniStore.isOpen()).isTrue();
        assertThat(uniStore.getStoreConfig().isUnifiedMemtable()).isTrue();
        assertThat(uniStore.getStoreConfig().getUnifiedMemtableWriteBufferSize())
            .isEqualTo(32 * 1024 * 1024);

        // Verify basic operations work with unified memtable
        uniStore.put(Bytes.wrap("uk1".getBytes()), "uv1".getBytes());
        assertThat(uniStore.get(Bytes.wrap("uk1".getBytes()))).isEqualTo("uv1".getBytes());

        uniStore.close();
    }

    @Test
    @DisplayName("Should expose unified memtable config getters")
    void testUnifiedMemtableConfigGetters() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .unifiedMemtable(false)
            .unifiedMemtableWriteBufferSize(0)
            .unifiedMemtableSkipListMaxLevel(0)
            .unifiedMemtableSkipListProbability(0)
            .unifiedMemtableSyncMode(0)
            .unifiedMemtableSyncIntervalUs(0)
            .build();

        assertThat(config.isUnifiedMemtable()).isFalse();
        assertThat(config.getUnifiedMemtableWriteBufferSize()).isEqualTo(0);
        assertThat(config.getUnifiedMemtableSkipListMaxLevel()).isEqualTo(0);
        assertThat(config.getUnifiedMemtableSkipListProbability()).isEqualTo(0);
        assertThat(config.getUnifiedMemtableSyncMode()).isEqualTo(0);
        assertThat(config.getUnifiedMemtableSyncIntervalUs()).isEqualTo(0);
    }

    // ==================== New CF Config Field Tests ====================

    @Test
    @DisplayName("Should create store with dividingLevelOffset and minDiskSpace")
    void testNewCfConfigFields() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .dividingLevelOffset(3)
            .minDiskSpace(200 * 1024 * 1024)
            .build();

        TidesDBStore cfStore = new TidesDBStore("cf-config-store", config);
        StateStoreContext cfCtx = mock(StateStoreContext.class);
        File cfDir = new File(tempDir, "cfconfig");
        cfDir.mkdirs();
        when(cfCtx.stateDir()).thenReturn(cfDir);

        cfStore.init(cfCtx, cfStore);
        assertThat(cfStore.isOpen()).isTrue();
        assertThat(cfStore.getStoreConfig().getDividingLevelOffset()).isEqualTo(3);
        assertThat(cfStore.getStoreConfig().getMinDiskSpace()).isEqualTo(200 * 1024 * 1024);

        cfStore.put(Bytes.wrap("dk1".getBytes()), "dv1".getBytes());
        assertThat(cfStore.get(Bytes.wrap("dk1".getBytes()))).isEqualTo("dv1".getBytes());

        cfStore.close();
    }

    @Test
    @DisplayName("Should expose comparatorName config")
    void testComparatorNameConfig() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .comparatorName("reverse")
            .build();

        assertThat(config.getComparatorName()).isEqualTo("reverse");
    }

    @Test
    @DisplayName("Should have correct default values for new config fields")
    void testNewConfigDefaults() {
        TidesDBStoreConfig config = TidesDBStoreConfig.defaultConfig();

        assertThat(config.getDividingLevelOffset()).isEqualTo(2);
        assertThat(config.getMinDiskSpace()).isEqualTo(100 * 1024 * 1024);
        assertThat(config.getComparatorName()).isEqualTo("");
        assertThat(config.isUnifiedMemtable()).isFalse();
        assertThat(config.getUnifiedMemtableWriteBufferSize()).isEqualTo(0);
        assertThat(config.getObjectStoreFsPath()).isNull();
        assertThat(config.getObjectStoreConfig()).isNull();
        assertThat(config.getObjectTargetFileSize()).isEqualTo(0);
        assertThat(config.isObjectLazyCompaction()).isFalse();
        assertThat(config.isObjectPrefetchCompaction()).isTrue();
    }

    // ==================== Object Store Config Tests ====================

    @Test
    @DisplayName("Should create store config with object store FS path")
    void testObjectStoreFsPathConfig() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .objectStoreFsPath("/tmp/objstore")
            .build();

        assertThat(config.getObjectStoreFsPath()).isEqualTo("/tmp/objstore");
    }

    @Test
    @DisplayName("Should create store config with ObjectStoreConfig")
    void testObjectStoreConfig() {
        ObjectStoreConfig objConfig = ObjectStoreConfig.builder()
            .localCacheMaxBytes(512 * 1024 * 1024)
            .cacheOnRead(true)
            .cacheOnWrite(false)
            .maxConcurrentUploads(8)
            .maxConcurrentDownloads(16)
            .multipartThreshold(128 * 1024 * 1024)
            .multipartPartSize(16 * 1024 * 1024)
            .syncManifestToObject(true)
            .replicateWal(true)
            .walUploadSync(false)
            .walSyncThresholdBytes(2 * 1024 * 1024)
            .walSyncOnCommit(false)
            .replicaMode(false)
            .replicaSyncIntervalUs(10000000)
            .replicaReplayWal(true)
            .build();

        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .objectStoreConfig(objConfig)
            .build();

        assertThat(config.getObjectStoreConfig()).isNotNull();
        assertThat(config.getObjectStoreConfig().getMaxConcurrentUploads()).isEqualTo(8);
        assertThat(config.getObjectStoreConfig().getMaxConcurrentDownloads()).isEqualTo(16);
        assertThat(config.getObjectStoreConfig().isCacheOnWrite()).isFalse();
    }

    @Test
    @DisplayName("Should create store config with CF-level object store options")
    void testObjectStoreCfConfig() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .objectTargetFileSize(512 * 1024 * 1024)
            .objectLazyCompaction(true)
            .objectPrefetchCompaction(false)
            .build();

        assertThat(config.getObjectTargetFileSize()).isEqualTo(512 * 1024 * 1024);
        assertThat(config.isObjectLazyCompaction()).isTrue();
        assertThat(config.isObjectPrefetchCompaction()).isFalse();
    }

    // ==================== Column Family Management Tests ====================

    @Test
    @DisplayName("Should list column families")
    void testListColumnFamilies() throws Exception {
        String[] cfNames = store.listColumnFamilies();
        assertThat(cfNames).isNotNull();
        assertThat(cfNames).hasSizeGreaterThanOrEqualTo(1);
        assertThat(cfNames).contains("default");
    }

    @Test
    @DisplayName("Should clone column family")
    void testCloneColumnFamily() throws Exception {
        // Write data to the default CF
        store.put(Bytes.wrap("clone-k1".getBytes()), "clone-v1".getBytes());
        store.flush();

        // Clone the default CF
        store.cloneColumnFamily("default", "cloned_cf");

        // Verify clone exists
        String[] cfNames = store.listColumnFamilies();
        assertThat(cfNames).contains("cloned_cf");

        // Clean up
        store.dropColumnFamily("cloned_cf");
    }

    @Test
    @DisplayName("Should rename column family")
    void testRenameColumnFamily() throws Exception {
        // Create a CF to rename
        TidesDB db = store.getDb();
        ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder().build();
        db.createColumnFamily("rename_source", cfConfig);

        // Rename it
        store.renameColumnFamily("rename_source", "rename_dest");

        // Verify the rename
        String[] cfNames = store.listColumnFamilies();
        assertThat(cfNames).contains("rename_dest");
        assertThat(cfNames).doesNotContain("rename_source");

        // Clean up
        store.dropColumnFamily("rename_dest");
    }

    @Test
    @DisplayName("Should drop column family")
    void testDropColumnFamily() throws Exception {
        // Create a CF to drop
        TidesDB db = store.getDb();
        ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder().build();
        db.createColumnFamily("drop_me", cfConfig);

        String[] beforeNames = store.listColumnFamilies();
        assertThat(beforeNames).contains("drop_me");

        // Drop it
        store.dropColumnFamily("drop_me");

        String[] afterNames = store.listColumnFamilies();
        assertThat(afterNames).doesNotContain("drop_me");
    }

    // ==================== Comparator Tests ====================

    @Test
    @DisplayName("Should expose registerComparator method")
    void testRegisterComparator() {
        // Built-in comparators like "reverse" are already registered internally,
        // so registering them again throws invalid arguments.
        // This test verifies the method is callable and routes to the underlying API.
        assertThatThrownBy(() -> store.registerComparator("reverse", null))
            .isInstanceOf(TidesDBException.class);
    }

    // ==================== Replica Tests ====================

    @Test
    @DisplayName("Should expose promoteToPrimary method")
    void testPromoteToPrimaryNotReplica() {
        // Calling on non-replica should throw or handle gracefully
        assertThatThrownBy(() -> store.promoteToPrimary())
            .isInstanceOf(Exception.class);
    }

    // ==================== Original Tests ====================

    @Test
    @DisplayName("Should handle concurrent reads")
    void testConcurrentReads() throws InterruptedException {
        // Populate store
        for (int i = 0; i < 1000; i++) {
            store.put(
                Bytes.wrap(("key" + i).getBytes()),
                ("value" + i).getBytes()
            );
        }

        // Start multiple reader threads
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    int idx = (int) (Math.random() * 1000);
                    byte[] value = store.get(Bytes.wrap(("key" + idx).getBytes()));
                    assertThat(value).isNotNull();
                }
            });
            threads[i].start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
