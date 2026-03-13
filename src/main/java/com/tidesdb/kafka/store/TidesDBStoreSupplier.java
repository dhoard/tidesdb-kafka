package com.tidesdb.kafka.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Supplier for TidesDB-backed KeyValue stores in Kafka Streams.
 * 
 * Usage with Materialized:
 * <pre>
 * builder.stream("input")
 *     .groupByKey()
 *     .count(Materialized.as(new TidesDBStoreSupplier("my-store")));
 * </pre>
 * 
 * Usage with custom config:
 * <pre>
 * TidesDBStoreConfig config = TidesDBStoreConfig.builder()
 *     .compressionAlgorithm(CompressionAlgorithm.ZSTD_COMPRESSION)
 *     .syncMode(SyncMode.SYNC_NONE)
 *     .build();
 * builder.stream("input")
 *     .groupByKey()
 *     .count(Materialized.as(new TidesDBStoreSupplier("my-store", config)));
 * </pre>
 */
public class TidesDBStoreSupplier implements KeyValueBytesStoreSupplier {

    private final String name;
    private final TidesDBStoreConfig storeConfig;

    public TidesDBStoreSupplier(String name) {
        this(name, TidesDBStoreConfig.defaultConfig());
    }

    public TidesDBStoreSupplier(String name, TidesDBStoreConfig config) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be null or empty");
        }
        this.name = name;
        this.storeConfig = config != null ? config : TidesDBStoreConfig.defaultConfig();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return new TidesDBStore(name, storeConfig);
    }

    @Override
    public String metricsScope() {
        return "tidesdb";
    }

    /**
     * Create a new TidesDB store supplier with the given name
     */
    public static TidesDBStoreSupplier create(String name) {
        return new TidesDBStoreSupplier(name);
    }

    /**
     * Create a new TidesDB store supplier with the given name and config
     */
    public static TidesDBStoreSupplier create(String name, TidesDBStoreConfig config) {
        return new TidesDBStoreSupplier(name, config);
    }
}
