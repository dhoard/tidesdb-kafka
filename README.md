# tidesdb-kafka
TidesDB + Kafka Streams

A drop-in replacement for RocksDB state stores in Kafka Streams applications, powered by [TidesDB](https://tidesdb.com).

## Features

- Swap RocksDB for TidesDB with a one-line change
- Every TidesDB database and column family option exposed through `TidesDBStoreConfig`
- Default TTL per store or per-key TTL via `putWithTtl()`
- Optional B+tree SSTables for faster point lookups
- Change data capture callbacks on every transaction commit
- Online backup and near-instant hard-link checkpoints
- Column family stats, database stats, and block cache stats
- Tiered storage on a filesystem or S3-compatible object store (AWS S3, MinIO, GCS)
- Change write buffer, bloom FPR, sync mode without restart
- Pooled transaction with `reset()` to minimize allocation on the hot path

## Installation

```bash
./install.sh
```

This will:
1. Build and install TidesDB native library
2. Build and install TidesDB Java bindings 
3. Install the Kafka Streams plugin

## Quick Start

```java
import com.tidesdb.kafka.store.TidesDBStoreSupplier;

// Default config
KTable<String, Long> counts = input
    .groupByKey()
    .count(Materialized.as(new TidesDBStoreSupplier("my-counts")));

// Custom config
TidesDBStoreConfig config = TidesDBStoreConfig.builder()
    .compressionAlgorithm(CompressionAlgorithm.ZSTD_COMPRESSION)
    .syncMode(SyncMode.SYNC_NONE)
    .enableBloomFilter(true)
    .blockCacheSize(128 * 1024 * 1024)
    .defaultTtlSeconds(3600)
    .build();

KTable<String, Long> counts = input
    .groupByKey()
    .count(Materialized.as(new TidesDBStoreSupplier("my-counts", config)));
```

See [kafka.md](kafka.md) for the full reference documentation.

## Running Tests and Benchmarks

```bash
./run.sh [options]

Options:
  -t, --tests            Run unit tests
  -b, --benchmarks       Run benchmarks
  -c, --charts           Generate charts from benchmark data
  -a, --all              Run everything
  -d, --data-dir <path>  Set data directory for benchmark databases
  -h, --help             Show this help message
```

### Examples

```bash
# Run unit tests
./run.sh -t

# Run benchmarks with default temp directory
./run.sh -b

# Run benchmarks on a fast SSD
./run.sh -b -d /mnt/nvme/bench

# Run benchmarks and generate charts
./run.sh -b -c

# Run everything
./run.sh -a
```

### Dynamic Benchmark Configuration

All benchmark parameters are configurable via `-D` system properties:

```bash
mvn test -Dtest=StateStoreBenchmark \
    -DargLine="-Djava.library.path=/usr/local/lib \
               -Dbenchmark.sizes=1000,10000,100000 \
               -Dbenchmark.value.size=256 \
               -Dbenchmark.mixed.ratio=80 \
               -Dbenchmark.threads=1,4,8 \
               -Dbenchmark.percentiles=true"
```

Key parameters:

| Property | Default | Description |
|----------|---------|-------------|
| `benchmark.sizes` | 1000,5000,10000,50000,100000 | Operation counts for standard benchmarks |
| `benchmark.large.sizes` | 100000,...,25000000 | Sizes for large dataset benchmarks |
| `benchmark.threads` | 1,2,4,8,16 | Thread counts for concurrent benchmarks |
| `benchmark.value.size` | 64 | Value size in bytes |
| `benchmark.mixed.ratio` | 50 | Read percentage for mixed workload (0-100) |
| `benchmark.warmup` | 3 | Warmup iterations |
| `benchmark.iterations` | 5 | Measurement iterations |
| `benchmark.percentiles` | true | Track p50/p90/p95/p99/p99.9/max latencies |
| `benchmark.seed` | 42 | Random seed for reproducibility |

### Fair Comparison

Both engines are configured with equivalent settings:
- LZ4 on both
- ~1% FPR on both (TidesDB 0.01 FPR, RocksDB 10 bits/key)
- Block cache is 64 MB on both
- **Write buffer is 64 MB on both
- Background threads set to 4 on both (2 flush + 2 compaction)
- Sync disabled on both (TidesDB `SYNC_NONE`, RocksDB `sync=false`)

## License

**Apache Kafka is a trademark of the Apache Software Foundation.**

**This project/product is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.**

Licensed under Mozilla Public License 2.0
