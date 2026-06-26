# Memory Utilisation Benchmark — Plan

Companion project to this latency benchmark. Answers: **how does in-process memory footprint scale with reference dataset size, per strategy — and at what point does GlobalKTable become a liability?**

## Why a separate project

All five pipelines in the latency benchmark share a single JVM, so per-strategy memory cannot be isolated. A clean measurement requires running each strategy as a separate OS process, capturing metrics after the state store is fully loaded, then tearing it down.

## What to measure

| Metric | Source | Why |
|--------|--------|-----|
| JVM heap used | `Runtime.getRuntime()` or JMX `java.lang:type=Memory` | Baseline process memory |
| RocksDB block cache | Kafka Streams metric `rocksdb.block-cache-usage` | Hot data cached in native memory |
| RocksDB live data estimate | Kafka Streams metric `rocksdb.estimate-live-data-size` | Total state store size on disk/mem |
| Process RSS | `/proc/self/status` (Linux) or `ps -o rss=` (macOS) | Ground-truth OS view of memory |

For the Mongo strategies, only heap and RSS matter — there is no state store.

## Dataset sizes

Run at: **1k, 10k, 100k, 500k, 1M products**

This range is wide enough to show where the GlobalKTable curve diverges from KTable.

## Architecture

```
driver (shell or Gradle task)
  └── for each strategy:
        1. start JVM with --strategy=<name> --product-count=<n>
        2. wait for HTTP /ready endpoint (pipeline reached RUNNING + state loaded)
        3. GET /metrics → { heapUsedMb, rocksdbBlockCacheMb, rocksdbLiveDataMb, rssMb }
        4. kill process
        5. record (strategy, productCount, metrics)
  └── write CSV + HTML report (one line chart per metric, one line per strategy)
```

Each strategy JVM exposes a lightweight HTTP metrics endpoint (e.g. via `com.sun.net.httpserver.HttpServer` — no framework needed) that emits JSON after the pipeline is RUNNING.

## Expected results

```
                  1k     10k    100k   500k    1M
KTable-Co        ~60MB  ~62MB  ~80MB  ~150MB  ~400MB   ← scales with partition slice
GlobalKTable     ~60MB  ~65MB  ~120MB ~450MB  ~1.5GB   ← scales with full dataset × instances
KTable           ~60MB  ~62MB  ~80MB  ~150MB  ~400MB   ← same as KTable-Co (same state store)
Mongo-Batch      ~50MB  ~50MB  ~50MB  ~50MB   ~50MB    ← flat (no state)
Mongo-Sync       ~50MB  ~50MB  ~50MB  ~50MB   ~50MB    ← flat (no state)
```

Numbers are rough estimates. The divergence between GlobalKTable and the KTable strategies is the key chart — it shows the scaling cliff.

## Report

Same HTML + Chart.js pattern as the latency benchmark:

- **Line chart:** RSS (MB) vs product count, one line per strategy — shows the scaling cliff visually
- **Data table:** all four metrics at each dataset size per strategy

## Project structure

```
memory-benchmark/
  docker-compose.yml         (same Kafka + Mongo stack)
  build.gradle
  src/main/java/
    MemoryBenchmarkRunner.java   (driver: loop over strategies × dataset sizes)
    strategy/
      StrategyProcess.java       (launches child JVM, waits for /ready, reads /metrics)
    pipeline/
      CopartitionedKTableApp.java
      GlobalKTableApp.java
      KTableApp.java
      MongoBatchApp.java
      MongoSyncApp.java
    MetricsServer.java           (tiny HTTP server: /ready + /metrics endpoints)
    HtmlReporter.java
```

Each `*App.java` is a self-contained main class that:
1. Creates and starts its pipeline
2. Waits for RUNNING state
3. Forces a GC
4. Starts the metrics HTTP server
5. Blocks until killed by the driver

## Key implementation notes

- **Force GC before snapshot** — `System.gc()` before reading heap to reduce noise from unreachable objects
- **Wait for state store saturation** — after RUNNING, sleep 2s to allow RocksDB compaction to settle before reading metrics
- **RocksDB metrics** — enable via `StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG = "DEBUG"` and read through `KafkaStreams.metrics()`; the relevant metric names are `rocksdb-state-id.block-cache-usage` and `rocksdb-state-id.estimate-live-data-size`
- **RSS on macOS** — use `ProcessHandle.current().pid()` then shell out to `ps -o rss= -p <pid>` (returns KB)
- **Product seeding** — each child JVM seeds its own products into Kafka and MongoDB at startup; the driver does not share state across runs

## Suggested repo name

`kstream-ktable-vs-mongo-memory`
