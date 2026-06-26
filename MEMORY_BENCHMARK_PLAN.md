# Memory Benchmark

**Implemented** — run with `./gradlew runMemory`.

Answers: **how does in-process memory footprint scale with reference dataset size, per strategy — and at what point does GlobalKTable become a liability?**

## How it works

Each strategy runs in a separate child JVM (`StrategyApp`) so per-strategy memory can be isolated. The driver (`MemoryBenchmarkRunner`) spawns one child per (strategy × dataset size), waits for it to reach RUNNING and signal readiness via `/ready`, reads metrics from `/metrics`, then kills the process.

```
MemoryBenchmarkRunner
  for each productCount in MEMORY_PRODUCT_COUNTS:
    DataSetup.setupAll(productCount)        ← seeds Kafka + MongoDB
    for each strategy:
      spawn StrategyApp --strategy <s> --port <p> --product-count <n>
      poll GET /ready until 200 (up to 5 min)
      GET /metrics → { heapUsedMb, rocksdbBlockCacheMb, rocksdbLiveDataMb, rssMb }
      kill child
  write results/memory-<timestamp>.html
```

## Metrics

| Metric | How collected |
|--------|--------------|
| `heapUsedMb` | `Runtime.totalMemory() - Runtime.freeMemory()` after `System.gc()` |
| `rocksdbBlockCacheMb` | Kafka Streams metric `rocksdb-block-cache-usage` (summed across stores) |
| `rocksdbLiveDataMb` | Kafka Streams metric `rocksdb-estimate-live-data-size` (summed across stores) |
| `rssMb` | `ps -o rss= -p <pid>` — ground-truth OS view of total process memory |

RocksDB metrics require `METRICS_RECORDING_LEVEL = DEBUG` (set by `StrategyApp`).
Mongo strategies show 0 for RocksDB — they have no state store.

## Dataset sizes

Default: **1k, 10k, 100k, 500k products** — configurable via `AppConfig.MEMORY_PRODUCT_COUNTS`.

Note: at large dataset sizes, each KTable strategy can take several minutes to load its state store from Kafka. The full sweep (5 strategies × 4 sizes = 20 child processes) can take 30–60 minutes at 500k products.

## Expected results

```
                  1k     10k    100k   500k
KTable-Co        ~70    ~75    ~100   ~250   ← RSS (MB): scales with partition slice (1/4 of dataset)
GlobalKTable     ~70    ~80    ~180   ~700   ← RSS (MB): scales with full dataset × instances
KTable           ~70    ~75    ~100   ~250   ← RSS (MB): same as KTable-Co (same store, different key)
Mongo-Batch      ~60    ~60    ~60    ~60    ← flat: no state store
Mongo-Sync       ~60    ~60    ~60    ~60    ← flat: no state store
```

Numbers are rough estimates; actual values depend on JVM GC state and hardware.

## Key files

| File | Purpose |
|------|---------|
| `memory/MemoryBenchmarkRunner.java` | Driver: sweeps dataset sizes, launches children, collects results |
| `memory/StrategyApp.java` | Child JVM: starts one strategy, serves /ready + /metrics |
| `memory/MetricsServer.java` | Lightweight HTTP server (JDK `com.sun.net.httpserver`) |
| `memory/MemoryHtmlReporter.java` | Writes HTML report with two Chart.js line charts |
| `memory/MemoryStats.java` | Record: heap, rocksdb block cache, rocksdb live data, rss |
| `memory/MemoryResult.java` | Record: (strategy, productCount, MemoryStats) |
| `config/AppConfig.java` | `MEMORY_PRODUCT_COUNTS` — tune dataset sizes here |
