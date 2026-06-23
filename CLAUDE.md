# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Benchmarks five approaches for enriching a Kafka stream with reference data across four target throughput rates, measuring per-record latency from produce to enriched output:

1. **KTable-Co** — KTable join with the stream *pre-keyed* by `productId` at produce time; both topics share the same partition count and partitioner, so Kafka Streams joins within each task with no repartition hop
2. **GlobalKTable** — GlobalKTable join; every instance holds the full reference dataset, so the join key is extracted from the stream value at join time — no copartitioning or `selectKey` needed
3. **KTable** — KTable join where the stream arrives keyed by `orderId`; a `selectKey` triggers an automatic repartition through an internal topic before the join
4. **Mongo-Batch** — records buffered for `WINDOW_MS` (default 100ms); a single `$in` query fetches all unique products for the window at once
5. **Mongo-Sync** — a synchronous `findOne` against MongoDB for each individual record

### Expected ordering: KTable-Co ≈ GlobalKTable < KTable < Mongo-Batch ≪ Mongo-Sync

- **KTable-Co** and **GlobalKTable** are the fastest: zero external I/O, no repartition hop. GlobalKTable trades replication overhead for flexibility (no copartitioning requirement).
- **KTable** has the same zero-I/O join but pays one extra Kafka round-trip (produce to + consume from the repartition topic).
- **Mongo-Batch** has a latency floor of `WINDOW_MS` but far fewer MongoDB round-trips than Mongo-Sync at high load.
- **Mongo-Sync** has the lowest best-case latency (a single MongoDB RTT) but degrades sharply with throughput as sequential round-trips queue up.

**Copartitioning requirement:** `orders-by-product` and `products-ref` must have the **same partition count** (both 4) and use the **same partitioner** (default murmur2 on the key). If either differs, Kafka Streams will throw a `TopologyException` at startup. `DataSetup` enforces this; do not change one partition count without the other.

## Prerequisites

- **Java 21** — auto-provisioned by Gradle on first run if no compatible JDK is found locally (via the [Foojay Toolchain Resolver](https://github.com/gradle/foojay-toolchains)). If you manage JDKs yourself (SDKMAN, Homebrew, etc.) any Java 21 distribution works.
- **Docker** with the Compose plugin (`docker compose version` should succeed)

## Setup and Running

```bash
# 1. Start Kafka (+ Zookeeper) and MongoDB
docker compose up -d

# 2. Wait ~10 seconds for services to be ready, then run the benchmark (~4 minutes)
./gradlew run

# Tear down and reset all state (topics, Mongo data, Kafka Streams local store)
docker compose down -v
```

On first run Gradle may download Java 21 automatically — this is expected and only happens once.

## Common Commands

```bash
./gradlew run                                                         # run the full benchmark
./gradlew test                                                        # unit tests (no infrastructure needed)
./gradlew test --tests "com.benchmark.KTablePipelineTest"             # single test class
./gradlew test --tests "com.benchmark.CopartitionedKTablePipelineTest"
./gradlew test --tests "com.benchmark.GlobalKTablePipelineTest"
./gradlew build                                                       # compile + test
```

## Output

Each run prints a latency table and a p99-by-rate summary to stdout, and writes a self-contained HTML report to `results/benchmark-<timestamp>.html`. The report contains two charts:

- **Latency distribution at max load** — grouped bar chart (min / mean / p50 / p99 / p99.9)
- **p99 latency vs throughput rate** — line chart showing degradation curves per strategy across all four target rates

Both charts have individual **"Download PNG"** buttons for slides.

`results/` is gitignored — it accumulates local run history but is never committed. To promote a result for stakeholders, copy it to `showcase/`:

```bash
cp results/benchmark-<timestamp>.html showcase/benchmark-demo.html
git add showcase/benchmark-demo.html && git commit -m "update showcase result"
```

Kafka UI is available at **http://localhost:8080** (when started) to inspect topics, messages, and consumer group lag in real time.

## Architecture

```
DataSetup
  ├── seeds products-ref       (compacted, 4 partitions, key=productId)
  └── seeds MongoDB "benchmark.products" (indexed on productId)

OrderProducer
  ├── orders-raw          (4 partitions, key=orderId)   ← used by KTable, GlobalKTable, Mongo pipelines
  └── orders-by-product   (4 partitions, key=productId) ← copartitioned with products-ref

orders-by-product ──► CopartitionedKTablePipeline ──join(KTable, no repartition)──► orders-enriched-ktable-copart
orders-raw        ──► GlobalKTablePipeline          ──join(GlobalKTable)────────────► orders-enriched-ktable-global
orders-raw        ──► KTablePipeline               ──selectKey→repartition→join───► orders-enriched-ktable
orders-raw        ──► MongoPipeline                ──findOne per record────────────► orders-enriched-mongo
orders-raw        ──► WindowedMongoPipeline         ──$in per WINDOW_MS─────────────► orders-enriched-windowed-mongo

BenchmarkCollector reads all five output topics in parallel (virtual threads) → BenchmarkRunner prints table + writes HTML report
```

**`BenchmarkRunner` orchestration sequence (per rate run):**
1. `DataSetup.setupAll()` — create topics, load 1,000 products into Kafka + MongoDB (first run only)
2. Start all five `KafkaStreams` apps
3. Poll until all reach `RUNNING` state; sleep 3 s on first run for KTable changelog replay, 1 s on subsequent runs (local state is warm)
4. `OrderProducer` emits `THROUGHPUT_ORDER_COUNT` orders at the target rate using nanosecond-precision sleep-based pacing
5. `BenchmarkCollector.collectAll()` reads all five output topics in parallel with a shared 60 s deadline
6. Store `RateRun(ratePerSecond, statsList)` for this pass
7. Stop all pipelines; reset input + output topics (keep `products-ref`); repeat for next rate

After all rate runs: print latency table + throughput summary, write HTML report to `results/`.

## Key Files

| File | Purpose |
|------|---------|
| `pipeline/CopartitionedKTablePipeline.java` | KTable join without repartition; reads `orders-by-product` (keyed by productId) |
| `pipeline/GlobalKTablePipeline.java` | GlobalKTable join; reads `orders-raw` (keyed by orderId), extracts join key from value |
| `pipeline/KTablePipeline.java` | KTable join with `selectKey` repartition; `buildTopology()` testable without Kafka |
| `pipeline/MongoPipeline.java` | Topology with synchronous `findOne` per record |
| `pipeline/WindowedMongoPipeline.java` | Topology using `BatchEnrichProcessor` |
| `pipeline/BatchEnrichProcessor.java` | Buffers records for `WINDOW_MS`, issues one `$in` query per window flush |
| `config/AppConfig.java` | All tunable constants (topic names, counts, connection strings, `WINDOW_MS`, `THROUGHPUT_RATES`) |
| `config/SerdeFactory.java` | Jackson JSON `Serde<T>` for all model types |
| `producer/DataSetup.java` | Topic creation + deletion, product seeding; `resetForNextRun()` resets between rate passes |
| `producer/OrderProducer.java` | Dual-produces to `orders-raw` and `orders-by-product`; supports rate-limited produce |
| `BenchmarkRunner.java` | Entry point; multi-rate loop, pipeline lifecycle helpers, stdout report |
| `BenchmarkCollector.java` | Collects latencies; `collectAll()` fans out across all topics in parallel via virtual threads |
| `HtmlReporter.java` | Writes `results/benchmark-<timestamp>.html` with two Chart.js charts |

## Design Notes

**KTable re-keying (KTable pipeline):** Orders arrive keyed by `orderId`; the KTable is keyed by `productId`. `KTablePipeline` calls `selectKey(order -> order.productId)`, which triggers an automatic repartition: Kafka Streams writes the re-keyed records to an internal topic (`KSTREAM-KEY-SELECT-...-repartition`), then reads them back before joining. Both the produce and consume of the repartition topic are included in the measured latency.

**Copartitioned join (KTable-Co pipeline):** `OrderProducer` sends each order to `orders-by-product` keyed by `productId`. Since both `orders-by-product` and `products-ref` have 4 partitions and use the default (murmur2) partitioner on `productId`, Kafka Streams assigns the same task to partition N of both topics. The join happens entirely within each task's local state — no `selectKey`, no repartition topic, no extra network hop.

**GlobalKTable join:** Every application instance reads all partitions of `products-ref` and maintains a full local copy. The join uses a `KeyValueMapper` `(orderId, order) -> order.productId` to extract the join key from the stream value, bypassing the copartitioning requirement. The tradeoff is memory/disk: every instance holds the entire reference dataset. Appropriate for small-to-medium reference data when rekeying the stream is not possible.

**Clean-slate between rate runs:** `DataSetup.resetForNextRun()` deletes and recreates all topics except `products-ref`. This ensures pipelines start from offset 0 on fresh data each pass (avoiding cross-run latency contamination where a new pipeline with no committed offsets would read historical records). `products-ref` is preserved so KTable and GlobalKTable local state stores remain valid — subsequent runs skip the full changelog replay.

**MongoDB index:** `DataSetup` creates a single-field ascending index on `productId` before the benchmark starts. Remove `col.createIndex(...)` in `DataSetup.java` to measure the un-indexed cost.

**Caching disabled:** All pipelines set `CACHE_MAX_BYTES_BUFFERING_CONFIG = 0` so records are forwarded immediately rather than batched in the Streams internal cache, giving more accurate per-record latency numbers.

**Benchmark parameters:** Change `THROUGHPUT_RATES`, `THROUGHPUT_ORDER_COUNT`, `PRODUCT_COUNT`, and `WINDOW_MS` in `AppConfig.java` to scale the dataset or tune the batch window.

## Claude Code (`.claude/`)

`.claude/launch.json` is gitignored (it may contain machine-specific absolute paths). Copy `.claude/launch.json.example` to `.claude/launch.json` to use the Claude Code preview integrations for starting Kafka, MongoDB, Kafka UI, and the benchmark.
