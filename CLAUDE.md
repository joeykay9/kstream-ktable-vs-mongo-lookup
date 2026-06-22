# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Benchmarks four approaches for enriching a Kafka stream with reference data:

1. **KTable-Co** — KTable join with the stream *pre-keyed* by `productId` at produce time; both topics share the same partition count and partitioner, so Kafka Streams joins within each task with no repartition hop
2. **KTable** — KTable join where the stream arrives keyed by `orderId`; a `selectKey` triggers an automatic repartition through an internal topic before the join
3. **Mongo-Batch** — records buffered for `WINDOW_MS` (default 100ms); a single `$in` query fetches all unique products for the window at once
4. **Mongo-Sync** — a synchronous `findOne` against MongoDB for each individual record

### Expected ordering: KTable-Co < KTable < Mongo-Batch < Mongo-Sync

- **KTable-Co** is the fastest: zero external I/O, and no internal repartition topic between producer and join.
- **KTable** has the same zero-I/O join but pays one extra Kafka round-trip (produce to + consume from the repartition topic). On localhost this is a few ms; on a multi-broker cluster it crosses the network twice per record.
- **Mongo-Batch** has a latency floor of `WINDOW_MS` but far fewer MongoDB round-trips than Mongo-Sync at high load.
- **Mongo-Sync** has the lowest best-case latency (a single MongoDB RTT) but degrades linearly with throughput.

**Copartitioning requirement:** `orders-by-product` and `products-ref` must have the **same partition count** (both 4) and use the **same partitioner** (default murmur2 on the key). If either differs, Kafka Streams will throw a `TopologyException` at startup. `DataSetup` enforces this; do not change one partition count without the other.

## Prerequisites

- **Java 21** — auto-provisioned by Gradle on first run if no compatible JDK is found locally (via the [Foojay Toolchain Resolver](https://github.com/gradle/foojay-toolchains)). If you manage JDKs yourself (SDKMAN, Homebrew, etc.) any Java 21 distribution works.
- **Docker** with the Compose plugin (`docker compose version` should succeed)

## Setup and Running

```bash
# 1. Start Kafka (+ Zookeeper) and MongoDB
docker compose up -d

# 2. Wait ~10 seconds for services to be ready, then run the benchmark
./gradlew run

# Tear down and reset all state (topics, Mongo data, Kafka Streams local store)
docker compose down -v
```

On first run Gradle may download Java 21 automatically — this is expected and only happens once.

## Common Commands

```bash
./gradlew run                                                    # run the full benchmark
./gradlew test                                                   # unit tests (no infrastructure needed)
./gradlew test --tests "com.benchmark.KTablePipelineTest"        # single test class
./gradlew test --tests "com.benchmark.CopartitionedKTablePipelineTest"
./gradlew build                                                  # compile + test
```

## Output

Each run prints a latency table to stdout and writes a self-contained HTML report to `results/benchmark-<timestamp>.html`. Open it in any browser to see a grouped bar chart across all four strategies.

Kafka UI is available at **http://localhost:8080** (when started) to inspect topics, messages, and consumer group lag in real time.

## Architecture

```
DataSetup
  ├── seeds products-ref       (compacted, 4 partitions, key=productId)
  └── seeds MongoDB "benchmark.products" (indexed on productId)

OrderProducer
  ├── orders-raw          (4 partitions, key=orderId)   ← existing pipelines
  └── orders-by-product   (4 partitions, key=productId) ← copartitioned with products-ref

orders-by-product ──► CopartitionedKTablePipeline ──join(KTable, no repartition)──► orders-enriched-ktable-copart
orders-raw        ──► KTablePipeline               ──selectKey→repartition→join───► orders-enriched-ktable
orders-raw        ──► MongoPipeline                ──findOne per record────────────► orders-enriched-mongo
orders-raw        ──► WindowedMongoPipeline         ──$in per WINDOW_MS─────────────► orders-enriched-windowed-mongo

BenchmarkCollector reads all four output topics → BenchmarkRunner prints latency table + writes HTML report
```

**`BenchmarkRunner` orchestration sequence:**
1. `DataSetup.setupAll()` — create topics, load 1 000 products into Kafka + MongoDB
2. Start all four `KafkaStreams` apps
3. Poll until all reach `RUNNING` state, then sleep 3 s for KTable changelog replay
4. `OrderProducer` emits 10 000 orders to both `orders-raw` and `orders-by-product`
5. `BenchmarkCollector` reads all four output topics (up to 2-minute timeout), sorts latencies
6. Print latency table to stdout and write HTML report to `results/`

## Key Files

| File | Purpose |
|------|---------|
| `pipeline/CopartitionedKTablePipeline.java` | KTable join without repartition; reads `orders-by-product` (keyed by productId) |
| `pipeline/KTablePipeline.java` | KTable join with `selectKey` repartition; `buildTopology()` testable without Kafka |
| `pipeline/MongoPipeline.java` | Topology with synchronous `findOne` per record |
| `pipeline/WindowedMongoPipeline.java` | Topology using `BatchEnrichProcessor` |
| `pipeline/BatchEnrichProcessor.java` | Buffers records for `WINDOW_MS`, issues one `$in` query per window flush |
| `config/AppConfig.java` | All tunable constants (topic names, counts, connection strings, `WINDOW_MS`) |
| `config/SerdeFactory.java` | Jackson JSON `Serde<T>` for all model types |
| `producer/DataSetup.java` | Topic creation, product seeding for both Kafka and MongoDB |
| `producer/OrderProducer.java` | Produces each order to both `orders-raw` (key=orderId) and `orders-by-product` (key=productId) |
| `BenchmarkRunner.java` | Entry point; orchestrates setup → pipelines → collect → report |
| `HtmlReporter.java` | Writes `results/benchmark-<timestamp>.html` with Chart.js bar chart |

## Design Notes

**KTable re-keying (KTable pipeline):** Orders arrive keyed by `orderId`; the KTable is keyed by `productId`. `KTablePipeline` calls `selectKey(order -> order.productId)`, which triggers an automatic repartition: Kafka Streams writes the re-keyed records to an internal topic (`KSTREAM-KEY-SELECT-...-repartition`), then reads them back before joining. Both the produce and consume of the repartition topic are included in the measured latency.

**Copartitioned join (KTable-Co pipeline):** `OrderProducer` sends each order to `orders-by-product` keyed by `productId`. Since both `orders-by-product` and `products-ref` have 4 partitions and use the default (murmur2) partitioner on `productId`, Kafka Streams assigns the same task to partition N of both topics. The join happens entirely within each task's local state — no `selectKey`, no repartition topic, no extra network hop.

**MongoDB index:** `DataSetup` creates a single-field ascending index on `productId` before the benchmark starts. Remove `col.createIndex(...)` in `DataSetup.java` to measure the un-indexed cost.

**Caching disabled:** All pipelines set `CACHE_MAX_BYTES_BUFFERING_CONFIG = 0` so records are forwarded immediately rather than batched in the Streams internal cache, giving more accurate per-record latency numbers.

**Benchmark parameters:** Change `PRODUCT_COUNT`, `ORDER_COUNT`, and `WINDOW_MS` in `AppConfig.java` to scale the dataset or tune the batch window. Halving `WINDOW_MS` roughly halves Mongo-Batch's latency floor at the cost of doubling its MongoDB query rate.

## Claude Code (`.claude/`)

`.claude/launch.json` is gitignored (it may contain machine-specific absolute paths). Copy `.claude/launch.json.example` to `.claude/launch.json` to use the Claude Code preview integrations for starting Kafka, MongoDB, Kafka UI, and the benchmark.
