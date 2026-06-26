# KStream KTable vs MongoDB Lookup Benchmark

Answers two questions engineers face when enriching a Kafka stream with reference data:

1. **Which lookup strategy has the lowest latency?**
2. **Which strategies hold up under load — and which ones fall apart?**

Five strategies are benchmarked end-to-end at four throughput rates (500 / 2,000 / 5,000 / 10,000 rec/s), measuring per-record latency from produce to enriched output:

| Strategy | How it works |
|---|---|
| **KTable-Co** | KTable join — stream pre-keyed by `productId`, copartitioned with the reference topic. No repartition hop. |
| **GlobalKTable** | GlobalKTable join — full reference table replicated to every instance. No copartitioning required; join key extracted from the stream value at join time. |
| **KTable** | KTable join — stream arrives keyed by `orderId`; `selectKey` triggers an internal repartition topic before the join. |
| **Mongo-Batch** | Records buffered for `WINDOW_MS` (default 100 ms); one `$in` query per window flush. |
| **Mongo-Sync** | Synchronous `findOne` per record. |

Expected ordering at high load: **KTable-Co ≈ GlobalKTable < KTable < Mongo-Batch ≪ Mongo-Sync**

## Prerequisites

- **Docker** with the Compose plugin — verify with `docker compose version`
- Nothing else — Gradle downloads Java 21 automatically on first run

## Quick start

```bash
# Start Kafka and MongoDB
docker compose up -d

# Wait ~10 seconds, then run the benchmark (~4 minutes)
./gradlew run

# Tear down and reset all state when done
docker compose down -v
```

The benchmark runs 5,000 orders through all five strategies at each of four target rates. Each run prints a latency table to stdout and writes a self-contained HTML report to `results/benchmark-<timestamp>.html` with two charts:

- **Latency distribution** — grouped bar chart of min / mean / p50 / p99 / p99.9 at max load
- **p99 latency vs throughput rate** — line chart showing how each strategy degrades under increasing load

Both charts have a **"Download PNG"** button for dropping into slides.

## Sample results

Open [`showcase/benchmark-demo.html`](showcase/benchmark-demo.html) in a browser — or view [`showcase/benchmark-demo.png`](showcase/benchmark-demo.png) — to see a real benchmark run without running it yourself.

## Architecture

```
DataSetup
  ├── seeds products-ref       (compacted, 4 partitions, key=productId)
  └── seeds MongoDB "benchmark.products" (indexed on productId)

OrderProducer
  ├── orders-raw          (4 partitions, key=orderId)
  └── orders-by-product   (4 partitions, key=productId)  ← copartitioned with products-ref

orders-by-product ──► CopartitionedKTablePipeline ──join(KTable, no repartition)──► orders-enriched-ktable-copart
orders-raw        ──► GlobalKTablePipeline          ──join(GlobalKTable)────────────► orders-enriched-ktable-global
orders-raw        ──► KTablePipeline               ──selectKey→repartition→join───► orders-enriched-ktable
orders-raw        ──► MongoPipeline                ──findOne per record────────────► orders-enriched-mongo
orders-raw        ──► WindowedMongoPipeline         ──$in per WINDOW_MS─────────────► orders-enriched-windowed-mongo
```

All five pipelines run concurrently. Topics are deleted and recreated between rate runs so each pass starts from a clean slate. The KTable local state store is preserved across runs (only `products-ref` is kept) so subsequent passes start up quickly.

## Tuning

Edit [`src/main/java/com/benchmark/config/AppConfig.java`](src/main/java/com/benchmark/config/AppConfig.java):

| Constant | Default | Effect |
|---|---|---|
| `THROUGHPUT_RATES` | `{500, 2000, 5000, 10000}` | Target producer rates (rec/s) |
| `THROUGHPUT_ORDER_COUNT` | 5000 | Orders produced per rate run |
| `PRODUCT_COUNT` | 1000 | Reference dataset size |
| `WINDOW_MS` | 100 | Mongo-Batch flush interval — halving this halves the latency floor at the cost of 2× the query rate |

## Memory tradeoff (not measured here)

KTable-Co and KTable each hold only their assigned partition slice of the reference dataset — memory per instance scales as *dataset size ÷ partition count*, so adding instances keeps the per-instance footprint flat. GlobalKTable replicates the **full** dataset to every instance regardless of how many are running: at 1,000 products this is negligible, but at 100k+ products it becomes a hard ceiling on horizontal scaling. Mongo-Batch and Mongo-Sync hold no reference data in-process at all.

A companion benchmark that runs each strategy in isolation and measures heap + RocksDB off-heap across varying dataset sizes (1k → 1M products) would make this tradeoff empirical. See `MEMORY_BENCHMARK_PLAN.md` for the design.

## Monitoring

Kafka UI runs at **http://localhost:8080** while the Docker stack is up — inspect topics, messages, and consumer group lag in real time.

## Unit tests

```bash
./gradlew test
```

Tests use `TopologyTestDriver` — no Kafka or MongoDB needed.
