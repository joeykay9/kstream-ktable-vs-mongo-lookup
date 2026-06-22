# KStream KTable vs MongoDB Lookup Benchmark

Answers the question: **when enriching a Kafka stream with reference data, how much does the lookup strategy matter?**

Four strategies are benchmarked end-to-end, measuring per-record latency from produce to enriched output:

| Strategy | How it works |
|---|---|
| **KTable-Co** | KTable join — stream pre-keyed by `productId`, copartitioned with the reference topic. No repartition hop. |
| **KTable** | KTable join — stream arrives keyed by `orderId`, `selectKey` triggers an internal repartition topic before the join. |
| **Mongo-Batch** | Records buffered for `WINDOW_MS` (default 100 ms); one `$in` query per window flush. |
| **Mongo-Sync** | Synchronous `findOne` per record. |

Expected ordering: **KTable-Co < KTable < Mongo-Batch < Mongo-Sync**

## Prerequisites

- **Docker** with the Compose plugin — verify with `docker compose version`
- Nothing else — Gradle downloads Java 21 automatically on first run

## Quick start

```bash
# Start Kafka and MongoDB
docker compose up -d

# Wait ~10 seconds, then run the benchmark
./gradlew run

# Tear down and reset all state when done
docker compose down -v
```

Each run prints a latency table to stdout and writes a self-contained HTML report to `results/benchmark-<timestamp>.html`. The report has a **"Download chart PNG"** button for pulling the chart into slides.

## Sample results

Open [`showcase/benchmark-demo.html`](showcase/benchmark-demo.html) in a browser to see a real benchmark run without having to execute one yourself.

## Architecture

```
DataSetup
  ├── seeds products-ref       (compacted, 4 partitions, key=productId)
  └── seeds MongoDB "benchmark.products" (indexed on productId)

OrderProducer
  ├── orders-raw          (4 partitions, key=orderId)
  └── orders-by-product   (4 partitions, key=productId)  ← copartitioned with products-ref

orders-by-product ──► CopartitionedKTablePipeline ──join(KTable, no repartition)──► orders-enriched-ktable-copart
orders-raw        ──► KTablePipeline               ──selectKey→repartition→join───► orders-enriched-ktable
orders-raw        ──► MongoPipeline                ──findOne per record────────────► orders-enriched-mongo
orders-raw        ──► WindowedMongoPipeline         ──$in per WINDOW_MS─────────────► orders-enriched-windowed-mongo
```

All four pipelines run concurrently. `BenchmarkCollector` reads all four output topics and sorts latencies before printing the report.

## Tuning

Edit [`src/main/java/com/benchmark/config/AppConfig.java`](src/main/java/com/benchmark/config/AppConfig.java):

| Constant | Default | Effect |
|---|---|---|
| `ORDER_COUNT` | 10000 | Total orders produced |
| `PRODUCT_COUNT` | 1000 | Reference dataset size |
| `WINDOW_MS` | 100 | Mongo-Batch flush interval — halving this halves the latency floor at the cost of 2× the query rate |

## Monitoring

Kafka UI runs at **http://localhost:8080** while the Docker stack is up — inspect topics, messages, and consumer group lag in real time.

## Unit tests

```bash
./gradlew test
```

Tests use `TopologyTestDriver` — no Kafka or MongoDB needed.
