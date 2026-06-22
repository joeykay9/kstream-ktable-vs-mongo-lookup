package com.benchmark;

import com.benchmark.config.AppConfig;
import com.benchmark.pipeline.CopartitionedKTablePipeline;
import com.benchmark.pipeline.GlobalKTablePipeline;
import com.benchmark.pipeline.KTablePipeline;
import com.benchmark.pipeline.MongoPipeline;
import com.benchmark.pipeline.WindowedMongoPipeline;
import com.benchmark.producer.DataSetup;
import com.benchmark.producer.OrderProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BenchmarkRunner {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    private static final List<String> LABELS = List.of(
        "KTable-Co", "GlobalKTable", "KTable", "Mongo-Batch", "Mongo-Sync");
    private static final List<String> TOPICS = List.of(
        AppConfig.TOPIC_ENRICHED_KTABLE_COPART,
        AppConfig.TOPIC_ENRICHED_KTABLE_GLOBAL,
        AppConfig.TOPIC_ENRICHED_KTABLE,
        AppConfig.TOPIC_ENRICHED_WINDOWED_MONGO,
        AppConfig.TOPIC_ENRICHED_MONGO);

    public static void main(String[] args) throws Exception {
        log.info("=== KStream KTable vs MongoDB Lookup Benchmark ===");
        log.info("Products: {}, Orders per rate: {}, Window: {}ms",
            AppConfig.PRODUCT_COUNT, AppConfig.THROUGHPUT_ORDER_COUNT, AppConfig.WINDOW_MS);

        DataSetup setup    = new DataSetup();
        var       products = setup.setupAll();
        var       collector = new BenchmarkCollector();
        var       producer  = new OrderProducer();

        List<BenchmarkCollector.RateRun> rateRuns = new ArrayList<>();
        List<?>[] streamsHolder = {null};

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            @SuppressWarnings("unchecked")
            var s = (List<KafkaStreams>) streamsHolder[0];
            if (s != null) stopAll(s);
        }));

        List<KafkaStreams> streams = null;
        for (int r = 0; r < AppConfig.THROUGHPUT_RATES.length; r++) {
            int rate = AppConfig.THROUGHPUT_RATES[r];
            log.info("--- Rate run {}/{}: {} rec/s ---", r + 1, AppConfig.THROUGHPUT_RATES.length, rate);

            if (r > 0) {
                stopAll(streams);
                setup.resetForNextRun();
            }

            streams = createPipelines();
            streamsHolder[0] = streams;
            startAll(streams);
            waitForRunning(streams);
            // First run: wait for full KTable changelog replay.
            // Subsequent runs: local state is warm, brief pause suffices.
            Thread.sleep(r == 0 ? 3_000 : 1_000);

            log.info("Producing {} orders at {} rec/s...", AppConfig.THROUGHPUT_ORDER_COUNT, rate);
            producer.produce(products, AppConfig.THROUGHPUT_ORDER_COUNT, rate);

            log.info("Collecting results (parallel, 60s deadline)...");
            List<BenchmarkCollector.Stats> stats =
                collector.collectAll(TOPICS, LABELS, AppConfig.THROUGHPUT_ORDER_COUNT, 60_000);
            rateRuns.add(new BenchmarkCollector.RateRun(rate, stats));
        }

        stopAll(streams);

        // Headline stats = highest rate run
        var headlineStats = rateRuns.get(rateRuns.size() - 1).statsList();
        printReport(headlineStats, rateRuns);

        try {
            new HtmlReporter().write(headlineStats, rateRuns, Path.of("results"));
        } catch (Exception e) {
            log.warn("Could not write HTML report: {}", e.getMessage());
        }
    }

    // ── pipeline lifecycle ────────────────────────────────────────────────────

    private static List<KafkaStreams> createPipelines() {
        return List.of(
            new CopartitionedKTablePipeline().build(),
            buildStreams(new GlobalKTablePipeline().buildTopology(), GlobalKTablePipeline.APPLICATION_ID),
            new KTablePipeline().build(),
            new MongoPipeline().build(),
            new WindowedMongoPipeline().build()
        );
    }

    private static void startAll(List<KafkaStreams> streams) {
        streams.forEach(KafkaStreams::start);
    }

    private static void waitForRunning(List<KafkaStreams> streams) throws InterruptedException {
        log.info("Waiting for all pipelines to reach RUNNING...");
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (streams.stream().allMatch(s -> s.state() == KafkaStreams.State.RUNNING)) return;
            Thread.sleep(500);
        }
        log.warn("Timed out waiting for pipelines to reach RUNNING");
    }

    private static void stopAll(List<KafkaStreams> streams) {
        streams.forEach(s -> s.close(Duration.ofSeconds(10)));
    }

    private static KafkaStreams buildStreams(org.apache.kafka.streams.Topology topology, String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return new KafkaStreams(topology, props);
    }

    // ── reporting ─────────────────────────────────────────────────────────────

    private static void printReport(List<BenchmarkCollector.Stats> headline,
                                    List<BenchmarkCollector.RateRun> rateRuns) {
        System.out.println();
        System.out.printf("  Window size (Mongo-Batch): %dms  |  Headline = %d rec/s run%n",
            AppConfig.WINDOW_MS, rateRuns.get(rateRuns.size() - 1).ratePerSecond());
        System.out.println("┌──────────────┬───────┬───────┬───────┬────────┬─────────┬───────┐");
        System.out.println("│          ENRICHMENT LATENCY BENCHMARK (milliseconds)            │");
        System.out.println("├──────────────┬───────┬───────┬───────┬────────┬─────────┬───────┤");
        System.out.printf( "│ %-12s │ %-5s │ %-5s │ %-5s │ %-6s │ %-7s │ %-5s │%n",
            "Strategy", "min", "mean", "p50", "p99", "p99.9", "n");
        System.out.println("├──────────────┼───────┼───────┼───────┼────────┼─────────┼───────┤");
        for (var s : headline) {
            System.out.printf("│ %-12s │ %-5.0f │ %-5.0f │ %-5.0f │ %-6.0f │ %-7.0f │ %-5d │%n",
                s.label(), s.min(), s.mean(), s.p50(), s.p99(), s.p999(), s.count());
        }
        System.out.println("└──────────────┴───────┴───────┴───────┴────────┴─────────┴───────┘");

        System.out.println();
        System.out.println("  p99 latency (ms) by target rate:");
        System.out.print(  "  Strategy      ");
        for (var run : rateRuns) System.out.printf("  %6d/s", run.ratePerSecond());
        System.out.println();
        for (int i = 0; i < LABELS.size(); i++) {
            System.out.printf("  %-14s", LABELS.get(i));
            for (var run : rateRuns) System.out.printf("  %8.0f", run.statsList().get(i).p99());
            System.out.println();
        }
        System.out.println();
    }
}
