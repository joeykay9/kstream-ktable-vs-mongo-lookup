package com.benchmark;

import com.benchmark.config.AppConfig;
import com.benchmark.pipeline.CopartitionedKTablePipeline;
import com.benchmark.pipeline.KTablePipeline;
import com.benchmark.pipeline.MongoPipeline;
import com.benchmark.pipeline.WindowedMongoPipeline;
import com.benchmark.producer.DataSetup;
import com.benchmark.producer.OrderProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public class BenchmarkRunner {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    public static void main(String[] args) throws Exception {
        log.info("=== KStream KTable vs MongoDB Lookup Benchmark ===");
        log.info("Products: {}, Orders: {}, Window: {}ms", AppConfig.PRODUCT_COUNT, AppConfig.ORDER_COUNT, AppConfig.WINDOW_MS);

        var products = new DataSetup().setupAll();

        KafkaStreams copartStreams  = new CopartitionedKTablePipeline().build();
        KafkaStreams ktableStreams  = new KTablePipeline().build();
        KafkaStreams mongoStreams   = new MongoPipeline().build();
        KafkaStreams windowedStreams = new WindowedMongoPipeline().build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            copartStreams.close(Duration.ofSeconds(5));
            ktableStreams.close(Duration.ofSeconds(5));
            mongoStreams.close(Duration.ofSeconds(5));
            windowedStreams.close(Duration.ofSeconds(5));
        }));

        copartStreams.start();
        ktableStreams.start();
        mongoStreams.start();
        windowedStreams.start();

        log.info("Waiting for all pipelines to reach RUNNING state...");
        long deadline = System.currentTimeMillis() + 60_000;
        while ((copartStreams.state()   != KafkaStreams.State.RUNNING ||
                ktableStreams.state()   != KafkaStreams.State.RUNNING ||
                mongoStreams.state()    != KafkaStreams.State.RUNNING ||
                windowedStreams.state() != KafkaStreams.State.RUNNING) &&
               System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
        }
        Thread.sleep(3_000); // allow both KTables to finish changelog replay

        log.info("Producing {} orders...", AppConfig.ORDER_COUNT);
        new OrderProducer().produce(products, AppConfig.ORDER_COUNT);

        var collector  = new BenchmarkCollector();
        long timeoutMs = 120_000;

        log.info("Collecting KTable-Co results (no repartition)...");
        var copartStats = collector.collect(AppConfig.TOPIC_ENRICHED_KTABLE_COPART, "KTable-Co", AppConfig.ORDER_COUNT, timeoutMs);

        log.info("Collecting KTable results (with repartition)...");
        var ktableStats = collector.collect(AppConfig.TOPIC_ENRICHED_KTABLE, "KTable", AppConfig.ORDER_COUNT, timeoutMs);

        log.info("Collecting Mongo-Batch results...");
        var windowedStats = collector.collect(AppConfig.TOPIC_ENRICHED_WINDOWED_MONGO, "Mongo-Batch", AppConfig.ORDER_COUNT, timeoutMs);

        log.info("Collecting Mongo-Sync results...");
        var mongoStats = collector.collect(AppConfig.TOPIC_ENRICHED_MONGO, "Mongo-Sync", AppConfig.ORDER_COUNT, timeoutMs);

        var statsList = List.of(copartStats, ktableStats, windowedStats, mongoStats);
        printReport(statsList);

        try {
            new HtmlReporter().write(statsList, Path.of("results"));
        } catch (Exception e) {
            log.warn("Could not write HTML report: {}", e.getMessage());
        }

        copartStreams.close(Duration.ofSeconds(10));
        ktableStreams.close(Duration.ofSeconds(10));
        mongoStreams.close(Duration.ofSeconds(10));
        windowedStreams.close(Duration.ofSeconds(10));
    }

    private static void printReport(List<BenchmarkCollector.Stats> statsList) {
        System.out.println();
        System.out.printf("  Window size (Mongo-Batch): %dms%n", AppConfig.WINDOW_MS);
        System.out.println("┌──────────────┬───────┬───────┬───────┬────────┬─────────┬───────┐");
        System.out.println("│          ENRICHMENT LATENCY BENCHMARK (milliseconds)            │");
        System.out.println("├──────────────┬───────┬───────┬───────┬────────┬─────────┬───────┤");
        System.out.printf( "│ %-12s │ %-5s │ %-5s │ %-5s │ %-6s │ %-7s │ %-5s │%n",
            "Strategy", "min", "mean", "p50", "p99", "p99.9", "n");
        System.out.println("├──────────────┼───────┼───────┼───────┼────────┼─────────┼───────┤");
        for (var s : statsList) {
            System.out.printf("│ %-12s │ %-5.0f │ %-5.0f │ %-5.0f │ %-6.0f │ %-7.0f │ %-5d │%n",
                s.label(), s.min(), s.mean(), s.p50(), s.p99(), s.p999(), s.count());
        }
        System.out.println("└──────────────┴───────┴───────┴───────┴────────┴─────────┴───────┘");
        System.out.println();
    }
}
