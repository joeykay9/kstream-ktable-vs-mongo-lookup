package com.benchmark.memory;

import com.benchmark.config.AppConfig;
import com.benchmark.pipeline.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Child JVM entry point for the memory benchmark.
 *
 * Starts one pipeline strategy, waits for RUNNING, forces GC, then exposes
 * /ready and /metrics over HTTP until the parent kills this process.
 *
 * Args: --strategy <name>  --product-count <n>  --port <n>
 */
public class StrategyApp {
    private static final Logger log = LoggerFactory.getLogger(StrategyApp.class);

    public static void main(String[] args) throws Exception {
        String strategy    = requireArg(args, "--strategy");
        int    productCount = Integer.parseInt(requireArg(args, "--product-count"));
        int    port         = Integer.parseInt(requireArg(args, "--port"));

        log.info("StrategyApp starting: strategy={} productCount={} port={}", strategy, productCount, port);

        KafkaStreams streams = buildStreams(strategy, productCount);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("StrategyApp shutting down");
            streams.close(Duration.ofSeconds(10));
        }));

        streams.start();
        waitForRunning(streams, 300_000);

        Thread.sleep(2_000);   // let RocksDB compaction settle
        System.gc();
        Thread.sleep(500);

        MetricsServer server = new MetricsServer(port, streams);
        log.info("StrategyApp ready on port {}", port);

        Thread.currentThread().join();
    }

    private static KafkaStreams buildStreams(String strategy, int productCount) {
        // KTable strategies get unique app IDs per (strategy, productCount) so each run
        // uses its own state directory and internal changelog topics.
        // Mongo strategies have no state store, so the hardcoded IDs from their build() are fine.
        return switch (strategy) {
            case "ktable-co"    -> new KafkaStreams(
                new CopartitionedKTablePipeline().buildTopology(),
                memConfig("mem-ktable-co-"     + productCount));
            case "global-ktable" -> new KafkaStreams(
                new GlobalKTablePipeline().buildTopology(),
                memConfig("mem-global-ktable-" + productCount));
            case "ktable"       -> new KafkaStreams(
                new KTablePipeline().buildTopology(),
                memConfig("mem-ktable-"        + productCount));
            case "mongo-batch"  -> new WindowedMongoPipeline().build();
            case "mongo-sync"   -> new MongoPipeline().build();
            default -> throw new IllegalArgumentException("Unknown strategy: " + strategy);
        };
    }

    private static Properties memConfig(String appId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,           appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,        AppConfig.BOOTSTRAP_SERVERS);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // DEBUG exposes per-store RocksDB metrics (block-cache-usage, estimate-live-data-size)
        p.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG,  "DEBUG");
        return p;
    }

    private static void waitForRunning(KafkaStreams streams, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (streams.state() == KafkaStreams.State.RUNNING) return;
            Thread.sleep(500);
        }
        throw new IllegalStateException("Pipeline did not reach RUNNING within " + timeoutMs + "ms");
    }

    private static String requireArg(String[] args, String name) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        throw new IllegalArgumentException("Missing required argument: " + name);
    }
}
