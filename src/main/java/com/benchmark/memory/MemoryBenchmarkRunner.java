package com.benchmark.memory;

import com.benchmark.config.AppConfig;
import com.benchmark.producer.DataSetup;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MemoryBenchmarkRunner {
    private static final Logger log = LoggerFactory.getLogger(MemoryBenchmarkRunner.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final List<String> STRATEGIES =
        List.of("ktable-co", "global-ktable", "ktable", "mongo-batch", "mongo-sync");
    private static final List<String> LABELS =
        List.of("KTable-Co", "GlobalKTable", "KTable", "Mongo-Batch", "Mongo-Sync");

    public static void main(String[] args) throws Exception {
        int[] productCounts = AppConfig.MEMORY_PRODUCT_COUNTS;
        log.info("=== Memory Benchmark: state store footprint vs dataset size ===");
        log.info("Dataset sizes: {} strategies, {} product counts",
            STRATEGIES.size(), productCounts.length);

        DataSetup setup = new DataSetup();

        // Wipe any internal Kafka topics left over from a previous memory benchmark run so
        // KTable pipelines always rebuild state from a fresh products-ref changelog.
        setup.clearMemoryBenchmarkTopics();

        List<MemoryResult> results = new ArrayList<>();

        for (int productCount : productCounts) {
            log.info("--- Dataset: {} products ---", productCount);
            // Seeds products-ref + MongoDB, deletes/recreates all other topics, clears local state.
            setup.setupAll(productCount);

            for (int i = 0; i < STRATEGIES.size(); i++) {
                String strategy = STRATEGIES.get(i);
                String label    = LABELS.get(i);
                log.info("  [{}] launching child JVM...", label);

                int     port  = findFreePort();
                Process child = launchChild(strategy, productCount, port);
                MemoryStats stats;
                try {
                    waitForReady(port, 300_000);
                    stats = readMetrics(port);
                    log.info("  [{}] heap={}MB  rss={}MB  rocksdb-live={}MB",
                        label, stats.heapUsedMb(), stats.rssMb(), stats.rocksdbLiveDataMb());
                } catch (Exception e) {
                    log.error("  [{}] failed to read metrics: {}", label, e.getMessage());
                    stats = new MemoryStats(0, 0, 0, 0);
                } finally {
                    child.destroyForcibly();
                    child.waitFor(10, TimeUnit.SECONDS);
                }
                results.add(new MemoryResult(label, productCount, stats));
            }
        }

        printResults(results, LABELS, productCounts);

        try {
            Path out = new MemoryHtmlReporter().write(results, LABELS, productCounts, Path.of("results"));
            System.out.println("HTML report → file://" + out.toAbsolutePath());
        } catch (Exception e) {
            log.warn("Could not write HTML report: {}", e.getMessage());
        }
    }

    // ── child process ─────────────────────────────────────────────────────────

    private static Process launchChild(String strategy, int productCount, int port) throws Exception {
        String javaCmd    = ProcessHandle.current().info().command().orElse("java");
        String classpath  = System.getProperty("java.class.path");
        List<String> cmd  = new ArrayList<>(List.of(
            javaCmd, "-cp", classpath,
            "-Xmx768m",
            "com.benchmark.memory.StrategyApp",
            "--strategy",      strategy,
            "--product-count", String.valueOf(productCount),
            "--port",          String.valueOf(port)
        ));
        return new ProcessBuilder(cmd)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void waitForReady(int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpURLConnection conn = (HttpURLConnection)
                    new URL("http://localhost:" + port + "/ready").openConnection();
                conn.setConnectTimeout(1_000);
                conn.setReadTimeout(1_000);
                if (conn.getResponseCode() == 200) return;
            } catch (Exception ignored) {}
            Thread.sleep(1_000);
        }
        throw new TimeoutException("Strategy app did not become ready (port " + port + ")");
    }

    private static MemoryStats readMetrics(int port) throws Exception {
        HttpURLConnection conn = (HttpURLConnection)
            new URL("http://localhost:" + port + "/metrics").openConnection();
        conn.setReadTimeout(5_000);
        String json = new String(conn.getInputStream().readAllBytes());
        @SuppressWarnings("unchecked")
        Map<String, Number> m = MAPPER.readValue(json, Map.class);
        return new MemoryStats(
            m.get("heapUsedMb").longValue(),
            m.get("rocksdbBlockCacheMb").longValue(),
            m.get("rocksdbLiveDataMb").longValue(),
            m.get("rssMb").longValue()
        );
    }

    // ── stdout report ─────────────────────────────────────────────────────────

    private static void printResults(List<MemoryResult> results, List<String> labels, int[] productCounts) {
        System.out.println();
        printTable(results, labels, productCounts, "RSS (MB) — process memory including RocksDB off-heap",
            r -> r.stats().rssMb() + "MB");
        System.out.println();
        printTable(results, labels, productCounts, "RocksDB live data (MB) — state store on-disk footprint",
            r -> r.stats().rocksdbLiveDataMb() + "MB");
        System.out.println();
    }

    private static void printTable(List<MemoryResult> results, List<String> labels,
                                   int[] productCounts, String title,
                                   java.util.function.Function<MemoryResult, String> cell) {
        System.out.println("  " + title);
        System.out.printf("  %-14s", "Strategy");
        for (int pc : productCounts) System.out.printf("  %8s", formatCount(pc));
        System.out.println();
        for (String label : labels) {
            System.out.printf("  %-14s", label);
            for (int pc : productCounts) {
                MemoryResult r = find(results, label, pc);
                System.out.printf("  %8s", r != null ? cell.apply(r) : "n/a");
            }
            System.out.println();
        }
    }

    static MemoryResult find(List<MemoryResult> results, String label, int productCount) {
        return results.stream()
            .filter(r -> r.strategy().equals(label) && r.productCount() == productCount)
            .findFirst().orElse(null);
    }

    static String formatCount(int n) {
        if (n >= 1_000_000) return (n / 1_000_000) + "M";
        if (n >= 1_000)     return (n / 1_000) + "k";
        return String.valueOf(n);
    }
}
