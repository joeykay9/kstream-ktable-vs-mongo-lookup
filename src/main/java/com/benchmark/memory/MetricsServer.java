package com.benchmark.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

public class MetricsServer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final HttpServer server;

    public MetricsServer(int port, KafkaStreams streams) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/ready", exchange -> {
            byte[] body = "OK".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });

        server.createContext("/metrics", exchange -> {
            try {
                byte[] body = MAPPER.writeValueAsBytes(snapshot(streams));
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.getResponseBody().close();
            } catch (Exception e) {
                exchange.sendResponseHeaders(500, 0);
                exchange.getResponseBody().close();
            }
        });

        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    private Map<String, Long> snapshot(KafkaStreams streams) {
        Runtime rt = Runtime.getRuntime();
        long heapUsedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long rocksdbBlockCacheMb = sumMetric(streams, "rocksdb-block-cache-usage") / (1024 * 1024);
        long rocksdbLiveDataMb   = sumMetric(streams, "rocksdb-estimate-live-data-size") / (1024 * 1024);
        long rssMb = readRssMb();

        Map<String, Long> m = new LinkedHashMap<>();
        m.put("heapUsedMb",           heapUsedMb);
        m.put("rocksdbBlockCacheMb",   rocksdbBlockCacheMb);
        m.put("rocksdbLiveDataMb",     rocksdbLiveDataMb);
        m.put("rssMb",                 rssMb);
        return m;
    }

    private long sumMetric(KafkaStreams streams, String metricName) {
        return streams.metrics().entrySet().stream()
            .filter(e -> e.getKey().name().equals(metricName))
            .mapToLong(e -> {
                Object v = e.getValue().metricValue();
                return v instanceof Number n ? n.longValue() : 0L;
            })
            .sum();
    }

    private long readRssMb() {
        try {
            long pid = ProcessHandle.current().pid();
            Process p = new ProcessBuilder("ps", "-o", "rss=", "-p", String.valueOf(pid)).start();
            String line = new String(p.getInputStream().readAllBytes()).trim();
            return line.isEmpty() ? 0L : Long.parseLong(line) / 1024L;
        } catch (Exception e) {
            return 0L;
        }
    }
}
