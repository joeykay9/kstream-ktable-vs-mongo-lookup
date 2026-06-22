package com.benchmark;

import com.benchmark.config.AppConfig;
import com.benchmark.model.EnrichedOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class BenchmarkCollector {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkCollector.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record Stats(String label, List<Long> latencies) {
        long count() { return latencies.size(); }
        double min()  { return latencies.isEmpty() ? 0 : latencies.get(0); }
        double max()  { return latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1); }
        double mean() { return latencies.stream().mapToLong(Long::longValue).average().orElse(0); }
        double p50()  { return percentile(50); }
        double p99()  { return percentile(99); }
        double p999() { return percentile(99.9); }

        private double percentile(double p) {
            if (latencies.isEmpty()) return 0;
            int idx = (int) Math.ceil(p / 100.0 * latencies.size()) - 1;
            return latencies.get(Math.max(0, Math.min(idx, latencies.size() - 1)));
        }
    }

    public Stats collect(String topic, String label, int expectedCount, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "collector-" + label + "-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        List<Long> latencies = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (latencies.size() < expectedCount && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    try {
                        latencies.add(MAPPER.readValue(r.value(), EnrichedOrder.class).latencyMs);
                    } catch (Exception ignored) {}
                });
            }
        }

        Collections.sort(latencies);
        log.info("Collected {}/{} results from {}", latencies.size(), expectedCount, topic);
        return new Stats(label, latencies);
    }
}
