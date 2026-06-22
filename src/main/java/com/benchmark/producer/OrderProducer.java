package com.benchmark.producer;

import com.benchmark.config.AppConfig;
import com.benchmark.model.Order;
import com.benchmark.model.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OrderProducer {
    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public void produce(List<Product> products, int count) throws Exception {
        produceAtRate(products, count, Integer.MAX_VALUE);
    }

    public void produce(List<Product> products, int count, int targetRatePerSecond) throws Exception {
        produceAtRate(products, count, targetRatePerSecond);
    }

    private void produceAtRate(List<Product> products, int count, int targetRatePerSecond) throws Exception {
        boolean rateLimit = targetRatePerSecond < Integer.MAX_VALUE;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        // Disable linger when rate-limiting so sleep-based pacing isn't undermined by batching delays
        props.put(ProducerConfig.LINGER_MS_CONFIG, rateLimit ? 0 : 5);

        Random rng = new Random();
        long startNs   = System.nanoTime();
        long intervalNs = rateLimit ? 1_000_000_000L / targetRatePerSecond : 0;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String orderId   = UUID.randomUUID().toString();
                String productId = products.get(rng.nextInt(products.size())).productId;
                Order  order     = new Order(orderId, productId, rng.nextInt(10) + 1);
                String json      = MAPPER.writeValueAsString(order);
                producer.send(new ProducerRecord<>(AppConfig.TOPIC_ORDERS_RAW,        orderId,   json));
                producer.send(new ProducerRecord<>(AppConfig.TOPIC_ORDERS_BY_PRODUCT, productId, json));

                if (rateLimit) {
                    long targetNs = startNs + (long)(i + 1) * intervalNs;
                    long sleepNs  = targetNs - System.nanoTime();
                    if (sleepNs > 1_000_000L)
                        Thread.sleep(sleepNs / 1_000_000L, (int)(sleepNs % 1_000_000L));
                }
            }
            producer.flush();
        }
        log.info("Produced {} orders at target {} rec/s", count,
            rateLimit ? targetRatePerSecond : "unlimited");
    }
}
