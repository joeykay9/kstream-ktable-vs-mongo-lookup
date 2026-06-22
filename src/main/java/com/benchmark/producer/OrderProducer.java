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
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        Random rng = new Random();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String orderId = UUID.randomUUID().toString();
                String productId = products.get(rng.nextInt(products.size())).productId;
                Order order = new Order(orderId, productId, rng.nextInt(10) + 1);
                String json = MAPPER.writeValueAsString(order);
                // Keyed by orderId — requires selectKey repartition in KTablePipeline
                producer.send(new ProducerRecord<>(AppConfig.TOPIC_ORDERS_RAW, orderId, json));
                // Keyed by productId — copartitioned with products-ref, no repartition needed
                producer.send(new ProducerRecord<>(AppConfig.TOPIC_ORDERS_BY_PRODUCT, productId, json));
            }
            producer.flush();
        }
        log.info("Produced {} orders to {} and {}", count, AppConfig.TOPIC_ORDERS_RAW, AppConfig.TOPIC_ORDERS_BY_PRODUCT);
    }
}
