package com.benchmark.producer;

import com.benchmark.config.AppConfig;
import com.benchmark.model.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DataSetup {
    private static final Logger log = LoggerFactory.getLogger(DataSetup.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String[] CATEGORIES = {"Electronics", "Clothing", "Books", "Food", "Toys"};

    public List<Product> setupAll() throws Exception {
        List<Product> products = generateProducts();
        createTopics();
        loadProductsToKafka(products);
        loadProductsToMongo(products);
        return products;
    }

    private List<Product> generateProducts() {
        List<Product> products = new ArrayList<>();
        Random rng = new Random(42);
        for (int i = 0; i < AppConfig.PRODUCT_COUNT; i++) {
            products.add(new Product(
                "product-" + i,
                "Product " + i,
                1.0 + rng.nextDouble() * 999.0,
                CATEGORIES[i % CATEGORIES.length]
            ));
        }
        return products;
    }

    private void createTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", AppConfig.BOOTSTRAP_SERVERS);
        try (AdminClient admin = AdminClient.create(props)) {
            List<NewTopic> topics = List.of(
                compactedTopic(AppConfig.TOPIC_PRODUCTS_REF, 4),
                new NewTopic(AppConfig.TOPIC_ORDERS_RAW, 4, (short) 1),
                // Must match products-ref partition count (4) for copartitioned join
                new NewTopic(AppConfig.TOPIC_ORDERS_BY_PRODUCT, 4, (short) 1),
                new NewTopic(AppConfig.TOPIC_ENRICHED_KTABLE, 4, (short) 1),
                new NewTopic(AppConfig.TOPIC_ENRICHED_KTABLE_COPART, 4, (short) 1),
                new NewTopic(AppConfig.TOPIC_ENRICHED_MONGO, 4, (short) 1),
                new NewTopic(AppConfig.TOPIC_ENRICHED_WINDOWED_MONGO, 4, (short) 1)
            );
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            log.info("Topics created");
        } catch (Exception e) {
            log.warn("Topic creation skipped (may already exist): {}", e.getMessage());
        }
    }

    private NewTopic compactedTopic(String name, int partitions) {
        NewTopic topic = new NewTopic(name, partitions, (short) 1);
        topic.configs(Map.of("cleanup.policy", "compact", "segment.ms", "60000"));
        return topic;
    }

    private void loadProductsToKafka(List<Product> products) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (Product p : products) {
                producer.send(new ProducerRecord<>(AppConfig.TOPIC_PRODUCTS_REF, p.productId, MAPPER.writeValueAsString(p)));
            }
            producer.flush();
        }
        log.info("Loaded {} products into {}", products.size(), AppConfig.TOPIC_PRODUCTS_REF);
    }

    private void loadProductsToMongo(List<Product> products) {
        try (MongoClient client = MongoClients.create(AppConfig.MONGO_URI)) {
            MongoCollection<Document> col = client
                .getDatabase(AppConfig.MONGO_DB)
                .getCollection(AppConfig.MONGO_COLLECTION);
            col.drop();
            col.insertMany(products.stream().map(p -> new Document()
                .append("productId", p.productId)
                .append("name", p.name)
                .append("price", p.price)
                .append("category", p.category)
            ).collect(Collectors.toList()));
            col.createIndex(new Document("productId", 1));
        }
        log.info("Loaded {} products into MongoDB, indexed by productId", products.size());
    }
}
