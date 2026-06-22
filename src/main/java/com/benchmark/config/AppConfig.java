package com.benchmark.config;

public final class AppConfig {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String MONGO_URI = "mongodb://localhost:27017";
    public static final String MONGO_DB = "benchmark";
    public static final String MONGO_COLLECTION = "products";

    public static final String TOPIC_ORDERS_RAW = "orders-raw";
    // Produced keyed by productId so it is copartitioned with products-ref (same key, same partition count).
    // Eliminates the selectKey repartition hop required by TOPIC_ORDERS_RAW.
    public static final String TOPIC_ORDERS_BY_PRODUCT = "orders-by-product";
    public static final String TOPIC_PRODUCTS_REF = "products-ref";
    public static final String TOPIC_ENRICHED_KTABLE = "orders-enriched-ktable";
    public static final String TOPIC_ENRICHED_KTABLE_COPART = "orders-enriched-ktable-copart";
    public static final String TOPIC_ENRICHED_MONGO = "orders-enriched-mongo";
    public static final String TOPIC_ENRICHED_WINDOWED_MONGO = "orders-enriched-windowed-mongo";

    public static final int PRODUCT_COUNT = 1_000;
    public static final int ORDER_COUNT = 10_000;
    // How long the windowed batch pipeline buffers records before flushing to MongoDB
    public static final long WINDOW_MS = 100L;

    private AppConfig() {}
}
