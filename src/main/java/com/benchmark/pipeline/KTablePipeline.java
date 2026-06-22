package com.benchmark.pipeline;

import com.benchmark.config.AppConfig;
import com.benchmark.config.SerdeFactory;
import com.benchmark.model.EnrichedOrder;
import com.benchmark.model.Order;
import com.benchmark.model.Product;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KTablePipeline {

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Product> products = builder.table(
            AppConfig.TOPIC_PRODUCTS_REF,
            Consumed.with(Serdes.String(), SerdeFactory.productSerde())
        );

        // Orders arrive keyed by orderId; re-key by productId to join with KTable.
        // Kafka Streams automatically repartitions through an internal topic after selectKey.
        builder.stream(AppConfig.TOPIC_ORDERS_RAW, Consumed.with(Serdes.String(), SerdeFactory.orderSerde()))
            .selectKey((k, order) -> order.productId)
            .join(products, KTablePipeline::enrich)
            .to(AppConfig.TOPIC_ENRICHED_KTABLE, Produced.with(Serdes.String(), SerdeFactory.enrichedOrderSerde()));

        return builder.build();
    }

    public KafkaStreams build() {
        return new KafkaStreams(buildTopology(), streamsConfig());
    }

    static EnrichedOrder enrich(Order order, Product product) {
        EnrichedOrder e = new EnrichedOrder();
        e.orderId = order.orderId;
        e.productId = order.productId;
        e.quantity = order.quantity;
        e.producedAtMs = order.producedAtMs;
        e.productName = product.name;
        e.productPrice = product.price;
        e.productCategory = product.category;
        e.enrichedAtMs = System.currentTimeMillis();
        e.latencyMs = e.enrichedAtMs - e.producedAtMs;
        return e;
    }

    private Properties streamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-enricher");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // disable buffering for latency accuracy
        return props;
    }
}
