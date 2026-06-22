package com.benchmark.pipeline;

import com.benchmark.config.AppConfig;
import com.benchmark.config.SerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * KTable join where the stream is already keyed by productId at produce time.
 *
 * Both orders-by-product and products-ref have 4 partitions and use the default
 * (murmur2) partitioner on productId, so Kafka Streams assigns the same task to
 * matching partitions. No selectKey → no internal repartition topic → one fewer
 * Kafka round-trip per record compared to KTablePipeline.
 */
public class CopartitionedKTablePipeline {

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, com.benchmark.model.Product> products = builder.table(
            AppConfig.TOPIC_PRODUCTS_REF,
            Consumed.with(Serdes.String(), SerdeFactory.productSerde())
        );

        // Key is already productId — join is local within each task, no repartition.
        builder.stream(AppConfig.TOPIC_ORDERS_BY_PRODUCT, Consumed.with(Serdes.String(), SerdeFactory.orderSerde()))
            .join(products, KTablePipeline::enrich)
            .to(AppConfig.TOPIC_ENRICHED_KTABLE_COPART, Produced.with(Serdes.String(), SerdeFactory.enrichedOrderSerde()));

        return builder.build();
    }

    public KafkaStreams build() {
        return new KafkaStreams(buildTopology(), streamsConfig());
    }

    private Properties streamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "copartitioned-ktable-enricher");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return props;
    }
}
