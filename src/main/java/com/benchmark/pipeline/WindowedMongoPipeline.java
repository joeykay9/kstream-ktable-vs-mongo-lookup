package com.benchmark.pipeline;

import com.benchmark.config.AppConfig;
import com.benchmark.config.SerdeFactory;
import com.benchmark.model.EnrichedOrder;
import com.benchmark.model.Order;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.bson.Document;

import java.util.Properties;

public class WindowedMongoPipeline {

    public KafkaStreams build() {
        StreamsBuilder builder = new StreamsBuilder();

        MongoClient mongoClient = MongoClients.create(AppConfig.MONGO_URI);
        MongoCollection<Document> collection = mongoClient
            .getDatabase(AppConfig.MONGO_DB)
            .getCollection(AppConfig.MONGO_COLLECTION);

        ProcessorSupplier<String, Order, String, EnrichedOrder> supplier =
            () -> new BatchEnrichProcessor(collection, AppConfig.WINDOW_MS);

        builder.stream(AppConfig.TOPIC_ORDERS_RAW, Consumed.with(Serdes.String(), SerdeFactory.orderSerde()))
            .process(supplier)
            .to(AppConfig.TOPIC_ENRICHED_WINDOWED_MONGO, Produced.with(Serdes.String(), SerdeFactory.enrichedOrderSerde()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig());
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.NOT_RUNNING) mongoClient.close();
        });
        return streams;
    }

    private Properties streamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-mongo-enricher");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return props;
    }
}
