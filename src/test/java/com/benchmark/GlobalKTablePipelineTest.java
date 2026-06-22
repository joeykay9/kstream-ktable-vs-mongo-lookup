package com.benchmark;

import com.benchmark.config.AppConfig;
import com.benchmark.config.SerdeFactory;
import com.benchmark.model.EnrichedOrder;
import com.benchmark.model.Order;
import com.benchmark.model.Product;
import com.benchmark.pipeline.GlobalKTablePipeline;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalKTablePipelineTest {
    private TopologyTestDriver driver;
    private TestInputTopic<String, Product> productTopic;
    private TestInputTopic<String, Order> orderTopic;
    private TestOutputTopic<String, EnrichedOrder> outputTopic;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        driver = new TopologyTestDriver(new GlobalKTablePipeline().buildTopology(), props);

        productTopic = driver.createInputTopic(
            AppConfig.TOPIC_PRODUCTS_REF,
            Serdes.String().serializer(),
            SerdeFactory.productSerde().serializer()
        );
        // Orders arrive keyed by orderId — GlobalKTable join extracts productId from the value,
        // so no repartition is needed even though the keys don't match.
        orderTopic = driver.createInputTopic(
            AppConfig.TOPIC_ORDERS_RAW,
            Serdes.String().serializer(),
            SerdeFactory.orderSerde().serializer()
        );
        outputTopic = driver.createOutputTopic(
            AppConfig.TOPIC_ENRICHED_KTABLE_GLOBAL,
            Serdes.String().deserializer(),
            SerdeFactory.enrichedOrderSerde().deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void joinSucceedsWithoutCopartitioning() {
        productTopic.pipeInput("p1", new Product("p1", "Widget", 9.99, "Electronics"));
        // Key is orderId, not productId — proves no copartitioning requirement
        orderTopic.pipeInput("order-xyz", new Order("order-xyz", "p1", 2));

        var results = outputTopic.readValuesToList();
        assertThat(results).hasSize(1);
        EnrichedOrder enriched = results.get(0);
        assertThat(enriched.orderId).isEqualTo("order-xyz");
        assertThat(enriched.productName).isEqualTo("Widget");
        assertThat(enriched.productPrice).isEqualTo(9.99);
        assertThat(enriched.quantity).isEqualTo(2);
    }

    @Test
    void orderWithUnknownProductIsDropped() {
        orderTopic.pipeInput("o1", new Order("o1", "no-such-product", 1));
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void multipleProductsJoinCorrectly() {
        productTopic.pipeInput("p1", new Product("p1", "Widget", 9.99, "Electronics"));
        productTopic.pipeInput("p2", new Product("p2", "Gadget", 49.99, "Electronics"));

        orderTopic.pipeInput("o1", new Order("o1", "p1", 1));
        orderTopic.pipeInput("o2", new Order("o2", "p2", 3));
        orderTopic.pipeInput("o3", new Order("o3", "p1", 5));

        var results = outputTopic.readValuesToList();
        assertThat(results).hasSize(3);
        assertThat(results).extracting(e -> e.productName)
            .containsExactlyInAnyOrder("Widget", "Gadget", "Widget");
    }
}
