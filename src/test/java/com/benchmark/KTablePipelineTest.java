package com.benchmark;

import com.benchmark.config.AppConfig;
import com.benchmark.config.SerdeFactory;
import com.benchmark.model.EnrichedOrder;
import com.benchmark.model.Order;
import com.benchmark.model.Product;
import com.benchmark.pipeline.KTablePipeline;
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

class KTablePipelineTest {
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

        driver = new TopologyTestDriver(new KTablePipeline().buildTopology(), props);

        productTopic = driver.createInputTopic(
            AppConfig.TOPIC_PRODUCTS_REF,
            Serdes.String().serializer(),
            SerdeFactory.productSerde().serializer()
        );
        orderTopic = driver.createInputTopic(
            AppConfig.TOPIC_ORDERS_RAW,
            Serdes.String().serializer(),
            SerdeFactory.orderSerde().serializer()
        );
        outputTopic = driver.createOutputTopic(
            AppConfig.TOPIC_ENRICHED_KTABLE,
            Serdes.String().deserializer(),
            SerdeFactory.enrichedOrderSerde().deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void enrichedOrderContainsProductData() {
        productTopic.pipeInput("p1", new Product("p1", "Widget", 9.99, "Electronics"));
        orderTopic.pipeInput("o1", new Order("o1", "p1", 3));

        var results = outputTopic.readValuesToList();
        assertThat(results).hasSize(1);
        EnrichedOrder enriched = results.get(0);
        assertThat(enriched.orderId).isEqualTo("o1");
        assertThat(enriched.productName).isEqualTo("Widget");
        assertThat(enriched.productPrice).isEqualTo(9.99);
        assertThat(enriched.productCategory).isEqualTo("Electronics");
        assertThat(enriched.quantity).isEqualTo(3);
        assertThat(enriched.latencyMs).isGreaterThanOrEqualTo(0);
    }

    @Test
    void orderWithUnknownProductIsDropped() {
        orderTopic.pipeInput("o1", new Order("o1", "unknown-product", 1));
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void multipleOrdersJoinCorrectly() {
        productTopic.pipeInput("p1", new Product("p1", "Widget", 9.99, "Electronics"));
        productTopic.pipeInput("p2", new Product("p2", "Gadget", 49.99, "Electronics"));

        orderTopic.pipeInput("o1", new Order("o1", "p1", 1));
        orderTopic.pipeInput("o2", new Order("o2", "p2", 2));
        orderTopic.pipeInput("o3", new Order("o3", "p1", 5));

        var results = outputTopic.readValuesToList();
        assertThat(results).hasSize(3);
        assertThat(results).extracting(e -> e.productName)
            .containsExactlyInAnyOrder("Widget", "Gadget", "Widget");
    }
}
