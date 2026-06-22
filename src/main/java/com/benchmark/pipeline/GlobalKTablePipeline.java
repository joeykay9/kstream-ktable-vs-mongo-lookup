package com.benchmark.pipeline;

import com.benchmark.config.AppConfig;
import com.benchmark.config.SerdeFactory;
import com.benchmark.model.Order;
import com.benchmark.model.Product;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Produced;

public class GlobalKTablePipeline {
    public static final String APPLICATION_ID = "global-ktable-enricher";

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // GlobalKTable replicates all partitions to every instance — no copartitioning needed.
        // The join key is extracted from the stream value (order.productId) at join time,
        // so orders-raw can stay keyed by orderId with no selectKey repartition.
        GlobalKTable<String, Product> productsTable = builder.globalTable(
            AppConfig.TOPIC_PRODUCTS_REF,
            Consumed.with(Serdes.String(), SerdeFactory.productSerde())
        );

        builder.stream(AppConfig.TOPIC_ORDERS_RAW,
                Consumed.with(Serdes.String(), SerdeFactory.orderSerde()))
            .join(productsTable,
                (orderId, order) -> order.productId,
                KTablePipeline::enrich)
            .to(AppConfig.TOPIC_ENRICHED_KTABLE_GLOBAL,
                Produced.with(Serdes.String(), SerdeFactory.enrichedOrderSerde()));

        return builder.build();
    }
}
