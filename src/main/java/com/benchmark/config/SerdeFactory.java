package com.benchmark.config;

import com.benchmark.model.EnrichedOrder;
import com.benchmark.model.Order;
import com.benchmark.model.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public final class SerdeFactory {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Serde<Order> orderSerde() { return jsonSerde(Order.class); }
    public static Serde<Product> productSerde() { return jsonSerde(Product.class); }
    public static Serde<EnrichedOrder> enrichedOrderSerde() { return jsonSerde(EnrichedOrder.class); }

    private static <T> Serde<T> jsonSerde(Class<T> clazz) {
        Serializer<T> serializer = (topic, data) -> {
            if (data == null) return null;
            try { return MAPPER.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
        Deserializer<T> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try { return MAPPER.readValue(bytes, clazz); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
