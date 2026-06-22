package com.benchmark.pipeline;

import com.benchmark.model.EnrichedOrder;
import com.benchmark.model.Order;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Buffers incoming orders for {@code windowMs} milliseconds, then issues a single
 * {@code $in} query to MongoDB for all unique productIds in the window.
 *
 * Trade-off: fewer MongoDB round-trips at the cost of a latency floor equal to
 * the window size. Every record waits up to {@code windowMs} before being forwarded.
 */
class BatchEnrichProcessor implements Processor<String, Order, String, EnrichedOrder> {
    private static final Logger log = LoggerFactory.getLogger(BatchEnrichProcessor.class);

    private final MongoCollection<Document> collection;
    private final long windowMs;
    private ProcessorContext<String, EnrichedOrder> context;
    private final List<Record<String, Order>> buffer = new ArrayList<>();

    BatchEnrichProcessor(MongoCollection<Document> collection, long windowMs) {
        this.collection = collection;
        this.windowMs = windowMs;
    }

    @Override
    public void init(ProcessorContext<String, EnrichedOrder> context) {
        this.context = context;
        context.schedule(Duration.ofMillis(windowMs), PunctuationType.WALL_CLOCK_TIME, ts -> flush());
    }

    @Override
    public void process(Record<String, Order> record) {
        buffer.add(record);
    }

    @Override
    public void close() {
        flush(); // drain any remaining records on shutdown
    }

    private void flush() {
        if (buffer.isEmpty()) return;

        List<String> productIds = buffer.stream()
            .map(r -> r.value().productId)
            .distinct()
            .collect(Collectors.toList());

        // Single $in query replaces N individual findOne calls
        Map<String, Document> productMap = new HashMap<>();
        collection.find(Filters.in("productId", productIds))
            .forEach(doc -> productMap.put(doc.getString("productId"), doc));

        long enrichedAt = System.currentTimeMillis();
        for (Record<String, Order> rec : buffer) {
            Order order = rec.value();
            Document doc = productMap.get(order.productId);
            if (doc == null) continue;
            EnrichedOrder e = new EnrichedOrder();
            e.orderId = order.orderId;
            e.productId = order.productId;
            e.quantity = order.quantity;
            e.producedAtMs = order.producedAtMs;
            e.productName = doc.getString("name");
            e.productPrice = doc.getDouble("price");
            e.productCategory = doc.getString("category");
            e.enrichedAtMs = enrichedAt;
            e.latencyMs = enrichedAt - order.producedAtMs;
            context.forward(new Record<>(rec.key(), e, enrichedAt));
        }
        log.debug("Flushed batch: {} orders, {} unique product lookups", buffer.size(), productIds.size());
        buffer.clear();
    }
}
