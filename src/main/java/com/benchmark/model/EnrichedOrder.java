package com.benchmark.model;

public class EnrichedOrder {
    public String orderId;
    public String productId;
    public String productName;
    public double productPrice;
    public String productCategory;
    public int quantity;
    public long producedAtMs;
    public long enrichedAtMs;
    public long latencyMs;

    public EnrichedOrder() {}
}
