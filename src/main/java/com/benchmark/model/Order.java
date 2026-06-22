package com.benchmark.model;

public class Order {
    public String orderId;
    public String productId;
    public int quantity;
    public long producedAtMs;

    public Order() {}

    public Order(String orderId, String productId, int quantity) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.producedAtMs = System.currentTimeMillis();
    }
}
