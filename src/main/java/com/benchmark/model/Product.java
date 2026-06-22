package com.benchmark.model;

public class Product {
    public String productId;
    public String name;
    public double price;
    public String category;

    public Product() {}

    public Product(String productId, String name, double price, String category) {
        this.productId = productId;
        this.name = name;
        this.price = price;
        this.category = category;
    }
}
