package com.example.platform.inventory.messaging;

public class InsufficientStockException extends RuntimeException {

    public InsufficientStockException(String sku, Long orderId) {
        super("Insufficient stock for " + sku + " (order " + orderId + ")");
    }
}
