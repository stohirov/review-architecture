package com.example.platform.inventory.messaging;

import java.util.List;
import java.util.UUID;

public final class Messages {

    private Messages() {
    }

    public static final String INVENTORY_EVENTS = "inventory-events";

    public record Line(String sku, int quantity) {
    }

    public record ReserveStock(UUID orderId, List<Line> lines) {
    }

    public record ReleaseStock(UUID orderId) {
    }

    public record StockReserved(UUID orderId) {
    }

    public record StockRejected(UUID orderId, String sku) {
    }
}
