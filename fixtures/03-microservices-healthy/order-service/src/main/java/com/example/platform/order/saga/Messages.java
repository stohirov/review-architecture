package com.example.platform.order.saga;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/**
 * Commands the order saga sends and replies it receives. Versioned by type name; fields are only ever added.
 */
public final class Messages {

    private Messages() {
    }

    public static final String INVENTORY_COMMANDS = "inventory-commands";
    public static final String PAYMENT_COMMANDS = "payment-commands";
    public static final String ORDER_EVENTS = "order-events";

    public record Line(String sku, int quantity) {
    }

    public record ReserveStock(UUID orderId, List<Line> lines) {
    }

    public record ReleaseStock(UUID orderId) {
    }

    public record ChargePayment(UUID orderId, String customerId, BigDecimal amount) {
    }

    public record OrderConfirmed(UUID orderId, String customerId, BigDecimal total) {
    }

    public record OrderCancelled(UUID orderId, String reason) {
    }

    public record StockReserved(UUID orderId) {
    }

    public record StockRejected(UUID orderId, String sku) {
    }

    public record PaymentCaptured(UUID orderId, String paymentId) {
    }

    public record PaymentFailed(UUID orderId, String reason) {
    }
}
