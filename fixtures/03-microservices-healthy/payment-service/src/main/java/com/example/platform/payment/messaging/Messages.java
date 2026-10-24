package com.example.platform.payment.messaging;

import java.math.BigDecimal;
import java.util.UUID;

public final class Messages {

    private Messages() {
    }

    public static final String PAYMENT_EVENTS = "payment-events";

    public record ChargePayment(UUID orderId, String customerId, BigDecimal amount) {
    }

    public record PaymentCaptured(UUID orderId, String paymentId) {
    }

    public record PaymentFailed(UUID orderId, String reason) {
    }
}
