package com.example.platform.inventory.messaging;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderPaidEvent(Long orderId, String customerId, BigDecimal total, Instant paidAt) {
}
