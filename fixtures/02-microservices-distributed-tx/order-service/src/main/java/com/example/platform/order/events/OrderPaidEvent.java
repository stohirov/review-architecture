package com.example.platform.order.events;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderPaidEvent(Long orderId, String customerId, BigDecimal total, Instant paidAt) {
}
