package com.example.platform.order.saga;

import com.example.platform.order.domain.Order;
import com.example.platform.order.domain.OrderRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;

/**
 * Cancels orders whose stock reservation never got a reply. Orders stuck after stock was reserved are
 * waiting on payment, which retries and dead-letters on its own; those are exposed as a gauge for alerting
 * rather than cancelled, because a late capture must never land on a cancelled order.
 */
@Component
public class SagaWatchdog {

    private static final Duration RESERVATION_TIMEOUT = Duration.ofMinutes(15);
    private static final Duration PAYMENT_ALERT_AFTER = Duration.ofMinutes(30);

    private final OrderRepository orders;
    private final OrderSaga saga;
    private final TransactionTemplate tx;

    public SagaWatchdog(OrderRepository orders, OrderSaga saga, TransactionTemplate tx, MeterRegistry meters) {
        this.orders = orders;
        this.saga = saga;
        this.tx = tx;
        Gauge.builder("orders.saga.stuck_awaiting_payment", () ->
                        orders.countByStatusAndCreatedAtBefore(Order.Status.STOCK_RESERVED, Instant.now().minus(PAYMENT_ALERT_AFTER)))
                .register(meters);
    }

    @Scheduled(fixedDelay = 60_000)
    @SchedulerLock(name = "saga-watchdog")
    public void cancelUnreservedOrders() {
        Instant cutoff = Instant.now().minus(RESERVATION_TIMEOUT);
        for (Order order : orders.findByStatusAndCreatedAtBefore(Order.Status.PENDING, cutoff, PageRequest.of(0, 100))) {
            tx.executeWithoutResult(status -> saga.timeOut(orders.findById(order.getId()).orElseThrow()));
        }
    }
}
