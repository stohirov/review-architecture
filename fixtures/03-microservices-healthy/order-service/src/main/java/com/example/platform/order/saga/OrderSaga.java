package com.example.platform.order.saga;

import com.example.platform.order.domain.Order;
import com.example.platform.order.domain.OrderRepository;
import com.example.platform.order.outbox.Outbox;
import com.example.platform.order.saga.Messages.ChargePayment;
import com.example.platform.order.saga.Messages.Line;
import com.example.platform.order.saga.Messages.OrderCancelled;
import com.example.platform.order.saga.Messages.OrderConfirmed;
import com.example.platform.order.saga.Messages.ReleaseStock;
import com.example.platform.order.saga.Messages.ReserveStock;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static com.example.platform.order.saga.Messages.INVENTORY_COMMANDS;
import static com.example.platform.order.saga.Messages.ORDER_EVENTS;
import static com.example.platform.order.saga.Messages.PAYMENT_COMMANDS;

/**
 * Orchestrates place-order: reserve stock, then charge, compensating with ReleaseStock when payment fails.
 * Order.status is the saga state; each step runs in the transaction that consumed the reply.
 */
@Component
public class OrderSaga {

    private final OrderRepository orders;
    private final Outbox outbox;

    public OrderSaga(OrderRepository orders, Outbox outbox) {
        this.orders = orders;
        this.outbox = outbox;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void start(Order order) {
        outbox.publish(INVENTORY_COMMANDS, key(order.getId()), new ReserveStock(order.getId(),
                order.getItems().stream().map(i -> new Line(i.getSku(), i.getQuantity())).toList()));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void onStockReserved(UUID orderId) {
        Order order = load(orderId);
        if (order.stockReserved()) {
            outbox.publish(PAYMENT_COMMANDS, key(orderId), new ChargePayment(orderId, order.getCustomerId(), order.getTotal()));
        } else if (order.getStatus() == Order.Status.CANCELLED) {
            // Reply arrived after the watchdog timed the order out: give the stock back.
            outbox.publish(INVENTORY_COMMANDS, key(orderId), new ReleaseStock(orderId));
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void timeOut(Order order) {
        if (order.cancel("timed out waiting for stock reservation")) {
            outbox.publish(ORDER_EVENTS, key(order.getId()), new OrderCancelled(order.getId(), order.getCancelReason()));
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void onStockRejected(UUID orderId, String sku) {
        Order order = load(orderId);
        if (order.cancel("out of stock: " + sku)) {
            outbox.publish(ORDER_EVENTS, key(orderId), new OrderCancelled(orderId, order.getCancelReason()));
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void onPaymentCaptured(UUID orderId, String paymentId) {
        Order order = load(orderId);
        if (order.confirm(paymentId)) {
            outbox.publish(ORDER_EVENTS, key(orderId), new OrderConfirmed(orderId, order.getCustomerId(), order.getTotal()));
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void onPaymentFailed(UUID orderId, String reason) {
        Order order = load(orderId);
        if (order.cancel("payment failed: " + reason)) {
            outbox.publish(INVENTORY_COMMANDS, key(orderId), new ReleaseStock(orderId));
            outbox.publish(ORDER_EVENTS, key(orderId), new OrderCancelled(orderId, order.getCancelReason()));
        }
    }

    private Order load(UUID orderId) {
        return orders.findById(orderId).orElseThrow(() -> new IllegalStateException("Unknown order " + orderId));
    }

    private static String key(UUID orderId) {
        return orderId.toString();
    }
}
