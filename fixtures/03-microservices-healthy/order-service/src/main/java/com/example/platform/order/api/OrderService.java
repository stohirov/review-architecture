package com.example.platform.order.api;

import com.example.platform.order.domain.Order;
import com.example.platform.order.domain.OrderRepository;
import com.example.platform.order.saga.OrderSaga;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final OrderSaga saga;

    public OrderService(OrderRepository orders, OrderSaga saga) {
        this.orders = orders;
        this.saga = saga;
    }

    public record Line(String sku, int quantity, BigDecimal unitPrice) {
    }

    /**
     * Idempotent on {@code requestKey}: a retried request returns the order created by the first attempt.
     */
    @Transactional
    public Order placeOrder(String requestKey, String customerId, List<Line> lines) {
        return orders.findByRequestKey(requestKey).orElseGet(() -> {
            Order order = new Order(requestKey, customerId);
            lines.forEach(l -> order.addItem(l.sku(), l.quantity(), l.unitPrice()));
            orders.save(order);
            saga.start(order);
            return order;
        });
    }
}
