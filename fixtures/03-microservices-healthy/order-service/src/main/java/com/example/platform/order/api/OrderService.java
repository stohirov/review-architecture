package com.example.platform.order.api;

import com.example.platform.order.domain.Order;
import com.example.platform.order.domain.OrderRepository;
import com.example.platform.order.domain.PriceList;
import com.example.platform.order.saga.OrderSaga;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Service
public class OrderService {

    static final int MAX_LINES = 50;

    private final OrderRepository orders;
    private final PriceList prices;
    private final OrderSaga saga;

    public OrderService(OrderRepository orders, PriceList prices, OrderSaga saga) {
        this.orders = orders;
        this.prices = prices;
        this.saga = saga;
    }

    /** Prices are never taken from the client; they come from the price list. */
    public record Line(String sku, int quantity) {
    }

    /**
     * Idempotent on {@code requestKey}: a retried request returns the order created by the first attempt.
     */
    @Transactional
    public Order placeOrder(String requestKey, String customerId, List<Line> lines) {
        validate(lines);
        return orders.findByRequestKey(requestKey).orElseGet(() -> {
            Order order = new Order(requestKey, customerId);
            lines.forEach(l -> order.addItem(l.sku(), l.quantity(), priceOf(l.sku())));
            orders.save(order);
            saga.start(order);
            return order;
        });
    }

    private BigDecimal priceOf(String sku) {
        return prices.currentPrice(sku).orElseThrow(() -> new InvalidOrderException("unknown SKU " + sku));
    }

    private static void validate(List<Line> lines) {
        if (lines == null || lines.isEmpty() || lines.size() > MAX_LINES) {
            throw new InvalidOrderException("an order needs between 1 and " + MAX_LINES + " lines");
        }
        if (lines.stream().map(Line::sku).distinct().count() != lines.size()) {
            throw new InvalidOrderException("each SKU may appear only once");
        }
        for (Line line : lines) {
            if (line.quantity() <= 0 || line.quantity() > 100) {
                throw new InvalidOrderException("quantity must be between 1 and 100 for " + line.sku());
            }
        }
    }
}
