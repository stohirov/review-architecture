package com.example.platform.inventory.messaging;

import com.example.platform.inventory.domain.OrderLine;
import com.example.platform.inventory.domain.OrderLineRepository;
import com.example.platform.inventory.domain.StockItem;
import com.example.platform.inventory.domain.StockRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final OrderLineRepository orderLines;
    private final StockRepository stock;

    public OrderEventsListener(OrderLineRepository orderLines, StockRepository stock) {
        this.orderLines = orderLines;
        this.stock = stock;
    }

    @KafkaListener(topics = "order-events")
    @Transactional
    public void onOrderPaid(OrderPaidEvent event) {
        for (OrderLine line : orderLines.findByOrderId(event.orderId())) {
            StockItem item = stock.findById(line.getSku())
                    .orElseThrow(() -> new IllegalStateException("Unknown SKU " + line.getSku()));
            if (item.getAvailable() < line.getQuantity()) {
                throw new InsufficientStockException(line.getSku(), event.orderId());
            }
            item.setAvailable(item.getAvailable() - line.getQuantity());
        }
        log.info("Stock decremented for order {}", event.orderId());
    }
}
