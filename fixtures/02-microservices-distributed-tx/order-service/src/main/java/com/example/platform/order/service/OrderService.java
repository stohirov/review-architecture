package com.example.platform.order.service;

import com.example.platform.order.client.PaymentClient;
import com.example.platform.order.client.PaymentClient.ChargeRequest;
import com.example.platform.order.client.PaymentClient.ChargeResponse;
import com.example.platform.order.domain.Order;
import com.example.platform.order.domain.OrderRepository;
import com.example.platform.order.events.OrderPaidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    static final String ORDER_EVENTS = "order-events";

    private final OrderRepository orders;
    private final PaymentClient paymentClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderService(OrderRepository orders, PaymentClient paymentClient, KafkaTemplate<String, Object> kafkaTemplate) {
        this.orders = orders;
        this.paymentClient = paymentClient;
        this.kafkaTemplate = kafkaTemplate;
    }

    public record Line(String sku, int quantity, BigDecimal unitPrice) {
    }

    @Transactional
    public Order placeOrder(String customerId, List<Line> lines) {
        Order order = new Order(customerId);
        lines.forEach(l -> order.addItem(l.sku(), l.quantity(), l.unitPrice()));
        orders.save(order);

        ChargeResponse payment = takePayment(order);
        order.markPaid(payment.paymentId());

        kafkaTemplate.send(ORDER_EVENTS, new OrderPaidEvent(order.getId(), customerId, order.getTotal(), Instant.now()));
        log.info("Order {} placed and paid ({})", order.getId(), payment.paymentId());
        return order;
    }

    private ChargeResponse takePayment(Order order) {
        return paymentClient.charge(new ChargeRequest(order.getId(), order.getCustomerId(), order.getTotal()));
    }
}
