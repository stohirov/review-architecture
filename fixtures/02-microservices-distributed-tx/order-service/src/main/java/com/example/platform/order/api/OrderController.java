package com.example.platform.order.api;

import com.example.platform.order.domain.Order;
import com.example.platform.order.domain.OrderRepository;
import com.example.platform.order.service.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orders;

    public OrderController(OrderService orderService, OrderRepository orders) {
        this.orderService = orderService;
        this.orders = orders;
    }

    public record PlaceOrderRequest(String customerId, List<OrderService.Line> lines) {
    }

    public record OrderView(Long id, String status, BigDecimal total, String paymentId) {
        static OrderView of(Order o) {
            return new OrderView(o.getId(), o.getStatus().name(), o.getTotal(), o.getPaymentId());
        }
    }

    @PostMapping
    public ResponseEntity<OrderView> place(@RequestBody PlaceOrderRequest request) {
        Order order = orderService.placeOrder(request.customerId(), request.lines());
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderView.of(order));
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderView> get(@PathVariable Long id) {
        return orders.findById(id).map(OrderView::of).map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
