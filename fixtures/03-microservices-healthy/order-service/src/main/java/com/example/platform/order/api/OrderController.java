package com.example.platform.order.api;

import com.example.platform.order.domain.Order;
import com.example.platform.order.domain.OrderRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final int MAX_PAGE_SIZE = 50;

    private final OrderService orderService;
    private final OrderRepository orders;

    public OrderController(OrderService orderService, OrderRepository orders) {
        this.orderService = orderService;
        this.orders = orders;
    }

    public record PlaceOrderRequest(List<OrderService.Line> lines) {
    }

    public record OrderView(UUID id, String status, BigDecimal total, String cancelReason) {
        static OrderView of(Order o) {
            return new OrderView(o.getId(), o.getStatus().name(), o.getTotal(), o.getCancelReason());
        }
    }

    /** Returns 202: the saga completes asynchronously; clients poll GET /orders/{id}. */
    @PostMapping
    public ResponseEntity<OrderView> place(@AuthenticationPrincipal Jwt customer,
                                           @RequestHeader("Idempotency-Key") String idempotencyKey,
                                           @RequestBody PlaceOrderRequest request) {
        Order order = orderService.placeOrder(idempotencyKey, customer.getSubject(), request.lines());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(OrderView.of(order));
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderView> get(@AuthenticationPrincipal Jwt customer, @PathVariable UUID id) {
        return orders.findById(id)
                .filter(o -> o.getCustomerId().equals(customer.getSubject()))
                .map(OrderView::of).map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public Page<OrderView> mine(@AuthenticationPrincipal Jwt customer,
                                      @RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "20") int size) {
        return orders.findByCustomerIdOrderByCreatedAtDesc(customer.getSubject(), PageRequest.of(page, Math.min(size, MAX_PAGE_SIZE)))
                .map(OrderView::of);
    }
}
