package com.example.shopfront.order;

import com.example.shopfront.cart.Cart;
import jakarta.servlet.http.HttpSession;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/checkout")
    @ResponseStatus(HttpStatus.CREATED)
    public OrderSummary checkout(@RequestParam String email, HttpSession session) {
        Cart cart = (Cart) session.getAttribute("cart");
        if (cart == null || cart.isEmpty()) {
            throw new IllegalStateException("Cart is empty");
        }
        CustomerOrder order = orderService.checkout(email, cart);
        cart.clear();
        return OrderSummary.of(order);
    }

    @GetMapping
    public List<OrderSummary> history(@RequestParam String email) {
        return orderService.history(email).stream().map(OrderSummary::of).toList();
    }

    public record LineSummary(String sku, String name, int quantity, BigDecimal unitPrice) {
    }

    public record OrderSummary(Long id, Instant placedAt, BigDecimal total, String status, List<LineSummary> lines) {

        static OrderSummary of(CustomerOrder order) {
            List<LineSummary> lines = order.getLines().stream()
                    .map(l -> new LineSummary(l.getProduct().getSku(), l.getProduct().getName(),
                            l.getQuantity(), l.getUnitPrice()))
                    .toList();
            return new OrderSummary(order.getId(), order.getPlacedAt(), order.getTotal(),
                    order.getStatus().name(), lines);
        }
    }
}
