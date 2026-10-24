package com.example.platform.order.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "orders")
public class Order {

    public enum Status { PENDING, PAID, CANCELLED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String customerId;
    private BigDecimal total = BigDecimal.ZERO;
    private String paymentId;
    private Instant createdAt = Instant.now();

    @Enumerated(EnumType.STRING)
    private Status status = Status.PENDING;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    private List<OrderItem> items = new ArrayList<>();

    protected Order() {
    }

    public Order(String customerId) {
        this.customerId = customerId;
    }

    public void addItem(String sku, int quantity, BigDecimal unitPrice) {
        items.add(new OrderItem(this, sku, quantity, unitPrice));
        total = total.add(unitPrice.multiply(BigDecimal.valueOf(quantity)));
    }

    public void markPaid(String paymentId) {
        this.paymentId = paymentId;
        this.status = Status.PAID;
    }

    public Long getId() { return id; }
    public String getCustomerId() { return customerId; }
    public BigDecimal getTotal() { return total; }
    public String getPaymentId() { return paymentId; }
    public Status getStatus() { return status; }
    public List<OrderItem> getItems() { return items; }
}
