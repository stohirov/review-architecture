package com.example.platform.order.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    public enum Status { PENDING, STOCK_RESERVED, CONFIRMED, CANCELLED }

    @Id
    private UUID id;

    private String requestKey;
    private String customerId;
    private BigDecimal total = BigDecimal.ZERO;
    private String cancelReason;
    private String paymentId;
    private Instant createdAt;

    @Enumerated(EnumType.STRING)
    private Status status;

    @Version
    private long version;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    private List<OrderItem> items = new ArrayList<>();

    protected Order() {
    }

    public Order(String requestKey, String customerId) {
        this.id = UUID.randomUUID();
        this.requestKey = requestKey;
        this.customerId = customerId;
        this.status = Status.PENDING;
        this.createdAt = Instant.now();
    }

    public void addItem(String sku, int quantity, BigDecimal unitPrice) {
        items.add(new OrderItem(this, sku, quantity, unitPrice));
        total = total.add(unitPrice.multiply(BigDecimal.valueOf(quantity)));
    }

    /** @return false when the transition was already applied (a redelivered reply). */
    public boolean stockReserved() {
        return transition(Status.PENDING, Status.STOCK_RESERVED);
    }

    public boolean confirm(String paymentId) {
        if (!transition(Status.STOCK_RESERVED, Status.CONFIRMED)) {
            return false;
        }
        this.paymentId = paymentId;
        return true;
    }

    public boolean cancel(String reason) {
        if (status == Status.CANCELLED || status == Status.CONFIRMED) {
            return false;
        }
        this.status = Status.CANCELLED;
        this.cancelReason = reason;
        return true;
    }

    private boolean transition(Status from, Status to) {
        if (status != from) {
            return false;
        }
        status = to;
        return true;
    }

    public UUID getId() { return id; }
    public String getRequestKey() { return requestKey; }
    public String getCustomerId() { return customerId; }
    public BigDecimal getTotal() { return total; }
    public Status getStatus() { return status; }
    public String getCancelReason() { return cancelReason; }
    public String getPaymentId() { return paymentId; }
    public Instant getCreatedAt() { return createdAt; }
    public List<OrderItem> getItems() { return items; }
}
