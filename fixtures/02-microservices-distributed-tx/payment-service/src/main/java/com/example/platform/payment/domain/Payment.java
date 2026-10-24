package com.example.platform.payment.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
public class Payment {

    public enum Status { INITIATED, CAPTURED, FAILED }

    @Id
    private String id = UUID.randomUUID().toString();

    private Long orderId;
    private String customerId;
    private BigDecimal amount;
    private String pspReference;
    private Instant createdAt = Instant.now();

    @Enumerated(EnumType.STRING)
    private Status status = Status.INITIATED;

    protected Payment() {
    }

    public Payment(Long orderId, String customerId, BigDecimal amount) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
    }

    public void captured(String pspReference) {
        this.pspReference = pspReference;
        this.status = Status.CAPTURED;
    }

    public String getId() { return id; }
    public Long getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public BigDecimal getAmount() { return amount; }
    public Status getStatus() { return status; }
}
