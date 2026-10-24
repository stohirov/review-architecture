package com.example.platform.payment.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
public class Payment {

    public enum Status { CAPTURED, DECLINED }

    @Id
    private String id;

    private UUID orderId;
    private String customerId;
    private BigDecimal amount;
    private String pspReference;
    private Instant createdAt;

    @Enumerated(EnumType.STRING)
    private Status status;

    protected Payment() {
    }

    public Payment(UUID orderId, String customerId, BigDecimal amount, Status status, String pspReference) {
        this.id = UUID.randomUUID().toString();
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.pspReference = pspReference;
        this.createdAt = Instant.now();
    }

    public String getId() { return id; }
    public UUID getOrderId() { return orderId; }
    public Status getStatus() { return status; }
}
