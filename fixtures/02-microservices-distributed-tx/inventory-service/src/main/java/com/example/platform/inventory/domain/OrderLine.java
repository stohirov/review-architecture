package com.example.platform.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;

/**
 * Read-only view of order lines so we know what to decrement when an order is paid.
 */
@Entity
@Immutable
@Table(name = "order_items")
public class OrderLine {

    @Id
    private Long id;

    @Column(name = "order_id")
    private Long orderId;

    private String sku;
    private int quantity;

    protected OrderLine() {
    }

    public Long getOrderId() { return orderId; }
    public String getSku() { return sku; }
    public int getQuantity() { return quantity; }
}
