package com.example.shopfront.order;

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
@Table(name = "customer_order")
public class CustomerOrder {

    public enum Status { PLACED, SHIPPED, CANCELLED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String customerEmail;
    private Instant placedAt;
    private BigDecimal total;
    private boolean invoiced;

    @Enumerated(EnumType.STRING)
    private Status status;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    private List<OrderLine> lines = new ArrayList<>();

    protected CustomerOrder() {
    }

    public CustomerOrder(String customerEmail) {
        this.customerEmail = customerEmail;
        this.placedAt = Instant.now();
        this.status = Status.PLACED;
        this.total = BigDecimal.ZERO;
    }

    public void addLine(OrderLine line) {
        lines.add(line);
        total = total.add(line.getUnitPrice().multiply(BigDecimal.valueOf(line.getQuantity())));
    }

    public Long getId() { return id; }
    public String getCustomerEmail() { return customerEmail; }
    public Instant getPlacedAt() { return placedAt; }
    public BigDecimal getTotal() { return total; }
    public Status getStatus() { return status; }
    public List<OrderLine> getLines() { return lines; }
    public boolean isInvoiced() { return invoiced; }
    public void markInvoiced() { this.invoiced = true; }
}
