package com.example.platform.inventory.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "stock_items")
public class StockItem {

    @Id
    private String sku;

    private int available;

    @Version
    private long version;

    protected StockItem() {
    }

    public String getSku() { return sku; }
    public int getAvailable() { return available; }
}
