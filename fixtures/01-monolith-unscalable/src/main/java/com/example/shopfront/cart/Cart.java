package com.example.shopfront.cart;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class Cart implements Serializable {

    private final Map<Long, Integer> quantities = new LinkedHashMap<>();

    public void add(Long productId, int quantity) {
        quantities.merge(productId, quantity, Integer::sum);
    }

    public void remove(Long productId) {
        quantities.remove(productId);
    }

    public Map<Long, Integer> getQuantities() {
        return quantities;
    }

    public boolean isEmpty() {
        return quantities.isEmpty();
    }

    public void clear() {
        quantities.clear();
    }
}
