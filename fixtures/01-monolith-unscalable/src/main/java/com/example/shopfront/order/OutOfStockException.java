package com.example.shopfront.order;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.CONFLICT)
public class OutOfStockException extends RuntimeException {

    public OutOfStockException(String sku) {
        super("Not enough stock for " + sku);
    }
}
