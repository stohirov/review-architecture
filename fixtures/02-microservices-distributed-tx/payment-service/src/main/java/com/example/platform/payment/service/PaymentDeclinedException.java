package com.example.platform.payment.service;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.PAYMENT_REQUIRED)
public class PaymentDeclinedException extends RuntimeException {

    public PaymentDeclinedException(Long orderId) {
        super("Payment declined for order " + orderId);
    }
}
