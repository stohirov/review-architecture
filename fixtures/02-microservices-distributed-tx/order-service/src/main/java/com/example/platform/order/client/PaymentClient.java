package com.example.platform.order.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.math.BigDecimal;

@FeignClient(name = "payment-service", url = "${clients.payment-service.url}")
public interface PaymentClient {

    @PostMapping("/payments")
    ChargeResponse charge(@RequestBody ChargeRequest request);

    record ChargeRequest(Long orderId, String customerId, BigDecimal amount) {
    }

    record ChargeResponse(String paymentId, String status) {
    }
}
