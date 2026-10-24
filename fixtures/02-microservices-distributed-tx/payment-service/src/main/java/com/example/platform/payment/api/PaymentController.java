package com.example.platform.payment.api;

import com.example.platform.payment.domain.Payment;
import com.example.platform.payment.service.PaymentService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    public record ChargeRequest(Long orderId, String customerId, BigDecimal amount) {
    }

    public record ChargeResponse(String paymentId, String status) {
    }

    @PostMapping
    public ChargeResponse charge(@RequestBody ChargeRequest request) {
        Payment payment = paymentService.charge(request.orderId(), request.customerId(), request.amount());
        return new ChargeResponse(payment.getId(), payment.getStatus().name());
    }
}
