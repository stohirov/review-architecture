package com.example.platform.payment.service;

import com.example.platform.payment.domain.Payment;
import com.example.platform.payment.domain.PaymentRepository;
import com.example.platform.payment.psp.PspClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
public class PaymentService {

    private final PaymentRepository payments;
    private final PspClient psp;

    public PaymentService(PaymentRepository payments, PspClient psp) {
        this.payments = payments;
        this.psp = psp;
    }

    @Transactional
    public Payment charge(Long orderId, String customerId, BigDecimal amount) {
        Payment payment = payments.save(new Payment(orderId, customerId, amount));
        PspClient.CaptureResponse response = psp.capture(customerId, amount);
        if (!"approved".equals(response.outcome())) {
            throw new PaymentDeclinedException(orderId);
        }
        payment.captured(response.reference());
        return payment;
    }
}
