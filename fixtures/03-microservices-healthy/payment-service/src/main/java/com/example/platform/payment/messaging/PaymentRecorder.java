package com.example.platform.payment.messaging;

import com.example.platform.payment.domain.Payment;
import com.example.platform.payment.domain.PaymentRepository;
import com.example.platform.payment.outbox.Outbox;
import com.example.platform.payment.psp.PspClient.CaptureResponse;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import static com.example.platform.payment.messaging.Messages.PAYMENT_EVENTS;

/**
 * Records the PSP outcome and the reply in one local transaction, after the PSP call has returned.
 */
@Component
public class PaymentRecorder {

    private final PaymentRepository payments;
    private final Inbox inbox;
    private final Outbox outbox;

    public PaymentRecorder(PaymentRepository payments, Inbox inbox, Outbox outbox) {
        this.payments = payments;
        this.inbox = inbox;
        this.outbox = outbox;
    }

    @Transactional
    public void record(String messageId, Messages.ChargePayment command, CaptureResponse response) {
        if (!inbox.firstDelivery(messageId) || payments.findByOrderId(command.orderId()).isPresent()) {
            return;
        }
        String key = command.orderId().toString();
        if (response.approved()) {
            Payment payment = payments.save(new Payment(command.orderId(), command.customerId(), command.amount(),
                    Payment.Status.CAPTURED, response.reference()));
            outbox.publish(PAYMENT_EVENTS, key, new Messages.PaymentCaptured(command.orderId(), payment.getId()));
        } else {
            payments.save(new Payment(command.orderId(), command.customerId(), command.amount(),
                    Payment.Status.DECLINED, response.reference()));
            outbox.publish(PAYMENT_EVENTS, key, new Messages.PaymentFailed(command.orderId(), response.outcome()));
        }
    }
}
