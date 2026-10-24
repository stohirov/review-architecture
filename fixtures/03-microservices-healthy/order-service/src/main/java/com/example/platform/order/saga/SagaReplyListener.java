package com.example.platform.order.saga;

import com.example.platform.order.messaging.Headers;
import com.example.platform.order.messaging.Inbox;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class SagaReplyListener {

    private final OrderSaga saga;
    private final Inbox inbox;
    private final ObjectMapper json;

    public SagaReplyListener(OrderSaga saga, Inbox inbox, ObjectMapper json) {
        this.saga = saga;
        this.inbox = inbox;
        this.json = json;
    }

    @KafkaListener(topics = {"inventory-events", "payment-events"})
    @Transactional
    public void onReply(ConsumerRecord<String, String> record) throws JsonProcessingException {
        if (!inbox.firstDelivery(Headers.messageId(record))) {
            return;
        }
        switch (Headers.messageType(record)) {
            case "StockReserved" -> saga.onStockReserved(read(record, Messages.StockReserved.class).orderId());
            case "StockRejected" -> {
                Messages.StockRejected m = read(record, Messages.StockRejected.class);
                saga.onStockRejected(m.orderId(), m.sku());
            }
            case "PaymentCaptured" -> {
                Messages.PaymentCaptured m = read(record, Messages.PaymentCaptured.class);
                saga.onPaymentCaptured(m.orderId(), m.paymentId());
            }
            case "PaymentFailed" -> {
                Messages.PaymentFailed m = read(record, Messages.PaymentFailed.class);
                saga.onPaymentFailed(m.orderId(), m.reason());
            }
            default -> { /* replies this saga does not handle */ }
        }
    }

    private <T> T read(ConsumerRecord<String, String> record, Class<T> type) throws JsonProcessingException {
        return json.readValue(record.value(), type);
    }
}
