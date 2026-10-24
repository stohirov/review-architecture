package com.example.platform.payment.messaging;

import com.example.platform.payment.psp.PspClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

/**
 * Not transactional on purpose: the PSP call happens outside any DB transaction. A PSP 4xx is a
 * final decline and becomes PaymentFailed. Transient PSP failures (timeouts, 5xx, open circuit) propagate so the container's error handler retries with
 * backoff and finally dead-letters the command.
 */
@Component
public class PaymentCommandListener {

    private final PspClient psp;
    private final PaymentRecorder recorder;
    private final Inbox inbox;
    private final ObjectMapper json;

    public PaymentCommandListener(PspClient psp, PaymentRecorder recorder, Inbox inbox, ObjectMapper json) {
        this.psp = psp;
        this.recorder = recorder;
        this.inbox = inbox;
        this.json = json;
    }

    @KafkaListener(topics = "payment-commands", concurrency = "4")
    public void onCommand(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String messageId = Headers.messageId(record);
        if (inbox.alreadyProcessed(messageId) || !"ChargePayment".equals(Headers.messageType(record))) {
            return;
        }
        Messages.ChargePayment command = json.readValue(record.value(), Messages.ChargePayment.class);
        PspClient.CaptureResponse response;
        try {
            response = psp.capture("capture-" + command.orderId(), command.customerId(), command.amount());
        } catch (HttpClientErrorException rejected) {
            // A 4xx is the PSP's final answer (card declined, invalid request): fail the payment, don't retry.
            response = new PspClient.CaptureResponse(null, "rejected_" + rejected.getStatusCode().value());
        }
        recorder.record(messageId, command, response);
    }
}
