package com.example.platform.payment.messaging;

import com.example.platform.payment.psp.PspClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import java.util.Set;

/**
 * Not transactional on purpose: the PSP call happens outside any DB transaction. A final PSP decline
 * (400, 402, 422) becomes PaymentFailed; every other failure is retried. Transient PSP failures (timeouts, 5xx, open circuit) propagate so the container's error handler retries with
 * backoff and finally dead-letters the command.
 */
@Component
public class PaymentCommandListener {

    /** PSP statuses that are the final answer for this charge: invalid request, card declined, unprocessable. */
    private static final Set<Integer> FINAL_DECLINES = Set.of(400, 402, 422);

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
            if (!FINAL_DECLINES.contains(rejected.getStatusCode().value())) {
                // 401/403 (our credentials), 408/409/425/429 (retry later): not the customer's answer.
                // Propagate so the error handler retries, then dead-letters and pages.
                throw rejected;
            }
            response = new PspClient.CaptureResponse(null, "declined_" + rejected.getStatusCode().value());
        }
        recorder.record(messageId, command, response);
    }
}
