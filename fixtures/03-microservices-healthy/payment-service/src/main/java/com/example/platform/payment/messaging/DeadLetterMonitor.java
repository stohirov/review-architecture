package com.example.platform.payment.messaging;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Counts and logs every dead-lettered record so the {@code kafka_dlt_records_total} alert fires.
 * Records stay on the DLT; ops replays them with the dlt-redrive runbook once the cause is fixed.
 */
@Component
public class DeadLetterMonitor {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterMonitor.class);

    private final MeterRegistry meters;

    public DeadLetterMonitor(MeterRegistry meters) {
        this.meters = meters;
    }

    @KafkaListener(topicPattern = ".*-dlt", groupId = "payment-service-dlt-monitor")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        Counter.builder("kafka.dlt.records").tag("topic", record.topic()).register(meters).increment();
        Header cause = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        log.error("Dead-lettered record on {} key={} cause={}", record.topic(), record.key(),
                cause == null ? "unknown" : new String(cause.value(), StandardCharsets.UTF_8));
    }
}
