package com.example.platform.inventory.outbox;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxRelay {

    private final OutboxRepository events;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;
    private final Duration retention;

    public OutboxRelay(OutboxRepository events, KafkaTemplate<String, String> kafka,
                       @Value("${outbox.batch-size}") int batchSize,
                       @Value("${outbox.retention-days}") int retentionDays) {
        this.events = events;
        this.kafka = kafka;
        this.batchSize = batchSize;
        this.retention = Duration.ofDays(retentionDays);
    }

    @Scheduled(fixedDelayString = "${outbox.relay-interval-ms}")
    @SchedulerLock(name = "outbox-relay", lockAtMostFor = "PT30S")
    public void relay() throws Exception {
        for (OutboxEvent event : events.findUnsent(PageRequest.of(0, batchSize))) {
            ProducerRecord<String, String> record = new ProducerRecord<>(event.getTopic(), event.getAggregateKey(), event.getPayload());
            record.headers().add("message-id", event.getId().toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("message-type", event.getType().getBytes(StandardCharsets.UTF_8));
            kafka.send(record).get(10, TimeUnit.SECONDS);
            event.markSent();
            events.save(event);
        }
    }

    @Scheduled(cron = "0 15 3 * * *")
    @SchedulerLock(name = "outbox-cleanup")
    public void purgeSent() {
        events.deleteSentBefore(Instant.now().minus(retention));
    }
}
