package com.example.platform.inventory.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox")
public class OutboxEvent {

    @Id
    private UUID id;

    private String topic;
    private String aggregateKey;
    private String type;
    private String payload;
    private Instant createdAt;
    private Instant sentAt;

    protected OutboxEvent() {
    }

    OutboxEvent(String topic, String aggregateKey, String type, String payload) {
        this.id = UUID.randomUUID();
        this.topic = topic;
        this.aggregateKey = aggregateKey;
        this.type = type;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    void markSent() {
        this.sentAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getTopic() { return topic; }
    public String getAggregateKey() { return aggregateKey; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
}
