package com.example.platform.order.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Records an event in the same transaction as the state change that caused it. {@link OutboxRelay} publishes it.
 */
@Component
public class Outbox {

    private final OutboxRepository events;
    private final ObjectMapper json;

    public Outbox(OutboxRepository events, ObjectMapper json) {
        this.events = events;
        this.json = json;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(String topic, String aggregateKey, Object message) {
        try {
            events.save(new OutboxEvent(topic, aggregateKey, message.getClass().getSimpleName(), json.writeValueAsString(message)));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize " + message.getClass(), e);
        }
    }
}
