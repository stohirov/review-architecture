package com.example.platform.order.messaging;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Deduplicates redelivered messages. Must run in the same transaction as the handler's effects.
 */
@Component
public class Inbox {

    private final JdbcTemplate jdbc;

    public Inbox(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean firstDelivery(String messageId) {
        return jdbc.update("insert into processed_messages (message_id, processed_at) values (?, now()) on conflict do nothing",
                messageId) == 1;
    }

    public boolean alreadyProcessed(String messageId) {
        Integer count = jdbc.queryForObject("select count(*) from processed_messages where message_id = ?", Integer.class, messageId);
        return count != null && count > 0;
    }
}
