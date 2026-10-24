package com.example.platform.payment.messaging;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Deduplicates redelivered messages. Must run in the same transaction as the handler's effects.
 */
@Component
public class Inbox {

    private final JdbcTemplate jdbc;
    private final int retentionDays;

    public Inbox(JdbcTemplate jdbc, @Value("${outbox.retention-days}") int retentionDays) {
        this.jdbc = jdbc;
        this.retentionDays = retentionDays;
    }

    /** Redeliveries happen within minutes; a week of ids is ample. Deleted in chunks to keep locks short. */
    @Scheduled(cron = "0 45 3 * * *")
    @SchedulerLock(name = "inbox-cleanup")
    public void purgeOld() {
        int deleted;
        do {
            deleted = jdbc.update("delete from processed_messages where message_id in (select message_id from processed_messages "
                    + "where processed_at < now() - make_interval(days => ?) limit 5000)", retentionDays);
        } while (deleted > 0);
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
