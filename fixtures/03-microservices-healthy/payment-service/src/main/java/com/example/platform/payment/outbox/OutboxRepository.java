package com.example.platform.payment.outbox;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query("select e from OutboxEvent e where e.sentAt is null order by e.createdAt")
    List<OutboxEvent> findUnsent(Pageable page);

    @Modifying
    @Transactional
    @Query("delete from OutboxEvent e where e.sentAt < :cutoff")
    int deleteSentBefore(Instant cutoff);
}
