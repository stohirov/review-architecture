package com.example.platform.order.domain;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    @EntityGraph(attributePaths = "items")
    Optional<Order> findWithItemsById(UUID id);

    Optional<Order> findByRequestKey(String requestKey);

    Page<Order> findByCustomerIdOrderByCreatedAtDesc(String customerId, Pageable page);

    List<Order> findByStatusAndCreatedAtBefore(Order.Status status, Instant cutoff, Pageable page);

    long countByStatusAndCreatedAtBefore(Order.Status status, Instant cutoff);
}
