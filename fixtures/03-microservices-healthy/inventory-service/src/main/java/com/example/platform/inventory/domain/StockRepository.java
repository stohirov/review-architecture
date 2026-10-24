package com.example.platform.inventory.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface StockRepository extends JpaRepository<StockItem, String> {

    /** Atomic conditional decrement; returns 0 when there is not enough stock. */
    @Modifying
    @Query("update StockItem s set s.available = s.available - :qty, s.version = s.version + 1 "
            + "where s.sku = :sku and s.available >= :qty")
    int tryReserve(String sku, int qty);

    @Modifying
    @Query("update StockItem s set s.available = s.available + :qty, s.version = s.version + 1 where s.sku = :sku")
    int release(String sku, int qty);
}
