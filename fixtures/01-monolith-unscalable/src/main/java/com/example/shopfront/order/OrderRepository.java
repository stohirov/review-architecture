package com.example.shopfront.order;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderRepository extends JpaRepository<CustomerOrder, Long> {

    List<CustomerOrder> findByCustomerEmailOrderByPlacedAtDesc(String customerEmail);

    List<CustomerOrder> findByInvoicedFalse();
}
