package com.example.platform.order.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * Current selling prices. The pricing back office writes {@code price_list}; order-service only reads it.
 */
@Component
public class PriceList {

    private final JdbcTemplate jdbc;

    public PriceList(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<BigDecimal> currentPrice(String sku) {
        List<BigDecimal> rows = jdbc.queryForList(
                "select unit_price from price_list where sku = ? and active", BigDecimal.class, sku);
        return rows.stream().findFirst();
    }
}
