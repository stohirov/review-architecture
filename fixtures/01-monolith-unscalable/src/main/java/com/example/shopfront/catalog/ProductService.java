package com.example.shopfront.catalog;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ProductService {

    private final ProductRepository products;
    private final Map<Long, BigDecimal> priceCache = new ConcurrentHashMap<>();

    public ProductService(ProductRepository products) {
        this.products = products;
    }

    public List<Product> listAll() {
        return products.findAll();
    }

    public List<Product> search(String query) {
        return products.findByNameContainingIgnoreCase(query);
    }

    public Product get(Long id) {
        return products.findById(id).orElseThrow(() -> new ProductNotFoundException(id));
    }

    public BigDecimal currentPrice(Long productId) {
        return priceCache.computeIfAbsent(productId, id -> get(id).getPrice());
    }

    @Transactional
    public Product updatePrice(Long productId, BigDecimal newPrice) {
        Product product = get(productId);
        product.setPrice(newPrice);
        priceCache.put(productId, newPrice);
        return product;
    }
}
