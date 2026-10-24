package com.example.shopfront.order;

import com.example.shopfront.cart.Cart;
import com.example.shopfront.catalog.Product;
import com.example.shopfront.catalog.ProductRepository;
import com.example.shopfront.catalog.ProductService;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final ProductRepository products;
    private final ProductService productService;
    private final JavaMailSender mailSender;

    public OrderService(OrderRepository orders, ProductRepository products,
                        ProductService productService, JavaMailSender mailSender) {
        this.orders = orders;
        this.products = products;
        this.productService = productService;
        this.mailSender = mailSender;
    }

    @Transactional
    public CustomerOrder checkout(String customerEmail, Cart cart) {
        CustomerOrder order = new CustomerOrder(customerEmail);
        for (Map.Entry<Long, Integer> entry : cart.getQuantities().entrySet()) {
            Product product = productService.get(entry.getKey());
            int quantity = entry.getValue();
            if (product.getStock() < quantity) {
                throw new OutOfStockException(product.getSku());
            }
            product.setStock(product.getStock() - quantity);
            products.save(product);
            order.addLine(new OrderLine(order, product, quantity, productService.currentPrice(product.getId())));
        }
        CustomerOrder saved = orders.save(order);
        sendConfirmation(saved);
        return saved;
    }

    public List<CustomerOrder> history(String customerEmail) {
        return orders.findByCustomerEmailOrderByPlacedAtDesc(customerEmail);
    }

    private void sendConfirmation(CustomerOrder order) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(order.getCustomerEmail());
        message.setSubject("Your Shopfront order #" + order.getId());
        message.setText("Thanks! We received your order totalling " + order.getTotal() + ".");
        mailSender.send(message);
    }
}
