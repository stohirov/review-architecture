package com.example.shopfront.cart;

import jakarta.servlet.http.HttpSession;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cart")
public class CartController {

    static final String CART = "cart";

    @GetMapping
    public Cart view(HttpSession session) {
        return cartOf(session);
    }

    @PostMapping("/items")
    public Cart add(@RequestParam Long productId, @RequestParam(defaultValue = "1") int quantity, HttpSession session) {
        Cart cart = cartOf(session);
        cart.add(productId, quantity);
        return cart;
    }

    @DeleteMapping("/items/{productId}")
    public Cart remove(@PathVariable Long productId, HttpSession session) {
        Cart cart = cartOf(session);
        cart.remove(productId);
        return cart;
    }

    static Cart cartOf(HttpSession session) {
        Cart cart = (Cart) session.getAttribute(CART);
        if (cart == null) {
            cart = new Cart();
            session.setAttribute(CART, cart);
        }
        return cart;
    }
}
