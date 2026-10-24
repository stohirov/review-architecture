package com.example.shopfront;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ShopfrontApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShopfrontApplication.class, args);
    }
}
