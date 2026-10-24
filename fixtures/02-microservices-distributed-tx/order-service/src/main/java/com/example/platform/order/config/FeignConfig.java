package com.example.platform.order.config;

import feign.Retryer;
import org.springframework.context.annotation.Bean;

public class FeignConfig {

    /**
     * Payment-service is occasionally slow under load; retry so checkout doesn't fail on a blip.
     */
    @Bean
    public Retryer feignRetryer() {
        return new Retryer.Default(100, 1000, 5);
    }
}
