package com.example.platform.payment.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConfig {

    /**
     * Retries a failing record with backoff, then parks it on {@code <topic>-dlt} so the partition keeps moving.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        // ~3 minutes in total: longer than a circuit-breaker open window or a DB failover.
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(8);
        backOff.setInitialInterval(1_000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(60_000);
        DefaultErrorHandler handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backOff);
        handler.addNotRetryableExceptions(com.fasterxml.jackson.core.JsonProcessingException.class);
        return handler;
    }
}
