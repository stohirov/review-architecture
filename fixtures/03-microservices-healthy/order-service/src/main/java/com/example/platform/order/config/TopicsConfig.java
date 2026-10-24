package com.example.platform.order.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Topics this service produces. 12 partitions leave room for 3 replicas x 4 consumer threads.
 */
@Configuration
public class TopicsConfig {

    @Bean
    public KafkaAdmin.NewTopics producedTopics() {
        return new KafkaAdmin.NewTopics(
            TopicBuilder.name("inventory-commands").partitions(12).replicas(3).build(),
            TopicBuilder.name("inventory-commands-dlt").partitions(12).replicas(3).build(),
            TopicBuilder.name("payment-commands").partitions(12).replicas(3).build(),
            TopicBuilder.name("payment-commands-dlt").partitions(12).replicas(3).build(),
            TopicBuilder.name("order-events").partitions(12).replicas(3).build(),
            TopicBuilder.name("order-events-dlt").partitions(12).replicas(3).build()
        );
    }
}
