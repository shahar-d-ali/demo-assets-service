package com.demo.assetservice.events.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String PING_TOPIC = "demo-ping-topic";

    @Bean
    public NewTopic pingTopic() {
        return TopicBuilder.name(PING_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
    // Producer factory is auto-configured by Spring Boot from
    // spring.kafka.producer.* properties (KafkaAvroSerializer + schema registry).
}
