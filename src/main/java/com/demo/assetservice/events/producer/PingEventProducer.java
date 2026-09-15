package com.demo.assetservice.events.producer;

import com.demo.assetservice.events.config.KafkaTopicConfig;
import com.demo.assetservice.events.dto.PingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class PingEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PingEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PingEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public PingEvent sendPing(String message) {
        PingEvent event = PingEvent.builder()
                .id(UUID.randomUUID().toString())
                .message(message != null ? message : "ping")
                .sourceService("demo-assets-service")
                .timestamp(Instant.now())
                .build();

        log.info("Publishing ping event to topic {}: {}", KafkaTopicConfig.PING_TOPIC, event);
        kafkaTemplate.send(KafkaTopicConfig.PING_TOPIC, event.getId(), event);
        return event;
    }
}
