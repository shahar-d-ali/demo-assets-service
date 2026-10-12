package com.demo.assetservice.events.producer;

import com.demo.shared.events.dto.PingEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class PingEventProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void testValidateEventPassesWithLogicalTimestamp() {
        PingEventProducer producer = new PingEventProducer(kafkaTemplate);

        PingEvent event = PingEvent.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setMessage("Health check ping")
                .setSourceService("payment-gateway")
                .setTimestamp(Instant.now())
                .build();

        assertDoesNotThrow(() -> producer.validateEvent(event));
    }

    @Test
    void testValidateEventRejectsNullEvent() {
        PingEventProducer producer = new PingEventProducer(kafkaTemplate);
        assertThrows(IllegalArgumentException.class, () -> producer.validateEvent(null));
    }

    @Test
    void testValidateEventRejectsBlankId() {
        PingEventProducer producer = new PingEventProducer(kafkaTemplate);
        PingEvent event = PingEvent.newBuilder()
                .setId("   ")
                .setMessage("test")
                .setSourceService("service-1")
                .setTimestamp(Instant.now())
                .build();

        assertThrows(IllegalArgumentException.class, () -> producer.validateEvent(event));
    }
}
