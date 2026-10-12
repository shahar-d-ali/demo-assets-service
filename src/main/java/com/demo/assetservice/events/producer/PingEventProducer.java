package com.demo.assetservice.events.producer;

import java.time.Instant;
import java.util.UUID;

import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.demo.assetservice.events.config.KafkaTopicConfig;
import com.demo.shared.events.dto.PingEvent;

@Service
public class PingEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PingEventProducer.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    }

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PingEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public PingEvent sendPing(String message) {
        return sendPing(UUID.randomUUID().toString(), message, "demo-assets-service", Instant.now());
    }

    public PingEvent sendPing(String id, String message, String sourceService, Instant timestamp) {
        String eventId = (id != null && !id.isBlank()) ? id : UUID.randomUUID().toString();
        PingEvent event = PingEvent.newBuilder()
                .setId(eventId)
                .setMessage(message != null ? message : "Health check ping")
                .setSourceService(sourceService != null && !sourceService.isBlank() ? sourceService : "payment-gateway")
                .setTimestamp(timestamp != null ? timestamp : Instant.now())
                .build();

        return sendPing(event);
    }

    public PingEvent sendPing(PingEvent event) {
        validateEvent(event);
        String eventId = event.getId().toString();
        // log.info("Publishing validated ping event to topic {}: {}",
        // KafkaTopicConfig.PING_TOPIC, event);
        kafkaTemplate.send(KafkaTopicConfig.PING_TOPIC, eventId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send ping event to topic {}: {}", KafkaTopicConfig.PING_TOPIC, event,
                                exception);
                    } else {
                        // log.info("Successfully sent ping event to topic {}: {}",
                        // KafkaTopicConfig.PING_TOPIC, event);
                        RecordMetadata recordMetadata = result.getRecordMetadata();
                        String topic = recordMetadata.topic();
                        int partition = recordMetadata.partition();
                        long offset = recordMetadata.offset();

                        log.info("Record metadata: {} {} {}", topic, partition, offset);
                    }
                });
        return event;
    }

    /**
     * Validates that the event strictly complies with the PingEvent.avsc schema:
     * 1. Non-null instance check
     * 2. Schema compatibility with PingEvent.getClassSchema()
     * 3. Avro SpecificData.validate(schema, datum)
     * 4. Presence and format checks for required fields (id, message,
     * sourceService, timestamp)
     *
     * @param event the PingEvent to validate
     * @throws IllegalArgumentException if the event is invalid or does not match
     *                                  the Avro schema
     */
    public void validateEvent(PingEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Cannot send null PingEvent");
        }

        // 1. Verify schema identity and namespace
        if (!PingEvent.getClassSchema().equals(event.getSchema())) {
            throw new IllegalArgumentException(String.format(
                    "Event schema [%s] does not match target Avro schema [%s]",
                    event.getSchema().getFullName(), PingEvent.getClassSchema().getFullName()));
        }

        // 2. Validate Avro binary encoding and logical type compatibility
        try {
            PingEvent.getEncoder().encode(event);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "PingEvent fails Avro schema validation for schema %s: %s",
                    PingEvent.getClassSchema().getFullName(), e.getMessage()), e);
        }

        // 3. Validate required fields as specified in PingEvent.avsc
        if (event.getId() == null || event.getId().toString().trim().isEmpty()) {
            throw new IllegalArgumentException("PingEvent 'id' field is required by Avro schema and cannot be blank");
        }

        if (event.getMessage() == null) {
            throw new IllegalArgumentException(
                    "PingEvent 'message' field is required by Avro schema and cannot be null");
        }

        if (event.getSourceService() == null || event.getSourceService().toString().trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "PingEvent 'sourceService' field is required by Avro schema and cannot be blank");
        }

        if (event.getTimestamp() == null) {
            throw new IllegalArgumentException(
                    "PingEvent 'timestamp' field is required by Avro schema (timestamp-millis) and cannot be null");
        }

        log.debug("PingEvent Avro schema validation passed for event id: {}", event.getId());
    }
}
