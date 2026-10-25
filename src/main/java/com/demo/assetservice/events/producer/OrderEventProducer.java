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
import com.demo.shared.events.dto.OrderEvent;

/**
 * Producer for OrderEvent messages targeting Kafka topic 'order-events'.
 *
 * Designed to teach and demonstrate:
 * - Kafka message publication with Avro serialization and Schema Registry.
 * - Retry mechanisms (transient error handling & exponential/fixed backoff).
 * - Dead Letter Topic (DLT) routing when maximum retry attempts are exhausted.
 * - DefaultErrorHandler behavior for fatal (non-retryable) vs retryable exceptions.
 * - Controlled test failure scenario triggering.
 */
@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    // Common Failure Scenario Constants
    public static final String SCENARIO_NONE = "NONE";
    public static final String SCENARIO_TRANSIENT_RETRY = "TRANSIENT_RETRY";
    public static final String SCENARIO_EXHAUST_RETRY_TO_DLT = "EXHAUST_RETRY_TO_DLT";
    public static final String SCENARIO_NON_RETRYABLE_FATAL = "NON_RETRYABLE_FATAL";

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    }

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a normal, successful order event.
     */
    public OrderEvent sendSuccessOrder(String customerId, Double amount) {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return sendOrder(orderId, customerId, amount, "CREATED", SCENARIO_NONE, "Standard successful order");
    }

    /**
     * Publishes an order marked with TRANSIENT_RETRY scenario.
     * The consumer will fail on initial attempt(s) with a retryable exception,
     * and succeed upon retry attempt, demonstrating automatic retry recovery.
     */
    public OrderEvent sendTransientRetryOrder(String customerId, Double amount) {
        String orderId = "ORD-RETRY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return sendOrder(orderId, customerId, amount, "PENDING", SCENARIO_TRANSIENT_RETRY,
                "Simulated transient failure: will recover after retry");
    }

    /**
     * Publishes an order marked with EXHAUST_RETRY_TO_DLT scenario.
     * The consumer will continuously throw a retryable exception, exhaust all retries,
     * and DefaultErrorHandler will forward the message to the Dead Letter Topic (order-events.DLT).
     */
    public OrderEvent sendExhaustRetryDltOrder(String customerId, Double amount) {
        String orderId = "ORD-DLT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return sendOrder(orderId, customerId, amount, "PENDING", SCENARIO_EXHAUST_RETRY_TO_DLT,
                "Simulated persistent failure: will exhaust retries and route to DLT");
    }

    /**
     * Publishes an order marked with NON_RETRYABLE_FATAL scenario.
     * The consumer will throw a fatal/non-retryable exception (e.g. IllegalArgumentException),
     * causing DefaultErrorHandler to immediately route the event straight to DLT without retrying.
     */
    public OrderEvent sendFatalNonRetryableOrder(String customerId, Double amount) {
        String orderId = "ORD-FATAL-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return sendOrder(orderId, customerId, amount, "FAILED", SCENARIO_NON_RETRYABLE_FATAL,
                "Simulated fatal non-retryable exception: routed directly to DLT");
    }

    /**
     * Builds and sends an OrderEvent with specified attributes.
     */
    public OrderEvent sendOrder(
            String orderId,
            String customerId,
            Double amount,
            String status,
            String failureScenario,
            String message) {

        String finalOrderId = (orderId != null && !orderId.isBlank())
                ? orderId
                : "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String finalCustomerId = (customerId != null && !customerId.isBlank()) ? customerId : "CUST-1001";
        double finalAmount = (amount != null && amount >= 0) ? amount : 99.99;
        String finalStatus = (status != null && !status.isBlank()) ? status : "CREATED";
        String finalScenario = (failureScenario != null && !failureScenario.isBlank())
                ? failureScenario.toUpperCase()
                : SCENARIO_NONE;
        String finalMessage = (message != null) ? message : "";

        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId(finalOrderId)
                .setCustomerId(finalCustomerId)
                .setAmount(finalAmount)
                .setCurrency("USD")
                .setStatus(finalStatus)
                .setFailureScenario(finalScenario)
                .setRetryCount(0)
                .setMessage(finalMessage)
                .setTimestamp(Instant.now())
                .build();

        return sendOrder(event);
    }

    /**
     * Sends an OrderEvent instance to the Kafka topic 'order-events' with validation and async callback logging.
     */
    public OrderEvent sendOrder(OrderEvent event) {
        validateEvent(event);
        String orderId = event.getOrderId().toString();
        String scenario = event.getFailureScenario() != null ? event.getFailureScenario().toString() : "NONE";

        log.info("[ORDER-PRODUCER] Publishing order {} (scenario={}) to topic {}",
                orderId, scenario, KafkaTopicConfig.ORDER_TOPIC);

        kafkaTemplate.send(KafkaTopicConfig.ORDER_TOPIC, orderId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("[ORDER-PRODUCER] Failed to send order {} to topic {}: {}",
                                orderId, KafkaTopicConfig.ORDER_TOPIC, exception.getMessage(), exception);
                    } else {
                        RecordMetadata metadata = result.getRecordMetadata();
                        log.info("[ORDER-PRODUCER] Successfully published order {} to topic {} [partition={}, offset={}]",
                                orderId, metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });

        return event;
    }

    /**
     * Validates that the order strictly complies with OrderEvent.avsc schema and business constraints.
     */
    public void validateEvent(OrderEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Cannot send null OrderEvent");
        }

        if (!OrderEvent.getClassSchema().equals(event.getSchema())) {
            throw new IllegalArgumentException(String.format(
                    "Event schema [%s] does not match target Avro schema [%s]",
                    event.getSchema().getFullName(), OrderEvent.getClassSchema().getFullName()));
        }

        try {
            OrderEvent.getEncoder().encode(event);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "OrderEvent fails Avro schema validation for schema %s: %s",
                    OrderEvent.getClassSchema().getFullName(), e.getMessage()), e);
        }

        if (event.getOrderId() == null || event.getOrderId().toString().trim().isEmpty()) {
            throw new IllegalArgumentException("OrderEvent 'orderId' is required and cannot be blank");
        }

        if (event.getCustomerId() == null || event.getCustomerId().toString().trim().isEmpty()) {
            throw new IllegalArgumentException("OrderEvent 'customerId' is required and cannot be blank");
        }

        if (event.getAmount() < 0) {
            throw new IllegalArgumentException("OrderEvent 'amount' cannot be negative: " + event.getAmount());
        }

        if (event.getTimestamp() == null) {
            throw new IllegalArgumentException("OrderEvent 'timestamp' is required and cannot be null");
        }

        log.debug("[ORDER-PRODUCER] Avro validation passed for orderId: {}", event.getOrderId());
    }
}
