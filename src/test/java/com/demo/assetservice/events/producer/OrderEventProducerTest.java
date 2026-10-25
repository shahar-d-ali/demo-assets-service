package com.demo.assetservice.events.producer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.demo.assetservice.events.config.KafkaTopicConfig;
import com.demo.shared.events.dto.OrderEvent;

@ExtendWith(MockitoExtension.class)
class OrderEventProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private SendResult<String, Object> sendResult;

    private OrderEventProducer producer;

    @BeforeEach
    void setUp() {
        producer = new OrderEventProducer(kafkaTemplate);
    }

    @Test
    void testValidateEventPassesForValidOrder() {
        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId("ORD-1001")
                .setCustomerId("CUST-5001")
                .setAmount(199.95)
                .setCurrency("USD")
                .setStatus("CREATED")
                .setFailureScenario(OrderEventProducer.SCENARIO_NONE)
                .setRetryCount(0)
                .setMessage("Valid test order")
                .setTimestamp(Instant.now())
                .build();

        assertDoesNotThrow(() -> producer.validateEvent(event));
    }

    @Test
    void testValidateEventRejectsNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> producer.validateEvent(null));
    }

    @Test
    void testValidateEventRejectsBlankOrderId() {
        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId("   ")
                .setCustomerId("CUST-5001")
                .setAmount(50.0)
                .setCurrency("USD")
                .setStatus("CREATED")
                .setFailureScenario(OrderEventProducer.SCENARIO_NONE)
                .setRetryCount(0)
                .setMessage("Missing order ID")
                .setTimestamp(Instant.now())
                .build();

        assertThrows(IllegalArgumentException.class, () -> producer.validateEvent(event));
    }

    @Test
    void testValidateEventRejectsBlankCustomerId() {
        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId("ORD-1002")
                .setCustomerId("")
                .setAmount(50.0)
                .setCurrency("USD")
                .setStatus("CREATED")
                .setFailureScenario(OrderEventProducer.SCENARIO_NONE)
                .setRetryCount(0)
                .setMessage("Missing customer ID")
                .setTimestamp(Instant.now())
                .build();

        assertThrows(IllegalArgumentException.class, () -> producer.validateEvent(event));
    }

    @Test
    void testValidateEventRejectsNegativeAmount() {
        OrderEvent event = OrderEvent.newBuilder()
                .setOrderId("ORD-1003")
                .setCustomerId("CUST-5001")
                .setAmount(-10.0)
                .setCurrency("USD")
                .setStatus("CREATED")
                .setFailureScenario(OrderEventProducer.SCENARIO_NONE)
                .setRetryCount(0)
                .setMessage("Negative amount")
                .setTimestamp(Instant.now())
                .build();

        assertThrows(IllegalArgumentException.class, () -> producer.validateEvent(event));
    }

    @Test
    void testSendSuccessOrder() {
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(eq(KafkaTopicConfig.ORDER_TOPIC), any(), any())).thenReturn(future);

        OrderEvent result = producer.sendSuccessOrder("CUST-1001", 89.50);

        assertNotNull(result);
        assertTrue(result.getOrderId().toString().startsWith("ORD-"));
        assertEquals("CUST-1001", result.getCustomerId().toString());
        assertEquals(89.50, result.getAmount());
        assertEquals("NONE", result.getFailureScenario().toString());
        verify(kafkaTemplate).send(eq(KafkaTopicConfig.ORDER_TOPIC), eq(result.getOrderId().toString()), eq(result));
    }

    @Test
    void testSendTransientRetryOrder() {
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(eq(KafkaTopicConfig.ORDER_TOPIC), any(), any())).thenReturn(future);

        OrderEvent result = producer.sendTransientRetryOrder("CUST-1002", 120.00);

        assertNotNull(result);
        assertEquals("TRANSIENT_RETRY", result.getFailureScenario().toString());
        verify(kafkaTemplate).send(eq(KafkaTopicConfig.ORDER_TOPIC), eq(result.getOrderId().toString()), eq(result));
    }

    @Test
    void testSendExhaustRetryDltOrder() {
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(eq(KafkaTopicConfig.ORDER_TOPIC), any(), any())).thenReturn(future);

        OrderEvent result = producer.sendExhaustRetryDltOrder("CUST-1003", 350.00);

        assertNotNull(result);
        assertEquals("EXHAUST_RETRY_TO_DLT", result.getFailureScenario().toString());
        verify(kafkaTemplate).send(eq(KafkaTopicConfig.ORDER_TOPIC), eq(result.getOrderId().toString()), eq(result));
    }

    @Test
    void testSendFatalNonRetryableOrder() {
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(eq(KafkaTopicConfig.ORDER_TOPIC), any(), any())).thenReturn(future);

        OrderEvent result = producer.sendFatalNonRetryableOrder("CUST-1004", 500.00);

        assertNotNull(result);
        assertEquals("NON_RETRYABLE_FATAL", result.getFailureScenario().toString());
        verify(kafkaTemplate).send(eq(KafkaTopicConfig.ORDER_TOPIC), eq(result.getOrderId().toString()), eq(result));
    }
}
