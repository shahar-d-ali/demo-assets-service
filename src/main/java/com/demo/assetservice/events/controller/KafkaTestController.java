package com.demo.assetservice.events.controller;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.demo.assetservice.events.dto.OrderRequest;
import com.demo.assetservice.events.dto.PingRequest;
import com.demo.assetservice.events.mapper.OrderResponse;
import com.demo.assetservice.events.mapper.PingResponse;
import com.demo.assetservice.events.producer.OrderEventProducer;
import com.demo.assetservice.events.producer.PingEventProducer;
import com.demo.assetservice.events.producer.TransactionEventProducer;
import com.demo.shared.events.dto.OrderEvent;
import com.demo.shared.events.dto.PingEvent;
import com.demo.shared.events.dto.Transaction;

@RestController
@RequestMapping("/kafka")
public class KafkaTestController {

    private final PingEventProducer pingEventProducer;
    private final TransactionEventProducer transactionEventProducer;
    private final OrderEventProducer orderEventProducer;

    public KafkaTestController(
            PingEventProducer pingEventProducer,
            TransactionEventProducer transactionEventProducer,
            OrderEventProducer orderEventProducer) {
        this.pingEventProducer = pingEventProducer;
        this.transactionEventProducer = transactionEventProducer;
        this.orderEventProducer = orderEventProducer;
    }

    @GetMapping("/transaction")
    public ResponseEntity<String> sendTransactionEvent(
            @RequestParam(name = "count", defaultValue = "100") int count) {
        List<Transaction> transactions = transactionEventProducer.generateTransactions(count);
        String submitResult = transactionEventProducer.sendTransactionEvent(transactions);
        return ResponseEntity.ok(submitResult);
    }

    @PostMapping("/ping")
    public ResponseEntity<PingResponse> ping(
            @RequestBody(required = false) PingRequest request,
            @RequestParam(required = false) String message) {

        String eventId = (request != null && request.id() != null && !request.id().isBlank())
                ? request.id()
                : UUID.randomUUID().toString();

        String eventMessage = (request != null && request.message() != null && !request.message().isBlank())
                ? request.message()
                : (message != null && !message.isBlank() ? message : "Health check ping");

        String sourceService = (request != null && request.sourceService() != null
                && !request.sourceService().isBlank())
                ? request.sourceService()
                : "payment-gateway";

        Instant timestamp = (request != null && request.timestamp() != null && request.timestamp() > 0)
                ? Instant.ofEpochMilli(request.timestamp())
                : Instant.now();

        PingEvent pingEvent = pingEventProducer.sendPing(eventId, eventMessage, sourceService, timestamp);
        return ResponseEntity.ok(PingResponse.from(pingEvent));
    }

    @PostMapping("/order")
    public ResponseEntity<OrderResponse> sendOrder(@RequestBody(required = false) OrderRequest request) {
        String orderId = (request != null && request.orderId() != null && !request.orderId().isBlank())
                ? request.orderId()
                : "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        String customerId = (request != null && request.customerId() != null && !request.customerId().isBlank())
                ? request.customerId()
                : "CUST-1001";

        Double amount = (request != null && request.amount() != null) ? request.amount() : 149.99;
        String status = (request != null && request.status() != null && !request.status().isBlank())
                ? request.status()
                : "CREATED";

        String failureScenario = (request != null && request.failureScenario() != null
                && !request.failureScenario().isBlank())
                ? request.failureScenario().toUpperCase()
                : OrderEventProducer.SCENARIO_NONE;

        String message = (request != null && request.message() != null)
                ? request.message()
                : "Manual order submission";

        OrderEvent event = orderEventProducer.sendOrder(
                orderId, customerId, amount, status, failureScenario, message);

        return ResponseEntity.ok(OrderResponse.from(event));
    }

    @PostMapping("/order/scenario/{scenario}")
    public ResponseEntity<OrderResponse> triggerScenario(
            @PathVariable String scenario,
            @RequestParam(required = false, defaultValue = "CUST-1001") String customerId,
            @RequestParam(required = false, defaultValue = "249.50") Double amount) {

        String norm = scenario.trim().toLowerCase();
        OrderEvent event = switch (norm) {
            case "success", "none", "normal" ->
                orderEventProducer.sendSuccessOrder(customerId, amount);
            case "transient-retry", "transient", "retry" ->
                orderEventProducer.sendTransientRetryOrder(customerId, amount);
            case "exhaust-retry-dlt", "exhaust", "dlt", "retry-exhaust" ->
                orderEventProducer.sendExhaustRetryDltOrder(customerId, amount);
            case "fatal-error", "fatal", "non-retryable" ->
                orderEventProducer.sendFatalNonRetryableOrder(customerId, amount);
            default ->
                orderEventProducer.sendOrder(
                        null, customerId, amount, "PENDING", scenario.toUpperCase(),
                        "Custom scenario trigger: " + scenario);
        };

        return ResponseEntity.ok(OrderResponse.from(event));
    }

    @GetMapping("/order/scenarios")
    public ResponseEntity<Map<String, Object>> getAvailableScenarios() {
        return ResponseEntity.ok(Map.of(
                "title", "Kafka Error Handling & Failure Scenarios Learning Kit",
                "scenarios", List.of(
                        Map.of(
                                "key", "success",
                                "name", "Normal Success",
                                "description", "Order is processed cleanly on first attempt with no errors.",
                                "expectedBehavior", "Delivered to consumer -> ACK -> Status: ORDER_SUCCESS"
                        ),
                        Map.of(
                                "key", "transient-retry",
                                "name", "Transient Error (Retry Recovery)",
                                "description", "Simulates temporary network/DB timeout. Fails attempt #1, then succeeds on attempt #2.",
                                "expectedBehavior", "Attempt 1 fails -> BackOff delay (1s) -> Retry Attempt 2 succeeds -> ACK -> Status: RECOVERED_AFTER_RETRY"
                        ),
                        Map.of(
                                "key", "exhaust-retry-dlt",
                                "name", "Persistent Error (Exhaust Retries to DLT)",
                                "description", "Throws retryable exception continuously. Exhausts all 3 attempts.",
                                "expectedBehavior", "Attempts 1, 2, 3 fail -> DefaultErrorHandler invokes DeadLetterPublishingRecoverer -> Forwarded to order-events.DLT"
                        ),
                        Map.of(
                                "key", "fatal-error",
                                "name", "Fatal / Non-Retryable Error (Bypass Retries to DLT)",
                                "description", "Throws IllegalArgumentException (configured in DefaultErrorHandler as not retryable).",
                                "expectedBehavior", "Directly forwarded to order-events.DLT on attempt 1 without redundant retries."
                        )
                ),
                "endpoints", Map.of(
                        "triggerScenario", "POST /kafka/order/scenario/{scenario} (values: success, transient-retry, exhaust-retry-dlt, fatal-error)",
                        "customOrder", "POST /kafka/order",
                        "consumerStatus", "GET http://localhost:8002/api/planogram/kafka/order/status",
                        "dltStatus", "GET http://localhost:8002/api/planogram/kafka/order/dlt",
                        "viewDltPayloads", "GET http://localhost:8002/api/planogram/kafka/topic/payloads?topic=order-events.DLT"
                )
        ));
    }
}

