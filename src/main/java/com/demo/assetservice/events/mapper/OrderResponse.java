package com.demo.assetservice.events.mapper;

import java.time.Instant;

import com.demo.shared.events.dto.OrderEvent;

public record OrderResponse(
    String orderId,
    String customerId,
    Double amount,
    String currency,
    String status,
    String failureScenario,
    Integer retryCount,
    String message,
    Instant timestamp
) {
    public static OrderResponse from(OrderEvent event) {
        return new OrderResponse(
            event.getOrderId() != null ? event.getOrderId().toString() : null,
            event.getCustomerId() != null ? event.getCustomerId().toString() : null,
            event.getAmount(),
            event.getCurrency() != null ? event.getCurrency().toString() : "USD",
            event.getStatus() != null ? event.getStatus().toString() : "PENDING",
            event.getFailureScenario() != null ? event.getFailureScenario().toString() : "NONE",
            event.getRetryCount(),
            event.getMessage() != null ? event.getMessage().toString() : "",
            event.getTimestamp()
        );
    }
}
