package com.demo.assetservice.events.dto;

public record OrderRequest(
    String orderId,
    String customerId,
    Double amount,
    String currency,
    String status,
    String failureScenario,
    Integer retryCount,
    String message,
    Long timestamp
) {}
