package com.demo.assetservice.events.dto;

public record PingRequest(
    String id,
    String message,
    String sourceService,
    Long timestamp
) {}
