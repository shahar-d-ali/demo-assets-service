package com.demo.assetservice.events.mapper;

import java.time.Instant;

import com.demo.shared.events.dto.PingEvent;

public record PingResponse(String id, String message, String sourceService, Instant timestamp) {
    public static PingResponse from(PingEvent event) {
        return new PingResponse(
            event.getId().toString(),
            event.getMessage().toString(),
            event.getSourceService().toString(),
            event.getTimestamp()
        );
    }
}
