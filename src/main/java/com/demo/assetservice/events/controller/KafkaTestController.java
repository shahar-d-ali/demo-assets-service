package com.demo.assetservice.events.controller;

import com.demo.assetservice.events.dto.PingEvent;
import com.demo.assetservice.events.producer.PingEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaTestController {

    private final PingEventProducer pingEventProducer;

    public KafkaTestController(PingEventProducer pingEventProducer) {
        this.pingEventProducer = pingEventProducer;
    }

    @PostMapping("/ping")
    public ResponseEntity<PingEvent> ping(@RequestParam(defaultValue = "ping from assets-service") String message) {
        PingEvent event = pingEventProducer.sendPing(message);
        return ResponseEntity.ok(event);
    }
}
