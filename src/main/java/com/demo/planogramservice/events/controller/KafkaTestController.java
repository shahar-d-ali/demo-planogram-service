package com.demo.planogramservice.events.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.demo.planogramservice.events.consumer.PingEventConsumer;
import com.demo.shared.events.dto.PingEvent;

@RestController
@RequestMapping("/kafka")
public class KafkaTestController {

    private final PingEventConsumer pingEventConsumer;

    public KafkaTestController(PingEventConsumer pingEventConsumer) {
        this.pingEventConsumer = pingEventConsumer;
    }

    @GetMapping("/ping/last")
    public ResponseEntity<?> getLastPing() {
        PingEvent lastPing = pingEventConsumer.getLastReceivedPing();
        if (lastPing == null) {
            return ResponseEntity.ok(Map.of("message", "No ping received yet"));
        }

        return ResponseEntity.ok(Map.of(
                "id", lastPing.getId().toString(),
                "message", lastPing.getMessage().toString(),
                "sourceService", lastPing.getSourceService().toString(),
                "timestamp", lastPing.getTimestamp()));
    }
}
