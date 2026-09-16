package com.demo.planogramservice.events.consumer;

import com.demo.planogramservice.events.dto.PingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicReference;

@Service
public class PingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PingEventConsumer.class);

    private final AtomicReference<PingEvent> lastReceivedPing = new AtomicReference<>();

    @KafkaListener(topics = "demo-ping-topic", groupId = "planogram-service-group")
    public void consume(PingEvent event) {
        log.info("Received ping event from Kafka topic demo-ping-topic: {}", event);
        lastReceivedPing.set(event);
    }

    public PingEvent getLastReceivedPing() {
        return lastReceivedPing.get();
    }
}
