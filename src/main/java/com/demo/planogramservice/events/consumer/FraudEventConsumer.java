package com.demo.planogramservice.events.consumer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.demo.planogramservice.events.config.KafkaTopicConfig;
import com.demo.planogramservice.events.domain.KafkaEventLogs;
import com.demo.planogramservice.events.domain.KafkaEventLogsRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class FraudEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(FraudEventConsumer.class);
    public static final String GROUP_ID = "planogram-fraud-service-group";

    private final KafkaEventLogsRepository kafkaEventLogsRepository;
    private final ObjectMapper objectMapper;

    private final AtomicInteger totalFraudDetected = new AtomicInteger(0);
    private final AtomicReference<Map<String, Object>> lastFraudEvent = new AtomicReference<>();
    private final List<Map<String, Object>> recentFraudEvents = Collections.synchronizedList(new ArrayList<>());

    public FraudEventConsumer(KafkaEventLogsRepository kafkaEventLogsRepository, ObjectMapper objectMapper) {
        this.kafkaEventLogsRepository = kafkaEventLogsRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = KafkaTopicConfig.FRAUD_DETECTION_TOPIC,
            groupId = GROUP_ID,
            properties = {
                    "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                    "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
            }
    )
    public void onFraudEvent(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String payload = record.value();
            log.warn("ALERT: Fraudulent transaction detected on topic {} [partition={}, offset={}, key={}]: {}",
                    record.topic(), record.partition(), record.offset(), record.key(), payload);

            totalFraudDetected.incrementAndGet();

            String txId = record.key();
            Double amount = null;
            try {
                JsonNode node = objectMapper.readTree(payload);
                if (node.has("transactionId") && !node.get("transactionId").isNull()) {
                    txId = node.get("transactionId").asText();
                }
                if (node.has("amount") && !node.get("amount").isNull()) {
                    amount = node.get("amount").asDouble();
                }
            } catch (Exception ignored) {}

            Map<String, Object> fraudData = Map.of(
                    "transactionId", txId != null ? txId : "unknown",
                    "amount", amount != null ? amount : 0.0,
                    "partition", record.partition(),
                    "offset", record.offset(),
                    "detectedAt", Instant.now().toString(),
                    "payload", payload
            );

            lastFraudEvent.set(fraudData);
            if (recentFraudEvents.size() >= 20) {
                recentFraudEvents.remove(0);
            }
            recentFraudEvents.add(fraudData);

            KafkaEventLogs logEntry = KafkaEventLogs.builder()
                    .topic(record.topic())
                    .partitionId(record.partition())
                    .recordOffset(record.offset())
                    .eventKey(record.key())
                    .recordTimestamp(record.timestamp())
                    .timestampType(record.timestampType() != null ? record.timestampType().name : "CreateTime")
                    .consumerGroupId(GROUP_ID)
                    .workerName("Fraud-Streams-Worker")
                    .eventId(txId)
                    .sourceService("planogram-fraud-detection")
                    .message("Suspicious transaction flagged (> 1000)")
                    .payloadJson(payload)
                    .consumedAt(Instant.now())
                    .status("FRAUD_DETECTED")
                    .build();

            kafkaEventLogsRepository.save(logEntry);
        } catch (Exception e) {
            log.error("Error processing fraud event record: {}", record, e);
        } finally {
            if (ack != null) {
                ack.acknowledge();
            }
        }
    }

    public Map<String, Object> getFraudStatus() {
        return Map.of(
                "totalFraudDetected", totalFraudDetected.get(),
                "lastFraudEvent", lastFraudEvent.get() != null ? lastFraudEvent.get() : Map.of("message", "No fraud events received yet"),
                "recentFraudEvents", new ArrayList<>(recentFraudEvents)
        );
    }
}
