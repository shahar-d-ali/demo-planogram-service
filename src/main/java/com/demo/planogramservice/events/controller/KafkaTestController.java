package com.demo.planogramservice.events.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.demo.planogramservice.events.consumer.AuditPingEventConsumer;
import com.demo.planogramservice.events.consumer.NotificationPingEventConsumer;
import com.demo.planogramservice.events.consumer.PingEventConsumer;
import com.demo.planogramservice.events.domain.KafkaEventLogsRepository;
import com.demo.planogramservice.events.dto.KafkaPullResultDto;
import com.demo.planogramservice.events.service.KafkaEventPullService;
import com.demo.shared.events.dto.PingEvent;

import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Kafka Messaging & Audit Logs", description = "Kafka Consumer Group Telemetry, Event Logs, and Broker Record Sync APIs")
@RestController
@RequestMapping("/kafka")
public class KafkaTestController {

    private final PingEventConsumer pingEventConsumer;
    private final AuditPingEventConsumer auditPingEventConsumer;
    private final NotificationPingEventConsumer notificationPingEventConsumer;
    private final KafkaEventLogsRepository kafkaEventLogsRepository;
    private final KafkaEventPullService kafkaEventPullService;

    public KafkaTestController(
            PingEventConsumer pingEventConsumer,
            AuditPingEventConsumer auditPingEventConsumer,
            NotificationPingEventConsumer notificationPingEventConsumer,
            KafkaEventLogsRepository kafkaEventLogsRepository,
            KafkaEventPullService kafkaEventPullService) {
        this.pingEventConsumer = pingEventConsumer;
        this.auditPingEventConsumer = auditPingEventConsumer;
        this.notificationPingEventConsumer = notificationPingEventConsumer;
        this.kafkaEventLogsRepository = kafkaEventLogsRepository;
        this.kafkaEventPullService = kafkaEventPullService;
    }

    /**
     * Primary consumer group: planogram-service-group
     */
    @GetMapping("/ping/last")
    public ResponseEntity<?> getLastPing() {
        Map<String, Object> pingMap = pingEventConsumer.getLastPingMap();
        if (pingMap != null) {
            return ResponseEntity.ok(pingMap);
        }

        PingEvent lastPing = pingEventConsumer.getLastReceivedPing();
        if (lastPing == null) {
            return ResponseEntity.ok(Map.of("message", "No ping received yet by planogram-service-group"));
        }

        return ResponseEntity.ok(Map.of(
                "id", lastPing.getId().toString(),
                "message", lastPing.getMessage().toString(),
                "sourceService", lastPing.getSourceService().toString(),
                "timestamp", lastPing.getTimestamp()));
    }

    /**
     * Second consumer group: audit-service-group
     */
    @GetMapping("/audit/last")
    public ResponseEntity<?> getLastAuditPing() {
        Map<String, Object> pingMap = auditPingEventConsumer.getLastPingMap();
        if (pingMap != null) {
            return ResponseEntity.ok(pingMap);
        }
        return ResponseEntity.ok(Map.of("message", "No ping received yet by audit-service-group"));
    }

    /**
     * Third consumer group: notification-service-group
     */
    @GetMapping("/notification/last")
    public ResponseEntity<?> getLastNotificationPing() {
        Map<String, Object> pingMap = notificationPingEventConsumer.getLastPingMap();
        if (pingMap != null) {
            return ResponseEntity.ok(pingMap);
        }
        return ResponseEntity.ok(Map.of("message", "No ping received yet by notification-service-group"));
    }

    /**
     * Fan-Out status overview comparing all 3 independent consumer groups
     */
    @GetMapping("/consumer-groups")
    public ResponseEntity<?> getAllConsumerGroups() {
        return ResponseEntity.ok(Map.of(
                "topic", "demo-ping-topic",
                "pattern", "Fan-Out (Publish-Subscribe)",
                "consumerCount", 3,
                "groups", List.of(
                        pingEventConsumer.getGroupStatus(),
                        auditPingEventConsumer.getGroupStatus(),
                        notificationPingEventConsumer.getGroupStatus())));
    }

    /**
     * Inspect load-balancing across workers inside planogram-service-group
     */
    @GetMapping("/planogram/workers")
    public ResponseEntity<?> getPlanogramWorkers() {
        return ResponseEntity.ok(pingEventConsumer.getGroupStatus());
    }

    /**
     * Complete historical audit trail of all Kafka event coordinates logged in
     * MySQL
     */
    @GetMapping("/logs")
    public ResponseEntity<?> getKafkaLogs() {
        return ResponseEntity.ok(kafkaEventLogsRepository.findTop50ByOrderByConsumedAtDesc());
    }

    /**
     * Pulls existing events from Kafka topic across all partitions starting from offset 0
     * and saves any unlogged events into the kafka_event_logs table.
     */
    @PostMapping("/pull-existing")
    public ResponseEntity<KafkaPullResultDto> pullExistingEvents(
            @RequestParam(name = "topic", defaultValue = "demo-ping-topic") String topic) {
        return ResponseEntity.ok(kafkaEventPullService.pullExistingEvents(topic));
    }

    @GetMapping("/pull-existing")
    public ResponseEntity<KafkaPullResultDto> pullExistingEventsGet(
            @RequestParam(name = "topic", defaultValue = "demo-ping-topic") String topic) {
        return ResponseEntity.ok(kafkaEventPullService.pullExistingEvents(topic));
    }
}
