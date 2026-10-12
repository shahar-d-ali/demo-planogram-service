package com.demo.planogramservice.events.domain;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Entity that logs every consumed Kafka event along with its complete
 * broker coordinates, consumer instance metadata, and payload details.
 */
@Entity
@Table(
    name = "kafka_event_logs",
    indexes = {
        @Index(name = "idx_kafka_coords", columnList = "topic, partition_id, record_offset"),
        @Index(name = "idx_event_id", columnList = "event_id"),
        @Index(name = "idx_consumer_group", columnList = "consumer_group_id"),
        @Index(name = "idx_consumed_at", columnList = "consumed_at")
    }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class KafkaEventLogs {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // =========================================================================
    // 1. KAFKA BROKER / CLUSTER COORDINATES
    // =========================================================================

    /**
     * Kafka Topic name (e.g., demo-ping-topic)
     */
    @Column(name = "topic", nullable = false, length = 120)
    private String topic;

    /**
     * Specific partition ID where the event was stored (0, 1, 2, ...)
     */
    @Column(name = "partition_id", nullable = false)
    private Integer partitionId;

    /**
     * Sequential position / offset of this record within its partition
     */
    @Column(name = "record_offset", nullable = false)
    private Long recordOffset;

    /**
     * Partitioning key sent by producer (e.g., eventId)
     */
    @Column(name = "event_key", length = 100)
    private String eventKey;

    /**
     * Kafka record timestamp in milliseconds epoch
     */
    @Column(name = "record_timestamp")
    private Long recordTimestamp;

    /**
     * Type of timestamp: CreateTime or LogAppendTime
     */
    @Column(name = "timestamp_type", length = 50)
    private String timestampType;

    /**
     * Partition leader epoch assigned by Kafka broker, if available
     */
    @Column(name = "leader_epoch")
    private Integer leaderEpoch;

    // =========================================================================
    // 2. CONSUMER & WORKER COORDINATES
    // =========================================================================

    /**
     * The consumer group that consumed the event (e.g., planogram-service-group, audit-service-group)
     */
    @Column(name = "consumer_group_id", nullable = false, length = 100)
    private String consumerGroupId;

    /**
     * Identifier of the worker thread/instance (e.g., Worker-1, Worker-2, Worker-3)
     */
    @Column(name = "worker_name", length = 100)
    private String workerName;

    /**
     * Kafka client ID prefix or consumer client identifier
     */
    @Column(name = "consumer_client_id", length = 150)
    private String consumerClientId;

    /**
     * JVM thread name executing the consumer listener (e.g., planogram-worker-1-0-C-1)
     */
    @Column(name = "thread_name", length = 150)
    private String threadName;

    // =========================================================================
    // 3. BUSINESS EVENT DATA & PAYLOAD COORDINATES
    // =========================================================================

    /**
     * Business Event ID extracted from payload (e.g., EV001, A1B2C)
     */
    @Column(name = "event_id", length = 50)
    private String eventId;

    /**
     * Originating producer service name (e.g., demo-assets-service)
     */
    @Column(name = "source_service", length = 100)
    private String sourceService;

    /**
     * Text payload / ping message
     */
    @Column(name = "message", length = 1000)
    private String message;

    /**
     * Producer-generated epoch timestamp inside the event payload
     */
    @Column(name = "payload_timestamp")
    private Long payloadTimestamp;

    /**
     * Complete JSON serialization of the event payload for auditability
     */
    @Column(name = "payload_json", columnDefinition = "TEXT")
    private String payloadJson;

    /**
     * Kafka record headers serialized as JSON / key-value string
     */
    @Column(name = "headers_json", length = 2000)
    private String headersJson;

    // =========================================================================
    // 4. CONSUMPTION LIFECYCLE & AUDIT
    // =========================================================================

    /**
     * Exact timestamp when the record was consumed and logged by this service
     */
    @Column(name = "consumed_at", nullable = false)
    private Instant consumedAt;

    /**
     * Time taken in milliseconds to process and acknowledge the event
     */
    @Column(name = "processing_duration_ms")
    private Long processingDurationMs;

    /**
     * Consumption status: CONSUMED, ACKNOWLEDGED, REPLAYED, or ERROR
     */
    @Column(name = "status", nullable = false, length = 50)
    private String status;

    /**
     * Error message or stacktrace summary if ingestion failed
     */
    @Column(name = "error_message", length = 2000)
    private String errorMessage;

    @PrePersist
    public void prePersist() {
        if (this.consumedAt == null) {
            this.consumedAt = Instant.now();
        }
        if (this.status == null) {
            this.status = "CONSUMED";
        }
    }
}
