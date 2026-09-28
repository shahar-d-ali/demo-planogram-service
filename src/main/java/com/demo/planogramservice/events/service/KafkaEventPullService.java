package com.demo.planogramservice.events.service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import com.demo.planogramservice.events.domain.KafkaEventLogs;
import com.demo.planogramservice.events.domain.KafkaEventLogsRepository;
import com.demo.planogramservice.events.dto.KafkaPullResultDto;
import com.demo.shared.events.dto.PingEvent;

/**
 * Service to pull existing events from Apache Kafka topic partitions from offset 0 (earliest)
 * and persist any unlogged records into MySQL kafka_event_logs table.
 */
@Service
public class KafkaEventPullService {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPullService.class);

    private final ConsumerFactory<Object, Object> consumerFactory;
    private final KafkaEventLogsRepository kafkaEventLogsRepository;

    public KafkaEventPullService(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaEventLogsRepository kafkaEventLogsRepository) {
        this.consumerFactory = consumerFactory;
        this.kafkaEventLogsRepository = kafkaEventLogsRepository;
    }

    /**
     * Pulls all existing records for the given topic from offset 0 across all partitions
     * and saves any missing records into the kafka_event_logs table.
     *
     * @param topic the Kafka topic name (defaults to "demo-ping-topic")
     * @return KafkaPullResultDto with statistics of pulled and saved records
     */
    public KafkaPullResultDto pullExistingEvents(String topic) {
        if (topic == null || topic.isBlank()) {
            topic = "demo-ping-topic";
        }

        String tempGroupId = "historical-pull-sync-" + UUID.randomUUID();
        Consumer<Object, Object> consumer = null;

        try {
            consumer = consumerFactory.createConsumer(tempGroupId, "topic-puller");

            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                log.warn("[PULL-SERVICE] Topic '{}' has no partitions or does not exist on Kafka broker.", topic);
                return new KafkaPullResultDto(
                        "WARNING",
                        topic,
                        0,
                        0L,
                        0,
                        0,
                        Map.of(),
                        List.of(),
                        "Topic '" + topic + "' has no partitions or does not exist on the broker."
                );
            }

            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .sorted(Comparator.comparingInt(TopicPartition::partition))
                    .toList();

            // Assign partitions directly to avoid consumer group rebalancing overhead
            consumer.assign(topicPartitions);

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            long totalAvailableMessages = 0;
            Map<Integer, Long> brokerPartitionOffsets = new LinkedHashMap<>();
            List<TopicPartition> activePartitions = new ArrayList<>();

            for (TopicPartition tp : topicPartitions) {
                long beg = beginningOffsets.getOrDefault(tp, 0L);
                long end = endOffsets.getOrDefault(tp, 0L);
                long count = Math.max(0, end - beg);
                totalAvailableMessages += count;
                brokerPartitionOffsets.put(tp.partition(), end);
                if (end > beg) {
                    activePartitions.add(tp);
                }
            }

            log.info("[PULL-SERVICE] Found {} total message(s) on broker for topic '{}' across {} partitions",
                    totalAvailableMessages, topic, topicPartitions.size());

            // Seek all partitions to earliest offset (0)
            consumer.seekToBeginning(topicPartitions);

            int totalPolled = 0;
            int newlySavedCount = 0;
            int alreadyPresentCount = 0;
            List<Map<String, Object>> savedSummaries = new ArrayList<>();

            if (!activePartitions.isEmpty()) {
                boolean allReachedEnd = false;
                int emptyPollStreak = 0;
                final int maxEmptyPolls = 3;
                int iterationCount = 0;
                final int maxIterations = 100;

                while (!allReachedEnd && emptyPollStreak < maxEmptyPolls && iterationCount < maxIterations) {
                    iterationCount++;
                    ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(500));

                    if (records.isEmpty()) {
                        emptyPollStreak++;
                    } else {
                        emptyPollStreak = 0;
                        for (ConsumerRecord<Object, Object> record : records) {
                            totalPolled++;
                            int partition = record.partition();
                            long offset = record.offset();
                            String recordTopic = record.topic();

                            // Check if this coordinate already exists in kafka_event_logs table
                            Optional<KafkaEventLogs> existingLog = kafkaEventLogsRepository
                                    .findFirstByTopicAndPartitionIdAndRecordOffset(recordTopic, partition, offset);

                            if (existingLog.isPresent()) {
                                alreadyPresentCount++;
                                continue;
                            }

                            // Save new log entry
                            KafkaEventLogs savedEntry = persistRecord(record);
                            if (savedEntry != null) {
                                newlySavedCount++;
                                if (savedSummaries.size() < 25) {
                                    Map<String, Object> summary = new LinkedHashMap<>();
                                    summary.put("id", savedEntry.getId());
                                    summary.put("eventId", savedEntry.getEventId() != null ? savedEntry.getEventId() : "N/A");
                                    summary.put("partition", savedEntry.getPartitionId());
                                    summary.put("offset", savedEntry.getRecordOffset());
                                    summary.put("sourceService", savedEntry.getSourceService());
                                    summary.put("message", savedEntry.getMessage());
                                    summary.put("status", savedEntry.getStatus());
                                    savedSummaries.add(summary);
                                }
                            }
                        }
                    }

                    // Check if current position of all active partitions reached or exceeded endOffset
                    allReachedEnd = true;
                    for (TopicPartition tp : activePartitions) {
                        long targetEnd = endOffsets.getOrDefault(tp, 0L);
                        long currentPos = consumer.position(tp);
                        if (currentPos < targetEnd) {
                            allReachedEnd = false;
                            break;
                        }
                    }
                }
            }

            String message = String.format(
                    "Topic '%s' scan completed: %d total broker message(s), %d new record(s) saved to kafka_event_logs, %d record(s) already existed.",
                    topic, totalAvailableMessages, newlySavedCount, alreadyPresentCount
            );

            log.info("[PULL-SERVICE] {}", message);

            return new KafkaPullResultDto(
                    "SUCCESS",
                    topic,
                    topicPartitions.size(),
                    totalAvailableMessages,
                    newlySavedCount,
                    alreadyPresentCount,
                    brokerPartitionOffsets,
                    savedSummaries,
                    message
            );

        } catch (Exception e) {
            log.error("[PULL-SERVICE] Error pulling existing events from Kafka topic '{}': {}", topic, e.getMessage(), e);
            return new KafkaPullResultDto(
                    "ERROR",
                    topic,
                    0,
                    0L,
                    0,
                    0,
                    Map.of(),
                    List.of(),
                    "Failed to pull events from Kafka: " + e.getMessage()
            );
        } finally {
            if (consumer != null) {
                try {
                    consumer.close(Duration.ofSeconds(2));
                } catch (Exception e) {
                    log.warn("[PULL-SERVICE] Exception closing standalone Kafka consumer: {}", e.getMessage());
                }
            }
        }
    }

    private KafkaEventLogs persistRecord(ConsumerRecord<Object, Object> record) {
        Object value = record.value();
        String eventId = null;
        String message = null;
        String sourceService = null;
        Long payloadTimestamp = null;
        String payloadJson = null;

        if (value instanceof PingEvent pingEvent) {
            eventId = pingEvent.getId() != null ? pingEvent.getId().toString() : null;
            message = pingEvent.getMessage() != null ? pingEvent.getMessage().toString() : null;
            sourceService = pingEvent.getSourceService() != null ? pingEvent.getSourceService().toString() : null;
            payloadTimestamp = pingEvent.getTimestamp() != null ? pingEvent.getTimestamp().toEpochMilli() : null;
            payloadJson = pingEvent.toString();
        } else if (value != null) {
            try {
                eventId = extractStringProperty(value, "getId");
                message = extractStringProperty(value, "getMessage");
                sourceService = extractStringProperty(value, "getSourceService");
                Object ts = value.getClass().getMethod("getTimestamp").invoke(value);
                if (ts instanceof Number num) {
                    payloadTimestamp = num.longValue();
                } else if (ts instanceof Instant inst) {
                    payloadTimestamp = inst.toEpochMilli();
                }
            } catch (Exception ex) {
                log.debug("[PULL-SERVICE] Could not extract event fields via reflection: {}", ex.getMessage());
            }
            payloadJson = value.toString();
        }

        String headersJson = null;
        if (record.headers() != null) {
            Map<String, String> headersMap = new LinkedHashMap<>();
            record.headers().forEach(header ->
                    headersMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8)));
            if (!headersMap.isEmpty()) {
                headersJson = headersMap.toString();
            }
        }

        try {
            KafkaEventLogs logEntry = KafkaEventLogs.builder()
                    .topic(record.topic())
                    .partitionId(record.partition())
                    .recordOffset(record.offset())
                    .eventKey(record.key() != null ? record.key().toString() : null)
                    .recordTimestamp(record.timestamp())
                    .timestampType(record.timestampType() != null ? record.timestampType().name() : null)
                    .leaderEpoch(record.leaderEpoch() != null && record.leaderEpoch().isPresent() ? record.leaderEpoch().get() : null)
                    .consumerGroupId("historical-pull-sync")
                    .workerName("Topic-Puller")
                    .consumerClientId("planogram-topic-puller")
                    .threadName(Thread.currentThread().getName())
                    .eventId(eventId)
                    .sourceService(sourceService != null ? sourceService : "demo-assets-service")
                    .message(message)
                    .payloadTimestamp(payloadTimestamp)
                    .payloadJson(payloadJson)
                    .headersJson(headersJson)
                    .consumedAt(Instant.now())
                    .status("PULLED_FROM_TOPIC")
                    .errorMessage(null)
                    .build();

            return kafkaEventLogsRepository.save(logEntry);
        } catch (Exception e) {
            log.error("[PULL-SERVICE] Failed to persist record [topic={}, partition={}, offset={}]: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            return null;
        }
    }

    private String extractStringProperty(Object obj, String methodName) {
        try {
            Object result = obj.getClass().getMethod(methodName).invoke(obj);
            return result != null ? result.toString() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
