package com.demo.planogramservice.events.dto;

import java.util.List;
import java.util.Map;

/**
 * Result DTO returned when pulling existing events from a Kafka topic
 * and persisting unlogged records into kafka_event_logs table.
 */
public record KafkaPullResultDto(
        String status,
        String topic,
        int partitionCount,
        long totalMessagesOnBroker,
        int newlySavedCount,
        int alreadyPresentCount,
        Map<Integer, Long> brokerPartitionOffsets,
        List<Map<String, Object>> newlySavedEventsSummary,
        String message
) {}
