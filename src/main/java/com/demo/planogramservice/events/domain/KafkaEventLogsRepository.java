package com.demo.planogramservice.events.domain;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KafkaEventLogsRepository extends JpaRepository<KafkaEventLogs, Long> {

    List<KafkaEventLogs> findByTopicOrderByConsumedAtDesc(String topic);

    List<KafkaEventLogs> findByConsumerGroupIdOrderByConsumedAtDesc(String consumerGroupId);

    List<KafkaEventLogs> findByEventIdOrderByConsumedAtDesc(String eventId);

    Optional<KafkaEventLogs> findFirstByTopicAndPartitionIdAndRecordOffset(
            String topic, Integer partitionId, Long recordOffset);

    List<KafkaEventLogs> findTop50ByOrderByConsumedAtDesc();
}
