package com.demo.planogramservice.events.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.ConsumerFactory;

import com.demo.planogramservice.events.domain.KafkaEventLogs;
import com.demo.planogramservice.events.domain.KafkaEventLogsRepository;
import com.demo.planogramservice.events.dto.KafkaPullResultDto;
import com.demo.shared.events.dto.PingEvent;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class KafkaEventPullServiceTest {

    @Mock
    private ConsumerFactory<Object, Object> consumerFactory;

    @Mock
    private KafkaEventLogsRepository kafkaEventLogsRepository;

    @Mock
    private Consumer<Object, Object> consumer;

    private KafkaEventPullService service;

    @BeforeEach
    void setUp() {
        service = new KafkaEventPullService(consumerFactory, kafkaEventLogsRepository);
    }

    @Test
    @DisplayName("Should return warning when topic does not exist or has no partitions")
    void shouldReturnWarningWhenTopicHasNoPartitions() {
        when(consumerFactory.createConsumer(anyString(), eq("topic-puller"))).thenReturn(consumer);
        when(consumer.partitionsFor("non-existent-topic")).thenReturn(null);

        KafkaPullResultDto result = service.pullExistingEvents("non-existent-topic");

        assertThat(result.status()).isEqualTo("WARNING");
        assertThat(result.partitionCount()).isEqualTo(0);
        assertThat(result.newlySavedCount()).isEqualTo(0);
        verify(consumer).close(any(java.time.Duration.class));
    }

    @Test
    @DisplayName("Should poll existing events and save unlogged records to repository")
    void shouldPollExistingEventsAndSaveUnloggedRecords() {
        String topic = "demo-ping-topic";
        TopicPartition tp0 = new TopicPartition(topic, 0);

        when(consumerFactory.createConsumer(anyString(), eq("topic-puller"))).thenReturn(consumer);
        when(consumer.partitionsFor(topic)).thenReturn(List.of(
                new PartitionInfo(topic, 0, null, null, null)
        ));
        when(consumer.beginningOffsets(List.of(tp0))).thenReturn(Map.of(tp0, 0L));
        when(consumer.endOffsets(List.of(tp0))).thenReturn(Map.of(tp0, 1L));

        PingEvent event = PingEvent.newBuilder()
                .setId("EVT-100")
                .setMessage("Existing event on topic")
                .setSourceService("demo-assets-service")
                .setTimestamp(Instant.now())
                .build();

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                topic, 0, 0L, 1789000000000L, TimestampType.CREATE_TIME,
                0, 0, "key-1", event, new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty()
        );

        ConsumerRecords<Object, Object> records = new ConsumerRecords<>(Map.of(tp0, List.of(record)));

        when(consumer.poll(any())).thenReturn(records);
        when(consumer.position(tp0)).thenReturn(1L);

        when(kafkaEventLogsRepository.findFirstByTopicAndPartitionIdAndRecordOffset(topic, 0, 0L))
                .thenReturn(Optional.empty());

        when(kafkaEventLogsRepository.save(any(KafkaEventLogs.class))).thenAnswer(invocation -> {
            KafkaEventLogs log = invocation.getArgument(0);
            log.setId(1L);
            return log;
        });

        KafkaPullResultDto result = service.pullExistingEvents(topic);

        assertThat(result.status()).isEqualTo("SUCCESS");
        assertThat(result.partitionCount()).isEqualTo(1);
        assertThat(result.totalMessagesOnBroker()).isEqualTo(1L);
        assertThat(result.newlySavedCount()).isEqualTo(1);
        assertThat(result.alreadyPresentCount()).isEqualTo(0);
        assertThat(result.newlySavedEventsSummary()).hasSize(1);
        assertThat(result.newlySavedEventsSummary().get(0).get("eventId")).isEqualTo("EVT-100");

        verify(kafkaEventLogsRepository).save(any(KafkaEventLogs.class));
        verify(consumer).close(any(java.time.Duration.class));
    }

    @Test
    @DisplayName("Should skip records that were already saved into kafka_event_logs")
    void shouldSkipAlreadySavedRecords() {
        String topic = "demo-ping-topic";
        TopicPartition tp0 = new TopicPartition(topic, 0);

        when(consumerFactory.createConsumer(anyString(), eq("topic-puller"))).thenReturn(consumer);
        when(consumer.partitionsFor(topic)).thenReturn(List.of(
                new PartitionInfo(topic, 0, null, null, null)
        ));
        when(consumer.beginningOffsets(List.of(tp0))).thenReturn(Map.of(tp0, 0L));
        when(consumer.endOffsets(List.of(tp0))).thenReturn(Map.of(tp0, 1L));

        PingEvent event = PingEvent.newBuilder()
                .setId("EVT-100")
                .setMessage("Already consumed event")
                .setSourceService("demo-assets-service")
                .setTimestamp(Instant.now())
                .build();

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                topic, 0, 0L, 1789000000000L, TimestampType.CREATE_TIME,
                0, 0, "key-1", event, new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty()
        );

        ConsumerRecords<Object, Object> records = new ConsumerRecords<>(Map.of(tp0, List.of(record)));

        when(consumer.poll(any())).thenReturn(records);
        when(consumer.position(tp0)).thenReturn(1L);

        when(kafkaEventLogsRepository.findFirstByTopicAndPartitionIdAndRecordOffset(topic, 0, 0L))
                .thenReturn(Optional.of(new KafkaEventLogs()));

        KafkaPullResultDto result = service.pullExistingEvents(topic);

        assertThat(result.status()).isEqualTo("SUCCESS");
        assertThat(result.newlySavedCount()).isEqualTo(0);
        assertThat(result.alreadyPresentCount()).isEqualTo(1);
        verify(consumer).close(any(java.time.Duration.class));
    }
}
