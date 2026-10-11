package com.demo.planogramservice.events.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import com.demo.planogramservice.events.config.KafkaTopicConfig;
import com.demo.shared.events.dto.Transaction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@EnableKafkaStreams
@Configuration
@Slf4j
public class FraudDetectionStream {

    private final ObjectMapper objectMapper;

    public FraudDetectionStream(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Bean
    public KStream<String, String> fraudDetectionTopology(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream(KafkaTopicConfig.TRANSACTION_TOPIC);

        stream.filter((key, value) -> isSuspicious(value))
                .peek((key, value) -> log.warn("Suspicious transaction detected in planogram service: key={}, payload={}", key, value))
                .to(KafkaTopicConfig.FRAUD_DETECTION_TOPIC);

        return stream;
    }

    private boolean isSuspicious(String value) {
        try {
            JsonNode root = objectMapper.readTree(value);
            if (root != null && root.has("amount") && !root.get("amount").isNull()) {
                return root.get("amount").asDouble() > 1000.0;
            }
            Transaction transaction = objectMapper.readValue(value, Transaction.class);
            return transaction.getAmount() > 1000.0;
        } catch (Exception e) {
            log.error("Error parsing transaction event in fraud detection stream: {}", value, e);
            return false;
        }
    }
}
