package com.demo.planogramservice.events.streams;

import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import com.demo.planogramservice.events.config.KafkaTopicConfig;
import com.demo.shared.events.dto.Transaction;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;

@EnableKafkaStreams
@Configuration
@Slf4j
public class FraudDetectionStream {

    @Bean
    public KStream<String, Transaction> fraudDetectionTopology(
            StreamsBuilder streamsBuilder,
            @Value("${spring.kafka.streams.properties.schema.registry.url:http://localhost:8081}") String schemaRegistryUrl,
            @Value("${kafka.fraud.amount-threshold:1000.0}") double amountThreshold) {

        SpecificAvroSerde<Transaction> transactionAvroSerde = new SpecificAvroSerde<>();
        transactionAvroSerde.configure(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                false // false indicates value SerDe
        );

        KStream<String, Transaction> stream = streamsBuilder.stream(
                KafkaTopicConfig.TRANSACTION_TOPIC,
                Consumed.with(Serdes.String(), transactionAvroSerde)
        );

        stream.filter((key, transaction) -> transaction != null && transaction.getAmount() > amountThreshold)
                .peek((key, transaction) -> log.warn(
                        "Suspicious transaction detected in planogram service: key={}, amount={}", key, transaction.getAmount()))
                .to(KafkaTopicConfig.FRAUD_DETECTION_TOPIC, Produced.with(Serdes.String(), transactionAvroSerde));

        return stream;
    }
}
