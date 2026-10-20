package com.demo.planogramservice.events.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import com.demo.planogramservice.events.config.KafkaTopicConfig;
import com.demo.shared.events.dto.Transaction;
import lombok.extern.slf4j.Slf4j;

@EnableKafkaStreams
@Configuration
@Slf4j
public class FraudDetectionStream {

    @Value("${kafka.fraud.amount-threshold:1000.0}")
    private double amountThreshold;

    @Autowired
    public void configureStreamsBuilderFactoryBean(StreamsBuilderFactoryBean factoryBean) {
        factoryBean.setStreamsUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception in Kafka Streams thread: {}", exception.getMessage(), exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }

    @Bean
    public KStream<String, Transaction> fraudDetectionTopology(StreamsBuilder streamsBuilder) {

        KStream<String, Transaction> stream = streamsBuilder.stream(KafkaTopicConfig.TRANSACTION_TOPIC);

        KStream<String, Transaction> fraudStream = stream
                .filter((key, transaction) -> transaction != null && transaction.getAmount() > amountThreshold)
                .peek((key, transaction) -> log.warn("Suspicious transaction detected: key={}, amount={}", key,
                        transaction.getAmount()));
        fraudStream.to(KafkaTopicConfig.FRAUD_DETECTION_TOPIC);

        return fraudStream;
    }
}
