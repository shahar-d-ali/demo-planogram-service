package com.demo.planogramservice.events.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    // Consumer factory is auto-configured by Spring Boot from
    // spring.kafka.consumer.* properties (KafkaAvroDeserializer + schema registry).
}
