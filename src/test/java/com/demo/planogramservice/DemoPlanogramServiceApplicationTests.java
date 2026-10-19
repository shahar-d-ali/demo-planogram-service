package com.demo.planogramservice;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.demo.shared.events.dto.Transaction;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@SpringBootTest
class DemoPlanogramServiceApplicationTests {

    @Test
    void testTransactionAvroSerdeRoundTrip() {
        SpecificAvroSerde<Transaction> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"), false);

        Transaction original = new Transaction(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                1500.0,
                "USD",
                "DEBIT",
                "COMPLETED",
                Instant.now()
        );

        byte[] bytes = serde.serializer().serialize("transaction-events", original);
        Assertions.assertNotNull(bytes);

        Transaction deserialized = serde.deserializer().deserialize("transaction-events", bytes);
        Assertions.assertNotNull(deserialized);
        Assertions.assertEquals(1500.0, deserialized.getAmount());
    }

}
