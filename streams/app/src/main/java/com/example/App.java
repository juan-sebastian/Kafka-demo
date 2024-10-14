package com.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-contract-joiner");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put("schema.registry.url", "http://localhost:8081");

        final StreamsBuilder builder = new StreamsBuilder();

        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        final SpecificAvroSerde<User> userSerde = new SpecificAvroSerde<>();
        userSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Contract> contractSerde = new SpecificAvroSerde<>();
        contractSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<UserContractDetails> userContractDetailsSerde = new SpecificAvroSerde<>();
        userContractDetailsSerde.configure(serdeConfig, false);

        // Create KTable for Users
        KTable<String, User> users = builder.table("user-topic", Consumed.with(Serdes.String(), userSerde));

        // Create KStream for Contracts and repartition based on user_id
        KStream<String, Contract> contracts = builder.stream("contract-topic", Consumed.with(Serdes.String(), contractSerde))
                .selectKey((key, contract) -> contract.getUserId());

        // Join Contracts stream with Users table
        KStream<String, UserContractDetails> userContractStream = contracts.join(users,
                (contract, user) -> {
                    log.info("Joining contract {} with user {}", contract.getContractId(), user.getId());
                    return UserContractDetails.newBuilder()
                            .setUserId(user.getId())
                            .setName(user.getName())
                            .setEmail(user.getEmail())
                            .setAddress(user.getAddress())
                            .setContractId(contract.getContractId())
                            .setContractDate(contract.getContractDate())
                            .setAmount(contract.getAmount())
                            .build();
                },
                Joined.with(Serdes.String(), contractSerde, userSerde));

        // Write the joined stream to the output topic
        userContractStream.to("user-contract-details-topic", Produced.with(Serdes.String(), userContractDetailsSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            log.info("Streams started");
            latch.await();
        } catch (Throwable e) {
            log.error("Error in Kafka Streams application", e);
            System.exit(1);
        }
        System.exit(0);
    }
}
