package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

public class App {
    private static final Map<String, User> USERS = new HashMap<>();
    private static final Map<String, Contract> CONTRACTS = new HashMap<>();

    static {
        // Initialize users
        USERS.put("U001", new User("U001", "Alice Johnson", "alice@example.com", "123 Main St"));
        USERS.put("U002", new User("U002", "Bob Smith", "bob@example.com", "456 Elm St"));
        USERS.put("U003", new User("U003", "Charlie Brown", "charlie@example.com", "789 Oak St"));
        USERS.put("U004", new User("U004", "Diana Ross", "diana@example.com", "101 Pine St"));
        USERS.put("U005", new User("U005", "Ethan Hunt", "ethan@example.com", "202 Maple St"));

        // Initialize contracts
        CONTRACTS.put("C001", new Contract("C001", "U001", "2023-06-01", 1000.0));
        CONTRACTS.put("C002", new Contract("C002", "U002", "2023-06-02", 2000.0));
        CONTRACTS.put("C003", new Contract("C003", "U003", "2023-06-03", 3000.0));
        CONTRACTS.put("C004", new Contract("C004", "U004", "2023-06-04", 4000.0));
        CONTRACTS.put("C005", new Contract("C005", "U005", "2023-06-05", 5000.0));
    }

    public static void main(String[] args) {
        Thread userThread = new Thread(new UserProducerRunnable());
        Thread contractThread = new Thread(new ContractProducerRunnable());

        userThread.start();
        contractThread.start();

        try {
            userThread.join();
            contractThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class UserProducerRunnable implements Runnable {
        @Override
        public void run() {
            Properties props = getProducerProps();
            try (Producer<String, User> producer = new KafkaProducer<>(props)) {
                for (Map.Entry<String, User> entry : USERS.entrySet()) {
                    String userId = entry.getKey();
                    User user = entry.getValue();

                    ProducerRecord<String, User> record = new ProducerRecord<>("user-topic", userId, user);

                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println("User record sent successfully. Topic: " + 
                                               metadata.topic() + ", Partition: " + 
                                               metadata.partition() + ", Offset: " + 
                                               metadata.offset());
                        } else {
                            System.err.println("Error sending user record: " + exception.getMessage());
                        }
                    });
                }
            }
        }
    }

    private static class ContractProducerRunnable implements Runnable {
        @Override
        public void run() {
            Properties props = getProducerProps();
            try (Producer<String, Contract> producer = new KafkaProducer<>(props)) {
                for (Map.Entry<String, Contract> entry : CONTRACTS.entrySet()) {
                    String contractId = entry.getKey();
                    Contract contract = entry.getValue();

                    ProducerRecord<String, Contract> record = new ProducerRecord<>("contract-topic", contractId, contract);

                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println("Contract record sent successfully. Topic: " + 
                                               metadata.topic() + ", Partition: " + 
                                               metadata.partition() + ", Offset: " + 
                                               metadata.offset());
                        } else {
                            System.err.println("Error sending contract record: " + exception.getMessage());
                        }
                    });
                }
            }
        }
    }

    private static Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        return props;
    }
}
