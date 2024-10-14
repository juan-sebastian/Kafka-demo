package com.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class App {
    public static void main(String[] args) {
        Thread userThread = new Thread(new ConsumerRunnable("user-topic", User.class));
        Thread contractThread = new Thread(new ConsumerRunnable("contract-topic", Contract.class));
        Thread userContractDetailsThread = new Thread(new ConsumerRunnable("user-contract-details-topic", UserContractDetails.class));

        userThread.start();
        contractThread.start();
        userContractDetailsThread.start();

        try {
            userThread.join();
            contractThread.join();
            userContractDetailsThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class ConsumerRunnable implements Runnable {
        private final String topic;
        private final Class<?> valueClass;

        public ConsumerRunnable(String topic, Class<?> valueClass) {
            this.topic = topic;
            this.valueClass = valueClass;
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-" + topic);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
            props.put("schema.registry.url", "http://localhost:8081");

            try (Consumer<String, Object> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Arrays.asList(topic));

                while (true) {
                    ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, Object> record : records) {
                        System.out.printf("Consumed from %s: key = %s, value = %s%n", topic, record.key(), record.value());
                        System.out.flush();
                    }
                }
            }
        }
    }
}
