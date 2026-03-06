package com.creedpetitt.workersdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Worker {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, Handler> handlers = new HashMap<>();
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private String bootstrapServers;
    private String groupId;

    public void register(String action, Handler handler) {
        handlers.put(action, handler);
    }

    public void start() {
        start("kafka:9092", "workflow-workers-java");
    }

    public void start(String bootstrapServers, String groupId) {

        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;

        connectWithRetry();

        consumer.subscribe(Collections.singleton("workflow-jobs"));

        System.out.println("Worker started, listening for jobs in group: " + groupId);

        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                JobMessage msg;
                try {
                    msg = deserializeJob(record.value());
                } catch (Exception e) {
                    System.err.println("Failed to deserialize job, skipping. Error: " + e.getMessage());
                    continue; // Poison pill protection
                }

                Handler handler = handlers.get(msg.action());
                if (handler == null) {
                    // System.out.println("No handler for " + msg.action() + " in this worker.");
                    continue; // Correctly ignore messages meant for other worker types
                }

                System.out.println("Processing job: " + msg.action());

                String output;
                String status = "SUCCESS";
                try {
                    output = handler.handle(msg.payload());
                } catch (Exception e) {
                    status = "FAILED";
                    // Properly serialize the error using Jackson
                    Map<String, String> errorMap = new HashMap<>();
                    errorMap.put("error", e.getMessage());
                    try {
                        output = MAPPER.writeValueAsString(errorMap);
                    } catch (Exception jsonEx) {
                        output = "{\"error\":\"Serialization failed\"}";
                    }
                }

                ResultMessage res =
                        new ResultMessage(msg.workflowRunId(), msg.action(), output, status);

                try {
                    producer.send(
                            new ProducerRecord<>("workflow-results", serializeResult(res))
                    );
                } catch (Exception e) {
                    System.err.println("Failed to serialize or send result: " + e.getMessage());
                }
            }
        }
    }

    private void connectWithRetry() {
        int maxAttempts = 30;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                this.consumer = new KafkaConsumer<>(getConsumerProps());
                this.producer = new KafkaProducer<>(getProducerProps());
                // Test the connection
                this.consumer.partitionsFor("workflow-jobs"); 
                return;
            } catch (Exception e) {
                System.out.println("Kafka not ready, retrying (" + attempt + "/" + maxAttempts + ")...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (attempt == maxAttempts) {
                    throw new RuntimeException("Could not connect to Kafka after " + maxAttempts + " attempts", e);
                }
            }
        }
    }

    // Serialize/deserialize helper methods
    private String serializeResult(ResultMessage result) throws Exception {
        return MAPPER.writeValueAsString(result);
    }

    private JobMessage deserializeJob(String json) throws Exception {
        return MAPPER.readValue(json, JobMessage.class);
    }

    // Kafka consumer/producer properties
    private Properties getConsumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        consumerProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("group.id", groupId);
        consumerProps.put("auto.offset.reset", "earliest");

        return consumerProps;
    }

    private Properties getProducerProps() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return producerProps;
    }
}