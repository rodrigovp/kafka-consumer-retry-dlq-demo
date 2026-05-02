package com.example.consumer;

import com.example.consumer.config.AppConfig;
import com.example.consumer.dlq.DlqProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

public class OrderConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;
    private final DlqProducer dlqProducer;
    private final AppConfig config;

    public OrderConsumer(KafkaConsumer<String, String> consumer, ObjectMapper objectMapper, DlqProducer dlqProducer, AppConfig config) {
        this.consumer = consumer;
        this.objectMapper = objectMapper;
        this.dlqProducer = dlqProducer;
        this.config = config;
    }

    public void start() {
        consumer.subscribe(List.of("orders"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                boolean processed = false;
                int attempts = 0;

                // Simple retry strategy (in-memory). Not suitable for production scale.
                while (!processed && attempts < config.maxRetries()) {
                    try {
                        attempts++;

                        OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);

                        process(event);

                        processed = true;

                    } catch (Exception e) {
                        System.err.println("Processing failed. attempt=" + attempts + ", key=" + record.key());
                        e.printStackTrace();

                        if (attempts >= config.maxRetries()) {
                            System.err.println("Max retries reached. Sending to DLQ. key=" + record.key());
                            dlqProducer.send(record.key(), record.value(), e);
                        } else {
                            try {
                                Thread.sleep(1000); // pequeno backoff
                            } catch (InterruptedException ignored) {}
                        }
                    }
                }

                consumer.commitSync();
            }
        }
    }

    private void process(OrderEvent event) {
        if (event.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new RuntimeException("Invalid amount");
        }

        System.out.println("Processed: " + event.orderId());
    }
}