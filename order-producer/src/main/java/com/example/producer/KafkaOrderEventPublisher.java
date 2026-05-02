package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

@Repository
public class KafkaOrderEventPublisher implements OrderEventPublisher {

    private ObjectMapper objectMapper;
    private KafkaProducer<String, String> producer;

    public KafkaOrderEventPublisher(ObjectMapper objectMapper, KafkaProducer<String, String> producer) {
        this.objectMapper = objectMapper;
        this.producer = producer;
    }

    @Override
    public void publish(OrderEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("orders", event.orderId(), payload);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending event for orderId=" + event.orderId());
                    exception.printStackTrace();
                } else {
                    System.out.println("Sent to partition " + metadata.partition());
                }
            });

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
