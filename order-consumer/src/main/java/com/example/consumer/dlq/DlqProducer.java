package com.example.consumer.dlq;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DlqProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String dlqTopic;

    public DlqProducer(KafkaProducer<String, String> producer, String dlqTopic) {
        this.producer = producer;
        this.dlqTopic = dlqTopic;
    }

    public void send(String key, String payload, Exception exception) {

        try {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(dlqTopic, key, payload);

            // 💡 podemos enriquecer depois com headers

            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    System.err.println("Error sending to DLQ for key=" + key);
                } else {
                    System.out.println("Message sent to DLQ topic " + metadata.topic() +
                            " partition " + metadata.partition() +
                            " offset " + metadata.offset());
                }
            });

        } catch (Exception e) {
            System.err.println("Unexpected error while sending to DLQ for key=" + key);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}