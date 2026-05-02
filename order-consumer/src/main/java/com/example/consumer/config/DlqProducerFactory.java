package com.example.consumer.config;

import com.example.consumer.dlq.DlqProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DlqProducerFactory {

    public static DlqProducer create(AppConfig config) {
        Properties props = buildProducerProperties(config.bootstrapServers(), config.producerAcks());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return new DlqProducer(producer, config.dlqTopic());
    }

    private static Properties buildProducerProperties(String bootstrapServers, String acks) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);

        return props;
    }
}
