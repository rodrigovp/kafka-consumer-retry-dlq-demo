package com.example.consumer;

import com.example.consumer.config.AppConfig;
import com.example.consumer.config.DlqProducerFactory;
import com.example.consumer.config.KafkaConsumerFactory;
import com.example.consumer.dlq.DlqProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import tools.jackson.databind.ObjectMapper;

import static com.example.consumer.config.KafkaConfigLoader.load;

public class Application {

    public static void main(String[] args) {
        AppConfig config = new AppConfig(load());
        ObjectMapper mapper = new ObjectMapper();
        try(KafkaConsumer<String, String> consumer = KafkaConsumerFactory.create(config);
            DlqProducer dlqProducer = DlqProducerFactory.create(config)) {
            new OrderConsumer(consumer, mapper, dlqProducer, config).start();
        }
    }
}
