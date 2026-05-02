package com.example.consumer.config;

import java.util.Properties;

public record AppConfig(
    String bootstrapServers,
    String topic,
    String groupId,
    String dlqTopic,
    int maxRetries,
    long retryDelayMs,
    String producerAcks
) {

    public AppConfig(Properties props) {
        this(
            props.getProperty("kafka.bootstrap.servers"),
            props.getProperty("kafka.topic"),
            props.getProperty("kafka.group.id"),
            props.getProperty("kafka.dlq.topic"),
            Integer.parseInt(props.getProperty("kafka.retry.max.attempts", "3")),
            Long.parseLong(props.getProperty("kafka.retry.backoff.ms", "1000")),
            props.getProperty("kafka.producer.acks", "all")
        );
    }
}