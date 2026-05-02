package com.example.consumer.config;

import java.io.InputStream;
import java.util.Properties;

public class KafkaConfigLoader {

    public static Properties load() {
        Properties props = new Properties();

        try (InputStream input = KafkaConfigLoader.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (input == null) {
                throw new RuntimeException("application.properties not found");
            }

            props.load(input);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load properties", e);
        }

        return props;
    }
}