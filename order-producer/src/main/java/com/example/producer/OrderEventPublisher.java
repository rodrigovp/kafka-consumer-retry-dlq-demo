package com.example.producer;

public interface OrderEventPublisher {

    void publish(OrderEvent event);
}
