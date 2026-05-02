package com.example.producer;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;

import static java.util.UUID.randomUUID;

@Service
public class OrderService {

    private OrderEventPublisher publisher;

    public OrderService(OrderEventPublisher publisher) {
        this.publisher = publisher;
    }

    public void createOrder(BigDecimal value) {
        String orderId = randomUUID().toString();
        OrderEvent event = new OrderEvent(orderId, value);

        publisher.publish(event);
    }
}
