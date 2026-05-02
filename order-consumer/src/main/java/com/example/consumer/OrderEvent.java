package com.example.consumer;

import java.math.BigDecimal;

public record OrderEvent(String orderId, BigDecimal amount) {

}
