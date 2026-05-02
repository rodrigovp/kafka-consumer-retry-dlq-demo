package com.example.producer;

import java.math.BigDecimal;

public record OrderEvent(String orderId, BigDecimal amount) {

}
