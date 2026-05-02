package com.example.producer.controller;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public record OrderDTO(@JsonProperty BigDecimal value) {
}
