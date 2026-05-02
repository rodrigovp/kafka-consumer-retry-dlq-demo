# Kafka Retry and DLQ Demo

This project demonstrates how to produce and consume events in Apache Kafka by manually implementing:

- Retry
- Dead Letter Queue (DLQ)

The goal is to demonstrate how these patterns work **without using frameworks**, relying only on the native Kafka client.

🇧🇷 [Read this in Portuguese](README.pt-BR.md)

---

## What problem does this solve?

In distributed systems, failures are notably inevitable:

- Messages may fail during processing
- External dependencies may be unavailable
- Data may be invalid

Without proper handling, this can lead to:

- Message loss
- Infinite reprocessing
- Inconsistent systems

---

## How does it work?

The solution implements two main strategies:

### 🔁 Retry

When an error occurs during processing:

1. The consumer tries to process again
2. Waits for a short interval (backoff)
3. Repeats until reaching the configured limit

---

### ☠️ DLQ (Dead Letter Queue)

If all attempts fail:

1. The message is sent to an error topic (`orders.dlq`)
2. Processing continues without blocking the system

---

## Architecture (simplified)

Producer (Spring Boot) -> Kafka (orders) -> Consumer (retry + DLQ) -> Kafka (orders.dlq)

---

## Important decision

This project **intentionally mixes approaches**:

- Producer with Spring Boot (ease of HTTP integration)
- Consumer with pure Kafka (`kafka-clients`)

This was done to:

- Show how real applications produce events
- Expose how Kafka works internally (without abstractions)

---

## How to run

Start the infrastructure:

```bash
docker-compose up -d
```

Run the producer:

```bash
mvn -f order-producer/pom.xml spring-boot:start
```

Run the consumer:

```
mvn -f order-consumer/pom.xml compile exec:java -Dexec.mainClass="com.example.consumer.Main"
```

## How to test
### ✅ Send a valid request

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"value": "10"}'
```

Expected result:

* Message processed successfully
* Does not go to DLQ

### How to check the topic

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic orders \
  --from-beginning
```

### ❌ Send an invalid request (triggers error)

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"value": "-10"}'
```

Expected result:

* Consumer retries processing multiple times
* After reaching the limit → sends to DLQ


### How to check the DLQ

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic orders.dlq \
  --from-beginning
```

Important notes
* Retry is implemented in memory (not persistent)
* A small backoff is applied between attempts
* The DLQ preserves failed messages for later analysis


## Learn more

If you want to go deeper into Kafka, event modeling and best practices:

- 📘 Kafka Connect e Kafka Streams — integre suas aplicações ao Kafka usando as melhores práticas  
  (Portuguese edition — available on [Amazon](https://www.amazon.com.br/Kafka-Connect-Streams-aplicações-melhores-ebook/dp/B0D4R7N7SR) and [Casa do Código](https://www.casadocodigo.com.br/products/livro-kafka-connect-e-kafka-streams))

- 📘 Kafka e Schema Registry — modelagem de eventos com robustez e segurança  
  (Portuguese edition — available on [Amazon](https://www.amazon.com.br/Kafka-Schema-Registry-Modelagem-segurança-ebook/dp/B0FKDDMFPG) and [Casa do Código](https://www.casadocodigo.com.br/products/livro-kafka-schema-registry))
