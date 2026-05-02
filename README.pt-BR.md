# Kafka Retry e DLQ Demo

Este projeto mostra como produzir e consumir eventos no Apache Kafka implementando manualmente:

- Retry (retentativas)
- Dead Letter Queue (DLQ)

O objetivo é demonstrar como esses padrões funcionam **sem uso de frameworks**, utilizando apenas o client nativo do Kafka.

---

## Qual problema isso resolve?

Em sistemas distribuídos, as falhas são notavelmente inevitáveis:

- Mensagens podem falhar durante o processamento
- Dependências externas podem estar indisponíveis
- Dados podem estar inválidos

Sem tratamento adequado, isso pode causar:

- Perda de mensagens
- Reprocessamento infinito
- Sistemas inconsistentes

---

## Como funciona?

A solução implementa duas estratégias principais:

### 🔁 Retry (retentativas)

Quando ocorre erro no processamento:

1. O consumer tenta processar novamente
2. Aguarda um pequeno intervalo (backoff)
3. Repete até atingir o limite configurado

---

### ☠️ DLQ (Dead Letter Queue)

Se todas as tentativas falharem:

1. A mensagem é enviada para um tópico de erro (`orders.dlq`)
2. O processamento segue sem travar o sistema

---

## Arquitetura (simplificada)

Producer (Spring Boot) -> Kafka (orders) -> Consumer (retry + DLQ) -> Kafka (orders.dlq)

---

## Decisão importante

Este projeto **intencionalmente mistura abordagens**:

- Producer com Spring Boot (facilidade de integração HTTP)
- Consumer com Kafka puro (`kafka-clients`)

Isso foi feito para:

- Mostrar como aplicações reais produzem eventos
- Expor como o Kafka funciona internamente (sem abstrações)

---

## Como rodar

Subir a infraestrutura:

```bash
docker-compose up -d
```

Rodar o producer:

```bash
mvn -f order-producer/pom.xml spring-boot:start
```

Rodar o consumer:

```
mvn -f order-consumer/pom.xml compile exec:java -Dexec.mainClass="com.example.consumer.Main"
```

## Como testar
### ✅ Enviar pedido válido

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"value": "10"}'
```

Resultado esperado:

* Mensagem processada com sucesso
* Não vai para DLQ

### Como verificar o tópico
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic orders \
  --from-beginning
```

### ❌ Enviar pedido inválido (gera erro)

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"value": "-10"}'
```

Resultado esperado:

* Consumer tenta processar várias vezes
* Após atingir o limite → envia para DLQ

### Como verificar a DLQ

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic orders.dlq \
  --from-beginning
```

### Observações importantes
* O retry é implementado em memória (não persistente)
* Um pequeno backoff é aplicado entre tentativas
* A DLQ preserva mensagens com erro para análise posterior

### Aprofundamento

Se quiser aprender mais sobre Kafka e boas práticas:

- 📘 Kafka Connect e Kafka Streams - integre suas aplicações ao Kafka usando as melhores práticas
  ([Amazon](https://www.amazon.com.br/Kafka-Connect-Streams-aplicações-melhores-ebook/dp/B0D4R7N7SR) | [Casa do Código](https://www.casadocodigo.com.br/products/livro-kafka-connect-e-kafka-streams))

- 📘 Kafka e Schema Registry - modelagem de eventos com robustez e segurança
  ([Amazon](https://www.amazon.com.br/Kafka-Schema-Registry-Modelagem-segurança-ebook/dp/B0FKDDMFPG) | [Casa do Código](https://www.casadocodigo.com.br/products/livro-kafka-schema-registry))
