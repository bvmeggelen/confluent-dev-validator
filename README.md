> This is a proof of concept, quickly put together. Quality does not exist here.

# Quick Start

```shell
docker compose up -d

# open http://localhost:9021/
# create topic "debug"

docker run confluent-js sh -c "node produce.js 1"

# browse to Topics > debug > messages and see your message
```

# Confluent Kafka Development Environment

A complete development environment for working with Apache Kafka, Schema Registry, and Avro schemas using JavaScript/Node.js.

## 🚀 Features

- **Full Confluent Platform**: Kafka, Schema Registry, Connect, ksqlDB, Control Center
- **Avro Schema Management**: Load schemas from files and register them automatically
- **Message Validation**: Validate messages against Avro schemas before sending
- **Sample-based Development**: Use predefined schema/data samples for quick testing
- **Docker Compose Setup**: One-command environment setup
- **JavaScript Producer**: Feature-rich Kafka producer with schema validation

## 📋 Prerequisites

- Docker and Docker Compose
- Node.js 18+ (for local development)
- Git
