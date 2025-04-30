# 📥 GoMQTT Installation Guide

This document contains detailed installation and configuration instructions for GoMQTT.

## Prerequisites

| Component    | Minimum Requirement                   |
| :----------- | :------------------------------------ |
| Go           | 1.24+                                 |
| PostgreSQL   | 16+ (optional if using Redis)         |
| Redis        | 7+ (optional if using PostgreSQL)     |
| Linux Server | Ubuntu 22.04 recommended              |
| MQTT Clients | Any clients supporting MQTT v3.1.1/v5 |

## Basic Installation

```bash
# Clone the repository
git clone https://github.com/mstgnz/gomqtt.git
cd gomqtt

# Download dependencies
go mod download

# Build
go build -o gomqtt ./cmd

# Run
./gomqtt
```

## Configuration

GoMQTT uses a JSON configuration file to customize its behavior:

```bash
# Create a default configuration file
mkdir -p config
cp config/default.example.json config/default.json

# Edit the configuration file according to your needs
nano config/default.json
```

### Configuration Options

Here's an example of a configuration file with common settings:

```json
{
  "mqtt": {
    "host": "0.0.0.0",
    "port": 1883,
    "max_connections": 1000,
    "max_message_size": 16384,
    "allow_anonymous": false,
    "tls": {
      "enabled": true,
      "port": 8883,
      "cert_file": "certs/server.crt",
      "key_file": "certs/server.key"
    },
    "websocket": {
      "enabled": true,
      "host": "0.0.0.0",
      "port": 9001,
      "path": "/mqtt",
      "tls": {
        "enabled": true,
        "port": 9443,
        "cert_file": "certs/server.crt",
        "key_file": "certs/server.key"
      }
    }
  },
  "api": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8080
  },
  "auth": {
    "jwt_secret": "change-me-in-production",
    "jwt_expires": 24
  },
  "database": {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "db_name": "gomqtt",
    "ssl_mode": "disable"
  },
  "redis": {
    "enabled": false,
    "host": "localhost",
    "port": 6379,
    "password": "",
    "db": 0,
    "key_prefix": "gomqtt:"
  },
  "storage": {
    "enabled": true,
    "type": "postgres",
    "message_retention": 24,
    "cleanup_interval": 1,
    "batch_size": 100
  },
  "plugins": {
    "enabled": true,
    "directory": "./plugins",
    "autoload": []
  },
  "logging": {
    "level": "info",
    "format": "text",
    "file": ""
  }
}
```

### Storage Configuration

GoMQTT supports two storage backends:

1. **PostgreSQL**: Default storage option, best for high-volume deployments and complex queries.
2. **Redis**: Lightweight in-memory storage with persistence, ideal for edge devices or simpler deployments.

To use Redis as the storage backend:

```json
{
  "redis": {
    "enabled": true,
    "host": "localhost",
    "port": 6379,
    "password": "",
    "db": 0,
    "key_prefix": "gomqtt:"
  },
  "storage": {
    "enabled": true,
    "type": "redis",
    "message_retention": 24
  }
}
```

## Setting Up TLS/MQTTS

For secure connections, you'll need SSL certificates. In a development environment, you can create self-signed certificates:

```bash
# Create certificates directory
mkdir -p certs

# Generate CA key and certificate
openssl genrsa -out certs/ca.key 2048
openssl req -new -x509 -days 365 -key certs/ca.key -out certs/ca.crt -subj "/CN=GoMQTT CA"

# Generate server key
openssl genrsa -out certs/server.key 2048
openssl req -new -key certs/server.key -out certs/server.csr -subj "/CN=localhost"

# Create and sign the server certificate
openssl x509 -req -days 365 -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/server.crt

# Generate client key and certificate (for mTLS)
openssl genrsa -out certs/client.key 2048
openssl req -new -key certs/client.key -out certs/client.csr -subj "/CN=mqttclient"
openssl x509 -req -days 365 -in certs/client.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/client.crt
```

## Docker Installation

GoMQTT can also be deployed using Docker:

```bash
# Build the Docker image
docker build -t gomqtt .

# Run with Docker
docker run -d \
  --name gomqtt \
  -p 1883:1883 \
  -p 8883:8883 \
  -p 9001:9001 \
  -p 9443:9443 \
  -p 8080:8080 \
  -p 8081:8081 \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/certs:/app/certs \
  gomqtt
```

## Docker Compose Installation

For a complete setup including PostgreSQL and Redis:

```yaml
# docker-compose.yml
version: "3"

services:
  gomqtt:
    build: .
    ports:
      - "1883:1883" # MQTT
      - "8883:8883" # MQTTS
      - "9001:9001" # WebSocket
      - "9443:9443" # Secure WebSocket
      - "8080:8080" # REST API
      - "8081:8081" # Admin Panel
    volumes:
      - ./config:/app/config
      - ./certs:/app/certs
    depends_on:
      - postgres
      - redis
    environment:
      - DB_HOST=postgres
      - DB_USER=postgres
      - DB_PASSWORD=postgres
      - DB_NAME=gomqtt
      - REDIS_HOST=redis
      - REDIS_PORT=6379

  postgres:
    image: postgres:16
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=gomqtt
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes

volumes:
  postgres_data:
  redis_data:
```

To start the services:

```bash
docker-compose up -d
```

## Verifying Installation

You can verify your installation by:

1. Checking that the services are running:

   ```bash
   ps aux | grep gomqtt
   ```

2. Testing a connection using an MQTT client like mosquitto_pub:

   ```bash
   mosquitto_pub -h localhost -p 1883 -t test/topic -m "Hello GoMQTT"
   ```

3. Checking the logs:
   ```bash
   tail -f /var/log/gomqtt.log   # If logging to file is enabled
   ```

## Troubleshooting

Common issues and solutions:

- **Connection refused**: Check that the server is running and listening on the specified port
- **Authentication failed**: Verify your credentials and JWT configuration
- **TLS connection issues**: Ensure certificates are properly configured
- **Database connection error**: Verify PostgreSQL is running and the connection details are correct
- **Redis connection error**: Verify Redis is running and the connection details are correct

For more examples and detailed client connection instructions, see [examples.md](examples.md).
