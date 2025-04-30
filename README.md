# 🛰️ GoMQTT - Modern, Scalable, Lightweight MQTT Broker

[![GoMqtt](assets/logo.svg)](#)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## 📖 Project Overview

GoMQTT is a **lightweight**, **high-performance**, and **modern** MQTT broker designed for IoT and edge devices. Written in Go and seamlessly integrating with modern databases like PostgreSQL.

### 🌟 Why GoMQTT?

- 💡 **Lightweight & Fast**: Efficiently manages thousands of concurrent connections with Go's concurrency capabilities
- 🔌 **Plugin System**: Extendable with an easily integratable plugin system
- 🔐 **Secure**: Authentication with JWT, API Key, OAuth2, and mTLS support
- 🔄 **Multi-Transport**: Supports TCP, WebSocket, TLS (MQTTS), and WSS
- 📊 **Admin Panel**: Fast interface built with Go + HTMX + templ
- 🛢️ **Database Integration**: Message and session persistence with PostgreSQL

## 🚀 Features

✅ **MQTT Protocol Support**

- MQTT v3.1.1 compatible
- MQTT v5.0 compatible with full feature support
- QoS 0, 1, and 2 support
- Persistent sessions
- Retained messages
- Clean/Dirty session control
- Will messages

✅ **MQTT v5.0 Features**

- User properties
- Subscription identifiers
- Topic aliases
- Shared subscriptions
- Session and message expiry intervals
- Enhanced authentication (AUTH packets)
- Reason codes for detailed error reporting
- Server disconnect
- Will delay intervals
- Response topic and correlation data for request/response
- Maximum packet size and QoS control

✅ **Security**

- TLS/SSL support (MQTTS)
- Secure WebSocket (WSS)
- JWT-based authentication
- OAuth2 authentication
- Client certificate verification (mTLS)
- Topic-based permission control

✅ **Transport**

- TCP Server (1883)
- TLS Server (8883)
- WebSocket (9001)
- Secure WebSocket (9443)

✅ **Database & Storage**

- PostgreSQL message persistence
- Scalable batch operations
- Message history API
- Automatic message cleanup
- Message expiration feature

✅ **Admin & Monitoring**

- View connected devices
- Monitor live message flow
- System resource usage
- Message statistics

✅ **Plugin System**

- Event-based plugin architecture
- Webhook integration
- Custom authentication

## 📋 Planned Features

- [x] Full MQTT v5.0 support
- [x] Clustering support
- [x] Shared subscriptions
- [x] Redis integration
- [ ] Bridge mode
- [ ] Packet filtering
- [x] Rate limiting
- [ ] More database options (SQLite, MySQL)
- [x] Prometheus metrics
- [x] Multi-node deployment with Docker Compose
- [x] OAuth2 integration
- [x] RBAC (Role-Based Access Control)

## 🔄 Clustering

GoMQTT supports clustering to provide high availability and scalability. Multiple GoMQTT brokers can be connected together to form a cluster. The following features are supported in cluster mode:

- **Automatic node discovery**: Nodes automatically discover and connect to each other
- **State synchronization**: Subscriptions and retained messages are synchronized across the cluster
- **Message sharing**: Messages published to one node are distributed to subscribers on other nodes
- **Shared subscriptions**: Multiple subscribers can share a subscription across different nodes
- **High availability**: If one node fails, clients can connect to another node in the cluster

### Cluster Configuration

To enable clustering, update your configuration file:

```json
{
  "cluster": {
    "enabled": true,
    "node_id": "node1", // Unique identifier for this node
    "node_host": "localhost", // Host address for cluster communication
    "node_port": 7946, // Port for cluster communication (Memberlist default)
    "gossip_port": 7947, // Port for gossip protocol
    "seed_nodes": [
      // List of existing nodes to join
      "node2:7946",
      "node3:7946"
    ],
    "sync_interval": 30 // Synchronization interval in seconds
  }
}
```

### Multi-Node Deployment

For production environments, GoMQTT provides a ready-to-use multi-node deployment configuration with Docker Compose. This setup includes:

- Multiple GoMQTT nodes forming a cluster
- HAProxy load balancer for traffic distribution
- Shared PostgreSQL database for message persistence
- API Gateway for REST access
- Admin dashboard for monitoring

To deploy a multi-node cluster:

1. Generate TLS certificates:
   ```bash
   ./scripts/generate-certs.sh
   ```
2. Launch the cluster:

   ```bash
   docker-compose -f docker-compose-cluster.yml up -d
   ```

3. Access services:
   - MQTT: `localhost:1883` (Load balanced)
   - Admin UI: `http://localhost:8081`
   - API: `http://localhost:8080`
   - HAProxy Stats: `http://localhost:8404`

For more details, see [CLUSTER-SETUP.md](CLUSTER-SETUP.md).

## 🔒 OAuth2 Authentication

GoMQTT supports OAuth2 authentication for secure client connections. This allows integration with popular identity providers like Google, GitHub, Auth0, and others.

### OAuth2 Configuration

To enable OAuth2 authentication, update your configuration file:

```json
{
  "auth": {
    "jwt_secret": "your-jwt-secret",
    "jwt_expires": 24,
    "oauth2": {
      "enabled": true,
      "client_id": "your-client-id",
      "client_secret": "your-client-secret",
      "auth_url": "https://accounts.google.com/o/oauth2/auth",
      "token_url": "https://oauth2.googleapis.com/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["email", "profile"],
      "user_info_url": "https://www.googleapis.com/oauth2/v3/userinfo",
      "token_field": "password",
      "username_field": "email"
    }
  }
}
```

- `enabled`: Set to `true` to enable OAuth2 authentication
- `client_id` and `client_secret`: Credentials from your OAuth2 provider
- `auth_url`: Authorization URL for the OAuth2 provider
- `token_url`: Token URL for the OAuth2 provider
- `redirect_url`: Callback URL for the OAuth2 flow
- `scopes`: OAuth2 scopes to request
- `user_info_url`: URL to retrieve user information
- `token_field`: Field in MQTT CONNECT packet to use for the token (usually "password")
- `username_field`: Field in user info response to use as username

### Client Configuration

When connecting with an MQTT client, use:

- Username: The username field from your OAuth2 provider
- Password: The OAuth2 token received from your provider

### Supported Providers

GoMQTT has been tested with the following OAuth2 providers:

- Google
- GitHub
- Auth0
- Okta
- Azure AD

## 🏗️ Connection Options

GoMQTT supports various connection methods:

| Transport   | Port | Security | Description                |
| ----------- | ---- | -------- | -------------------------- |
| MQTT/TCP    | 1883 | -        | Standard MQTT              |
| MQTTS/TCP   | 8883 | TLS      | Secure MQTT with TLS       |
| MQTT/WS     | 9001 | -        | MQTT over WebSocket        |
| MQTT/WSS    | 9443 | TLS      | MQTT over Secure WebSocket |
| REST API    | 8080 | JWT      | HTTP REST API              |
| Admin Panel | 8081 | JWT      | Web interface              |

## 📚 Getting Started

For installation and configuration instructions, see [installation.md](installation.md).

For client examples and code samples in various programming languages, see [examples.md](examples.md).

## 🔌 REST API Documentation

GoMQTT includes a comprehensive REST API for monitoring and management. The API allows you to:

- Monitor connected clients and their subscriptions
- Publish messages and access message history
- View topic information and statistics
- Manage broker settings

We use [Scalar](https://scalar.com/) for our API documentation, providing an interactive experience based on OpenAPI specifications.

For more information, see [api-docs.md](api-docs.md).

## 💡 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Installation

### Using Go

```bash
go get github.com/yourusername/gomqtt
```

### Using Docker

```bash
docker pull yourusername/gomqtt
docker run -p 1883:1883 -p 8883:8883 yourusername/gomqtt
```

## Quick Start

```go
package main

import (
    "github.com/yourusername/gomqtt"
)

func main() {
    broker := gomqtt.NewBroker()
    broker.Start()
}
```

## Configuration

GoMQTT can be configured using environment variables or a configuration file:

```bash
GOMQTT_PORT=1883
GOMQTT_REDIS_URL=redis://localhost:6379
GOMQTT_AUTH_ENABLED=true
```
