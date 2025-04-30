# 🛰️ GoMQTT - Modern, Scalable, Lightweight MQTT Broker

![GoMQTT Logo](https://via.placeholder.com/150x150/0096FF/FFFFFF?text=GoMQTT)

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## 📖 Project Overview

GoMQTT is a **lightweight**, **high-performance**, and **modern** MQTT broker designed for IoT and edge devices. Written in Go and seamlessly integrating with modern databases like PostgreSQL.

### 🌟 Why GoMQTT?

- 💡 **Lightweight & Fast**: Efficiently manages thousands of concurrent connections with Go's concurrency capabilities
- 🔌 **Plugin System**: Extendable with an easily integratable plugin system
- 🔐 **Secure**: Authentication with JWT, API Key, and mTLS support
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
- [ ] Clustering support
- [x] Shared subscriptions
- [ ] Bridge mode
- [ ] Packet filtering
- [ ] Rate limiting
- [ ] More database options (SQLite, MySQL)
- [ ] Redis integration
- [ ] Prometheus metrics
- [ ] Multi-node deployment with Docker Compose
- [ ] OAuth2 integration
- [ ] RBAC (Role-Based Access Control)

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
