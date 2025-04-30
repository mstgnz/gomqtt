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

### MQTT Protocol Support

- MQTT v3.1.1 compatible
- MQTT v5.0 compatible with full feature support
- QoS 0, 1, and 2 support
- Persistent sessions
- Retained messages
- Clean/Dirty session control
- Will messages

### MQTT v5.0 Features

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

### Security

- TLS/SSL support (MQTTS)
- Secure WebSocket (WSS)
- JWT-based authentication
- OAuth2 authentication
- Client certificate verification (mTLS)
- Topic-based permission control
- RBAC (Role-Based Access Control)
- Advanced DoS protection system
- Temporary IP/client banning
- Connection flood detection
- Progressive penalties for repeat offenders

### Transport

- TCP Server (1883)
- TLS Server (8883)
- WebSocket (9001)
- Secure WebSocket (9443)

### Database & Storage

- PostgreSQL message persistence
- Redis support
- Scalable batch operations
- Message history API
- Automatic message cleanup
- Message expiration feature

### Admin & Monitoring

- View connected devices
- Monitor live message flow
- System resource usage
- Message statistics
- Prometheus metrics

### Clustering

- Multi-node deployment
- High availability
- Load balancing
- Automatic node discovery
- State synchronization
- Shared subscriptions

### Plugin System

- 🧩 **Flexible Plugin Architecture**: Extend the broker with custom plugins
- 🔧 **Event-Based System**: Plugins can hook into various broker events
- 📦 **Built-in Plugins**: Webhook, Rate Limiter, HTTP Authentication, Message Transformation
- 🔌 **External Plugins**: Load Go plugins at runtime
- 🛠️ **Developer-Friendly API**: Easy to create new plugins

### Protocol Bridges

- 🌉 **Multiple Protocol Support**: Bridge MQTT to other protocols
- 🔄 **HTTP Bridge**: Connect MQTT to RESTful services
- 📡 **CoAP Bridge**: Integrate with Constrained Application Protocol
- 📊 **MQTT-MQTT Bridge**: Connect different MQTT brokers
- 🔁 **AMQP/Kafka Support**: Bridge to enterprise messaging systems
- 🔌 **gRPC Integration**: Connect to modern microservices

### Visualization & Monitoring

- 📊 **Rich Dashboard**: Real-time metrics with interactive charts
- 🔍 **Message Flow Visualization**: See message paths between clients
- 🌐 **Geographical Connection Map**: View client locations worldwide
- 🌡️ **Topic Heatmap**: Identify hot topics with high activity
- 📈 **Time-Series Analytics**: Track historical performance metrics
- 🌲 **Topic Hierarchy Visualization**: Navigate topic structures visually

### Message Transformation

- 🔄 **Content Format Conversion**: Transform between JSON, XML, and other formats
- 🎯 **Content-based Filtering**: Filter messages based on payload content
- 🔍 **Pattern Matching**: Apply regular expressions to message payloads
- 🔌 **Data Enrichment**: Add metadata or additional information to messages
- ⚙️ **Template-based Transformation**: Use templates to reshape message structure
- 📐 **Schema Validation**: Ensure messages conform to defined schemas

## 📚 Documentation

For more detailed information, check out the following documentation:

- [Installation Guide](installation.md) - Setup instructions for different environments
- [Usage Examples](examples.md) - Code samples for various languages and platforms
- [Clustering Setup](cluster/cluster-setup.md) - Configure high-availability deployments
- [API Documentation](cmd/api/api-docs.md) - REST API for monitoring and management
- [Configuration Guide](configuration.md) - Detailed configuration options
- [OAuth2 Authentication](auth/oauth2-authentication.md) - Set up OAuth2 with various providers
- [Plugin System](plugins/plugins.md) - Extend GoMQTT with custom plugins

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
