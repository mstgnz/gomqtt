/*
Package gomqtt is the documentation root for GoMQTT, a lightweight, high-performance, and modern MQTT broker designed for IoT and edge devices.

GoMQTT is a complete MQTT broker implementation supporting both MQTT v3.1.1 and v5.0 protocols.
It's designed with performance, security, and extensibility in mind, making it suitable for
IoT applications of various scales - from small edge deployments to large distributed systems.

# Features

  - Full MQTT v3.1.1 and v5.0 protocol support
  - QoS 0, 1, and 2 message delivery
  - Retained messages
  - Will messages and delayed will messages
  - Session persistence
  - Shared subscriptions
  - Topic aliases
  - TLS/SSL support (MQTTS)
  - WebSocket support (WS and WSS)
  - Authentication with JWT, OAuth2, mTLS
  - Role-Based Access Control (RBAC)
  - Plugin system for extensibility
  - Message storage with PostgreSQL or MySQL
  - Clustering: gossip membership with subscription/retained state sync (live cross-node message routing is on the roadmap)
  - Rate limiting and connection throttling
  - Prometheus metrics integration

# Core Packages

  - mqtt: The core MQTT protocol implementation and broker
  - auth: Authentication and authorization mechanisms
  - storage: Message persistence and storage interfaces
  - cluster: Multi-node clustering capabilities
  - config: Configuration loading and management
  - plugin: Extensible plugin system
  - metrics: Prometheus metrics for monitoring
  - admin: Admin web interface
  - cmd: Command line tools and server entry points
  - rate: Connection and message rate limiting

# Getting Started

To start a basic MQTT broker with default settings:

	package main

	import "github.com/mstgnz/gomqtt/cmd"

	func main() {
	    cmd.Execute()
	}

For a custom broker with specific configuration:

	package main

	import (
	    "github.com/mstgnz/gomqtt/config"
	    "github.com/mstgnz/gomqtt/mqtt"
	)

	func main() {
	    // Create a new broker
	    broker := mqtt.NewServer("localhost", 1883)

	    // Enable TLS for secure connections
	    broker.EnableTLS(8883, "/path/to/cert.pem", "/path/to/key.pem")

	    // Enable WebSocket support
	    broker.EnableWebSocket("0.0.0.0", 9001, "/mqtt")

	    // Start the broker
	    if err := broker.Start(); err != nil {
	        panic(err)
	    }
	}

# Security

GoMQTT provides multiple security mechanisms:

  - TLS/SSL encryption for MQTT and WebSocket connections
  - Client authentication via username/password, JWT tokens, or mTLS
  - OAuth2 integration for identity provider support
  - Role-based access control for topic permissions
  - Rate limiting to prevent DoS attacks

# Configuration

GoMQTT can be configured via a JSON configuration file or environment variables.
See the config package for detailed configuration options.
*/
package gomqtt
