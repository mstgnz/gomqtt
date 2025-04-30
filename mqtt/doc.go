/*
Package mqtt implements a full-featured MQTT broker supporting MQTT v3.1.1 and v5.0 protocols.

This package provides the core functionality of the GoMQTT broker, including:

  - Complete MQTT protocol implementation (v3.1.1 and v5.0)
  - Client connection handling and session management
  - Message routing and distribution
  - Subscription management including shared subscriptions
  - QoS 0, 1, and 2 delivery guarantees
  - Retained message handling
  - Will message processing
  - Transport layer support (TCP, TLS, WebSocket, WSS)

# Server

The Server type is the central component that manages all broker operations:

	// Create a new broker instance
	broker := mqtt.NewServer("0.0.0.0", 1883)

	// Enable TLS support
	broker.EnableTLS(8883, "server.crt", "server.key")

	// Enable WebSocket support
	broker.EnableWebSocket("0.0.0.0", 9001, "/mqtt")

	// Start the broker
	if err := broker.Start(); err != nil {
	    log.Fatalf("Failed to start MQTT broker: %v", err)
	}

# Packets

The package implements all MQTT packet types for both MQTT v3.1.1 and v5.0:

  - CONNECT/CONNACK: Client connection and authentication
  - PUBLISH: Message publishing
  - SUBSCRIBE/SUBACK: Topic subscription
  - UNSUBSCRIBE/UNSUBACK: Subscription removal
  - PINGREQ/PINGRESP: Connection keep-alive
  - DISCONNECT: Clean session termination
  - AUTH: Enhanced authentication (v5.0 only)

MQTT v5.0 specific features include:

  - User properties
  - Subscription identifiers
  - Topic aliases
  - Reason codes
  - Session and message expiry
  - Shared subscriptions
  - Maximum packet size negotiation

# Client Management

The Client type represents an MQTT client connection:

  - Connection state tracking
  - Subscription management
  - Message queue handling
  - Session persistence
  - Will message processing

# Subscription Management

The Subscription type handles topic matching and message delivery:

  - Topic filter patterns using MQTT wildcards (+ and #)
  - QoS level management
  - Shared subscription support
  - Subscription identifiers (MQTT v5.0)

# Rate Limiting

The package provides rate limiting capabilities for:

  - Connection rate limits
  - Message publishing rate limits
  - Subscription rate limits
  - Maximum message size limits

# Examples

A simple broker with basic settings:

	broker := mqtt.NewServer("0.0.0.0", 1883)
	if err := broker.Start(); err != nil {
	    log.Fatal(err)
	}
	defer broker.Stop()

	// Block until shutdown signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

A broker with TLS and WebSocket support:

	broker := mqtt.NewServer("0.0.0.0", 1883)

	// Enable TLS
	broker.EnableTLS(8883, "server.crt", "server.key")

	// Enable WebSocket
	broker.EnableWebSocket("0.0.0.0", 9001, "/mqtt")

	// Enable secure WebSocket
	broker.EnableSecureWebSocket(9443, "server.crt", "server.key")

	if err := broker.Start(); err != nil {
	    log.Fatal(err)
	}
	defer broker.Stop()

A broker with custom rate limits:

	broker := mqtt.NewServer("0.0.0.0", 1883)

	// Configure rate limits: connections/sec, publish/sec, subscribe/sec, max message size, burst multiplier
	broker.ConfigureRateLimiter(10, 100, 20, 32768, 2)

	if err := broker.Start(); err != nil {
	    log.Fatal(err)
	}
*/
package mqtt
