package mqtt

import (
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	t.Run("Create server", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		if server.Host != "localhost" {
			t.Errorf("Expected host localhost, got %s", server.Host)
		}
		if server.Port != 1883 {
			t.Errorf("Expected port 1883, got %d", server.Port)
		}

		// Check default values
		if server.TLSEnabled {
			t.Error("Expected TLS to be disabled by default")
		}
		if server.TLSPort != 8883 {
			t.Errorf("Expected default TLS port 8883, got %d", server.TLSPort)
		}
		if server.WSEnabled {
			t.Error("Expected WebSocket to be disabled by default")
		}
		if server.WSPort != 9001 {
			t.Errorf("Expected default WebSocket port 9001, got %d", server.WSPort)
		}
		if server.WSPath != "/mqtt" {
			t.Errorf("Expected default WebSocket path '/mqtt', got '%s'", server.WSPath)
		}

		// Check internal maps are initialized
		if server.clients == nil {
			t.Error("Expected clients map to be initialized")
		}
		if server.subscriptions == nil {
			t.Error("Expected subscriptions map to be initialized")
		}
		if server.retainedMessages == nil {
			t.Error("Expected retainedMessages map to be initialized")
		}
		if server.inflightMessages == nil {
			t.Error("Expected inflightMessages map to be initialized")
		}
		if server.pendingQoS2Messages == nil {
			t.Error("Expected pendingQoS2Messages map to be initialized")
		}
	})

	t.Run("Enable TLS", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		server.EnableTLS(8884, "cert.pem", "key.pem")

		if !server.TLSEnabled {
			t.Error("Expected TLS to be enabled")
		}
		if server.TLSPort != 8884 {
			t.Errorf("Expected TLS port 8884, got %d", server.TLSPort)
		}
		if server.TLSCertFile != "cert.pem" {
			t.Errorf("Expected cert file 'cert.pem', got '%s'", server.TLSCertFile)
		}
		if server.TLSKeyFile != "key.pem" {
			t.Errorf("Expected key file 'key.pem', got '%s'", server.TLSKeyFile)
		}
	})

	t.Run("Enable client certificate verification", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		server.EnableClientCertVerification("ca.pem")

		if !server.TLSRequireClientCert {
			t.Error("Expected client cert verification to be enabled")
		}
		if server.TLSCACertFile != "ca.pem" {
			t.Errorf("Expected CA cert file 'ca.pem', got '%s'", server.TLSCACertFile)
		}
	})

	t.Run("Enable WebSocket", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		server.EnableWebSocket("0.0.0.0", 9002, "/mqtt/ws")

		if !server.WSEnabled {
			t.Error("Expected WebSocket to be enabled")
		}
		if server.WSHost != "0.0.0.0" {
			t.Errorf("Expected WebSocket host '0.0.0.0', got '%s'", server.WSHost)
		}
		if server.WSPort != 9002 {
			t.Errorf("Expected WebSocket port 9002, got %d", server.WSPort)
		}
		if server.WSPath != "/mqtt/ws" {
			t.Errorf("Expected WebSocket path '/mqtt/ws', got '%s'", server.WSPath)
		}
	})

	t.Run("Enable secure WebSocket", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		server.EnableSecureWebSocket(9444, "cert.pem", "key.pem")

		if !server.WSSTLSEnabled {
			t.Error("Expected secure WebSocket to be enabled")
		}
		if server.WSSTLSPort != 9444 {
			t.Errorf("Expected secure WebSocket port 9444, got %d", server.WSSTLSPort)
		}
		if server.WSSTLSCertFile != "cert.pem" {
			t.Errorf("Expected cert file 'cert.pem', got '%s'", server.WSSTLSCertFile)
		}
		if server.WSSTLSKeyFile != "key.pem" {
			t.Errorf("Expected key file 'key.pem', got '%s'", server.WSSTLSKeyFile)
		}
	})
}

func TestServerTopicMatching(t *testing.T) {
	t.Run("Topic matching function", func(t *testing.T) {
		testCases := []struct {
			name        string
			subTopic    string
			pubTopic    string
			shouldMatch bool
		}{
			{"Exact match", "topic", "topic", true},
			{"Single level wildcard", "topic/+", "topic/subtopic", true},
			{"Single level wildcard mismatch", "topic/+", "topic/subtopic/deeper", false},
			{"Multi-level wildcard", "topic/#", "topic/subtopic/deeper", true},
			{"Multi-level wildcard at root", "#", "any/topic/structure", true},
			{"Mixed wildcards", "topic/+/+/specific", "topic/a/b/specific", true},
			{"Mixed wildcards mismatch", "topic/+/+/specific", "topic/a/b/c/specific", false},
			{"Prefix mismatch", "topic", "topics", false},
			{"Suffix mismatch", "topic", "topic/subtopic", false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := topicMatches(tc.subTopic, tc.pubTopic)

				if result != tc.shouldMatch {
					t.Errorf("Expected topicMatches('%s', '%s') = %v, got %v",
						tc.subTopic, tc.pubTopic, tc.shouldMatch, result)
				}
			})
		}
	})
}

func TestMessageDistribution(t *testing.T) {
	t.Run("Store and retrieve retained message", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		// Store a retained message
		server.storeRetainedMessage("test/retained", []byte("retained data"), 1)

		// Check it was stored
		if len(server.retainedMessages) != 1 {
			t.Errorf("Expected 1 retained message, got %d", len(server.retainedMessages))
		}

		// Check the message content
		msg, ok := server.retainedMessages["test/retained"]
		if !ok {
			t.Fatal("Retained message not found")
		}

		if msg.Topic != "test/retained" {
			t.Errorf("Expected topic 'test/retained', got '%s'", msg.Topic)
		}
		if string(msg.Payload) != "retained data" {
			t.Errorf("Expected payload 'retained data', got '%s'", string(msg.Payload))
		}
		if msg.QoS != 1 {
			t.Errorf("Expected QoS 1, got %d", msg.QoS)
		}

		// Storing an empty payload should remove the retained message
		server.storeRetainedMessage("test/retained", []byte{}, 0)

		// Check it was removed
		if len(server.retainedMessages) != 0 {
			t.Errorf("Expected 0 retained messages after removal, got %d", len(server.retainedMessages))
		}
	})

	t.Run("Generate message ID", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		// Generate first message ID
		id1 := server.generateMessageID("client1")

		// Should be positive (uint16 can't be > 65535)
		if id1 < 1 {
			t.Errorf("Expected message ID to be at least 1, got %d", id1)
		}

		// Generate another message ID for the same client
		id2 := server.generateMessageID("client1")

		// Should be different from the first one
		if id2 == id1 {
			t.Errorf("Expected different message IDs, got %d and %d", id1, id2)
		}

		// Generate message ID for a different client and verify maps are created
		_ = server.generateMessageID("client2")

		// Should be independent from the first client's IDs
		if server.inflightMessages["client1"] == nil || server.inflightMessages["client2"] == nil {
			t.Error("Expected inflight message maps to be created for both clients")
		}
	})

	t.Run("Store and acknowledge inflight message", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		// Store an inflight message
		server.storeInflightMessage("client1", 123, "test/topic", []byte("payload"), 1)

		// Check it was stored
		if server.inflightMessages["client1"] == nil {
			t.Fatal("Expected client1 inflight messages map to be created")
		}
		if len(server.inflightMessages["client1"]) != 1 {
			t.Errorf("Expected 1 inflight message, got %d", len(server.inflightMessages["client1"]))
		}

		// Check the message
		msg, ok := server.inflightMessages["client1"][123]
		if !ok {
			t.Fatal("Inflight message not found")
		}

		if msg.MessageID != 123 {
			t.Errorf("Expected message ID 123, got %d", msg.MessageID)
		}
		if msg.ClientID != "client1" {
			t.Errorf("Expected client ID 'client1', got '%s'", msg.ClientID)
		}
		if msg.Topic != "test/topic" {
			t.Errorf("Expected topic 'test/topic', got '%s'", msg.Topic)
		}
		if string(msg.Payload) != "payload" {
			t.Errorf("Expected payload 'payload', got '%s'", string(msg.Payload))
		}
		if msg.QoS != 1 {
			t.Errorf("Expected QoS 1, got %d", msg.QoS)
		}
		if msg.Acknowledged {
			t.Error("Expected message to not be acknowledged initially")
		}

		// Acknowledge the message
		server.acknowledgeInflightMessage("client1", 123)

		// Check it was acknowledged
		msg, ok = server.inflightMessages["client1"][123]
		if !ok {
			t.Fatal("Inflight message not found after acknowledgement")
		}
		if !msg.Acknowledged {
			t.Error("Expected message to be acknowledged")
		}

		// Remove the message
		server.removeInflightMessage("client1", 123)

		// Check it was removed
		if len(server.inflightMessages["client1"]) != 0 {
			t.Errorf("Expected 0 inflight messages after removal, got %d", len(server.inflightMessages["client1"]))
		}
	})
}

func TestMessageRetention(t *testing.T) {
	t.Run("Set message retention", func(t *testing.T) {
		server := NewServer("localhost", 1883)

		// Default retention should be 24h
		if server.messageRetention != 24*time.Hour {
			t.Errorf("Expected default retention 24h, got %v", server.messageRetention)
		}

		// Set to 1 hour
		server.SetMessageRetention(1 * time.Hour)

		if server.messageRetention != 1*time.Hour {
			t.Errorf("Expected retention 1h, got %v", server.messageRetention)
		}

		// Set to 0 (forever)
		server.SetMessageRetention(0)

		if server.messageRetention != 0 {
			t.Errorf("Expected retention 0, got %v", server.messageRetention)
		}
	})
}
