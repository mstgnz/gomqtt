package mqtt

import (
	"net"
	"testing"
	"time"
)

// mockConn is a mock net.Conn implementation for testing
type mockConn struct {
	readData    []byte
	writtenData []byte
	closed      bool
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if len(m.readData) == 0 {
		return 0, nil
	}
	n = copy(b, m.readData)
	m.readData = m.readData[n:]
	return n, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.writtenData = append(m.writtenData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1883}
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(192, 168, 1, 1), Port: 12345}
}

func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestClient(t *testing.T) {
	t.Run("Create client", func(t *testing.T) {
		conn := &mockConn{}
		client := NewClient("client123", conn)

		if client.ID != "client123" {
			t.Errorf("Expected client ID 'client123', got '%s'", client.ID)
		}
		if client.Conn != conn {
			t.Errorf("Expected connection to be set")
		}
		if !client.IsConnected {
			t.Error("Expected client to be connected")
		}
		if client.Subscriptions == nil {
			t.Error("Expected subscriptions map to be initialized")
		}
		if len(client.Subscriptions) != 0 {
			t.Errorf("Expected empty subscriptions, got %d", len(client.Subscriptions))
		}

		// Check default values
		if client.ProtocolVersion != 4 {
			t.Errorf("Expected protocol version 4, got %d", client.ProtocolVersion)
		}
		if client.ReceiveMaximum != 65535 {
			t.Errorf("Expected ReceiveMaximum 65535, got %d", client.ReceiveMaximum)
		}
		if client.TopicAliasMaximum != 0 {
			t.Errorf("Expected TopicAliasMaximum 0, got %d", client.TopicAliasMaximum)
		}
		if !client.RequestProblemInfo {
			t.Error("Expected RequestProblemInfo to be true")
		}
		if client.RequestResponseInfo {
			t.Error("Expected RequestResponseInfo to be false")
		}
	})

	t.Run("Subscribe to topic", func(t *testing.T) {
		conn := &mockConn{}
		client := NewClient("client123", conn)

		sub := client.Subscribe("sensors/temperature", 1)

		// Check the subscription was added
		if len(client.Subscriptions) != 1 {
			t.Errorf("Expected 1 subscription, got %d", len(client.Subscriptions))
		}

		// Check if it's retrievable
		if s, ok := client.Subscriptions["sensors/temperature"]; !ok || s != sub {
			t.Error("Subscription not found or incorrect in map")
		}

		// Check subscription properties
		if sub.Topic != "sensors/temperature" {
			t.Errorf("Expected topic 'sensors/temperature', got '%s'", sub.Topic)
		}
		if sub.QoS != 1 {
			t.Errorf("Expected QoS 1, got %d", sub.QoS)
		}
		if sub.ClientID != "client123" {
			t.Errorf("Expected ClientID client123, got %s", sub.ClientID)
		}
	})

	t.Run("Unsubscribe from topic", func(t *testing.T) {
		conn := &mockConn{}
		client := NewClient("client123", conn)

		client.Subscribe("sensors/temperature", 1)
		client.Subscribe("sensors/humidity", 0)

		// Verify we have 2 subscriptions
		if len(client.Subscriptions) != 2 {
			t.Errorf("Expected 2 subscriptions, got %d", len(client.Subscriptions))
		}

		// Unsubscribe from one topic
		client.Unsubscribe("sensors/temperature")

		// Verify we have 1 subscription left
		if len(client.Subscriptions) != 1 {
			t.Errorf("Expected 1 subscription, got %d", len(client.Subscriptions))
		}

		// The remaining subscription should be sensors/humidity
		if _, ok := client.Subscriptions["sensors/humidity"]; !ok {
			t.Error("Expected 'sensors/humidity' subscription to remain")
		}

		// sensors/temperature should be gone
		if _, ok := client.Subscriptions["sensors/temperature"]; ok {
			t.Error("Expected 'sensors/temperature' subscription to be removed")
		}
	})

	t.Run("Disconnect client", func(t *testing.T) {
		conn := &mockConn{}
		client := NewClient("client123", conn)

		if !client.IsConnected {
			t.Error("Expected client to be connected initially")
		}

		client.Disconnect()

		if client.IsConnected {
			t.Error("Expected client to be disconnected")
		}
		if !conn.closed {
			t.Error("Expected connection to be closed")
		}

		// Calling disconnect again shouldn't cause issues
		client.Disconnect()
	})

	t.Run("Process will message", func(t *testing.T) {
		conn := &mockConn{}
		client := NewClient("client123", conn)

		// No will message set initially
		if client.ProcessWill() {
			t.Error("Expected ProcessWill to return false when no will message set")
		}

		// Set will message
		client.WillTopic = "last/will"
		client.WillMessage = []byte("Goodbye!")
		client.WillQoS = 1
		client.WillRetain = true

		// Now process will should return true
		if !client.ProcessWill() {
			t.Error("Expected ProcessWill to return true when will message is set")
		}
	})

	t.Run("Topic aliases", func(t *testing.T) {
		conn := &mockConn{}
		client := NewClient("client123", conn)

		// Set protocol version to MQTT 5.0
		client.ProtocolVersion = MQTT_5_0

		// Add a topic alias
		client.AddTopicAlias(1, "sensors/temperature")

		// Verify the alias was added
		topic, exists := client.ResolveTopicAlias(1)
		if !exists {
			t.Error("Expected alias 1 to exist")
		}
		if topic != "sensors/temperature" {
			t.Errorf("Expected alias 1 to resolve to 'sensors/temperature', got '%s'", topic)
		}

		// Nonexistent alias should return false
		_, exists = client.ResolveTopicAlias(2)
		if exists {
			t.Error("Expected alias 2 to not exist")
		}

		// Set protocol version to MQTT 3.1.1, aliases shouldn't work
		client = NewClient("client123", conn)
		client.ProtocolVersion = 4 // MQTT 3.1.1

		client.AddTopicAlias(1, "sensors/temperature")

		// Verify the alias was not added
		_, exists = client.ResolveTopicAlias(1)
		if exists {
			t.Error("Expected alias to not be added for MQTT 3.1.1 client")
		}
	})
}
