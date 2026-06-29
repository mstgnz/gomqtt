package mqtt

import (
	"testing"
	"time"
)

// TestServerPublishMessageDelivered verifies that PublishMessage, the
// server-side injection path used by the REST API, actually delivers the
// message to a subscribed client (rather than silently dropping it as the
// old REST /publish stub did).
func TestServerPublishMessageDelivered(t *testing.T) {
	network := newMockNetwork()

	subConn := network.createClient("subscriber")
	if err := connectClient(subConn, "subscriber", true); err != nil {
		t.Fatalf("Failed to connect subscriber: %v", err)
	}

	if err := subscribeTopic(subConn, "api/test", 0); err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Allow the subscription to register.
	time.Sleep(100 * time.Millisecond)

	// Inject a message the same way the REST API handler does.
	network.server.PublishMessage("api", "api/test", []byte("from-rest-api"), 0, false)

	packet, err := readPublishedMessage(subConn, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Subscriber did not receive injected message: %v", err)
	}

	if packet.PacketType != PUBLISH {
		t.Errorf("Expected PUBLISH packet, got %d", packet.PacketType)
	}
	if packet.TopicName != "api/test" {
		t.Errorf("Expected topic 'api/test', got '%s'", packet.TopicName)
	}
	if string(packet.Payload) != "from-rest-api" {
		t.Errorf("Expected payload 'from-rest-api', got '%s'", string(packet.Payload))
	}
}

// TestServerPublishMessageRetained verifies that a retained injection is stored
// and replayed to a client that subscribes afterwards.
func TestServerPublishMessageRetained(t *testing.T) {
	network := newMockNetwork()

	network.server.PublishMessage("api", "api/retained", []byte("retained-via-api"), 0, true)

	if got := network.server.RetainedMessageCount(); got != 1 {
		t.Fatalf("Expected 1 retained message, got %d", got)
	}

	subConn := network.createClient("late-subscriber")
	if err := connectClient(subConn, "late-subscriber", true); err != nil {
		t.Fatalf("Failed to connect subscriber: %v", err)
	}

	if err := subscribeTopic(subConn, "api/retained", 0); err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	packet, err := readPublishedMessage(subConn, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Subscriber did not receive retained message: %v", err)
	}
	if string(packet.Payload) != "retained-via-api" {
		t.Errorf("Expected payload 'retained-via-api', got '%s'", string(packet.Payload))
	}
}
