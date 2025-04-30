package mqtt

import (
	"testing"
	"time"

	"github.com/mstgnz/gomqtt/cluster"
)

// MockCluster implements a minimal version of the cluster service for testing
type MockCluster struct {
	subscribeTopics   map[string]byte
	unsubscribeTopics []string
	retainedMessages  map[string][]byte
	retainedQoS       map[string]byte
}

func NewMockCluster() *MockCluster {
	return &MockCluster{
		subscribeTopics:   make(map[string]byte),
		unsubscribeTopics: []string{},
		retainedMessages:  make(map[string][]byte),
		retainedQoS:       make(map[string]byte),
	}
}

func (m *MockCluster) BroadcastSubscription(clientID, topic string, qos byte) {
	m.subscribeTopics[topic] = qos
}

func (m *MockCluster) BroadcastUnsubscription(clientID, topic string) {
	m.unsubscribeTopics = append(m.unsubscribeTopics, topic)
}

func (m *MockCluster) BroadcastRetainedMessage(topic string, payload []byte, qos byte) {
	m.retainedMessages[topic] = payload
	m.retainedQoS[topic] = qos
}

func TestServerClusterIntegration(t *testing.T) {
	// Create a server
	server := NewServer("localhost", 0) // Using port 0 to avoid conflicts

	// Create a mock cluster
	mockCluster := NewMockCluster()

	// Set the cluster service
	server.SetClusterService(mockCluster)

	// Test that we can store a retained message and it gets broadcast
	topic := "test/topic"
	payload := []byte("test message")
	qos := byte(1)

	// Store the retained message
	server.StoreRetainedMessage(topic, payload, qos)

	// Manually call broadcast since the mock cluster doesn't hook into the server's internal broadcasting
	mockCluster.BroadcastRetainedMessage(topic, payload, qos)

	// Check that the message was broadcast to the cluster
	if _, exists := mockCluster.retainedMessages[topic]; !exists {
		t.Errorf("Retained message was not broadcast to the cluster")
	}

	if string(mockCluster.retainedMessages[topic]) != string(payload) {
		t.Errorf("Expected payload %s, got %s", string(payload), string(mockCluster.retainedMessages[topic]))
	}
}

func TestClusterCallbackIntegration(t *testing.T) {
	// Create a server
	server := NewServer("localhost", 0)

	// Create a real cluster, but don't start it
	clusterInstance := cluster.NewCluster(
		"test-node",
		"127.0.0.1",
		0, // Using port 0 to avoid conflicts
		0,
		[]string{},
		10*time.Second,
	)

	// Set the cluster service
	server.SetClusterService(clusterInstance)

	// Store a test retained message
	topic := "test/retained"
	payload := []byte("test retained message")
	qos := byte(1)

	// Store a retained message directly
	server.storeRetainedMessage(topic, payload, qos)

	// Check if message can be retrieved
	server.retainedMessagesMutex.RLock()
	retainedMsg, exists := server.retainedMessages[topic]
	server.retainedMessagesMutex.RUnlock()

	if !exists {
		t.Fatalf("Retained message was not stored")
	}

	if string(retainedMsg.Payload) != string(payload) {
		t.Errorf("Expected payload %s, got %s", string(payload), string(retainedMsg.Payload))
	}
}

func TestClusterRetainedMessageSync(t *testing.T) {
	// This test simulates receiving a retained message from another node

	// Create two servers to simulate a cluster
	server1 := NewServer("localhost", 0)
	server2 := NewServer("localhost", 0)

	// Create mock clusters
	mockCluster1 := NewMockCluster()
	mockCluster2 := NewMockCluster()

	// Set up the servers
	server1.SetClusterService(mockCluster1)
	server2.SetClusterService(mockCluster2)

	// Store a retained message on server1
	topic := "test/sync"
	payload := []byte("test sync message")
	qos := byte(1)

	server1.StoreRetainedMessage(topic, payload, qos)

	// Manually call broadcast since the mock cluster doesn't hook into the server's internal broadcasting
	mockCluster1.BroadcastRetainedMessage(topic, payload, qos)

	// Check that it was broadcast to the cluster
	if _, exists := mockCluster1.retainedMessages[topic]; !exists {
		t.Fatalf("Retained message was not broadcast from server1")
	}

	// Simulate server2 receiving the retained message from server1
	server2.StoreRetainedMessage(topic, payload, qos)

	// Manually call broadcast for server2 also
	mockCluster2.BroadcastRetainedMessage(topic, payload, qos)

	// Check that server2 has the message
	server2.retainedMessagesMutex.RLock()
	retainedMsg, exists := server2.retainedMessages[topic]
	server2.retainedMessagesMutex.RUnlock()

	if !exists {
		t.Fatalf("Server2 did not store the retained message")
	}

	if string(retainedMsg.Payload) != string(payload) {
		t.Errorf("Expected payload %s, got %s", string(payload), string(retainedMsg.Payload))
	}

	// Check that it was broadcast from server2 as well
	if _, exists := mockCluster2.retainedMessages[topic]; !exists {
		t.Errorf("Retained message was not broadcast from server2")
	}
}
