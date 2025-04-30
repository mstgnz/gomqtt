package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

func TestNewCluster(t *testing.T) {
	// Create a new cluster instance
	nodeID := "test-node-1"
	nodeHost := "127.0.0.1"
	nodePort := 7946
	gossipPort := 7947
	seedNodes := []string{}
	syncInterval := 10 * time.Second

	cluster := NewCluster(nodeID, nodeHost, nodePort, gossipPort, seedNodes, syncInterval)

	// Check if the cluster was created correctly
	if cluster.NodeID != nodeID {
		t.Errorf("Expected NodeID to be %s, got %s", nodeID, cluster.NodeID)
	}

	if cluster.NodeHost != nodeHost {
		t.Errorf("Expected NodeHost to be %s, got %s", nodeHost, cluster.NodeHost)
	}

	if cluster.NodePort != nodePort {
		t.Errorf("Expected NodePort to be %d, got %d", nodePort, cluster.NodePort)
	}

	if cluster.GossipPort != gossipPort {
		t.Errorf("Expected GossipPort to be %d, got %d", gossipPort, cluster.GossipPort)
	}

	if cluster.SyncInterval != syncInterval {
		t.Errorf("Expected SyncInterval to be %s, got %s", syncInterval, cluster.SyncInterval)
	}

	if len(cluster.SeedNodes) != 0 {
		t.Errorf("Expected SeedNodes to be empty, got %v", cluster.SeedNodes)
	}

	if cluster.nodes == nil {
		t.Error("Expected nodes map to be initialized")
	}

	if cluster.eventCh == nil {
		t.Error("Expected eventCh to be initialized")
	}

	if cluster.shutdownCh == nil {
		t.Error("Expected shutdownCh to be initialized")
	}

	if cluster.memberDelegate == nil {
		t.Error("Expected memberDelegate to be initialized")
	}
}

func TestClusterNodeIDGeneration(t *testing.T) {
	// Test auto-generation of node ID when not provided
	cluster := NewCluster("", "127.0.0.1", 7946, 7947, []string{}, 10*time.Second)

	if cluster.NodeID == "" {
		t.Error("Expected NodeID to be auto-generated, got empty string")
	}
}

func TestBroadcastMethods(t *testing.T) {
	// Create a cluster for testing message broadcasting
	cluster := NewCluster("test-node", "127.0.0.1", 7946, 7947, []string{}, 10*time.Second)

	// Test broadcasting a subscription
	clientID := "client1"
	topic := "test/topic"
	qos := byte(1)

	// This shouldn't panic
	cluster.BroadcastSubscription(clientID, topic, qos)

	// Test broadcasting an unsubscription
	cluster.BroadcastUnsubscription(clientID, topic)

	// Test broadcasting a retained message
	payload := []byte("test message")
	cluster.BroadcastRetainedMessage(topic, payload, qos)
}

func TestClusterCallbacks(t *testing.T) {
	cluster := NewCluster("test-node", "127.0.0.1", 7946, 7947, []string{}, 10*time.Second)

	// Set up test flags to check if callbacks are executed
	subscribeExecuted := false
	unsubscribeExecuted := false
	publishExecuted := false

	// Register callbacks
	cluster.RegisterCallbacks(
		func(clientID, topic string, qos byte) {
			subscribeExecuted = true
			// Check parameters
			if clientID != "client1" || topic != "test/topic" || qos != 1 {
				t.Errorf("Subscribe callback received unexpected parameters: %s, %s, %d", clientID, topic, qos)
			}
		},
		func(clientID, topic string) {
			unsubscribeExecuted = true
			// Check parameters
			if clientID != "client1" || topic != "test/topic" {
				t.Errorf("Unsubscribe callback received unexpected parameters: %s, %s", clientID, topic)
			}
		},
		func(topic string, payload []byte, qos byte, retained bool) {
			publishExecuted = true
			// Check parameters
			if topic != "test/topic" || string(payload) != "test message" || qos != 1 || !retained {
				t.Errorf("Publish callback received unexpected parameters: %s, %s, %d, %v",
					topic, string(payload), qos, retained)
			}
		},
	)

	// Create test messages
	subInfo := SubscriptionInfo{
		ClientID: "client1",
		Topic:    "test/topic",
		QoS:      1,
	}
	subPayload, _ := json.Marshal(subInfo)
	subMsg := ClusterMessage{
		Type:      EventTopicSubscribe,
		NodeID:    "other-node",
		Timestamp: time.Now().UnixNano(),
		Payload:   subPayload,
	}

	// Simulate receiving subscribe event
	cluster.handleLocalEvent(subMsg)
	if !subscribeExecuted {
		t.Error("Subscribe callback was not executed")
	}

	// Simulate receiving unsubscribe event
	unsubMsg := ClusterMessage{
		Type:      EventTopicUnsubscribe,
		NodeID:    "other-node",
		Timestamp: time.Now().UnixNano(),
		Payload:   subPayload, // Same payload structure
	}
	cluster.handleLocalEvent(unsubMsg)
	if !unsubscribeExecuted {
		t.Error("Unsubscribe callback was not executed")
	}

	// Simulate receiving retained message event
	msgInfo := RetainedMessageInfo{
		Topic:    "test/topic",
		Payload:  []byte("test message"),
		QoS:      1,
		Modified: time.Now().UnixNano(),
	}
	msgPayload, _ := json.Marshal(msgInfo)
	retainedMsg := ClusterMessage{
		Type:      EventRetainedMessage,
		NodeID:    "other-node",
		Timestamp: time.Now().UnixNano(),
		Payload:   msgPayload,
	}
	cluster.handleLocalEvent(retainedMsg)
	if !publishExecuted {
		t.Error("Publish callback was not executed")
	}
}

// Mock implementation for broadcast testing
type mockBroadcast struct {
	message []byte
}

func (b *mockBroadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *mockBroadcast) Message() []byte {
	return b.message
}

func (b *mockBroadcast) Finished() {}
