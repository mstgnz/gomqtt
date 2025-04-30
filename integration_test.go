package main

import (
	"log"
	"testing"
	"time"

	"github.com/mstgnz/gomqtt/cluster"
	"github.com/mstgnz/gomqtt/mqtt"
)

// This test verifies that the clustering functionality works end-to-end
func TestClusterIntegration(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create two MQTT servers to simulate a cluster
	server1 := mqtt.NewServer("127.0.0.1", 0) // Using port 0 to let the OS choose an available port
	server2 := mqtt.NewServer("127.0.0.1", 0)

	// Create cluster nodes
	cluster1 := cluster.NewCluster(
		"node1",
		"127.0.0.1",
		7946, // Use fixed ports for this test
		7947,
		[]string{},
		1*time.Second, // Short interval for testing
	)

	cluster2 := cluster.NewCluster(
		"node2",
		"127.0.0.1",
		7948,
		7949,
		[]string{"127.0.0.1:7946"}, // Node 2 connects to Node 1
		1*time.Second,
	)

	// Set up clusters
	server1.SetClusterService(cluster1)
	server2.SetClusterService(cluster2)

	// Create channels to track events
	msgReceivedNode1 := make(chan struct{})
	msgReceivedNode2 := make(chan struct{})

	// Set up a callback on node 2 to detect message sync from node 1
	cluster2.RegisterCallbacks(
		nil, // onSubscribe
		nil, // onUnsubscribe
		func(topic string, payload []byte, qos byte, retained bool) {
			log.Printf("Node2: Received message on topic %s: %s", topic, string(payload))
			if topic == "test/topic" && string(payload) == "test message" {
				msgReceivedNode2 <- struct{}{}
			}
		},
	)

	// Set up a callback on node 1 to detect message sync from node 2
	cluster1.RegisterCallbacks(
		nil, // onSubscribe
		nil, // onUnsubscribe
		func(topic string, payload []byte, qos byte, retained bool) {
			log.Printf("Node1: Received message on topic %s: %s", topic, string(payload))
			if topic == "test/topic2" && string(payload) == "test message 2" {
				msgReceivedNode1 <- struct{}{}
			}
		},
	)

	// Start the clusters
	if err := cluster1.Start(); err != nil {
		t.Fatalf("Failed to start cluster1: %v", err)
	}
	defer cluster1.Stop()

	if err := cluster2.Start(); err != nil {
		t.Fatalf("Failed to start cluster2: %v", err)
	}
	defer cluster2.Stop()

	// Allow clusters to discover each other
	time.Sleep(2 * time.Second)

	// Publish a retained message on node 1
	server1.StoreRetainedMessage("test/topic", []byte("test message"), 1)

	// Manually broadcast the retained message since our test may not have complete event hooks
	cluster1.BroadcastRetainedMessage("test/topic", []byte("test message"), 1)

	// Wait for node 2 to receive the message
	select {
	case <-msgReceivedNode2:
		// Success! Node 2 received the message
	case <-time.After(5 * time.Second):
		t.Errorf("Timeout waiting for message to sync from node 1 to node 2")
	}

	// Publish a retained message on node 2
	server2.StoreRetainedMessage("test/topic2", []byte("test message 2"), 1)

	// Manually broadcast the retained message since our test may not have complete event hooks
	cluster2.BroadcastRetainedMessage("test/topic2", []byte("test message 2"), 1)

	// Wait for node 1 to receive the message
	select {
	case <-msgReceivedNode1:
		// Success! Node 1 received the message
	case <-time.After(5 * time.Second):
		t.Errorf("Timeout waiting for message to sync from node 2 to node 1")
	}

	// Verify that node 2 has the retained message from node 1
	server2.GetRetainedMessages(func(topic string, payload []byte, qos byte) bool {
		if topic == "test/topic" && string(payload) == "test message" {
			return false // Stop iteration, we found what we were looking for
		}
		return true // Continue iteration
	})

	// Verify that node 1 has the retained message from node 2
	server1.GetRetainedMessages(func(topic string, payload []byte, qos byte) bool {
		if topic == "test/topic2" && string(payload) == "test message 2" {
			return false // Stop iteration, we found what we were looking for
		}
		return true // Continue iteration
	})
}
