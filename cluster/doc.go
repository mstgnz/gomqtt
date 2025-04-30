/*
Package cluster provides multi-node clustering support for GoMQTT.

This package implements a distributed broker cluster that enables:

  - Automatic node discovery and membership management
  - Message synchronization between nodes
  - Shared subscriptions across the cluster
  - Retained message replication
  - Session state synchronization
  - High availability through redundancy

# Cluster Architecture

The cluster uses a gossip protocol (based on the memberlist library) for node discovery and
communication. Each node in the cluster maintains:

  - Membership list of all active nodes
  - Synchronized subscription database
  - Replicated retained messages
  - State synchronization mechanism

# Cluster Configuration

Creating and starting a cluster:

	// Create a new cluster instance
	clusterService := cluster.NewCluster(
	    "node1",              // Unique node ID
	    "192.168.1.10",       // Node host address
	    7946,                 // Node port for memberlist
	    7947,                 // Gossip port
	    []string{"node2:7946", "node3:7946"}, // Seed nodes to join
	    30 * time.Second,     // Sync interval
	)

	// Register callbacks for cluster events
	clusterService.RegisterCallbacks(
	    // onSubscribe
	    func(clientID, topic string, qos byte) {
	        log.Printf("Remote subscription: %s -> %s (QoS %d)", clientID, topic, qos)
	    },
	    // onUnsubscribe
	    func(clientID, topic string) {
	        log.Printf("Remote unsubscription: %s -> %s", clientID, topic)
	    },
	    // onPublish
	    func(topic string, payload []byte, qos byte, retained bool) {
	        log.Printf("Remote publish to %s (QoS %d, retained: %v)", topic, qos, retained)
	    },
	)

	// Start the cluster
	if err := clusterService.Start(); err != nil {
	    log.Fatalf("Failed to start cluster: %v", err)
	}
	defer clusterService.Stop()

# Cluster Features

## Node Discovery

Nodes automatically discover each other using:

  - Seed node list for initial connection
  - Gossip protocol for membership updates
  - Health checking for node availability monitoring
  - Automatic reconnection after network partitions

## Message Synchronization

When a message is published to one node, it is replicated to all other nodes:

  - QoS 0, 1, and 2 message synchronization
  - Retained message replication
  - Will message distribution
  - Optimized binary protocol for efficient transfer

## Shared Subscriptions

Shared subscriptions ($share/group/topic) are supported across the cluster:

  - Load balanced message distribution
  - Fair sharing between subscribers
  - Group membership tracking
  - Subscriber failure handling

## High Availability

The cluster provides high availability through:

  - Automatic failover for client connections
  - Session state replication
  - Subscription synchronization
  - Node health monitoring

# Integration with MQTT Server

To integrate the cluster with the MQTT server:

	// Create and start the cluster
	clusterService := cluster.NewCluster(...)
	if err := clusterService.Start(); err != nil {
	    log.Fatalf("Failed to start cluster: %v", err)
	}

	// Create the MQTT server
	mqttServer := mqtt.NewServer("0.0.0.0", 1883)

	// Set the cluster service
	mqttServer.SetClusterService(clusterService)

	// Start the MQTT server
	if err := mqttServer.Start(); err != nil {
	    log.Fatalf("Failed to start MQTT server: %v", err)
	}

	// Shutdown gracefully
	defer func() {
	    mqttServer.Stop()
	    clusterService.Stop()
	}()

# Examples

Multi-node deployment with Docker Compose:

	version: '3'
	services:
	  node1:
	    image: gomqtt:latest
	    environment:
	      - GOMQTT_CLUSTER_ENABLED=true
	      - GOMQTT_CLUSTER_NODE_ID=node1
	      - GOMQTT_CLUSTER_NODE_HOST=node1
	      - GOMQTT_CLUSTER_SEED_NODES=node2:7946,node3:7946
	    ports:
	      - "1883:1883"
	    networks:
	      - mqtt-cluster

	  node2:
	    image: gomqtt:latest
	    environment:
	      - GOMQTT_CLUSTER_ENABLED=true
	      - GOMQTT_CLUSTER_NODE_ID=node2
	      - GOMQTT_CLUSTER_NODE_HOST=node2
	      - GOMQTT_CLUSTER_SEED_NODES=node1:7946,node3:7946
	    ports:
	      - "1884:1883"
	    networks:
	      - mqtt-cluster

	  node3:
	    image: gomqtt:latest
	    environment:
	      - GOMQTT_CLUSTER_ENABLED=true
	      - GOMQTT_CLUSTER_NODE_ID=node3
	      - GOMQTT_CLUSTER_NODE_HOST=node3
	      - GOMQTT_CLUSTER_SEED_NODES=node1:7946,node2:7946
	    ports:
	      - "1885:1883"
	    networks:
	      - mqtt-cluster

	networks:
	  mqtt-cluster:
*/
package cluster
