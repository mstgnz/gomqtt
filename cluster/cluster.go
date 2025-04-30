package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
)

// Constants for cluster events
const (
	EventTopicSubscribe   = "topic.subscribe"
	EventTopicUnsubscribe = "topic.unsubscribe"
	EventClientConnect    = "client.connect"
	EventClientDisconnect = "client.disconnect"
	EventRetainedMessage  = "retained.message"
)

// ClusterMessage represents a message shared across the cluster
type ClusterMessage struct {
	Type      string          `json:"type"`
	NodeID    string          `json:"node_id"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// SubscriptionInfo contains information about a subscription
type SubscriptionInfo struct {
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
	QoS      byte   `json:"qos"`
}

// RetainedMessageInfo contains information about a retained message
type RetainedMessageInfo struct {
	Topic    string `json:"topic"`
	Payload  []byte `json:"payload"`
	QoS      byte   `json:"qos"`
	Modified int64  `json:"modified"`
}

// ClientInfo contains information about a client connection
type ClientInfo struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	NodeID   string `json:"node_id"` // Which node the client is connected to
}

// ClusterNode represents a node in the cluster
type ClusterNode struct {
	ID           string
	Host         string
	Port         int
	LastSeen     time.Time
	IsReady      bool
	Capabilities map[string]bool
}

// ClusterMember implements the memberlist.Delegate interface
type ClusterMember struct {
	Meta             []byte
	Broadcasts       *memberlist.TransmitLimitedQueue
	cluster          *Cluster
	subscriptions    map[string]map[string]SubscriptionInfo // topic -> clientID -> info
	clients          map[string]ClientInfo                  // clientID -> info
	retainedMessages map[string]RetainedMessageInfo         // topic -> info
	mutex            sync.RWMutex
}

// Cluster represents the cluster manager
type Cluster struct {
	// Configuration
	NodeID       string
	NodeHost     string
	NodePort     int
	GossipPort   int
	SeedNodes    []string
	SyncInterval time.Duration

	// Memberlist
	memberlist     *memberlist.Memberlist
	memberDelegate *ClusterMember

	// Node tracking
	nodes      map[string]*ClusterNode
	nodesMutex sync.RWMutex

	// Callbacks for handling events
	onSubscribe   func(clientID, topic string, qos byte)
	onUnsubscribe func(clientID, topic string)
	onPublish     func(topic string, payload []byte, qos byte, retained bool)

	// Local node events channel
	eventCh chan ClusterMessage

	// Shutdown flag
	isShutdown bool
	shutdownCh chan struct{}
}

// NewCluster creates a new cluster instance
func NewCluster(nodeID, nodeHost string, nodePort, gossipPort int, seedNodes []string, syncInterval time.Duration) *Cluster {
	// If no node ID is provided, generate one
	if nodeID == "" {
		nodeID = uuid.New().String()
		log.Printf("Generated cluster node ID: %s", nodeID)
	}

	c := &Cluster{
		NodeID:       nodeID,
		NodeHost:     nodeHost,
		NodePort:     nodePort,
		GossipPort:   gossipPort,
		SeedNodes:    seedNodes,
		SyncInterval: syncInterval,
		nodes:        make(map[string]*ClusterNode),
		eventCh:      make(chan ClusterMessage, 100),
		shutdownCh:   make(chan struct{}),
	}

	// Create memberlist delegate
	c.memberDelegate = &ClusterMember{
		Broadcasts:       &memberlist.TransmitLimitedQueue{RetransmitMult: 10, NumNodes: func() int { return 1024 }},
		cluster:          c,
		subscriptions:    make(map[string]map[string]SubscriptionInfo),
		clients:          make(map[string]ClientInfo),
		retainedMessages: make(map[string]RetainedMessageInfo),
	}

	return c
}

// Start initializes and starts the cluster
func (c *Cluster) Start() error {
	log.Printf("Starting cluster node %s on %s:%d", c.NodeID, c.NodeHost, c.NodePort)

	// Create memberlist configuration
	config := memberlist.DefaultLANConfig()
	config.Name = c.NodeID
	config.BindAddr = c.NodeHost
	config.BindPort = c.NodePort
	config.AdvertisePort = c.NodePort
	config.Delegate = c.memberDelegate
	config.Events = &memberlist.ChannelEventDelegate{Ch: make(chan memberlist.NodeEvent, 100)}

	// Create the memberlist
	ml, err := memberlist.Create(config)
	if err != nil {
		return fmt.Errorf("failed to create memberlist: %v", err)
	}
	c.memberlist = ml

	// Add ourselves to the node list
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	c.nodesMutex.Lock()
	c.nodes[c.NodeID] = &ClusterNode{
		ID:           c.NodeID,
		Host:         c.NodeHost,
		Port:         c.NodePort,
		LastSeen:     time.Now(),
		IsReady:      true,
		Capabilities: map[string]bool{"mqtt": true},
	}
	c.nodesMutex.Unlock()

	// Join the cluster if seed nodes are provided
	if len(c.SeedNodes) > 0 {
		_, err := c.memberlist.Join(c.SeedNodes)
		if err != nil {
			log.Printf("Failed to join cluster: %v", err)
			// Continue anyway, other nodes might join us
		}
	}

	// Start the event processor
	go c.processEvents()

	// Start the periodic sync
	go c.periodicSync()

	log.Printf("Cluster node started. Active nodes: %d", c.memberlist.NumMembers())
	return nil
}

// Stop shuts down the cluster node
func (c *Cluster) Stop() error {
	c.isShutdown = true
	close(c.shutdownCh)

	if c.memberlist != nil {
		if err := c.memberlist.Leave(time.Second * 5); err != nil {
			log.Printf("Error leaving cluster: %v", err)
		}

		if err := c.memberlist.Shutdown(); err != nil {
			return fmt.Errorf("failed to shutdown memberlist: %v", err)
		}
	}

	log.Printf("Cluster node %s stopped", c.NodeID)
	return nil
}

// processEvents handles cluster events
func (c *Cluster) processEvents() {
	for {
		select {
		case <-c.shutdownCh:
			return
		case msg := <-c.eventCh:
			c.handleLocalEvent(msg)
		}
	}
}

// handleLocalEvent processes local node events
func (c *Cluster) handleLocalEvent(msg ClusterMessage) {
	switch msg.Type {
	case EventTopicSubscribe:
		var subInfo SubscriptionInfo
		if err := json.Unmarshal(msg.Payload, &subInfo); err != nil {
			log.Printf("Error unmarshaling subscription info: %v", err)
			return
		}

		if c.onSubscribe != nil {
			c.onSubscribe(subInfo.ClientID, subInfo.Topic, subInfo.QoS)
		}

	case EventTopicUnsubscribe:
		var subInfo SubscriptionInfo
		if err := json.Unmarshal(msg.Payload, &subInfo); err != nil {
			log.Printf("Error unmarshaling unsubscription info: %v", err)
			return
		}

		if c.onUnsubscribe != nil {
			c.onUnsubscribe(subInfo.ClientID, subInfo.Topic)
		}

	case EventRetainedMessage:
		var msgInfo RetainedMessageInfo
		if err := json.Unmarshal(msg.Payload, &msgInfo); err != nil {
			log.Printf("Error unmarshaling retained message info: %v", err)
			return
		}

		if c.onPublish != nil {
			c.onPublish(msgInfo.Topic, msgInfo.Payload, msgInfo.QoS, true)
		}
	}
}

// periodicSync runs a periodic sync with cluster nodes
func (c *Cluster) periodicSync() {
	ticker := time.NewTicker(c.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.shutdownCh:
			return
		case <-ticker.C:
			c.syncClusterState()
		}
	}
}

// syncClusterState synchronizes state across cluster nodes
func (c *Cluster) syncClusterState() {
	// Update node list based on memberlist
	c.updateNodeList()

	// Broadcast our state to other nodes
	c.broadcastState()
}

// updateNodeList updates the internal node list based on memberlist members
func (c *Cluster) updateNodeList() {
	members := c.memberlist.Members()
	now := time.Now()

	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	// Mark nodes as seen
	for _, member := range members {
		if node, exists := c.nodes[member.Name]; exists {
			node.LastSeen = now
		} else {
			// New node
			hostPort := member.Addr.String()
			host, _, _ := net.SplitHostPort(hostPort)
			if host == "" {
				host = member.Addr.String()
			}

			// Try to parse port from meta, default to NodePort
			port := c.NodePort
			if len(member.Meta) > 0 {
				var meta map[string]any
				if json.Unmarshal(member.Meta, &meta) == nil {
					if p, ok := meta["port"].(float64); ok {
						port = int(p)
					}
				}
			}

			c.nodes[member.Name] = &ClusterNode{
				ID:       member.Name,
				Host:     host,
				Port:     port,
				LastSeen: now,
				IsReady:  true,
			}

			log.Printf("New cluster node discovered: %s at %s:%d", member.Name, host, port)
		}
	}

	// Remove nodes that haven't been seen in a while (3x sync interval)
	cutoff := now.Add(-3 * c.SyncInterval)
	for id, node := range c.nodes {
		if id != c.NodeID && node.LastSeen.Before(cutoff) {
			delete(c.nodes, id)
			log.Printf("Removed inactive cluster node: %s", id)
		}
	}
}

// broadcastState broadcasts our state to other nodes
func (c *Cluster) broadcastState() {
	// We don't need to broadcast if we're the only node
	if c.memberlist.NumMembers() <= 1 {
		return
	}

	// Broadcast metadata about our node
	metadata := map[string]any{
		"id":   c.NodeID,
		"host": c.NodeHost,
		"port": c.NodePort,
		"capabilities": map[string]bool{
			"mqtt": true,
		},
	}

	metaJson, err := json.Marshal(metadata)
	if err == nil {
		c.memberDelegate.Meta = metaJson
	}
}

// BroadcastSubscription broadcasts a new subscription to the cluster
func (c *Cluster) BroadcastSubscription(clientID, topic string, qos byte) {
	subInfo := SubscriptionInfo{
		ClientID: clientID,
		Topic:    topic,
		QoS:      qos,
	}

	payload, err := json.Marshal(subInfo)
	if err != nil {
		log.Printf("Error marshaling subscription info: %v", err)
		return
	}

	msg := ClusterMessage{
		Type:      EventTopicSubscribe,
		NodeID:    c.NodeID,
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
	}

	c.broadcastMessage(msg)
}

// BroadcastUnsubscription broadcasts an unsubscription to the cluster
func (c *Cluster) BroadcastUnsubscription(clientID, topic string) {
	subInfo := SubscriptionInfo{
		ClientID: clientID,
		Topic:    topic,
	}

	payload, err := json.Marshal(subInfo)
	if err != nil {
		log.Printf("Error marshaling unsubscription info: %v", err)
		return
	}

	msg := ClusterMessage{
		Type:      EventTopicUnsubscribe,
		NodeID:    c.NodeID,
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
	}

	c.broadcastMessage(msg)
}

// BroadcastRetainedMessage broadcasts a retained message to the cluster
func (c *Cluster) BroadcastRetainedMessage(topic string, payload []byte, qos byte) {
	msgInfo := RetainedMessageInfo{
		Topic:    topic,
		Payload:  payload,
		QoS:      qos,
		Modified: time.Now().UnixNano(),
	}

	msgPayload, err := json.Marshal(msgInfo)
	if err != nil {
		log.Printf("Error marshaling retained message info: %v", err)
		return
	}

	msg := ClusterMessage{
		Type:      EventRetainedMessage,
		NodeID:    c.NodeID,
		Timestamp: time.Now().UnixNano(),
		Payload:   msgPayload,
	}

	c.broadcastMessage(msg)
}

// broadcastMessage distributes a message to all cluster nodes
func (c *Cluster) broadcastMessage(msg ClusterMessage) {
	// Handle locally first
	c.eventCh <- msg

	// Then broadcast to other nodes
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling cluster message: %v", err)
		return
	}

	c.memberDelegate.Broadcasts.QueueBroadcast(&broadcast{
		msg:    msgBytes,
		notify: nil,
	})
}

// RegisterCallbacks registers callback functions for cluster events
func (c *Cluster) RegisterCallbacks(
	onSubscribe func(clientID, topic string, qos byte),
	onUnsubscribe func(clientID, topic string),
	onPublish func(topic string, payload []byte, qos byte, retained bool),
) {
	c.onSubscribe = onSubscribe
	c.onUnsubscribe = onUnsubscribe
	c.onPublish = onPublish
}

// GetNodes returns a list of all nodes in the cluster
func (c *Cluster) GetNodes() []ClusterNode {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()

	nodes := make([]ClusterNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, *node)
	}
	return nodes
}

// broadcast is an implementation of the memberlist.Broadcast interface
type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

// NodeMeta is used to retrieve metadata about the current node
func (m *ClusterMember) NodeMeta(limit int) []byte {
	if len(m.Meta) > limit {
		return m.Meta[:limit]
	}
	return m.Meta
}

// NotifyMsg is called when a message is received from another node
func (m *ClusterMember) NotifyMsg(data []byte) {
	if len(data) == 0 {
		return
	}

	var msg ClusterMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("Error decoding cluster message: %v", err)
		return
	}

	// Skip messages from ourselves
	if msg.NodeID == m.cluster.NodeID {
		return
	}

	// Process based on message type
	switch msg.Type {
	case EventTopicSubscribe:
		var subInfo SubscriptionInfo
		if err := json.Unmarshal(msg.Payload, &subInfo); err != nil {
			log.Printf("Error decoding subscription info: %v", err)
			return
		}

		m.mutex.Lock()
		if _, exists := m.subscriptions[subInfo.Topic]; !exists {
			m.subscriptions[subInfo.Topic] = make(map[string]SubscriptionInfo)
		}
		m.subscriptions[subInfo.Topic][subInfo.ClientID] = subInfo
		m.mutex.Unlock()

		// Forward to application layer
		if m.cluster.onSubscribe != nil {
			m.cluster.onSubscribe(subInfo.ClientID, subInfo.Topic, subInfo.QoS)
		}

	case EventTopicUnsubscribe:
		var subInfo SubscriptionInfo
		if err := json.Unmarshal(msg.Payload, &subInfo); err != nil {
			log.Printf("Error decoding unsubscription info: %v", err)
			return
		}

		m.mutex.Lock()
		if topicSubs, exists := m.subscriptions[subInfo.Topic]; exists {
			delete(topicSubs, subInfo.ClientID)
			if len(topicSubs) == 0 {
				delete(m.subscriptions, subInfo.Topic)
			}
		}
		m.mutex.Unlock()

		// Forward to application layer
		if m.cluster.onUnsubscribe != nil {
			m.cluster.onUnsubscribe(subInfo.ClientID, subInfo.Topic)
		}

	case EventRetainedMessage:
		var msgInfo RetainedMessageInfo
		if err := json.Unmarshal(msg.Payload, &msgInfo); err != nil {
			log.Printf("Error decoding retained message info: %v", err)
			return
		}

		m.mutex.Lock()
		m.retainedMessages[msgInfo.Topic] = msgInfo
		m.mutex.Unlock()

		// Forward to application layer
		if m.cluster.onPublish != nil {
			m.cluster.onPublish(msgInfo.Topic, msgInfo.Payload, msgInfo.QoS, true)
		}

	case EventClientConnect:
		var clientInfo ClientInfo
		if err := json.Unmarshal(msg.Payload, &clientInfo); err != nil {
			log.Printf("Error decoding client connect info: %v", err)
			return
		}

		m.mutex.Lock()
		m.clients[clientInfo.ClientID] = clientInfo
		m.mutex.Unlock()

	case EventClientDisconnect:
		var clientInfo ClientInfo
		if err := json.Unmarshal(msg.Payload, &clientInfo); err != nil {
			log.Printf("Error decoding client disconnect info: %v", err)
			return
		}

		m.mutex.Lock()
		delete(m.clients, clientInfo.ClientID)
		m.mutex.Unlock()
	}
}

// GetBroadcasts is called when memberlist wants to broadcast a message
func (m *ClusterMember) GetBroadcasts(overhead, limit int) [][]byte {
	return m.Broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState is used to retrieve local state that should be transmitted to remote nodes
func (m *ClusterMember) LocalState(join bool) []byte {
	// For simplicity, we'll just use a JSON-encoded map of our current state
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	state := map[string]any{
		"node_id":           m.cluster.NodeID,
		"subscriptions":     m.subscriptions,
		"retained_messages": m.retainedMessages,
		"clients":           m.clients,
	}

	stateBytes, err := json.Marshal(state)
	if err != nil {
		log.Printf("Error encoding local state: %v", err)
		return nil
	}

	return stateBytes
}

// MergeRemoteState is used to merge the remote state into our local state
func (m *ClusterMember) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}

	var remoteState map[string]json.RawMessage
	if err := json.Unmarshal(buf, &remoteState); err != nil {
		log.Printf("Error decoding remote state: %v", err)
		return
	}

	// Extract node ID from remote state
	var nodeID string
	if id, ok := remoteState["node_id"]; ok {
		if err := json.Unmarshal(id, &nodeID); err == nil && nodeID != "" {
			// Skip our own state
			if nodeID == m.cluster.NodeID {
				return
			}
		}
	}

	// Merge subscriptions
	if subsJSON, ok := remoteState["subscriptions"]; ok {
		var remoteSubs map[string]map[string]SubscriptionInfo
		if err := json.Unmarshal(subsJSON, &remoteSubs); err != nil {
			log.Printf("Error decoding remote subscriptions: %v", err)
		} else {
			m.mutex.Lock()
			for topic, clients := range remoteSubs {
				if _, exists := m.subscriptions[topic]; !exists {
					m.subscriptions[topic] = make(map[string]SubscriptionInfo)
				}
				for clientID, subInfo := range clients {
					m.subscriptions[topic][clientID] = subInfo

					// Notify application layer of subscriptions
					if m.cluster.onSubscribe != nil {
						m.cluster.onSubscribe(clientID, topic, subInfo.QoS)
					}
				}
			}
			m.mutex.Unlock()
		}
	}

	// Merge retained messages
	if msgsJSON, ok := remoteState["retained_messages"]; ok {
		var remoteMsgs map[string]RetainedMessageInfo
		if err := json.Unmarshal(msgsJSON, &remoteMsgs); err != nil {
			log.Printf("Error decoding remote retained messages: %v", err)
		} else {
			m.mutex.Lock()
			for topic, msgInfo := range remoteMsgs {
				// Check if we already have a newer version
				if existing, ok := m.retainedMessages[topic]; !ok || existing.Modified < msgInfo.Modified {
					m.retainedMessages[topic] = msgInfo

					// Notify application layer of retained messages
					if m.cluster.onPublish != nil {
						m.cluster.onPublish(topic, msgInfo.Payload, msgInfo.QoS, true)
					}
				}
			}
			m.mutex.Unlock()
		}
	}

	// Merge client information
	if clientsJSON, ok := remoteState["clients"]; ok {
		var remoteClients map[string]ClientInfo
		if err := json.Unmarshal(clientsJSON, &remoteClients); err != nil {
			log.Printf("Error decoding remote clients: %v", err)
		} else {
			m.mutex.Lock()
			for clientID, clientInfo := range remoteClients {
				m.clients[clientID] = clientInfo
			}
			m.mutex.Unlock()
		}
	}
}
