package mqtt

import (
	"log"
	"net"
	"sync"
	"time"
)

// Client represents a connected MQTT client
type Client struct {
	// Client identification
	ID       string
	Username string

	// Connection
	Conn     net.Conn
	ConnTime time.Time

	// Subscription management
	Subscriptions map[string]*Subscription
	subMutex      sync.RWMutex

	// State management
	IsConnected bool
	LastSeen    time.Time

	// Will message (Last Will and Testament)
	WillTopic   string
	WillMessage []byte
	WillQoS     byte
	WillRetain  bool

	// MQTT v5.0 specific fields
	ProtocolVersion       byte
	SessionExpiryInterval int                 // Session expiry interval in seconds (0 = clean session)
	WillDelayInterval     int                 // Will delay interval in seconds
	ReceiveMaximum        int                 // Maximum receive limit for QoS > 0
	MaximumPacketSize     int                 // Maximum packet size client can receive
	TopicAliasMaximum     int                 // Maximum topic aliases
	RequestProblemInfo    bool                // Whether client wants problem info
	RequestResponseInfo   bool                // Whether client wants response info
	UserProperties        map[string][]string // User properties from CONNECT

	// Topic aliases (MQTT v5.0)
	TopicAliases      map[int]string // Alias -> topic mapping
	topicAliasesMutex sync.RWMutex
}

// NewClient creates a new MQTT client
func NewClient(id string, conn net.Conn) *Client {
	return &Client{
		ID:                  id,
		Conn:                conn,
		ConnTime:            time.Now(),
		Subscriptions:       make(map[string]*Subscription),
		IsConnected:         true,
		LastSeen:            time.Now(),
		ProtocolVersion:     4,     // Default to MQTT 3.1.1
		ReceiveMaximum:      65535, // Default
		MaximumPacketSize:   0,     // Unlimited by default
		TopicAliasMaximum:   0,     // No aliases by default
		RequestProblemInfo:  true,  // Default
		RequestResponseInfo: false, // Default
		UserProperties:      make(map[string][]string),
		TopicAliases:        make(map[int]string),
	}
}

// Subscribe subscribes a client to a topic
func (c *Client) Subscribe(topic string, qos byte) *Subscription {
	c.subMutex.Lock()
	defer c.subMutex.Unlock()

	sub := NewSubscription(topic, qos, c.ID)

	c.Subscriptions[topic] = sub
	return sub
}

// Unsubscribe removes a subscription for this client
func (c *Client) Unsubscribe(topic string) {
	c.subMutex.Lock()
	defer c.subMutex.Unlock()

	delete(c.Subscriptions, topic)
}

// Disconnect closes the client connection
func (c *Client) Disconnect() {
	if c.IsConnected {
		c.Conn.Close()
		c.IsConnected = false
	}
}

// ProcessWill publishes the client's will message if one is set
func (c *Client) ProcessWill() bool {
	// This will be called when a connection is closed unexpectedly
	if c.WillTopic != "" && len(c.WillMessage) > 0 {
		log.Printf("Processing will message for client %s on topic %s", c.ID, c.WillTopic)
		return true
	}
	return false
}

// AddTopicAlias adds a topic alias for MQTT v5.0
func (c *Client) AddTopicAlias(alias int, topic string) {
	if c.ProtocolVersion != MQTT_5_0 {
		return // Only for MQTT v5.0
	}

	c.topicAliasesMutex.Lock()
	defer c.topicAliasesMutex.Unlock()

	c.TopicAliases[alias] = topic
}

// ResolveTopicAlias gets the topic name for an alias
func (c *Client) ResolveTopicAlias(alias int) (string, bool) {
	if c.ProtocolVersion != MQTT_5_0 {
		return "", false // Only for MQTT v5.0
	}

	c.topicAliasesMutex.RLock()
	defer c.topicAliasesMutex.RUnlock()

	topic, exists := c.TopicAliases[alias]
	return topic, exists
}
