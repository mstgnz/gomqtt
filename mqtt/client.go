package mqtt

import (
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
}

// NewClient creates a new MQTT client
func NewClient(id string, conn net.Conn) *Client {
	return &Client{
		ID:            id,
		Conn:          conn,
		ConnTime:      time.Now(),
		Subscriptions: make(map[string]*Subscription),
		IsConnected:   true,
		LastSeen:      time.Now(),
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
