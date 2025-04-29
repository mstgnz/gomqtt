package mqtt

import (
	"time"
)

// Subscription represents a client's subscription to a topic
type Subscription struct {
	// Topic information
	Topic string
	QoS   byte

	// Client identification
	ClientID string

	// Subscription metadata
	Created time.Time

	// Optional filter function for advanced message filtering
	Filter func([]byte) bool
}

// NewSubscription creates a new subscription
func NewSubscription(topic string, qos byte, clientID string) *Subscription {
	return &Subscription{
		Topic:    topic,
		QoS:      qos,
		ClientID: clientID,
		Created:  time.Now(),
	}
}

// MatchesTopic determines if this subscription matches the given topic
// Implementation of MQTT topic matching with wildcards
func (s *Subscription) MatchesTopic(publishTopic string) bool {
	// TODO: Implement proper MQTT topic matching with + and # wildcards
	return s.Topic == publishTopic
}
