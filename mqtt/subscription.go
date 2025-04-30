package mqtt

import (
	"strings"
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

	// MQTT v5.0 specific fields
	NoLocal           bool // Don't receive messages published by this client
	RetainAsPublished bool // Keep RETAIN flag on forwarded messages
	RetainHandling    byte // Handling of retained messages (0=send, 1=send if new, 2=don't send)
	SubscriptionID    int  // Identifier for the subscription

	// Shared subscription fields
	IsShared   bool   // Whether this is a shared subscription
	ShareGroup string // The share group name (if shared subscription)
}

// NewSubscription creates a new subscription
func NewSubscription(topic string, qos byte, clientID string) *Subscription {
	// Check if this is a shared subscription
	isShared := false
	shareGroup := ""

	if strings.HasPrefix(topic, "$share/") {
		parts := strings.SplitN(topic, "/", 3)
		if len(parts) >= 3 {
			isShared = true
			shareGroup = parts[1]
			// Don't modify the topic here since we need to keep it for subscription indexing
		}
	}

	return &Subscription{
		Topic:             topic,
		QoS:               qos,
		ClientID:          clientID,
		Created:           time.Now(),
		NoLocal:           false,
		RetainAsPublished: false,
		RetainHandling:    0,
		SubscriptionID:    0,
		IsShared:          isShared,
		ShareGroup:        shareGroup,
	}
}

// MatchesTopic determines if this subscription matches the given topic
// Implementation of MQTT topic matching with wildcards
func (s *Subscription) MatchesTopic(publishTopic string) bool {
	// Split into parts
	topicParts := strings.Split(publishTopic, "/")
	patternParts := strings.Split(s.Topic, "/")

	return matchTopicParts(topicParts, patternParts)
}

// matchTopicParts implements MQTT topic matching with + and # wildcards
func matchTopicParts(topic, pattern []string) bool {
	// Handle shared subscriptions
	if len(pattern) > 0 && strings.HasPrefix(pattern[0], "$share") {
		if len(pattern) < 3 {
			return false // Invalid shared subscription format
		}
		// Skip the $share and group name components
		pattern = pattern[2:]
	}

	i := 0
	for i < len(pattern) {
		// # wildcard - matches all remaining levels
		if pattern[i] == "#" {
			// # must be the last character
			return i == len(pattern)-1
		}

		// No more topic parts to match but still have pattern parts
		if i >= len(topic) {
			return false
		}

		// + wildcard - matches exactly one level
		if pattern[i] == "+" {
			// Continue to next level
			i++
			continue
		}

		// Exact match required at this level
		if pattern[i] != topic[i] {
			return false
		}

		i++
	}

	// Make sure we've consumed all topic parts
	return i == len(topic)
}
