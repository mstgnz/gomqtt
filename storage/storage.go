package storage

import (
	"encoding/json"
	"time"
)

// Message represents a stored MQTT message
type Message struct {
	ID        int64
	Topic     string
	Payload   []byte
	QoS       byte
	Retained  bool
	ClientID  string
	Timestamp time.Time
}

// MessageQuery represents query parameters for filtering messages
type MessageQuery struct {
	Topic         string
	ClientID      string
	FromTimestamp time.Time
	ToTimestamp   time.Time
	Limit         int
	Offset        int
}

// ClientInfo stores information about MQTT clients
type ClientInfo struct {
	ClientID    string
	Username    string
	LastSeen    time.Time
	Connected   bool
	ConnectTime time.Time
}

// Permission represents a permission for API responses
type Permission struct {
	Username     string `json:"username"`
	TopicPattern string `json:"topic_pattern"`
	AccessLevel  string `json:"access_level"`
}

// MessagesPage represents a paginated result of messages
type MessagesPage struct {
	Messages   []Message `json:"messages"`
	TotalCount int       `json:"total_count"`
	HasMore    bool      `json:"has_more"`
	NextOffset int       `json:"next_offset,omitempty"`
}

// AuditLog represents an entry in the audit log
type AuditLog struct {
	ID         int64
	ActionType string
	Username   string
	ClientID   string
	EntityType string
	EntityID   string
	Details    json.RawMessage
	IPAddress  string
	Timestamp  time.Time
}

// AuditLogQuery represents query parameters for filtering audit logs
type AuditLogQuery struct {
	ActionType    string
	Username      string
	EntityType    string
	EntityID      string
	FromTimestamp time.Time
	ToTimestamp   time.Time
	Limit         int
	Offset        int
}

// AuditLogPage represents a paginated result of audit logs
type AuditLogPage struct {
	Logs       []AuditLog `json:"logs"`
	TotalCount int        `json:"total_count"`
	HasMore    bool       `json:"has_more"`
	NextOffset int        `json:"next_offset,omitempty"`
}

// Storage defines the interface for storage implementations
type Storage interface {
	// Close closes the storage connection
	Close()

	// StoreMessage stores an MQTT message with optional expiration
	StoreMessage(msg *Message, retention time.Duration) error

	// BatchStoreMessages stores multiple MQTT messages in a single transaction
	BatchStoreMessages(messages []*Message, retention time.Duration) error

	// GetMessages retrieves messages based on query parameters
	GetMessages(query MessageQuery) (*MessagesPage, error)

	// GetRetainedMessages retrieves retained messages for a specific topic pattern
	GetRetainedMessages(topicPattern string) ([]Message, error)

	// DeleteRetainedMessage deletes a retained message for a specific topic
	DeleteRetainedMessage(topic string) error

	// CleanupExpiredMessages removes expired messages from storage
	CleanupExpiredMessages() (int, error)

	// StartMessageCleanup starts a background task to clean up expired messages
	StartMessageCleanup(interval time.Duration)

	// GetClientInfo retrieves client information for a specific client ID
	GetClientInfo(clientID string) (*ClientInfo, error)

	// UpdateClientConnection updates client connection status
	UpdateClientConnection(clientID, username string, connected bool) error

	// GetAllPermissions retrieves all permission entries
	GetAllPermissions() ([]Permission, error)

	// StorePermission stores a permission entry
	StorePermission(username, topicPattern string, accessLevel int) error

	// DeletePermission deletes a permission entry
	DeletePermission(username, topicPattern string) error

	// LogAction logs an action to the audit log
	LogAction(log *AuditLog) error

	// GetAuditLogs retrieves audit logs based on query parameters
	GetAuditLogs(query AuditLogQuery) (*AuditLogPage, error)
}
