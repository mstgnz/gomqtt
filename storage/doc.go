/*
Package storage provides interfaces and implementations for message persistence in GoMQTT.

This package defines a Storage interface that abstracts various storage backends and provides
concrete implementations for:

  - PostgreSQL: Robust relational database storage with SQL queries
  - Redis: In-memory data store with persistence for high-performance scenarios

The storage system handles:

  - Message persistence for QoS 1 and QoS 2 delivery
  - Retained message storage
  - Session state persistence
  - Client connection tracking
  - Permission storage for access control

# Storage Interface

The Storage interface defines methods that must be implemented by any storage backend:

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

	    // Additional methods for client and permission management...
	}

# PostgreSQL Storage

The PostgreSQL implementation provides:

  - Efficient SQL queries for message storage and retrieval
  - Transaction support for batch operations
  - Indexes for fast topic pattern matching
  - Automatic cleanup of expired messages
  - Schema migration support

# Redis Storage

The Redis implementation provides:

  - High-performance in-memory storage with persistence
  - Pub/Sub capabilities for message distribution
  - Key expiration for automatic message cleanup
  - Atomic operations for distributed environments
  - Pipeline support for batch operations

# Message Retention

Both storage implementations support message retention policies:

  - Time-based expiration for messages
  - Automatic cleanup of expired messages
  - Configurable cleanup intervals
  - Size-based limits for retained messages

# Examples

Using PostgreSQL storage:

	// Connect to PostgreSQL
	store, err := storage.NewPostgresStorage("postgres://user:password@localhost/mqtt?sslmode=disable")
	if err != nil {
	    log.Fatalf("Failed to connect to database: %v", err)
	}
	defer store.Close()

	// Configure message cleanup
	store.StartMessageCleanup(24 * time.Hour)

	// Store a message
	msg := &storage.Message{
	    Topic:     "sensors/temperature",
	    Payload:   []byte("23.5"),
	    QoS:       1,
	    Retained:  true,
	    ClientID:  "device1",
	    Timestamp: time.Now(),
	}

	if err := store.StoreMessage(msg, 48 * time.Hour); err != nil {
	    log.Printf("Failed to store message: %v", err)
	}

Using Redis storage:

	// Connect to Redis
	store, err := storage.NewRedisStorage("redis://localhost:6379/0", "mqtt:")
	if err != nil {
	    log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer store.Close()

	// Query messages
	query := storage.MessageQuery{
	    Topic:     "sensors/#",
	    FromTimestamp: time.Now().Add(-24 * time.Hour),
	    Limit:     100,
	}

	messages, err := store.GetMessages(query)
	if err != nil {
	    log.Printf("Failed to query messages: %v", err)
	}
*/
package storage
