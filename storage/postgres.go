package storage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStorage implements the storage interface for PostgreSQL
type PostgresStorage struct {
	pool *pgxpool.Pool
}

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

// NewPostgresStorage creates a new PostgreSQL storage instance
func NewPostgresStorage(connString string) (*PostgresStorage, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	storage := &PostgresStorage{
		pool: pool,
	}

	// Initialize database schema
	if err := storage.initSchema(); err != nil {
		return nil, err
	}

	return storage, nil
}

// Close closes the database connection
func (s *PostgresStorage) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// initSchema creates the required tables if they don't exist
func (s *PostgresStorage) initSchema() error {
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create messages table with improved schema
	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS messages (
			id SERIAL PRIMARY KEY,
			topic TEXT NOT NULL,
			payload BYTEA,
			qos SMALLINT NOT NULL,
			retained BOOLEAN NOT NULL,
			client_id TEXT,
			timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			expires_at TIMESTAMP WITH TIME ZONE
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create messages table: %w", err)
	}

	// Create indexes to improve query performance
	_, err = tx.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages (topic);
		CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp);
		CREATE INDEX IF NOT EXISTS idx_messages_client_id ON messages (client_id);
		CREATE INDEX IF NOT EXISTS idx_messages_retention ON messages (expires_at) 
		    WHERE expires_at IS NOT NULL;
	`)
	if err != nil {
		return fmt.Errorf("failed to create message indexes: %w", err)
	}

	// Create clients table
	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS clients (
			client_id TEXT PRIMARY KEY,
			username TEXT,
			last_seen TIMESTAMP WITH TIME ZONE,
			connected BOOLEAN DEFAULT FALSE,
			connect_time TIMESTAMP WITH TIME ZONE
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create clients table: %w", err)
	}

	// Create subscriptions table
	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS subscriptions (
			id SERIAL PRIMARY KEY,
			client_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			qos SMALLINT NOT NULL,
			created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			UNIQUE(client_id, topic)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create subscriptions table: %w", err)
	}

	// Create permissions table
	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS permissions (
			id SERIAL PRIMARY KEY,
			username TEXT NOT NULL,
			topic_pattern TEXT NOT NULL,
			access_level INTEGER NOT NULL,
			created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			UNIQUE(username, topic_pattern)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create permissions table: %w", err)
	}

	return tx.Commit(ctx)
}

// StoreMessage stores an MQTT message with optional expiration
func (s *PostgresStorage) StoreMessage(msg *Message, retention time.Duration) error {
	var expiresAt *time.Time

	// If retention is specified, calculate expiration time
	if retention > 0 {
		expTime := time.Now().Add(retention)
		expiresAt = &expTime
	}

	_, err := s.pool.Exec(context.Background(), `
		INSERT INTO messages (topic, payload, qos, retained, client_id, timestamp, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, msg.Topic, msg.Payload, msg.QoS, msg.Retained, msg.ClientID, time.Now(), expiresAt)

	return err
}

// BatchStoreMessages stores multiple MQTT messages in a single transaction
func (s *PostgresStorage) BatchStoreMessages(messages []*Message, retention time.Duration) error {
	if len(messages) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var expiresAt *time.Time
	if retention > 0 {
		expTime := time.Now().Add(retention)
		expiresAt = &expTime
	}

	// Create a batch for efficient insertion
	batch := &pgx.Batch{}
	for _, msg := range messages {
		batch.Queue(`
			INSERT INTO messages (topic, payload, qos, retained, client_id, timestamp, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, msg.Topic, msg.Payload, msg.QoS, msg.Retained, msg.ClientID, time.Now(), expiresAt)
	}

	// Execute the batch
	batchResults := tx.SendBatch(ctx, batch)
	defer batchResults.Close()

	// Check for errors in the batch results
	for i := 0; i < batch.Len(); i++ {
		_, err := batchResults.Exec()
		if err != nil {
			return fmt.Errorf("error in batch insert at position %d: %w", i, err)
		}
	}

	return tx.Commit(ctx)
}

// GetMessages retrieves messages based on query parameters
func (s *PostgresStorage) GetMessages(query MessageQuery) (*MessagesPage, error) {
	// Build the query
	baseQuery := `
		SELECT id, topic, payload, qos, retained, client_id, timestamp
		FROM messages
		WHERE 1=1
	`

	countQuery := `
		SELECT COUNT(*)
		FROM messages
		WHERE 1=1
	`

	var conditions []string
	var params []any
	paramIndex := 1

	// Add filters
	if query.Topic != "" {
		// Support for topic patterns with wildcards
		if strings.Contains(query.Topic, "#") || strings.Contains(query.Topic, "+") {
			// This is a simplified wildcard replacement - a more complex implementation
			// would handle MQTT wildcard semantics properly
			topicPattern := strings.ReplaceAll(query.Topic, "#", "%")
			topicPattern = strings.ReplaceAll(topicPattern, "+", "%")

			conditions = append(conditions, fmt.Sprintf("topic LIKE $%d", paramIndex))
			params = append(params, topicPattern)
		} else {
			conditions = append(conditions, fmt.Sprintf("topic = $%d", paramIndex))
			params = append(params, query.Topic)
		}
		paramIndex++
	}

	if query.ClientID != "" {
		conditions = append(conditions, fmt.Sprintf("client_id = $%d", paramIndex))
		params = append(params, query.ClientID)
		paramIndex++
	}

	if !query.FromTimestamp.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", paramIndex))
		params = append(params, query.FromTimestamp)
		paramIndex++
	}

	if !query.ToTimestamp.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", paramIndex))
		params = append(params, query.ToTimestamp)
		paramIndex++
	}

	// Only include non-expired messages
	conditions = append(conditions, "(expires_at IS NULL OR expires_at > NOW())")

	// Add conditions to both queries
	for _, condition := range conditions {
		baseQuery += " AND " + condition
		countQuery += " AND " + condition
	}

	// Add pagination and ordering
	baseQuery += " ORDER BY timestamp DESC"

	// Set default limit if not specified
	limit := 100
	if query.Limit > 0 {
		limit = query.Limit
	}

	baseQuery += fmt.Sprintf(" LIMIT $%d", paramIndex)
	params = append(params, limit)
	paramIndex++

	if query.Offset > 0 {
		baseQuery += fmt.Sprintf(" OFFSET $%d", paramIndex)
		params = append(params, query.Offset)
	}

	// Query for count first
	var totalCount int
	err := s.pool.QueryRow(context.Background(), countQuery, params[:len(params)-1]...).Scan(&totalCount)
	if err != nil {
		return nil, fmt.Errorf("failed to count messages: %w", err)
	}

	// Execute the main query
	rows, err := s.pool.Query(context.Background(), baseQuery, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		err := rows.Scan(
			&msg.ID,
			&msg.Topic,
			&msg.Payload,
			&msg.QoS,
			&msg.Retained,
			&msg.ClientID,
			&msg.Timestamp,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan message row: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating message rows: %w", err)
	}

	// Calculate if there are more results and the next offset
	hasMore := totalCount > query.Offset+len(messages)
	nextOffset := 0
	if hasMore {
		nextOffset = query.Offset + len(messages)
	}

	return &MessagesPage{
		Messages:   messages,
		TotalCount: totalCount,
		HasMore:    hasMore,
		NextOffset: nextOffset,
	}, nil
}

// CleanupExpiredMessages removes expired messages from the database
func (s *PostgresStorage) CleanupExpiredMessages() (int, error) {
	result, err := s.pool.Exec(context.Background(), `
		DELETE FROM messages 
		WHERE expires_at < $1
	`, time.Now())
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup expired messages: %w", err)
	}

	count := result.RowsAffected()
	return int(count), nil
}

// StartMessageCleanup starts a goroutine that periodically cleans up expired messages
func (s *PostgresStorage) StartMessageCleanup(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			count, err := s.CleanupExpiredMessages()
			if err != nil {
				log.Printf("Error cleaning up expired messages: %v", err)
			} else if count > 0 {
				log.Printf("Cleaned up %d expired messages", count)
			}
		}
	}()

	log.Printf("Started message cleanup scheduler with interval %s", interval)
}

// GetClientInfo retrieves client information
func (s *PostgresStorage) GetClientInfo(clientID string) (*ClientInfo, error) {
	var client ClientInfo
	err := s.pool.QueryRow(context.Background(), `
		SELECT client_id, username, last_seen, connected, connect_time
		FROM clients
		WHERE client_id = $1
	`, clientID).Scan(
		&client.ClientID,
		&client.Username,
		&client.LastSeen,
		&client.Connected,
		&client.ConnectTime,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &client, nil
}

// UpdateClientConnection updates a client's connection status
func (s *PostgresStorage) UpdateClientConnection(clientID, username string, connected bool) error {
	now := time.Now()

	// First try to update the existing client
	_, err := s.pool.Exec(context.Background(), `
		UPDATE clients 
		SET username = $1, connected = $2, last_seen = $3
		WHERE client_id = $4
	`, username, connected, now, clientID)

	// If it's a new client and needs to be connected, insert it
	if err == nil && connected {
		_, err = s.pool.Exec(context.Background(), `
			INSERT INTO clients (client_id, username, last_seen, connected, connect_time)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (client_id) DO NOTHING
		`, clientID, username, now, connected, now)
	}

	return err
}

// GetAllPermissions retrieves all permissions from the database
func (s *PostgresStorage) GetAllPermissions() ([]Permission, error) {
	rows, err := s.pool.Query(context.Background(), `
		SELECT username, topic_pattern, access_level
		FROM permissions
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query permissions: %w", err)
	}
	defer rows.Close()

	var permissions []Permission
	for rows.Next() {
		var perm Permission
		var accessLevelInt int
		err := rows.Scan(&perm.Username, &perm.TopicPattern, &accessLevelInt)
		if err != nil {
			return nil, err
		}

		// Convert access level integer to string
		switch accessLevelInt {
		case 0:
			perm.AccessLevel = "read-only"
		case 1:
			perm.AccessLevel = "read-write"
		case 2:
			perm.AccessLevel = "admin"
		default:
			perm.AccessLevel = "unknown"
		}

		permissions = append(permissions, perm)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating permission rows: %w", err)
	}

	return permissions, nil
}

// StorePermission stores a permission in the database
func (s *PostgresStorage) StorePermission(username, topicPattern string, accessLevel int) error {
	_, err := s.pool.Exec(context.Background(), `
		INSERT INTO permissions (username, topic_pattern, access_level)
		VALUES ($1, $2, $3)
		ON CONFLICT (username, topic_pattern) 
		DO UPDATE SET 
			access_level = EXCLUDED.access_level
	`, username, topicPattern, accessLevel)

	return err
}

// DeletePermission removes a permission from the database
func (s *PostgresStorage) DeletePermission(username, topicPattern string) error {
	_, err := s.pool.Exec(context.Background(), `
		DELETE FROM permissions
		WHERE username = $1 AND topic_pattern = $2
	`, username, topicPattern)

	return err
}
