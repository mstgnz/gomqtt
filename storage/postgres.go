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
	var params []interface{}
	paramIdx := 1

	// Apply filters
	if query.Topic != "" {
		conditions = append(conditions, fmt.Sprintf("topic = $%d", paramIdx))
		params = append(params, query.Topic)
		paramIdx++
	}

	if query.ClientID != "" {
		conditions = append(conditions, fmt.Sprintf("client_id = $%d", paramIdx))
		params = append(params, query.ClientID)
		paramIdx++
	}

	if !query.FromTimestamp.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", paramIdx))
		params = append(params, query.FromTimestamp)
		paramIdx++
	}

	if !query.ToTimestamp.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", paramIdx))
		params = append(params, query.ToTimestamp)
		paramIdx++
	}

	// Add conditions to the queries
	for _, condition := range conditions {
		baseQuery += " AND " + condition
		countQuery += " AND " + condition
	}

	// Add order, limit and offset
	baseQuery += " ORDER BY timestamp DESC"

	// Set default limit if not provided
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}

	baseQuery += fmt.Sprintf(" LIMIT $%d OFFSET $%d", paramIdx, paramIdx+1)
	params = append(params, limit, query.Offset)

	// Query for total count
	ctx := context.Background()
	var totalCount int
	err := s.pool.QueryRow(ctx, countQuery, params[:paramIdx-1]...).Scan(&totalCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get message count: %w", err)
	}

	// Execute the main query
	rows, err := s.pool.Query(ctx, baseQuery, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	// Process the result
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
			return nil, fmt.Errorf("failed to scan message: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through result rows: %w", err)
	}

	// Check if there are more results
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

// CleanupExpiredMessages removes expired messages from storage
func (s *PostgresStorage) CleanupExpiredMessages() (int, error) {
	ctx := context.Background()
	result, err := s.pool.Exec(ctx, `
		DELETE FROM messages
		WHERE expires_at < NOW()
	`)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup expired messages: %w", err)
	}

	count := result.RowsAffected()
	return int(count), nil
}

// StartMessageCleanup starts a background task to clean up expired messages
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
}

// GetClientInfo retrieves client information for a specific client ID
func (s *PostgresStorage) GetClientInfo(clientID string) (*ClientInfo, error) {
	ctx := context.Background()
	var client ClientInfo

	err := s.pool.QueryRow(ctx, `
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

	if err == pgx.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to get client info: %w", err)
	}

	return &client, nil
}

// UpdateClientConnection updates client connection status
func (s *PostgresStorage) UpdateClientConnection(clientID, username string, connected bool) error {
	ctx := context.Background()
	now := time.Now()

	if connected {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO clients (client_id, username, last_seen, connected, connect_time)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (client_id) DO UPDATE
			SET username = $2, last_seen = $3, connected = $4, connect_time = $5
		`, clientID, username, now, connected, now)
		return err
	} else {
		_, err := s.pool.Exec(ctx, `
			UPDATE clients
			SET last_seen = $2, connected = $3
			WHERE client_id = $1
		`, clientID, now, connected)
		return err
	}
}

// GetAllPermissions retrieves all permission entries
func (s *PostgresStorage) GetAllPermissions() ([]Permission, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx, `
		SELECT username, topic_pattern, access_level
		FROM permissions
		ORDER BY username, topic_pattern
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query permissions: %w", err)
	}
	defer rows.Close()

	var permissions []Permission
	for rows.Next() {
		var perm Permission
		var accessLevel int

		err := rows.Scan(&perm.Username, &perm.TopicPattern, &accessLevel)
		if err != nil {
			return nil, fmt.Errorf("failed to scan permission: %w", err)
		}

		// Convert access level to string
		perm.AccessLevel = fmt.Sprintf("%d", accessLevel)
		permissions = append(permissions, perm)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through permissions: %w", err)
	}

	return permissions, nil
}

// StorePermission stores a permission entry
func (s *PostgresStorage) StorePermission(username, topicPattern string, accessLevel int) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO permissions (username, topic_pattern, access_level)
		VALUES ($1, $2, $3)
		ON CONFLICT (username, topic_pattern) DO UPDATE
		SET access_level = $3
	`, username, topicPattern, accessLevel)
	return err
}

// DeletePermission deletes a permission entry
func (s *PostgresStorage) DeletePermission(username, topicPattern string) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		DELETE FROM permissions
		WHERE username = $1 AND topic_pattern = $2
	`, username, topicPattern)
	return err
}

// DeleteRetainedMessage deletes a retained message for a specific topic
func (s *PostgresStorage) DeleteRetainedMessage(topic string) error {
	_, err := s.pool.Exec(context.Background(), `
		DELETE FROM messages
		WHERE topic = $1 AND retained = true
	`, topic)
	return err
}

// GetRetainedMessages retrieves retained messages for a specific topic pattern
func (s *PostgresStorage) GetRetainedMessages(topicPattern string) ([]Message, error) {
	ctx := context.Background()
	var messages []Message

	query := `
		SELECT id, topic, payload, qos, retained, client_id, timestamp
		FROM messages
		WHERE retained = true
	`

	params := []interface{}{}

	// If a specific topic pattern is provided, filter by it
	if topicPattern != "" && topicPattern != "#" {
		// Convert MQTT wildcards to SQL LIKE pattern
		// # multi-level wildcard becomes %
		// + single-level wildcard becomes a SQL pattern for single level
		sqlPattern := strings.ReplaceAll(topicPattern, "#", "%")

		// Replace + with a pattern that matches a single level
		// This is a simplified version - for production code you'd want a more robust conversion
		sqlPattern = strings.ReplaceAll(sqlPattern, "+", "[^/]*")

		query += " AND topic LIKE $1"
		params = append(params, sqlPattern)
	}

	// Execute the query
	rows, err := s.pool.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query retained messages: %w", err)
	}
	defer rows.Close()

	// Process the result rows
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
			return nil, fmt.Errorf("failed to scan retained message: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through result rows: %w", err)
	}

	return messages, nil
}
