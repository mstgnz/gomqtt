package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLStorage implements the storage interface for MySQL
type MySQLStorage struct {
	db            *sql.DB
	cleanupCtx    context.Context
	cleanupCancel context.CancelFunc
}

// NewMySQLStorage creates a new MySQL storage instance
func NewMySQLStorage(connString string) (*MySQLStorage, error) {
	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, fmt.Errorf("unable to open database connection: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())

	storage := &MySQLStorage{
		db:            db,
		cleanupCtx:    ctx,
		cleanupCancel: cancel,
	}

	// Initialize database schema
	if err := storage.initSchema(); err != nil {
		return nil, err
	}

	return storage, nil
}

// Close closes the database connection
func (s *MySQLStorage) Close() {
	if s.cleanupCancel != nil {
		s.cleanupCancel()
	}
	if s.db != nil {
		s.db.Close()
	}
}

// initSchema creates the required tables if they don't exist
func (s *MySQLStorage) initSchema() error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create messages table with MySQL-specific syntax
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS messages (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			topic VARCHAR(255) NOT NULL,
			payload BLOB,
			qos TINYINT NOT NULL,
			retained BOOLEAN NOT NULL,
			client_id VARCHAR(255),
			timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP NULL
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create messages table: %w", err)
	}

	// Create indexes to improve query performance
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages (topic)`)
	if err != nil {
		return fmt.Errorf("failed to create topic index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp)`)
	if err != nil {
		return fmt.Errorf("failed to create timestamp index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_messages_client_id ON messages (client_id)`)
	if err != nil {
		return fmt.Errorf("failed to create client_id index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_messages_expiration ON messages (expires_at)`)
	if err != nil {
		return fmt.Errorf("failed to create expiration index: %w", err)
	}

	// Create clients table
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS clients (
			client_id VARCHAR(255) PRIMARY KEY,
			username VARCHAR(255),
			last_seen TIMESTAMP NULL,
			connected BOOLEAN DEFAULT FALSE,
			connect_time TIMESTAMP NULL
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create clients table: %w", err)
	}

	// Create subscriptions table
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS subscriptions (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			client_id VARCHAR(255) NOT NULL,
			topic VARCHAR(255) NOT NULL,
			qos TINYINT NOT NULL,
			created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE KEY (client_id, topic)
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create subscriptions table: %w", err)
	}

	// Create permissions table
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS permissions (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			username VARCHAR(255) NOT NULL,
			topic_pattern VARCHAR(255) NOT NULL,
			access_level INT NOT NULL,
			created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE KEY (username, topic_pattern)
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create permissions table: %w", err)
	}

	// Create users table
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			username VARCHAR(255) PRIMARY KEY,
			password VARCHAR(255) NOT NULL,
			is_admin BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			last_login TIMESTAMP NULL
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Create api_keys table
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS api_keys (
			key_value VARCHAR(255) PRIMARY KEY,
			username VARCHAR(255) NOT NULL,
			description TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP NULL,
			last_used TIMESTAMP NULL,
			FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create api_keys table: %w", err)
	}

	// Create roles table
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS roles (
			name VARCHAR(255) PRIMARY KEY,
			description TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create roles table: %w", err)
	}

	// Create user_roles table (many-to-many relationship)
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS user_roles (
			username VARCHAR(255) NOT NULL,
			role_name VARCHAR(255) NOT NULL,
			PRIMARY KEY (username, role_name),
			FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE,
			FOREIGN KEY (role_name) REFERENCES roles(name) ON DELETE CASCADE
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create user_roles table: %w", err)
	}

	// Create role_permissions table
	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS role_permissions (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			role_name VARCHAR(255) NOT NULL,
			topic_pattern VARCHAR(255) NOT NULL,
			access_level INT NOT NULL,
			UNIQUE KEY (role_name, topic_pattern),
			FOREIGN KEY (role_name) REFERENCES roles(name) ON DELETE CASCADE
		) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
	`)
	if err != nil {
		return fmt.Errorf("failed to create role_permissions table: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema changes: %w", err)
	}

	return nil
}

// StoreMessage stores an MQTT message with optional expiration
func (s *MySQLStorage) StoreMessage(msg *Message, retention time.Duration) error {
	// If message doesn't have a timestamp, set it to now
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// Calculate expiration time if retention is specified
	var expiresAt *time.Time
	if retention > 0 {
		exp := msg.Timestamp.Add(retention)
		expiresAt = &exp
	}

	_, err := s.db.ExecContext(
		context.Background(),
		`INSERT INTO messages (topic, payload, qos, retained, client_id, timestamp, expires_at) 
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msg.Topic, msg.Payload, msg.QoS, msg.Retained, msg.ClientID, msg.Timestamp, expiresAt,
	)
	if err != nil {
		return fmt.Errorf("failed to store message: %w", err)
	}

	return nil
}

// BatchStoreMessages stores multiple MQTT messages in a single transaction
func (s *MySQLStorage) BatchStoreMessages(messages []*Message, retention time.Duration) error {
	if len(messages) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO messages (topic, payload, qos, retained, client_id, timestamp, expires_at) 
         VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		// If message doesn't have a timestamp, set it to now
		if msg.Timestamp.IsZero() {
			msg.Timestamp = time.Now()
		}

		// Calculate expiration time if retention is specified
		var expiresAt *time.Time
		if retention > 0 {
			exp := msg.Timestamp.Add(retention)
			expiresAt = &exp
		}

		_, err := stmt.ExecContext(
			ctx,
			msg.Topic, msg.Payload, msg.QoS, msg.Retained, msg.ClientID, msg.Timestamp, expiresAt,
		)
		if err != nil {
			return fmt.Errorf("failed to store message: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetMessages retrieves messages based on query parameters
func (s *MySQLStorage) GetMessages(query MessageQuery) (*MessagesPage, error) {
	// Set default limit if not provided
	if query.Limit <= 0 {
		query.Limit = 100
	}

	// Build the query conditions
	conditions := []string{}
	args := []interface{}{}

	if query.Topic != "" {
		// Check if topic contains wildcards
		if strings.Contains(query.Topic, "+") || strings.Contains(query.Topic, "#") {
			// FIXME: MySQL doesn't support MQTT-style wildcards directly
			// This is a simplified approach that would need to be improved
			basePattern := strings.ReplaceAll(query.Topic, "+", "%")
			basePattern = strings.ReplaceAll(basePattern, "#", "%")
			conditions = append(conditions, "topic LIKE ?")
			args = append(args, basePattern)
		} else {
			conditions = append(conditions, "topic = ?")
			args = append(args, query.Topic)
		}
	}

	if query.ClientID != "" {
		conditions = append(conditions, "client_id = ?")
		args = append(args, query.ClientID)
	}

	if !query.FromTimestamp.IsZero() {
		conditions = append(conditions, "timestamp >= ?")
		args = append(args, query.FromTimestamp)
	}

	if !query.ToTimestamp.IsZero() {
		conditions = append(conditions, "timestamp <= ?")
		args = append(args, query.ToTimestamp)
	}

	// Build the WHERE clause
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Build the count query
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages %s", whereClause)
	var totalCount int
	err := s.db.QueryRowContext(context.Background(), countQuery, args...).Scan(&totalCount)
	if err != nil {
		return nil, fmt.Errorf("failed to count messages: %w", err)
	}

	// Build the main query with pagination
	query.Limit++ // Fetch one more to check if there are more results
	mainQuery := fmt.Sprintf(`
		SELECT id, topic, payload, qos, retained, client_id, timestamp
		FROM messages
		%s
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?
	`, whereClause)

	// Add limit and offset to args
	queryArgs := append(args, query.Limit, query.Offset)

	rows, err := s.db.QueryContext(context.Background(), mainQuery, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	messages := []Message{}
	for rows.Next() {
		var msg Message
		if err := rows.Scan(
			&msg.ID,
			&msg.Topic,
			&msg.Payload,
			&msg.QoS,
			&msg.Retained,
			&msg.ClientID,
			&msg.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("failed to scan message row: %w", err)
		}
		messages = append(messages, msg)
	}

	// Check if there are more results
	hasMore := false
	nextOffset := query.Offset
	if len(messages) > query.Limit-1 {
		hasMore = true
		nextOffset = query.Offset + query.Limit - 1
		messages = messages[:query.Limit-1] // Remove the extra message we fetched
	} else if len(messages) > 0 {
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
func (s *MySQLStorage) CleanupExpiredMessages() (int, error) {
	ctx := context.Background()
	res, err := s.db.ExecContext(ctx, "DELETE FROM messages WHERE expires_at IS NOT NULL AND expires_at < NOW()")
	if err != nil {
		return 0, fmt.Errorf("failed to clean up expired messages: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return int(affected), nil
}

// StartMessageCleanup starts a background task to clean up expired messages
func (s *MySQLStorage) StartMessageCleanup(interval time.Duration) {
	if interval <= 0 {
		interval = 1 * time.Hour // Default to hourly cleanup
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				count, err := s.CleanupExpiredMessages()
				if err != nil {
					log.Printf("Error cleaning up expired messages: %v", err)
				} else if count > 0 {
					log.Printf("Cleaned up %d expired messages", count)
				}
			case <-s.cleanupCtx.Done():
				return
			}
		}
	}()

	log.Printf("Message cleanup service started with interval: %s", interval)
}

// GetClientInfo retrieves client information for a specific client ID
func (s *MySQLStorage) GetClientInfo(clientID string) (*ClientInfo, error) {
	var info ClientInfo
	var lastSeen, connectTime sql.NullTime

	err := s.db.QueryRowContext(
		context.Background(),
		"SELECT client_id, username, last_seen, connected, connect_time FROM clients WHERE client_id = ?",
		clientID,
	).Scan(&info.ClientID, &info.Username, &lastSeen, &info.Connected, &connectTime)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("client not found: %s", clientID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get client info: %w", err)
	}

	if lastSeen.Valid {
		info.LastSeen = lastSeen.Time
	}
	if connectTime.Valid {
		info.ConnectTime = connectTime.Time
	}

	return &info, nil
}

// UpdateClientConnection updates client connection status
func (s *MySQLStorage) UpdateClientConnection(clientID, username string, connected bool) error {
	ctx := context.Background()
	now := time.Now()

	// Check if client exists
	var exists bool
	err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM clients WHERE client_id = ?)", clientID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check client existence: %w", err)
	}

	if exists {
		// Update existing client
		if connected {
			_, err = s.db.ExecContext(ctx,
				"UPDATE clients SET username = ?, last_seen = ?, connected = ?, connect_time = ? WHERE client_id = ?",
				username, now, connected, now, clientID)
		} else {
			_, err = s.db.ExecContext(ctx,
				"UPDATE clients SET username = ?, last_seen = ?, connected = ? WHERE client_id = ?",
				username, now, connected, clientID)
		}
	} else {
		// Insert new client
		var connectTimeValue interface{} = nil
		if connected {
			connectTimeValue = now
		}
		_, err = s.db.ExecContext(ctx,
			"INSERT INTO clients (client_id, username, last_seen, connected, connect_time) VALUES (?, ?, ?, ?, ?)",
			clientID, username, now, connected, connectTimeValue)
	}

	if err != nil {
		return fmt.Errorf("failed to update client connection: %w", err)
	}

	return nil
}

// GetAllPermissions retrieves all permission entries
func (s *MySQLStorage) GetAllPermissions() ([]Permission, error) {
	rows, err := s.db.QueryContext(
		context.Background(),
		"SELECT username, topic_pattern, access_level FROM permissions ORDER BY username, topic_pattern",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query permissions: %w", err)
	}
	defer rows.Close()

	permissions := []Permission{}
	for rows.Next() {
		var perm Permission
		var accessLevel int
		if err := rows.Scan(&perm.Username, &perm.TopicPattern, &accessLevel); err != nil {
			return nil, fmt.Errorf("failed to scan permission row: %w", err)
		}

		// Convert access level to string representation
		switch accessLevel {
		case 0:
			perm.AccessLevel = "read-only"
		case 1:
			perm.AccessLevel = "read-write"
		case 2:
			perm.AccessLevel = "admin"
		default:
			perm.AccessLevel = fmt.Sprintf("unknown(%d)", accessLevel)
		}

		permissions = append(permissions, perm)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating permission rows: %w", err)
	}

	return permissions, nil
}

// StorePermission stores a permission entry
func (s *MySQLStorage) StorePermission(username, topicPattern string, accessLevel int) error {
	_, err := s.db.ExecContext(
		context.Background(),
		`INSERT INTO permissions (username, topic_pattern, access_level) 
         VALUES (?, ?, ?) 
         ON DUPLICATE KEY UPDATE access_level = ?`,
		username, topicPattern, accessLevel, accessLevel,
	)
	if err != nil {
		return fmt.Errorf("failed to store permission: %w", err)
	}
	return nil
}

// DeletePermission deletes a permission entry
func (s *MySQLStorage) DeletePermission(username, topicPattern string) error {
	_, err := s.db.ExecContext(
		context.Background(),
		"DELETE FROM permissions WHERE username = ? AND topic_pattern = ?",
		username, topicPattern,
	)
	if err != nil {
		return fmt.Errorf("failed to delete permission: %w", err)
	}
	return nil
}

// DeleteRetainedMessage deletes a retained message for a specific topic
func (s *MySQLStorage) DeleteRetainedMessage(topic string) error {
	_, err := s.db.ExecContext(
		context.Background(),
		"DELETE FROM messages WHERE topic = ? AND retained = TRUE",
		topic,
	)
	if err != nil {
		return fmt.Errorf("failed to delete retained message: %w", err)
	}
	return nil
}

// GetRetainedMessages retrieves retained messages for a specific topic pattern
func (s *MySQLStorage) GetRetainedMessages(topicPattern string) ([]Message, error) {
	var rows *sql.Rows
	var err error

	// Check if topic pattern contains wildcards
	if strings.Contains(topicPattern, "+") || strings.Contains(topicPattern, "#") {
		// Convert MQTT wildcards to SQL LIKE pattern (simplified)
		sqlPattern := strings.ReplaceAll(topicPattern, "+", "%")
		sqlPattern = strings.ReplaceAll(sqlPattern, "#", "%")

		rows, err = s.db.QueryContext(
			context.Background(),
			`SELECT id, topic, payload, qos, retained, client_id, timestamp
             FROM messages 
             WHERE retained = TRUE AND topic LIKE ?
             ORDER BY timestamp DESC`,
			sqlPattern,
		)
	} else {
		// Exact topic match
		rows, err = s.db.QueryContext(
			context.Background(),
			`SELECT id, topic, payload, qos, retained, client_id, timestamp
             FROM messages 
             WHERE retained = TRUE AND topic = ?
             ORDER BY timestamp DESC`,
			topicPattern,
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query retained messages: %w", err)
	}
	defer rows.Close()

	messages := []Message{}
	for rows.Next() {
		var msg Message
		if err := rows.Scan(
			&msg.ID,
			&msg.Topic,
			&msg.Payload,
			&msg.QoS,
			&msg.Retained,
			&msg.ClientID,
			&msg.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("failed to scan retained message row: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating retained message rows: %w", err)
	}

	return messages, nil
}
