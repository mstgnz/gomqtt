package storage

import (
	"context"
	"fmt"
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

// ClientInfo stores information about MQTT clients
type ClientInfo struct {
	ClientID    string
	Username    string
	LastSeen    time.Time
	Connected   bool
	ConnectTime time.Time
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

	// Create messages table
	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS messages (
			id SERIAL PRIMARY KEY,
			topic TEXT NOT NULL,
			payload BYTEA,
			qos SMALLINT NOT NULL,
			retained BOOLEAN NOT NULL,
			client_id TEXT,
			timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create messages table: %w", err)
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

	return tx.Commit(ctx)
}

// StoreMessage stores an MQTT message
func (s *PostgresStorage) StoreMessage(msg *Message) error {
	_, err := s.pool.Exec(context.Background(), `
		INSERT INTO messages (topic, payload, qos, retained, client_id, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, msg.Topic, msg.Payload, msg.QoS, msg.Retained, msg.ClientID, time.Now())

	return err
}

// GetClientInfo retrieves client information
func (s *PostgresStorage) GetClientInfo(clientID string) (*ClientInfo, error) {
	row := s.pool.QueryRow(context.Background(), `
		SELECT client_id, username, last_seen, connected, connect_time
		FROM clients
		WHERE client_id = $1
	`, clientID)

	var client ClientInfo
	err := row.Scan(
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

	if connected {
		_, err := s.pool.Exec(context.Background(), `
			INSERT INTO clients (client_id, username, last_seen, connected, connect_time)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (client_id) 
			DO UPDATE SET 
				username = EXCLUDED.username,
				last_seen = EXCLUDED.last_seen,
				connected = EXCLUDED.connected,
				connect_time = EXCLUDED.connect_time
		`, clientID, username, now, connected, now)
		return err
	} else {
		_, err := s.pool.Exec(context.Background(), `
			UPDATE clients 
			SET last_seen = $1, connected = $2
			WHERE client_id = $3
		`, now, connected, clientID)
		return err
	}
}
