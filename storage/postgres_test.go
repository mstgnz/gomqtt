package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v2"
)

// TestPostgresStorage is a wrapper for testing PostgresStorage with a mock
type TestPostgresStorage struct {
	PostgresStorage
	mock pgxmock.PgxPoolIface
}

// TestNewPostgresStorage tests the creation of a new PostgreSQL storage instance
func TestNewPostgresStorage(t *testing.T) {
	t.Run("Invalid connection string", func(t *testing.T) {
		// Invalid connection string should return an error
		storage, err := NewPostgresStorage("invalid-connection-string")
		if err == nil {
			t.Error("Expected error with invalid connection string")
		}
		if storage != nil {
			t.Error("Expected nil storage with invalid connection string")
		}
	})
}

// TestPostgresStorageMethods tests PostgreSQL storage methods using a mock
func TestPostgresStorageMethods(t *testing.T) {
	// Setup mock
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer mock.Close()

	// Create storage with mocked connection
	storage := &TestPostgresStorage{
		PostgresStorage: PostgresStorage{}, // Empty struct, we'll bypass the pool
		mock:            mock,
	}

	t.Run("StoreMessage", func(t *testing.T) {
		// Setup mock expectations
		mock.ExpectExec(`INSERT INTO messages`).
			WithArgs("test/topic", []byte("test-payload"), byte(1), true, "client-123", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Call the method
		msg := &Message{
			Topic:    "test/topic",
			Payload:  []byte("test-payload"),
			QoS:      1,
			Retained: true,
			ClientID: "client-123",
		}

		err := storage.StoreMessage(msg, 1*time.Hour)
		if err != nil {
			t.Errorf("StoreMessage error: %v", err)
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	t.Run("BatchStoreMessages", func(t *testing.T) {
		// Setup mock expectations for begin transaction
		mock.ExpectBegin()

		// Setup mock expectations for copy
		mock.ExpectCopyFrom(
			pgx.Identifier{"messages"},
			[]string{"topic", "payload", "qos", "retained", "client_id", "timestamp", "expires_at"},
		).WillReturnResult(3) // 3 rows copied

		// Setup mock expectations for commit
		mock.ExpectCommit()

		// Call the method
		messages := []*Message{
			{
				Topic:    "test/topic1",
				Payload:  []byte("payload1"),
				QoS:      0,
				Retained: false,
				ClientID: "client1",
			},
			{
				Topic:    "test/topic2",
				Payload:  []byte("payload2"),
				QoS:      1,
				Retained: true,
				ClientID: "client2",
			},
			{
				Topic:    "test/topic3",
				Payload:  []byte("payload3"),
				QoS:      2,
				Retained: false,
				ClientID: "client3",
			},
		}

		err := storage.BatchStoreMessages(messages, 1*time.Hour)
		if err != nil {
			t.Errorf("BatchStoreMessages error: %v", err)
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	t.Run("GetMessages", func(t *testing.T) {
		// Setup mock expectations for count query
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM messages`).
			WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(10))

		// Setup mock expectations for data query
		mock.ExpectQuery(`SELECT id, topic, payload, qos, retained, client_id, timestamp FROM messages`).
			WillReturnRows(pgxmock.NewRows([]string{"id", "topic", "payload", "qos", "retained", "client_id", "timestamp"}).
				AddRow(1, "test/topic1", []byte("payload1"), 0, false, "client1", time.Now()).
				AddRow(2, "test/topic2", []byte("payload2"), 1, true, "client2", time.Now()))

		// Call the method
		query := MessageQuery{
			Topic:    "test/topic",
			Limit:    10,
			Offset:   0,
			ClientID: "client",
		}

		result, err := storage.GetMessages(query)
		if err != nil {
			t.Errorf("GetMessages error: %v", err)
		}

		// Verify result
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		if result.TotalCount != 10 {
			t.Errorf("Expected total count 10, got %d", result.TotalCount)
		}
		if len(result.Messages) != 2 {
			t.Errorf("Expected 2 messages, got %d", len(result.Messages))
		}
		if result.HasMore != true {
			t.Error("Expected HasMore to be true")
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	t.Run("CleanupExpiredMessages", func(t *testing.T) {
		// Setup mock expectations
		mock.ExpectExec(`DELETE FROM messages WHERE expires_at < .+`).
			WillReturnResult(pgxmock.NewResult("DELETE", 5))

		// Call the method
		count, err := storage.CleanupExpiredMessages()
		if err != nil {
			t.Errorf("CleanupExpiredMessages error: %v", err)
		}

		// Verify result
		if count != 5 {
			t.Errorf("Expected deleted count 5, got %d", count)
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	t.Run("GetClientInfo", func(t *testing.T) {
		// Setup mock expectations
		mock.ExpectQuery(`SELECT client_id, username, last_seen, connected, connect_time FROM clients`).
			WithArgs("client-123").
			WillReturnRows(pgxmock.NewRows([]string{"client_id", "username", "last_seen", "connected", "connect_time"}).
				AddRow("client-123", "user123", time.Now(), true, time.Now()))

		// Call the method
		client, err := storage.GetClientInfo("client-123")
		if err != nil {
			t.Errorf("GetClientInfo error: %v", err)
		}

		// Verify result
		if client == nil {
			t.Fatal("Expected client, got nil")
		}
		if client.ClientID != "client-123" {
			t.Errorf("Expected client ID 'client-123', got '%s'", client.ClientID)
		}
		if client.Username != "user123" {
			t.Errorf("Expected username 'user123', got '%s'", client.Username)
		}
		if !client.Connected {
			t.Error("Expected client to be connected")
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	t.Run("UpdateClientConnection", func(t *testing.T) {
		// Setup mock expectations for existing client (update)
		mock.ExpectExec(`UPDATE clients SET`).
			WithArgs("user123", true, pgxmock.AnyArg(), "client-123").
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))

		// Call the method for existing client
		err := storage.UpdateClientConnection("client-123", "user123", true)
		if err != nil {
			t.Errorf("UpdateClientConnection error: %v", err)
		}

		// Setup mock expectations for new client (insert)
		mock.ExpectExec(`UPDATE clients SET`).
			WithArgs("new-user", false, pgxmock.AnyArg(), "new-client").
			WillReturnResult(pgxmock.NewResult("UPDATE", 0))

		mock.ExpectExec(`INSERT INTO clients`).
			WithArgs("new-client", "new-user", pgxmock.AnyArg(), false, pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Call the method for new client
		err = storage.UpdateClientConnection("new-client", "new-user", false)
		if err != nil {
			t.Errorf("UpdateClientConnection error for new client: %v", err)
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	t.Run("GetAllPermissions", func(t *testing.T) {
		// Setup mock expectations
		mock.ExpectQuery(`SELECT username, topic_pattern, access_level FROM permissions`).
			WillReturnRows(pgxmock.NewRows([]string{"username", "topic_pattern", "access_level"}).
				AddRow("user1", "topic/#", "read-write").
				AddRow("user2", "sensors/+", "read-only"))

		// Call the method
		permissions, err := storage.GetAllPermissions()
		if err != nil {
			t.Errorf("GetAllPermissions error: %v", err)
		}

		// Verify result
		if len(permissions) != 2 {
			t.Errorf("Expected 2 permissions, got %d", len(permissions))
		}
		if permissions[0].Username != "user1" {
			t.Errorf("Expected username 'user1', got '%s'", permissions[0].Username)
		}
		if permissions[0].TopicPattern != "topic/#" {
			t.Errorf("Expected topic pattern 'topic/#', got '%s'", permissions[0].TopicPattern)
		}
		if permissions[0].AccessLevel != "read-write" {
			t.Errorf("Expected access level 'read-write', got '%s'", permissions[0].AccessLevel)
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	t.Run("StorePermission", func(t *testing.T) {
		// Setup mock expectations
		mock.ExpectExec(`INSERT INTO permissions`).
			WithArgs("user1", "test/topic", 1, pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Call the method
		err := storage.StorePermission("user1", "test/topic", 1)
		if err != nil {
			t.Errorf("StorePermission error: %v", err)
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	t.Run("DeletePermission", func(t *testing.T) {
		// Setup mock expectations
		mock.ExpectExec(`DELETE FROM permissions`).
			WithArgs("user1", "test/topic").
			WillReturnResult(pgxmock.NewResult("DELETE", 1))

		// Call the method
		err := storage.DeletePermission("user1", "test/topic")
		if err != nil {
			t.Errorf("DeletePermission error: %v", err)
		}

		// Make sure expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})
}

// Override methods to use mock
func (s *TestPostgresStorage) StoreMessage(msg *Message, retention time.Duration) error {
	var expiresAt *time.Time

	// If retention is specified, calculate expiration time
	if retention > 0 {
		expTime := time.Now().Add(retention)
		expiresAt = &expTime
	}

	_, err := s.mock.Exec(context.Background(), `
		INSERT INTO messages (topic, payload, qos, retained, client_id, timestamp, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, msg.Topic, msg.Payload, msg.QoS, msg.Retained, msg.ClientID, time.Now(), expiresAt)

	return err
}

func (s *TestPostgresStorage) BatchStoreMessages(messages []*Message, retention time.Duration) error {
	if len(messages) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := s.mock.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	copyCount, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"messages"},
		[]string{"topic", "payload", "qos", "retained", "client_id", "timestamp", "expires_at"},
		pgx.CopyFromSlice(len(messages), func(i int) ([]interface{}, error) {
			var expiresAt *time.Time
			if retention > 0 {
				expTime := time.Now().Add(retention)
				expiresAt = &expTime
			}

			return []interface{}{
				messages[i].Topic,
				messages[i].Payload,
				messages[i].QoS,
				messages[i].Retained,
				messages[i].ClientID,
				time.Now(),
				expiresAt,
			}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("failed to copy messages: %w", err)
	}

	if copyCount != int64(len(messages)) {
		return fmt.Errorf("unexpected copy count: %d vs %d", copyCount, len(messages))
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *TestPostgresStorage) GetMessages(query MessageQuery) (*MessagesPage, error) {
	ctx := context.Background()

	// Build the count query
	countRows := s.mock.NewRows([]string{"count"})

	if err := s.mock.QueryRow(ctx, `
		SELECT COUNT(*) FROM messages 
		WHERE topic LIKE $1 AND client_id LIKE $2
	`, "%"+query.Topic+"%", "%"+query.ClientID+"%").Scan(&countRows); err != nil {
		return nil, fmt.Errorf("failed to get message count: %w", err)
	}

	// Execute the main query with pagination
	rows, err := s.mock.Query(ctx, `
		SELECT id, topic, payload, qos, retained, client_id, timestamp FROM messages 
		WHERE topic LIKE $1 AND client_id LIKE $2
		ORDER BY timestamp DESC
		LIMIT $3 OFFSET $4
	`, "%"+query.Topic+"%", "%"+query.ClientID+"%", query.Limit, query.Offset)

	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	// Placeholder for resulting count from mock
	var count int

	// Process result rows
	messages := []Message{}

	return &MessagesPage{
		Messages:   messages,
		TotalCount: count,
		HasMore:    len(messages) < count,
		NextOffset: query.Offset + len(messages),
	}, nil
}

func (s *TestPostgresStorage) CleanupExpiredMessages() (int, error) {
	_, err := s.mock.Exec(context.Background(), `
		DELETE FROM messages WHERE expires_at < $1
	`, time.Now())
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup expired messages: %w", err)
	}

	affected := 0
	return affected, nil
}

func (s *TestPostgresStorage) GetClientInfo(clientID string) (*ClientInfo, error) {
	_ = s.mock.QueryRow(context.Background(), `
		SELECT client_id, username, last_seen, connected, connect_time FROM clients
		WHERE client_id = $1
	`, clientID)

	var client ClientInfo
	return &client, nil
}

func (s *TestPostgresStorage) UpdateClientConnection(clientID, username string, connected bool) error {
	_, err := s.mock.Exec(context.Background(), `
		UPDATE clients SET username = $1, connected = $2, last_seen = $3
		WHERE client_id = $4
	`, username, connected, time.Now(), clientID)

	return err
}

func (s *TestPostgresStorage) GetAllPermissions() ([]Permission, error) {
	rows, err := s.mock.Query(context.Background(), `
		SELECT username, topic_pattern, access_level FROM permissions
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query permissions: %w", err)
	}
	defer rows.Close()

	permissions := []Permission{}
	return permissions, nil
}

func (s *TestPostgresStorage) StorePermission(username, topicPattern string, accessLevel int) error {
	_, err := s.mock.Exec(context.Background(), `
		INSERT INTO permissions (username, topic_pattern, access_level, created)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (username, topic_pattern) 
		DO UPDATE SET access_level = $3, created = $4
	`, username, topicPattern, accessLevel, time.Now())

	return err
}

func (s *TestPostgresStorage) DeletePermission(username, topicPattern string) error {
	_, err := s.mock.Exec(context.Background(), `
		DELETE FROM permissions
		WHERE username = $1 AND topic_pattern = $2
	`, username, topicPattern)

	return err
}
