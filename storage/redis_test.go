package storage

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

func setupTestRedis(t *testing.T) (*RedisStorage, *miniredis.Miniredis) {
	// Start a mini Redis server for testing
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to create miniredis: %v", err)
	}

	// Create a Redis storage with the mini Redis server
	storage, err := NewRedisStorage("redis://"+mr.Addr(), "test:")
	if err != nil {
		mr.Close()
		t.Fatalf("Failed to create Redis storage: %v", err)
	}

	return storage, mr
}

func TestNewRedisStorage(t *testing.T) {
	storage, mr := setupTestRedis(t)
	defer mr.Close()
	defer storage.Close()

	// Basic validation that the storage was created
	assert.NotNil(t, storage)
	assert.NotNil(t, storage.client)
	assert.Equal(t, "test:", storage.keyPrefix)
}

func TestStoreAndGetMessage(t *testing.T) {
	storage, mr := setupTestRedis(t)
	defer mr.Close()
	defer storage.Close()

	// Create test message
	msg := &Message{
		Topic:     "test/topic",
		Payload:   []byte("test payload"),
		Timestamp: time.Now(),
		QoS:       1,
		Retained:  true,
		ClientID:  "test-client",
	}

	// Store the message
	err := storage.StoreMessage(msg, time.Hour)
	assert.NoError(t, err)

	// Retrieve the message
	query := MessageQuery{
		Topic:  "test/topic",
		Limit:  10,
		Offset: 0,
	}
	result, err := storage.GetMessages(query)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.Messages))
	assert.Equal(t, msg.Topic, result.Messages[0].Topic)
	assert.Equal(t, msg.Payload, result.Messages[0].Payload)
}

func TestUpdateClientConnection(t *testing.T) {
	storage, mr := setupTestRedis(t)
	defer mr.Close()
	defer storage.Close()

	// Update client connection
	clientID := "test-client"
	username := "test-user"
	err := storage.UpdateClientConnection(clientID, username, true)
	assert.NoError(t, err)

	// Get client info
	clientInfo, err := storage.GetClientInfo(clientID)
	assert.NoError(t, err)
	assert.NotNil(t, clientInfo)
	assert.Equal(t, clientID, clientInfo.ClientID)
	assert.Equal(t, username, clientInfo.Username)
	assert.True(t, clientInfo.Connected)
	assert.False(t, clientInfo.ConnectTime.IsZero())
}

func TestPermissions(t *testing.T) {
	storage, mr := setupTestRedis(t)
	defer mr.Close()
	defer storage.Close()

	// Store permission
	username := "test-user"
	topicPattern := "test/#"
	accessLevel := 2
	err := storage.StorePermission(username, topicPattern, accessLevel)
	assert.NoError(t, err)

	// Get all permissions
	permissions, err := storage.GetAllPermissions()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(permissions))
	assert.Equal(t, username, permissions[0].Username)
	assert.Equal(t, topicPattern, permissions[0].TopicPattern)
	assert.Equal(t, "2", permissions[0].AccessLevel)

	// Delete permission
	err = storage.DeletePermission(username, topicPattern)
	assert.NoError(t, err)

	// Verify it's gone
	permissions, err = storage.GetAllPermissions()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(permissions))
}

func TestRetainedMessages(t *testing.T) {
	storage, mr := setupTestRedis(t)
	defer mr.Close()
	defer storage.Close()

	// Create test message
	msg := &Message{
		Topic:     "test/retained",
		Payload:   []byte("retained payload"),
		Timestamp: time.Now(),
		QoS:       1,
		Retained:  true,
		ClientID:  "test-client",
	}

	// Store the message
	err := storage.StoreMessage(msg, 0)
	assert.NoError(t, err)

	// Get retained messages
	retained, err := storage.GetRetainedMessages("test/#")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(retained))
	assert.Equal(t, msg.Topic, retained[0].Topic)
	assert.Equal(t, msg.Payload, retained[0].Payload)

	// Delete retained message
	err = storage.DeleteRetainedMessage(msg.Topic)
	assert.NoError(t, err)

	// Verify it's gone
	retained, err = storage.GetRetainedMessages("test/#")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(retained))
}
