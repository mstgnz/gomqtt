package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage implements the Storage interface for Redis
type RedisStorage struct {
	client     *redis.Client
	keyPrefix  string
	cleanupCtx context.Context
	cleanupFn  context.CancelFunc
}

// NewRedisStorage creates a new Redis storage instance
func NewRedisStorage(redisURL, keyPrefix string) (*RedisStorage, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	// Test connection
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("unable to connect to Redis: %w", err)
	}

	cleanupCtx, cleanupFn := context.WithCancel(context.Background())

	return &RedisStorage{
		client:     client,
		keyPrefix:  keyPrefix,
		cleanupCtx: cleanupCtx,
		cleanupFn:  cleanupFn,
	}, nil
}

// key formats a Redis key with the configured prefix
func (s *RedisStorage) key(format string, args ...interface{}) string {
	key := fmt.Sprintf(format, args...)
	return s.keyPrefix + key
}

// Close closes the Redis connection
func (s *RedisStorage) Close() {
	if s.cleanupFn != nil {
		s.cleanupFn()
	}
	if s.client != nil {
		s.client.Close()
	}
}

// messageKey returns the Redis key for a single message
func (s *RedisStorage) messageKey(topic string, timestamp time.Time, id int64) string {
	return s.key("msg:%s:%d:%d", topic, timestamp.UnixNano(), id)
}

// marshalMessage converts a Message to a JSON string for storage
func marshalMessage(msg *Message) (string, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("failed to marshal message: %w", err)
	}
	return string(data), nil
}

// unmarshalMessage converts a JSON string to a Message
func unmarshalMessage(data string) (*Message, error) {
	var msg Message
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	return &msg, nil
}

// StoreMessage stores an MQTT message with optional expiration
func (s *RedisStorage) StoreMessage(msg *Message, retention time.Duration) error {
	// If message doesn't have a timestamp, set it to now
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// If message doesn't have an ID, generate one based on timestamp
	if msg.ID == 0 {
		msg.ID = msg.Timestamp.UnixNano()
	}

	// Marshal message to JSON
	msgJSON, err := marshalMessage(msg)
	if err != nil {
		return err
	}

	// Store the message
	ctx := context.Background()
	msgKey := s.messageKey(msg.Topic, msg.Timestamp, msg.ID)
	pipe := s.client.Pipeline()

	// Store the message
	pipe.Set(ctx, msgKey, msgJSON, retention)

	// Add to sorted set for by-topic retrieval
	topicKey := s.key("topic:%s", msg.Topic)
	pipe.ZAdd(ctx, topicKey, redis.Z{
		Score:  float64(msg.Timestamp.UnixNano()),
		Member: msgKey,
	})

	// If retention is set, the sorted set needs to be cleaned up separately
	if retention > 0 {
		pipe.Expire(ctx, topicKey, retention)
	}

	// Add to client index if clientID is provided
	if msg.ClientID != "" {
		clientKey := s.key("client:%s:msgs", msg.ClientID)
		pipe.ZAdd(ctx, clientKey, redis.Z{
			Score:  float64(msg.Timestamp.UnixNano()),
			Member: msgKey,
		})
		if retention > 0 {
			pipe.Expire(ctx, clientKey, retention)
		}
	}

	// If message is retained, store it in the retained messages set
	if msg.Retained {
		retainedKey := s.key("retained:%s", msg.Topic)
		pipe.Set(ctx, retainedKey, msgJSON, retention)
	}

	_, err = pipe.Exec(ctx)
	return err
}

// BatchStoreMessages stores multiple MQTT messages in a single transaction
func (s *RedisStorage) BatchStoreMessages(messages []*Message, retention time.Duration) error {
	if len(messages) == 0 {
		return nil
	}

	ctx := context.Background()
	pipe := s.client.Pipeline()

	for _, msg := range messages {
		// If message doesn't have a timestamp, set it to now
		if msg.Timestamp.IsZero() {
			msg.Timestamp = time.Now()
		}

		// If message doesn't have an ID, generate one based on timestamp
		if msg.ID == 0 {
			msg.ID = msg.Timestamp.UnixNano()
		}

		// Marshal message to JSON
		msgJSON, err := marshalMessage(msg)
		if err != nil {
			return err
		}

		// Store the message
		msgKey := s.messageKey(msg.Topic, msg.Timestamp, msg.ID)
		pipe.Set(ctx, msgKey, msgJSON, retention)

		// Add to sorted set for by-topic retrieval
		topicKey := s.key("topic:%s", msg.Topic)
		pipe.ZAdd(ctx, topicKey, redis.Z{
			Score:  float64(msg.Timestamp.UnixNano()),
			Member: msgKey,
		})

		// Add to client index if clientID is provided
		if msg.ClientID != "" {
			clientKey := s.key("client:%s:msgs", msg.ClientID)
			pipe.ZAdd(ctx, clientKey, redis.Z{
				Score:  float64(msg.Timestamp.UnixNano()),
				Member: msgKey,
			})
		}

		// If message is retained, store it in the retained messages set
		if msg.Retained {
			retainedKey := s.key("retained:%s", msg.Topic)
			pipe.Set(ctx, retainedKey, msgJSON, retention)
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}

// GetMessages retrieves messages based on query parameters
func (s *RedisStorage) GetMessages(query MessageQuery) (*MessagesPage, error) {
	ctx := context.Background()
	var keys []string
	var err error

	// Set default limit if not provided
	if query.Limit <= 0 {
		query.Limit = 100
	}

	// Create timestamp range for the query
	var min, max string
	if !query.FromTimestamp.IsZero() {
		min = fmt.Sprintf("%d", query.FromTimestamp.UnixNano())
	} else {
		min = "0"
	}

	if !query.ToTimestamp.IsZero() {
		max = fmt.Sprintf("%d", query.ToTimestamp.UnixNano())
	} else {
		max = "+inf"
	}

	// Determine which key to query based on the provided filters
	if query.Topic != "" && query.ClientID != "" {
		// First get all message keys for the topic
		topicKey := s.key("topic:%s", query.Topic)
		topicKeys, err := s.client.ZRangeByScore(ctx, topicKey, &redis.ZRangeBy{
			Min:    min,
			Max:    max,
			Offset: int64(query.Offset),
			Count:  int64(query.Limit),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get messages by topic: %w", err)
		}

		// Then get all message keys for the client
		clientKey := s.key("client:%s:msgs", query.ClientID)
		clientKeys, err := s.client.ZRangeByScore(ctx, clientKey, &redis.ZRangeBy{
			Min: min,
			Max: max,
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get messages by client: %w", err)
		}

		// Find the intersection (messages that match both topic and client)
		topicKeyMap := make(map[string]struct{})
		for _, key := range topicKeys {
			topicKeyMap[key] = struct{}{}
		}

		for _, key := range clientKeys {
			if _, ok := topicKeyMap[key]; ok {
				keys = append(keys, key)
			}
		}

		// Apply offset and limit to the intersection result
		if len(keys) > query.Offset {
			end := query.Offset + query.Limit
			if end > len(keys) {
				end = len(keys)
			}
			keys = keys[query.Offset:end]
		} else {
			keys = []string{}
		}
	} else if query.Topic != "" {
		// Query by topic only
		topicKey := s.key("topic:%s", query.Topic)
		keys, err = s.client.ZRangeByScore(ctx, topicKey, &redis.ZRangeBy{
			Min:    min,
			Max:    max,
			Offset: int64(query.Offset),
			Count:  int64(query.Limit),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get messages by topic: %w", err)
		}
	} else if query.ClientID != "" {
		// Query by client ID only
		clientKey := s.key("client:%s:msgs", query.ClientID)
		keys, err = s.client.ZRangeByScore(ctx, clientKey, &redis.ZRangeBy{
			Min:    min,
			Max:    max,
			Offset: int64(query.Offset),
			Count:  int64(query.Limit),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get messages by client: %w", err)
		}
	} else {
		// Without specific filters, we can't efficiently query Redis
		// This implementation does not support full message scanning
		return &MessagesPage{
			Messages:   []Message{},
			TotalCount: 0,
			HasMore:    false,
		}, nil
	}

	// Get the actual message data for each key
	var messages []Message
	if len(keys) > 0 {
		// Get all messages in a single MGET operation
		vals, err := s.client.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get message data: %w", err)
		}

		for _, val := range vals {
			if val == nil {
				continue
			}
			strVal, ok := val.(string)
			if !ok {
				continue
			}

			msg, err := unmarshalMessage(strVal)
			if err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				continue
			}
			messages = append(messages, *msg)
		}

		// Sort messages by timestamp
		sort.Slice(messages, func(i, j int) bool {
			return messages[i].Timestamp.Before(messages[j].Timestamp)
		})
	}

	// Determine if there are more messages
	hasMore := false
	nextOffset := 0
	if len(messages) == query.Limit {
		hasMore = true
		nextOffset = query.Offset + query.Limit
	}

	return &MessagesPage{
		Messages:   messages,
		TotalCount: len(messages), // Redis doesn't provide an easy way to get total count without scanning
		HasMore:    hasMore,
		NextOffset: nextOffset,
	}, nil
}

// GetRetainedMessages retrieves retained messages for a specific topic pattern
func (s *RedisStorage) GetRetainedMessages(topicPattern string) ([]Message, error) {
	ctx := context.Background()
	var messages []Message

	// Redis doesn't support pattern matching on keys in an optimal way,
	// so we'll use SCAN to find all retained message keys
	pattern := s.key("retained:*")
	if topicPattern != "" && topicPattern != "#" {
		// If a specific pattern is provided, we can narrow the scan
		// Replace MQTT wildcards with Redis patterns
		redisPattern := strings.ReplaceAll(topicPattern, "+", "*")
		redisPattern = strings.ReplaceAll(redisPattern, "#", "*")
		pattern = s.key("retained:%s", redisPattern)
	}

	// Scan for retained message keys
	iter := s.client.Scan(ctx, 0, pattern, 0).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan retained messages: %w", err)
	}

	if len(keys) == 0 {
		return messages, nil
	}

	// Get the message data for each key
	vals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get retained message data: %w", err)
	}

	for _, val := range vals {
		if val == nil {
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			continue
		}

		msg, err := unmarshalMessage(strVal)
		if err != nil {
			log.Printf("Error unmarshaling retained message: %v", err)
			continue
		}
		messages = append(messages, *msg)
	}

	return messages, nil
}

// DeleteRetainedMessage deletes a retained message for a specific topic
func (s *RedisStorage) DeleteRetainedMessage(topic string) error {
	ctx := context.Background()
	retainedKey := s.key("retained:%s", topic)
	return s.client.Del(ctx, retainedKey).Err()
}

// CleanupExpiredMessages removes expired messages from storage
// With Redis, this happens automatically via TTL, but we still need to implement
// the method for the Storage interface
func (s *RedisStorage) CleanupExpiredMessages() (int, error) {
	// Redis handles expiration automatically
	return 0, nil
}

// StartMessageCleanup starts a background task to clean up expired messages
// Again, Redis handles this automatically, but we implement for the interface
func (s *RedisStorage) StartMessageCleanup(interval time.Duration) {
	// Redis handles automatic cleanup, no need to do anything here
}

// GetClientInfo retrieves client information for a specific client ID
func (s *RedisStorage) GetClientInfo(clientID string) (*ClientInfo, error) {
	ctx := context.Background()
	clientKey := s.key("client:%s:info", clientID)

	data, err := s.client.Get(ctx, clientKey).Result()
	if err == redis.Nil {
		return nil, nil // Client not found
	} else if err != nil {
		return nil, fmt.Errorf("failed to get client info: %w", err)
	}

	var clientInfo ClientInfo
	if err := json.Unmarshal([]byte(data), &clientInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal client info: %w", err)
	}

	return &clientInfo, nil
}

// UpdateClientConnection updates client connection status
func (s *RedisStorage) UpdateClientConnection(clientID, username string, connected bool) error {
	ctx := context.Background()
	clientKey := s.key("client:%s:info", clientID)

	// Get existing client info or create a new one
	var clientInfo ClientInfo
	data, err := s.client.Get(ctx, clientKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to get client info: %w", err)
	}

	if err == nil {
		// Client info exists, unmarshal it
		if err := json.Unmarshal([]byte(data), &clientInfo); err != nil {
			return fmt.Errorf("failed to unmarshal client info: %w", err)
		}
	} else {
		// Create new client info
		clientInfo = ClientInfo{
			ClientID: clientID,
			Username: username,
		}
	}

	// Update connection status
	now := time.Now()
	clientInfo.LastSeen = now
	clientInfo.Connected = connected
	if connected {
		clientInfo.ConnectTime = now
	}

	// Store updated client info
	bytes, err := json.Marshal(clientInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal client info: %w", err)
	}

	data = string(bytes)

	return s.client.Set(ctx, clientKey, data, 0).Err()
}

// GetAllPermissions retrieves all permission entries
func (s *RedisStorage) GetAllPermissions() ([]Permission, error) {
	ctx := context.Background()
	pattern := s.key("permission:*")

	// Scan for permission keys
	iter := s.client.Scan(ctx, 0, pattern, 0).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan permissions: %w", err)
	}

	if len(keys) == 0 {
		return []Permission{}, nil
	}

	// Get the permission data for each key
	var permissions []Permission
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return nil, fmt.Errorf("failed to get permission data: %w", err)
		}

		var perm Permission
		if err := json.Unmarshal([]byte(data), &perm); err != nil {
			log.Printf("Error unmarshaling permission: %v", err)
			continue
		}
		permissions = append(permissions, perm)
	}

	return permissions, nil
}

// StorePermission stores a permission entry
func (s *RedisStorage) StorePermission(username, topicPattern string, accessLevel int) error {
	ctx := context.Background()
	permKey := s.key("permission:%s:%s", username, topicPattern)

	perm := Permission{
		Username:     username,
		TopicPattern: topicPattern,
		AccessLevel:  fmt.Sprintf("%d", accessLevel),
	}

	data, err := json.Marshal(perm)
	if err != nil {
		return fmt.Errorf("failed to marshal permission: %w", err)
	}

	return s.client.Set(ctx, permKey, data, 0).Err()
}

// DeletePermission deletes a permission entry
func (s *RedisStorage) DeletePermission(username, topicPattern string) error {
	ctx := context.Background()
	permKey := s.key("permission:%s:%s", username, topicPattern)
	return s.client.Del(ctx, permKey).Err()
}
