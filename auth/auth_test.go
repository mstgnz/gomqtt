package auth

import (
	"testing"
	"time"
)

func TestGenerateAPIKey(t *testing.T) {
	a := New("test-secret")

	// Test with specific length
	key, err := a.GenerateAPIKey(16)
	if err != nil {
		t.Errorf("GenerateAPIKey failed: %v", err)
	}
	if len(key) != 16 {
		t.Errorf("Expected key length 16, got %d", len(key))
	}

	// Test with default length
	key, err = a.GenerateAPIKey(0)
	if err != nil {
		t.Errorf("GenerateAPIKey with default length failed: %v", err)
	}
	if len(key) != 32 {
		t.Errorf("Expected default key length 32, got %d", len(key))
	}
}

func TestGenerateToken(t *testing.T) {
	a := New("test-secret")

	token, err := a.GenerateToken("client1", "user1", time.Hour)
	if err != nil {
		t.Errorf("GenerateToken failed: %v", err)
	}
	if token == "" {
		t.Error("Generated token is empty")
	}

	// Validate the token
	claims, err := a.ValidateToken(token)
	if err != nil {
		t.Errorf("ValidateToken failed: %v", err)
	}
	if claims.ClientID != "client1" {
		t.Errorf("Expected ClientID 'client1', got %s", claims.ClientID)
	}
	if claims.Username != "user1" {
		t.Errorf("Expected Username 'user1', got %s", claims.Username)
	}
}

func TestInvalidToken(t *testing.T) {
	a := New("test-secret")

	// Test with invalid token
	_, err := a.ValidateToken("invalid-token")
	if err == nil {
		t.Error("ValidateToken should fail with invalid token")
	}

	// Test with token signed with different key
	a2 := New("different-secret")
	token, _ := a2.GenerateToken("client1", "user1", time.Hour)
	_, err = a.ValidateToken(token)
	if err == nil {
		t.Error("ValidateToken should fail with token signed with different key")
	}
}

func TestUserManagement(t *testing.T) {
	a := New("test-secret")

	// Register a user
	err := a.RegisterUser("testuser", "password", false)
	if err != nil {
		t.Errorf("RegisterUser failed: %v", err)
	}

	// Try to register the same user again
	err = a.RegisterUser("testuser", "password", false)
	if err == nil {
		t.Error("RegisterUser should fail for existing user")
	}

	// Authenticate user
	user, err := a.AuthenticateUser("testuser", "password")
	if err != nil {
		t.Errorf("AuthenticateUser failed: %v", err)
	}
	if user.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %s", user.Username)
	}

	// Authenticate with wrong password
	_, err = a.AuthenticateUser("testuser", "wrong-password")
	if err == nil {
		t.Error("AuthenticateUser should fail with wrong password")
	}

	// Authenticate non-existent user
	_, err = a.AuthenticateUser("nonexistent", "password")
	if err != ErrUserNotFound {
		t.Errorf("Expected ErrUserNotFound, got %v", err)
	}
}

func TestAPIKeyManagement(t *testing.T) {
	a := New("test-secret")

	// Register a user
	err := a.RegisterUser("testuser", "password", false)
	if err != nil {
		t.Errorf("RegisterUser failed: %v", err)
	}

	// Create API key for user
	permissions := []Permission{
		{TopicPattern: "test/#", AccessLevel: ReadWrite},
	}
	key, err := a.CreateAPIKey("testuser", "Test Key", permissions, time.Hour)
	if err != nil {
		t.Errorf("CreateAPIKey failed: %v", err)
	}
	if key == "" {
		t.Error("Generated API key is empty")
	}

	// Validate API key
	apiKey, err := a.ValidateAPIKey(key)
	if err != nil {
		t.Errorf("ValidateAPIKey failed: %v", err)
	}
	if apiKey.Description != "Test Key" {
		t.Errorf("Expected description 'Test Key', got %s", apiKey.Description)
	}

	// Validate non-existent API key
	_, err = a.ValidateAPIKey("non-existent-key")
	if err != ErrInvalidAPIKey {
		t.Errorf("Expected ErrInvalidAPIKey, got %v", err)
	}

	// Revoke API key
	err = a.RevokeAPIKey(key)
	if err != nil {
		t.Errorf("RevokeAPIKey failed: %v", err)
	}

	// Validate revoked API key
	_, err = a.ValidateAPIKey(key)
	if err != ErrInvalidAPIKey {
		t.Errorf("Expected ErrInvalidAPIKey for revoked key, got %v", err)
	}
}

func TestClientManagement(t *testing.T) {
	a := New("test-secret")

	// Register a user
	err := a.RegisterUser("testuser", "password", false)
	if err != nil {
		t.Errorf("RegisterUser failed: %v", err)
	}

	// Register a client
	err = a.RegisterClient("client1", "testuser")
	if err != nil {
		t.Errorf("RegisterClient failed: %v", err)
	}

	// Get client
	client, err := a.GetClient("client1")
	if err != nil {
		t.Errorf("GetClient failed: %v", err)
	}
	if client.ClientID != "client1" {
		t.Errorf("Expected ClientID 'client1', got %s", client.ClientID)
	}
	if client.Username != "testuser" {
		t.Errorf("Expected Username 'testuser', got %s", client.Username)
	}

	// Block client
	err = a.BlockClient("client1")
	if err != nil {
		t.Errorf("BlockClient failed: %v", err)
	}

	// Get blocked client
	client, err = a.GetClient("client1")
	if err != nil {
		t.Errorf("GetClient failed: %v", err)
	}
	if !client.IsBlocked {
		t.Error("Client should be blocked")
	}

	// Unblock client
	err = a.UnblockClient("client1")
	if err != nil {
		t.Errorf("UnblockClient failed: %v", err)
	}

	// Get unblocked client
	client, err = a.GetClient("client1")
	if err != nil {
		t.Errorf("GetClient failed: %v", err)
	}
	if client.IsBlocked {
		t.Error("Client should be unblocked")
	}
}

func TestPermissions(t *testing.T) {
	a := New("test-secret")

	// Register an admin user
	err := a.RegisterUser("admin", "password", true)
	if err != nil {
		t.Errorf("RegisterUser failed: %v", err)
	}

	// Register a regular user
	err = a.RegisterUser("user", "password", false)
	if err != nil {
		t.Errorf("RegisterUser failed: %v", err)
	}

	// Add permission to user
	err = a.AddUserPermission("user", "test/#", ReadWrite)
	if err != nil {
		t.Errorf("AddUserPermission failed: %v", err)
	}

	// Register clients
	err = a.RegisterClient("adminClient", "admin")
	if err != nil {
		t.Errorf("RegisterClient failed: %v", err)
	}
	err = a.RegisterClient("userClient", "user")
	if err != nil {
		t.Errorf("RegisterClient failed: %v", err)
	}

	// Check permissions
	// Admin should have access to any topic
	err = a.CheckTopicPermission("adminClient", "any/topic", true)
	if err != nil {
		t.Errorf("Admin should have access to any topic: %v", err)
	}

	// User should have access to topics matching their permissions
	err = a.CheckTopicPermission("userClient", "test/topic1", true)
	if err != nil {
		t.Errorf("User should have access to permitted topic: %v", err)
	}

	// User should not have access to topics not matching their permissions
	err = a.CheckTopicPermission("userClient", "other/topic", true)
	if err != ErrPermissionDenied {
		t.Errorf("Expected ErrPermissionDenied, got %v", err)
	}

	// Remove permission
	err = a.RemoveUserPermission("user", "test/#")
	if err != nil {
		t.Errorf("RemoveUserPermission failed: %v", err)
	}

	// User should no longer have access to previously permitted topic
	err = a.CheckTopicPermission("userClient", "test/topic1", true)
	if err != ErrPermissionDenied {
		t.Errorf("Expected ErrPermissionDenied after permission removal, got %v", err)
	}
}

func TestTopicMatching(t *testing.T) {
	tests := []struct {
		pattern string
		topic   string
		matches bool
	}{
		// Exact matches
		{"test", "test", true},
		{"test/topic", "test/topic", true},
		{"test", "other", false},

		// Single-level wildcard
		{"test/+", "test/topic", true},
		{"test/+", "test/other", true},
		{"test/+", "test/topic/subtopic", false},
		{"test/+/subtopic", "test/topic/subtopic", true},

		// Multi-level wildcard
		{"test/#", "test", true},
		{"test/#", "test/topic", true},
		{"test/#", "test/topic/subtopic", true},
		{"other/#", "test/topic", false},

		// Mixed wildcards
		{"test/+/#", "test/topic", true},
		{"test/+/#", "test/topic/subtopic", true},
	}

	for _, test := range tests {
		result := topicMatches(test.pattern, test.topic)
		if result != test.matches {
			t.Errorf("topicMatches(%q, %q) = %v, want %v",
				test.pattern, test.topic, result, test.matches)
		}
	}
}

func TestExpiredToken(t *testing.T) {
	a := New("test-secret")

	// Create a token that expires immediately
	token, err := a.GenerateToken("client1", "user1", -time.Hour)
	if err != nil {
		t.Errorf("GenerateToken failed: %v", err)
	}

	// Token should be expired
	_, err = a.ValidateToken(token)
	if err == nil {
		t.Error("ValidateToken should fail with expired token")
	}
}

func TestExpiredAPIKey(t *testing.T) {
	a := New("test-secret")

	// Register a user
	err := a.RegisterUser("testuser", "password", false)
	if err != nil {
		t.Errorf("RegisterUser failed: %v", err)
	}

	// Create API key that expires immediately
	permissions := []Permission{
		{TopicPattern: "test/#", AccessLevel: ReadWrite},
	}
	key, err := a.CreateAPIKey("testuser", "Test Key", permissions, -time.Hour)
	if err != nil {
		t.Errorf("CreateAPIKey failed: %v", err)
	}

	// API key should be expired
	_, err = a.ValidateAPIKey(key)
	if err == nil {
		t.Error("ValidateAPIKey should fail with expired API key")
	}
}
