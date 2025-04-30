package auth

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentAPIKeyValidation(t *testing.T) {
	a := New("test-secret")

	// Create a user
	err := a.RegisterUser("testuser", "password", false)
	if err != nil {
		t.Fatalf("RegisterUser failed: %v", err)
	}

	// Create API keys
	keys := make([]string, 10)
	perms := []Permission{{TopicPattern: "test/#", AccessLevel: ReadWrite}}

	for i := range 10 {
		key, err := a.CreateAPIKey("testuser", "Key "+string(rune(i+65)), perms, time.Hour)
		if err != nil {
			t.Fatalf("CreateAPIKey failed: %v", err)
		}
		keys[i] = key
	}

	// Concurrently validate keys
	var wg sync.WaitGroup
	concurrency := 10
	iterations := 100

	for i := range concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := range iterations {
				keyIndex := (id + j) % len(keys)
				_, err := a.ValidateAPIKey(keys[keyIndex])
				if err != nil {
					t.Errorf("ValidateAPIKey(%s) failed: %v", keys[keyIndex], err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestConcurrentUserOperations(t *testing.T) {
	a := New("test-secret")

	// Create users concurrently
	var wg sync.WaitGroup
	concurrency := 10

	for i := range concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			username := "user" + string(rune(id+65))

			// Register user
			err := a.RegisterUser(username, "password", false)
			if err != nil {
				t.Errorf("RegisterUser(%s) failed: %v", username, err)
				return
			}

			// Add permissions
			err = a.AddUserPermission(username, "topic/"+username+"/#", ReadWrite)
			if err != nil {
				t.Errorf("AddUserPermission for %s failed: %v", username, err)
				return
			}

			// Register client
			clientID := "client" + string(rune(id+65))
			err = a.RegisterClient(clientID, username)
			if err != nil {
				t.Errorf("RegisterClient(%s) failed: %v", clientID, err)
				return
			}

			// Check permission
			err = a.CheckTopicPermission(clientID, "topic/"+username+"/data", true)
			if err != nil {
				t.Errorf("CheckTopicPermission for %s failed: %v", username, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all users were created
	for i := range concurrency {
		username := "user" + string(rune(i+65))
		user, err := a.GetUser(username)
		if err != nil {
			t.Errorf("GetUser(%s) failed: %v", username, err)
			continue
		}

		if len(user.Permissions) != 1 {
			t.Errorf("Expected 1 permission for %s, got %d", username, len(user.Permissions))
		}
	}
}

func TestConcurrentTokenValidation(t *testing.T) {
	a := New("test-secret")

	// Generate tokens
	tokens := make([]string, 5)

	for i := range 5 {
		token, err := a.GenerateToken("client"+string(rune(i+65)), "user"+string(rune(i+65)), time.Hour)
		if err != nil {
			t.Fatalf("GenerateToken failed: %v", err)
		}
		tokens[i] = token
	}

	// Concurrently validate tokens
	var wg sync.WaitGroup
	concurrency := 20
	iterations := 50

	for i := range concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := range iterations {
				tokenIndex := (id + j) % len(tokens)
				_, err := a.ValidateToken(tokens[tokenIndex])
				if err != nil {
					t.Errorf("ValidateToken(%s) failed: %v", tokens[tokenIndex], err)
				}
			}
		}(i)
	}

	wg.Wait()
}
