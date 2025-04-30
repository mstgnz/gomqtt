package auth

import (
	"fmt"
	"time"
)

// This file contains examples that demonstrate proper usage of the auth package.
// These examples can be run as tests with `go test -run Example`.

func Example() {
	// Create a new auth service with a secret key
	auth := New("my-secret-key")

	// Register a user
	err := auth.RegisterUser("alice", "password123", false)
	if err != nil {
		fmt.Println("Error registering user:", err)
		return
	}

	// Add permissions to the user
	err = auth.AddUserPermission("alice", "sensors/#", ReadWrite)
	if err != nil {
		fmt.Println("Error adding permission:", err)
		return
	}

	// Register a client device for the user
	err = auth.RegisterClient("alices-device", "alice")
	if err != nil {
		fmt.Println("Error registering client:", err)
		return
	}

	// Check if the client has permission to access a topic
	err = auth.CheckTopicPermission("alices-device", "sensors/temperature", true)
	if err != nil {
		fmt.Println("Permission denied:", err)
		return
	}
	fmt.Println("Permission granted for sensors/temperature")

	// Check permission for an unauthorized topic
	err = auth.CheckTopicPermission("alices-device", "admin/logs", true)
	if err != nil {
		fmt.Println("Permission denied for admin/logs")
	}

	// Output:
	// Permission granted for sensors/temperature
	// Permission denied for admin/logs
}

func ExampleAuth_GenerateToken() {
	auth := New("my-secret-key")

	// Generate a token valid for 1 hour
	token, err := auth.GenerateToken("device-123", "bob", time.Hour)
	if err != nil {
		fmt.Println("Error generating token:", err)
		return
	}

	// Validate the token
	claims, err := auth.ValidateToken(token)
	if err != nil {
		fmt.Println("Token validation failed:", err)
		return
	}

	fmt.Printf("Token is valid for client %s and user %s\n", claims.ClientID, claims.Username)

	// Output:
	// Token is valid for client device-123 and user bob
}

func ExampleAuth_CreateAPIKey() {
	auth := New("my-secret-key")

	// Register a user
	_ = auth.RegisterUser("carol", "securepass", false)

	// Define permissions for the API key
	permissions := []Permission{
		{TopicPattern: "devices/+/data", AccessLevel: ReadOnly},
		{TopicPattern: "devices/+/config", AccessLevel: ReadWrite},
	}

	// Create an API key valid for 30 days
	apiKey, err := auth.CreateAPIKey("carol", "Mobile App Key", permissions, 30*24*time.Hour)
	if err != nil {
		fmt.Println("Error creating API key:", err)
		return
	}

	// Validate the API key
	key, err := auth.ValidateAPIKey(apiKey)
	if err != nil {
		fmt.Println("API key validation failed:", err)
		return
	}

	fmt.Printf("API key '%s' is valid with %d permissions\n", key.Description, len(key.Permissions))

	// Output:
	// API key 'Mobile App Key' is valid with 2 permissions
}

func Example_topicMatches() {
	// Demonstrate MQTT-style topic pattern matching
	patterns := []string{
		"sensors/temperature",  // Exact match
		"sensors/+",            // Single-level wildcard
		"sensors/#",            // Multi-level wildcard
		"sensors/+/readings/#", // Mixed wildcards
	}

	topic := "sensors/temperature/readings/celsius"

	for _, pattern := range patterns {
		match := topicMatches(pattern, topic)
		fmt.Printf("Pattern: %-25s Matches: %v\n", pattern, match)
	}

	// Output:
	// Pattern: sensors/temperature       Matches: false
	// Pattern: sensors/+                 Matches: false
	// Pattern: sensors/#                 Matches: true
	// Pattern: sensors/+/readings/#      Matches: true
}
