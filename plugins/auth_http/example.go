package auth_http

import (
	"log"

	"github.com/mstgnz/gomqtt/plugin"
)

// SetupHTTPAuthPlugin configures and registers the HTTP auth plugin
func SetupHTTPAuthPlugin(registry *plugin.PluginRegistry) *HTTPAuthPlugin {
	// Create the HTTP auth plugin
	httpAuthPlugin := NewHTTPAuthPlugin()

	// Configure the plugin
	config := &AuthConfig{
		AuthEndpoint:    "https://auth.example.com/mqtt/auth",
		ACLEndpoint:     "https://auth.example.com/mqtt/acl",
		Timeout:         5,   // 5 seconds
		CacheExpiry:     300, // 5 minutes
		EnableAuthCache: true,
		EnableACLCache:  true,
		Headers: map[string]string{
			"X-API-Key":  "your-api-key",
			"User-Agent": "GoMQTT/1.0",
		},
	}

	// Initialize the plugin
	err := httpAuthPlugin.Initialize(config)
	if err != nil {
		log.Printf("Failed to initialize HTTP auth plugin: %v", err)
		return nil
	}

	// Register the plugin with the registry
	err = registry.Register(httpAuthPlugin.Plugin())
	if err != nil {
		log.Printf("Failed to register HTTP auth plugin: %v", err)
	} else {
		log.Printf("HTTP auth plugin registered successfully")
	}

	return httpAuthPlugin
}

// Example of how to use the plugin programmatically
func ExampleUseHTTPAuthPlugin() {
	// Create a plugin registry
	registry := plugin.NewPluginRegistry()

	// Setup the HTTP auth plugin
	httpAuthPlugin := SetupHTTPAuthPlugin(registry)
	if httpAuthPlugin == nil {
		log.Printf("Failed to setup HTTP auth plugin")
		return
	}

	// Example of triggering authentication event
	const EventClientAuthenticate = "client.authenticate"
	authErrors := registry.TriggerEvent(&plugin.Context{
		Event:     EventClientAuthenticate,
		ClientID:  "client123",
		Username:  "test_user",
		Timestamp: 1625482000,
		Properties: map[string]any{
			"password": "test_password",
			"ip":       "192.168.1.100",
		},
	})

	if len(authErrors) > 0 {
		log.Printf("Authentication errors: %v", authErrors)
	} else {
		log.Printf("Authentication successful")
	}

	// Example of triggering ACL check event
	const EventACLCheck = "acl.check"
	aclErrors := registry.TriggerEvent(&plugin.Context{
		Event:     EventACLCheck,
		ClientID:  "client123",
		Username:  "test_user",
		Topic:     "sensors/temperature",
		Timestamp: 1625482001,
		Properties: map[string]any{
			"action": "publish",
		},
	})

	if len(aclErrors) > 0 {
		log.Printf("ACL check errors: %v", aclErrors)
	} else {
		log.Printf("ACL check successful")
	}
}

// Example of how to create an external auth server
func ExampleAuthServer() {
	// This function provides an example of how to implement a compatible
	// authentication and authorization server to work with the HTTP auth plugin.
	// In a real application, this would be a separate service.
	/*
		package main

		import (
			"encoding/json"
			"log"
			"net/http"
		)

		func main() {
			// Authentication endpoint
			http.HandleFunc("/mqtt/auth", func(w http.ResponseWriter, r *http.Request) {
				var authReq struct {
					ClientID  string `json:"client_id"`
					Username  string `json:"username"`
					Password  string `json:"password"`
					IPAddress string `json:"ip_address"`
				}

				// Parse request
				err := json.NewDecoder(r.Body).Decode(&authReq)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"authenticated": false,
						"error":         "invalid request format",
					})
					return
				}

				// Log authentication attempt
				log.Printf("Auth request: client=%s, user=%s, ip=%s",
					authReq.ClientID, authReq.Username, authReq.IPAddress)

				// Simple authentication logic (replace with your actual logic)
				authenticated := (authReq.Username == "test_user" && authReq.Password == "test_password")

				// Respond
				json.NewEncoder(w).Encode(map[string]interface{}{
					"authenticated": authenticated,
					"error":         authenticated ? "" : "invalid credentials",
				})
			})

			// ACL endpoint
			http.HandleFunc("/mqtt/acl", func(w http.ResponseWriter, r *http.Request) {
				var aclReq struct {
					ClientID string `json:"client_id"`
					Username string `json:"username"`
					Topic    string `json:"topic"`
					Action   string `json:"action"`
				}

				// Parse request
				err := json.NewDecoder(r.Body).Decode(&aclReq)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"allowed": false,
						"error":   "invalid request format",
					})
					return
				}

				// Log ACL check
				log.Printf("ACL request: client=%s, user=%s, topic=%s, action=%s",
					aclReq.ClientID, aclReq.Username, aclReq.Topic, aclReq.Action)

				// Simple ACL logic (replace with your actual logic)
				// Allow users to publish to their own topics
				allowed := false
				if aclReq.Username == "test_user" {
					if aclReq.Action == "publish" {
						// Allow publishing to specific topics
						allowed = aclReq.Topic == "sensors/temperature" ||
							aclReq.Topic == "sensors/humidity"
					} else if aclReq.Action == "subscribe" {
						// Allow subscribing to all sensor topics
						allowed = aclReq.Topic == "sensors/#"
					}
				}

				// Respond
				json.NewEncoder(w).Encode(map[string]interface{}{
					"allowed": allowed,
					"error":   allowed ? "" : "access denied to this topic",
				})
			})

			log.Println("Starting auth server on :8080")
			log.Fatal(http.ListenAndServe(":8080", nil))
		}
	*/
}
