package ratelimit

import (
	"log"

	"github.com/mstgnz/gomqtt/plugin"
)

// SetupRateLimitPlugin configures and registers the rate limit plugin
func SetupRateLimitPlugin(registry *plugin.PluginRegistry) *RateLimitPlugin {
	// Create the rate limit plugin
	rateLimitPlugin := NewRateLimitPlugin()

	// Configure the plugin
	config := &RateLimitConfig{
		ConnectionRate: 10,    // 10 connections per second per IP
		PublishRate:    100,   // 100 publish operations per second per client
		SubscribeRate:  20,    // 20 subscribe operations per second per client
		ByteRate:       10240, // 10KB per second per client
		WindowSize:     60,    // 60 second window
		IPWhitelist: []string{
			"127.0.0.1",      // Localhost
			"192.168.1.0/24", // Local network
		},
	}

	// Initialize the plugin
	err := rateLimitPlugin.Initialize(config)
	if err != nil {
		log.Printf("Failed to initialize rate limit plugin: %v", err)
		return nil
	}

	// Register the plugin with the registry
	err = registry.Register(rateLimitPlugin.Plugin())
	if err != nil {
		log.Printf("Failed to register rate limit plugin: %v", err)
	} else {
		log.Printf("Rate limit plugin registered successfully")
	}

	return rateLimitPlugin
}

// ExampleUseRateLimitPlugin demonstrates how to use the rate limit plugin
func ExampleUseRateLimitPlugin() {
	// Create a plugin registry
	registry := plugin.NewPluginRegistry()

	// Setup the rate limit plugin
	rateLimitPlugin := SetupRateLimitPlugin(registry)
	if rateLimitPlugin == nil {
		log.Printf("Failed to setup rate limit plugin")
		return
	}

	// Example of triggering a client connect event that will be processed by the plugin
	registry.TriggerEvent(&plugin.Context{
		Event:     plugin.EventClientConnect,
		ClientID:  "client123",
		Username:  "user",
		Timestamp: 1625482000,
		Properties: map[string]any{
			"ip": "203.0.113.1", // Example IP address
		},
	})

	// Example of triggering a publish event
	registry.TriggerEvent(&plugin.Context{
		Event:     plugin.EventMessagePublish,
		ClientID:  "client123",
		Username:  "user",
		Topic:     "sensors/temperature",
		Payload:   []byte(`{"temperature": 22.5}`),
		QoS:       1,
		Retained:  false,
		Timestamp: 1625482001,
	})

	// Example of triggering a subscribe event
	registry.TriggerEvent(&plugin.Context{
		Event:     plugin.EventSubscribe,
		ClientID:  "client123",
		Username:  "user",
		Topic:     "sensors/#",
		Timestamp: 1625482002,
	})
}
