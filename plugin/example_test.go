package plugin_test

import (
	"fmt"
	"log"
	"time"

	"github.com/mstgnz/gomqtt/plugin"
)

// This example demonstrates how to create and use plugins with the plugin system
func Example() {
	// Create a plugin registry
	registry := plugin.NewPluginRegistry()

	// Create a logging plugin
	loggingPlugin := plugin.NewPlugin(
		"logging_plugin",
		"Logs all events from the broker",
		"1.0",
		"MQTT Developer",
	)

	// Handle client connect events
	loggingPlugin.OnEvent(plugin.EventClientConnect, func(ctx *plugin.Context) error {
		fmt.Printf("Client connected: %s (user: %s)\n", ctx.ClientID, ctx.Username)
		return nil
	})

	// Handle message publish events
	loggingPlugin.OnEvent(plugin.EventMessagePublish, func(ctx *plugin.Context) error {
		fmt.Printf("Message published: topic=%s, payload=%s\n", ctx.Topic, string(ctx.Payload))
		return nil
	})

	// Create an auth plugin
	authPlugin := plugin.NewPlugin(
		"auth_plugin",
		"Performs client authentication",
		"1.0",
		"MQTT Developer",
	)

	// Simple authentication check
	authPlugin.OnEvent(plugin.EventClientConnect, func(ctx *plugin.Context) error {
		if ctx.Username == "" {
			return fmt.Errorf("authentication failed: username required")
		}

		if ctx.Username == "admin" {
			fmt.Printf("Admin user connected: %s\n", ctx.ClientID)
		} else {
			fmt.Printf("Regular user connected: %s (user: %s)\n", ctx.ClientID, ctx.Username)
		}
		return nil
	})

	// Register plugins
	err := registry.Register(loggingPlugin)
	if err != nil {
		log.Fatal(err)
	}

	err = registry.Register(authPlugin)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Plugins registered successfully")

	// Trigger a client connect event
	connectCtx := &plugin.Context{
		Event:      plugin.EventClientConnect,
		ClientID:   "client123",
		Username:   "user",
		Timestamp:  time.Now().Unix(),
		Properties: map[string]any{"ip": "192.168.1.100"},
	}

	errs := registry.TriggerEvent(connectCtx)
	if len(errs) > 0 {
		for _, err := range errs {
			fmt.Printf("Error: %v\n", err)
		}
	}

	// Trigger a message publish event
	publishCtx := &plugin.Context{
		Event:     plugin.EventMessagePublish,
		ClientID:  "client123",
		Topic:     "sensors/temperature",
		Payload:   []byte("23.5"),
		Timestamp: time.Now().Unix(),
		QoS:       1,
		Retained:  false,
	}

	registry.TriggerEvent(publishCtx)

	// Output:
	// Plugins registered successfully
	// Client connected: client123 (user: user)
	// Regular user connected: client123 (user: user)
	// Message published: topic=sensors/temperature, payload=23.5
}
