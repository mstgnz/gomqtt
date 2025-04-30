/*
Package plugin provides an extensible plugin system for GoMQTT.

This package implements an event-driven plugin architecture that allows extending
the broker's functionality without modifying the core codebase. Plugins can:

  - React to broker events (client connections, messages, etc.)
  - Modify broker behavior
  - Implement custom features
  - Integrate with external systems

# Plugin Registry

The PluginRegistry manages the collection of active plugins:

	// Create a plugin registry
	registry := plugin.NewPluginRegistry()

	// Register a plugin
	if err := registry.Register(myPlugin); err != nil {
	    log.Printf("Failed to register plugin: %v", err)
	}

	// Trigger an event on all plugins
	ctx := plugin.NewContext()
	ctx.Set("clientID", "client123")
	registry.TriggerEvent(plugin.EventClientConnect, ctx)

# Plugin Implementation

Plugins are created using the Plugin struct:

	// Create a plugin
	myPlugin := plugin.NewPlugin(
	    "my-plugin",           // ID
	    "My custom plugin",    // Name
	    "1.0.0",               // Version
	    "Plugin Author",       // Author
	)

	// Register event handlers
	myPlugin.OnEvent(plugin.EventClientConnect, func(ctx *plugin.Context) error {
	    clientID := ctx.GetString("clientID")
	    log.Printf("Client connected: %s", clientID)
	    return nil
	})

	myPlugin.OnEvent(plugin.EventMessagePublish, func(ctx *plugin.Context) error {
	    topic := ctx.GetString("topic")
	    payload := ctx.GetBytes("payload")
	    log.Printf("Message published on %s: %s", topic, string(payload))
	    return nil
	})

# Event Types

The package defines standard event types for common broker operations:

  - EventClientConnect: Fired when a client connects
  - EventClientDisconnect: Fired when a client disconnects
  - EventClientAuthenticate: Fired during client authentication
  - EventMessagePublish: Fired when a message is published
  - EventMessageDeliver: Fired when a message is delivered to a subscriber
  - EventSubscribe: Fired when a client subscribes to a topic
  - EventUnsubscribe: Fired when a client unsubscribes from a topic
  - EventRetainedMessageStore: Fired when a retained message is stored
  - EventACLCheck: Fired during access control checks
  - EventServerStart: Fired when the server starts
  - EventServerStop: Fired when the server stops

# Context

The Context struct provides event data and utility methods:

	type Context struct {
	    // Event data
	    data map[string]any

	    // Methods to access data
	    GetString(key string) string
	    GetInt(key string) int
	    GetBool(key string) bool
	    GetBytes(key string) []byte
	    Get(key string) any
	    Set(key string, value any)
	}

# Built-in Plugins

The package includes several built-in plugins:

  - Webhook plugin: Sends HTTP requests for broker events
  - Metrics plugin: Collects and exposes broker metrics
  - Authentication plugin: Provides custom authentication methods
  - Storage plugin: Extends storage capabilities
  - Bridge plugin: Connects to other MQTT brokers

# Examples

Creating a webhook plugin that sends messages to an HTTP endpoint:

	// Create webhook plugin
	webhookPlugin := plugin.NewPlugin(
	    "webhook",
	    "Webhook integration",
	    "1.0.0",
	    "GoMQTT Team",
	)

	// Configure webhook URL
	webhookURL := "https://example.com/webhook"
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Handle message publish events
	webhookPlugin.OnEvent(plugin.EventMessagePublish, func(ctx *plugin.Context) error {
	    // Extract message data
	    topic := ctx.GetString("topic")
	    payload := ctx.GetBytes("payload")
	    clientID := ctx.GetString("clientID")

	    // Create webhook payload
	    webhookData := map[string]any{
	        "event":    "message_publish",
	        "topic":    topic,
	        "payload":  string(payload),
	        "clientID": clientID,
	        "time":     time.Now(),
	    }

	    // Serialize to JSON
	    jsonData, err := json.Marshal(webhookData)
	    if err != nil {
	        return fmt.Errorf("failed to marshal webhook data: %v", err)
	    }

	    // Send HTTP request
	    go func() {
	        resp, err := httpClient.Post(webhookURL, "application/json", bytes.NewReader(jsonData))
	        if err != nil {
	            log.Printf("Webhook request failed: %v", err)
	            return
	        }
	        defer resp.Body.Close()

	        if resp.StatusCode >= 400 {
	            log.Printf("Webhook request returned error: %d", resp.StatusCode)
	        }
	    }()

	    return nil
	})

	// Register the plugin
	registry.Register(webhookPlugin)
*/
package plugin
