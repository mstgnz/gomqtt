package webhook

import (
	"log"

	"github.com/mstgnz/gomqtt/plugin"
)

// Example usage of the webhook plugin

// SetupWebhookPlugin configures and registers the webhook plugin
func SetupWebhookPlugin(registry *plugin.PluginRegistry) *WebhookPlugin {
	// Create the webhook plugin
	webhookPlugin := NewWebhookPlugin()

	// Configure the plugin
	config := &WebhookConfig{
		Timeout:       10,
		RetryCount:    3,
		RetryInterval: 5,
		Endpoints: []EndpointConfig{
			{
				URL:         "https://example.com/webhooks/temperature",
				TopicFilter: "sensors/temperature/#",
				Method:      "POST",
				QoS:         1,
				Enabled:     true,
				Headers: map[string]string{
					"Authorization": "Bearer your-token",
					"X-Custom":      "value",
				},
			},
			{
				URL:         "https://example.com/webhooks/humidity",
				TopicFilter: "sensors/humidity/#",
				Method:      "POST",
				QoS:         0,
				Enabled:     true,
			},
			{
				URL:         "https://example.com/webhooks/all",
				TopicFilter: "#",
				Method:      "POST",
				QoS:         0,
				Enabled:     true,
				Template:    `{"topic":"{{.Topic}}","payload":{{.Payload}}}`,
			},
		},
	}

	// Initialize the plugin
	err := webhookPlugin.Initialize(config)
	if err != nil {
		log.Printf("Failed to initialize webhook plugin: %v", err)
		return nil
	}

	// Register the plugin with the registry
	err = registry.Register(webhookPlugin.Plugin())
	if err != nil {
		log.Printf("Failed to register webhook plugin: %v", err)
	} else {
		log.Printf("Webhook plugin registered successfully")
	}

	return webhookPlugin
}

// Example of how to use the plugin programmatically
func ExampleUseWebhookPlugin() {
	// Create a plugin registry
	registry := plugin.NewPluginRegistry()

	// Setup the webhook plugin
	webhookPlugin := SetupWebhookPlugin(registry)
	if webhookPlugin == nil {
		log.Printf("Failed to setup webhook plugin")
		return
	}

	// Example of triggering events that will be processed by the plugin
	// In a real application, these events would be triggered by the MQTT broker
	registry.TriggerEvent(&plugin.Context{
		Event:     plugin.EventMessagePublish,
		ClientID:  "device123",
		Username:  "device",
		Topic:     "sensors/temperature/room1",
		Payload:   []byte(`{"temperature": 22.5}`),
		QoS:       1,
		Retained:  false,
		Timestamp: 1625482000,
	})
}
