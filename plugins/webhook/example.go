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

	// Register sample webhook endpoints
	// In a real application, these would be loaded from configuration
	webhookPlugin.RegisterEndpoint("sensors/temperature", "https://example.com/webhooks/temperature")
	webhookPlugin.RegisterEndpoint("sensors/humidity", "https://example.com/webhooks/humidity")
	webhookPlugin.RegisterEndpoint("#", "https://example.com/webhooks/all-messages") // Special case: all topics

	// Register the plugin with the registry
	err := registry.Register(webhookPlugin.Plugin())
	if err != nil {
		log.Printf("Failed to register webhook plugin: %v", err)
	} else {
		log.Printf("Webhook plugin registered successfully")
	}

	return webhookPlugin
}
