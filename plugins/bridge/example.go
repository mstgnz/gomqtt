package bridge

import (
	"log"

	"github.com/mstgnz/gomqtt/plugin"
)

// SetupBridgePlugin configures and registers the bridge plugin
func SetupBridgePlugin(registry *plugin.PluginRegistry) *BridgePlugin {
	// Create the bridge plugin
	bridgePlugin := NewBridgePlugin()

	// Configure the plugin
	config := &BridgeConfig{
		Timeout: 10,
		Bridges: []BridgeEndpoint{
			{
				Name:        "http-bridge",
				Type:        "http",
				URL:         "https://api.example.com/mqtt-gateway",
				Method:      "POST",
				TopicFilter: "sensors/#",
				QoS:         1,
				Enabled:     true,
				Headers: map[string]string{
					"Authorization": "Bearer your-api-token",
					"X-Source":      "mqtt-bridge",
				},
			},
			{
				Name:        "coap-bridge",
				Type:        "coap",
				URL:         "coap://iot.example.com:5683",
				TopicFilter: "devices/#",
				QoS:         0,
				Enabled:     true,
			},
		},
	}

	// Initialize the plugin
	err := bridgePlugin.Initialize(config)
	if err != nil {
		log.Printf("Failed to initialize bridge plugin: %v", err)
		return nil
	}

	// Register the plugin with the registry
	err = registry.Register(bridgePlugin.Plugin())
	if err != nil {
		log.Printf("Failed to register bridge plugin: %v", err)
	} else {
		log.Printf("Bridge plugin registered successfully")
	}

	return bridgePlugin
}

// ExampleUseBridgePlugin demonstrates how to use the bridge plugin
func ExampleUseBridgePlugin() {
	// Create a plugin registry
	registry := plugin.NewPluginRegistry()

	// Setup the bridge plugin
	bridgePlugin := SetupBridgePlugin(registry)
	if bridgePlugin == nil {
		log.Printf("Failed to setup bridge plugin")
		return
	}

	// Example of triggering a message publish event that will be bridged
	registry.TriggerEvent(&plugin.Context{
		Event:     plugin.EventMessagePublish,
		ClientID:  "device123",
		Username:  "device",
		Topic:     "sensors/temperature",
		Payload:   []byte(`{"temperature": 22.5, "humidity": 45.2}`),
		QoS:       1,
		Retained:  false,
		Timestamp: 1625482000,
	})

	// Example of triggering another message event for a different bridge
	registry.TriggerEvent(&plugin.Context{
		Event:     plugin.EventMessagePublish,
		ClientID:  "deviceABC",
		Username:  "device",
		Topic:     "devices/livingroom/light",
		Payload:   []byte(`{"state": "ON", "brightness": 80}`),
		QoS:       0,
		Retained:  true,
		Timestamp: 1625482010,
	})
}
