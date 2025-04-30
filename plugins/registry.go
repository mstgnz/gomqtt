package plugins

import (
	"log"
	"sync"

	"github.com/mstgnz/gomqtt/plugin"
	"github.com/mstgnz/gomqtt/plugins/auth_http"
	"github.com/mstgnz/gomqtt/plugins/ratelimit"
	"github.com/mstgnz/gomqtt/plugins/webhook"
)

var (
	// Ensure initialization happens only once
	once     sync.Once
	registry *plugin.PluginRegistry
	manager  *plugin.PluginManager
)

// AvailablePlugins lists all built-in plugins with their constructors
var AvailablePlugins = map[string]plugin.PluginConstructor{
	"webhook":   func() interface{ Plugin() *plugin.Plugin } { return webhook.NewWebhookPlugin() },
	"auth_http": func() interface{ Plugin() *plugin.Plugin } { return auth_http.NewHTTPAuthPlugin() },
	"ratelimit": func() interface{ Plugin() *plugin.Plugin } { return ratelimit.NewRateLimitPlugin() },
}

// InitializePlugins initializes the plugin system
func InitializePlugins(config plugin.PluginConfig) (*plugin.PluginRegistry, error) {
	once.Do(func() {
		registry = plugin.NewPluginRegistry()
		manager = plugin.NewPluginManager(registry, config)

		// Load built-in plugins
		if err := manager.LoadBuiltinPlugins(AvailablePlugins); err != nil {
			log.Printf("Error loading built-in plugins: %v", err)
		}

		// Load external plugins
		if err := manager.LoadExternalPlugins(); err != nil {
			log.Printf("Error loading external plugins: %v", err)
		}
	})

	return registry, nil
}

// GetPluginRegistry returns the plugin registry
func GetPluginRegistry() *plugin.PluginRegistry {
	if registry == nil {
		// Create a default registry if it doesn't exist
		registry = plugin.NewPluginRegistry()
	}
	return registry
}

// GetPluginManager returns the plugin manager
func GetPluginManager() *plugin.PluginManager {
	return manager
}

// LoadPluginConfig loads plugin configuration
func LoadPluginConfig(configFile string) error {
	if manager == nil {
		return nil
	}
	return manager.LoadPluginConfig(configFile)
}

// ShutdownPlugins gracefully shuts down all plugins
func ShutdownPlugins() {
	if manager != nil {
		manager.Shutdown()
	}
}
