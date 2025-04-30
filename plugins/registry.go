package plugins

import (
	"fmt"
	"log"
	"plugin"
	"sync"

	gomqttplugin "github.com/mstgnz/gomqtt/plugin"
	"github.com/mstgnz/gomqtt/plugins/auth_http"
	"github.com/mstgnz/gomqtt/plugins/bridge"
	"github.com/mstgnz/gomqtt/plugins/dos_protection"
	"github.com/mstgnz/gomqtt/plugins/ratelimit"
	"github.com/mstgnz/gomqtt/plugins/transform"
	"github.com/mstgnz/gomqtt/plugins/webhook"
)

var (
	// Ensure initialization happens only once
	once     sync.Once
	registry *gomqttplugin.PluginRegistry
	manager  *gomqttplugin.PluginManager
)

// AvailablePlugins lists all built-in plugins with their constructors
var AvailablePlugins = map[string]gomqttplugin.PluginConstructor{
	"webhook":        func() interface{ Plugin() *gomqttplugin.Plugin } { return webhook.NewWebhookPlugin() },
	"auth_http":      func() interface{ Plugin() *gomqttplugin.Plugin } { return auth_http.NewHTTPAuthPlugin() },
	"ratelimit":      func() interface{ Plugin() *gomqttplugin.Plugin } { return ratelimit.NewRateLimitPlugin() },
	"dos_protection": func() interface{ Plugin() *gomqttplugin.Plugin } { return dos_protection.NewDoSProtectionPlugin() },
}

// PluginLoader defines a function that creates a plugin instance
type PluginLoader func() any

// BuiltinPlugins is a map of internal plugin names to their loader functions
var BuiltinPlugins = map[string]PluginLoader{
	"ratelimit":      ratelimit.New,
	"webhook":        webhook.New,
	"auth_http":      auth_http.New,
	"transform":      transform.New,
	"bridge":         bridge.New,
	"dos_protection": dos_protection.New,
}

// init initializes the plugin registry
func init() {
	// Register built-in plugins
	once.Do(func() {
		log.Println("Registering built-in plugins...")

		// External plugins are configured in InitializePlugins instead
		// This code was causing "undefined: config" error
		/*
			if config.ExternalPluginsEnabled {
				if err := loadExternalPlugins(); err != nil {
					log.Printf("Error loading external plugins: %v", err)
				}
			}
		*/

		// Register transform plugin when it's implemented
		// This would need to follow the same pattern as other plugins
		// We'd need to adjust the transform plugin to match the expected interface
	})
}

// InitializePlugins initializes the plugin system
func InitializePlugins(config gomqttplugin.PluginConfig) (*gomqttplugin.PluginRegistry, error) {
	once.Do(func() {
		registry = gomqttplugin.NewPluginRegistry()
		manager = gomqttplugin.NewPluginManager(registry, config)

		// Load built-in plugins
		if err := manager.LoadBuiltinPlugins(AvailablePlugins); err != nil {
			log.Printf("Error loading built-in plugins: %v", err)
		}

		// Load external plugins
		if err := manager.LoadExternalPlugins(); err != nil {
			log.Printf("Error loading external plugins: %v", err)
		}

		// Transform plugin needs to be adapted to match the PluginConstructor interface
		// AvailablePlugins["transform"] = transform.New
	})

	return registry, nil
}

// GetPluginRegistry returns the plugin registry
func GetPluginRegistry() *gomqttplugin.PluginRegistry {
	if registry == nil {
		// Create a default registry if it doesn't exist
		registry = gomqttplugin.NewPluginRegistry()
	}
	return registry
}

// GetPluginManager returns the plugin manager
func GetPluginManager() *gomqttplugin.PluginManager {
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

// RegisterPlugin registers a new plugin
func RegisterPlugin(name string, constructor gomqttplugin.PluginConstructor) {
	AvailablePlugins[name] = constructor
}

// LoadBuiltinPlugin loads a built-in plugin by name
func LoadBuiltinPlugin(name string) (gomqttplugin.PluginInterface, error) {
	loader, exists := BuiltinPlugins[name]
	if !exists {
		return nil, fmt.Errorf("plugin %s not found", name)
	}

	instance := loader()
	p, ok := instance.(gomqttplugin.PluginInterface)
	if !ok {
		return nil, fmt.Errorf("plugin %s does not implement the PluginInterface", name)
	}

	return p, nil
}

// LoadExternalPlugin loads a plugin from a .so file
func LoadExternalPlugin(path string) (gomqttplugin.PluginInterface, error) {
	// Load the plugin
	plug, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin %s: %w", path, err)
	}

	// Look up the New symbol
	sym, err := plug.Lookup("New")
	if err != nil {
		return nil, fmt.Errorf("plugin %s does not export 'New' symbol: %w", path, err)
	}

	// Check if the symbol is a function
	newFunc, ok := sym.(func() any)
	if !ok {
		return nil, fmt.Errorf("plugin %s 'New' symbol is not a function: %w", path, err)
	}

	// Call the function to get the plugin instance
	instance := newFunc()
	p, ok := instance.(gomqttplugin.PluginInterface)
	if !ok {
		return nil, fmt.Errorf("plugin %s does not implement PluginInterface", path)
	}

	log.Printf("Loaded external plugin: %s (%s)", p.Name(), p.Description())
	return p, nil
}
