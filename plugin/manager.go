package plugin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"plugin"
	"strings"
)

// PluginConfig holds the plugin system configuration
type PluginConfig struct {
	Enabled   bool     `json:"enabled"`
	Directory string   `json:"directory"`
	Autoload  []string `json:"autoload"`
}

// PluginManager manages the plugin lifecycle
type PluginManager struct {
	registry   *PluginRegistry
	config     PluginConfig
	plugins    map[string]interface{}
	configData map[string]interface{}
}

// NewPluginManager creates a new plugin manager
func NewPluginManager(registry *PluginRegistry, config PluginConfig) *PluginManager {
	return &PluginManager{
		registry:   registry,
		config:     config,
		plugins:    make(map[string]interface{}),
		configData: make(map[string]interface{}),
	}
}

// LoadPluginConfig loads plugin specific configuration
func (m *PluginManager) LoadPluginConfig(configFile string) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse the main config to get plugin specific sections
	var config map[string]interface{}
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	// Store each plugin's config section
	for key, value := range config {
		if strings.HasPrefix(key, "plugin_") {
			pluginName := strings.TrimPrefix(key, "plugin_")
			m.configData[pluginName] = value
		}
	}

	return nil
}

// GetPluginConfig retrieves configuration for a specific plugin
func (m *PluginManager) GetPluginConfig(pluginName string) interface{} {
	return m.configData[pluginName]
}

// LoadBuiltinPlugins loads internal plugins that are compiled with the broker
func (m *PluginManager) LoadBuiltinPlugins(plugins map[string]PluginConstructor) error {
	for name, constructor := range plugins {
		// Skip plugins not in autoload if autoload is specified
		if len(m.config.Autoload) > 0 {
			found := false
			for _, autoload := range m.config.Autoload {
				if autoload == name {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Create the plugin instance
		plugin := constructor()

		// Store the plugin instance
		m.plugins[name] = plugin

		// Register with the registry
		if err := m.registry.Register(plugin.Plugin()); err != nil {
			log.Printf("Failed to register plugin %s: %v", name, err)
			continue
		}

		log.Printf("Loaded built-in plugin: %s", name)
	}

	return nil
}

// LoadExternalPlugins loads plugins from the plugin directory
func (m *PluginManager) LoadExternalPlugins() error {
	if m.config.Directory == "" {
		return nil
	}

	// Get list of .so files in the plugin directory
	pattern := filepath.Join(m.config.Directory, "*.so")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to scan plugin directory: %w", err)
	}

	for _, file := range files {
		// Skip plugins not in autoload if autoload is specified
		filename := filepath.Base(file)
		pluginName := strings.TrimSuffix(filename, filepath.Ext(filename))

		if len(m.config.Autoload) > 0 {
			found := false
			for _, autoload := range m.config.Autoload {
				if autoload == pluginName {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Load the plugin
		p, err := plugin.Open(file)
		if err != nil {
			log.Printf("Failed to load plugin %s: %v", file, err)
			continue
		}

		// Look up the plugin's exported symbol
		sym, err := p.Lookup("New")
		if err != nil {
			log.Printf("Plugin %s does not export 'New' symbol: %v", file, err)
			continue
		}

		// Convert to the expected constructor function type
		constructor, ok := sym.(func() interface{})
		if !ok {
			log.Printf("Plugin %s 'New' symbol is not a constructor function", file)
			continue
		}

		// Create plugin instance
		instance := constructor()

		// Check if the instance implements the Plugin method
		pluginGetter, ok := instance.(interface{ Plugin() *Plugin })
		if !ok {
			log.Printf("Plugin %s does not implement Plugin() method", file)
			continue
		}

		// Get the actual plugin and register it
		plugin := pluginGetter.Plugin()
		if err := m.registry.Register(plugin); err != nil {
			log.Printf("Failed to register plugin %s: %v", file, err)
			continue
		}

		log.Printf("Loaded external plugin: %s", file)
	}

	return nil
}

// PluginConstructor is a function that creates a new plugin instance
type PluginConstructor func() interface {
	Plugin() *Plugin
}

// Shutdown cleanly shutdowns all plugins
func (m *PluginManager) Shutdown() {
	// Nothing to do here currently, but could be expanded
	// to call cleanup methods on plugins that implement them
	log.Println("Shutting down plugin system")
}
