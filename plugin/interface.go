package plugin

// PluginInterface defines the standard interface for all plugins
type PluginInterface interface {
	// Plugin returns the underlying plugin
	Plugin() *Plugin

	// Initialize initializes the plugin with configuration
	Initialize(config any) error

	// Name returns the plugin name
	Name() string

	// Version returns the plugin version
	Version() string

	// Description returns the plugin description
	Description() string

	// Author returns the plugin author
	Author() string

	// Shutdown cleans up resources when the broker is shut down
	Shutdown() error
}

// BasePlugin provides a common implementation for plugins
type BasePlugin struct {
	plugin *Plugin
	config any
}

// NewBasePlugin creates a new base plugin
func NewBasePlugin(name, description, version, author string) *BasePlugin {
	return &BasePlugin{
		plugin: NewPlugin(name, description, version, author),
	}
}

// Plugin returns the underlying plugin
func (b *BasePlugin) Plugin() *Plugin {
	return b.plugin
}

// Initialize initializes the plugin with configuration
func (b *BasePlugin) Initialize(config any) error {
	b.config = config
	return nil
}

// Name returns the plugin name
func (b *BasePlugin) Name() string {
	return b.plugin.Name
}

// Version returns the plugin version
func (b *BasePlugin) Version() string {
	return b.plugin.Version
}

// Description returns the plugin description
func (b *BasePlugin) Description() string {
	return b.plugin.Description
}

// Author returns the plugin author
func (b *BasePlugin) Author() string {
	return b.plugin.Author
}

// Shutdown cleans up resources
func (b *BasePlugin) Shutdown() error {
	return nil
}

// RegisterEventHandler registers a handler for an event
func (b *BasePlugin) RegisterEventHandler(event string, handler Handler) {
	b.plugin.OnEvent(event, handler)
}
