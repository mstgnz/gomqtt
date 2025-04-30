# GoMQTT Plugin Core

This package provides the core functionality for the GoMQTT plugin system.

## Components

### Plugin

The `Plugin` struct represents a plugin that can be registered with the broker:

```go
type Plugin struct {
	Name        string
	Description string
	Version     string
	Author      string
	Handlers    map[string][]Handler
}
```

### PluginRegistry

The `PluginRegistry` manages a collection of plugins:

```go
type PluginRegistry struct {
	plugins map[string]*Plugin
	mutex   sync.RWMutex
}
```

### Context

The `Context` struct is passed to plugin handlers with event information:

```go
type Context struct {
	Event      string
	ClientID   string
	Username   string
	Topic      string
	Payload    []byte
	Timestamp  int64
	QoS        byte
	Retained   bool
	Properties map[string]any
}
```

### PluginInterface

The `PluginInterface` defines the standard plugin interface:

```go
type PluginInterface interface {
	Plugin() *Plugin
	Initialize(config any) error
	Name() string
	Version() string
	Description() string
	Author() string
	Shutdown() error
}
```

### BasePlugin

The `BasePlugin` provides a common implementation for all plugins:

```go
type BasePlugin struct {
	plugin *Plugin
	config any
}
```

### PluginManager

The `PluginManager` handles loading and configuring plugins:

```go
type PluginManager struct {
	registry   *PluginRegistry
	config     PluginConfig
	plugins    map[string]any
	configData map[string]any
}
```

## Usage

### Creating a Plugin Registry

```go
registry := plugin.NewPluginRegistry()
```

### Creating a Plugin

```go
p := plugin.NewPlugin(
	"my_plugin",
	"My custom plugin",
	"1.0.0",
	"Plugin Author",
)
```

### Registering Event Handlers

```go
p.OnEvent(plugin.EventClientConnect, func(ctx *plugin.Context) error {
	// Handle connect event
	return nil
})
```

### Triggering Events

```go
ctx := &plugin.Context{
	Event:     plugin.EventMessagePublish,
	ClientID:  "client123",
	Topic:     "sensors/temperature",
	Payload:   []byte(`{"temperature": 22.5}`),
	Timestamp: time.Now().Unix(),
}

errors := registry.TriggerEvent(ctx)
```

### Using the Plugin Manager

```go
config := plugin.PluginConfig{
	Enabled:   true,
	Directory: "./plugins",
	Autoload:  []string{"webhook"},
}

manager := plugin.NewPluginManager(registry, config)

// Load built-in plugins
manager.LoadBuiltinPlugins(builtinPlugins)

// Load external plugins
manager.LoadExternalPlugins()
```

## Events

Standard events available in the plugin system:

- `EventClientConnect`: Client connects to broker
- `EventClientDisconnect`: Client disconnects from broker
- `EventMessagePublish`: Message is published
- `EventMessageReceive`: Message is delivered to client
- `EventSubscribe`: Client subscribes to topic
- `EventUnsubscribe`: Client unsubscribes from topic

## File Structure

- **plugin.go**: Core plugin implementation
- **interface.go**: Plugin interface definitions
- **manager.go**: Plugin loading and configuration
- **doc.go**: Package documentation

## Integration with MQTT Broker

To integrate plugins with the MQTT broker, the broker needs to trigger events at appropriate points in the message flow. For example:

```go
// When a client connects
registry.TriggerEvent(&plugin.Context{
	Event:     plugin.EventClientConnect,
	ClientID:  clientID,
	Username:  username,
	Timestamp: time.Now().Unix(),
	Properties: map[string]any{
		"ip": ipAddress,
	},
})

// When a message is published
registry.TriggerEvent(&plugin.Context{
	Event:     plugin.EventMessagePublish,
	ClientID:  clientID,
	Topic:     topic,
	Payload:   payload,
	QoS:       qos,
	Retained:  retained,
	Timestamp: time.Now().Unix(),
})
```
