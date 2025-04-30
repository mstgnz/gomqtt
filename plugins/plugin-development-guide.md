# GoMQTT Plugin Development Guide

This guide provides a comprehensive overview of creating plugins for the GoMQTT broker. Plugins allow you to extend the broker's functionality without modifying the core codebase.

## Table of Contents

1. [Plugin System Overview](#plugin-system-overview)
2. [Plugin Architecture](#plugin-architecture)
3. [Creating a Basic Plugin](#creating-a-basic-plugin)
4. [Plugin Events](#plugin-events)
5. [Event Context](#event-context)
6. [Plugin Configuration](#plugin-configuration)
7. [Plugin Distribution](#plugin-distribution)
8. [Built-in Plugins](#built-in-plugins)
9. [Advanced Techniques](#advanced-techniques)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

## Plugin System Overview

The GoMQTT plugin system is built around an event-driven architecture. Plugins can register handlers for various broker events (client connections, message publishing, etc.) and respond to or modify the behavior of these events.

Key components of the plugin system:

- **Plugin**: A self-contained unit that adds functionality to the broker
- **PluginRegistry**: Manages the collection of active plugins
- **Event Handlers**: Functions that respond to specific broker events
- **Context**: Data structure that carries event information

## Plugin Architecture

The plugin system consists of several key components:

### Plugin Interface

All plugins implement the `PluginInterface` which defines the standard methods:

```go
type PluginInterface interface {
    // Plugin returns the underlying plugin
    Plugin() *Plugin

    // Initialize initializes the plugin with configuration
    Initialize(config interface{}) error

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
```

### BasePlugin

The `BasePlugin` provides a common implementation that simplifies creating plugins:

```go
type BasePlugin struct {
    plugin *Plugin
    config interface{}
}
```

### PluginRegistry

The `PluginRegistry` tracks and manages active plugins:

```go
type PluginRegistry struct {
    plugins map[string]*Plugin
    mutex   sync.RWMutex
}
```

### Plugin Manager

The `PluginManager` handles plugin loading and configuration:

```go
type PluginManager struct {
    registry   *PluginRegistry
    config     PluginConfig
    plugins    map[string]interface{}
    configData map[string]interface{}
}
```

## Creating a Basic Plugin

### Step 1: Create a New Plugin File

Start by creating a new Go package for your plugin:

```go
package myplugin

import (
    "log"
    "github.com/mstgnz/gomqtt/plugin"
)

// MyPlugin defines your custom plugin
type MyPlugin struct {
    *plugin.BasePlugin
    // Add custom fields here
    myConfig *MyConfig
    active   bool
}

// MyConfig holds configuration for your plugin
type MyConfig struct {
    Enabled bool   `json:"enabled"`
    Option1 string `json:"option1"`
    Option2 int    `json:"option2"`
}
```

### Step 2: Implement Required Methods

Implement the necessary methods for your plugin:

```go
// NewMyPlugin creates a new instance of your plugin
func NewMyPlugin() *MyPlugin {
    p := &MyPlugin{
        BasePlugin: plugin.NewBasePlugin(
            "my_plugin",                  // Name
            "My custom plugin",           // Description
            "1.0.0",                      // Version
            "Your Name",                  // Author
        ),
        active: true,
    }

    // Register event handlers
    p.RegisterEventHandler(plugin.EventClientConnect, p.handleClientConnect)
    p.RegisterEventHandler(plugin.EventMessagePublish, p.handleMessagePublish)

    return p
}

// Initialize initializes the plugin with configuration
func (p *MyPlugin) Initialize(rawConfig interface{}) error {
    // Parse configuration
    config, ok := rawConfig.(*MyConfig)
    if !ok {
        return fmt.Errorf("invalid configuration type")
    }

    p.myConfig = config
    log.Printf("My plugin initialized with option1=%s, option2=%d",
        config.Option1, config.Option2)

    return nil
}

// Shutdown cleans up resources
func (p *MyPlugin) Shutdown() error {
    p.active = false
    log.Printf("My plugin shut down")
    return nil
}

// New creates a new plugin instance (for external loading)
func New() interface{} {
    return NewMyPlugin()
}
```

### Step 3: Implement Event Handlers

Implement handlers for the events you want to respond to:

```go
// handleClientConnect handles client connect events
func (p *MyPlugin) handleClientConnect(ctx *plugin.Context) error {
    if !p.active {
        return nil
    }

    log.Printf("Client connected: %s (user: %s)", ctx.ClientID, ctx.Username)

    // Your custom logic here

    return nil
}

// handleMessagePublish handles message publish events
func (p *MyPlugin) handleMessagePublish(ctx *plugin.Context) error {
    if !p.active {
        return nil
    }

    log.Printf("Message published: topic=%s, payload=%s",
        ctx.Topic, string(ctx.Payload))

    // Your custom logic here

    return nil
}
```

## Plugin Events

GoMQTT defines several standard events that plugins can handle:

| Event                     | Description                    | Context Properties                             |
| ------------------------- | ------------------------------ | ---------------------------------------------- |
| `EventClientConnect`      | Client connects to broker      | ClientID, Username, Properties (ip, etc.)      |
| `EventClientDisconnect`   | Client disconnects from broker | ClientID, Username, Properties (reason)        |
| `EventMessagePublish`     | Message is published           | ClientID, Topic, Payload, QoS, Retained        |
| `EventMessageReceive`     | Message is delivered to client | ClientID, Topic, Payload, QoS, Retained        |
| `EventSubscribe`          | Client subscribes to topic     | ClientID, Topic, Properties (qos)              |
| `EventUnsubscribe`        | Client unsubscribes from topic | ClientID, Topic                                |
| `EventClientAuthenticate` | Client authentication          | ClientID, Username, Properties (password)      |
| `EventACLCheck`           | Access control check           | ClientID, Username, Topic, Properties (action) |

## Event Context

The `Context` struct provides event data:

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

You can access properties from the context:

```go
func (p *MyPlugin) handleClientConnect(ctx *plugin.Context) error {
    clientID := ctx.ClientID
    username := ctx.Username
    ipAddress, _ := ctx.Properties["ip"].(string)

    // Use the data...

    return nil
}
```

## Plugin Configuration

Plugins can be configured via the broker's configuration file:

```json
{
  "plugins": {
    "enabled": true,
    "directory": "./plugins",
    "autoload": ["my_plugin", "webhook"]
  },
  "plugin_my_plugin": {
    "enabled": true,
    "option1": "value1",
    "option2": 42
  }
}
```

Your plugin should parse this configuration in its `Initialize` method:

```go
func (p *MyPlugin) Initialize(rawConfig interface{}) error {
    config, ok := rawConfig.(*MyConfig)
    if !ok {
        return fmt.Errorf("invalid configuration type")
    }

    p.myConfig = config
    return nil
}
```

## Plugin Distribution

### Built-in Plugins

Built-in plugins are compiled directly into the broker. To include your plugin as a built-in plugin:

1. Place your plugin code in a subdirectory of the `plugins` directory
2. Register your plugin in the broker's startup code

### External Plugins (Go Plugins)

Go plugins are dynamically loaded at runtime:

1. Build your plugin as a Go plugin (.so file):

```bash
go build -buildmode=plugin -o my_plugin.so my_plugin.go
```

2. Place the .so file in the plugins directory
3. Enable in configuration:

```json
{
  "plugins": {
    "enabled": true,
    "directory": "./plugins",
    "autoload": ["my_plugin"]
  }
}
```

## Built-in Plugins

GoMQTT includes several built-in plugins that you can use as references:

### Webhook Plugin

Sends HTTP notifications when messages are published:

```go
// Example of using the webhook plugin
webhookPlugin := webhook.NewWebhookPlugin()

config := &webhook.WebhookConfig{
    Endpoints: []webhook.EndpointConfig{
        {
            URL:         "https://example.com/webhook",
            TopicFilter: "sensors/#",
            Method:      "POST",
            QoS:         1,
            Enabled:     true,
        },
    },
}

webhookPlugin.Initialize(config)
```

### Rate Limit Plugin

Controls connection and message rates:

```go
// Example of using the rate limit plugin
rateLimitPlugin := ratelimit.NewRateLimitPlugin()

config := &ratelimit.RateLimitConfig{
    ConnectionRate: 10,    // 10 connections per second per IP
    PublishRate:    100,   // 100 publish operations per second
    SubscribeRate:  20,    // 20 subscribe operations per second
    WindowSize:     60,    // 60 second window
}

rateLimitPlugin.Initialize(config)
```

### HTTP Auth Plugin

Delegates authentication to an external HTTP service:

```go
// Example of using the HTTP auth plugin
httpAuthPlugin := auth_http.NewHTTPAuthPlugin()

config := &auth_http.AuthConfig{
    AuthEndpoint: "https://auth.example.com/mqtt/auth",
    ACLEndpoint:  "https://auth.example.com/mqtt/acl",
    Timeout:      5,
    CacheExpiry:  300,
}

httpAuthPlugin.Initialize(config)
```

## Advanced Techniques

### Creating a Bridge Plugin

A bridge plugin can connect your broker to other MQTT brokers:

```go
type BridgePlugin struct {
    *plugin.BasePlugin
    clients    map[string]*mqtt.Client
    config     *BridgeConfig
    active     bool
    mutex      sync.RWMutex
}

// BridgeConfig holds configuration for bridge connections
type BridgeConfig struct {
    Connections []BridgeConnection `json:"connections"`
}

// BridgeConnection defines a connection to another broker
type BridgeConnection struct {
    Name      string   `json:"name"`
    Broker    string   `json:"broker"`
    ClientID  string   `json:"client_id"`
    Username  string   `json:"username"`
    Password  string   `json:"password"`
    Topics    []string `json:"topics"`
    QoS       byte     `json:"qos"`
    CleanSession bool  `json:"clean_session"`
}
```

### Creating a Storage Plugin

A storage plugin can implement custom message storage:

```go
type StoragePlugin struct {
    *plugin.BasePlugin
    db        *sql.DB
    config    *StorageConfig
    active    bool
    mutex     sync.RWMutex
}

// StorageConfig holds configuration for storage
type StorageConfig struct {
    ConnectionString string `json:"connection_string"`
    TableName        string `json:"table_name"`
    BatchSize        int    `json:"batch_size"`
    FlushInterval    int    `json:"flush_interval"`
}
```

## Best Practices

### Memory Management

- Use goroutines carefully for asynchronous operations
- Be aware of memory usage for high-volume events
- Clean up resources in the `Shutdown` method

```go
func (p *MyPlugin) Shutdown() error {
    // Close any open connections
    // Stop any running goroutines
    // Clean up resources
    return nil
}
```

### Error Handling

- Handle errors gracefully to avoid affecting broker operation
- Log errors with appropriate context
- Return meaningful error messages

```go
func (p *MyPlugin) handleSomeEvent(ctx *plugin.Context) error {
    // If something goes wrong
    if err != nil {
        log.Printf("Error in MyPlugin: %v", err)
        return fmt.Errorf("my plugin error: %w", err)
    }
    return nil
}
```

### Thread Safety

Ensure thread safety when accessing shared resources:

```go
func (p *MyPlugin) updateSomething(key string, value interface{}) {
    p.mutex.Lock()
    defer p.mutex.Unlock()

    // Update shared resource
    p.sharedMap[key] = value
}
```

### Performance

- Optimize handlers for frequently triggered events
- Use caching for expensive operations
- Benchmark your plugin under load

## Troubleshooting

### Common Issues

1. **Plugin not loading**

   - Check file permissions
   - Verify plugin is in the correct directory
   - Ensure plugin exports the `New` function

2. **Configuration errors**

   - Check JSON syntax in configuration file
   - Verify configuration structure matches plugin expectations

3. **Event handlers not being called**
   - Verify event name is correct
   - Check if plugin is properly registered

### Debugging Techniques

1. **Enable debug logging**

   ```json
   {
     "logging": {
       "level": "debug"
     }
   }
   ```

2. **Add context to log messages**

   ```go
   log.Printf("[MyPlugin] Processing event %s for client %s", ctx.Event, ctx.ClientID)
   ```

3. **Test event handlers in isolation**

   ```go
   func TestMyPluginHandlers(t *testing.T) {
       p := NewMyPlugin()
       ctx := &plugin.Context{
           Event:     plugin.EventClientConnect,
           ClientID:  "test-client",
           Username:  "test-user",
           Timestamp: time.Now().Unix(),
       }

       err := p.handleClientConnect(ctx)
       if err != nil {
           t.Errorf("Handler returned error: %v", err)
       }
   }
   ```

---

By following this guide, you should now be able to create, configure, and distribute plugins for the GoMQTT broker. The plugin system provides a powerful way to extend the broker's functionality while keeping the core codebase clean and focused.
