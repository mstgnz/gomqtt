# GoMQTT Plugin System

This directory contains the plugin system for GoMQTT, which allows extending the broker's functionality without modifying the core codebase.

## Available Plugins

GoMQTT comes with the following built-in plugins:

| Plugin       | Description                                     | Directory                |
| ------------ | ----------------------------------------------- | ------------------------ |
| Webhook      | Sends HTTP requests when messages are published | [webhook/](webhook/)     |
| Rate Limiter | Limits connection and message rates             | [ratelimit/](ratelimit/) |
| HTTP Auth    | Delegates authentication to an HTTP server      | [auth_http/](auth_http/) |

## Plugin Documentation

For comprehensive documentation on creating and using plugins, see:

- [Plugin Development Guide](plugin-development-guide.md) - Detailed guide on creating plugins
- [plugins.md](plugins.md) - General information about the plugin system

## Plugin Configuration

Plugins are configured in the main configuration file:

```json
{
  "plugins": {
    "enabled": true,
    "directory": "./plugins",
    "autoload": ["webhook", "auth_http"]
  },
  "plugin_webhook": {
    "endpoints": [
      {
        "url": "https://example.com/webhook",
        "topic_filter": "sensors/#",
        "qos": 1,
        "method": "POST",
        "headers": {
          "Authorization": "Bearer your-token",
          "Content-Type": "application/json"
        }
      }
    ]
  },
  "plugin_auth_http": {
    "auth_endpoint": "https://auth.example.com/mqtt/auth",
    "acl_endpoint": "https://auth.example.com/mqtt/acl",
    "timeout": 5,
    "cache_expiry": 300
  },
  "plugin_ratelimit": {
    "connection_rate": 10,
    "publish_rate": 100,
    "subscribe_rate": 20,
    "byte_rate": 10240,
    "window_size": 60
  }
}
```

## Using Plugins

### In Code

```go
import (
	"github.com/mstgnz/gomqtt/plugin"
	"github.com/mstgnz/gomqtt/plugins"
	"github.com/mstgnz/gomqtt/plugins/webhook"
)

// Initialize the plugin system
pluginConfig := plugin.PluginConfig{
	Enabled:   true,
	Directory: "./plugins",
	Autoload:  []string{"webhook", "auth_http"},
}

registry, err := plugins.InitializePlugins(pluginConfig)
if err != nil {
	log.Fatalf("Failed to initialize plugin system: %v", err)
}

// Use the plugin registry
registry.TriggerEvent(&plugin.Context{
	Event:     plugin.EventClientConnect,
	ClientID:  "client123",
	Username:  "user",
	Timestamp: time.Now().Unix(),
})
```

### External Plugins

To use external plugins:

1. Build the plugin as a Go plugin (.so file)
2. Place the .so file in the configured plugins directory
3. Add the plugin name to the `autoload` list in configuration

## Creating a New Plugin

See the [Plugin Development Guide](plugin-development-guide.md) for detailed instructions on creating new plugins.

Basic steps:

1. Create a package for your plugin
2. Implement the plugin interface
3. Register event handlers
4. Export a `New()` function

Example:

```go
package myplugin

import (
	"github.com/mstgnz/gomqtt/plugin"
)

type MyPlugin struct {
	*plugin.BasePlugin
	// your fields here
}

func NewMyPlugin() *MyPlugin {
	p := &MyPlugin{
		BasePlugin: plugin.NewBasePlugin(
			"my_plugin",
			"My custom plugin",
			"1.0.0",
			"Your Name",
		),
	}

	p.RegisterEventHandler(plugin.EventClientConnect, p.handleClientConnect)
	return p
}

func (p *MyPlugin) handleClientConnect(ctx *plugin.Context) error {
	// your implementation
	return nil
}

// This function is called when loading as an external plugin
func New() any {
	return NewMyPlugin()
}
```

## Plugin Examples

Each plugin directory contains example code showing how to use the plugin:

- [webhook/example.go](webhook/example.go)
- [ratelimit/example.go](ratelimit/example.go)
- [auth_http/example.go](auth_http/example.go)

## Plugin System Architecture

The plugin system consists of:

- **plugin/plugin.go**: Core plugin interface and registry
- **plugin/interface.go**: Plugin interface definitions
- **plugin/manager.go**: Plugin loading and configuration
- **plugins/registry.go**: Central registry of all available plugins
