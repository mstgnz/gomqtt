# 🔌 GoMQTT Plugin System

GoMQTT features a flexible plugin system that allows you to extend the broker's functionality. This document explains how to use and develop plugins for GoMQTT.

## Plugin Overview

Plugins in GoMQTT can:

- Process messages as they flow through the broker
- Authenticate and authorize clients
- Store and retrieve messages
- Add custom API endpoints
- Monitor broker events
- Integrate with external systems

## Built-in Plugins

GoMQTT comes with several built-in plugins:

| Plugin           | Description                                |
| ---------------- | ------------------------------------------ |
| `webhook`        | Sends HTTP requests on MQTT events         |
| `auth_http`      | Delegates authentication to an HTTP server |
| `prometheus`     | Exports metrics to Prometheus              |
| `logging`        | Enhanced logging capabilities              |
| `bridge`         | Bridges messages between brokers           |
| `rate_limiter`   | Limits connection and message rates        |
| `dos_protection` | Advanced DoS attack prevention system      |

## Enabling Plugins

To enable plugins, update your configuration file:

```json
{
  "plugins": {
    "enabled": true,
    "directory": "./plugins",
    "autoload": ["webhook", "auth_http"]
  }
}
```

- `enabled`: Set to `true` to enable the plugin system
- `directory`: Directory containing plugin files
- `autoload`: List of plugins to load automatically on startup

## Plugin Configuration

Each plugin can have its own configuration section:

```json
{
  "plugins": {
    "enabled": true,
    "directory": "./plugins",
    "autoload": ["webhook"]
  },
  "webhook": {
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
  }
}
```

## Built-in Plugin Configuration

### Webhook Plugin

The webhook plugin sends HTTP requests when messages are published to specific topics:

```json
{
  "webhook": {
    "endpoints": [
      {
        "url": "https://example.com/webhook",
        "topic_filter": "sensors/#",
        "qos": 1,
        "method": "POST",
        "headers": {
          "Authorization": "Bearer your-token",
          "Content-Type": "application/json"
        },
        "template": "{\"topic\":\"{{.Topic}}\",\"payload\":{{.Payload}},\"qos\":{{.QoS}},\"timestamp\":\"{{.Timestamp}}\"}"
      }
    ],
    "timeout": 5,
    "retry_count": 3,
    "retry_interval": 10
  }
}
```

- `endpoints`: List of webhook endpoints
  - `url`: The target URL to send requests to
  - `topic_filter`: MQTT topic filter to match (supports wildcards)
  - `qos`: Minimum QoS level to trigger the webhook
  - `method`: HTTP method (GET, POST, PUT, etc.)
  - `headers`: HTTP headers to include
  - `template`: Message template (using Go template syntax)
- `timeout`: Request timeout in seconds
- `retry_count`: Number of retry attempts for failed requests
- `retry_interval`: Interval between retries in seconds

### Auth HTTP Plugin

The Auth HTTP plugin delegates authentication and authorization to an external HTTP service:

```json
{
  "auth_http": {
    "auth_endpoint": "https://auth.example.com/mqtt/auth",
    "acl_endpoint": "https://auth.example.com/mqtt/acl",
    "timeout": 5,
    "cache_expiry": 300,
    "headers": {
      "X-API-Key": "your-api-key"
    }
  }
}
```

- `auth_endpoint`: URL for authentication requests
- `acl_endpoint`: URL for authorization (ACL) requests
- `timeout`: Request timeout in seconds
- `cache_expiry`: Authentication cache expiry in seconds
- `headers`: Additional HTTP headers to send with requests

### Rate Limiter Plugin

The rate limiter plugin limits connection and message rates:

```json
{
  "rate_limiter": {
    "connection_rate": 10,
    "publish_rate": 100,
    "subscribe_rate": 20,
    "byte_rate": 10240,
    "window_size": 60,
    "ip_whitelist": ["127.0.0.1", "192.168.1.0/24"]
  }
}
```

- `connection_rate`: Maximum connections per second per IP
- `publish_rate`: Maximum publish operations per second per client
- `subscribe_rate`: Maximum subscribe operations per second per client
- `byte_rate`: Maximum bytes per second per client
- `window_size`: Time window for rate calculation in seconds
- `ip_whitelist`: List of IPs or CIDR ranges exempt from rate limiting

### DoS Protection Plugin

The DoS protection plugin provides advanced protection against denial of service attacks:

```json
{
  "dos_protection": {
    "enabled": true,
    "connection_rate": 20,
    "connection_burst": 30,
    "publish_rate": 100,
    "subscribe_rate": 30,
    "byte_rate": 1048576,
    "window_size": 60,
    "ip_whitelist": ["127.0.0.1", "192.168.0.0/24"],
    "max_connections_per_ip": 10,
    "temporary_ban_duration": "5m",
    "failed_auth_threshold": 3,
    "connection_flood_interval": "10s",
    "connection_flood_count": 30,
    "global_connection_rate": 200,
    "progressive_ban_enabled": true,
    "max_ban_duration": "24h",
    "enable_logging": true
  }
}
```

- `connection_rate`: Maximum connection attempts per IP in window
- `connection_burst`: Maximum burst of connections allowed
- `publish_rate`: Maximum publish messages per client in window
- `subscribe_rate`: Maximum subscribe requests per client in window
- `byte_rate`: Maximum bytes per client in window
- `window_size`: Time window in seconds for rate counting
- `ip_whitelist`: IP addresses/CIDR ranges exempt from protection
- `max_connections_per_ip`: Maximum concurrent connections per IP
- `temporary_ban_duration`: Duration for temporary bans
- `failed_auth_threshold`: Failed authentication attempts before ban
- `connection_flood_interval`: Time window for connection flood detection
- `connection_flood_count`: Connections in interval to trigger flood protection
- `global_connection_rate`: Global limit for connections across all IPs
- `progressive_ban_enabled`: Enable escalating ban durations for repeat offenders
- `max_ban_duration`: Maximum ban duration
- `enable_logging`: Enable detailed DoS protection logging

## Developing Custom Plugins

GoMQTT plugins are written in Go and implement the Plugin interface:

```go
// Plugin interface for the broker
type Plugin interface {
	// Name returns the name of the plugin
	Name() string

	// Initialize initializes the plugin with the broker and config
	Initialize(broker *Broker, config any) error

	// Enabled returns whether the plugin is enabled
	Enabled() bool

	// Shutdown cleans up resources when the broker shuts down
	Shutdown() error
}
```

### Plugin Hooks

Plugins can register for various hooks:

- `OnClientConnect`: Called when a client connects
- `OnClientDisconnect`: Called when a client disconnects
- `OnClientAuthenticate`: Called to authenticate a client
- `OnClientAuthorize`: Called to authorize client actions
- `OnMessagePublish`: Called when a message is published
- `OnMessageDeliver`: Called before a message is delivered to a client
- `OnSubscribe`: Called when a client subscribes to a topic
- `OnUnsubscribe`: Called when a client unsubscribes from a topic

### Example Plugin Template

Here's a basic template for a custom plugin:

```go
package plugins

import (
	"github.com/mstgnz/gomqtt/mqtt"
)

// MyPlugin is a custom plugin implementation
type MyPlugin struct {
	enabled bool
	broker  *mqtt.Broker
	config  *MyPluginConfig
}

// MyPluginConfig holds configuration for MyPlugin
type MyPluginConfig struct {
	Enabled bool `json:"enabled"`
	Param1  string `json:"param1"`
	Param2  int    `json:"param2"`
}

// Name returns the plugin name
func (p *MyPlugin) Name() string {
	return "my_plugin"
}

// Initialize sets up the plugin
func (p *MyPlugin) Initialize(broker *mqtt.Broker, rawConfig any) error {
	p.broker = broker

	// Parse configuration
	if config, ok := rawConfig.(*MyPluginConfig); ok {
		p.config = config
		p.enabled = config.Enabled
	}

	// Register hooks
	broker.RegisterHook("message_publish", p.onMessagePublish)

	return nil
}

// Enabled returns whether the plugin is enabled
func (p *MyPlugin) Enabled() bool {
	return p.enabled
}

// Shutdown cleans up resources
func (p *MyPlugin) Shutdown() error {
	// Cleanup logic here
	return nil
}

// onMessagePublish is called when a message is published
func (p *MyPlugin) onMessagePublish(client *mqtt.Client, msg *mqtt.Message) *mqtt.Message {
	// Process message
	// Return nil to block the message, or return the message (possibly modified)
	return msg
}

// Initialize the plugin when the package is loaded
func init() {
	mqtt.RegisterPlugin("my_plugin", &MyPlugin{})
}
```

### Plugin Distribution

To distribute your plugin:

1. Create a Go package that implements the Plugin interface
2. Build the plugin as a Go plugin (.so file) or standalone package
3. For Go plugins, place the .so file in the plugins directory
4. For standalone packages, import and register them in your main application

## Plugin Development Best Practices

1. **Error Handling**: Gracefully handle errors to avoid affecting the broker
2. **Performance**: Be mindful of performance in hook functions
3. **Thread Safety**: Ensure thread-safe operations as hooks run concurrently
4. **Configuration**: Provide clear documentation for your plugin's configuration
5. **Logging**: Use the broker's logging system for consistency
6. **Testing**: Write tests to verify your plugin's behavior
7. **Versioning**: Clearly indicate compatibility with GoMQTT versions

## Troubleshooting Plugins

To troubleshoot plugin issues:

1. Enable debug logging:

```json
{
  "logging": {
    "level": "debug"
  }
}
```

2. Check plugin initialization errors in the logs
3. Verify the plugin is in the correct directory
4. Ensure plugin configuration is correct
5. Check for version compatibility issues

## Example Use Cases

### IoT Data Validation Plugin

```go
// Validates messages from IoT devices against a schema
func (p *ValidationPlugin) onMessagePublish(client *mqtt.Client, msg *mqtt.Message) *mqtt.Message {
	if strings.HasPrefix(msg.Topic, "device/data/") {
		valid, err := p.validateSchema(msg.Payload)
		if !valid || err != nil {
			// Invalid message, don't forward it
			p.broker.Log().Warn("Invalid device data received", "client", client.ID, "error", err)
			return nil
		}
	}
	return msg
}
```

### Message Transformation Plugin

```go
// Transforms message payloads (e.g., from XML to JSON)
func (p *TransformPlugin) onMessagePublish(client *mqtt.Client, msg *mqtt.Message) *mqtt.Message {
	if strings.HasPrefix(msg.Topic, "xml/data/") {
		// Transform XML to JSON
		jsonData, err := p.xmlToJson(msg.Payload)
		if err == nil {
			// Create a new topic replacing xml with json prefix
			newTopic := strings.Replace(msg.Topic, "xml/data/", "json/data/", 1)

			// Create a new message with the transformed data
			newMsg := *msg // Copy original message
			newMsg.Topic = newTopic
			newMsg.Payload = jsonData

			return &newMsg
		}
	}
	return msg
}
```

### Custom Authentication Plugin

```go
// Authenticates against a custom database
func (p *CustomAuthPlugin) onClientAuthenticate(client *mqtt.Client, username, password string) bool {
	// Connect to your custom user database
	authorized, err := p.authService.Authenticate(username, password)
	if err != nil {
		p.broker.Log().Error("Authentication error", "error", err)
		return false
	}
	return authorized
}
```
