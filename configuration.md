# 🔧 GoMQTT Configuration Guide

This document details the configuration options available for GoMQTT.

## Configuration Methods

GoMQTT can be configured through:

1. JSON configuration file
2. Environment variables
3. Command-line flags

## JSON Configuration

The primary configuration method is through a JSON file. By default, GoMQTT looks for a configuration file at `config/default.json`. You can specify a different file using the `-config` flag when starting the broker.

### Basic Configuration Example

```json
{
  "mqtt": {
    "host": "0.0.0.0",
    "port": 1883,
    "max_connections": 1000,
    "max_message_size": 16384,
    "allow_anonymous": false
  },
  "api": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8080
  },
  "logging": {
    "level": "info",
    "format": "text",
    "file": ""
  }
}
```

## Configuration Sections

### MQTT

Controls the core MQTT broker behavior:

```json
{
  "mqtt": {
    "host": "0.0.0.0", // Listening address
    "port": 1883, // MQTT port (default: 1883)
    "max_connections": 10000, // Maximum concurrent connections
    "max_message_size": 16384, // Maximum packet size in bytes
    "allow_anonymous": false, // Allow anonymous connections
    "connection_timeout": 60, // Connection timeout in seconds
    "persistent_session": true // Enable persistent sessions
  }
}
```

### TLS/MQTTS

Configure secure MQTT connections:

```json
{
  "mqtt": {
    "tls": {
      "enabled": true,
      "port": 8883, // Secure MQTT port (default: 8883)
      "cert_file": "certs/server.crt",
      "key_file": "certs/server.key",
      "ca_file": "certs/ca.crt", // Optional CA for client verification
      "verify_client": false, // Enable client certificate verification (mTLS)
      "cipher_suites": [], // Optional list of cipher suites
      "min_version": "TLS12" // Minimum TLS version (TLS10, TLS11, TLS12, TLS13)
    }
  }
}
```

### WebSocket

Configure WebSocket connections:

```json
{
  "mqtt": {
    "websocket": {
      "enabled": true,
      "host": "0.0.0.0",
      "port": 9001, // WebSocket port (default: 9001)
      "path": "/mqtt", // WebSocket endpoint path
      "origins": ["*"], // Allowed origins
      "tls": {
        "enabled": true,
        "port": 9443, // Secure WebSocket port (default: 9443)
        "cert_file": "certs/server.crt",
        "key_file": "certs/server.key"
      }
    }
  }
}
```

### Authentication

Configure user authentication methods:

```json
{
  "auth": {
    "enabled": true,
    "anonymous": false, // Allow anonymous access
    "default_user": "guest", // Default username for anonymous connections
    "users_file": "config/users.json", // File containing username/password pairs
    "jwt_secret": "your-jwt-secret-here", // Secret for JWT authentication
    "jwt_expires": 24, // JWT expiration time in hours
    "acl_file": "config/acl.json", // Access control list file
    "password_hash": "bcrypt" // Password hashing algorithm (bcrypt, argon2)
  }
}
```

### OAuth2 Authentication

Configure OAuth2 for authentication with identity providers:

```json
{
  "auth": {
    "oauth2": {
      "enabled": true,
      "client_id": "your-client-id",
      "client_secret": "your-client-secret",
      "auth_url": "https://accounts.google.com/o/oauth2/auth",
      "token_url": "https://oauth2.googleapis.com/token",
      "redirect_url": "http://localhost:8080/oauth/callback",
      "scopes": ["email", "profile"],
      "user_info_url": "https://www.googleapis.com/oauth2/v3/userinfo",
      "token_field": "password", // Field in MQTT CONNECT packet for the token
      "username_field": "email" // Field in user info response to use as username
    }
  }
}
```

### Database Configuration

Configure PostgreSQL database connection:

```json
{
  "database": {
    "type": "postgres", // Database type (postgres only for now)
    "host": "localhost", // Database host
    "port": 5432, // Database port
    "user": "postgres", // Database user
    "password": "postgres", // Database password
    "db_name": "gomqtt", // Database name
    "ssl_mode": "disable", // SSL mode (disable, require, verify-ca, verify-full)
    "max_connections": 10, // Maximum number of connections in the pool
    "connection_lifetime": 3600, // Maximum lifetime of a connection in seconds
    "connection_timeout": 30 // Connection timeout in seconds
  }
}
```

### Redis Configuration

Configure Redis for lightweight storage:

```json
{
  "redis": {
    "enabled": false,
    "host": "localhost",
    "port": 6379,
    "password": "",
    "db": 0,
    "key_prefix": "gomqtt:",
    "pool_size": 10,
    "min_idle_conns": 5,
    "max_retries": 3
  }
}
```

### Message Storage

Configure persistent message storage:

```json
{
  "storage": {
    "enabled": true,
    "type": "postgres", // Storage type (postgres or redis)
    "retained_messages": true, // Store retained messages
    "message_history": true, // Store message history
    "message_retention": 24, // Message retention period in hours
    "cleanup_interval": 1, // Cleanup interval in hours
    "batch_size": 100 // Batch size for database operations
  }
}
```

### Clustering

Configure multi-node clustering:

```json
{
  "cluster": {
    "enabled": true,
    "node_id": "node1", // Unique identifier for this node
    "node_host": "localhost", // Host address for cluster communication
    "node_port": 7946, // Port for cluster communication
    "gossip_port": 7947, // Port for gossip protocol
    "seed_nodes": [
      // List of existing nodes to join
      "node2:7946",
      "node3:7946"
    ],
    "sync_interval": 30, // Synchronization interval in seconds
    "transport": "tcp", // Transport protocol (tcp, udp)
    "log_level": "info" // Cluster-specific log level
  }
}
```

### API

Configure the REST API:

```json
{
  "api": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8080, // API port
    "base_path": "/api", // API base path
    "cors": {
      "enabled": true,
      "allowed_origins": ["*"],
      "allowed_methods": ["GET", "POST", "PUT", "DELETE"],
      "allowed_headers": ["Authorization", "Content-Type"],
      "max_age": 86400 // CORS preflight max age in seconds
    }
  }
}
```

### Plugins

Configure the plugin system:

```json
{
  "plugins": {
    "enabled": true,
    "directory": "./plugins", // Directory containing plugins
    "autoload": [
      // Plugins to load automatically
      "webhook",
      "auth_http"
    ]
  }
}
```

### Rate Limiting

Configure rate limiting to prevent abuse:

```json
{
  "rate_limits": {
    "enabled": true,
    "connection_rate": 10, // Connections per second per IP
    "publish_rate": 100, // Publish operations per second per client
    "subscribe_rate": 20, // Subscribe operations per second per client
    "byte_rate": 10240 // Bytes per second per client
  }
}
```

### Logging

Configure logging behavior:

```json
{
  "logging": {
    "level": "info", // Log level (debug, info, warn, error)
    "format": "json", // Log format (text, json)
    "file": "/var/log/gomqtt.log" // Log file (empty for stdout)
  }
}
```

### Metrics

Configure Prometheus metrics:

```json
{
  "metrics": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 9090, // Metrics port
    "path": "/metrics", // Metrics endpoint path
    "collection_interval": 15 // Collection interval in seconds
  }
}
```

## Environment Variables

All configuration options can also be set using environment variables with the `GOMQTT_` prefix. For nested properties, use underscores:

```bash
# Basic configuration
GOMQTT_MQTT_PORT=1883
GOMQTT_MQTT_HOST=0.0.0.0
GOMQTT_MQTT_MAX_CONNECTIONS=10000

# TLS configuration
GOMQTT_MQTT_TLS_ENABLED=true
GOMQTT_MQTT_TLS_PORT=8883
GOMQTT_MQTT_TLS_CERT_FILE=/path/to/cert.pem

# Authentication
GOMQTT_AUTH_ENABLED=true
GOMQTT_AUTH_ANONYMOUS=false
GOMQTT_AUTH_JWT_SECRET=your-secret-key
```

## Command-Line Flags

Some core options can be specified directly as command-line flags:

```bash
# Start GoMQTT with a custom configuration file
./gomqtt -config /path/to/config.json

# Override specific settings
./gomqtt -port 1883 -tls-port 8883 -ws-port 9001 -api-port 8080 -log-level debug
```

## Users and ACL Configuration

### users.json

For basic username/password authentication:

```json
{
  "users": [
    {
      "username": "admin",
      "password": "$2a$10$xyqCZ3DRDbfBUK1yKnlOl.mzOk27mPwTW9yQ9GK5y6yoLjJVWMNtK", // bcrypt hash for "admin_password"
      "roles": ["admin"]
    },
    {
      "username": "sensor",
      "password": "$2a$10$TXY28f6S9Ot2JMGCj.GoT.hg.Rw5TgEp5Jm/6GcKEz0tLR9Xs.g3u", // bcrypt hash for "sensor_password"
      "roles": ["device"]
    }
  ]
}
```

### acl.json

For access control to MQTT topics:

```json
{
  "roles": {
    "admin": {
      "publish": ["#"],
      "subscribe": ["#"]
    },
    "device": {
      "publish": ["sensors/${username}/#", "devices/${username}/status"],
      "subscribe": ["devices/${username}/control", "updates/#"]
    }
  }
}
```
