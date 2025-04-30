package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Config represents the broker configuration
type Config struct {
	MQTT struct {
		Host            string `json:"host"`
		Port            int    `json:"port"`
		MaxConnections  int    `json:"max_connections"`
		MaxMessageSize  int    `json:"max_message_size"`
		AllowAnonymous  bool   `json:"allow_anonymous"`
		MaxQueueSize    int    `json:"max_queue_size"`
		RetainAvailable bool   `json:"retain_available"`

		// Rate limiting configuration
		RateLimiting struct {
			Enabled         bool    `json:"enabled"`
			ConnectLimit    float64 `json:"connect_limit"`    // Connections per second
			PublishLimit    float64 `json:"publish_limit"`    // Messages per second
			SubscribeLimit  float64 `json:"subscribe_limit"`  // Subscriptions per second
			BurstMultiplier float64 `json:"burst_multiplier"` // Multiplier for burst capacity
		} `json:"rate_limiting"`

		// TLS/MQTTS configuration
		TLS struct {
			Enabled           bool   `json:"enabled"`
			Port              int    `json:"port"`
			CertFile          string `json:"cert_file"`
			KeyFile           string `json:"key_file"`
			RequireClientCert bool   `json:"require_client_cert"`
			CACertFile        string `json:"ca_cert_file"`
		} `json:"tls"`

		// WebSocket configuration
		WebSocket struct {
			Enabled bool   `json:"enabled"`
			Host    string `json:"host"`
			Port    int    `json:"port"`
			Path    string `json:"path"`

			// Secure WebSocket (WSS)
			TLS struct {
				Enabled  bool   `json:"enabled"`
				Port     int    `json:"port"`
				CertFile string `json:"cert_file"`
				KeyFile  string `json:"key_file"`
			} `json:"tls"`
		} `json:"websocket"`
	} `json:"mqtt"`

	API struct {
		Enabled bool   `json:"enabled"`
		Host    string `json:"host"`
		Port    int    `json:"port"`
	} `json:"api"`

	Auth struct {
		JWTSecret  string `json:"jwt_secret"`
		JWTExpires int    `json:"jwt_expires"` // in hours

		// OAuth2 configuration
		OAuth2 struct {
			Enabled       bool     `json:"enabled"`
			ClientID      string   `json:"client_id"`
			ClientSecret  string   `json:"client_secret"`
			AuthURL       string   `json:"auth_url"`
			TokenURL      string   `json:"token_url"`
			RedirectURL   string   `json:"redirect_url"`
			Scopes        []string `json:"scopes"`
			UserInfoURL   string   `json:"user_info_url"`
			TokenField    string   `json:"token_field"`    // Field from MQTT CONNECT containing token
			UsernameField string   `json:"username_field"` // JSON field in user info containing username
		} `json:"oauth2"`

		// RBAC configuration
		RBAC struct {
			Enabled             bool   `json:"enabled"`
			DefaultRole         string `json:"default_role"`          // Default role for new users
			PredefinedRolesFile string `json:"predefined_roles_file"` // JSON file containing predefined roles
		} `json:"rbac"`
	} `json:"auth"`

	Database struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		DBName   string `json:"db_name"`
		SSLMode  string `json:"ssl_mode"`
	} `json:"database"`

	Redis struct {
		Enabled   bool   `json:"enabled"`
		Host      string `json:"host"`
		Port      int    `json:"port"`
		Password  string `json:"password"`
		DB        int    `json:"db"`
		KeyPrefix string `json:"key_prefix"`
	} `json:"redis"`

	Storage struct {
		Enabled          bool   `json:"enabled"`
		Type             string `json:"type"`              // "postgres" or "redis"
		MessageRetention int    `json:"message_retention"` // in hours, 0 = forever
		CleanupInterval  int    `json:"cleanup_interval"`  // in hours
		BatchSize        int    `json:"batch_size"`        // for batch operations
	} `json:"storage"`

	Plugins struct {
		Enabled   bool     `json:"enabled"`
		Directory string   `json:"directory"`
		Autoload  []string `json:"autoload"`
	} `json:"plugins"`

	Logging struct {
		Level  string `json:"level"`
		Format string `json:"format"`
		File   string `json:"file"`
	} `json:"logging"`

	// Clustering configuration
	Cluster struct {
		Enabled      bool     `json:"enabled"`
		NodeID       string   `json:"node_id"`
		NodeHost     string   `json:"node_host"`
		NodePort     int      `json:"node_port"`
		SeedNodes    []string `json:"seed_nodes"`
		GossipPort   int      `json:"gossip_port"`
		SyncInterval int      `json:"sync_interval"` // in seconds
	} `json:"cluster"`
}

// DefaultConfig creates a default configuration
func DefaultConfig() *Config {
	cfg := &Config{}

	// MQTT defaults
	cfg.MQTT.Host = "0.0.0.0"
	cfg.MQTT.Port = 1883
	cfg.MQTT.MaxConnections = 1000
	cfg.MQTT.MaxMessageSize = 16384 // 16KB
	cfg.MQTT.AllowAnonymous = false
	cfg.MQTT.MaxQueueSize = 100
	cfg.MQTT.RetainAvailable = true

	// Rate limiting defaults
	cfg.MQTT.RateLimiting.Enabled = true
	cfg.MQTT.RateLimiting.ConnectLimit = 5    // 5 connections per second
	cfg.MQTT.RateLimiting.PublishLimit = 100  // 100 messages per second
	cfg.MQTT.RateLimiting.SubscribeLimit = 20 // 20 subscriptions per second
	cfg.MQTT.RateLimiting.BurstMultiplier = 2 // Allow bursts of 2x the normal rate

	// TLS/MQTTS defaults
	cfg.MQTT.TLS.Enabled = false
	cfg.MQTT.TLS.Port = 8883 // MQTT TLS standard port
	cfg.MQTT.TLS.CertFile = "certs/server.crt"
	cfg.MQTT.TLS.KeyFile = "certs/server.key"
	cfg.MQTT.TLS.RequireClientCert = false
	cfg.MQTT.TLS.CACertFile = "certs/ca.crt"

	// WebSocket defaults
	cfg.MQTT.WebSocket.Enabled = true
	cfg.MQTT.WebSocket.Host = "0.0.0.0"
	cfg.MQTT.WebSocket.Port = 9001
	cfg.MQTT.WebSocket.Path = "/mqtt"

	// Secure WebSocket defaults
	cfg.MQTT.WebSocket.TLS.Enabled = false
	cfg.MQTT.WebSocket.TLS.Port = 9443 // Custom port for secure WebSockets
	cfg.MQTT.WebSocket.TLS.CertFile = "certs/server.crt"
	cfg.MQTT.WebSocket.TLS.KeyFile = "certs/server.key"

	// API defaults
	cfg.API.Enabled = true
	cfg.API.Host = "0.0.0.0"
	cfg.API.Port = 8080

	// Auth defaults
	cfg.Auth.JWTSecret = "change-me-in-production"
	cfg.Auth.JWTExpires = 24 // 24 hours

	// OAuth2 defaults
	cfg.Auth.OAuth2.Enabled = false
	cfg.Auth.OAuth2.TokenField = "password" // By default, use password field for token
	cfg.Auth.OAuth2.UsernameField = "email" // Default username field in user info

	// RBAC defaults
	cfg.Auth.RBAC.Enabled = false
	cfg.Auth.RBAC.DefaultRole = "user"
	cfg.Auth.RBAC.PredefinedRolesFile = "config/roles.json"

	// Database defaults
	cfg.Database.Host = "localhost"
	cfg.Database.Port = 5432
	cfg.Database.User = "postgres"
	cfg.Database.Password = "postgres"
	cfg.Database.DBName = "gomqtt"
	cfg.Database.SSLMode = "disable"

	// Redis defaults
	cfg.Redis.Enabled = false
	cfg.Redis.Host = "localhost"
	cfg.Redis.Port = 6379
	cfg.Redis.Password = ""
	cfg.Redis.DB = 0
	cfg.Redis.KeyPrefix = "gomqtt:"

	// Storage defaults
	cfg.Storage.Enabled = true
	cfg.Storage.Type = "postgres"     // Default to PostgreSQL
	cfg.Storage.MessageRetention = 24 // Store messages for 24 hours
	cfg.Storage.CleanupInterval = 6   // Run cleanup every 6 hours
	cfg.Storage.BatchSize = 100       // Process up to 100 messages at a time

	// Plugin defaults
	cfg.Plugins.Enabled = true
	cfg.Plugins.Directory = "./plugins"
	cfg.Plugins.Autoload = []string{}

	// Logging defaults
	cfg.Logging.Level = "info"
	cfg.Logging.Format = "text"
	cfg.Logging.File = ""

	// Cluster defaults
	cfg.Cluster.Enabled = false
	cfg.Cluster.NodeID = "" // Will be auto-generated if empty
	cfg.Cluster.NodeHost = "127.0.0.1"
	cfg.Cluster.NodePort = 7946   // Default memberlist port
	cfg.Cluster.GossipPort = 7947 // Gossip protocol port
	cfg.Cluster.SeedNodes = []string{}
	cfg.Cluster.SyncInterval = 30 // 30 seconds

	return cfg
}

// LoadConfig loads configuration from a file
func LoadConfig(path string) (*Config, error) {
	// Use default config
	cfg := DefaultConfig()

	// Read configuration file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse JSON
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return cfg, nil
}

// SaveConfig saves configuration to a file
func SaveConfig(cfg *Config, path string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Serialize to JSON
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize config: %w", err)
	}

	// Write to file
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// GetDatabaseURL returns the PostgreSQL connection URL
func (c *Config) GetDatabaseURL() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.Database.User,
		c.Database.Password,
		c.Database.Host,
		c.Database.Port,
		c.Database.DBName,
		c.Database.SSLMode,
	)
}

// GetRedisURL returns the Redis connection URL
func (c *Config) GetRedisURL() string {
	if c.Redis.Password != "" {
		return fmt.Sprintf("redis://:%s@%s:%d/%d",
			c.Redis.Password,
			c.Redis.Host,
			c.Redis.Port,
			c.Redis.DB)
	}
	return fmt.Sprintf("redis://%s:%d/%d",
		c.Redis.Host,
		c.Redis.Port,
		c.Redis.DB)
}
