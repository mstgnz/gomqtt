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

		// WebSocket configuration
		WebSocket struct {
			Enabled bool   `json:"enabled"`
			Host    string `json:"host"`
			Port    int    `json:"port"`
			Path    string `json:"path"`
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
	} `json:"auth"`

	Database struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		DBName   string `json:"db_name"`
		SSLMode  string `json:"ssl_mode"`
	} `json:"database"`

	Storage struct {
		Enabled          bool `json:"enabled"`
		MessageRetention int  `json:"message_retention"` // in hours, 0 = forever
		CleanupInterval  int  `json:"cleanup_interval"`  // in hours
		BatchSize        int  `json:"batch_size"`        // for batch operations
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

	// WebSocket defaults
	cfg.MQTT.WebSocket.Enabled = true
	cfg.MQTT.WebSocket.Host = "0.0.0.0"
	cfg.MQTT.WebSocket.Port = 9001
	cfg.MQTT.WebSocket.Path = "/mqtt"

	// API defaults
	cfg.API.Enabled = true
	cfg.API.Host = "0.0.0.0"
	cfg.API.Port = 8080

	// Auth defaults
	cfg.Auth.JWTSecret = "change-me-in-production"
	cfg.Auth.JWTExpires = 24 // 24 hours

	// Database defaults
	cfg.Database.Host = "localhost"
	cfg.Database.Port = 5432
	cfg.Database.User = "postgres"
	cfg.Database.Password = "postgres"
	cfg.Database.DBName = "gomqtt"
	cfg.Database.SSLMode = "disable"

	// Storage defaults
	cfg.Storage.Enabled = true
	cfg.Storage.MessageRetention = 24 // 24 hours
	cfg.Storage.CleanupInterval = 1   // 1 hour
	cfg.Storage.BatchSize = 100       // 100 messages per batch

	// Plugin defaults
	cfg.Plugins.Enabled = true
	cfg.Plugins.Directory = "./plugins"
	cfg.Plugins.Autoload = []string{}

	// Logging defaults
	cfg.Logging.Level = "info"
	cfg.Logging.Format = "text"
	cfg.Logging.File = ""

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
