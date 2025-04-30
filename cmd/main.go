package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mstgnz/gomqtt/auth"
	"github.com/mstgnz/gomqtt/cmd/admin"
	"github.com/mstgnz/gomqtt/cmd/api"
	"github.com/mstgnz/gomqtt/config"
	"github.com/mstgnz/gomqtt/mqtt"
	"github.com/mstgnz/gomqtt/plugin"
	"github.com/mstgnz/gomqtt/plugins/webhook"
	"github.com/mstgnz/gomqtt/storage"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config/default.json", "Configuration file path")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Config file not found, using default settings")
			cfg = config.DefaultConfig()
		} else {
			log.Fatalf("Failed to load configuration: %v", err)
		}
	}

	// Setup authentication
	authService := auth.New(cfg.Auth.JWTSecret)

	// Setup storage
	store, err := storage.NewPostgresStorage(cfg.GetDatabaseURL())
	if err != nil {
		log.Printf("Warning: Failed to connect to database: %v", err)
		log.Println("Continuing without database support")
	} else {
		defer store.Close()
	}

	// Initialize plugin registry
	pluginRegistry := plugin.NewPluginRegistry()

	// Register example plugin
	examplePlugin := plugin.NewPlugin(
		"example",
		"Example plugin that logs events",
		"0.1.0",
		"GoMQTT Team",
	)

	examplePlugin.OnEvent(plugin.EventClientConnect, func(ctx *plugin.Context) error {
		log.Printf("Plugin: Client connected: %s", ctx.ClientID)
		return nil
	})

	if err := pluginRegistry.Register(examplePlugin); err != nil {
		log.Printf("Failed to register example plugin: %v", err)
	}

	// Setup webhook plugin
	webhook.SetupWebhookPlugin(pluginRegistry)
	log.Printf("Webhook plugin initialized")

	// Create MQTT server
	mqttServer := mqtt.NewServer(cfg.MQTT.Host, cfg.MQTT.Port)

	// Set plugin registry for MQTT server
	mqttServer.SetPluginRegistry(pluginRegistry)

	// Set auth service for permission checking
	mqttServer.SetAuthService(authService)

	// Create REST API server
	apiAddr := fmt.Sprintf("%s:%d", cfg.API.Host, cfg.API.Port)
	apiServer := api.NewServer(apiAddr, authService, store)

	// Create Admin Panel
	adminAddr := fmt.Sprintf("%s:%d", cfg.API.Host, 8081) // Admin panel on port 8081
	adminServer := admin.NewServer(adminAddr, "web/templates", store)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start MQTT server
	go func() {
		log.Printf("Starting MQTT broker on %s:%d", cfg.MQTT.Host, cfg.MQTT.Port)
		if err := mqttServer.Start(); err != nil {
			log.Fatalf("Failed to start MQTT server: %v", err)
		}
	}()

	// Start REST API server
	go func() {
		log.Printf("Starting REST API server on %s", apiAddr)
		if err := apiServer.Start(); err != nil {
			log.Fatalf("Failed to start REST API server: %v", err)
		}
	}()

	// Start Admin Panel
	go func() {
		log.Printf("Starting Admin Panel on %s", adminAddr)
		if err := adminServer.Start(); err != nil {
			log.Fatalf("Failed to start Admin Panel: %v", err)
		}
	}()

	log.Printf("GoMQTT broker started. Press Ctrl+C to shutdown")

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down...")

	// Cleanup and close connections
	if err := mqttServer.Stop(); err != nil {
		log.Printf("Error shutting down MQTT server: %v", err)
	}

	log.Println("Server stopped. Goodbye!")
}
