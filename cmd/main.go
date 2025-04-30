package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mstgnz/gomqtt/admin"
	"github.com/mstgnz/gomqtt/api"
	"github.com/mstgnz/gomqtt/auth"
	"github.com/mstgnz/gomqtt/cluster"
	"github.com/mstgnz/gomqtt/config"
	"github.com/mstgnz/gomqtt/metrics"
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

	// Setup OAuth2 if enabled
	if cfg.Auth.OAuth2.Enabled {
		log.Printf("Setting up OAuth2 authentication...")
		oauth2Provider := auth.NewOAuth2Provider(auth.OAuth2Config{
			Enabled:       true,
			ClientID:      cfg.Auth.OAuth2.ClientID,
			ClientSecret:  cfg.Auth.OAuth2.ClientSecret,
			AuthURL:       cfg.Auth.OAuth2.AuthURL,
			TokenURL:      cfg.Auth.OAuth2.TokenURL,
			RedirectURL:   cfg.Auth.OAuth2.RedirectURL,
			Scopes:        cfg.Auth.OAuth2.Scopes,
			UserInfoURL:   cfg.Auth.OAuth2.UserInfoURL,
			TokenField:    cfg.Auth.OAuth2.TokenField,
			UsernameField: cfg.Auth.OAuth2.UsernameField,
		})
		authService.SetOAuth2Provider(oauth2Provider)
		log.Printf("OAuth2 authentication initialized")
	}

	// Setup RBAC if enabled
	if cfg.Auth.RBAC.Enabled {
		log.Printf("Setting up Role-Based Access Control (RBAC)...")
		authService.SetRBACEnabled(true)
		authService.SetDefaultRole(cfg.Auth.RBAC.DefaultRole)

		if err := authService.LoadRolesFromFile(cfg.Auth.RBAC.PredefinedRolesFile); err != nil {
			log.Printf("Warning: Failed to load roles from file: %v", err)
			log.Println("RBAC will be enabled but without predefined roles")
		} else {
			log.Printf("RBAC initialized with predefined roles from %s", cfg.Auth.RBAC.PredefinedRolesFile)

			// Create default roles if not found in the file
			if _, err := authService.GetRole(cfg.Auth.RBAC.DefaultRole); err != nil {
				// Default role not found, create it
				log.Printf("Creating default role '%s'", cfg.Auth.RBAC.DefaultRole)
				defaultPermissions := []auth.Permission{
					{
						TopicPattern: "user/{username}/#",
						AccessLevel:  auth.ReadWrite,
					},
					{
						TopicPattern: "public/#",
						AccessLevel:  auth.ReadOnly,
					},
				}
				if err := authService.CreateRole(cfg.Auth.RBAC.DefaultRole, "Default user role", defaultPermissions); err != nil {
					log.Printf("Warning: Failed to create default role: %v", err)
				}
			}
		}
	} else {
		// RBAC is disabled
		authService.SetRBACEnabled(false)
	}

	// Setup storage
	var store storage.Storage
	var storageErr error

	switch cfg.Storage.Type {
	case "postgres", "": // Default to PostgreSQL if not specified
		log.Printf("Setting up PostgreSQL storage...")
		store, storageErr = storage.NewPostgresStorage(cfg.GetDatabaseURL())
		if storageErr != nil {
			log.Printf("Warning: Failed to connect to database: %v", storageErr)
			log.Println("Continuing without database support")
		} else {
			log.Printf("PostgreSQL storage initialized at %s:%d", cfg.Database.Host, cfg.Database.Port)
		}
	case "mysql":
		log.Printf("Setting up MySQL storage...")
		store, storageErr = storage.NewMySQLStorage(cfg.GetMySQLURL())
		if storageErr != nil {
			log.Printf("Warning: Failed to connect to MySQL: %v", storageErr)
			log.Println("Continuing without database support")
		} else {
			log.Printf("MySQL storage initialized at %s:%d", cfg.Database.Host, cfg.Database.Port)
		}
	default:
		log.Printf("Unknown storage type: %s, no storage will be used", cfg.Storage.Type)
	}

	// Setup storage cleanup if storage is available
	if store != nil && cfg.Storage.Enabled {
		defer store.Close()

		// Start message cleanup service if storage is enabled
		cleanupInterval := time.Duration(cfg.Storage.CleanupInterval) * time.Hour
		store.StartMessageCleanup(cleanupInterval)
		log.Printf("Message cleanup service started with interval: %s", cleanupInterval)
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

	// Setup cluster if enabled
	var clusterService *cluster.Cluster
	if cfg.Cluster.Enabled {
		log.Printf("Initializing cluster mode...")
		syncInterval := time.Duration(cfg.Cluster.SyncInterval) * time.Second

		clusterService = cluster.NewCluster(
			cfg.Cluster.NodeID,
			cfg.Cluster.NodeHost,
			cfg.Cluster.NodePort,
			cfg.Cluster.GossipPort,
			cfg.Cluster.SeedNodes,
			syncInterval,
		)

		// Register cluster callbacks to handle events from other nodes
		clusterService.RegisterCallbacks(
			// onSubscribe
			func(clientID, topic string, qos byte) {
				log.Printf("Cluster: Remote client %s subscribed to %s (QoS %d)", clientID, topic, qos)
			},
			// onUnsubscribe
			func(clientID, topic string) {
				log.Printf("Cluster: Remote client %s unsubscribed from %s", clientID, topic)
			},
			// onPublish
			func(topic string, payload []byte, qos byte, retained bool) {
				log.Printf("Cluster: Received retained message on topic %s (QoS %d)", topic, qos)
				mqttServer.StoreRetainedMessage(topic, payload, qos)
			},
		)

		// Start the cluster
		if err := clusterService.Start(); err != nil {
			log.Printf("Warning: Failed to start cluster: %v", err)
			log.Println("Continuing in standalone mode")
		} else {
			mqttServer.SetClusterService(clusterService)
			log.Printf("Cluster started. This node ID: %s", clusterService.NodeID)

			// Defer cluster shutdown
			defer clusterService.Stop()
		}
	}

	// Enable TLS/MQTTS if configured
	if cfg.MQTT.TLS.Enabled {
		mqttServer.EnableTLS(
			cfg.MQTT.TLS.Port,
			cfg.MQTT.TLS.CertFile,
			cfg.MQTT.TLS.KeyFile,
		)

		// Enable client certificate verification if configured
		if cfg.MQTT.TLS.RequireClientCert && cfg.MQTT.TLS.CACertFile != "" {
			mqttServer.EnableClientCertVerification(cfg.MQTT.TLS.CACertFile)
		}

		log.Printf("TLS/MQTTS enabled on port %d", cfg.MQTT.TLS.Port)
	}

	// Enable WebSocket support if configured
	if cfg.MQTT.WebSocket.Enabled {
		mqttServer.EnableWebSocket(
			cfg.MQTT.WebSocket.Host,
			cfg.MQTT.WebSocket.Port,
			cfg.MQTT.WebSocket.Path,
		)
		log.Printf("WebSocket transport enabled on %s:%d%s",
			cfg.MQTT.WebSocket.Host,
			cfg.MQTT.WebSocket.Port,
			cfg.MQTT.WebSocket.Path)
	}

	// Enable Secure WebSocket (WSS) if configured
	if cfg.MQTT.WebSocket.TLS.Enabled {
		mqttServer.EnableSecureWebSocket(
			cfg.MQTT.WebSocket.TLS.Port,
			cfg.MQTT.WebSocket.TLS.CertFile,
			cfg.MQTT.WebSocket.TLS.KeyFile,
		)
		log.Printf("Secure WebSocket (WSS) transport enabled on port %d",
			cfg.MQTT.WebSocket.TLS.Port)
	}

	// Set storage service for message persistence if available
	if store != nil && cfg.Storage.Enabled {
		mqttServer.SetStorageService(store)

		// Configure message retention (how long to keep messages)
		// 0 means store forever
		if cfg.Storage.MessageRetention > 0 {
			retention := time.Duration(cfg.Storage.MessageRetention) * time.Hour
			mqttServer.SetMessageRetention(retention)
			log.Printf("Message retention set to: %s", retention)
		} else {
			mqttServer.SetMessageRetention(0) // Store forever
			log.Println("Message retention set to: forever (no expiration)")
		}
	}

	// Create REST API server
	apiAddr := fmt.Sprintf("%s:%d", cfg.API.Host, cfg.API.Port)
	apiServer := api.NewServer(apiAddr, authService, store)

	// Pass MQTT server reference to API server for health checks
	apiServer.SetMQTTServer(mqttServer)

	// Create Admin Panel
	adminAddr := fmt.Sprintf("%s:%d", cfg.API.Host, 8081) // Admin panel on port 8081
	adminServer := admin.NewServer(adminAddr, "admin/template", store, mqttServer)

	// Initialize metrics server if enabled
	var metricsServer *metrics.Server
	if cfg.Metrics.Enabled {
		log.Printf("Setting up Prometheus metrics...")

		// Start system metrics collector
		metrics.StartSystemMetricsCollector(time.Duration(cfg.Metrics.SystemMetricsInterval) * time.Second)

		// Create metrics server
		metricsServer = metrics.NewServer(cfg.Metrics.Host, cfg.Metrics.Port, cfg.Metrics.Path)

		log.Printf("Prometheus metrics initialized on %s:%d%s",
			cfg.Metrics.Host, cfg.Metrics.Port, cfg.Metrics.Path)
	}

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

	// Start metrics server if enabled
	if cfg.Metrics.Enabled && metricsServer != nil {
		go func() {
			log.Printf("Starting Prometheus metrics server on %s:%d%s",
				cfg.Metrics.Host, cfg.Metrics.Port, cfg.Metrics.Path)
			if err := metricsServer.Start(); err != nil {
				log.Fatalf("Failed to start metrics server: %v", err)
			}
		}()
	}

	log.Printf("GoMQTT broker started. Press Ctrl+C to shutdown")

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down...")

	// Create a context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Cleanup and close connections
	log.Println("Stopping MQTT server...")
	if err := mqttServer.Stop(); err != nil {
		log.Printf("Error shutting down MQTT server: %v", err)
	}

	// Gracefully shutdown API server
	log.Println("Stopping API server...")
	if err := apiServer.Stop(); err != nil {
		log.Printf("Error shutting down API server: %v", err)
	}

	// Gracefully shutdown Admin server
	log.Println("Stopping Admin Panel...")
	if err := adminServer.Stop(); err != nil {
		log.Printf("Error shutting down Admin Panel: %v", err)
	}

	// Gracefully shutdown metrics server if running
	if cfg.Metrics.Enabled && metricsServer != nil {
		log.Println("Stopping metrics server...")
		if err := metricsServer.Stop(); err != nil {
			log.Printf("Error shutting down metrics server: %v", err)
		}
	}

	// Wait for context to complete or timeout
	<-ctx.Done()

	log.Println("All servers stopped. Goodbye!")
}
