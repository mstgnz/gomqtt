package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/mstgnz/gomqtt/auth"
	"github.com/mstgnz/gomqtt/config"
	"github.com/mstgnz/gomqtt/storage"
	"github.com/spf13/cobra"
)

// Client represents a connected MQTT client
type Client struct {
	ClientID    string
	Username    string
	Protocol    string
	IPAddress   string
	ConnectedAt time.Time
}

// Topic represents an MQTT topic with stats
type Topic struct {
	Name                 string
	SubscriberCount      int
	RetainedMessageCount int
}

// User represents a GoMQTT user
type User struct {
	Username  string
	Role      string
	CreatedAt time.Time
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "gomqtt-cli",
		Short: "GoMQTT CLI - Command-line interface for managing GoMQTT broker",
		Long: `GoMQTT CLI provides tools for managing and monitoring your MQTT broker.
Complete documentation is available at https://github.com/mstgnz/gomqtt`,
	}

	// Global flags
	var configPath string
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "config/default.json", "Configuration file path")

	// Start broker command
	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "Start the MQTT broker",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Starting MQTT broker...")
			// We'll just call the existing main entry point
			fmt.Println("Use the standard 'gomqtt' command to start the broker")
		},
	}

	// Status command
	var statusCmd = &cobra.Command{
		Use:   "status",
		Short: "Show broker status",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				log.Fatalf("Failed to load configuration: %v", err)
			}

			fmt.Println("GoMQTT Broker Status")
			fmt.Println("====================")

			// Try to connect to the broker to check its status
			fmt.Printf("MQTT Port: %d\n", cfg.MQTT.Port)
			fmt.Printf("WebSocket Enabled: %v\n", cfg.MQTT.WebSocket.Enabled)
			if cfg.MQTT.WebSocket.Enabled {
				fmt.Printf("WebSocket Port: %d\n", cfg.MQTT.WebSocket.Port)
			}
			fmt.Printf("TLS Enabled: %v\n", cfg.MQTT.TLS.Enabled)
			if cfg.MQTT.TLS.Enabled {
				fmt.Printf("TLS Port: %d\n", cfg.MQTT.TLS.Port)
			}
			fmt.Printf("Cluster Enabled: %v\n", cfg.Cluster.Enabled)
			if cfg.Cluster.Enabled {
				fmt.Printf("Node ID: %s\n", cfg.Cluster.NodeID)
			}
		},
	}

	// Client management commands
	var clientCmd = &cobra.Command{
		Use:   "client",
		Short: "Manage MQTT clients",
	}

	var listClientsCmd = &cobra.Command{
		Use:   "list",
		Short: "List connected clients",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				log.Fatalf("Failed to load configuration: %v", err)
			}

			// Connect to the storage to get clients
			var store storage.Storage
			var storageErr error

			switch cfg.Storage.Type {
			case "mysql":
				store, storageErr = storage.NewMySQLStorage(cfg.GetMySQLURL())
			case "postgres", "":
				store, storageErr = storage.NewPostgresStorage(cfg.GetDatabaseURL())
			default:
				log.Fatalf("Unknown storage type: %s", cfg.Storage.Type)
			}

			if storageErr != nil {
				log.Fatalf("Failed to connect to storage: %v", storageErr)
			}
			defer store.Close()

			// Note: This is a placeholder - the actual method would need to be implemented in the storage interface
			fmt.Println("Connected Clients:")
			fmt.Println("=================")
			fmt.Println("This functionality requires implementation in the storage interface")
			fmt.Println("See storage.Storage interface for details")
		},
	}

	var disconnectClientCmd = &cobra.Command{
		Use:   "disconnect [clientID]",
		Short: "Disconnect a client",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			clientID := args[0]
			fmt.Printf("Disconnecting client %s...\n", clientID)
			fmt.Println("This functionality requires API integration")
		},
	}

	// Topic management commands
	var topicCmd = &cobra.Command{
		Use:   "topic",
		Short: "Manage MQTT topics",
	}

	var listTopicsCmd = &cobra.Command{
		Use:   "list",
		Short: "List active topics",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				log.Fatalf("Failed to load configuration: %v", err)
			}

			// Connect to storage
			var store storage.Storage
			var storageErr error

			switch cfg.Storage.Type {
			case "mysql":
				store, storageErr = storage.NewMySQLStorage(cfg.GetMySQLURL())
			case "postgres", "":
				store, storageErr = storage.NewPostgresStorage(cfg.GetDatabaseURL())
			default:
				log.Fatalf("Unknown storage type: %s", cfg.Storage.Type)
			}

			if storageErr != nil {
				log.Fatalf("Failed to connect to storage: %v", storageErr)
			}
			defer store.Close()

			// Note: This is a placeholder - the actual method would need to be implemented in the storage interface
			fmt.Println("Active Topics:")
			fmt.Println("=============")
			fmt.Println("This functionality requires implementation in the storage interface")
			fmt.Println("See storage.Storage interface for details")
		},
	}

	var publishCmd = &cobra.Command{
		Use:   "publish [topic] [message]",
		Short: "Publish a message to a topic",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			topic := args[0]
			message := args[1]

			qos, _ := cmd.Flags().GetInt("qos")
			retain, _ := cmd.Flags().GetBool("retain")

			fmt.Printf("Publishing to topic %s (QoS %d, Retain: %v)...\n", topic, qos, retain)
			fmt.Printf("Message: %s\n", message)
			fmt.Println("This functionality requires API integration")
		},
	}
	publishCmd.Flags().IntP("qos", "q", 0, "QoS level (0, 1, or 2)")
	publishCmd.Flags().BoolP("retain", "r", false, "Set retain flag")

	// User management commands
	var userCmd = &cobra.Command{
		Use:   "user",
		Short: "Manage users",
	}

	var createUserCmd = &cobra.Command{
		Use:   "create [username] [password]",
		Short: "Create a new user",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			username := args[0]
			// We'll keep password for future use when the auth interface is extended
			_ = args[1] // password

			role, _ := cmd.Flags().GetString("role")

			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				log.Fatalf("Failed to load configuration: %v", err)
			}

			// We'll keep authService for future use
			_ = auth.New(cfg.Auth.JWTSecret) // authService

			// Note: This is a placeholder - the actual method would need to be implemented in the auth interface
			fmt.Printf("Creating user %s with role %s...\n", username, role)
			fmt.Println("This functionality requires implementation in the auth interface")
			fmt.Println("See auth.Auth interface for details")
		},
	}
	createUserCmd.Flags().StringP("role", "r", "user", "Role for the new user")

	var listUsersCmd = &cobra.Command{
		Use:   "list",
		Short: "List users",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				log.Fatalf("Failed to load configuration: %v", err)
			}

			// We'll keep authService for future use
			_ = auth.New(cfg.Auth.JWTSecret) // authService

			// Note: This is a placeholder - the actual method would need to be implemented in the auth interface
			fmt.Println("Users:")
			fmt.Println("======")
			fmt.Println("This functionality requires implementation in the auth interface")
			fmt.Println("See auth.Auth interface for details")
		},
	}

	// Config management
	var configCmd = &cobra.Command{
		Use:   "config",
		Short: "Manage configuration",
	}

	var showConfigCmd = &cobra.Command{
		Use:   "show",
		Short: "Show current configuration",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				log.Fatalf("Failed to load configuration: %v", err)
			}

			// Parse format flag
			format, _ := cmd.Flags().GetString("format")

			switch strings.ToLower(format) {
			case "json":
				jsonBytes, err := json.MarshalIndent(cfg, "", "  ")
				if err != nil {
					log.Fatalf("Failed to marshal config to JSON: %v", err)
				}
				fmt.Println(string(jsonBytes))
			default:
				// Display in a human-readable format
				fmt.Println("GoMQTT Configuration")
				fmt.Println("====================")
				fmt.Printf("MQTT Host: %s\n", cfg.MQTT.Host)
				fmt.Printf("MQTT Port: %d\n", cfg.MQTT.Port)
				fmt.Printf("WebSocket Enabled: %v\n", cfg.MQTT.WebSocket.Enabled)
				fmt.Printf("TLS Enabled: %v\n", cfg.MQTT.TLS.Enabled)
				fmt.Printf("Cluster Enabled: %v\n", cfg.Cluster.Enabled)
				fmt.Printf("Storage Type: %s\n", cfg.Storage.Type)
				fmt.Printf("Storage Enabled: %v\n", cfg.Storage.Enabled)
				fmt.Printf("Auth JWT Secret: %s\n", mask(cfg.Auth.JWTSecret))
				fmt.Printf("Auth RBAC Enabled: %v\n", cfg.Auth.RBAC.Enabled)
				fmt.Printf("Auth RBAC Default Role: %s\n", cfg.Auth.RBAC.DefaultRole)
				fmt.Printf("OAuth2 Enabled: %v\n", cfg.Auth.OAuth2.Enabled)
			}
		},
	}
	showConfigCmd.Flags().StringP("format", "f", "text", "Output format (text or json)")

	// Cluster management
	var clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "Manage cluster",
	}

	var clusterStatusCmd = &cobra.Command{
		Use:   "status",
		Short: "Show cluster status",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				log.Fatalf("Failed to load configuration: %v", err)
			}

			if !cfg.Cluster.Enabled {
				fmt.Println("Cluster mode is not enabled in configuration")
				return
			}

			fmt.Println("Cluster Configuration")
			fmt.Println("====================")
			fmt.Printf("Node ID: %s\n", cfg.Cluster.NodeID)
			fmt.Printf("Node Host: %s\n", cfg.Cluster.NodeHost)
			fmt.Printf("Node Port: %d\n", cfg.Cluster.NodePort)
			fmt.Printf("Gossip Port: %d\n", cfg.Cluster.GossipPort)
			fmt.Printf("Sync Interval: %d seconds\n", cfg.Cluster.SyncInterval)

			fmt.Println("\nSeed Nodes:")
			for i, node := range cfg.Cluster.SeedNodes {
				fmt.Printf("  %d. %s\n", i+1, node)
			}

			fmt.Println("\nNote: Runtime cluster status requires API integration")
		},
	}

	// Metrics and monitoring
	var metricsCmd = &cobra.Command{
		Use:   "metrics",
		Short: "Show broker metrics",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("GoMQTT Broker Metrics")
			fmt.Println("====================")
			fmt.Println("Metrics collection from the CLI requires API integration")
			fmt.Println("Please use the Prometheus metrics endpoint or Grafana dashboards")
		},
	}

	// Build the command hierarchy
	clientCmd.AddCommand(listClientsCmd, disconnectClientCmd)
	topicCmd.AddCommand(listTopicsCmd, publishCmd)
	userCmd.AddCommand(createUserCmd, listUsersCmd)
	configCmd.AddCommand(showConfigCmd)
	clusterCmd.AddCommand(clusterStatusCmd)

	rootCmd.AddCommand(startCmd, statusCmd, clientCmd, topicCmd, userCmd, configCmd, clusterCmd, metricsCmd)

	// Execute the root command
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Helper function to mask sensitive data
func mask(s string) string {
	if len(s) <= 4 {
		return "****"
	}
	return s[:2] + "****" + s[len(s)-2:]
}
