// Package admin provides the admin panel web interface for GoMQTT.
// It includes functionality for visualizing broker statistics, client management,
// and message monitoring through a browser-based interface.
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"path/filepath"
	"runtime"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/mstgnz/gomqtt/mqtt"
	"github.com/mstgnz/gomqtt/storage"
)

// Server represents the admin panel server with web UI capabilities
// for managing and monitoring the MQTT broker.
type Server struct {
	Router      chi.Router
	Storage     storage.Storage
	ListenAddr  string
	TemplateDir string
	templates   map[string]*template.Template
	httpServer  *http.Server
	mqttServer  *mqtt.Server
}

// NewServer creates a new admin panel server with the specified configuration.
//
// Parameters:
//   - listenAddr: Network address on which the admin server will listen
//   - templateDir: Directory path containing the HTML templates
//   - storage: Storage interface for accessing persistent data
//
// Returns:
//   - A configured admin server instance ready to be started
func NewServer(listenAddr, templateDir string, storage storage.Storage, mqttServer *mqtt.Server) *Server {
	s := &Server{
		Router:      chi.NewRouter(),
		Storage:     storage,
		ListenAddr:  listenAddr,
		TemplateDir: templateDir,
		templates:   make(map[string]*template.Template),
		mqttServer:  mqttServer,
	}

	// Setup middleware
	s.Router.Use(middleware.Logger)
	s.Router.Use(middleware.Recoverer)

	// Setup routes
	s.setupRoutes()

	// Load templates
	s.loadTemplates()

	return s
}

// Start begins the admin panel server on the configured listen address.
// This method blocks until the server is stopped or encounters an error.
//
// Returns:
//   - Any error encountered while starting or running the server
func (s *Server) Start() error {
	fmt.Printf("Admin panel started on %s\n", s.ListenAddr)
	s.httpServer = &http.Server{
		Addr:    s.ListenAddr,
		Handler: s.Router,
	}
	return s.httpServer.ListenAndServe()
}

// Stop gracefully shuts down the server with a timeout to allow
// in-flight requests to complete.
//
// Returns:
//   - Any error encountered during the shutdown process
func (s *Server) Stop() error {
	if s.httpServer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return s.httpServer.Shutdown(ctx)
}

// setupRoutes configures the admin panel routes for different pages
// and functionality.
func (s *Server) setupRoutes() {
	// Static files
	fileServer := http.FileServer(http.Dir(filepath.Join(s.TemplateDir, "static")))
	s.Router.Handle("/static/*", http.StripPrefix("/static/", fileServer))

	// Health check endpoint
	s.Router.Get("/health", s.handleHealthCheck())

	// Admin routes
	s.Router.Get("/", s.handleDashboard())
	s.Router.Get("/clients", s.handleClients())
	s.Router.Get("/messages", s.handleMessages())
	s.Router.Get("/settings", s.handleSettings())

	// Add new visualization API endpoints
	s.Router.HandleFunc("/api/dashboard/stats", s.handleDashboardStats)
	s.Router.HandleFunc("/api/dashboard/message-flow", s.handleMessageFlow)
	s.Router.HandleFunc("/api/dashboard/topic-tree", s.handleTopicTree)
	s.Router.HandleFunc("/api/dashboard/topic-heatmap", s.handleTopicHeatmap)
	s.Router.HandleFunc("/api/dashboard/connection-map", s.handleConnectionMap)
}

// loadTemplates loads all HTML templates used by the admin interface
// from the configured template directory.
func (s *Server) loadTemplates() {
	// Define templates to load
	templates := []string{
		"dashboard",
		"clients",
		"messages",
		"settings",
	}

	// Load each template
	for _, tmpl := range templates {
		// Parse both layout and content template into a named template
		templateFiles := []string{
			filepath.Join(s.TemplateDir, "view", "layout.html"),
			filepath.Join(s.TemplateDir, "view", tmpl+".html"),
		}

		// Create template with a proper name
		t, err := template.ParseFiles(templateFiles...)
		if err != nil {
			log.Printf("Error parsing template %s: %v", tmpl, err)
			continue
		}

		s.templates[tmpl] = t
	}
}

// render executes a template with the provided data and writes the output
// to the HTTP response writer.
//
// Parameters:
//   - w: HTTP response writer to render the template to
//   - name: Name of the template to render
//   - data: Data to pass to the template for rendering
func (s *Server) render(w http.ResponseWriter, name string, data any) {
	tmpl, ok := s.templates[name]
	if !ok {
		http.Error(w, "Template not found", http.StatusInternalServerError)
		return
	}

	// Use a struct to pass data with the Active field for navigation highlighting
	type TemplateData struct {
		Active string
		Data   any
		Stats  *DashboardStats // Add Stats field for dashboard
	}

	templateData := TemplateData{
		Active: name,
		Data:   data,
	}

	// If rendering dashboard, include stats data
	if name == "dashboard" {
		// Get mock stats data (in production, this would come from real sources)
		stats := s.generateDashboardStats()
		templateData.Stats = stats
	}

	if err := tmpl.ExecuteTemplate(w, "layout.html", templateData); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// generateDashboardStats creates sample dashboard stats
// In a real implementation, this would fetch actual data
func (s *Server) generateDashboardStats() *DashboardStats {
	activeConnections := 250  // Placeholder for s.mqttServer.GetClientCount()
	messagesPerSecond := 75.5 // Placeholder for s.mqttServer.GetMessageRate()

	return &DashboardStats{
		ActiveConnections:   activeConnections,
		TotalConnections:    activeConnections + 1000,
		MessagesPerSecond:   messagesPerSecond,
		TotalMessages:       10000,
		ActiveSubscriptions: 500,
		TotalSubscriptions:  2000,
		CpuUsage:            getCPUUsage(),
		MemoryUsage:         getMemoryUsage(),
		ConnectionsByCountry: map[string]int{
			"US": 120,
			"DE": 85,
			"CN": 65,
			"IN": 45,
			"BR": 35,
		},
		TopTopics: []TopicStats{
			{Topic: "sensors/temperature", Messages: 5420, TrendValue: 1.2},
			{Topic: "devices/lighting", Messages: 3210, TrendValue: 0.8},
			{Topic: "home/security", Messages: 1850, TrendValue: -0.3},
			{Topic: "weather/forecast", Messages: 1200, TrendValue: 0.5},
		},
		QosDistribution: [3]int{6500, 3200, 300}, // QoS 0, 1, 2
	}
}

// DashboardData contains the statistics and information displayed
// on the admin dashboard page.
type DashboardData struct {
	ActiveClients  int
	MessageCount   int
	TopTopics      []TopicStats
	RecentMessages []MessageData
	ServerUptime   string
	ConnectedSince string
}

// TopicStats represents statistics for a topic
type TopicStats struct {
	Topic      string  `json:"topic"`
	Messages   int     `json:"messages"`
	TrendValue float64 `json:"trend"`
}

// MessageData represents a message for display in the admin interface.
type MessageData struct {
	Topic     string
	ClientID  string
	Timestamp string
	Size      int
}

// DashboardStats represents the stats shown on the dashboard
type DashboardStats struct {
	ActiveConnections    int            `json:"activeConnections"`
	TotalConnections     int            `json:"totalConnections"`
	MessagesPerSecond    float64        `json:"messagesPerSecond"`
	TotalMessages        int            `json:"totalMessages"`
	ActiveSubscriptions  int            `json:"activeSubscriptions"`
	TotalSubscriptions   int            `json:"totalSubscriptions"`
	CpuUsage             float64        `json:"cpuUsage"`
	MemoryUsage          float64        `json:"memoryUsage"`
	ConnectionsByCountry map[string]int `json:"connectionsByCountry"`
	TopTopics            []TopicStats   `json:"topTopics"`
	MessageFlow          []MessageFlow  `json:"messageFlow"`
	TopicActivity        map[string]int `json:"topicActivity"`
	TopicHierarchy       TopicNode      `json:"topicHierarchy"`
	QosDistribution      [3]int         `json:"qosDistribution"`
}

// MessageFlow represents a message flow between clients
type MessageFlow struct {
	Source      string    `json:"source"`
	Destination string    `json:"destination"`
	Topic       string    `json:"topic"`
	Timestamp   time.Time `json:"timestamp"`
	Size        int       `json:"size"`
}

// TopicNode represents a node in the topic hierarchy tree
type TopicNode struct {
	Name     string      `json:"name"`
	Count    int         `json:"count"`
	Children []TopicNode `json:"children"`
}

// handleDashboard returns an HTTP handler function for the dashboard page.
// The dashboard shows an overview of broker statistics.
func (s *Server) handleDashboard() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: Get actual data from storage
		data := DashboardData{
			ActiveClients:  0,
			MessageCount:   0,
			TopTopics:      []TopicStats{},
			RecentMessages: []MessageData{},
			ServerUptime:   "0h 0m 0s",
			ConnectedSince: "Not connected",
		}

		s.render(w, "dashboard", data)
	}
}

// handleClients returns an HTTP handler function for the clients page.
// This page displays information about connected MQTT clients.
func (s *Server) handleClients() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will come later
		s.render(w, "clients", nil)
	}
}

// handleMessages returns an HTTP handler function for the messages page.
// This page displays information about MQTT messages passing through the broker.
func (s *Server) handleMessages() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will come later
		s.render(w, "messages", nil)
	}
}

// handleSettings returns an HTTP handler function for the settings page.
// This page allows configuration of the MQTT broker.
func (s *Server) handleSettings() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will come later
		s.render(w, "settings", nil)
	}
}

// handleHealthCheck returns an HTTP handler function for the health check endpoint.
// This endpoint provides status information about the admin server.
func (s *Server) handleHealthCheck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Health check response structure
		healthStatus := map[string]any{
			"status":    "ok",
			"timestamp": time.Now().Format(time.RFC3339),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(healthStatus); err != nil {
			http.Error(w, "Error encoding health status", http.StatusInternalServerError)
		}
	}
}

// handleDashboardStats returns all dashboard statistics
func (s *Server) handleDashboardStats(w http.ResponseWriter, r *http.Request) {
	// Get server stats
	// In a real implementation, these would come from actual MQTT server methods
	// For now, using dummy values since these methods don't exist
	activeConnections := 250  // Placeholder for s.mqttServer.GetClientCount()
	messagesPerSecond := 75.5 // Placeholder for s.mqttServer.GetMessageRate()

	// Create a sample statistics object
	// In a real implementation, these would come from the MQTT server
	stats := DashboardStats{
		ActiveConnections:   activeConnections,
		TotalConnections:    activeConnections + 1000, // Sample data
		MessagesPerSecond:   messagesPerSecond,
		TotalMessages:       10000, // Sample data
		ActiveSubscriptions: 500,   // Sample data
		TotalSubscriptions:  2000,  // Sample data
		CpuUsage:            getCPUUsage(),
		MemoryUsage:         getMemoryUsage(),
		ConnectionsByCountry: map[string]int{
			"US": 120,
			"DE": 85,
			"CN": 65,
			"IN": 45,
			"BR": 35,
		},
		TopTopics: []TopicStats{
			{Topic: "sensors/temperature", Messages: 5420, TrendValue: 1.2},
			{Topic: "devices/lighting", Messages: 3210, TrendValue: 0.8},
			{Topic: "home/security", Messages: 1850, TrendValue: -0.3},
			{Topic: "weather/forecast", Messages: 1200, TrendValue: 0.5},
		},
		QosDistribution: [3]int{6500, 3200, 300}, // QoS 0, 1, 2
	}

	// Generate sample message flow data
	stats.MessageFlow = generateSampleMessageFlow()

	// Generate sample topic activity data
	stats.TopicActivity = generateSampleTopicActivity()

	// Generate sample topic hierarchy
	stats.TopicHierarchy = generateSampleTopicHierarchy()

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleMessageFlow returns real-time message flow data
func (s *Server) handleMessageFlow(w http.ResponseWriter, r *http.Request) {
	// In a real implementation, this would fetch actual message flow data
	flow := generateSampleMessageFlow()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(flow)
}

// handleTopicTree returns the topic hierarchy tree
func (s *Server) handleTopicTree(w http.ResponseWriter, r *http.Request) {
	// In a real implementation, this would build the actual topic tree
	tree := generateSampleTopicHierarchy()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tree)
}

// handleTopicHeatmap returns data for the topic activity heatmap
func (s *Server) handleTopicHeatmap(w http.ResponseWriter, r *http.Request) {
	// In a real implementation, this would return actual topic activity
	heatmap := generateSampleTopicActivity()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(heatmap)
}

// handleConnectionMap returns client connection geographical data
func (s *Server) handleConnectionMap(w http.ResponseWriter, r *http.Request) {
	// In a real implementation, this would retrieve actual connection locations
	connections := map[string][]struct {
		Lat   float64 `json:"lat"`
		Lon   float64 `json:"lon"`
		Count int     `json:"count"`
	}{
		"locations": {
			{Lat: 40.7128, Lon: -74.0060, Count: 25},  // New York
			{Lat: 51.5074, Lon: -0.1278, Count: 18},   // London
			{Lat: 35.6762, Lon: 139.6503, Count: 15},  // Tokyo
			{Lat: 37.7749, Lon: -122.4194, Count: 12}, // San Francisco
			{Lat: 55.7558, Lon: 37.6173, Count: 10},   // Moscow
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(connections)
}

// Helper functions for system metrics
func getCPUUsage() float64 {
	// This is a simplistic approach for demo purposes
	// In a real application, you would use more sophisticated monitoring
	var cpuUsage float64 = 0

	// On Linux/Unix systems, you could parse /proc/stat
	// On Windows, you could use WMI
	// Here's a simple approximation:
	runtime.GC() // Force garbage collection to get a cleaner sample

	startTime := time.Now()
	startCPU := runtime.NumCPU()

	// Do some busy work
	time.Sleep(100 * time.Millisecond)

	endTime := time.Now()
	duration := endTime.Sub(startTime).Seconds()

	// This is not an accurate measure, just for demonstration
	cpuUsage = float64(startCPU) / duration * 5

	// Keep it reasonable for the demo
	if cpuUsage > 100 {
		cpuUsage = 35.5
	}

	return cpuUsage
}

func getMemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Convert bytes to MB
	return float64(m.Alloc) / 1024 / 1024
}

// Sample data generators
func generateSampleMessageFlow() []MessageFlow {
	now := time.Now()

	return []MessageFlow{
		{Source: "client1", Destination: "client2", Topic: "sensors/temperature", Timestamp: now.Add(-1 * time.Second), Size: 24},
		{Source: "client3", Destination: "client4", Topic: "devices/lighting", Timestamp: now.Add(-2 * time.Second), Size: 16},
		{Source: "client5", Destination: "client1", Topic: "home/security", Timestamp: now.Add(-3 * time.Second), Size: 32},
		{Source: "client2", Destination: "client5", Topic: "weather/forecast", Timestamp: now.Add(-4 * time.Second), Size: 48},
		{Source: "client4", Destination: "client3", Topic: "sensors/humidity", Timestamp: now.Add(-5 * time.Second), Size: 20},
	}
}

func generateSampleTopicActivity() map[string]int {
	return map[string]int{
		"sensors/temperature": 120,
		"sensors/humidity":    85,
		"devices/lighting":    95,
		"home/security":       65,
		"weather/forecast":    45,
		"system/logs":         30,
		"users/presence":      25,
	}
}

func generateSampleTopicHierarchy() TopicNode {
	return TopicNode{
		Name:  "root",
		Count: 460,
		Children: []TopicNode{
			{
				Name:  "sensors",
				Count: 205,
				Children: []TopicNode{
					{Name: "temperature", Count: 120},
					{Name: "humidity", Count: 85},
				},
			},
			{
				Name:  "devices",
				Count: 95,
				Children: []TopicNode{
					{Name: "lighting", Count: 95},
				},
			},
			{
				Name:  "home",
				Count: 65,
				Children: []TopicNode{
					{Name: "security", Count: 65},
				},
			},
			{
				Name:  "weather",
				Count: 45,
				Children: []TopicNode{
					{Name: "forecast", Count: 45},
				},
			},
			{
				Name:  "system",
				Count: 30,
				Children: []TopicNode{
					{Name: "logs", Count: 30},
				},
			},
			{
				Name:  "users",
				Count: 25,
				Children: []TopicNode{
					{Name: "presence", Count: 25},
				},
			},
		},
	}
}
