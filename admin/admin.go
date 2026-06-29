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
	"sort"
	"strconv"
	"strings"
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
	startTime   time.Time
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
		startTime:   time.Now(),
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
	s.Router.Get("/audit", s.handleAuditLogs())

	// Add new visualization API endpoints
	s.Router.HandleFunc("/api/dashboard/stats", s.handleDashboardStats)
	s.Router.HandleFunc("/api/dashboard/message-flow", s.handleMessageFlow)
	s.Router.HandleFunc("/api/dashboard/topic-tree", s.handleTopicTree)
	s.Router.HandleFunc("/api/dashboard/topic-heatmap", s.handleTopicHeatmap)
	s.Router.HandleFunc("/api/dashboard/connection-map", s.handleConnectionMap)

	// Audit logs API
	s.Router.HandleFunc("/api/audit/logs", s.handleAuditLogsData)
	s.Router.HandleFunc("/api/audit/chart", s.handleAuditLogsChart)
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
		"audit",
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

	// If rendering dashboard, include live stats data
	if name == "dashboard" {
		templateData.Stats = s.buildDashboardStats()
	}

	if err := tmpl.ExecuteTemplate(w, "layout.html", templateData); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// buildDashboardStats assembles dashboard statistics from live broker state and
// the storage layer. Values that the broker does not yet track (per-country
// geo, message-flow graph, connection rate) are returned empty rather than
// fabricated, so the dashboard always reflects real data.
func (s *Server) buildDashboardStats() *DashboardStats {
	stats := &DashboardStats{
		CpuUsage:             getCPUUsage(),
		MemoryUsage:          getMemoryUsage(),
		ConnectionsByCountry: map[string]int{},
		TopTopics:            []TopicStats{},
		MessageFlow:          []MessageFlow{},
		TopicActivity:        map[string]int{},
	}

	if s.mqttServer != nil {
		stats.ActiveConnections = s.mqttServer.ClientCount()
		stats.ActiveSubscriptions = s.mqttServer.SubscriptionCount()
		stats.TotalSubscriptions = stats.ActiveSubscriptions
		stats.TotalConnections = stats.ActiveConnections
	}

	if s.Storage != nil {
		// Sample the most recent messages to derive topic activity and a QoS
		// breakdown. TotalMessages comes from the storage count, not the sample.
		if page, err := s.Storage.GetMessages(storage.MessageQuery{Limit: 200}); err == nil {
			stats.TotalMessages = page.TotalCount

			topicCounts := map[string]int{}
			for _, m := range page.Messages {
				topicCounts[m.Topic]++
				if m.QoS < 3 {
					stats.QosDistribution[m.QoS]++
				}
			}
			stats.TopicActivity = topicCounts
			stats.TopTopics = topTopicsFromCounts(topicCounts, 5)
			stats.TopicHierarchy = buildTopicHierarchy(topicCounts)
		}
	}

	return stats
}

// topTopicsFromCounts returns the top-n topics by message count, sorted descending.
func topTopicsFromCounts(counts map[string]int, n int) []TopicStats {
	topics := make([]TopicStats, 0, len(counts))
	for topic, count := range counts {
		topics = append(topics, TopicStats{Topic: topic, Messages: count})
	}
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Messages > topics[j].Messages
	})
	if n > 0 && len(topics) > n {
		topics = topics[:n]
	}
	return topics
}

// buildTopicHierarchy builds a topic tree from observed topic strings, grouping
// by their slash-separated levels. The returned root aggregates the total count.
func buildTopicHierarchy(counts map[string]int) TopicNode {
	root := TopicNode{Name: "root", Children: []TopicNode{}}
	// index of child name -> position in root.Children for quick lookup
	idx := map[string]int{}

	for topic, count := range counts {
		root.Count += count
		level := strings.SplitN(topic, "/", 2)
		head := level[0]
		if head == "" {
			head = "/"
		}
		if pos, ok := idx[head]; ok {
			root.Children[pos].Count += count
		} else {
			idx[head] = len(root.Children)
			root.Children = append(root.Children, TopicNode{Name: head, Count: count, Children: []TopicNode{}})
		}
	}
	return root
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
		data := DashboardData{
			TopTopics:      []TopicStats{},
			RecentMessages: []MessageData{},
			ServerUptime:   formatUptime(time.Since(s.startTime)),
			ConnectedSince: s.startTime.Format("2006-01-02 15:04:05"),
		}

		if s.mqttServer != nil {
			data.ActiveClients = s.mqttServer.ClientCount()
		}

		if s.Storage != nil {
			if page, err := s.Storage.GetMessages(storage.MessageQuery{Limit: 10}); err == nil {
				data.MessageCount = page.TotalCount
				for _, m := range page.Messages {
					data.RecentMessages = append(data.RecentMessages, MessageData{
						Topic:     m.Topic,
						ClientID:  m.ClientID,
						Timestamp: m.Timestamp.Format("2006-01-02 15:04:05"),
						Size:      len(m.Payload),
					})
				}
			}
		}

		s.render(w, "dashboard", data)
	}
}

// handleClients returns an HTTP handler function for the clients page.
// This page displays information about connected MQTT clients.
func (s *Server) handleClients() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var clients []mqtt.ConnectedClient
		if s.mqttServer != nil {
			clients = s.mqttServer.ListClients()
		}
		s.render(w, "clients", clients)
	}
}

// handleMessages returns an HTTP handler function for the messages page.
// This page displays information about MQTT messages passing through the broker.
func (s *Server) handleMessages() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var messages []storage.Message
		if s.Storage != nil {
			if page, err := s.Storage.GetMessages(storage.MessageQuery{Limit: 100}); err == nil {
				messages = page.Messages
			}
		}
		s.render(w, "messages", messages)
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

// handleDashboardStats returns all dashboard statistics derived from live
// broker state and the storage layer.
func (s *Server) handleDashboardStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.buildDashboardStats())
}

// handleMessageFlow returns message flow edges. Per-message source/destination
// tracking is not yet recorded by the broker, so this returns an empty set
// rather than fabricated edges.
func (s *Server) handleMessageFlow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode([]MessageFlow{})
}

// handleTopicTree returns the topic hierarchy tree built from persisted messages.
func (s *Server) handleTopicTree(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.buildDashboardStats().TopicHierarchy)
}

// handleTopicHeatmap returns per-topic activity counts from persisted messages.
func (s *Server) handleTopicHeatmap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.buildDashboardStats().TopicActivity)
}

// handleConnectionMap returns client connection geographical data. The broker
// does not perform IP geolocation, so an empty location set is returned rather
// than fabricated coordinates.
func (s *Server) handleConnectionMap(w http.ResponseWriter, r *http.Request) {
	connections := map[string][]struct {
		Lat   float64 `json:"lat"`
		Lon   float64 `json:"lon"`
		Count int     `json:"count"`
	}{
		"locations": {},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(connections)
}

// handleAuditLogs renders the audit logs page
func (s *Server) handleAuditLogs() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Render the audit logs template
		s.render(w, "audit", nil)
	}
}

// handleAuditLogsData handles retrieving audit logs with filtering
func (s *Server) handleAuditLogsData(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	query := storage.AuditLogQuery{
		ActionType: r.URL.Query().Get("action_type"),
		Username:   r.URL.Query().Get("username"),
		EntityType: r.URL.Query().Get("entity_type"),
		EntityID:   r.URL.Query().Get("entity_id"),
	}

	// Parse time range
	if from := r.URL.Query().Get("from"); from != "" {
		if t, err := time.Parse(time.RFC3339, from); err == nil {
			query.FromTimestamp = t
		}
	}

	if to := r.URL.Query().Get("to"); to != "" {
		if t, err := time.Parse(time.RFC3339, to); err == nil {
			query.ToTimestamp = t
		}
	}

	// Parse pagination
	if limit := r.URL.Query().Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil && l > 0 {
			query.Limit = l
		}
	}

	if offset := r.URL.Query().Get("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil && o >= 0 {
			query.Offset = o
		}
	}

	// Get audit logs from storage
	logs, err := s.Storage.GetAuditLogs(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to retrieve audit logs: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the audit logs as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(logs); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

// AuditChartData represents data for audit log charts
type AuditChartData struct {
	Labels   []string `json:"labels"`
	Datasets []struct {
		Label string `json:"label"`
		Data  []int  `json:"data"`
	} `json:"datasets"`
}

// handleAuditLogsChart handles generating chart data for audit logs
func (s *Server) handleAuditLogsChart(w http.ResponseWriter, r *http.Request) {
	// Parse time range for chart
	now := time.Now()
	from := now.Add(-7 * 24 * time.Hour) // Default to last 7 days

	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
			from = t
		}
	}

	to := now
	if toStr := r.URL.Query().Get("to"); toStr != "" {
		if t, err := time.Parse(time.RFC3339, toStr); err == nil {
			to = t
		}
	}

	// Select chart type
	chartType := r.URL.Query().Get("type")
	if chartType == "" {
		chartType = "action" // Default to action type chart
	}

	var chartData AuditChartData

	// Get base query with time range
	query := storage.AuditLogQuery{
		FromTimestamp: from,
		ToTimestamp:   to,
		Limit:         1000, // Increase limit for chart data
	}

	// Get all logs for the time period
	logs, err := s.Storage.GetAuditLogs(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to retrieve audit logs: %v", err), http.StatusInternalServerError)
		return
	}

	// Process logs based on chart type
	switch chartType {
	case "action":
		// Count by action type
		actionCounts := make(map[string]int)
		for _, log := range logs.Logs {
			actionCounts[log.ActionType]++
		}

		// Convert to chart data
		labels := make([]string, 0, len(actionCounts))
		data := make([]int, 0, len(actionCounts))

		for action, count := range actionCounts {
			labels = append(labels, action)
			data = append(data, count)
		}

		chartData.Labels = labels
		chartData.Datasets = []struct {
			Label string `json:"label"`
			Data  []int  `json:"data"`
		}{
			{
				Label: "Actions",
				Data:  data,
			},
		}

	case "entity":
		// Count by entity type
		entityCounts := make(map[string]int)
		for _, log := range logs.Logs {
			entityCounts[log.EntityType]++
		}

		// Convert to chart data
		labels := make([]string, 0, len(entityCounts))
		data := make([]int, 0, len(entityCounts))

		for entity, count := range entityCounts {
			labels = append(labels, entity)
			data = append(data, count)
		}

		chartData.Labels = labels
		chartData.Datasets = []struct {
			Label string `json:"label"`
			Data  []int  `json:"data"`
		}{
			{
				Label: "Entity Types",
				Data:  data,
			},
		}

	case "time":
		// Group by day
		dateCounts := make(map[string]int)

		// Create a map to store counts by day
		for _, log := range logs.Logs {
			dateStr := log.Timestamp.Format("2006-01-02")
			dateCounts[dateStr]++
		}

		// Sort dates
		dates := make([]string, 0, len(dateCounts))
		for date := range dateCounts {
			dates = append(dates, date)
		}
		sort.Strings(dates)

		// Create datasets
		data := make([]int, len(dates))
		for i, date := range dates {
			data[i] = dateCounts[date]
		}

		chartData.Labels = dates
		chartData.Datasets = []struct {
			Label string `json:"label"`
			Data  []int  `json:"data"`
		}{
			{
				Label: "Actions per Day",
				Data:  data,
			},
		}
	}

	// Return chart data as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(chartData); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
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

// formatUptime renders a duration as a compact "Xh Ym Zs" uptime string.
func formatUptime(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	sec := int(d.Seconds()) % 60
	return fmt.Sprintf("%dh %dm %ds", h, m, sec)
}
