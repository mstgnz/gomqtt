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
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
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
func NewServer(listenAddr, templateDir string, storage storage.Storage) *Server {
	s := &Server{
		Router:      chi.NewRouter(),
		Storage:     storage,
		ListenAddr:  listenAddr,
		TemplateDir: templateDir,
		templates:   make(map[string]*template.Template),
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
	}

	templateData := TemplateData{
		Active: name,
		Data:   data,
	}

	if err := tmpl.ExecuteTemplate(w, "layout.html", templateData); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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

// TopicStats represents statistics for a specific MQTT topic.
type TopicStats struct {
	Topic    string
	Messages int
	Clients  int
}

// MessageData represents a message for display in the admin interface.
type MessageData struct {
	Topic     string
	ClientID  string
	Timestamp string
	Size      int
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
		healthStatus := map[string]interface{}{
			"status":    "ok",
			"timestamp": time.Now().Format(time.RFC3339),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(healthStatus); err != nil {
			http.Error(w, "Error encoding health status", http.StatusInternalServerError)
		}
	}
}
