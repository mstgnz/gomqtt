package admin

import (
	"context"
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

// Server represents the admin panel server
type Server struct {
	Router      chi.Router
	Storage     *storage.PostgresStorage
	ListenAddr  string
	TemplateDir string
	templates   map[string]*template.Template
	httpServer  *http.Server
}

// NewServer creates a new admin panel server
func NewServer(listenAddr, templateDir string, storage *storage.PostgresStorage) *Server {
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

// Start starts the admin panel server
func (s *Server) Start() error {
	fmt.Printf("Admin panel started on %s\n", s.ListenAddr)
	s.httpServer = &http.Server{
		Addr:    s.ListenAddr,
		Handler: s.Router,
	}
	return s.httpServer.ListenAndServe()
}

// Stop gracefully shuts down the server with a timeout
func (s *Server) Stop() error {
	if s.httpServer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return s.httpServer.Shutdown(ctx)
}

// setupRoutes configures the admin panel routes
func (s *Server) setupRoutes() {
	// Static files
	fileServer := http.FileServer(http.Dir(filepath.Join(s.TemplateDir, "static")))
	s.Router.Handle("/static/*", http.StripPrefix("/static/", fileServer))

	// Admin routes
	s.Router.Get("/", s.handleDashboard())
	s.Router.Get("/clients", s.handleClients())
	s.Router.Get("/messages", s.handleMessages())
	s.Router.Get("/settings", s.handleSettings())
}

// loadTemplates loads all templates
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
			filepath.Join(s.TemplateDir, "layout.html"),
			filepath.Join(s.TemplateDir, tmpl+".html"),
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

// render renders a template with data
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

// Dashboard data
type DashboardData struct {
	ActiveClients  int
	MessageCount   int
	TopTopics      []TopicStats
	RecentMessages []MessageData
	ServerUptime   string
	ConnectedSince string
}

// TopicStats represents stats for a topic
type TopicStats struct {
	Topic    string
	Messages int
	Clients  int
}

// MessageData represents a message for display
type MessageData struct {
	Topic     string
	ClientID  string
	Timestamp string
	Size      int
}

// handleDashboard handles the dashboard page
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

// handleClients handles the clients page
func (s *Server) handleClients() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will come later
		s.render(w, "clients", nil)
	}
}

// handleMessages handles the messages page
func (s *Server) handleMessages() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will come later
		s.render(w, "messages", nil)
	}
}

// handleSettings handles the settings page
func (s *Server) handleSettings() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will come later
		s.render(w, "settings", nil)
	}
}
