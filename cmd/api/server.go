package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/mstgnz/gomqtt/auth"
	"github.com/mstgnz/gomqtt/storage"
)

// Server represents the REST API server
type Server struct {
	Router     chi.Router
	Auth       *auth.Auth
	Storage    *storage.PostgresStorage
	ListenAddr string
	httpServer *http.Server
}

// NewServer creates a new REST API server
func NewServer(listenAddr string, authService *auth.Auth, storage *storage.PostgresStorage) *Server {
	s := &Server{
		Router:     chi.NewRouter(),
		Auth:       authService,
		Storage:    storage,
		ListenAddr: listenAddr,
	}

	// Setup middleware
	s.Router.Use(middleware.Logger)
	s.Router.Use(middleware.Recoverer)
	s.Router.Use(middleware.Timeout(30 * time.Second))

	// Setup routes
	s.setupRoutes()

	return s
}

// Start starts the REST API server
func (s *Server) Start() error {
	fmt.Printf("REST API started on %s\n", s.ListenAddr)
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

// setupRoutes configures the API routes
func (s *Server) setupRoutes() {
	s.Router.Get("/", s.handleHome())

	// API routes
	s.Router.Route("/api", func(r chi.Router) {
		// Public endpoints
		r.Post("/login", s.handleLogin())

		// Protected endpoints
		r.Group(func(r chi.Router) {
			r.Use(s.authMiddleware)

			r.Get("/clients", s.handleListClients())
			r.Get("/clients/{clientID}", s.handleGetClient())
			r.Get("/messages", s.handleListMessages())
			r.Post("/publish", s.handlePublish())

			// Permission management endpoints
			r.Route("/permissions", func(r chi.Router) {
				r.Get("/", s.handleListPermissions())
				r.Post("/", s.handleCreatePermission())
				r.Get("/{username}", s.handleGetUserPermissions())
				r.Delete("/{username}/{topic}", s.handleDeletePermission())
			})

			// Message history endpoints
			r.Route("/history", func(r chi.Router) {
				r.Get("/", s.handleGetMessageHistory())
				r.Get("/topics", s.handleGetMessageTopics())
				r.Get("/topics/{topic}", s.handleGetTopicHistory())
				r.Get("/clients/{clientID}", s.handleGetClientHistory())
			})
		})
	})
}

// authMiddleware validates JWT tokens
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Remove "Bearer " prefix if present
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}

		claims, err := s.Auth.ValidateToken(token)
		if err != nil {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}

		// Set claims in request context
		ctx := r.Context()
		ctx = context.WithValue(ctx, "user", claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// handleHome handles the root endpoint
func (s *Server) handleHome() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "GoMQTT API Server",
			"version": "0.1.0",
		})
	}
}

// handleLogin handles the login endpoint
func (s *Server) handleLogin() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will go here
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}
}

// handleListClients handles the clients endpoint
func (s *Server) handleListClients() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will go here
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}
}

// handleGetClient handles the client detail endpoint
func (s *Server) handleGetClient() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID := chi.URLParam(r, "clientID")

		client, err := s.Storage.GetClientInfo(clientID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if client == nil {
			http.Error(w, "Client not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(client)
	}
}

// handleListMessages handles the messages endpoint
func (s *Server) handleListMessages() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will go here
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}
}

// handlePublish handles the publish endpoint
func (s *Server) handlePublish() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Implementation will go here
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}
}

// PermissionRequest represents a request to add a permission
type PermissionRequest struct {
	Username     string `json:"username"`
	TopicPattern string `json:"topic_pattern"`
	AccessLevel  any    `json:"access_level"` // Can be int or string
}

// PermissionResponse represents a permission for the API
type PermissionResponse struct {
	Username     string `json:"username"`
	TopicPattern string `json:"topic_pattern"`
	AccessLevel  string `json:"access_level"`
}

// handleListPermissions handles listing all permissions
func (s *Server) handleListPermissions() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get all users with their permissions from the storage
		permissions, err := s.Storage.GetAllPermissions()
		if err != nil {
			http.Error(w, "Failed to fetch permissions", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(permissions)
	}
}

// handleCreatePermission handles creating a new permission
func (s *Server) handleCreatePermission() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req PermissionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Username == "" || req.TopicPattern == "" {
			http.Error(w, "Missing required fields", http.StatusBadRequest)
			return
		}

		// Convert access level to AccessLevel type
		var accessLevel auth.AccessLevel
		switch v := req.AccessLevel.(type) {
		case float64:
			accessLevel = auth.AccessLevel(int(v))
		case string:
			switch v {
			case "read_only", "ReadOnly":
				accessLevel = auth.ReadOnly
			case "read_write", "ReadWrite":
				accessLevel = auth.ReadWrite
			case "admin", "Admin":
				accessLevel = auth.Admin
			default:
				http.Error(w, "Invalid access level", http.StatusBadRequest)
				return
			}
		default:
			http.Error(w, "Invalid access level type", http.StatusBadRequest)
			return
		}

		// Add permission
		err := s.Auth.AddUserPermission(req.Username, req.TopicPattern, accessLevel)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Permission created successfully",
		})
	}
}

// handleGetUserPermissions handles getting permissions for a specific user
func (s *Server) handleGetUserPermissions() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username := chi.URLParam(r, "username")
		if username == "" {
			http.Error(w, "Username is required", http.StatusBadRequest)
			return
		}

		// Get user from auth service
		user, err := s.Auth.GetUser(username)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		// Convert permissions to API format
		var apiPermissions []PermissionResponse
		for _, perm := range user.Permissions {
			var accessLevel string
			switch perm.AccessLevel {
			case auth.ReadOnly:
				accessLevel = "read_only"
			case auth.ReadWrite:
				accessLevel = "read_write"
			case auth.Admin:
				accessLevel = "admin"
			}

			apiPermissions = append(apiPermissions, PermissionResponse{
				Username:     username,
				TopicPattern: perm.TopicPattern,
				AccessLevel:  accessLevel,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(apiPermissions)
	}
}

// handleDeletePermission handles deleting a permission
func (s *Server) handleDeletePermission() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username := chi.URLParam(r, "username")
		topicPattern := chi.URLParam(r, "topic")

		if username == "" || topicPattern == "" {
			http.Error(w, "Username and topic are required", http.StatusBadRequest)
			return
		}

		// Delete permission
		err := s.Auth.RemoveUserPermission(username, topicPattern)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Permission deleted successfully",
		})
	}
}

// handleGetMessageHistory retrieves message history with filtering
func (s *Server) handleGetMessageHistory() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Storage == nil {
			http.Error(w, "Storage is not available", http.StatusServiceUnavailable)
			return
		}

		// Parse query parameters
		query := storage.MessageQuery{
			Topic:    r.URL.Query().Get("topic"),
			ClientID: r.URL.Query().Get("client_id"),
		}

		// Parse timestamps
		if fromStr := r.URL.Query().Get("from"); fromStr != "" {
			fromTime, err := time.Parse(time.RFC3339, fromStr)
			if err != nil {
				http.Error(w, "Invalid 'from' timestamp format. Use RFC3339 format (e.g., 2006-01-02T15:04:05Z)", http.StatusBadRequest)
				return
			}
			query.FromTimestamp = fromTime
		}

		if toStr := r.URL.Query().Get("to"); toStr != "" {
			toTime, err := time.Parse(time.RFC3339, toStr)
			if err != nil {
				http.Error(w, "Invalid 'to' timestamp format. Use RFC3339 format (e.g., 2006-01-02T15:04:05Z)", http.StatusBadRequest)
				return
			}
			query.ToTimestamp = toTime
		}

		// Parse pagination parameters
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			limit, err := strconv.Atoi(limitStr)
			if err != nil {
				http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
				return
			}
			query.Limit = limit
		}

		if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
			offset, err := strconv.Atoi(offsetStr)
			if err != nil {
				http.Error(w, "Invalid offset parameter", http.StatusBadRequest)
				return
			}
			query.Offset = offset
		}

		// Query the database
		result, err := s.Storage.GetMessages(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to retrieve messages: %v", err), http.StatusInternalServerError)
			return
		}

		// Format and return response
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

// handleGetMessageTopics retrieves distinct topics from message history
func (s *Server) handleGetMessageTopics() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Currently we don't have a direct method to get topics,
		// so we'll implement a placeholder
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Topic history listing is not yet implemented",
		})
	}
}

// handleGetTopicHistory retrieves message history for a specific topic
func (s *Server) handleGetTopicHistory() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Storage == nil {
			http.Error(w, "Storage is not available", http.StatusServiceUnavailable)
			return
		}

		// Get topic from URL
		topic := chi.URLParam(r, "topic")
		if topic == "" {
			http.Error(w, "Topic is required", http.StatusBadRequest)
			return
		}

		// Create query with the topic
		query := storage.MessageQuery{
			Topic: topic,
		}

		// Parse timestamps
		if fromStr := r.URL.Query().Get("from"); fromStr != "" {
			fromTime, err := time.Parse(time.RFC3339, fromStr)
			if err != nil {
				http.Error(w, "Invalid 'from' timestamp format", http.StatusBadRequest)
				return
			}
			query.FromTimestamp = fromTime
		}

		if toStr := r.URL.Query().Get("to"); toStr != "" {
			toTime, err := time.Parse(time.RFC3339, toStr)
			if err != nil {
				http.Error(w, "Invalid 'to' timestamp format", http.StatusBadRequest)
				return
			}
			query.ToTimestamp = toTime
		}

		// Parse pagination parameters
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			limit, err := strconv.Atoi(limitStr)
			if err != nil {
				http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
				return
			}
			query.Limit = limit
		}

		if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
			offset, err := strconv.Atoi(offsetStr)
			if err != nil {
				http.Error(w, "Invalid offset parameter", http.StatusBadRequest)
				return
			}
			query.Offset = offset
		}

		// Query the database
		result, err := s.Storage.GetMessages(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to retrieve messages: %v", err), http.StatusInternalServerError)
			return
		}

		// Format and return response
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

// handleGetClientHistory retrieves message history for a specific client
func (s *Server) handleGetClientHistory() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Storage == nil {
			http.Error(w, "Storage is not available", http.StatusServiceUnavailable)
			return
		}

		// Get client ID from URL
		clientID := chi.URLParam(r, "clientID")
		if clientID == "" {
			http.Error(w, "Client ID is required", http.StatusBadRequest)
			return
		}

		// Create query with the client ID
		query := storage.MessageQuery{
			ClientID: clientID,
		}

		// Parse timestamps
		if fromStr := r.URL.Query().Get("from"); fromStr != "" {
			fromTime, err := time.Parse(time.RFC3339, fromStr)
			if err != nil {
				http.Error(w, "Invalid 'from' timestamp format", http.StatusBadRequest)
				return
			}
			query.FromTimestamp = fromTime
		}

		if toStr := r.URL.Query().Get("to"); toStr != "" {
			toTime, err := time.Parse(time.RFC3339, toStr)
			if err != nil {
				http.Error(w, "Invalid 'to' timestamp format", http.StatusBadRequest)
				return
			}
			query.ToTimestamp = toTime
		}

		// Parse pagination parameters
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			limit, err := strconv.Atoi(limitStr)
			if err != nil {
				http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
				return
			}
			query.Limit = limit
		}

		if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
			offset, err := strconv.Atoi(offsetStr)
			if err != nil {
				http.Error(w, "Invalid offset parameter", http.StatusBadRequest)
				return
			}
			query.Offset = offset
		}

		// Query the database
		result, err := s.Storage.GetMessages(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to retrieve messages: %v", err), http.StatusInternalServerError)
			return
		}

		// Format and return response
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}
