// Package api provides a RESTful HTTP API for the GoMQTT broker.
// It enables programmatic access to broker functionality including client management,
// authorization management, message history, and runtime configuration.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/mstgnz/gomqtt/auth"
	"github.com/mstgnz/gomqtt/storage"
)

// Define a custom type for context keys to avoid collisions
type contextKey string

// Define constants for different context keys
const (
	userContextKey contextKey = "user"
)

// Server represents the REST API server for the MQTT broker.
// It provides HTTP endpoints for management, monitoring, and integration
// with external systems.
type Server struct {
	Router     chi.Router
	Auth       *auth.Auth
	Storage    storage.Storage
	MQTTServer any // Reference to MQTT server for health checks
	ListenAddr string
	httpServer *http.Server
}

// NewServer creates a new REST API server with the specified configuration.
//
// Parameters:
//   - listenAddr: Network address on which the API server will listen
//   - authService: Authentication service for validating API credentials
//   - storage: Storage interface for accessing persistent data
//
// Returns:
//   - A configured API server instance ready to be started
func NewServer(listenAddr string, authService *auth.Auth, storage storage.Storage) *Server {
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

// Start begins the REST API server on the configured listen address.
// This method blocks until the server is stopped or encounters an error.
//
// Returns:
//   - Any error encountered while starting or running the server
func (s *Server) Start() error {
	fmt.Printf("REST API started on %s\n", s.ListenAddr)
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

// setupRoutes configures the API routes
func (s *Server) setupRoutes() {
	s.Router.Get("/", s.handleHome())

	// Add route for scalar.yaml
	s.Router.Get("/scalar.yaml", s.handleScalarYAML())

	// Health check endpoint
	s.Router.Get("/health", s.handleHealthCheck())

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

			// User management endpoints
			r.Route("/users", func(r chi.Router) {
				r.Get("/", s.handleListUsers())
				r.Post("/", s.handleCreateUser())
				r.Get("/{username}", s.handleGetUser())
				r.Delete("/{username}", s.handleDeleteUser())
			})

			// Role management endpoints (RBAC)
			r.Route("/roles", func(r chi.Router) {
				r.Get("/", s.handleListRoles())
				r.Post("/", s.handleCreateRole())
				r.Get("/{roleName}", s.handleGetRole())
				r.Put("/{roleName}", s.handleUpdateRole())
				r.Delete("/{roleName}", s.handleDeleteRole())

				// User-role assignments
				r.Post("/{roleName}/users/{username}", s.handleAssignRoleToUser())
				r.Delete("/{roleName}/users/{username}", s.handleRemoveRoleFromUser())
				r.Get("/users/{username}", s.handleGetUserRoles())
			})

			// Message history endpoints
			r.Route("/history", func(r chi.Router) {
				r.Get("/", s.handleGetMessageHistory())
				r.Get("/topics", s.handleGetMessageTopics())
				r.Get("/topics/{topic}", s.handleGetTopicHistory())
				r.Get("/clients/{clientID}", s.handleGetClientHistory())
			})

			// Audit logs endpoints
			r.Route("/audit", func(r chi.Router) {
				r.Get("/", s.handleGetAuditLogs())
				r.Get("/users/{username}", s.handleGetUserAuditLogs())
				r.Get("/actions/{actionType}", s.handleGetActionAuditLogs())
				r.Get("/entities/{entityType}/{entityID}", s.handleGetEntityAuditLogs())
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
		ctx = context.WithValue(ctx, userContextKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// handleHome handles the root endpoint of the API server.
// It serves the Scalar API documentation HTML page.
func (s *Server) handleHome() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set the content type to HTML
		w.Header().Set("Content-Type", "text/html")

		// Try to read the scalar.html file
		content, err := os.ReadFile("api/scalar.html")
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read Scalar documentation: %v", err), http.StatusInternalServerError)
			return
		}

		// Write the content to the response
		w.Write(content)
	}
}

// handleLogin handles user authentication and token generation
func (s *Server) handleLogin() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var credentials struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}

		if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate credentials
		if credentials.Username == "" || credentials.Password == "" {
			http.Error(w, "Username and password are required", http.StatusBadRequest)
			return
		}

		// Authenticate user
		user, err := s.Auth.AuthenticateUser(credentials.Username, credentials.Password)
		if err != nil {
			http.Error(w, "Invalid credentials", http.StatusUnauthorized)
			return
		}

		// Generate JWT token (assuming 24 hour validity)
		token, err := s.Auth.GenerateToken("", credentials.Username, 24*time.Hour)
		if err != nil {
			http.Error(w, "Failed to generate token", http.StatusInternalServerError)
			return
		}

		// Update last login timestamp
		user.LastLogin = time.Now()

		// Log the login action
		s.logAction(r, "login", "user", credentials.Username, nil)

		// Return token
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"token": token,
		})
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

// handlePublish handles publishing a message to a topic
func (s *Server) handlePublish() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Topic    string          `json:"topic"`
			Payload  json.RawMessage `json:"payload"`
			QoS      int             `json:"qos"`
			Retained bool            `json:"retained"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Topic == "" {
			http.Error(w, "Topic is required", http.StatusBadRequest)
			return
		}

		// Validate QoS
		if req.QoS < 0 || req.QoS > 2 {
			http.Error(w, "QoS must be 0, 1, or 2", http.StatusBadRequest)
			return
		}

		// Get publisher information from context
		ctx := r.Context()
		claims, ok := ctx.Value(userContextKey).(map[string]any)
		username := ""
		if ok {
			if u, ok := claims["username"].(string); ok {
				username = u
			}
		}

		// Message details for audit log
		details := map[string]any{
			"topic":    req.Topic,
			"qos":      req.QoS,
			"retained": req.Retained,
			"size":     len(req.Payload),
			"username": username,
		}

		// TODO: Publish message to MQTT broker
		// In a real implementation, this would call the MQTT broker's Publish method

		// Log the publish action
		s.logAction(r, "publish", "message", req.Topic, details)

		// Return success
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Message published successfully",
		})
	}
}

// PermissionRequest represents a request to add or modify a permission.
type PermissionRequest struct {
	Username     string `json:"username"`
	TopicPattern string `json:"topic_pattern"`
	AccessLevel  any    `json:"access_level"` // Can be int or string
}

// PermissionResponse represents a permission in API responses.
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
			http.Error(w, "Username and topic pattern are required", http.StatusBadRequest)
			return
		}

		// Convert access level to int
		var accessLevel int
		switch v := req.AccessLevel.(type) {
		case float64:
			accessLevel = int(v)
		case int:
			accessLevel = v
		case string:
			var err error
			accessLevel, err = strconv.Atoi(v)
			if err != nil {
				// Try to map string values to numeric values
				switch strings.ToLower(v) {
				case "read":
					accessLevel = 1
				case "write":
					accessLevel = 2
				case "readwrite", "read-write":
					accessLevel = 3
				default:
					http.Error(w, "Invalid access level format", http.StatusBadRequest)
					return
				}
			}
		default:
			http.Error(w, "Invalid access level format", http.StatusBadRequest)
			return
		}

		// Store permission
		if err := s.Storage.StorePermission(req.Username, req.TopicPattern, accessLevel); err != nil {
			http.Error(w, fmt.Sprintf("Failed to store permission: %v", err), http.StatusInternalServerError)
			return
		}

		// Log the action
		details := map[string]any{
			"topic_pattern": req.TopicPattern,
			"access_level":  accessLevel,
		}
		s.logAction(r, "create_permission", "permission", req.Username, details)

		// Return success response
		response := PermissionResponse{
			Username:     req.Username,
			TopicPattern: req.TopicPattern,
			AccessLevel:  fmt.Sprintf("%d", accessLevel), // Convert to string for response
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "Error encoding response", http.StatusInternalServerError)
			return
		}
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
		topic := chi.URLParam(r, "topic")

		if username == "" || topic == "" {
			http.Error(w, "Username and topic are required", http.StatusBadRequest)
			return
		}

		// Delete permission
		if err := s.Storage.DeletePermission(username, topic); err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete permission: %v", err), http.StatusInternalServerError)
			return
		}

		// Log the action
		details := map[string]any{
			"topic_pattern": topic,
		}
		s.logAction(r, "delete_permission", "permission", username, details)

		// Return success response
		w.WriteHeader(http.StatusNoContent)
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

// UserRequest represents a user creation or update request.
type UserRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	IsAdmin  bool   `json:"is_admin"`
}

// UserResponse represents a user in API responses.
type UserResponse struct {
	Username    string    `json:"username"`
	IsAdmin     bool      `json:"is_admin"`
	Permissions []string  `json:"permissions"`
	Roles       []string  `json:"roles,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	LastLogin   time.Time `json:"last_login,omitempty"`
}

// RoleRequest represents a role creation or update request.
type RoleRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Permissions []struct {
		TopicPattern string `json:"topic_pattern"`
		AccessLevel  any    `json:"access_level"` // Can be int or string
	} `json:"permissions"`
}

// RoleResponse represents a role in API responses.
type RoleResponse struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Permissions []string  `json:"permissions"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// handleListUsers handles the endpoint to list all users
func (s *Server) handleListUsers() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get users from authentication service
		// In a real implementation, this would need pagination
		users := []UserResponse{}

		// Convert internal user objects to API response format
		// This is a simplified example - in production you'd need proper pagination
		// and would not expose all users to all clients
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)

		// For security, only admin users can see all users
		user, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		if !user.IsAdmin {
			// Regular users can only see themselves
			userObj, err := s.Auth.GetUser(claims.Username)
			if err != nil {
				http.Error(w, "User not found", http.StatusNotFound)
				return
			}

			// Create a response with permissions formatted as strings
			permStrings := make([]string, len(userObj.Permissions))
			for i, perm := range userObj.Permissions {
				level := "read-only"
				if perm.AccessLevel == auth.ReadWrite {
					level = "read-write"
				} else if perm.AccessLevel == auth.Admin {
					level = "admin"
				}
				permStrings[i] = fmt.Sprintf("%s (%s)", perm.TopicPattern, level)
			}

			users = append(users, UserResponse{
				Username:    userObj.Username,
				IsAdmin:     userObj.IsAdmin,
				Permissions: permStrings,
				Roles:       userObj.Roles,
				CreatedAt:   userObj.CreatedAt,
				LastLogin:   userObj.LastLogin,
			})
		} else {
			// Admin users can see all users
			// In a real implementation, this would be paginated
			for username, userObj := range s.Auth.GetAllUsers() {
				permStrings := make([]string, len(userObj.Permissions))
				for i, perm := range userObj.Permissions {
					level := "read-only"
					if perm.AccessLevel == auth.ReadWrite {
						level = "read-write"
					} else if perm.AccessLevel == auth.Admin {
						level = "admin"
					}
					permStrings[i] = fmt.Sprintf("%s (%s)", perm.TopicPattern, level)
				}

				users = append(users, UserResponse{
					Username:    username,
					IsAdmin:     userObj.IsAdmin,
					Permissions: permStrings,
					Roles:       userObj.Roles,
					CreatedAt:   userObj.CreatedAt,
					LastLogin:   userObj.LastLogin,
				})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(users)
	}
}

// handleCreateUser handles creating a new user
func (s *Server) handleCreateUser() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Username == "" || req.Password == "" {
			http.Error(w, "Username and password are required", http.StatusBadRequest)
			return
		}

		// Check if username already exists
		// This is a simplified check and would need proper implementation
		// with the storage layer
		existingUser, err := s.Auth.GetUser(req.Username)
		if err == nil && existingUser != nil {
			http.Error(w, "Username already exists", http.StatusConflict)
			return
		}

		// Create user
		// Note: In a real implementation, this would be a call to the authentication
		// service to create the user with proper password hashing
		err = s.Auth.RegisterUser(req.Username, req.Password, req.IsAdmin)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to create user: %v", err), http.StatusInternalServerError)
			return
		}

		// Log the action
		details := map[string]any{
			"is_admin": req.IsAdmin,
		}
		s.logAction(r, "create_user", "user", req.Username, details)

		// Return success response
		response := UserResponse{
			Username:  req.Username,
			IsAdmin:   req.IsAdmin,
			CreatedAt: time.Now(),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "Error encoding response", http.StatusInternalServerError)
			return
		}
	}
}

// handleGetUser handles the endpoint to get a user by username
func (s *Server) handleGetUser() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username := chi.URLParam(r, "username")

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Only admins can view other users' details
		if username != claims.Username && !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required to view other users", http.StatusForbidden)
			return
		}

		// Get the requested user
		user, err := s.Auth.GetUser(username)
		if err != nil {
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}

		// Format permissions as strings
		permStrings := make([]string, len(user.Permissions))
		for i, perm := range user.Permissions {
			level := "read-only"
			if perm.AccessLevel == auth.ReadWrite {
				level = "read-write"
			} else if perm.AccessLevel == auth.Admin {
				level = "admin"
			}
			permStrings[i] = fmt.Sprintf("%s (%s)", perm.TopicPattern, level)
		}

		response := UserResponse{
			Username:    user.Username,
			IsAdmin:     user.IsAdmin,
			Permissions: permStrings,
			Roles:       user.Roles,
			CreatedAt:   user.CreatedAt,
			LastLogin:   user.LastLogin,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// handleDeleteUser handles deleting a user
func (s *Server) handleDeleteUser() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username := chi.URLParam(r, "username")
		if username == "" {
			http.Error(w, "Username is required", http.StatusBadRequest)
			return
		}

		// Check if the user exists
		user, err := s.Auth.GetUser(username)
		if err != nil || user == nil {
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}

		// Delete user
		if err := s.Auth.DeleteUser(username); err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete user: %v", err), http.StatusInternalServerError)
			return
		}

		// Log the action
		s.logAction(r, "delete_user", "user", username, nil)

		w.WriteHeader(http.StatusNoContent)
	}
}

// handleListRoles handles the endpoint to list all roles
func (s *Server) handleListRoles() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.IsRBACEnabled() {
			http.Error(w, "RBAC is not enabled", http.StatusNotImplemented)
			return
		}

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Only admins can view roles
		if !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required", http.StatusForbidden)
			return
		}

		// Get all roles
		roles := s.Auth.ListRoles()
		response := make([]RoleResponse, 0, len(roles))

		for _, role := range roles {
			// Format permissions as strings
			permStrings := make([]string, len(role.Permissions))
			for i, perm := range role.Permissions {
				level := "read-only"
				if perm.AccessLevel == auth.ReadWrite {
					level = "read-write"
				} else if perm.AccessLevel == auth.Admin {
					level = "admin"
				}
				permStrings[i] = fmt.Sprintf("%s (%s)", perm.TopicPattern, level)
			}

			response = append(response, RoleResponse{
				Name:        role.Name,
				Description: role.Description,
				Permissions: permStrings,
				CreatedAt:   role.CreatedAt,
				UpdatedAt:   role.UpdatedAt,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// handleCreateRole handles the endpoint to create a new role
func (s *Server) handleCreateRole() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.IsRBACEnabled() {
			http.Error(w, "RBAC is not enabled", http.StatusNotImplemented)
			return
		}

		var req RoleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		// Check required fields
		if req.Name == "" {
			http.Error(w, "Role name is required", http.StatusBadRequest)
			return
		}

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Only admins can create roles
		if !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required", http.StatusForbidden)
			return
		}

		// Convert permissions
		permissions := make([]auth.Permission, len(req.Permissions))
		for i, perm := range req.Permissions {
			var accessLevel auth.AccessLevel

			// Handle different access level formats
			switch v := perm.AccessLevel.(type) {
			case float64:
				accessLevel = auth.AccessLevel(int(v))
			case int:
				accessLevel = auth.AccessLevel(v)
			case string:
				switch v {
				case "read-only":
					accessLevel = auth.ReadOnly
				case "read-write":
					accessLevel = auth.ReadWrite
				case "admin":
					accessLevel = auth.Admin
				default:
					accessLevel = auth.ReadOnly
				}
			default:
				accessLevel = auth.ReadOnly
			}

			permissions[i] = auth.Permission{
				TopicPattern: perm.TopicPattern,
				AccessLevel:  accessLevel,
			}
		}

		// Create the role
		if err := s.Auth.CreateRole(req.Name, req.Description, permissions); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Log the action
		details := map[string]any{
			"name":              req.Name,
			"description":       req.Description,
			"permissions_count": len(permissions),
		}
		s.logAction(r, "create_role", "role", req.Name, details)

		// Get the created role
		role, _ := s.Auth.GetRole(req.Name)

		// Format permissions as strings
		permStrings := make([]string, len(role.Permissions))
		for i, perm := range role.Permissions {
			level := "read-only"
			if perm.AccessLevel == auth.ReadWrite {
				level = "read-write"
			} else if perm.AccessLevel == auth.Admin {
				level = "admin"
			}
			permStrings[i] = fmt.Sprintf("%s (%s)", perm.TopicPattern, level)
		}

		response := RoleResponse{
			Name:        role.Name,
			Description: role.Description,
			Permissions: permStrings,
			CreatedAt:   role.CreatedAt,
			UpdatedAt:   role.UpdatedAt,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(response)
	}
}

// handleGetRole handles the endpoint to get a role by name
func (s *Server) handleGetRole() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.IsRBACEnabled() {
			http.Error(w, "RBAC is not enabled", http.StatusNotImplemented)
			return
		}

		roleName := chi.URLParam(r, "roleName")

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Only admins can view roles
		if !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required", http.StatusForbidden)
			return
		}

		// Get the role
		role, err := s.Auth.GetRole(roleName)
		if err != nil {
			http.Error(w, "Role not found", http.StatusNotFound)
			return
		}

		// Format permissions as strings
		permStrings := make([]string, len(role.Permissions))
		for i, perm := range role.Permissions {
			level := "read-only"
			if perm.AccessLevel == auth.ReadWrite {
				level = "read-write"
			} else if perm.AccessLevel == auth.Admin {
				level = "admin"
			}
			permStrings[i] = fmt.Sprintf("%s (%s)", perm.TopicPattern, level)
		}

		response := RoleResponse{
			Name:        role.Name,
			Description: role.Description,
			Permissions: permStrings,
			CreatedAt:   role.CreatedAt,
			UpdatedAt:   role.UpdatedAt,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// handleUpdateRole handles the endpoint to update a role
func (s *Server) handleUpdateRole() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.IsRBACEnabled() {
			http.Error(w, "RBAC is not enabled", http.StatusNotImplemented)
			return
		}

		roleName := chi.URLParam(r, "roleName")

		var req RoleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Only admins can update roles
		if !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required", http.StatusForbidden)
			return
		}

		// Convert permissions
		permissions := make([]auth.Permission, len(req.Permissions))
		for i, perm := range req.Permissions {
			var accessLevel auth.AccessLevel

			// Handle different access level formats
			switch v := perm.AccessLevel.(type) {
			case float64:
				accessLevel = auth.AccessLevel(int(v))
			case int:
				accessLevel = auth.AccessLevel(v)
			case string:
				switch v {
				case "read-only":
					accessLevel = auth.ReadOnly
				case "read-write":
					accessLevel = auth.ReadWrite
				case "admin":
					accessLevel = auth.Admin
				default:
					accessLevel = auth.ReadOnly
				}
			default:
				accessLevel = auth.ReadOnly
			}

			permissions[i] = auth.Permission{
				TopicPattern: perm.TopicPattern,
				AccessLevel:  accessLevel,
			}
		}

		// Update the role
		if err := s.Auth.UpdateRole(roleName, req.Description, permissions); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Log the action
		details := map[string]any{
			"description":       req.Description,
			"permissions_count": len(permissions),
		}
		s.logAction(r, "update_role", "role", roleName, details)

		// Get the updated role
		role, _ := s.Auth.GetRole(roleName)

		// Format permissions as strings
		permStrings := make([]string, len(role.Permissions))
		for i, perm := range role.Permissions {
			level := "read-only"
			if perm.AccessLevel == auth.ReadWrite {
				level = "read-write"
			} else if perm.AccessLevel == auth.Admin {
				level = "admin"
			}
			permStrings[i] = fmt.Sprintf("%s (%s)", perm.TopicPattern, level)
		}

		response := RoleResponse{
			Name:        role.Name,
			Description: role.Description,
			Permissions: permStrings,
			CreatedAt:   role.CreatedAt,
			UpdatedAt:   role.UpdatedAt,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// handleDeleteRole handles the endpoint to delete a role
func (s *Server) handleDeleteRole() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.IsRBACEnabled() {
			http.Error(w, "RBAC is not enabled", http.StatusNotImplemented)
			return
		}

		roleName := chi.URLParam(r, "roleName")

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Only admins can delete roles
		if !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required", http.StatusForbidden)
			return
		}

		// Don't allow deleting the default role
		if roleName == s.Auth.GetDefaultRole() {
			http.Error(w, "Cannot delete the default role", http.StatusBadRequest)
			return
		}

		// Delete the role
		if err := s.Auth.DeleteRole(roleName); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		// Log the action
		s.logAction(r, "delete_role", "role", roleName, nil)

		w.WriteHeader(http.StatusNoContent)
	}
}

// handleAssignRoleToUser handles the endpoint to assign a role to a user
func (s *Server) handleAssignRoleToUser() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.IsRBACEnabled() {
			http.Error(w, "RBAC is not enabled", http.StatusNotImplemented)
			return
		}

		roleName := chi.URLParam(r, "roleName")
		username := chi.URLParam(r, "username")

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Only admins can assign roles
		if !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required", http.StatusForbidden)
			return
		}

		// Assign the role to the user
		if err := s.Auth.AssignRoleToUser(username, roleName); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Log the action
		details := map[string]any{
			"role_name": roleName,
			"username":  username,
		}
		s.logAction(r, "assign_role", "user_role", username, details)

		w.WriteHeader(http.StatusNoContent)
	}
}

// handleRemoveRoleFromUser handles the endpoint to remove a role from a user
func (s *Server) handleRemoveRoleFromUser() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.IsRBACEnabled() {
			http.Error(w, "RBAC is not enabled", http.StatusNotImplemented)
			return
		}

		roleName := chi.URLParam(r, "roleName")
		username := chi.URLParam(r, "username")

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Only admins can remove roles
		if !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required", http.StatusForbidden)
			return
		}

		// Remove the role from the user
		if err := s.Auth.RemoveRoleFromUser(username, roleName); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Log the action
		details := map[string]any{
			"role_name": roleName,
			"username":  username,
		}
		s.logAction(r, "remove_role", "user_role", username, details)

		w.WriteHeader(http.StatusNoContent)
	}
}

// handleGetUserRoles handles the endpoint to get roles assigned to a user
func (s *Server) handleGetUserRoles() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Auth.IsRBACEnabled() {
			http.Error(w, "RBAC is not enabled", http.StatusNotImplemented)
			return
		}

		username := chi.URLParam(r, "username")

		// Get current user from context to check permissions
		ctx := r.Context()
		claims := ctx.Value(userContextKey).(*auth.Claims)
		currentUser, err := s.Auth.GetUser(claims.Username)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Users can view their own roles, admins can view any user's roles
		if username != claims.Username && !currentUser.IsAdmin {
			http.Error(w, "Admin privileges required to view other users' roles", http.StatusForbidden)
			return
		}

		// Get the roles
		roles, err := s.Auth.GetUserRoles(username)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		// Format response
		response := make([]RoleResponse, 0, len(roles))
		for _, role := range roles {
			// Format permissions as strings
			permStrings := make([]string, len(role.Permissions))
			for i, perm := range role.Permissions {
				level := "read-only"
				if perm.AccessLevel == auth.ReadWrite {
					level = "read-write"
				} else if perm.AccessLevel == auth.Admin {
					level = "admin"
				}
				permStrings[i] = fmt.Sprintf("%s (%s)", perm.TopicPattern, level)
			}

			response = append(response, RoleResponse{
				Name:        role.Name,
				Description: role.Description,
				Permissions: permStrings,
				CreatedAt:   role.CreatedAt,
				UpdatedAt:   role.UpdatedAt,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// SetMQTTServer sets the MQTT server reference for health checks.
//
// Parameters:
//   - mqttServer: Reference to the MQTT server instance
func (s *Server) SetMQTTServer(mqttServer any) {
	s.MQTTServer = mqttServer
}

// handleHealthCheck returns an HTTP handler function for the health check endpoint.
// This endpoint provides status information about the API server and its dependent services.
func (s *Server) handleHealthCheck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Health check response structure
		healthStatus := map[string]any{
			"status":    "ok",
			"timestamp": time.Now().Format(time.RFC3339),
			"services": map[string]any{
				"api": map[string]string{
					"status": "ok",
				},
			},
		}

		// Add services status map if not exists
		services, ok := healthStatus["services"].(map[string]any)
		if !ok {
			services = make(map[string]any)
			healthStatus["services"] = services
		}

		// Check storage connection if available
		if s.Storage != nil {
			// Create a minimal query to check if storage is working
			query := storage.MessageQuery{
				Limit: 1,
			}

			_, err := s.Storage.GetMessages(query)
			if err != nil {
				healthStatus["status"] = "degraded"
				services["storage"] = map[string]string{
					"status":  "error",
					"message": err.Error(),
				}
			} else {
				services["storage"] = map[string]string{
					"status": "ok",
				}
			}
		}

		// MQTT server status
		if s.MQTTServer != nil {
			// We just check if the MQTT server reference exists
			// In a real implementation, you could call a method on the MQTT server to check its status
			services["mqtt"] = map[string]string{
				"status": "ok",
			}
		} else {
			healthStatus["status"] = "degraded"
			services["mqtt"] = map[string]string{
				"status":  "unknown",
				"message": "MQTT server reference not set",
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(healthStatus)
	}
}

// handleScalarYAML handles the scalar.yaml endpoint.
// This endpoint serves the OpenAPI specification file used for API documentation.
func (s *Server) handleScalarYAML() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set the content type to YAML
		w.Header().Set("Content-Type", "application/yaml")

		// Try to read the scalar.yaml file
		content, err := os.ReadFile("api/scalar.yaml")
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read API documentation: %v", err), http.StatusInternalServerError)
			return
		}

		// Write the content to the response
		w.Write(content)
	}
}

// logAction is a helper function to log an action to the audit log
func (s *Server) logAction(r *http.Request, actionType, entityType, entityID string, details any) {
	// Get username from context if available
	var username string
	if claims, ok := r.Context().Value(userContextKey).(map[string]any); ok {
		if u, ok := claims["username"].(string); ok {
			username = u
		}
	}

	// Convert details to JSON
	var detailsJSON json.RawMessage
	if details != nil {
		if d, err := json.Marshal(details); err == nil {
			detailsJSON = d
		}
	}

	// Get IP address
	ipAddress := r.RemoteAddr
	// Check for X-Forwarded-For header
	if forwardedFor := r.Header.Get("X-Forwarded-For"); forwardedFor != "" {
		// Use the first IP in the list
		ipAddress = strings.Split(forwardedFor, ",")[0]
	}

	// Create audit log entry
	log := &storage.AuditLog{
		ActionType: actionType,
		Username:   username,
		EntityType: entityType,
		EntityID:   entityID,
		Details:    detailsJSON,
		IPAddress:  ipAddress,
	}

	// Log action asynchronously to not block the request
	go func() {
		if err := s.Storage.LogAction(log); err != nil {
			fmt.Printf("Failed to log action: %v\n", err)
		}
	}()
}

// handleGetAuditLogs handles retrieving audit logs with filtering
func (s *Server) handleGetAuditLogs() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		// Get audit logs
		logs, err := s.Storage.GetAuditLogs(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to retrieve audit logs: %v", err), http.StatusInternalServerError)
			return
		}

		// Log this action
		s.logAction(r, "get_audit_logs", "audit_logs", "", nil)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(logs); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	}
}

// handleGetUserAuditLogs handles retrieving audit logs for a specific user
func (s *Server) handleGetUserAuditLogs() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username := chi.URLParam(r, "username")
		if username == "" {
			http.Error(w, "Username is required", http.StatusBadRequest)
			return
		}

		// Create query for user's audit logs
		query := storage.AuditLogQuery{
			Username: username,
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

		// Get audit logs
		logs, err := s.Storage.GetAuditLogs(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to retrieve audit logs: %v", err), http.StatusInternalServerError)
			return
		}

		// Log this action
		s.logAction(r, "get_user_audit_logs", "user", username, nil)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(logs); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	}
}

// handleGetActionAuditLogs handles retrieving audit logs for a specific action type
func (s *Server) handleGetActionAuditLogs() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actionType := chi.URLParam(r, "actionType")
		if actionType == "" {
			http.Error(w, "Action type is required", http.StatusBadRequest)
			return
		}

		// Create query for action type
		query := storage.AuditLogQuery{
			ActionType: actionType,
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

		// Get audit logs
		logs, err := s.Storage.GetAuditLogs(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to retrieve audit logs: %v", err), http.StatusInternalServerError)
			return
		}

		// Log this action
		s.logAction(r, "get_action_audit_logs", "action_type", actionType, nil)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(logs); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	}
}

// handleGetEntityAuditLogs handles retrieving audit logs for a specific entity
func (s *Server) handleGetEntityAuditLogs() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entityType := chi.URLParam(r, "entityType")
		entityID := chi.URLParam(r, "entityID")

		if entityType == "" || entityID == "" {
			http.Error(w, "Entity type and ID are required", http.StatusBadRequest)
			return
		}

		// Create query for entity
		query := storage.AuditLogQuery{
			EntityType: entityType,
			EntityID:   entityID,
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

		// Get audit logs
		logs, err := s.Storage.GetAuditLogs(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to retrieve audit logs: %v", err), http.StatusInternalServerError)
			return
		}

		// Log this action
		s.logAction(r, "get_entity_audit_logs", entityType, entityID, nil)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(logs); err != nil {
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	}
}
