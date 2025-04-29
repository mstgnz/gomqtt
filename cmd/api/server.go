package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	return http.ListenAndServe(s.ListenAddr, s.Router)
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
