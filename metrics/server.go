package metrics

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server represents the metrics HTTP server
type Server struct {
	listenAddr string
	path       string
	httpServer *http.Server
}

// NewServer creates a new metrics server
func NewServer(host string, port int, path string) *Server {
	if path == "" {
		path = "/metrics"
	}
	if path[0] != '/' {
		path = "/" + path
	}

	return &Server{
		listenAddr: fmt.Sprintf("%s:%d", host, port),
		path:       path,
	}
}

// Start starts the metrics server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Register Prometheus metrics handler
	mux.Handle(s.path, promhttp.Handler())

	// Add a simple index page that links to metrics
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(fmt.Sprintf(`
			<html>
				<head><title>GoMQTT Metrics</title></head>
				<body>
					<h1>GoMQTT Metrics</h1>
					<p><a href="%s">Metrics</a></p>
				</body>
			</html>
		`, s.path)))
	})

	s.httpServer = &http.Server{
		Addr:    s.listenAddr,
		Handler: mux,
	}

	log.Printf("Starting metrics server on %s%s", s.listenAddr, s.path)
	return s.httpServer.ListenAndServe()
}

// Stop gracefully shuts down the metrics server
func (s *Server) Stop() error {
	if s.httpServer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return s.httpServer.Shutdown(ctx)
}
